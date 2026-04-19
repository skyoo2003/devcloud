// SPDX-License-Identifier: Apache-2.0

// internal/services/glue/provider_test.go
package glue

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/skyoo2003/devcloud/internal/plugin"
)

func newTestProvider(t *testing.T) *Provider {
	t.Helper()
	p := &Provider{}
	if err := p.Init(plugin.PluginConfig{DataDir: t.TempDir()}); err != nil {
		t.Fatalf("Init: %v", err)
	}
	t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
	return p
}

func callOp(t *testing.T, p *Provider, op, body string) *plugin.Response {
	t.Helper()
	req := httptest.NewRequest("POST", "/"+op, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err := p.HandleRequest(context.Background(), op, req)
	if err != nil {
		t.Fatalf("%s: unexpected error: %v", op, err)
	}
	if resp == nil {
		t.Fatalf("%s: nil response", op)
	}
	return resp
}

func parseBody(t *testing.T, resp *plugin.Response) map[string]any {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal(resp.Body, &m); err != nil {
		t.Fatalf("unmarshal: %v (body=%s)", err, string(resp.Body))
	}
	return m
}

func assertOK(t *testing.T, resp *plugin.Response) {
	t.Helper()
	if resp.StatusCode != 200 {
		t.Fatalf("expected 200, got %d: %s", resp.StatusCode, string(resp.Body))
	}
}

func TestDatabaseCRUD(t *testing.T) {
	p := newTestProvider(t)

	// Create
	resp := callOp(t, p, "CreateDatabase", `{"DatabaseInput":{"Name":"mydb","Description":"test db"}}`)
	assertOK(t, resp)

	// Duplicate create
	resp = callOp(t, p, "CreateDatabase", `{"DatabaseInput":{"Name":"mydb"}}`)
	if resp.StatusCode == 200 {
		t.Fatal("expected error on duplicate database create")
	}

	// Get
	resp = callOp(t, p, "GetDatabase", `{"Name":"mydb"}`)
	assertOK(t, resp)
	body := parseBody(t, resp)
	db, _ := body["Database"].(map[string]any)
	if db["Name"] != "mydb" {
		t.Errorf("expected Name=mydb, got %v", db["Name"])
	}
	if db["Description"] != "test db" {
		t.Errorf("expected Description='test db', got %v", db["Description"])
	}

	// GetDatabases
	callOp(t, p, "CreateDatabase", `{"DatabaseInput":{"Name":"otherdb"}}`)
	resp = callOp(t, p, "GetDatabases", `{}`)
	assertOK(t, resp)
	body = parseBody(t, resp)
	list, _ := body["DatabaseList"].([]any)
	if len(list) < 2 {
		t.Errorf("expected at least 2 databases, got %d", len(list))
	}

	// UpdateDatabase
	resp = callOp(t, p, "UpdateDatabase", `{"Name":"mydb","DatabaseInput":{"Name":"mydb","Description":"updated"}}`)
	assertOK(t, resp)
	resp = callOp(t, p, "GetDatabase", `{"Name":"mydb"}`)
	body = parseBody(t, resp)
	db, _ = body["Database"].(map[string]any)
	if db["Description"] != "updated" {
		t.Errorf("expected Description=updated, got %v", db["Description"])
	}

	// DeleteDatabase
	resp = callOp(t, p, "DeleteDatabase", `{"Name":"mydb"}`)
	assertOK(t, resp)
	resp = callOp(t, p, "GetDatabase", `{"Name":"mydb"}`)
	if resp.StatusCode == 200 {
		t.Fatal("expected error after delete")
	}
}

func TestTableCRUD(t *testing.T) {
	p := newTestProvider(t)

	// Setup database
	callOp(t, p, "CreateDatabase", `{"DatabaseInput":{"Name":"testdb"}}`)

	// CreateTable
	resp := callOp(t, p, "CreateTable", `{
		"DatabaseName": "testdb",
		"TableInput": {
			"Name": "mytable",
			"Description": "test table",
			"TableType": "EXTERNAL_TABLE",
			"StorageDescriptor": {
				"Columns": [{"Name":"id","Type":"int"},{"Name":"val","Type":"string"}]
			}
		}
	}`)
	assertOK(t, resp)

	// GetTable
	resp = callOp(t, p, "GetTable", `{"DatabaseName":"testdb","Name":"mytable"}`)
	assertOK(t, resp)
	body := parseBody(t, resp)
	tbl, _ := body["Table"].(map[string]any)
	if tbl["Name"] != "mytable" {
		t.Errorf("expected Name=mytable, got %v", tbl["Name"])
	}

	// GetTables
	callOp(t, p, "CreateTable", `{"DatabaseName":"testdb","TableInput":{"Name":"anothertable"}}`)
	resp = callOp(t, p, "GetTables", `{"DatabaseName":"testdb"}`)
	assertOK(t, resp)
	body = parseBody(t, resp)
	list, _ := body["TableList"].([]any)
	if len(list) < 2 {
		t.Errorf("expected at least 2 tables, got %d", len(list))
	}

	// UpdateTable
	resp = callOp(t, p, "UpdateTable", `{"DatabaseName":"testdb","TableInput":{"Name":"mytable","Description":"updated table"}}`)
	assertOK(t, resp)

	// BatchDeleteTable
	resp = callOp(t, p, "BatchDeleteTable", `{"DatabaseName":"testdb","TablesToDelete":["anothertable"]}`)
	assertOK(t, resp)

	// GetTableVersion / GetTableVersions
	resp = callOp(t, p, "GetTableVersion", `{"DatabaseName":"testdb","TableName":"mytable"}`)
	assertOK(t, resp)
	resp = callOp(t, p, "GetTableVersions", `{"DatabaseName":"testdb","TableName":"mytable"}`)
	assertOK(t, resp)

	// DeleteTable
	resp = callOp(t, p, "DeleteTable", `{"DatabaseName":"testdb","Name":"mytable"}`)
	assertOK(t, resp)
	resp = callOp(t, p, "GetTable", `{"DatabaseName":"testdb","Name":"mytable"}`)
	if resp.StatusCode == 200 {
		t.Fatal("expected error after delete")
	}
}

func TestPartitionCRUD(t *testing.T) {
	p := newTestProvider(t)

	callOp(t, p, "CreateDatabase", `{"DatabaseInput":{"Name":"partdb"}}`)
	callOp(t, p, "CreateTable", `{"DatabaseName":"partdb","TableInput":{"Name":"parttable"}}`)

	// CreatePartition
	resp := callOp(t, p, "CreatePartition", `{
		"DatabaseName": "partdb",
		"TableName": "parttable",
		"PartitionInput": {"Values": ["2024-01-01"]}
	}`)
	assertOK(t, resp)

	// GetPartition
	resp = callOp(t, p, "GetPartition", `{"DatabaseName":"partdb","TableName":"parttable","PartitionValues":["2024-01-01"]}`)
	assertOK(t, resp)
	body := parseBody(t, resp)
	part, _ := body["Partition"].(map[string]any)
	vals, _ := part["Values"].([]any)
	if len(vals) != 1 || vals[0] != "2024-01-01" {
		t.Errorf("expected Values=[2024-01-01], got %v", vals)
	}

	// GetPartitions
	callOp(t, p, "CreatePartition", `{"DatabaseName":"partdb","TableName":"parttable","PartitionInput":{"Values":["2024-01-02"]}}`)
	resp = callOp(t, p, "GetPartitions", `{"DatabaseName":"partdb","TableName":"parttable"}`)
	assertOK(t, resp)
	body = parseBody(t, resp)
	parts, _ := body["Partitions"].([]any)
	if len(parts) < 2 {
		t.Errorf("expected at least 2 partitions, got %d", len(parts))
	}

	// BatchCreatePartition
	resp = callOp(t, p, "BatchCreatePartition", `{
		"DatabaseName": "partdb",
		"TableName": "parttable",
		"PartitionInputList": [
			{"Values": ["2024-01-03"]},
			{"Values": ["2024-01-04"]}
		]
	}`)
	assertOK(t, resp)

	// BatchGetPartition
	resp = callOp(t, p, "BatchGetPartition", `{
		"DatabaseName": "partdb",
		"TableName": "parttable",
		"PartitionsToGet": [
			{"Values": ["2024-01-01"]},
			{"Values": ["2024-01-03"]}
		]
	}`)
	assertOK(t, resp)
	body = parseBody(t, resp)
	found, _ := body["Partitions"].([]any)
	if len(found) != 2 {
		t.Errorf("expected 2 partitions, got %d", len(found))
	}

	// BatchDeletePartition
	resp = callOp(t, p, "BatchDeletePartition", `{
		"DatabaseName": "partdb",
		"TableName": "parttable",
		"PartitionsToDelete": [{"Values":["2024-01-03"]},{"Values":["2024-01-04"]}]
	}`)
	assertOK(t, resp)
}

func TestCrawlerCRUD(t *testing.T) {
	p := newTestProvider(t)

	// CreateCrawler
	resp := callOp(t, p, "CreateCrawler", `{
		"Name": "mycrawler",
		"Role": "arn:aws:iam::000000000000:role/glue-role",
		"DatabaseName": "mydb",
		"Targets": {"S3Targets": [{"Path": "s3://bucket/prefix"}]}
	}`)
	assertOK(t, resp)

	// GetCrawler
	resp = callOp(t, p, "GetCrawler", `{"Name":"mycrawler"}`)
	assertOK(t, resp)
	body := parseBody(t, resp)
	c, _ := body["Crawler"].(map[string]any)
	if c["Name"] != "mycrawler" {
		t.Errorf("expected Name=mycrawler, got %v", c["Name"])
	}

	// GetCrawlers
	callOp(t, p, "CreateCrawler", `{"Name":"crawler2","Role":"role2","DatabaseName":"db2","Targets":{}}`)
	resp = callOp(t, p, "GetCrawlers", `{}`)
	assertOK(t, resp)
	body = parseBody(t, resp)
	list, _ := body["Crawlers"].([]any)
	if len(list) < 2 {
		t.Errorf("expected at least 2 crawlers, got %d", len(list))
	}

	// StartCrawler / StopCrawler
	resp = callOp(t, p, "StartCrawler", `{"Name":"mycrawler"}`)
	assertOK(t, resp)
	resp = callOp(t, p, "GetCrawler", `{"Name":"mycrawler"}`)
	body = parseBody(t, resp)
	c, _ = body["Crawler"].(map[string]any)
	if c["State"] != "RUNNING" {
		t.Errorf("expected State=RUNNING, got %v", c["State"])
	}
	resp = callOp(t, p, "StopCrawler", `{"Name":"mycrawler"}`)
	assertOK(t, resp)

	// BatchGetCrawlers
	resp = callOp(t, p, "BatchGetCrawlers", `{"CrawlerNames":["mycrawler","crawler2","nonexistent"]}`)
	assertOK(t, resp)
	body = parseBody(t, resp)
	found, _ := body["Crawlers"].([]any)
	if len(found) != 2 {
		t.Errorf("expected 2 crawlers, got %d", len(found))
	}
	missing, _ := body["CrawlersNotFound"].([]any)
	if len(missing) != 1 {
		t.Errorf("expected 1 missing crawler, got %d", len(missing))
	}

	// UpdateCrawler
	resp = callOp(t, p, "UpdateCrawler", `{"Name":"mycrawler","Role":"updated-role","DatabaseName":"newdb","Targets":{}}`)
	assertOK(t, resp)

	// DeleteCrawler
	resp = callOp(t, p, "DeleteCrawler", `{"Name":"mycrawler"}`)
	assertOK(t, resp)
	resp = callOp(t, p, "GetCrawler", `{"Name":"mycrawler"}`)
	if resp.StatusCode == 200 {
		t.Fatal("expected error after delete")
	}
}

func TestJobCRUD(t *testing.T) {
	p := newTestProvider(t)

	// CreateJob
	resp := callOp(t, p, "CreateJob", `{
		"Name": "myjob",
		"Role": "arn:aws:iam::000000000000:role/glue-role",
		"Command": {"Name":"glueetl","ScriptLocation":"s3://bucket/script.py"},
		"MaxRetries": 2,
		"Timeout": 60
	}`)
	assertOK(t, resp)
	body := parseBody(t, resp)
	if body["Name"] != "myjob" {
		t.Errorf("expected Name=myjob, got %v", body["Name"])
	}

	// GetJob
	resp = callOp(t, p, "GetJob", `{"JobName":"myjob"}`)
	assertOK(t, resp)
	body = parseBody(t, resp)
	j, _ := body["Job"].(map[string]any)
	if j["Name"] != "myjob" {
		t.Errorf("expected Name=myjob, got %v", j["Name"])
	}

	// GetJobs
	callOp(t, p, "CreateJob", `{"Name":"job2","Role":"role2","Command":{}}`)
	resp = callOp(t, p, "GetJobs", `{}`)
	assertOK(t, resp)
	body = parseBody(t, resp)
	list, _ := body["Jobs"].([]any)
	if len(list) < 2 {
		t.Errorf("expected at least 2 jobs, got %d", len(list))
	}

	// StartJobRun
	resp = callOp(t, p, "StartJobRun", `{"JobName":"myjob"}`)
	assertOK(t, resp)
	body = parseBody(t, resp)
	runID, _ := body["JobRunId"].(string)
	if runID == "" {
		t.Fatal("expected JobRunId")
	}

	// GetJobRun
	resp = callOp(t, p, "GetJobRun", `{"RunId":"`+runID+`"}`)
	assertOK(t, resp)
	body = parseBody(t, resp)
	run, _ := body["JobRun"].(map[string]any)
	if run["Id"] != runID {
		t.Errorf("expected Id=%s, got %v", runID, run["Id"])
	}

	// GetJobRuns
	resp = callOp(t, p, "GetJobRuns", `{"JobName":"myjob"}`)
	assertOK(t, resp)
	body = parseBody(t, resp)
	runs, _ := body["JobRuns"].([]any)
	if len(runs) < 1 {
		t.Errorf("expected at least 1 run, got %d", len(runs))
	}

	// BatchGetJobs
	resp = callOp(t, p, "BatchGetJobs", `{"JobNames":["myjob","job2","nonexistent"]}`)
	assertOK(t, resp)
	body = parseBody(t, resp)
	foundJobs, _ := body["Jobs"].([]any)
	if len(foundJobs) != 2 {
		t.Errorf("expected 2 jobs, got %d", len(foundJobs))
	}

	// UpdateJob
	resp = callOp(t, p, "UpdateJob", `{"JobName":"myjob","JobUpdate":{"Role":"updated-role","Command":{}}}`)
	assertOK(t, resp)

	// DeleteJob
	resp = callOp(t, p, "DeleteJob", `{"JobName":"myjob"}`)
	assertOK(t, resp)
	resp = callOp(t, p, "GetJob", `{"JobName":"myjob"}`)
	if resp.StatusCode == 200 {
		t.Fatal("expected error after delete")
	}
}

func TestConnectionCRUD(t *testing.T) {
	p := newTestProvider(t)

	// CreateConnection
	resp := callOp(t, p, "CreateConnection", `{
		"ConnectionInput": {
			"Name": "myconn",
			"ConnectionType": "JDBC",
			"ConnectionProperties": {"JDBC_CONNECTION_URL":"jdbc:mysql://localhost:3306/db","USERNAME":"user","PASSWORD":"pass"}
		}
	}`)
	assertOK(t, resp)

	// GetConnection
	resp = callOp(t, p, "GetConnection", `{"Name":"myconn"}`)
	assertOK(t, resp)
	body := parseBody(t, resp)
	c, _ := body["Connection"].(map[string]any)
	if c["Name"] != "myconn" {
		t.Errorf("expected Name=myconn, got %v", c["Name"])
	}
	if c["ConnectionType"] != "JDBC" {
		t.Errorf("expected ConnectionType=JDBC, got %v", c["ConnectionType"])
	}

	// GetConnections
	callOp(t, p, "CreateConnection", `{"ConnectionInput":{"Name":"conn2","ConnectionType":"MONGODB","ConnectionProperties":{}}}`)
	resp = callOp(t, p, "GetConnections", `{}`)
	assertOK(t, resp)
	body = parseBody(t, resp)
	list, _ := body["ConnectionList"].([]any)
	if len(list) < 2 {
		t.Errorf("expected at least 2 connections, got %d", len(list))
	}

	// UpdateConnection
	resp = callOp(t, p, "UpdateConnection", `{"Name":"myconn","ConnectionInput":{"Name":"myconn","ConnectionType":"KAFKA","ConnectionProperties":{}}}`)
	assertOK(t, resp)
	resp = callOp(t, p, "GetConnection", `{"Name":"myconn"}`)
	body = parseBody(t, resp)
	c, _ = body["Connection"].(map[string]any)
	if c["ConnectionType"] != "KAFKA" {
		t.Errorf("expected ConnectionType=KAFKA, got %v", c["ConnectionType"])
	}

	// BatchDeleteConnection
	resp = callOp(t, p, "BatchDeleteConnection", `{"ConnectionNameList":["conn2"]}`)
	assertOK(t, resp)

	// DeleteConnection
	resp = callOp(t, p, "DeleteConnection", `{"ConnectionName":"myconn"}`)
	assertOK(t, resp)
	resp = callOp(t, p, "GetConnection", `{"Name":"myconn"}`)
	if resp.StatusCode == 200 {
		t.Fatal("expected error after delete")
	}
}

func TestTriggerCRUD(t *testing.T) {
	p := newTestProvider(t)

	// CreateTrigger
	resp := callOp(t, p, "CreateTrigger", `{
		"Name": "mytrigger",
		"Type": "ON_DEMAND",
		"Actions": [{"JobName":"myjob"}]
	}`)
	assertOK(t, resp)
	body := parseBody(t, resp)
	if body["Name"] != "mytrigger" {
		t.Errorf("expected Name=mytrigger, got %v", body["Name"])
	}

	// GetTrigger
	resp = callOp(t, p, "GetTrigger", `{"Name":"mytrigger"}`)
	assertOK(t, resp)
	body = parseBody(t, resp)
	trig, _ := body["Trigger"].(map[string]any)
	if trig["Name"] != "mytrigger" {
		t.Errorf("expected Name=mytrigger, got %v", trig["Name"])
	}
	if trig["Type"] != "ON_DEMAND" {
		t.Errorf("expected Type=ON_DEMAND, got %v", trig["Type"])
	}

	// GetTriggers
	callOp(t, p, "CreateTrigger", `{"Name":"trigger2","Type":"SCHEDULED","Schedule":"cron(0 * * * ? *)","Actions":[]}`)
	resp = callOp(t, p, "GetTriggers", `{}`)
	assertOK(t, resp)
	body = parseBody(t, resp)
	list, _ := body["Triggers"].([]any)
	if len(list) < 2 {
		t.Errorf("expected at least 2 triggers, got %d", len(list))
	}

	// StartTrigger / StopTrigger
	resp = callOp(t, p, "StartTrigger", `{"Name":"mytrigger"}`)
	assertOK(t, resp)
	resp = callOp(t, p, "GetTrigger", `{"Name":"mytrigger"}`)
	body = parseBody(t, resp)
	trig, _ = body["Trigger"].(map[string]any)
	if trig["State"] != "ACTIVATED" {
		t.Errorf("expected State=ACTIVATED, got %v", trig["State"])
	}
	resp = callOp(t, p, "StopTrigger", `{"Name":"mytrigger"}`)
	assertOK(t, resp)

	// BatchGetTriggers
	resp = callOp(t, p, "BatchGetTriggers", `{"TriggerNames":["mytrigger","trigger2","nonexistent"]}`)
	assertOK(t, resp)
	body = parseBody(t, resp)
	found, _ := body["Triggers"].([]any)
	if len(found) != 2 {
		t.Errorf("expected 2 triggers, got %d", len(found))
	}
	missing, _ := body["TriggersNotFound"].([]any)
	if len(missing) != 1 {
		t.Errorf("expected 1 missing trigger, got %d", len(missing))
	}

	// UpdateTrigger
	resp = callOp(t, p, "UpdateTrigger", `{"Name":"mytrigger","TriggerUpdate":{"Actions":[{"JobName":"otherjob"}]}}`)
	assertOK(t, resp)

	// DeleteTrigger
	resp = callOp(t, p, "DeleteTrigger", `{"Name":"mytrigger"}`)
	assertOK(t, resp)
	resp = callOp(t, p, "GetTrigger", `{"Name":"mytrigger"}`)
	if resp.StatusCode == 200 {
		t.Fatal("expected error after delete")
	}
}

func TestSecurityConfigurationCRUD(t *testing.T) {
	p := newTestProvider(t)

	// Create
	resp := callOp(t, p, "CreateSecurityConfiguration", `{
		"Name": "myconfig",
		"EncryptionConfiguration": {"S3Encryption":[{"S3EncryptionMode":"SSE-S3"}]}
	}`)
	assertOK(t, resp)
	body := parseBody(t, resp)
	if body["Name"] != "myconfig" {
		t.Errorf("expected Name=myconfig, got %v", body["Name"])
	}

	// Get
	resp = callOp(t, p, "GetSecurityConfiguration", `{"Name":"myconfig"}`)
	assertOK(t, resp)
	body = parseBody(t, resp)
	cfg, _ := body["SecurityConfiguration"].(map[string]any)
	if cfg["Name"] != "myconfig" {
		t.Errorf("expected Name=myconfig, got %v", cfg["Name"])
	}

	// GetSecurityConfigurations
	callOp(t, p, "CreateSecurityConfiguration", `{"Name":"config2","EncryptionConfiguration":{}}`)
	resp = callOp(t, p, "GetSecurityConfigurations", `{}`)
	assertOK(t, resp)
	body = parseBody(t, resp)
	list, _ := body["SecurityConfigurations"].([]any)
	if len(list) < 2 {
		t.Errorf("expected at least 2 configs, got %d", len(list))
	}

	// Delete
	resp = callOp(t, p, "DeleteSecurityConfiguration", `{"Name":"myconfig"}`)
	assertOK(t, resp)
	resp = callOp(t, p, "GetSecurityConfiguration", `{"Name":"myconfig"}`)
	if resp.StatusCode == 200 {
		t.Fatal("expected error after delete")
	}
}

func TestTags(t *testing.T) {
	p := newTestProvider(t)

	arn := "arn:aws:glue:us-east-1:000000000000:database/mydb"

	// TagResource
	resp := callOp(t, p, "TagResource", `{"ResourceArn":"`+arn+`","TagsToAdd":{"env":"prod","team":"data"}}`)
	assertOK(t, resp)

	// GetTags
	resp = callOp(t, p, "GetTags", `{"ResourceArn":"`+arn+`"}`)
	assertOK(t, resp)
	body := parseBody(t, resp)
	tags, _ := body["Tags"].(map[string]any)
	if tags["env"] != "prod" {
		t.Errorf("expected env=prod, got %v", tags["env"])
	}
	if tags["team"] != "data" {
		t.Errorf("expected team=data, got %v", tags["team"])
	}

	// UntagResource
	resp = callOp(t, p, "UntagResource", `{"ResourceArn":"`+arn+`","TagsToRemove":["env"]}`)
	assertOK(t, resp)
	resp = callOp(t, p, "GetTags", `{"ResourceArn":"`+arn+`"}`)
	body = parseBody(t, resp)
	tags, _ = body["Tags"].(map[string]any)
	if _, exists := tags["env"]; exists {
		t.Error("expected env tag to be removed")
	}
	if tags["team"] != "data" {
		t.Errorf("expected team=data, got %v", tags["team"])
	}

	// Stub ops should return 200
	resp = callOp(t, p, "GetDataflowGraph", `{}`)
	assertOK(t, resp)
	resp = callOp(t, p, "ListWorkflows", `{}`)
	assertOK(t, resp)
	resp = callOp(t, p, "GetMLTransform", `{}`)
	assertOK(t, resp)
}
