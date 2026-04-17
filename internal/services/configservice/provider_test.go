// SPDX-License-Identifier: Apache-2.0

// internal/services/configservice/provider_test.go
package configservice

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
	t.Cleanup(func() { p.Shutdown(context.Background()) })
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

func assertError(t *testing.T, resp *plugin.Response) {
	t.Helper()
	if resp.StatusCode == 200 {
		t.Fatalf("expected error response, got 200: %s", string(resp.Body))
	}
}

// TestConfigRuleCRUD tests Put/Describe/Delete of ConfigRule.
func TestConfigRuleCRUD(t *testing.T) {
	p := newTestProvider(t)

	// Put
	resp := callOp(t, p, "PutConfigRule", `{"ConfigRule":{"ConfigRuleName":"my-rule","Source":{"Owner":"AWS","SourceIdentifier":"S3_BUCKET_VERSIONING_ENABLED"}}}`)
	assertOK(t, resp)

	// Describe all
	resp = callOp(t, p, "DescribeConfigRules", `{}`)
	assertOK(t, resp)
	body := parseBody(t, resp)
	rules, _ := body["ConfigRules"].([]any)
	if len(rules) != 1 {
		t.Fatalf("expected 1 rule, got %d", len(rules))
	}
	rule, _ := rules[0].(map[string]any)
	if rule["ConfigRuleName"] != "my-rule" {
		t.Errorf("expected ConfigRuleName=my-rule, got %v", rule["ConfigRuleName"])
	}

	// Describe by name
	resp = callOp(t, p, "DescribeConfigRules", `{"ConfigRuleNames":["my-rule"]}`)
	assertOK(t, resp)
	body = parseBody(t, resp)
	rules, _ = body["ConfigRules"].([]any)
	if len(rules) != 1 {
		t.Fatalf("expected 1 rule by name filter, got %d", len(rules))
	}

	// Put second rule
	callOp(t, p, "PutConfigRule", `{"ConfigRule":{"ConfigRuleName":"rule-2","Source":{"Owner":"CUSTOM_LAMBDA","SourceIdentifier":"arn:aws:lambda:us-east-1:000000000000:function:my-fn"}}}`)
	resp = callOp(t, p, "DescribeConfigRules", `{}`)
	assertOK(t, resp)
	body = parseBody(t, resp)
	rules, _ = body["ConfigRules"].([]any)
	if len(rules) != 2 {
		t.Fatalf("expected 2 rules, got %d", len(rules))
	}

	// Delete
	resp = callOp(t, p, "DeleteConfigRule", `{"ConfigRuleName":"my-rule"}`)
	assertOK(t, resp)

	resp = callOp(t, p, "DescribeConfigRules", `{}`)
	assertOK(t, resp)
	body = parseBody(t, resp)
	rules, _ = body["ConfigRules"].([]any)
	if len(rules) != 1 {
		t.Fatalf("expected 1 rule after delete, got %d", len(rules))
	}

	// Delete non-existent
	resp = callOp(t, p, "DeleteConfigRule", `{"ConfigRuleName":"no-such-rule"}`)
	assertError(t, resp)

	// Missing required field
	resp = callOp(t, p, "PutConfigRule", `{}`)
	assertError(t, resp)
}

// TestConfigurationRecorderCRUD tests Put/Describe/Start/Stop/Delete of ConfigurationRecorder.
func TestConfigurationRecorderCRUD(t *testing.T) {
	p := newTestProvider(t)

	// Put
	resp := callOp(t, p, "PutConfigurationRecorder", `{"ConfigurationRecorder":{"name":"default","roleARN":"arn:aws:iam::000000000000:role/config-role"}}`)
	assertOK(t, resp)

	// Describe
	resp = callOp(t, p, "DescribeConfigurationRecorders", `{}`)
	assertOK(t, resp)
	body := parseBody(t, resp)
	recs, _ := body["ConfigurationRecorders"].([]any)
	if len(recs) != 1 {
		t.Fatalf("expected 1 recorder, got %d", len(recs))
	}
	rec, _ := recs[0].(map[string]any)
	if rec["name"] != "default" {
		t.Errorf("expected name=default, got %v", rec["name"])
	}

	// Status (should be STOPPED initially)
	resp = callOp(t, p, "DescribeConfigurationRecorderStatus", `{}`)
	assertOK(t, resp)
	body = parseBody(t, resp)
	statuses, _ := body["ConfigurationRecordersStatus"].([]any)
	if len(statuses) != 1 {
		t.Fatalf("expected 1 recorder status, got %d", len(statuses))
	}
	status, _ := statuses[0].(map[string]any)
	if status["recording"] != false {
		t.Errorf("expected recording=false, got %v", status["recording"])
	}

	// Start
	resp = callOp(t, p, "StartConfigurationRecorder", `{"ConfigurationRecorderName":"default"}`)
	assertOK(t, resp)

	resp = callOp(t, p, "DescribeConfigurationRecorderStatus", `{}`)
	assertOK(t, resp)
	body = parseBody(t, resp)
	statuses, _ = body["ConfigurationRecordersStatus"].([]any)
	status, _ = statuses[0].(map[string]any)
	if status["recording"] != true {
		t.Errorf("expected recording=true after start, got %v", status["recording"])
	}

	// Stop
	resp = callOp(t, p, "StopConfigurationRecorder", `{"ConfigurationRecorderName":"default"}`)
	assertOK(t, resp)

	// Delete
	resp = callOp(t, p, "DeleteConfigurationRecorder", `{"ConfigurationRecorderName":"default"}`)
	assertOK(t, resp)

	resp = callOp(t, p, "DescribeConfigurationRecorders", `{}`)
	assertOK(t, resp)
	body = parseBody(t, resp)
	recs, _ = body["ConfigurationRecorders"].([]any)
	if len(recs) != 0 {
		t.Fatalf("expected 0 recorders after delete, got %d", len(recs))
	}

	// Delete non-existent
	resp = callOp(t, p, "DeleteConfigurationRecorder", `{"ConfigurationRecorderName":"default"}`)
	assertError(t, resp)
}

// TestDeliveryChannelCRUD tests Put/Describe/Delete of DeliveryChannel.
func TestDeliveryChannelCRUD(t *testing.T) {
	p := newTestProvider(t)

	// Put
	resp := callOp(t, p, "PutDeliveryChannel", `{"DeliveryChannel":{"name":"default","s3BucketName":"my-config-bucket"}}`)
	assertOK(t, resp)

	// Describe
	resp = callOp(t, p, "DescribeDeliveryChannels", `{}`)
	assertOK(t, resp)
	body := parseBody(t, resp)
	channels, _ := body["DeliveryChannels"].([]any)
	if len(channels) != 1 {
		t.Fatalf("expected 1 channel, got %d", len(channels))
	}
	ch, _ := channels[0].(map[string]any)
	if ch["s3BucketName"] != "my-config-bucket" {
		t.Errorf("expected s3BucketName=my-config-bucket, got %v", ch["s3BucketName"])
	}

	// Channel status
	resp = callOp(t, p, "DescribeDeliveryChannelStatus", `{}`)
	assertOK(t, resp)
	body = parseBody(t, resp)
	statuses, _ := body["DeliveryChannelsStatus"].([]any)
	if len(statuses) != 1 {
		t.Fatalf("expected 1 channel status, got %d", len(statuses))
	}

	// Update
	callOp(t, p, "PutDeliveryChannel", `{"DeliveryChannel":{"name":"default","s3BucketName":"updated-bucket"}}`)
	resp = callOp(t, p, "DescribeDeliveryChannels", `{}`)
	assertOK(t, resp)
	body = parseBody(t, resp)
	channels, _ = body["DeliveryChannels"].([]any)
	ch, _ = channels[0].(map[string]any)
	if ch["s3BucketName"] != "updated-bucket" {
		t.Errorf("expected updated-bucket, got %v", ch["s3BucketName"])
	}

	// Delete
	resp = callOp(t, p, "DeleteDeliveryChannel", `{"DeliveryChannelName":"default"}`)
	assertOK(t, resp)

	resp = callOp(t, p, "DescribeDeliveryChannels", `{}`)
	assertOK(t, resp)
	body = parseBody(t, resp)
	channels, _ = body["DeliveryChannels"].([]any)
	if len(channels) != 0 {
		t.Fatalf("expected 0 channels after delete, got %d", len(channels))
	}

	// Delete non-existent
	resp = callOp(t, p, "DeleteDeliveryChannel", `{"DeliveryChannelName":"default"}`)
	assertError(t, resp)
}

// TestConformancePackCRUD tests Put/Describe/Delete of ConformancePack.
func TestConformancePackCRUD(t *testing.T) {
	p := newTestProvider(t)

	// Put
	resp := callOp(t, p, "PutConformancePack", `{"ConformancePackName":"my-pack","TemplateBody":"{}","DeliveryS3Bucket":"my-bucket"}`)
	assertOK(t, resp)
	body := parseBody(t, resp)
	if _, ok := body["ConformancePackArn"]; !ok {
		t.Error("expected ConformancePackArn in response")
	}

	// Describe
	resp = callOp(t, p, "DescribeConformancePacks", `{}`)
	assertOK(t, resp)
	body = parseBody(t, resp)
	packs, _ := body["ConformancePackDetails"].([]any)
	if len(packs) != 1 {
		t.Fatalf("expected 1 conformance pack, got %d", len(packs))
	}
	pack, _ := packs[0].(map[string]any)
	if pack["ConformancePackName"] != "my-pack" {
		t.Errorf("expected ConformancePackName=my-pack, got %v", pack["ConformancePackName"])
	}

	// Status
	resp = callOp(t, p, "DescribeConformancePackStatus", `{}`)
	assertOK(t, resp)
	body = parseBody(t, resp)
	statuses, _ := body["ConformancePackStatusDetails"].([]any)
	if len(statuses) != 1 {
		t.Fatalf("expected 1 conformance pack status, got %d", len(statuses))
	}

	// Describe by name
	resp = callOp(t, p, "DescribeConformancePacks", `{"ConformancePackNames":["my-pack"]}`)
	assertOK(t, resp)
	body = parseBody(t, resp)
	packs, _ = body["ConformancePackDetails"].([]any)
	if len(packs) != 1 {
		t.Fatalf("expected 1 pack by name, got %d", len(packs))
	}

	// Delete
	resp = callOp(t, p, "DeleteConformancePack", `{"ConformancePackName":"my-pack"}`)
	assertOK(t, resp)

	resp = callOp(t, p, "DescribeConformancePacks", `{}`)
	assertOK(t, resp)
	body = parseBody(t, resp)
	packs, _ = body["ConformancePackDetails"].([]any)
	if len(packs) != 0 {
		t.Fatalf("expected 0 packs after delete, got %d", len(packs))
	}

	// Delete non-existent
	resp = callOp(t, p, "DeleteConformancePack", `{"ConformancePackName":"no-such-pack"}`)
	assertError(t, resp)
}

// TestAggregatorCRUD tests Put/Describe/Delete of ConfigurationAggregator.
func TestAggregatorCRUD(t *testing.T) {
	p := newTestProvider(t)

	// Put
	resp := callOp(t, p, "PutConfigurationAggregator", `{"ConfigurationAggregatorName":"my-agg","AccountAggregationSources":[{"AccountIds":["111111111111"],"AllAwsRegions":true}]}`)
	assertOK(t, resp)
	body := parseBody(t, resp)
	agg, _ := body["ConfigurationAggregator"].(map[string]any)
	if agg["ConfigurationAggregatorName"] != "my-agg" {
		t.Errorf("expected ConfigurationAggregatorName=my-agg, got %v", agg["ConfigurationAggregatorName"])
	}

	// Describe all
	resp = callOp(t, p, "DescribeConfigurationAggregators", `{}`)
	assertOK(t, resp)
	body = parseBody(t, resp)
	aggs, _ := body["ConfigurationAggregators"].([]any)
	if len(aggs) != 1 {
		t.Fatalf("expected 1 aggregator, got %d", len(aggs))
	}

	// Sources status
	resp = callOp(t, p, "DescribeConfigurationAggregatorSourcesStatus", `{"ConfigurationAggregatorName":"my-agg"}`)
	assertOK(t, resp)

	// Put second
	callOp(t, p, "PutConfigurationAggregator", `{"ConfigurationAggregatorName":"my-agg-2","AccountAggregationSources":[]}`)
	resp = callOp(t, p, "DescribeConfigurationAggregators", `{}`)
	assertOK(t, resp)
	body = parseBody(t, resp)
	aggs, _ = body["ConfigurationAggregators"].([]any)
	if len(aggs) != 2 {
		t.Fatalf("expected 2 aggregators, got %d", len(aggs))
	}

	// Delete
	resp = callOp(t, p, "DeleteConfigurationAggregator", `{"ConfigurationAggregatorName":"my-agg"}`)
	assertOK(t, resp)

	resp = callOp(t, p, "DescribeConfigurationAggregators", `{}`)
	assertOK(t, resp)
	body = parseBody(t, resp)
	aggs, _ = body["ConfigurationAggregators"].([]any)
	if len(aggs) != 1 {
		t.Fatalf("expected 1 aggregator after delete, got %d", len(aggs))
	}

	// Delete non-existent
	resp = callOp(t, p, "DeleteConfigurationAggregator", `{"ConfigurationAggregatorName":"no-such-agg"}`)
	assertError(t, resp)
}

// TestStoredQueryCRUD tests Put/Get/List/Delete of StoredQuery.
func TestStoredQueryCRUD(t *testing.T) {
	p := newTestProvider(t)

	// Put
	resp := callOp(t, p, "PutStoredQuery", `{"StoredQuery":{"QueryName":"my-query","Expression":"SELECT * FROM AWS::EC2::Instance","Description":"list ec2 instances"}}`)
	assertOK(t, resp)
	body := parseBody(t, resp)
	if _, ok := body["QueryArn"]; !ok {
		t.Error("expected QueryArn in response")
	}

	// Get
	resp = callOp(t, p, "GetStoredQuery", `{"QueryName":"my-query"}`)
	assertOK(t, resp)
	body = parseBody(t, resp)
	q, _ := body["StoredQuery"].(map[string]any)
	if q["QueryName"] != "my-query" {
		t.Errorf("expected QueryName=my-query, got %v", q["QueryName"])
	}
	if q["Expression"] != "SELECT * FROM AWS::EC2::Instance" {
		t.Errorf("expected expression, got %v", q["Expression"])
	}

	// List
	callOp(t, p, "PutStoredQuery", `{"StoredQuery":{"QueryName":"other-query","Expression":"SELECT * FROM AWS::S3::Bucket"}}`)
	resp = callOp(t, p, "ListStoredQueries", `{}`)
	assertOK(t, resp)
	body = parseBody(t, resp)
	queries, _ := body["StoredQueryMetadata"].([]any)
	if len(queries) != 2 {
		t.Fatalf("expected 2 stored queries, got %d", len(queries))
	}

	// Update (idempotent put keeps same ID)
	arn1 := q["QueryArn"]
	resp = callOp(t, p, "PutStoredQuery", `{"StoredQuery":{"QueryName":"my-query","Expression":"SELECT resourceId FROM AWS::EC2::Instance"}}`)
	assertOK(t, resp)
	resp = callOp(t, p, "GetStoredQuery", `{"QueryName":"my-query"}`)
	body = parseBody(t, resp)
	q2, _ := body["StoredQuery"].(map[string]any)
	if q2["QueryArn"] != arn1 {
		t.Errorf("expected same ARN after update, got %v vs %v", q2["QueryArn"], arn1)
	}

	// Delete
	resp = callOp(t, p, "DeleteStoredQuery", `{"QueryName":"my-query"}`)
	assertOK(t, resp)

	resp = callOp(t, p, "ListStoredQueries", `{}`)
	assertOK(t, resp)
	body = parseBody(t, resp)
	queries, _ = body["StoredQueryMetadata"].([]any)
	if len(queries) != 1 {
		t.Fatalf("expected 1 stored query after delete, got %d", len(queries))
	}

	// Get non-existent
	resp = callOp(t, p, "GetStoredQuery", `{"QueryName":"my-query"}`)
	assertError(t, resp)

	// Delete non-existent
	resp = callOp(t, p, "DeleteStoredQuery", `{"QueryName":"my-query"}`)
	assertError(t, resp)
}

// TestTags tests TagResource/UntagResource/ListTagsForResource.
func TestTags(t *testing.T) {
	p := newTestProvider(t)

	arn := "arn:aws:config:us-east-1:000000000000:config-rule/my-rule"

	// Tag
	resp := callOp(t, p, "TagResource", `{"ResourceArn":"arn:aws:config:us-east-1:000000000000:config-rule/my-rule","Tags":[{"Key":"env","Value":"test"},{"Key":"team","Value":"platform"}]}`)
	assertOK(t, resp)

	// List
	resp = callOp(t, p, "ListTagsForResource", `{"ResourceArn":"arn:aws:config:us-east-1:000000000000:config-rule/my-rule"}`)
	assertOK(t, resp)
	body := parseBody(t, resp)
	tags, _ := body["Tags"].([]any)
	if len(tags) != 2 {
		t.Fatalf("expected 2 tags, got %d", len(tags))
	}

	// Untag
	resp = callOp(t, p, "UntagResource", `{"ResourceArn":"arn:aws:config:us-east-1:000000000000:config-rule/my-rule","TagKeys":["env"]}`)
	assertOK(t, resp)

	resp = callOp(t, p, "ListTagsForResource", `{"ResourceArn":"arn:aws:config:us-east-1:000000000000:config-rule/my-rule"}`)
	assertOK(t, resp)
	body = parseBody(t, resp)
	tags, _ = body["Tags"].([]any)
	if len(tags) != 1 {
		t.Fatalf("expected 1 tag after untag, got %d", len(tags))
	}

	// Missing ARN
	resp = callOp(t, p, "ListTagsForResource", `{}`)
	assertError(t, resp)

	_ = arn
}
