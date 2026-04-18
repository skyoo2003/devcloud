// SPDX-License-Identifier: Apache-2.0

// internal/services/athena/provider_test.go
package athena

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/skyoo2003/devcloud/internal/plugin"
)

func newTestProvider(t *testing.T) *Provider {
	t.Helper()
	dir := t.TempDir()
	p := &Provider{}
	if err := p.Init(plugin.PluginConfig{DataDir: dir}); err != nil {
		t.Fatalf("Init: %v", err)
	}
	t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
	return p
}

func call(t *testing.T, p *Provider, action string, body map[string]any) map[string]any {
	t.Helper()
	b, _ := json.Marshal(body)
	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(b))
	req.Header.Set("X-Amz-Target", "AmazonAthena."+action)
	resp, err := p.HandleRequest(context.Background(), "", req)
	if err != nil {
		t.Fatalf("%s: unexpected error: %v", action, err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("%s: unexpected status %d: %s", action, resp.StatusCode, string(resp.Body))
	}
	var out map[string]any
	_ = json.Unmarshal(resp.Body, &out)
	return out
}

func TestMain(m *testing.M) {
	os.Exit(m.Run())
}

func TestWorkGroupCRUD(t *testing.T) {
	p := newTestProvider(t)

	// Create
	call(t, p, "CreateWorkGroup", map[string]any{
		"Name":        "my-wg",
		"Description": "test workgroup",
	})

	// Get
	out := call(t, p, "GetWorkGroup", map[string]any{"WorkGroup": "my-wg"})
	wg, _ := out["WorkGroup"].(map[string]any)
	if wg["Name"] != "my-wg" {
		t.Errorf("expected Name=my-wg, got %v", wg["Name"])
	}
	if wg["State"] != "ENABLED" {
		t.Errorf("expected State=ENABLED, got %v", wg["State"])
	}

	// List
	list := call(t, p, "ListWorkGroups", map[string]any{})
	wgs, _ := list["WorkGroups"].([]any)
	if len(wgs) != 1 {
		t.Errorf("expected 1 workgroup, got %d", len(wgs))
	}

	// Update
	call(t, p, "UpdateWorkGroup", map[string]any{
		"WorkGroup":   "my-wg",
		"Description": "updated",
		"State":       "DISABLED",
	})
	out2 := call(t, p, "GetWorkGroup", map[string]any{"WorkGroup": "my-wg"})
	wg2, _ := out2["WorkGroup"].(map[string]any)
	if wg2["State"] != "DISABLED" {
		t.Errorf("expected State=DISABLED after update, got %v", wg2["State"])
	}

	// Delete
	call(t, p, "DeleteWorkGroup", map[string]any{"WorkGroup": "my-wg"})
	list2 := call(t, p, "ListWorkGroups", map[string]any{})
	wgs2, _ := list2["WorkGroups"].([]any)
	if len(wgs2) != 0 {
		t.Errorf("expected 0 workgroups after delete, got %d", len(wgs2))
	}
}

func TestNamedQueryCRUD(t *testing.T) {
	p := newTestProvider(t)

	// Create
	out := call(t, p, "CreateNamedQuery", map[string]any{
		"Name":        "my-query",
		"QueryString": "SELECT 1",
		"Database":    "default",
		"WorkGroup":   "primary",
	})
	id, _ := out["NamedQueryId"].(string)
	if id == "" {
		t.Fatal("expected NamedQueryId")
	}

	// Get
	out2 := call(t, p, "GetNamedQuery", map[string]any{"NamedQueryId": id})
	nq, _ := out2["NamedQuery"].(map[string]any)
	if nq["Name"] != "my-query" {
		t.Errorf("expected Name=my-query, got %v", nq["Name"])
	}

	// List
	list := call(t, p, "ListNamedQueries", map[string]any{"WorkGroup": "primary"})
	ids, _ := list["NamedQueryIds"].([]any)
	if len(ids) != 1 {
		t.Errorf("expected 1 named query, got %d", len(ids))
	}

	// BatchGet
	batch := call(t, p, "BatchGetNamedQuery", map[string]any{"NamedQueryIds": []any{id}})
	queries, _ := batch["NamedQueries"].([]any)
	if len(queries) != 1 {
		t.Errorf("expected 1 result from BatchGetNamedQuery, got %d", len(queries))
	}

	// Delete
	call(t, p, "DeleteNamedQuery", map[string]any{"NamedQueryId": id})
	list2 := call(t, p, "ListNamedQueries", map[string]any{"WorkGroup": "primary"})
	ids2, _ := list2["NamedQueryIds"].([]any)
	if len(ids2) != 0 {
		t.Errorf("expected 0 after delete, got %d", len(ids2))
	}
}

func TestStartAndGetQueryExecution(t *testing.T) {
	p := newTestProvider(t)

	// Start
	out := call(t, p, "StartQueryExecution", map[string]any{
		"QueryString": "SELECT * FROM table",
		"WorkGroup":   "primary",
		"QueryExecutionContext": map[string]any{
			"Database": "default",
		},
	})
	qid, _ := out["QueryExecutionId"].(string)
	if qid == "" {
		t.Fatal("expected QueryExecutionId")
	}

	// Get
	out2 := call(t, p, "GetQueryExecution", map[string]any{"QueryExecutionId": qid})
	qe, _ := out2["QueryExecution"].(map[string]any)
	status, _ := qe["Status"].(map[string]any)
	if status["State"] != "SUCCEEDED" {
		t.Errorf("expected State=SUCCEEDED, got %v", status["State"])
	}

	// GetQueryResults
	res := call(t, p, "GetQueryResults", map[string]any{"QueryExecutionId": qid})
	if _, ok := res["ResultSet"]; !ok {
		t.Error("expected ResultSet in response")
	}

	// ListQueryExecutions
	list := call(t, p, "ListQueryExecutions", map[string]any{"WorkGroup": "primary"})
	ids, _ := list["QueryExecutionIds"].([]any)
	if len(ids) != 1 {
		t.Errorf("expected 1 execution, got %d", len(ids))
	}

	// BatchGet
	batch := call(t, p, "BatchGetQueryExecution", map[string]any{"QueryExecutionIds": []any{qid}})
	execs, _ := batch["QueryExecutions"].([]any)
	if len(execs) != 1 {
		t.Errorf("expected 1 from BatchGetQueryExecution, got %d", len(execs))
	}

	// GetQueryRuntimeStatistics
	stats := call(t, p, "GetQueryRuntimeStatistics", map[string]any{"QueryExecutionId": qid})
	if _, ok := stats["QueryRuntimeStatistics"]; !ok {
		t.Error("expected QueryRuntimeStatistics in response")
	}

	// Stop (cancel)
	call(t, p, "StopQueryExecution", map[string]any{"QueryExecutionId": qid})
	out3 := call(t, p, "GetQueryExecution", map[string]any{"QueryExecutionId": qid})
	qe3, _ := out3["QueryExecution"].(map[string]any)
	status3, _ := qe3["Status"].(map[string]any)
	if status3["State"] != "CANCELLED" {
		t.Errorf("expected State=CANCELLED after stop, got %v", status3["State"])
	}
}

func TestDataCatalogCRUD(t *testing.T) {
	p := newTestProvider(t)

	// Create
	call(t, p, "CreateDataCatalog", map[string]any{
		"Name":        "my-catalog",
		"Type":        "GLUE",
		"Description": "test catalog",
	})

	// Get
	out := call(t, p, "GetDataCatalog", map[string]any{"Name": "my-catalog"})
	dc, _ := out["DataCatalog"].(map[string]any)
	if dc["Name"] != "my-catalog" {
		t.Errorf("expected Name=my-catalog, got %v", dc["Name"])
	}
	if dc["Type"] != "GLUE" {
		t.Errorf("expected Type=GLUE, got %v", dc["Type"])
	}

	// List
	list := call(t, p, "ListDataCatalogs", map[string]any{})
	catalogs, _ := list["DataCatalogsSummary"].([]any)
	if len(catalogs) != 1 {
		t.Errorf("expected 1 catalog, got %d", len(catalogs))
	}

	// Update
	call(t, p, "UpdateDataCatalog", map[string]any{
		"Name":        "my-catalog",
		"Type":        "GLUE",
		"Description": "updated",
	})
	out2 := call(t, p, "GetDataCatalog", map[string]any{"Name": "my-catalog"})
	dc2, _ := out2["DataCatalog"].(map[string]any)
	if dc2["Description"] != "updated" {
		t.Errorf("expected Description=updated, got %v", dc2["Description"])
	}

	// Delete
	call(t, p, "DeleteDataCatalog", map[string]any{"Name": "my-catalog"})
	list2 := call(t, p, "ListDataCatalogs", map[string]any{})
	catalogs2, _ := list2["DataCatalogsSummary"].([]any)
	if len(catalogs2) != 0 {
		t.Errorf("expected 0 catalogs after delete, got %d", len(catalogs2))
	}
}

func TestPreparedStatementCRUD(t *testing.T) {
	p := newTestProvider(t)

	wg := "primary"

	// Create
	call(t, p, "CreatePreparedStatement", map[string]any{
		"StatementName":  "my-stmt",
		"WorkGroup":      wg,
		"QueryStatement": "SELECT ? FROM table",
		"Description":    "test stmt",
	})

	// Get
	out := call(t, p, "GetPreparedStatement", map[string]any{
		"StatementName": "my-stmt",
		"WorkGroup":     wg,
	})
	ps, _ := out["PreparedStatement"].(map[string]any)
	if ps["StatementName"] != "my-stmt" {
		t.Errorf("expected StatementName=my-stmt, got %v", ps["StatementName"])
	}

	// List
	list := call(t, p, "ListPreparedStatements", map[string]any{"WorkGroup": wg})
	stmts, _ := list["PreparedStatements"].([]any)
	if len(stmts) != 1 {
		t.Errorf("expected 1 prepared statement, got %d", len(stmts))
	}

	// BatchGet
	batch := call(t, p, "BatchGetPreparedStatement", map[string]any{
		"WorkGroup":              wg,
		"PreparedStatementNames": []any{"my-stmt"},
	})
	found, _ := batch["PreparedStatements"].([]any)
	if len(found) != 1 {
		t.Errorf("expected 1 from BatchGetPreparedStatement, got %d", len(found))
	}

	// Update
	call(t, p, "UpdatePreparedStatement", map[string]any{
		"StatementName":  "my-stmt",
		"WorkGroup":      wg,
		"QueryStatement": "SELECT ? FROM other_table",
	})
	out2 := call(t, p, "GetPreparedStatement", map[string]any{
		"StatementName": "my-stmt",
		"WorkGroup":     wg,
	})
	ps2, _ := out2["PreparedStatement"].(map[string]any)
	if ps2["QueryStatement"] != "SELECT ? FROM other_table" {
		t.Errorf("expected updated QueryStatement, got %v", ps2["QueryStatement"])
	}

	// Delete
	call(t, p, "DeletePreparedStatement", map[string]any{
		"StatementName": "my-stmt",
		"WorkGroup":     wg,
	})
	list2 := call(t, p, "ListPreparedStatements", map[string]any{"WorkGroup": wg})
	stmts2, _ := list2["PreparedStatements"].([]any)
	if len(stmts2) != 0 {
		t.Errorf("expected 0 after delete, got %d", len(stmts2))
	}
}

func TestTags(t *testing.T) {
	p := newTestProvider(t)

	// Create a workgroup to get an ARN
	call(t, p, "CreateWorkGroup", map[string]any{
		"Name": "tag-wg",
		"Tags": []any{
			map[string]any{"Key": "env", "Value": "test"},
		},
	})

	out := call(t, p, "GetWorkGroup", map[string]any{"WorkGroup": "tag-wg"})
	wg, _ := out["WorkGroup"].(map[string]any)
	_ = wg

	arn := "arn:aws:athena:us-east-1:000000000000:workgroup/tag-wg"

	// TagResource
	call(t, p, "TagResource", map[string]any{
		"ResourceARN": arn,
		"Tags": []any{
			map[string]any{"Key": "team", "Value": "data"},
		},
	})

	// ListTagsForResource
	tagOut := call(t, p, "ListTagsForResource", map[string]any{"ResourceARN": arn})
	tags, _ := tagOut["Tags"].([]any)
	if len(tags) < 1 {
		t.Errorf("expected at least 1 tag, got %d", len(tags))
	}

	// UntagResource
	call(t, p, "UntagResource", map[string]any{
		"ResourceARN": arn,
		"TagKeys":     []any{"team"},
	})
	tagOut2 := call(t, p, "ListTagsForResource", map[string]any{"ResourceARN": arn})
	tags2, _ := tagOut2["Tags"].([]any)
	// "env" tag was added during CreateWorkGroup, "team" removed
	for _, traw := range tags2 {
		tag, _ := traw.(map[string]string)
		if tag["Key"] == "team" {
			t.Error("expected 'team' tag to be removed")
		}
	}
}
