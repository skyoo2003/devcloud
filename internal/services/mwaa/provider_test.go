// SPDX-License-Identifier: Apache-2.0

// internal/services/mwaa/provider_test.go
package mwaa

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/skyoo2003/devcloud/internal/plugin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestProvider(t *testing.T) *Provider {
	t.Helper()
	p := &Provider{}
	err := p.Init(plugin.PluginConfig{DataDir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { p.Shutdown(context.Background()) })
	return p
}

func callREST(t *testing.T, p *Provider, method, path, op, body string) *plugin.Response {
	t.Helper()
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err := p.HandleRequest(context.Background(), op, req)
	require.NoError(t, err)
	return resp
}

func parseBody(t *testing.T, resp *plugin.Response) map[string]any {
	t.Helper()
	var m map[string]any
	require.NoError(t, json.Unmarshal(resp.Body, &m))
	return m
}

func TestCreateAndGetEnvironment(t *testing.T) {
	p := newTestProvider(t)

	// Create environment
	body := `{"AirflowVersion":"2.8.1","EnvironmentClass":"mw1.small","MaxWorkers":5,"MinWorkers":1,"SourceBucketArn":"arn:aws:s3:::my-bucket","DagS3Path":"dags","ExecutionRoleArn":"arn:aws:iam::000000000000:role/mwaa-role"}`
	resp := callREST(t, p, "PUT", "/environments/my-env", "CreateEnvironment", body)
	assert.Equal(t, 200, resp.StatusCode)
	rb := parseBody(t, resp)
	arn, ok := rb["Arn"].(string)
	require.True(t, ok)
	assert.NotEmpty(t, arn)

	// Get environment
	resp2 := callREST(t, p, "GET", "/environments/my-env", "GetEnvironment", "")
	assert.Equal(t, 200, resp2.StatusCode)
	rb2 := parseBody(t, resp2)
	env, ok := rb2["Environment"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "my-env", env["Name"])
	assert.Equal(t, "2.8.1", env["AirflowVersion"])
	assert.Equal(t, "AVAILABLE", env["Status"])

	// Duplicate create should conflict
	resp3 := callREST(t, p, "PUT", "/environments/my-env", "CreateEnvironment", body)
	assert.Equal(t, 409, resp3.StatusCode)

	// Get non-existent
	resp4 := callREST(t, p, "GET", "/environments/nonexistent", "GetEnvironment", "")
	assert.Equal(t, 404, resp4.StatusCode)
}

func TestListEnvironments(t *testing.T) {
	p := newTestProvider(t)

	callREST(t, p, "PUT", "/environments/env-a", "CreateEnvironment", `{}`)
	callREST(t, p, "PUT", "/environments/env-b", "CreateEnvironment", `{}`)
	callREST(t, p, "PUT", "/environments/env-c", "CreateEnvironment", `{}`)

	resp := callREST(t, p, "GET", "/environments", "ListEnvironments", "")
	assert.Equal(t, 200, resp.StatusCode)
	rb := parseBody(t, resp)
	envs, ok := rb["Environments"].([]any)
	require.True(t, ok)
	assert.Len(t, envs, 3)
}

func TestDeleteEnvironment(t *testing.T) {
	p := newTestProvider(t)

	callREST(t, p, "PUT", "/environments/to-delete", "CreateEnvironment", `{}`)

	// Delete
	resp := callREST(t, p, "DELETE", "/environments/to-delete", "DeleteEnvironment", "")
	assert.Equal(t, 200, resp.StatusCode)

	// Get after delete should 404
	resp2 := callREST(t, p, "GET", "/environments/to-delete", "GetEnvironment", "")
	assert.Equal(t, 404, resp2.StatusCode)

	// Delete non-existent should 404
	resp3 := callREST(t, p, "DELETE", "/environments/nonexistent", "DeleteEnvironment", "")
	assert.Equal(t, 404, resp3.StatusCode)
}

func TestUpdateEnvironment(t *testing.T) {
	p := newTestProvider(t)

	callREST(t, p, "PUT", "/environments/upd-env", "CreateEnvironment", `{"AirflowVersion":"2.7.0","MaxWorkers":3}`)

	// Update
	resp := callREST(t, p, "PATCH", "/environments/upd-env", "UpdateEnvironment",
		`{"AirflowVersion":"2.8.1","MaxWorkers":8}`)
	assert.Equal(t, 200, resp.StatusCode)
	rb := parseBody(t, resp)
	assert.NotEmpty(t, rb["Arn"])

	// Verify update via Get
	resp2 := callREST(t, p, "GET", "/environments/upd-env", "GetEnvironment", "")
	assert.Equal(t, 200, resp2.StatusCode)
	rb2 := parseBody(t, resp2)
	env := rb2["Environment"].(map[string]any)
	assert.Equal(t, "2.8.1", env["AirflowVersion"])
	assert.Equal(t, float64(8), env["MaxWorkers"])

	// Update non-existent
	resp3 := callREST(t, p, "PATCH", "/environments/nonexistent", "UpdateEnvironment", `{}`)
	assert.Equal(t, 404, resp3.StatusCode)
}

func TestDagCRUD(t *testing.T) {
	p := newTestProvider(t)

	callREST(t, p, "PUT", "/environments/dag-env", "CreateEnvironment", `{}`)

	// Create DAG
	resp := callREST(t, p, "POST", "/environments/dag-env/dags", "CreateDag",
		`{"DagId": "example_dag", "FileUri": "s3://bucket/example_dag.py"}`)
	assert.Equal(t, 200, resp.StatusCode)

	// List DAGs
	resp2 := callREST(t, p, "GET", "/environments/dag-env/dags", "ListDags", "")
	assert.Equal(t, 200, resp2.StatusCode)
	rb2 := parseBody(t, resp2)
	dags := rb2["Dags"].([]any)
	assert.Len(t, dags, 1)

	// Get DAG
	resp3 := callREST(t, p, "GET", "/environments/dag-env/dags/example_dag", "GetDag", "")
	assert.Equal(t, 200, resp3.StatusCode)
	rb3 := parseBody(t, resp3)
	assert.Equal(t, "example_dag", rb3["DagId"])

	// Pause DAG
	resp4 := callREST(t, p, "PATCH", "/environments/dag-env/dags/example_dag", "PauseDag",
		`{"Paused": true}`)
	assert.Equal(t, 200, resp4.StatusCode)

	// Delete DAG
	resp5 := callREST(t, p, "DELETE", "/environments/dag-env/dags/example_dag", "DeleteDag", "")
	assert.Equal(t, 200, resp5.StatusCode)
}

func TestDagRuns(t *testing.T) {
	p := newTestProvider(t)

	callREST(t, p, "PUT", "/environments/runs-env", "CreateEnvironment", `{}`)

	// Create DAG run
	resp := callREST(t, p, "POST", "/environments/runs-env/dagruns", "CreateDagRun",
		`{"DagId": "example_dag", "RunId": "manual_run_1", "State": "queued"}`)
	assert.Equal(t, 200, resp.StatusCode)

	// List DAG runs
	resp2 := callREST(t, p, "GET", "/environments/runs-env/dagruns", "ListDagRuns", "")
	assert.Equal(t, 200, resp2.StatusCode)

	// Get DAG run
	resp3 := callREST(t, p, "GET", "/environments/runs-env/dagruns/example_dag/manual_run_1", "GetDagRun", "")
	assert.Equal(t, 200, resp3.StatusCode)
	rb := parseBody(t, resp3)
	assert.Equal(t, "example_dag", rb["DagId"])
	assert.Equal(t, "manual_run_1", rb["RunId"])

	// Delete DAG run
	resp4 := callREST(t, p, "DELETE", "/environments/runs-env/dagruns/example_dag/manual_run_1", "DeleteDagRun", "")
	assert.Equal(t, 200, resp4.StatusCode)
}

func TestVariables(t *testing.T) {
	p := newTestProvider(t)

	callREST(t, p, "PUT", "/environments/var-env", "CreateEnvironment", `{}`)

	// Create variables
	resp := callREST(t, p, "POST", "/environments/var-env/variables", "CreateVariables",
		`{"Variables": [{"Key": "var1", "Value": "val1"}, {"Key": "var2", "Value": "val2", "IsEncrypted": true}]}`)
	assert.Equal(t, 200, resp.StatusCode)
	rb := parseBody(t, resp)
	created := rb["CreatedVariables"].([]any)
	assert.Len(t, created, 2)

	// List variables
	resp2 := callREST(t, p, "GET", "/environments/var-env/variables", "ListVariables", "")
	assert.Equal(t, 200, resp2.StatusCode)
	rb2 := parseBody(t, resp2)
	vars := rb2["Variables"].([]any)
	assert.Len(t, vars, 2)

	// Update variables
	resp3 := callREST(t, p, "PATCH", "/environments/var-env/variables", "UpdateVariables",
		`{"Variables": [{"Key": "var1", "Value": "updated"}]}`)
	assert.Equal(t, 200, resp3.StatusCode)

	// Get specific variable
	resp4 := callREST(t, p, "PUT", "/environments/var-env/variables", "GetVariables",
		`{"VariableNames": ["var1"]}`)
	assert.Equal(t, 200, resp4.StatusCode)
	rb4 := parseBody(t, resp4)
	list := rb4["Variables"].([]any)
	assert.Len(t, list, 1)
	first := list[0].(map[string]any)
	assert.Equal(t, "updated", first["Value"])

	// Delete variables
	resp5 := callREST(t, p, "DELETE", "/environments/var-env/variables", "DeleteVariables",
		`{"VariableNames": ["var1", "var2"]}`)
	assert.Equal(t, 200, resp5.StatusCode)
}

func TestCliAndWebToken(t *testing.T) {
	p := newTestProvider(t)

	callREST(t, p, "PUT", "/environments/tok-env", "CreateEnvironment", `{}`)

	// CLI token
	resp := callREST(t, p, "POST", "/clitoken/tok-env", "CreateCliToken", "")
	assert.Equal(t, 200, resp.StatusCode)
	rb := parseBody(t, resp)
	assert.NotEmpty(t, rb["CliToken"])

	// Web token
	resp2 := callREST(t, p, "POST", "/webtoken/tok-env", "CreateWebLoginToken", "")
	assert.Equal(t, 200, resp2.StatusCode)
	rb2 := parseBody(t, resp2)
	assert.NotEmpty(t, rb2["WebToken"])
}

func TestEnvironmentLifecycle(t *testing.T) {
	p := newTestProvider(t)

	callREST(t, p, "PUT", "/environments/life-env", "CreateEnvironment", `{}`)

	// Status
	resp := callREST(t, p, "GET", "/environments/life-env/status", "GetEnvironmentStatus", "")
	assert.Equal(t, 200, resp.StatusCode)

	// Config
	resp2 := callREST(t, p, "GET", "/environments/life-env/config", "GetEnvironmentConfig", "")
	assert.Equal(t, 200, resp2.StatusCode)

	// Restart/Start/Stop
	resp3 := callREST(t, p, "POST", "/environments/life-env/restart", "RestartEnvironment", "")
	assert.Equal(t, 200, resp3.StatusCode)

	resp4 := callREST(t, p, "POST", "/environments/life-env/start", "StartEnvironment", "")
	assert.Equal(t, 200, resp4.StatusCode)

	resp5 := callREST(t, p, "POST", "/environments/life-env/stop", "StopEnvironment", "")
	assert.Equal(t, 200, resp5.StatusCode)

	// Logs
	resp6 := callREST(t, p, "GET", "/environments/life-env/logs", "GetEnvironmentLogs", "")
	assert.Equal(t, 200, resp6.StatusCode)
}

func TestTags(t *testing.T) {
	p := newTestProvider(t)

	// Create environment
	createResp := callREST(t, p, "PUT", "/environments/tagged-env", "CreateEnvironment", `{}`)
	rb := parseBody(t, createResp)
	arn := rb["Arn"].(string)
	require.NotEmpty(t, arn)

	// TagResource
	tagBody, _ := json.Marshal(map[string]any{
		"Tags": map[string]string{"Env": "prod", "Team": "data"},
	})
	resp := callREST(t, p, "POST", "/tags/"+arn, "TagResource", string(tagBody))
	assert.Equal(t, 200, resp.StatusCode)

	// ListTagsForResource
	resp2 := callREST(t, p, "GET", "/tags/"+arn, "ListTagsForResource", "")
	assert.Equal(t, 200, resp2.StatusCode)
	rb2 := parseBody(t, resp2)
	tags, ok := rb2["Tags"].(map[string]any)
	require.True(t, ok)
	assert.Len(t, tags, 2)
	assert.Equal(t, "prod", tags["Env"])

	// UntagResource
	req := httptest.NewRequest("DELETE", "/tags/"+arn+"?tagKeys=Env", strings.NewReader(""))
	untagResp, err := p.HandleRequest(context.Background(), "UntagResource", req)
	require.NoError(t, err)
	assert.Equal(t, 200, untagResp.StatusCode)

	// Verify 1 tag remains via GetEnvironment (tags embedded)
	resp3 := callREST(t, p, "GET", "/tags/"+arn, "ListTagsForResource", "")
	rb3 := parseBody(t, resp3)
	tags3 := rb3["Tags"].(map[string]any)
	assert.Len(t, tags3, 1)
	assert.Equal(t, "data", tags3["Team"])
}
