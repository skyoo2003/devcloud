// SPDX-License-Identifier: Apache-2.0

package lambda

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/skyoo2003/devcloud/internal/plugin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestLambdaProvider(t *testing.T) *LambdaProvider {
	t.Helper()
	dir := t.TempDir()
	p := &LambdaProvider{}
	err := p.Init(plugin.PluginConfig{DataDir: dir})
	require.NoError(t, err)
	return p
}

// handleLambda fires a request through the provider and returns the response.
func handleLambda(t *testing.T, p *LambdaProvider, method, path string, body any) *plugin.Response {
	t.Helper()
	var reqBody string
	if body != nil {
		b, err := json.Marshal(body)
		require.NoError(t, err)
		reqBody = string(b)
	}
	req := httptest.NewRequest(method, path, strings.NewReader(reqBody))
	req.Header.Set("Content-Type", "application/json")
	resp, err := p.HandleRequest(context.Background(), "", req)
	require.NoError(t, err)
	return resp
}

func TestLambdaProvider_CreateFunction(t *testing.T) {
	p := newTestLambdaProvider(t)

	zipData := base64.StdEncoding.EncodeToString([]byte("fake zip"))
	resp := handleLambda(t, p, http.MethodPost, "/2015-03-31/functions", map[string]any{
		"FunctionName": "my-func",
		"Runtime":      "python3.12",
		"Handler":      "index.handler",
		"Role":         "arn:aws:iam::000000000000:role/lambda-role",
		"Code":         map[string]string{"ZipFile": zipData},
	})

	assert.Equal(t, http.StatusCreated, resp.StatusCode)

	var result map[string]any
	require.NoError(t, json.Unmarshal(resp.Body, &result))
	assert.Equal(t, "my-func", result["FunctionName"])
	assert.Contains(t, result["FunctionArn"], "my-func")
}

func TestLambdaProvider_ListFunctions(t *testing.T) {
	p := newTestLambdaProvider(t)

	zipData := base64.StdEncoding.EncodeToString([]byte("fake zip"))
	handleLambda(t, p, http.MethodPost, "/2015-03-31/functions", map[string]any{
		"FunctionName": "func-alpha",
		"Runtime":      "python3.12",
		"Handler":      "index.handler",
		"Role":         "arn:aws:iam::000000000000:role/role",
		"Code":         map[string]string{"ZipFile": zipData},
	})
	handleLambda(t, p, http.MethodPost, "/2015-03-31/functions", map[string]any{
		"FunctionName": "func-beta",
		"Runtime":      "nodejs20.x",
		"Handler":      "index.handler",
		"Role":         "arn:aws:iam::000000000000:role/role",
		"Code":         map[string]string{"ZipFile": zipData},
	})

	resp := handleLambda(t, p, http.MethodGet, "/2015-03-31/functions", nil)
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var result map[string]any
	require.NoError(t, json.Unmarshal(resp.Body, &result))
	functions, ok := result["Functions"].([]any)
	require.True(t, ok, "expected Functions array")
	require.Len(t, functions, 2)

	names := make([]string, 0, 2)
	for _, f := range functions {
		fm := f.(map[string]any)
		names = append(names, fm["FunctionName"].(string))
	}
	assert.Contains(t, names, "func-alpha")
	assert.Contains(t, names, "func-beta")
}

func TestLambdaProvider_GetFunction(t *testing.T) {
	p := newTestLambdaProvider(t)

	zipData := base64.StdEncoding.EncodeToString([]byte("fake zip"))
	handleLambda(t, p, http.MethodPost, "/2015-03-31/functions", map[string]any{
		"FunctionName": "my-func",
		"Runtime":      "python3.12",
		"Handler":      "index.handler",
		"Role":         "arn:aws:iam::000000000000:role/role",
		"Code":         map[string]string{"ZipFile": zipData},
	})

	resp := handleLambda(t, p, http.MethodGet, "/2015-03-31/functions/my-func", nil)
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var result map[string]any
	require.NoError(t, json.Unmarshal(resp.Body, &result))
	cfg, ok := result["Configuration"].(map[string]any)
	require.True(t, ok, "expected Configuration object")
	assert.Equal(t, "my-func", cfg["FunctionName"])
}

func TestLambdaProvider_DeleteFunction(t *testing.T) {
	p := newTestLambdaProvider(t)

	zipData := base64.StdEncoding.EncodeToString([]byte("fake zip"))
	handleLambda(t, p, http.MethodPost, "/2015-03-31/functions", map[string]any{
		"FunctionName": "my-func",
		"Runtime":      "python3.12",
		"Handler":      "index.handler",
		"Role":         "arn:aws:iam::000000000000:role/role",
		"Code":         map[string]string{"ZipFile": zipData},
	})

	delResp := handleLambda(t, p, http.MethodDelete, "/2015-03-31/functions/my-func", nil)
	assert.Equal(t, http.StatusNoContent, delResp.StatusCode)

	getResp := handleLambda(t, p, http.MethodGet, "/2015-03-31/functions/my-func", nil)
	assert.Equal(t, http.StatusNotFound, getResp.StatusCode)
}

func TestLambdaProvider_Invoke(t *testing.T) {
	p := newTestLambdaProvider(t)

	zipData := base64.StdEncoding.EncodeToString([]byte("fake zip"))
	handleLambda(t, p, http.MethodPost, "/2015-03-31/functions", map[string]any{
		"FunctionName": "my-func",
		"Runtime":      "python3.12",
		"Handler":      "index.handler",
		"Role":         "arn:aws:iam::000000000000:role/role",
		"Code":         map[string]string{"ZipFile": zipData},
	})

	resp := handleLambda(t, p, http.MethodPost, "/2015-03-31/functions/my-func/invocations",
		map[string]string{"key": "value"})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

// --- Task 24: Versions ---

func createTestFunction(t *testing.T, p *LambdaProvider, name string) {
	t.Helper()
	zipData := base64.StdEncoding.EncodeToString([]byte("fake zip"))
	handleLambda(t, p, http.MethodPost, "/2015-03-31/functions", map[string]any{
		"FunctionName": name,
		"Runtime":      "python3.12",
		"Handler":      "index.handler",
		"Role":         "arn:aws:iam::000000000000:role/role",
		"Code":         map[string]string{"ZipFile": zipData},
	})
}

func TestLambdaProvider_PublishVersion(t *testing.T) {
	p := newTestLambdaProvider(t)
	createTestFunction(t, p, "ver-func")

	resp := handleLambda(t, p, http.MethodPost, "/2015-03-31/functions/ver-func/versions", nil)
	assert.Equal(t, http.StatusCreated, resp.StatusCode)

	var result map[string]any
	require.NoError(t, json.Unmarshal(resp.Body, &result))
	assert.Equal(t, "1", result["Version"])

	listResp := handleLambda(t, p, http.MethodGet, "/2015-03-31/functions/ver-func/versions", nil)
	assert.Equal(t, http.StatusOK, listResp.StatusCode)

	var listResult map[string]any
	require.NoError(t, json.Unmarshal(listResp.Body, &listResult))
	versions := listResult["Versions"].([]any)
	assert.Len(t, versions, 1)
	assert.Equal(t, "1", versions[0].(map[string]any)["Version"])
}

// --- Task 24: Aliases ---

func TestLambdaProvider_Aliases(t *testing.T) {
	p := newTestLambdaProvider(t)
	createTestFunction(t, p, "alias-func")
	handleLambda(t, p, http.MethodPost, "/2015-03-31/functions/alias-func/versions", nil)

	// CreateAlias
	createResp := handleLambda(t, p, http.MethodPost, "/2015-03-31/functions/alias-func/aliases", map[string]any{
		"Name":            "prod",
		"FunctionVersion": "1",
	})
	assert.Equal(t, http.StatusCreated, createResp.StatusCode)
	var createResult map[string]any
	require.NoError(t, json.Unmarshal(createResp.Body, &createResult))
	assert.Equal(t, "prod", createResult["Name"])
	assert.Equal(t, "1", createResult["FunctionVersion"])

	// GetAlias
	getResp := handleLambda(t, p, http.MethodGet, "/2015-03-31/functions/alias-func/aliases/prod", nil)
	assert.Equal(t, http.StatusOK, getResp.StatusCode)

	// UpdateAlias
	updateResp := handleLambda(t, p, http.MethodPut, "/2015-03-31/functions/alias-func/aliases/prod", map[string]any{
		"FunctionVersion": "1",
	})
	assert.Equal(t, http.StatusOK, updateResp.StatusCode)

	// ListAliases
	listResp := handleLambda(t, p, http.MethodGet, "/2015-03-31/functions/alias-func/aliases", nil)
	assert.Equal(t, http.StatusOK, listResp.StatusCode)
	var listResult map[string]any
	require.NoError(t, json.Unmarshal(listResp.Body, &listResult))
	aliases := listResult["Aliases"].([]any)
	assert.Len(t, aliases, 1)

	// DeleteAlias
	delResp := handleLambda(t, p, http.MethodDelete, "/2015-03-31/functions/alias-func/aliases/prod", nil)
	assert.Equal(t, http.StatusNoContent, delResp.StatusCode)
}

// --- Task 26: Tags ---

func TestLambdaProvider_Tags(t *testing.T) {
	p := newTestLambdaProvider(t)

	arn := "arn:aws:lambda:us-east-1:000000000000:function:tag-func"

	// TagResource
	tagResp := handleLambda(t, p, http.MethodPost, "/2017-03-31/tags/"+arn, map[string]any{
		"Tags": map[string]string{"env": "test", "team": "dev"},
	})
	assert.Equal(t, http.StatusNoContent, tagResp.StatusCode)

	// ListTags
	listResp := handleLambda(t, p, http.MethodGet, "/2017-03-31/tags/"+arn, nil)
	assert.Equal(t, http.StatusOK, listResp.StatusCode)
	var listResult map[string]any
	require.NoError(t, json.Unmarshal(listResp.Body, &listResult))
	tags := listResult["Tags"].(map[string]any)
	assert.Equal(t, "test", tags["env"])
	assert.Equal(t, "dev", tags["team"])

	// UntagResource
	untagResp := handleLambda(t, p, http.MethodDelete, "/2017-03-31/tags/"+arn+"?tagKeys=env", nil)
	assert.Equal(t, http.StatusNoContent, untagResp.StatusCode)
}

// --- Task 25: Permissions ---

func TestLambdaProvider_Permissions(t *testing.T) {
	p := newTestLambdaProvider(t)
	createTestFunction(t, p, "perm-func")

	// AddPermission
	addResp := handleLambda(t, p, http.MethodPost, "/2015-03-31/functions/perm-func/policy", map[string]any{
		"StatementId": "stmt-1",
		"Action":      "lambda:InvokeFunction",
		"Principal":   "apigateway.amazonaws.com",
	})
	assert.Equal(t, http.StatusCreated, addResp.StatusCode)

	// GetPolicy
	getResp := handleLambda(t, p, http.MethodGet, "/2015-03-31/functions/perm-func/policy", nil)
	assert.Equal(t, http.StatusOK, getResp.StatusCode)
	var policyResult map[string]any
	require.NoError(t, json.Unmarshal(getResp.Body, &policyResult))
	assert.Contains(t, policyResult, "Policy")

	// RemovePermission
	delResp := handleLambda(t, p, http.MethodDelete, "/2015-03-31/functions/perm-func/policy/stmt-1", nil)
	assert.Equal(t, http.StatusNoContent, delResp.StatusCode)
}

// --- Task 27: Event Source Mappings ---

func TestLambdaProvider_EventSourceMappings(t *testing.T) {
	p := newTestLambdaProvider(t)
	createTestFunction(t, p, "esm-func")

	// CreateEventSourceMapping
	createResp := handleLambda(t, p, http.MethodPost, "/2015-03-31/event-source-mappings/", map[string]any{
		"FunctionName":   "esm-func",
		"EventSourceArn": "arn:aws:sqs:us-east-1:000000000000:my-queue",
		"BatchSize":      5,
	})
	assert.Equal(t, http.StatusCreated, createResp.StatusCode)
	var createResult map[string]any
	require.NoError(t, json.Unmarshal(createResp.Body, &createResult))
	uuidVal := createResult["UUID"].(string)
	assert.NotEmpty(t, uuidVal)
	assert.Equal(t, float64(5), createResult["BatchSize"])

	// GetEventSourceMapping
	getResp := handleLambda(t, p, http.MethodGet, "/2015-03-31/event-source-mappings/"+uuidVal, nil)
	assert.Equal(t, http.StatusOK, getResp.StatusCode)

	// UpdateEventSourceMapping
	updateResp := handleLambda(t, p, http.MethodPut, "/2015-03-31/event-source-mappings/"+uuidVal, map[string]any{
		"BatchSize": 10,
	})
	assert.Equal(t, http.StatusOK, updateResp.StatusCode)
	var updateResult map[string]any
	require.NoError(t, json.Unmarshal(updateResp.Body, &updateResult))
	assert.Equal(t, float64(10), updateResult["BatchSize"])

	// ListEventSourceMappings
	listResp := handleLambda(t, p, http.MethodGet, "/2015-03-31/event-source-mappings/?FunctionName=esm-func", nil)
	assert.Equal(t, http.StatusOK, listResp.StatusCode)
	var listResult map[string]any
	require.NoError(t, json.Unmarshal(listResp.Body, &listResult))
	mappings := listResult["EventSourceMappings"].([]any)
	assert.Len(t, mappings, 1)

	// DeleteEventSourceMapping
	delResp := handleLambda(t, p, http.MethodDelete, "/2015-03-31/event-source-mappings/"+uuidVal, nil)
	assert.Equal(t, http.StatusAccepted, delResp.StatusCode)
}
