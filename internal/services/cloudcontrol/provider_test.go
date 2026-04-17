// SPDX-License-Identifier: Apache-2.0

// internal/services/cloudcontrol/provider_test.go
package cloudcontrol

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http/httptest"
	"testing"

	"github.com/skyoo2003/devcloud/internal/plugin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestProvider(t *testing.T) *CloudControlProvider {
	t.Helper()
	p := &CloudControlProvider{}
	err := p.Init(plugin.PluginConfig{DataDir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { p.Shutdown(context.Background()) })
	return p
}

func callOp(t *testing.T, p *CloudControlProvider, op string, body map[string]any) *plugin.Response {
	t.Helper()
	b, _ := json.Marshal(body)
	req := httptest.NewRequest("POST", "/", bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/x-amz-json-1.0")
	req.Header.Set("X-Amz-Target", "CloudApiService."+op)
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

func TestCreateAndGetResource(t *testing.T) {
	p := newTestProvider(t)

	desiredState := `{"Name":"my-bucket","Region":"us-east-1"}`
	resp := callOp(t, p, "CreateResource", map[string]any{
		"TypeName":     "AWS::S3::Bucket",
		"DesiredState": desiredState,
	})
	assert.Equal(t, 200, resp.StatusCode)
	rb := parseBody(t, resp)
	pe, ok := rb["ProgressEvent"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "CREATE", pe["Operation"])
	assert.Equal(t, "SUCCESS", pe["OperationStatus"])
	assert.NotEmpty(t, pe["RequestToken"])
	identifier := pe["Identifier"].(string)
	assert.NotEmpty(t, identifier)

	// GetResource
	resp2 := callOp(t, p, "GetResource", map[string]any{
		"TypeName":   "AWS::S3::Bucket",
		"Identifier": identifier,
	})
	assert.Equal(t, 200, resp2.StatusCode)
	rb2 := parseBody(t, resp2)
	desc, ok := rb2["ResourceDescription"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, identifier, desc["Identifier"])
}

func TestListResources(t *testing.T) {
	p := newTestProvider(t)

	for i := 0; i < 3; i++ {
		callOp(t, p, "CreateResource", map[string]any{
			"TypeName":     "AWS::Lambda::Function",
			"DesiredState": `{}`,
		})
	}

	resp := callOp(t, p, "ListResources", map[string]any{
		"TypeName": "AWS::Lambda::Function",
	})
	assert.Equal(t, 200, resp.StatusCode)
	rb := parseBody(t, resp)
	items, ok := rb["ResourceDescriptions"].([]any)
	require.True(t, ok)
	assert.Len(t, items, 3)
}

func TestUpdateResource(t *testing.T) {
	p := newTestProvider(t)

	createResp := callOp(t, p, "CreateResource", map[string]any{
		"TypeName":     "AWS::DynamoDB::Table",
		"DesiredState": `{"TableName":"my-table"}`,
	})
	pe := parseBody(t, createResp)["ProgressEvent"].(map[string]any)
	identifier := pe["Identifier"].(string)

	resp := callOp(t, p, "UpdateResource", map[string]any{
		"TypeName":      "AWS::DynamoDB::Table",
		"Identifier":    identifier,
		"PatchDocument": `[{"op":"replace","path":"/BillingMode","value":"PAY_PER_REQUEST"}]`,
	})
	assert.Equal(t, 200, resp.StatusCode)
	rb := parseBody(t, resp)
	pe2 := rb["ProgressEvent"].(map[string]any)
	assert.Equal(t, "UPDATE", pe2["Operation"])
	assert.Equal(t, "SUCCESS", pe2["OperationStatus"])
}

func TestDeleteResource(t *testing.T) {
	p := newTestProvider(t)

	createResp := callOp(t, p, "CreateResource", map[string]any{
		"TypeName":     "AWS::SNS::Topic",
		"DesiredState": `{}`,
	})
	pe := parseBody(t, createResp)["ProgressEvent"].(map[string]any)
	identifier := pe["Identifier"].(string)

	resp := callOp(t, p, "DeleteResource", map[string]any{
		"TypeName":   "AWS::SNS::Topic",
		"Identifier": identifier,
	})
	assert.Equal(t, 200, resp.StatusCode)
	rb := parseBody(t, resp)
	pe2 := rb["ProgressEvent"].(map[string]any)
	assert.Equal(t, "DELETE", pe2["Operation"])
	assert.Equal(t, "SUCCESS", pe2["OperationStatus"])

	// Get should 404
	resp2 := callOp(t, p, "GetResource", map[string]any{
		"TypeName":   "AWS::SNS::Topic",
		"Identifier": identifier,
	})
	assert.Equal(t, 404, resp2.StatusCode)
}

func TestGetResourceRequestStatus(t *testing.T) {
	p := newTestProvider(t)

	createResp := callOp(t, p, "CreateResource", map[string]any{
		"TypeName":     "AWS::EC2::VPC",
		"DesiredState": `{}`,
	})
	pe := parseBody(t, createResp)["ProgressEvent"].(map[string]any)
	requestToken := pe["RequestToken"].(string)

	resp := callOp(t, p, "GetResourceRequestStatus", map[string]any{
		"RequestToken": requestToken,
	})
	assert.Equal(t, 200, resp.StatusCode)
	rb := parseBody(t, resp)
	pe2 := rb["ProgressEvent"].(map[string]any)
	assert.Equal(t, requestToken, pe2["RequestToken"])
	assert.Equal(t, "SUCCESS", pe2["OperationStatus"])
}

func TestListResourceRequests(t *testing.T) {
	p := newTestProvider(t)

	callOp(t, p, "CreateResource", map[string]any{"TypeName": "AWS::S3::Bucket", "DesiredState": `{}`})
	callOp(t, p, "CreateResource", map[string]any{"TypeName": "AWS::S3::Bucket", "DesiredState": `{}`})

	resp := callOp(t, p, "ListResourceRequests", map[string]any{})
	assert.Equal(t, 200, resp.StatusCode)
	rb := parseBody(t, resp)
	items, ok := rb["ResourceRequestStatusSummaries"].([]any)
	require.True(t, ok)
	assert.GreaterOrEqual(t, len(items), 2)
}
