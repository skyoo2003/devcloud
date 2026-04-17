// SPDX-License-Identifier: Apache-2.0

// internal/services/serverlessrepo/provider_test.go
package serverlessrepo

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

func newTestProvider(t *testing.T) *ServerlessRepoProvider {
	t.Helper()
	p := &ServerlessRepoProvider{}
	require.NoError(t, p.Init(plugin.PluginConfig{DataDir: t.TempDir()}))
	t.Cleanup(func() { p.Shutdown(context.Background()) })
	return p
}

func callREST(t *testing.T, p *ServerlessRepoProvider, method, path, body string) *plugin.Response {
	t.Helper()
	var bodyReader *strings.Reader
	if body != "" {
		bodyReader = strings.NewReader(body)
	} else {
		bodyReader = strings.NewReader("{}")
	}
	req := httptest.NewRequest(method, path, bodyReader)
	req.Header.Set("Content-Type", "application/json")
	resp, err := p.HandleRequest(context.Background(), "", req)
	require.NoError(t, err)
	return resp
}

func parseJSON(t *testing.T, resp *plugin.Response) map[string]any {
	t.Helper()
	var m map[string]any
	require.NoError(t, json.Unmarshal(resp.Body, &m))
	return m
}

func TestApplicationCRUD(t *testing.T) {
	p := newTestProvider(t)

	// Create application
	resp := callREST(t, p, "POST", "/applications", `{
		"Name": "my-app",
		"Description": "My test application",
		"Author": "Test Author",
		"SemanticVersion": "1.0.0"
	}`)
	assert.Equal(t, 201, resp.StatusCode)
	m := parseJSON(t, resp)
	appID, _ := m["ApplicationId"].(string)
	assert.NotEmpty(t, appID)
	assert.Equal(t, "my-app", m["Name"])

	// Get application
	resp2 := callREST(t, p, "GET", "/applications/"+appID, "")
	assert.Equal(t, 200, resp2.StatusCode)
	m2 := parseJSON(t, resp2)
	assert.Equal(t, "my-app", m2["Name"])
	assert.Equal(t, "My test application", m2["Description"])

	// Update application
	resp3 := callREST(t, p, "PATCH", "/applications/"+appID, `{
		"Description": "Updated description"
	}`)
	assert.Equal(t, 200, resp3.StatusCode)
	m3 := parseJSON(t, resp3)
	assert.Equal(t, "Updated description", m3["Description"])

	// List applications
	resp4 := callREST(t, p, "GET", "/applications", "")
	assert.Equal(t, 200, resp4.StatusCode)
	m4 := parseJSON(t, resp4)
	apps := m4["Applications"].([]any)
	assert.Len(t, apps, 1)

	// Delete application
	resp5 := callREST(t, p, "DELETE", "/applications/"+appID, "")
	assert.Equal(t, 204, resp5.StatusCode)

	// Get should fail
	resp6 := callREST(t, p, "GET", "/applications/"+appID, "")
	assert.Equal(t, 404, resp6.StatusCode)
}

func TestApplicationVersions(t *testing.T) {
	p := newTestProvider(t)

	// Create application
	resp := callREST(t, p, "POST", "/applications", `{"Name": "versioned-app", "Description": "test"}`)
	m := parseJSON(t, resp)
	appID := m["ApplicationId"].(string)

	// Create version
	resp2 := callREST(t, p, "PUT", "/applications/"+appID+"/versions/1.0.0", `{
		"TemplateUrl": "https://s3.amazonaws.com/my-bucket/template.yaml"
	}`)
	assert.Equal(t, 201, resp2.StatusCode)
	m2 := parseJSON(t, resp2)
	assert.Equal(t, "1.0.0", m2["SemanticVersion"])

	// Create second version
	resp3 := callREST(t, p, "PUT", "/applications/"+appID+"/versions/2.0.0", `{}`)
	assert.Equal(t, 201, resp3.StatusCode)

	// List versions
	resp4 := callREST(t, p, "GET", "/applications/"+appID+"/versions", "")
	assert.Equal(t, 200, resp4.StatusCode)
	m4 := parseJSON(t, resp4)
	versions := m4["Versions"].([]any)
	assert.Len(t, versions, 2)
}

func TestApplicationPolicy(t *testing.T) {
	p := newTestProvider(t)

	// Create application
	resp := callREST(t, p, "POST", "/applications", `{"Name": "policy-app", "Description": "test"}`)
	m := parseJSON(t, resp)
	appID := m["ApplicationId"].(string)

	// Get policy (empty)
	resp2 := callREST(t, p, "GET", "/applications/"+appID+"/policy", "")
	assert.Equal(t, 200, resp2.StatusCode)

	// Put policy
	resp3 := callREST(t, p, "PUT", "/applications/"+appID+"/policy", `{
		"Statements": [{"Actions": ["GetApplication"], "Principals": ["*"]}]
	}`)
	assert.Equal(t, 200, resp3.StatusCode)

	// Get policy
	resp4 := callREST(t, p, "GET", "/applications/"+appID+"/policy", "")
	assert.Equal(t, 200, resp4.StatusCode)
	m4 := parseJSON(t, resp4)
	assert.NotNil(t, m4["Statements"])
}

func TestListApplicationsEmpty(t *testing.T) {
	p := newTestProvider(t)

	resp := callREST(t, p, "GET", "/applications", "")
	assert.Equal(t, 200, resp.StatusCode)
	m := parseJSON(t, resp)
	apps := m["Applications"].([]any)
	assert.Empty(t, apps)
}

func TestGetNonExistentApplication(t *testing.T) {
	p := newTestProvider(t)

	resp := callREST(t, p, "GET", "/applications/no-such-app", "")
	assert.Equal(t, 404, resp.StatusCode)
}
