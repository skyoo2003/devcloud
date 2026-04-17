// SPDX-License-Identifier: Apache-2.0

// internal/services/serverlessrepo/extended_test.go
package serverlessrepo

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateCloudFormationChangeSet(t *testing.T) {
	p := newTestProvider(t)

	// Create app first
	resp := callREST(t, p, "POST", "/applications", `{
		"Name": "cf-app",
		"Author": "me",
		"SemanticVersion": "1.0.0"
	}`)
	require.Equal(t, 201, resp.StatusCode)
	appID := parseJSON(t, resp)["ApplicationId"].(string)

	// Create change set
	cs := callREST(t, p, "POST", "/applications/"+appID+"/changesets", `{
		"StackName": "my-stack",
		"SemanticVersion": "1.0.0"
	}`)
	assert.Equal(t, 201, cs.StatusCode)
	m := parseJSON(t, cs)
	assert.NotEmpty(t, m["ChangeSetId"])
}

func TestCreateAndGetCloudFormationTemplate(t *testing.T) {
	p := newTestProvider(t)

	resp := callREST(t, p, "POST", "/applications", `{
		"Name": "tmpl-app",
		"Author": "me",
		"SemanticVersion": "1.0.0"
	}`)
	require.Equal(t, 201, resp.StatusCode)
	appID := parseJSON(t, resp)["ApplicationId"].(string)

	tmpl := callREST(t, p, "POST", "/applications/"+appID+"/templates", `{
		"SemanticVersion": "1.0.0",
		"TemplateUrl": "https://example.com/template.yaml"
	}`)
	assert.Equal(t, 201, tmpl.StatusCode)
	tm := parseJSON(t, tmpl)
	templateID := tm["TemplateId"].(string)

	got := callREST(t, p, "GET", "/applications/"+appID+"/templates/"+templateID, "")
	assert.Equal(t, 200, got.StatusCode)
	gm := parseJSON(t, got)
	assert.Equal(t, templateID, gm["TemplateId"])
}

func TestUnshareAndListDependencies(t *testing.T) {
	p := newTestProvider(t)

	resp := callREST(t, p, "POST", "/applications", `{
		"Name": "share-app",
		"Author": "me",
		"SemanticVersion": "1.0.0"
	}`)
	appID := parseJSON(t, resp)["ApplicationId"].(string)

	un := callREST(t, p, "POST", "/applications/"+appID+"/unshare", `{
		"OrganizationId": "o-1234"
	}`)
	assert.Equal(t, 200, un.StatusCode)

	deps := callREST(t, p, "GET", "/applications/"+appID+"/dependencies", "")
	assert.Equal(t, 200, deps.StatusCode)
}

func TestGetApplicationVersionAndDeletePolicy(t *testing.T) {
	p := newTestProvider(t)

	resp := callREST(t, p, "POST", "/applications", `{
		"Name": "ver-app",
		"Author": "me",
		"SemanticVersion": "1.0.0"
	}`)
	appID := parseJSON(t, resp)["ApplicationId"].(string)

	// Get Version
	vresp := callREST(t, p, "GET", "/applications/"+appID+"/versions/1.0.0", "")
	assert.Equal(t, 200, vresp.StatusCode)
	vm := parseJSON(t, vresp)
	assert.Equal(t, "1.0.0", vm["SemanticVersion"])

	// Delete Policy
	dp := callREST(t, p, "DELETE", "/applications/"+appID+"/policy", "")
	assert.Equal(t, 200, dp.StatusCode)
}
