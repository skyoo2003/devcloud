// SPDX-License-Identifier: Apache-2.0

// internal/services/resourcegroups/provider_test.go
package resourcegroups

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

func TestGroupCRUD(t *testing.T) {
	p := newTestProvider(t)

	// Create group
	body := `{"Name":"my-group","Description":"test group","ResourceQuery":{"Type":"TAG_FILTERS_1_0","Query":"{\"ResourceTypeFilters\":[\"AWS::AllSupported\"]}"}}`
	resp := callREST(t, p, "POST", "/groups", "CreateGroup", body)
	assert.Equal(t, 200, resp.StatusCode)
	rb := parseBody(t, resp)
	grp, ok := rb["Group"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "my-group", grp["Name"])
	arn, _ := grp["GroupArn"].(string)
	assert.NotEmpty(t, arn)

	// Duplicate create should conflict
	resp2 := callREST(t, p, "POST", "/groups", "CreateGroup", body)
	assert.Equal(t, 409, resp2.StatusCode)

	// Get group
	resp3 := callREST(t, p, "POST", "/get-group", "GetGroup", `{"GroupName":"my-group"}`)
	assert.Equal(t, 200, resp3.StatusCode)
	rb3 := parseBody(t, resp3)
	g3, ok := rb3["Group"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "my-group", g3["Name"])

	// Get non-existent
	resp4 := callREST(t, p, "POST", "/get-group", "GetGroup", `{"GroupName":"nonexistent"}`)
	assert.Equal(t, 404, resp4.StatusCode)

	// List groups
	callREST(t, p, "POST", "/groups", "CreateGroup", `{"Name":"group-b"}`)
	listResp := callREST(t, p, "POST", "/groups-list", "ListGroups", `{}`)
	assert.Equal(t, 200, listResp.StatusCode)
	listBody := parseBody(t, listResp)
	ids, ok := listBody["GroupIdentifiers"].([]any)
	require.True(t, ok)
	assert.GreaterOrEqual(t, len(ids), 2)

	// Update group
	updResp := callREST(t, p, "POST", "/update-group", "UpdateGroup", `{"GroupName":"my-group","Description":"updated"}`)
	assert.Equal(t, 200, updResp.StatusCode)
	updBody := parseBody(t, updResp)
	updGrp, _ := updBody["Group"].(map[string]any)
	assert.Equal(t, "updated", updGrp["Description"])

	// Update non-existent
	resp5 := callREST(t, p, "POST", "/update-group", "UpdateGroup", `{"GroupName":"nonexistent","Description":"x"}`)
	assert.Equal(t, 404, resp5.StatusCode)

	// GetGroupQuery
	gqResp := callREST(t, p, "POST", "/get-group-query", "GetGroupQuery", `{"GroupName":"my-group"}`)
	assert.Equal(t, 200, gqResp.StatusCode)
	gqBody := parseBody(t, gqResp)
	gq, ok := gqBody["GroupQuery"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "my-group", gq["GroupName"])

	// UpdateGroupQuery
	uqResp := callREST(t, p, "POST", "/update-group-query", "UpdateGroupQuery",
		`{"GroupName":"my-group","ResourceQuery":{"Type":"CLOUDFORMATION_STACK_1_0","Query":"{}"}}`)
	assert.Equal(t, 200, uqResp.StatusCode)

	// GetGroupConfiguration
	gcResp := callREST(t, p, "POST", "/get-group-configuration", "GetGroupConfiguration", `{"Group":"my-group"}`)
	assert.Equal(t, 200, gcResp.StatusCode)

	// PutGroupConfiguration
	pcResp := callREST(t, p, "POST", "/put-group-configuration", "PutGroupConfiguration",
		`{"Group":"my-group","Configuration":[{"Type":"AWS::ResourceGroups::Generic"}]}`)
	assert.Equal(t, 200, pcResp.StatusCode)

	// GroupResources
	grpRes := callREST(t, p, "POST", "/group-resources", "GroupResources",
		`{"Group":"my-group","ResourceArns":["arn:aws:s3:::bucket1"]}`)
	assert.Equal(t, 200, grpRes.StatusCode)
	grpResBody := parseBody(t, grpRes)
	succeeded, _ := grpResBody["Succeeded"].([]any)
	assert.Len(t, succeeded, 1)

	// UngroupResources
	ugrpRes := callREST(t, p, "POST", "/ungroup-resources", "UngroupResources",
		`{"Group":"my-group","ResourceArns":["arn:aws:s3:::bucket1"]}`)
	assert.Equal(t, 200, ugrpRes.StatusCode)

	// ListGroupResources
	lgrpRes := callREST(t, p, "POST", "/list-group-resources", "ListGroupResources", `{"GroupName":"my-group"}`)
	assert.Equal(t, 200, lgrpRes.StatusCode)

	// SearchResources
	srchResp := callREST(t, p, "POST", "/resources/search", "SearchResources",
		`{"ResourceQuery":{"Type":"TAG_FILTERS_1_0","Query":"{}"}}`)
	assert.Equal(t, 200, srchResp.StatusCode)

	// GetAccountSettings
	gasResp := callREST(t, p, "POST", "/get-account-settings", "GetAccountSettings", `{}`)
	assert.Equal(t, 200, gasResp.StatusCode)

	// UpdateAccountSettings
	uasResp := callREST(t, p, "POST", "/update-account-settings", "UpdateAccountSettings",
		`{"GroupLifecycleEventsDesiredStatus":"INACTIVE"}`)
	assert.Equal(t, 200, uasResp.StatusCode)
	uasBody := parseBody(t, uasResp)
	as, _ := uasBody["AccountSettings"].(map[string]any)
	assert.Equal(t, "INACTIVE", as["GroupLifecycleEventsDesiredStatus"])

	// Delete group
	delResp := callREST(t, p, "POST", "/delete-group", "DeleteGroup", `{"GroupName":"my-group"}`)
	assert.Equal(t, 200, delResp.StatusCode)

	// Get after delete should 404
	resp6 := callREST(t, p, "POST", "/get-group", "GetGroup", `{"GroupName":"my-group"}`)
	assert.Equal(t, 404, resp6.StatusCode)

	// Delete non-existent should 404
	resp7 := callREST(t, p, "POST", "/delete-group", "DeleteGroup", `{"GroupName":"nonexistent"}`)
	assert.Equal(t, 404, resp7.StatusCode)
}

func TestTags(t *testing.T) {
	p := newTestProvider(t)

	// Create group
	createResp := callREST(t, p, "POST", "/groups", "CreateGroup", `{"Name":"tagged-group"}`)
	assert.Equal(t, 200, createResp.StatusCode)
	rb := parseBody(t, createResp)
	grp, _ := rb["Group"].(map[string]any)
	arn, _ := grp["GroupArn"].(string)
	require.NotEmpty(t, arn)

	// Tag
	tagBody, _ := json.Marshal(map[string]any{
		"Tags": map[string]string{"Env": "prod", "Team": "platform"},
	})
	tagResp := callREST(t, p, "PUT", "/resources/"+arn+"/tags", "Tag", string(tagBody))
	assert.Equal(t, 200, tagResp.StatusCode)
	tagRb := parseBody(t, tagResp)
	tags, ok := tagRb["Tags"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "prod", tags["Env"])

	// GetTags
	getTagsResp := callREST(t, p, "GET", "/resources/"+arn+"/tags", "GetTags", "")
	assert.Equal(t, 200, getTagsResp.StatusCode)
	getTagsRb := parseBody(t, getTagsResp)
	gtTags, ok := getTagsRb["Tags"].(map[string]any)
	require.True(t, ok)
	assert.Len(t, gtTags, 2)
	assert.Equal(t, "prod", gtTags["Env"])
	assert.Equal(t, "platform", gtTags["Team"])

	// Untag
	untagBody := `{"Keys":["Env"]}`
	untagResp := callREST(t, p, "PATCH", "/resources/"+arn+"/tags", "Untag", untagBody)
	assert.Equal(t, 200, untagResp.StatusCode)

	// Verify 1 tag remains
	resp2 := callREST(t, p, "GET", "/resources/"+arn+"/tags", "GetTags", "")
	rb2 := parseBody(t, resp2)
	tags2, _ := rb2["Tags"].(map[string]any)
	assert.Len(t, tags2, 1)
	assert.Equal(t, "platform", tags2["Team"])
}
