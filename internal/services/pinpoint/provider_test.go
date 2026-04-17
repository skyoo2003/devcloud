// SPDX-License-Identifier: Apache-2.0

// internal/services/pinpoint/provider_test.go
package pinpoint

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

func call(t *testing.T, p *Provider, method, path, op, body string) *plugin.Response {
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

// ── TestAppCRUD ───────────────────────────────────────────────────────────────

func TestAppCRUD(t *testing.T) {
	p := newTestProvider(t)

	// CreateApp — body is the ApplicationResponse payload directly
	resp := call(t, p, "POST", "/v1/apps", "CreateApp", `{"Name":"my-app"}`)
	assert.Equal(t, 201, resp.StatusCode)
	appResp := parseBody(t, resp)
	appID, _ := appResp["Id"].(string)
	assert.NotEmpty(t, appID)
	assert.Equal(t, "my-app", appResp["Name"])

	// GetApp
	resp2 := call(t, p, "GET", "/v1/apps/"+appID, "GetApp", "")
	assert.Equal(t, 200, resp2.StatusCode)
	appResp2 := parseBody(t, resp2)
	assert.Equal(t, appID, appResp2["Id"])

	// GetApps — body is the ApplicationsResponse payload directly
	resp3 := call(t, p, "GET", "/v1/apps", "GetApps", "")
	assert.Equal(t, 200, resp3.StatusCode)
	appsResp := parseBody(t, resp3)
	items, _ := appsResp["Item"].([]any)
	assert.Len(t, items, 1)

	// DeleteApp
	resp4 := call(t, p, "DELETE", "/v1/apps/"+appID, "DeleteApp", "")
	assert.Equal(t, 200, resp4.StatusCode)

	// GetApp after delete → 404
	resp5 := call(t, p, "GET", "/v1/apps/"+appID, "GetApp", "")
	assert.Equal(t, 404, resp5.StatusCode)

	// GetApps after delete → empty
	resp6 := call(t, p, "GET", "/v1/apps", "GetApps", "")
	appsResp6 := parseBody(t, resp6)
	items6, _ := appsResp6["Item"].([]any)
	assert.Len(t, items6, 0)
}

// ── TestCampaignCRUD ──────────────────────────────────────────────────────────

func TestCampaignCRUD(t *testing.T) {
	p := newTestProvider(t)

	// Create app first
	appResp := call(t, p, "POST", "/v1/apps", "CreateApp", `{"Name":"test-app"}`)
	require.Equal(t, 201, appResp.StatusCode)
	appBody := parseBody(t, appResp)
	appID := appBody["Id"].(string)

	// CreateCampaign
	resp := call(t, p, "POST", "/v1/apps/"+appID+"/campaigns", "CreateCampaign",
		`{"Name":"my-campaign","Description":"desc","SegmentId":"seg-1"}`)
	assert.Equal(t, 201, resp.StatusCode)
	cResp := parseBody(t, resp)
	campaignID := cResp["Id"].(string)
	assert.NotEmpty(t, campaignID)
	assert.Equal(t, "my-campaign", cResp["Name"])

	// GetCampaign
	resp2 := call(t, p, "GET", "/v1/apps/"+appID+"/campaigns/"+campaignID, "GetCampaign", "")
	assert.Equal(t, 200, resp2.StatusCode)

	// GetCampaigns
	resp3 := call(t, p, "GET", "/v1/apps/"+appID+"/campaigns", "GetCampaigns", "")
	assert.Equal(t, 200, resp3.StatusCode)
	rb3 := parseBody(t, resp3)
	cItems, _ := rb3["Item"].([]any)
	assert.Len(t, cItems, 1)

	// UpdateCampaign
	resp4 := call(t, p, "PUT", "/v1/apps/"+appID+"/campaigns/"+campaignID, "UpdateCampaign",
		`{"Name":"updated-campaign","Description":"new-desc","SegmentId":"seg-2"}`)
	assert.Equal(t, 200, resp4.StatusCode)
	cResp4 := parseBody(t, resp4)
	assert.Equal(t, "updated-campaign", cResp4["Name"])

	// GetCampaignActivities
	resp5 := call(t, p, "GET", "/v1/apps/"+appID+"/campaigns/"+campaignID+"/activities", "GetCampaignActivities", "")
	assert.Equal(t, 200, resp5.StatusCode)

	// GetCampaignVersions
	resp6 := call(t, p, "GET", "/v1/apps/"+appID+"/campaigns/"+campaignID+"/versions", "GetCampaignVersions", "")
	assert.Equal(t, 200, resp6.StatusCode)

	// DeleteCampaign
	resp7 := call(t, p, "DELETE", "/v1/apps/"+appID+"/campaigns/"+campaignID, "DeleteCampaign", "")
	assert.Equal(t, 200, resp7.StatusCode)

	// GetCampaign after delete → 404
	resp8 := call(t, p, "GET", "/v1/apps/"+appID+"/campaigns/"+campaignID, "GetCampaign", "")
	assert.Equal(t, 404, resp8.StatusCode)
}

// ── TestSegmentCRUD ───────────────────────────────────────────────────────────

func TestSegmentCRUD(t *testing.T) {
	p := newTestProvider(t)

	// Create app
	appResp := call(t, p, "POST", "/v1/apps", "CreateApp", `{"Name":"test-app"}`)
	require.Equal(t, 201, appResp.StatusCode)
	appID := parseBody(t, appResp)["Id"].(string)

	// CreateSegment
	resp := call(t, p, "POST", "/v1/apps/"+appID+"/segments", "CreateSegment",
		`{"Name":"my-segment","SegmentType":"DIMENSIONAL"}`)
	assert.Equal(t, 201, resp.StatusCode)
	sResp := parseBody(t, resp)
	segmentID := sResp["Id"].(string)
	assert.NotEmpty(t, segmentID)
	assert.Equal(t, "my-segment", sResp["Name"])

	// GetSegment
	resp2 := call(t, p, "GET", "/v1/apps/"+appID+"/segments/"+segmentID, "GetSegment", "")
	assert.Equal(t, 200, resp2.StatusCode)

	// GetSegments
	resp3 := call(t, p, "GET", "/v1/apps/"+appID+"/segments", "GetSegments", "")
	assert.Equal(t, 200, resp3.StatusCode)
	rb3 := parseBody(t, resp3)
	sItems, _ := rb3["Item"].([]any)
	assert.Len(t, sItems, 1)

	// UpdateSegment
	resp4 := call(t, p, "PUT", "/v1/apps/"+appID+"/segments/"+segmentID, "UpdateSegment",
		`{"Name":"updated-segment"}`)
	assert.Equal(t, 200, resp4.StatusCode)
	sResp4 := parseBody(t, resp4)
	assert.Equal(t, "updated-segment", sResp4["Name"])

	// GetSegmentVersions
	resp5 := call(t, p, "GET", "/v1/apps/"+appID+"/segments/"+segmentID+"/versions", "GetSegmentVersions", "")
	assert.Equal(t, 200, resp5.StatusCode)

	// DeleteSegment
	resp6 := call(t, p, "DELETE", "/v1/apps/"+appID+"/segments/"+segmentID, "DeleteSegment", "")
	assert.Equal(t, 200, resp6.StatusCode)

	// GetSegment after delete → 404
	resp7 := call(t, p, "GET", "/v1/apps/"+appID+"/segments/"+segmentID, "GetSegment", "")
	assert.Equal(t, 404, resp7.StatusCode)
}

// ── TestJourneyCRUD ───────────────────────────────────────────────────────────

func TestJourneyCRUD(t *testing.T) {
	p := newTestProvider(t)

	// Create app
	appResp := call(t, p, "POST", "/v1/apps", "CreateApp", `{"Name":"test-app"}`)
	require.Equal(t, 201, appResp.StatusCode)
	appID := parseBody(t, appResp)["Id"].(string)

	// CreateJourney
	resp := call(t, p, "POST", "/v1/apps/"+appID+"/journeys", "CreateJourney",
		`{"Name":"my-journey"}`)
	assert.Equal(t, 201, resp.StatusCode)
	jResp := parseBody(t, resp)
	journeyID := jResp["Id"].(string)
	assert.NotEmpty(t, journeyID)
	assert.Equal(t, "my-journey", jResp["Name"])
	assert.Equal(t, "DRAFT", jResp["State"])

	// GetJourney
	resp2 := call(t, p, "GET", "/v1/apps/"+appID+"/journeys/"+journeyID, "GetJourney", "")
	assert.Equal(t, 200, resp2.StatusCode)

	// ListJourneys
	resp3 := call(t, p, "GET", "/v1/apps/"+appID+"/journeys", "ListJourneys", "")
	assert.Equal(t, 200, resp3.StatusCode)
	rb3 := parseBody(t, resp3)
	jItems, _ := rb3["Item"].([]any)
	assert.Len(t, jItems, 1)

	// UpdateJourney
	resp4 := call(t, p, "PUT", "/v1/apps/"+appID+"/journeys/"+journeyID, "UpdateJourney",
		`{"Name":"updated-journey"}`)
	assert.Equal(t, 200, resp4.StatusCode)
	jResp4 := parseBody(t, resp4)
	assert.Equal(t, "updated-journey", jResp4["Name"])

	// UpdateJourneyState
	resp5 := call(t, p, "PUT", "/v1/apps/"+appID+"/journeys/"+journeyID+"/state", "UpdateJourneyState",
		`{"State":"ACTIVE"}`)
	assert.Equal(t, 200, resp5.StatusCode)
	jResp5 := parseBody(t, resp5)
	assert.Equal(t, "ACTIVE", jResp5["State"])

	// DeleteJourney
	resp6 := call(t, p, "DELETE", "/v1/apps/"+appID+"/journeys/"+journeyID, "DeleteJourney", "")
	assert.Equal(t, 200, resp6.StatusCode)

	// GetJourney after delete → 404
	resp7 := call(t, p, "GET", "/v1/apps/"+appID+"/journeys/"+journeyID, "GetJourney", "")
	assert.Equal(t, 404, resp7.StatusCode)
}

// ── TestTemplateCRUD ──────────────────────────────────────────────────────────

func TestTemplateCRUD(t *testing.T) {
	p := newTestProvider(t)

	// CreateEmailTemplate — body is the CreateTemplateMessageBody payload
	resp := call(t, p, "POST", "/v1/templates/my-email/email", "CreateEmailTemplate",
		`{"Subject":"Hello","HtmlPart":"<p>Hi</p>","TextPart":"Hi"}`)
	assert.Equal(t, 201, resp.StatusCode)
	rb := parseBody(t, resp)
	assert.NotNil(t, rb["Arn"])

	// GetEmailTemplate — body is the EmailTemplateResponse payload
	resp2 := call(t, p, "GET", "/v1/templates/my-email/email", "GetEmailTemplate", "")
	assert.Equal(t, 200, resp2.StatusCode)
	tResp2 := parseBody(t, resp2)
	assert.Equal(t, "my-email", tResp2["TemplateName"])
	assert.Equal(t, "Hello", tResp2["Subject"])

	// UpdateEmailTemplate — body is the MessageBody payload
	resp3 := call(t, p, "PUT", "/v1/templates/my-email/email", "UpdateEmailTemplate",
		`{"Subject":"Updated Subject","HtmlPart":"<p>Updated</p>","TextPart":"Updated"}`)
	assert.Equal(t, 200, resp3.StatusCode)
	rb3 := parseBody(t, resp3)
	assert.Equal(t, "Updated", rb3["Message"])

	// CreateSmsTemplate
	resp4 := call(t, p, "POST", "/v1/templates/my-sms/sms", "CreateSmsTemplate",
		`{"Body":"Hello SMS"}`)
	assert.Equal(t, 201, resp4.StatusCode)

	// ListTemplates — body is the TemplatesResponse payload
	resp5 := call(t, p, "GET", "/v1/templates", "ListTemplates", "")
	assert.Equal(t, 200, resp5.StatusCode)
	rb5 := parseBody(t, resp5)
	tItems, _ := rb5["Item"].([]any)
	assert.Len(t, tItems, 2) // email + sms

	// DeleteEmailTemplate
	resp6 := call(t, p, "DELETE", "/v1/templates/my-email/email", "DeleteEmailTemplate", "")
	assert.Equal(t, 200, resp6.StatusCode)

	// GetEmailTemplate after delete → 404
	resp7 := call(t, p, "GET", "/v1/templates/my-email/email", "GetEmailTemplate", "")
	assert.Equal(t, 404, resp7.StatusCode)
}

// ── TestSendMessages ─────────────────────────────────────────────────────────

func TestSendMessages(t *testing.T) {
	p := newTestProvider(t)

	// Create app
	appResp := call(t, p, "POST", "/v1/apps", "CreateApp", `{"Name":"msg-app"}`)
	require.Equal(t, 201, appResp.StatusCode)
	appID := parseBody(t, appResp)["Id"].(string)

	// SendMessages — body is the MessageResponse payload
	resp := call(t, p, "POST", "/v1/apps/"+appID+"/messages", "SendMessages",
		`{"MessageConfiguration":{},"Addresses":{}}`)
	assert.Equal(t, 200, resp.StatusCode)
	msgResp := parseBody(t, resp)
	assert.NotEmpty(t, msgResp["RequestId"])

	// SendUsersMessages — body is the SendUsersMessageResponse payload
	resp2 := call(t, p, "POST", "/v1/apps/"+appID+"/users-messages", "SendUsersMessages",
		`{"MessageConfiguration":{},"Users":{}}`)
	assert.Equal(t, 200, resp2.StatusCode)
	usersResp := parseBody(t, resp2)
	assert.NotEmpty(t, usersResp["RequestId"])

	// PutEvents
	resp3 := call(t, p, "POST", "/v1/apps/"+appID+"/events", "PutEvents",
		`{"Batch":{}}`)
	assert.Equal(t, 202, resp3.StatusCode)
}

// ── TestTags ─────────────────────────────────────────────────────────────────

func TestTags(t *testing.T) {
	p := newTestProvider(t)

	// Create app to get a real ARN
	appResp := call(t, p, "POST", "/v1/apps", "CreateApp", `{"Name":"tag-app"}`)
	require.Equal(t, 201, appResp.StatusCode)
	appBody := parseBody(t, appResp)
	arn := appBody["Arn"].(string)

	tagPath := "/v1/tags/" + arn

	// TagResource
	resp := call(t, p, "POST", tagPath, "TagResource", `{"tags":{"env":"dev","team":"backend"}}`)
	assert.Equal(t, 200, resp.StatusCode)

	// ListTagsForResource — body is the TagsModel payload
	resp2 := call(t, p, "GET", tagPath, "ListTagsForResource", "")
	assert.Equal(t, 200, resp2.StatusCode)
	rb2 := parseBody(t, resp2)
	tags, ok := rb2["tags"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "dev", tags["env"])
	assert.Equal(t, "backend", tags["team"])

	// UntagResource
	resp3 := call(t, p, "DELETE", tagPath+"?tagKeys=env", "UntagResource", "")
	// Rebuild request with proper query params
	req := httptest.NewRequest("DELETE", tagPath+"?tagKeys=env", strings.NewReader(""))
	resp3raw, _ := p.HandleRequest(context.Background(), "UntagResource", req)
	assert.Equal(t, 200, resp3raw.StatusCode)
	_ = resp3

	// ListTagsForResource after untag
	resp4 := call(t, p, "GET", tagPath, "ListTagsForResource", "")
	rb4 := parseBody(t, resp4)
	tags4 := rb4["tags"].(map[string]any)
	_, hasEnv := tags4["env"]
	assert.False(t, hasEnv, "env tag should be removed")
	assert.Equal(t, "backend", tags4["team"])
}
