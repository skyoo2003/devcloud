// SPDX-License-Identifier: Apache-2.0

// internal/services/ram/provider_test.go
package ram

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

func TestResourceShareCRUD(t *testing.T) {
	p := newTestProvider(t)

	// Create
	body := `{"name":"my-share","allowExternalPrincipals":false}`
	resp := callREST(t, p, "POST", "/resourceshares", "CreateResourceShare", body)
	assert.Equal(t, 200, resp.StatusCode)
	rb := parseBody(t, resp)
	rs, ok := rb["resourceShare"].(map[string]any)
	require.True(t, ok)
	arn, _ := rs["resourceShareArn"].(string)
	assert.NotEmpty(t, arn)
	assert.Equal(t, "my-share", rs["name"])
	assert.Equal(t, "ACTIVE", rs["status"])

	// GetResourceShares (list)
	resp2 := callREST(t, p, "POST", "/getresourceshares", "GetResourceShares", `{}`)
	assert.Equal(t, 200, resp2.StatusCode)
	rb2 := parseBody(t, resp2)
	list, ok := rb2["resourceShares"].([]any)
	require.True(t, ok)
	assert.Len(t, list, 1)

	// Update
	updateBody, _ := json.Marshal(map[string]any{"resourceShareArn": arn, "name": "updated-share"})
	resp3 := callREST(t, p, "PATCH", "/resourceshares/"+arn, "UpdateResourceShare", string(updateBody))
	assert.Equal(t, 200, resp3.StatusCode)
	rb3 := parseBody(t, resp3)
	updated := rb3["resourceShare"].(map[string]any)
	assert.Equal(t, "updated-share", updated["name"])

	// Delete
	resp4 := callREST(t, p, "DELETE", "/resourceshares/"+arn, "DeleteResourceShare", "")
	assert.Equal(t, 200, resp4.StatusCode)

	// List after delete
	resp5 := callREST(t, p, "POST", "/getresourceshares", "GetResourceShares", `{}`)
	rb5 := parseBody(t, resp5)
	list5, _ := rb5["resourceShares"].([]any)
	assert.Empty(t, list5)
}

func TestShareAssociation(t *testing.T) {
	p := newTestProvider(t)

	// Create share
	resp := callREST(t, p, "POST", "/resourceshares", "CreateResourceShare", `{"name":"assoc-share"}`)
	rb := parseBody(t, resp)
	rs := rb["resourceShare"].(map[string]any)
	shareARN, _ := rs["resourceShareArn"].(string)

	// Associate resource
	assocBody, _ := json.Marshal(map[string]any{
		"resourceShareArn": shareARN,
		"resourceArns":     []string{"arn:aws:s3:::my-bucket"},
		"principals":       []string{"123456789012"},
	})
	resp2 := callREST(t, p, "POST", "/resourceshares/associate", "AssociateResourceShare", string(assocBody))
	assert.Equal(t, 200, resp2.StatusCode)
	rb2 := parseBody(t, resp2)
	assocs, _ := rb2["resourceShareAssociations"].([]any)
	assert.Len(t, assocs, 2)

	// Get associations
	getBody, _ := json.Marshal(map[string]any{
		"associationType":   "RESOURCE",
		"resourceShareArns": shareARN,
	})
	resp3 := callREST(t, p, "POST", "/getresourceshareassociations", "GetResourceShareAssociations", string(getBody))
	assert.Equal(t, 200, resp3.StatusCode)
	rb3 := parseBody(t, resp3)
	assocList, _ := rb3["resourceShareAssociations"].([]any)
	assert.Len(t, assocList, 1)

	// Disassociate
	disBody, _ := json.Marshal(map[string]any{
		"resourceShareArn": shareARN,
		"resourceArns":     []string{"arn:aws:s3:::my-bucket"},
	})
	resp4 := callREST(t, p, "POST", "/resourceshares/disassociate", "DisassociateResourceShare", string(disBody))
	assert.Equal(t, 200, resp4.StatusCode)
}

func TestPermissionCRUD(t *testing.T) {
	p := newTestProvider(t)

	// Create
	body := `{"name":"my-permission","resourceType":"ec2:Instance"}`
	resp := callREST(t, p, "POST", "/permissions", "CreatePermission", body)
	assert.Equal(t, 200, resp.StatusCode)
	rb := parseBody(t, resp)
	perm, ok := rb["permission"].(map[string]any)
	require.True(t, ok)
	permARN, _ := perm["arn"].(string)
	assert.NotEmpty(t, permARN)
	assert.Equal(t, "my-permission", perm["name"])

	// Duplicate
	resp2 := callREST(t, p, "POST", "/permissions", "CreatePermission", body)
	assert.Equal(t, 409, resp2.StatusCode)

	// Get
	resp3 := callREST(t, p, "GET", "/permissions/"+permARN, "GetPermission", "")
	assert.Equal(t, 200, resp3.StatusCode)

	// List
	resp4 := callREST(t, p, "GET", "/permissions", "ListPermissions", `{}`)
	assert.Equal(t, 200, resp4.StatusCode)
	rb4 := parseBody(t, resp4)
	list, _ := rb4["permissions"].([]any)
	assert.Len(t, list, 1)

	// Delete
	resp5 := callREST(t, p, "DELETE", "/permissions/"+permARN, "DeletePermission", "")
	assert.Equal(t, 200, resp5.StatusCode)

	// Get after delete
	resp6 := callREST(t, p, "GET", "/permissions/"+permARN, "GetPermission", "")
	assert.Equal(t, 400, resp6.StatusCode)
}

func TestInvitations(t *testing.T) {
	p := newTestProvider(t)

	// Create share
	resp := callREST(t, p, "POST", "/resourceshares", "CreateResourceShare", `{"name":"inv-share"}`)
	rb := parseBody(t, resp)
	rs := rb["resourceShare"].(map[string]any)
	shareARN, _ := rs["resourceShareArn"].(string)

	// Create invitation manually via store
	invARN := "arn:aws:ram:us-east-1:000000000000:resource-share-invitation/test-inv"
	inv := &ShareInvitation{
		ARN:      invARN,
		ShareARN: shareARN,
		Sender:   "000000000000",
		Receiver: "111111111111",
		Status:   "PENDING",
	}
	require.NoError(t, p.store.CreateInvitation(inv))

	// List invitations
	resp2 := callREST(t, p, "GET", "/resourceshareinvitations", "GetResourceShareInvitations", `{}`)
	assert.Equal(t, 200, resp2.StatusCode)
	rb2 := parseBody(t, resp2)
	invList, _ := rb2["resourceShareInvitations"].([]any)
	assert.Len(t, invList, 1)

	// Accept
	acceptBody, _ := json.Marshal(map[string]any{"resourceShareInvitationArn": invARN})
	resp3 := callREST(t, p, "POST", "/acceptresourceshareinvitation", "AcceptResourceShareInvitation", string(acceptBody))
	assert.Equal(t, 200, resp3.StatusCode)
	rb3 := parseBody(t, resp3)
	accepted := rb3["resourceShareInvitation"].(map[string]any)
	assert.Equal(t, "ACCEPTED", accepted["status"])

	// Accept again should fail
	resp4 := callREST(t, p, "POST", "/acceptresourceshareinvitation", "AcceptResourceShareInvitation", string(acceptBody))
	assert.Equal(t, 400, resp4.StatusCode)

	// Create another invitation for reject test
	invARN2 := "arn:aws:ram:us-east-1:000000000000:resource-share-invitation/test-inv-2"
	inv2 := &ShareInvitation{
		ARN:      invARN2,
		ShareARN: shareARN,
		Sender:   "000000000000",
		Receiver: "222222222222",
		Status:   "PENDING",
	}
	require.NoError(t, p.store.CreateInvitation(inv2))

	rejectBody, _ := json.Marshal(map[string]any{"resourceShareInvitationArn": invARN2})
	resp5 := callREST(t, p, "POST", "/rejectresourceshareinvitation", "RejectResourceShareInvitation", string(rejectBody))
	assert.Equal(t, 200, resp5.StatusCode)
	rb5 := parseBody(t, resp5)
	rejected := rb5["resourceShareInvitation"].(map[string]any)
	assert.Equal(t, "REJECTED", rejected["status"])
}

func TestTags(t *testing.T) {
	p := newTestProvider(t)

	// Create share
	resp := callREST(t, p, "POST", "/resourceshares", "CreateResourceShare", `{"name":"tag-share"}`)
	rb := parseBody(t, resp)
	rs := rb["resourceShare"].(map[string]any)
	shareARN, _ := rs["resourceShareArn"].(string)

	// Tag
	tagBody, _ := json.Marshal(map[string]any{
		"resourceShareArn": shareARN,
		"tags": []map[string]string{
			{"key": "env", "value": "prod"},
			{"key": "team", "value": "platform"},
		},
	})
	resp2 := callREST(t, p, "POST", "/tags", "TagResource", string(tagBody))
	assert.Equal(t, 200, resp2.StatusCode)

	// Untag
	untagBody, _ := json.Marshal(map[string]any{
		"resourceShareArn": shareARN,
		"tagKeys":          []string{"env"},
	})
	resp3 := callREST(t, p, "DELETE", "/tags", "UntagResource", string(untagBody))
	assert.Equal(t, 200, resp3.StatusCode)

	// Verify via tags store
	tags, err := p.store.tags.ListTags(shareARN)
	require.NoError(t, err)
	assert.Len(t, tags, 1)
	assert.Equal(t, "platform", tags["team"])
}
