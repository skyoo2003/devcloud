// SPDX-License-Identifier: Apache-2.0

// internal/services/identitystore/provider_test.go
package identitystore

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

func newTestProvider(t *testing.T) *IdentityStoreProvider {
	t.Helper()
	p := &IdentityStoreProvider{}
	require.NoError(t, p.Init(plugin.PluginConfig{DataDir: t.TempDir()}))
	t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
	return p
}

func callJSON(t *testing.T, p *IdentityStoreProvider, target, body string) *plugin.Response {
	t.Helper()
	req := httptest.NewRequest("POST", "/", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req.Header.Set("X-Amz-Target", target)
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

const svcTarget = "AWSIdentityStore"

func TestUserCRUD(t *testing.T) {
	p := newTestProvider(t)

	// Create user
	resp := callJSON(t, p, svcTarget+".CreateUser", `{
		"IdentityStoreId": "d-test",
		"UserName": "alice",
		"DisplayName": "Alice Smith",
		"Emails": [{"Value": "alice@example.com", "Type": "work", "Primary": true}]
	}`)
	assert.Equal(t, 200, resp.StatusCode)
	m := parseJSON(t, resp)
	userID, _ := m["UserId"].(string)
	assert.NotEmpty(t, userID)
	assert.Equal(t, "d-test", m["IdentityStoreId"])

	// Describe user
	resp2 := callJSON(t, p, svcTarget+".DescribeUser", `{
		"IdentityStoreId": "d-test",
		"UserId": "`+userID+`"
	}`)
	assert.Equal(t, 200, resp2.StatusCode)
	m2 := parseJSON(t, resp2)
	assert.Equal(t, "alice", m2["UserName"])
	assert.Equal(t, "Alice Smith", m2["DisplayName"])

	// List users
	resp3 := callJSON(t, p, svcTarget+".ListUsers", `{"IdentityStoreId": "d-test"}`)
	assert.Equal(t, 200, resp3.StatusCode)
	m3 := parseJSON(t, resp3)
	users := m3["Users"].([]any)
	assert.Len(t, users, 1)

	// Delete user
	resp4 := callJSON(t, p, svcTarget+".DeleteUser", `{
		"IdentityStoreId": "d-test",
		"UserId": "`+userID+`"
	}`)
	assert.Equal(t, 200, resp4.StatusCode)

	// User should be gone
	resp5 := callJSON(t, p, svcTarget+".DescribeUser", `{
		"IdentityStoreId": "d-test",
		"UserId": "`+userID+`"
	}`)
	assert.Equal(t, 404, resp5.StatusCode)
}

func TestGroupCRUD(t *testing.T) {
	p := newTestProvider(t)

	// Create group
	resp := callJSON(t, p, svcTarget+".CreateGroup", `{
		"IdentityStoreId": "d-test",
		"DisplayName": "Admins",
		"Description": "Administrators"
	}`)
	assert.Equal(t, 200, resp.StatusCode)
	m := parseJSON(t, resp)
	groupID, _ := m["GroupId"].(string)
	assert.NotEmpty(t, groupID)

	// Describe group
	resp2 := callJSON(t, p, svcTarget+".DescribeGroup", `{
		"IdentityStoreId": "d-test",
		"GroupId": "`+groupID+`"
	}`)
	assert.Equal(t, 200, resp2.StatusCode)
	m2 := parseJSON(t, resp2)
	assert.Equal(t, "Admins", m2["DisplayName"])
	assert.Equal(t, "Administrators", m2["Description"])

	// List groups
	resp3 := callJSON(t, p, svcTarget+".ListGroups", `{"IdentityStoreId": "d-test"}`)
	assert.Equal(t, 200, resp3.StatusCode)
	m3 := parseJSON(t, resp3)
	groups := m3["Groups"].([]any)
	assert.Len(t, groups, 1)

	// Delete group
	resp4 := callJSON(t, p, svcTarget+".DeleteGroup", `{
		"IdentityStoreId": "d-test",
		"GroupId": "`+groupID+`"
	}`)
	assert.Equal(t, 200, resp4.StatusCode)

	// Group should be gone
	resp5 := callJSON(t, p, svcTarget+".DescribeGroup", `{
		"IdentityStoreId": "d-test",
		"GroupId": "`+groupID+`"
	}`)
	assert.Equal(t, 404, resp5.StatusCode)
}

func TestGroupMembershipCRUD(t *testing.T) {
	p := newTestProvider(t)

	// Create user
	userResp := callJSON(t, p, svcTarget+".CreateUser", `{
		"IdentityStoreId": "d-test",
		"UserName": "bob"
	}`)
	userM := parseJSON(t, userResp)
	userID := userM["UserId"].(string)

	// Create group
	groupResp := callJSON(t, p, svcTarget+".CreateGroup", `{
		"IdentityStoreId": "d-test",
		"DisplayName": "DevTeam"
	}`)
	groupM := parseJSON(t, groupResp)
	groupID := groupM["GroupId"].(string)

	// Create membership
	resp := callJSON(t, p, svcTarget+".CreateGroupMembership", `{
		"IdentityStoreId": "d-test",
		"GroupId": "`+groupID+`",
		"MemberId": {"UserId": "`+userID+`"}
	}`)
	assert.Equal(t, 200, resp.StatusCode)
	m := parseJSON(t, resp)
	membershipID := m["MembershipId"].(string)
	assert.NotEmpty(t, membershipID)

	// List memberships
	resp2 := callJSON(t, p, svcTarget+".ListGroupMemberships", `{
		"IdentityStoreId": "d-test",
		"GroupId": "`+groupID+`"
	}`)
	assert.Equal(t, 200, resp2.StatusCode)
	m2 := parseJSON(t, resp2)
	memberships := m2["GroupMemberships"].([]any)
	assert.Len(t, memberships, 1)
	mem := memberships[0].(map[string]any)
	memberIDMap := mem["MemberId"].(map[string]any)
	assert.Equal(t, userID, memberIDMap["UserId"])

	// Delete membership
	resp3 := callJSON(t, p, svcTarget+".DeleteGroupMembership", `{
		"IdentityStoreId": "d-test",
		"MembershipId": "`+membershipID+`"
	}`)
	assert.Equal(t, 200, resp3.StatusCode)

	// Memberships should be empty
	resp4 := callJSON(t, p, svcTarget+".ListGroupMemberships", `{
		"IdentityStoreId": "d-test",
		"GroupId": "`+groupID+`"
	}`)
	m4 := parseJSON(t, resp4)
	memberships4 := m4["GroupMemberships"].([]any)
	assert.Empty(t, memberships4)
}

func TestDuplicateUser(t *testing.T) {
	p := newTestProvider(t)

	body := `{"IdentityStoreId": "d-test", "UserName": "dupuser"}`
	resp1 := callJSON(t, p, svcTarget+".CreateUser", body)
	assert.Equal(t, 200, resp1.StatusCode)

	resp2 := callJSON(t, p, svcTarget+".CreateUser", body)
	assert.Equal(t, 409, resp2.StatusCode)
}

func TestDeleteNonExistentGroup(t *testing.T) {
	p := newTestProvider(t)

	resp := callJSON(t, p, svcTarget+".DeleteGroup", `{
		"IdentityStoreId": "d-test",
		"GroupId": "no-such-group"
	}`)
	assert.Equal(t, 404, resp.StatusCode)
}
