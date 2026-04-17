// SPDX-License-Identifier: Apache-2.0

// internal/services/identitystore/extended_test.go
package identitystore

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetUserIdAndUpdateUser(t *testing.T) {
	p := newTestProvider(t)

	// Create a user
	resp := callJSON(t, p, svcTarget+".CreateUser", `{
		"IdentityStoreId": "d-test",
		"UserName": "carol",
		"DisplayName": "Carol"
	}`)
	require.Equal(t, 200, resp.StatusCode)
	m := parseJSON(t, resp)
	userID := m["UserId"].(string)

	// GetUserId via UserName
	resp2 := callJSON(t, p, svcTarget+".GetUserId", `{
		"IdentityStoreId": "d-test",
		"AlternateIdentifier": {
			"UniqueAttribute": {
				"AttributePath": "UserName",
				"AttributeValue": "carol"
			}
		}
	}`)
	assert.Equal(t, 200, resp2.StatusCode)
	m2 := parseJSON(t, resp2)
	assert.Equal(t, userID, m2["UserId"])

	// UpdateUser display name
	resp3 := callJSON(t, p, svcTarget+".UpdateUser", `{
		"IdentityStoreId": "d-test",
		"UserId": "`+userID+`",
		"Operations": [{"AttributePath":"DisplayName","AttributeValue":"Carol Updated"}]
	}`)
	assert.Equal(t, 200, resp3.StatusCode)

	// Describe to verify
	resp4 := callJSON(t, p, svcTarget+".DescribeUser", `{
		"IdentityStoreId": "d-test",
		"UserId": "`+userID+`"
	}`)
	m4 := parseJSON(t, resp4)
	assert.Equal(t, "Carol Updated", m4["DisplayName"])
}

func TestIsMemberInGroupsAndMembershipForMember(t *testing.T) {
	p := newTestProvider(t)

	uResp := callJSON(t, p, svcTarget+".CreateUser",
		`{"IdentityStoreId":"d-test","UserName":"dave"}`)
	userID := parseJSON(t, uResp)["UserId"].(string)

	gResp := callJSON(t, p, svcTarget+".CreateGroup",
		`{"IdentityStoreId":"d-test","DisplayName":"Ops"}`)
	groupID := parseJSON(t, gResp)["GroupId"].(string)

	// Create membership
	callJSON(t, p, svcTarget+".CreateGroupMembership", `{
		"IdentityStoreId": "d-test",
		"GroupId": "`+groupID+`",
		"MemberId": {"UserId": "`+userID+`"}
	}`)

	// IsMemberInGroups
	resp := callJSON(t, p, svcTarget+".IsMemberInGroups", `{
		"IdentityStoreId": "d-test",
		"MemberId": {"UserId": "`+userID+`"},
		"GroupIds": ["`+groupID+`", "no-such-group"]
	}`)
	assert.Equal(t, 200, resp.StatusCode)
	m := parseJSON(t, resp)
	results := m["Results"].([]any)
	require.Len(t, results, 2)
	first := results[0].(map[string]any)
	assert.True(t, first["MembershipExists"].(bool))
	second := results[1].(map[string]any)
	assert.False(t, second["MembershipExists"].(bool))

	// ListGroupMembershipsForMember
	resp2 := callJSON(t, p, svcTarget+".ListGroupMembershipsForMember", `{
		"IdentityStoreId": "d-test",
		"MemberId": {"UserId": "`+userID+`"}
	}`)
	assert.Equal(t, 200, resp2.StatusCode)
	m2 := parseJSON(t, resp2)
	memberships := m2["GroupMemberships"].([]any)
	assert.Len(t, memberships, 1)
}

func TestDirectoryHandlers(t *testing.T) {
	p := newTestProvider(t)

	lst := callJSON(t, p, svcTarget+".ListDirectories", `{}`)
	assert.Equal(t, 200, lst.StatusCode)
	m := parseJSON(t, lst)
	dirs := m["Directories"].([]any)
	assert.NotEmpty(t, dirs)

	desc := callJSON(t, p, svcTarget+".DescribeDirectory", `{"DirectoryId":"d-test"}`)
	assert.Equal(t, 200, desc.StatusCode)
}
