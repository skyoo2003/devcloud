// SPDX-License-Identifier: Apache-2.0

package iam

import (
	"context"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/skyoo2003/devcloud/internal/plugin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestIAMProvider(t *testing.T) *IAMProvider {
	t.Helper()
	dir := t.TempDir()
	p := &IAMProvider{}
	err := p.Init(plugin.PluginConfig{
		DataDir: dir,
		Options: map[string]any{
			"db_path": filepath.Join(dir, "iam.db"),
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { p.Shutdown(context.Background()) })
	return p
}

func iamRequest(t *testing.T, body string) *httptest.ResponseRecorder {
	t.Helper()
	return httptest.NewRecorder()
}

func TestIAMProvider_CreateUser(t *testing.T) {
	p := newTestIAMProvider(t)

	req := httptest.NewRequest("POST", "/", strings.NewReader("Action=CreateUser&UserName=testuser"))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := p.HandleRequest(context.Background(), "", req)
	require.NoError(t, err)
	assert.Equal(t, 200, resp.StatusCode)
	assert.Contains(t, string(resp.Body), "testuser")
	assert.Contains(t, string(resp.Body), "CreateUserResponse")
	assert.Contains(t, string(resp.Body), "UserId")
	assert.Contains(t, string(resp.Body), "arn:aws:iam::000000000000:user/testuser")

	_ = iamRequest(t, "")
}

func TestIAMProvider_ListUsers(t *testing.T) {
	p := newTestIAMProvider(t)

	// Create two users
	req1 := httptest.NewRequest("POST", "/", strings.NewReader("Action=CreateUser&UserName=alice"))
	p.HandleRequest(context.Background(), "", req1)

	req2 := httptest.NewRequest("POST", "/", strings.NewReader("Action=CreateUser&UserName=bob"))
	p.HandleRequest(context.Background(), "", req2)

	// List users
	req := httptest.NewRequest("POST", "/", strings.NewReader("Action=ListUsers"))
	resp, err := p.HandleRequest(context.Background(), "", req)
	require.NoError(t, err)
	assert.Equal(t, 200, resp.StatusCode)
	body := string(resp.Body)
	assert.Contains(t, body, "alice")
	assert.Contains(t, body, "bob")
	assert.Contains(t, body, "ListUsersResponse")
}

func TestIAMProvider_CreateRole(t *testing.T) {
	p := newTestIAMProvider(t)

	req := httptest.NewRequest("POST", "/", strings.NewReader(
		"Action=CreateRole&RoleName=myrole&AssumeRolePolicyDocument=%7B%7D",
	))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := p.HandleRequest(context.Background(), "", req)
	require.NoError(t, err)
	assert.Equal(t, 200, resp.StatusCode)
	body := string(resp.Body)
	assert.Contains(t, body, "myrole")
	assert.Contains(t, body, "CreateRoleResponse")
	assert.Contains(t, body, "arn:aws:iam::000000000000:role/myrole")
}

func TestIAMProvider_CreateAccessKey(t *testing.T) {
	p := newTestIAMProvider(t)

	// Create user first
	req1 := httptest.NewRequest("POST", "/", strings.NewReader("Action=CreateUser&UserName=keyuser"))
	resp1, err := p.HandleRequest(context.Background(), "", req1)
	require.NoError(t, err)
	require.Equal(t, 200, resp1.StatusCode)

	// Create access key for the user
	req2 := httptest.NewRequest("POST", "/", strings.NewReader("Action=CreateAccessKey&UserName=keyuser"))
	resp2, err := p.HandleRequest(context.Background(), "", req2)
	require.NoError(t, err)
	assert.Equal(t, 200, resp2.StatusCode)
	body := string(resp2.Body)
	assert.Contains(t, body, "CreateAccessKeyResponse")
	assert.Contains(t, body, "AKIA")
	assert.Contains(t, body, "keyuser")
	assert.Contains(t, body, "Active")
	assert.Contains(t, body, "SecretAccessKey")
}

func TestIAMProvider_AttachRolePolicy(t *testing.T) {
	p := newTestIAMProvider(t)

	// Create role first
	req1 := httptest.NewRequest("POST", "/", strings.NewReader("Action=CreateRole&RoleName=myrole&AssumeRolePolicyDocument=%7B%7D"))
	p.HandleRequest(context.Background(), "", req1)

	// Attach policy
	req2 := httptest.NewRequest("POST", "/", strings.NewReader(
		"Action=AttachRolePolicy&RoleName=myrole&PolicyArn=arn%3Aaws%3Aiam%3A%3Aaws%3Apolicy%2FReadOnlyAccess",
	))
	resp, err := p.HandleRequest(context.Background(), "", req2)
	require.NoError(t, err)
	assert.Equal(t, 200, resp.StatusCode)
	assert.Contains(t, string(resp.Body), "AttachRolePolicyResponse")
}

func TestIAMProvider_ListRoles(t *testing.T) {
	p := newTestIAMProvider(t)

	req1 := httptest.NewRequest("POST", "/", strings.NewReader("Action=CreateRole&RoleName=role-a&AssumeRolePolicyDocument=%7B%7D"))
	p.HandleRequest(context.Background(), "", req1)
	req2 := httptest.NewRequest("POST", "/", strings.NewReader("Action=CreateRole&RoleName=role-b&AssumeRolePolicyDocument=%7B%7D"))
	p.HandleRequest(context.Background(), "", req2)

	req := httptest.NewRequest("POST", "/", strings.NewReader("Action=ListRoles"))
	resp, err := p.HandleRequest(context.Background(), "", req)
	require.NoError(t, err)
	assert.Equal(t, 200, resp.StatusCode)
	body := string(resp.Body)
	assert.Contains(t, body, "role-a")
	assert.Contains(t, body, "role-b")
	assert.Contains(t, body, "ListRolesResponse")
}

func TestIAMProvider_UnknownAction(t *testing.T) {
	p := newTestIAMProvider(t)

	req := httptest.NewRequest("POST", "/", strings.NewReader("Action=DeleteEverything"))
	resp, err := p.HandleRequest(context.Background(), "", req)
	require.NoError(t, err)
	assert.Equal(t, 400, resp.StatusCode)
}

func TestIAMProvider_GetUser(t *testing.T) {
	p := newTestIAMProvider(t)
	req1 := httptest.NewRequest("POST", "/", strings.NewReader("Action=CreateUser&UserName=alice"))
	p.HandleRequest(context.Background(), "", req1)

	req := httptest.NewRequest("POST", "/", strings.NewReader("Action=GetUser&UserName=alice"))
	resp, err := p.HandleRequest(context.Background(), "", req)
	require.NoError(t, err)
	assert.Equal(t, 200, resp.StatusCode)
	assert.Contains(t, string(resp.Body), "alice")
	assert.Contains(t, string(resp.Body), "GetUserResponse")
}

func TestIAMProvider_GetUser_NotFound(t *testing.T) {
	p := newTestIAMProvider(t)
	req := httptest.NewRequest("POST", "/", strings.NewReader("Action=GetUser&UserName=nobody"))
	resp, err := p.HandleRequest(context.Background(), "", req)
	require.NoError(t, err)
	assert.Equal(t, 404, resp.StatusCode)
	assert.Contains(t, string(resp.Body), "NoSuchEntity")
}

func TestIAMProvider_GetRole(t *testing.T) {
	p := newTestIAMProvider(t)
	req1 := httptest.NewRequest("POST", "/", strings.NewReader("Action=CreateRole&RoleName=myrole&AssumeRolePolicyDocument=%7B%7D"))
	p.HandleRequest(context.Background(), "", req1)

	req := httptest.NewRequest("POST", "/", strings.NewReader("Action=GetRole&RoleName=myrole"))
	resp, err := p.HandleRequest(context.Background(), "", req)
	require.NoError(t, err)
	assert.Equal(t, 200, resp.StatusCode)
	assert.Contains(t, string(resp.Body), "myrole")
	assert.Contains(t, string(resp.Body), "GetRoleResponse")
}

func TestIAMProvider_DeleteRole(t *testing.T) {
	p := newTestIAMProvider(t)
	req1 := httptest.NewRequest("POST", "/", strings.NewReader("Action=CreateRole&RoleName=delrole&AssumeRolePolicyDocument=%7B%7D"))
	p.HandleRequest(context.Background(), "", req1)

	req := httptest.NewRequest("POST", "/", strings.NewReader("Action=DeleteRole&RoleName=delrole"))
	resp, err := p.HandleRequest(context.Background(), "", req)
	require.NoError(t, err)
	assert.Equal(t, 200, resp.StatusCode)

	req2 := httptest.NewRequest("POST", "/", strings.NewReader("Action=GetRole&RoleName=delrole"))
	resp2, _ := p.HandleRequest(context.Background(), "", req2)
	assert.Equal(t, 404, resp2.StatusCode)
}

func TestIAMProvider_GetPolicy(t *testing.T) {
	p := newTestIAMProvider(t)

	// Create policy first
	req1 := httptest.NewRequest("POST", "/", strings.NewReader(
		"Action=CreatePolicy&PolicyName=mypolicy&PolicyDocument=%7B%7D",
	))
	resp1, err := p.HandleRequest(context.Background(), "", req1)
	require.NoError(t, err)
	require.Equal(t, 200, resp1.StatusCode)

	// Get policy by ARN
	req2 := httptest.NewRequest("POST", "/", strings.NewReader(
		"Action=GetPolicy&PolicyArn=arn%3Aaws%3Aiam%3A%3A000000000000%3Apolicy%2Fmypolicy",
	))
	resp2, err := p.HandleRequest(context.Background(), "", req2)
	require.NoError(t, err)
	assert.Equal(t, 200, resp2.StatusCode)
	assert.Contains(t, string(resp2.Body), "mypolicy")
	assert.Contains(t, string(resp2.Body), "GetPolicyResponse")
}

func TestIAMProvider_GetPolicy_NotFound(t *testing.T) {
	p := newTestIAMProvider(t)

	req := httptest.NewRequest("POST", "/", strings.NewReader(
		"Action=GetPolicy&PolicyArn=arn%3Aaws%3Aiam%3A%3A000000000000%3Apolicy%2Fnonexistent",
	))
	resp, err := p.HandleRequest(context.Background(), "", req)
	require.NoError(t, err)
	assert.Equal(t, 404, resp.StatusCode)
	assert.Contains(t, string(resp.Body), "NoSuchEntity")
}

func TestIAMProvider_DeletePolicy(t *testing.T) {
	p := newTestIAMProvider(t)

	// Create policy
	req1 := httptest.NewRequest("POST", "/", strings.NewReader(
		"Action=CreatePolicy&PolicyName=delpol&PolicyDocument=%7B%7D",
	))
	p.HandleRequest(context.Background(), "", req1)

	// Delete policy
	req2 := httptest.NewRequest("POST", "/", strings.NewReader(
		"Action=DeletePolicy&PolicyArn=arn%3Aaws%3Aiam%3A%3A000000000000%3Apolicy%2Fdelpol",
	))
	resp, err := p.HandleRequest(context.Background(), "", req2)
	require.NoError(t, err)
	assert.Equal(t, 200, resp.StatusCode)
	assert.Contains(t, string(resp.Body), "DeletePolicyResponse")

	// Verify it's gone
	req3 := httptest.NewRequest("POST", "/", strings.NewReader(
		"Action=GetPolicy&PolicyArn=arn%3Aaws%3Aiam%3A%3A000000000000%3Apolicy%2Fdelpol",
	))
	resp3, _ := p.HandleRequest(context.Background(), "", req3)
	assert.Equal(t, 404, resp3.StatusCode)
}

func TestIAMProvider_DeletePolicy_NotFound(t *testing.T) {
	p := newTestIAMProvider(t)

	req := httptest.NewRequest("POST", "/", strings.NewReader(
		"Action=DeletePolicy&PolicyArn=arn%3Aaws%3Aiam%3A%3A000000000000%3Apolicy%2Fghost",
	))
	resp, err := p.HandleRequest(context.Background(), "", req)
	require.NoError(t, err)
	assert.Equal(t, 404, resp.StatusCode)
	assert.Contains(t, string(resp.Body), "NoSuchEntity")
}

func TestIAMProvider_DetachRolePolicy(t *testing.T) {
	p := newTestIAMProvider(t)

	// Create role
	req1 := httptest.NewRequest("POST", "/", strings.NewReader("Action=CreateRole&RoleName=detrole&AssumeRolePolicyDocument=%7B%7D"))
	p.HandleRequest(context.Background(), "", req1)

	// Attach policy
	req2 := httptest.NewRequest("POST", "/", strings.NewReader(
		"Action=AttachRolePolicy&RoleName=detrole&PolicyArn=arn%3Aaws%3Aiam%3A%3Aaws%3Apolicy%2FReadOnlyAccess",
	))
	p.HandleRequest(context.Background(), "", req2)

	// Detach policy
	req3 := httptest.NewRequest("POST", "/", strings.NewReader(
		"Action=DetachRolePolicy&RoleName=detrole&PolicyArn=arn%3Aaws%3Aiam%3A%3Aaws%3Apolicy%2FReadOnlyAccess",
	))
	resp, err := p.HandleRequest(context.Background(), "", req3)
	require.NoError(t, err)
	assert.Equal(t, 200, resp.StatusCode)
	assert.Contains(t, string(resp.Body), "DetachRolePolicyResponse")

	// Verify it's detached
	req4 := httptest.NewRequest("POST", "/", strings.NewReader("Action=ListAttachedRolePolicies&RoleName=detrole"))
	resp4, _ := p.HandleRequest(context.Background(), "", req4)
	assert.NotContains(t, string(resp4.Body), "ReadOnlyAccess")
}

func TestIAMProvider_AttachUserPolicy_And_List(t *testing.T) {
	p := newTestIAMProvider(t)

	// Create user
	req1 := httptest.NewRequest("POST", "/", strings.NewReader("Action=CreateUser&UserName=poluser"))
	p.HandleRequest(context.Background(), "", req1)

	// Create policy
	req2 := httptest.NewRequest("POST", "/", strings.NewReader(
		"Action=CreatePolicy&PolicyName=userpol&PolicyDocument=%7B%7D",
	))
	p.HandleRequest(context.Background(), "", req2)

	arn := "arn%3Aaws%3Aiam%3A%3A000000000000%3Apolicy%2Fuserpol"

	// Attach user policy
	req3 := httptest.NewRequest("POST", "/", strings.NewReader(
		"Action=AttachUserPolicy&UserName=poluser&PolicyArn="+arn,
	))
	resp3, err := p.HandleRequest(context.Background(), "", req3)
	require.NoError(t, err)
	assert.Equal(t, 200, resp3.StatusCode)
	assert.Contains(t, string(resp3.Body), "AttachUserPolicyResponse")

	// List attached user policies
	req4 := httptest.NewRequest("POST", "/", strings.NewReader("Action=ListAttachedUserPolicies&UserName=poluser"))
	resp4, err := p.HandleRequest(context.Background(), "", req4)
	require.NoError(t, err)
	assert.Equal(t, 200, resp4.StatusCode)
	assert.Contains(t, string(resp4.Body), "userpol")
	assert.Contains(t, string(resp4.Body), "ListAttachedUserPoliciesResponse")
}

func TestIAMProvider_DetachUserPolicy(t *testing.T) {
	p := newTestIAMProvider(t)

	// Create user and policy
	req1 := httptest.NewRequest("POST", "/", strings.NewReader("Action=CreateUser&UserName=detuser"))
	p.HandleRequest(context.Background(), "", req1)
	req2 := httptest.NewRequest("POST", "/", strings.NewReader(
		"Action=CreatePolicy&PolicyName=detuserpol&PolicyDocument=%7B%7D",
	))
	p.HandleRequest(context.Background(), "", req2)

	arn := "arn%3Aaws%3Aiam%3A%3A000000000000%3Apolicy%2Fdetuserpol"

	// Attach
	req3 := httptest.NewRequest("POST", "/", strings.NewReader(
		"Action=AttachUserPolicy&UserName=detuser&PolicyArn="+arn,
	))
	p.HandleRequest(context.Background(), "", req3)

	// Detach
	req4 := httptest.NewRequest("POST", "/", strings.NewReader(
		"Action=DetachUserPolicy&UserName=detuser&PolicyArn="+arn,
	))
	resp4, err := p.HandleRequest(context.Background(), "", req4)
	require.NoError(t, err)
	assert.Equal(t, 200, resp4.StatusCode)

	// Verify detached
	req5 := httptest.NewRequest("POST", "/", strings.NewReader("Action=ListAttachedUserPolicies&UserName=detuser"))
	resp5, _ := p.HandleRequest(context.Background(), "", req5)
	assert.NotContains(t, string(resp5.Body), "detuserpol")
}

func TestIAMProvider_CreateAndGetPolicyVersion(t *testing.T) {
	p := newTestIAMProvider(t)

	policyArn := "arn%3Aaws%3Aiam%3A%3A000000000000%3Apolicy%2Fverpol"

	// Create a policy version
	req1 := httptest.NewRequest("POST", "/", strings.NewReader(
		"Action=CreatePolicyVersion&PolicyArn="+policyArn+"&PolicyDocument=%7B%22Version%22%3A%222012-10-17%22%7D&SetAsDefault=true",
	))
	resp1, err := p.HandleRequest(context.Background(), "", req1)
	require.NoError(t, err)
	assert.Equal(t, 200, resp1.StatusCode)
	assert.Contains(t, string(resp1.Body), "CreatePolicyVersionResponse")
	assert.Contains(t, string(resp1.Body), "v1")

	// Get the policy version
	req2 := httptest.NewRequest("POST", "/", strings.NewReader(
		"Action=GetPolicyVersion&PolicyArn="+policyArn+"&VersionId=v1",
	))
	resp2, err := p.HandleRequest(context.Background(), "", req2)
	require.NoError(t, err)
	assert.Equal(t, 200, resp2.StatusCode)
	assert.Contains(t, string(resp2.Body), "GetPolicyVersionResponse")
	assert.Contains(t, string(resp2.Body), "v1")

	// List policy versions
	req3 := httptest.NewRequest("POST", "/", strings.NewReader(
		"Action=ListPolicyVersions&PolicyArn="+policyArn,
	))
	resp3, err := p.HandleRequest(context.Background(), "", req3)
	require.NoError(t, err)
	assert.Equal(t, 200, resp3.StatusCode)
	assert.Contains(t, string(resp3.Body), "ListPolicyVersionsResponse")
	assert.Contains(t, string(resp3.Body), "v1")
}

// Task 4: Inline policies

func TestIAMProvider_InlineUserPolicyRoundtrip(t *testing.T) {
	p := newTestIAMProvider(t)
	ctx := context.Background()

	req1 := httptest.NewRequest("POST", "/", strings.NewReader("Action=CreateUser&UserName=inlineuser"))
	p.HandleRequest(ctx, "", req1)

	req2 := httptest.NewRequest("POST", "/", strings.NewReader(
		"Action=PutUserPolicy&UserName=inlineuser&PolicyName=mypol&PolicyDocument=%7B%7D",
	))
	resp2, err := p.HandleRequest(ctx, "", req2)
	require.NoError(t, err)
	assert.Equal(t, 200, resp2.StatusCode)
	assert.Contains(t, string(resp2.Body), "PutUserPolicyResponse")

	req3 := httptest.NewRequest("POST", "/", strings.NewReader(
		"Action=GetUserPolicy&UserName=inlineuser&PolicyName=mypol",
	))
	resp3, err := p.HandleRequest(ctx, "", req3)
	require.NoError(t, err)
	assert.Equal(t, 200, resp3.StatusCode)
	body3 := string(resp3.Body)
	assert.Contains(t, body3, "GetUserPolicyResponse")
	assert.Contains(t, body3, "mypol")
	assert.Contains(t, body3, "inlineuser")

	req4 := httptest.NewRequest("POST", "/", strings.NewReader(
		"Action=ListUserPolicies&UserName=inlineuser",
	))
	resp4, err := p.HandleRequest(ctx, "", req4)
	require.NoError(t, err)
	assert.Equal(t, 200, resp4.StatusCode)
	assert.Contains(t, string(resp4.Body), "mypol")
	assert.Contains(t, string(resp4.Body), "ListUserPoliciesResponse")

	req5 := httptest.NewRequest("POST", "/", strings.NewReader(
		"Action=DeleteUserPolicy&UserName=inlineuser&PolicyName=mypol",
	))
	resp5, err := p.HandleRequest(ctx, "", req5)
	require.NoError(t, err)
	assert.Equal(t, 200, resp5.StatusCode)
	assert.Contains(t, string(resp5.Body), "DeleteUserPolicyResponse")

	req6 := httptest.NewRequest("POST", "/", strings.NewReader(
		"Action=GetUserPolicy&UserName=inlineuser&PolicyName=mypol",
	))
	resp6, _ := p.HandleRequest(ctx, "", req6)
	assert.Equal(t, 404, resp6.StatusCode)
}

func TestIAMProvider_InlineRolePolicyRoundtrip(t *testing.T) {
	p := newTestIAMProvider(t)
	ctx := context.Background()

	req1 := httptest.NewRequest("POST", "/", strings.NewReader(
		"Action=PutRolePolicy&RoleName=myrole&PolicyName=rolepol&PolicyDocument=%7B%7D",
	))
	resp1, err := p.HandleRequest(ctx, "", req1)
	require.NoError(t, err)
	assert.Equal(t, 200, resp1.StatusCode)

	req2 := httptest.NewRequest("POST", "/", strings.NewReader(
		"Action=GetRolePolicy&RoleName=myrole&PolicyName=rolepol",
	))
	resp2, err := p.HandleRequest(ctx, "", req2)
	require.NoError(t, err)
	assert.Equal(t, 200, resp2.StatusCode)
	assert.Contains(t, string(resp2.Body), "GetRolePolicyResponse")
	assert.Contains(t, string(resp2.Body), "rolepol")

	req3 := httptest.NewRequest("POST", "/", strings.NewReader(
		"Action=ListRolePolicies&RoleName=myrole",
	))
	resp3, err := p.HandleRequest(ctx, "", req3)
	require.NoError(t, err)
	assert.Equal(t, 200, resp3.StatusCode)
	assert.Contains(t, string(resp3.Body), "rolepol")

	req4 := httptest.NewRequest("POST", "/", strings.NewReader(
		"Action=DeleteRolePolicy&RoleName=myrole&PolicyName=rolepol",
	))
	resp4, err := p.HandleRequest(ctx, "", req4)
	require.NoError(t, err)
	assert.Equal(t, 200, resp4.StatusCode)
}

// Task 5: Groups

func TestIAMProvider_GroupRoundtrip(t *testing.T) {
	p := newTestIAMProvider(t)
	ctx := context.Background()

	req1 := httptest.NewRequest("POST", "/", strings.NewReader("Action=CreateGroup&GroupName=mygroup"))
	resp1, err := p.HandleRequest(ctx, "", req1)
	require.NoError(t, err)
	assert.Equal(t, 200, resp1.StatusCode)
	assert.Contains(t, string(resp1.Body), "CreateGroupResponse")
	assert.Contains(t, string(resp1.Body), "mygroup")

	req2 := httptest.NewRequest("POST", "/", strings.NewReader("Action=CreateUser&UserName=grpuser"))
	p.HandleRequest(ctx, "", req2)

	req3 := httptest.NewRequest("POST", "/", strings.NewReader("Action=AddUserToGroup&GroupName=mygroup&UserName=grpuser"))
	resp3, err := p.HandleRequest(ctx, "", req3)
	require.NoError(t, err)
	assert.Equal(t, 200, resp3.StatusCode)

	req4 := httptest.NewRequest("POST", "/", strings.NewReader("Action=GetGroup&GroupName=mygroup"))
	resp4, err := p.HandleRequest(ctx, "", req4)
	require.NoError(t, err)
	assert.Equal(t, 200, resp4.StatusCode)
	body4 := string(resp4.Body)
	assert.Contains(t, body4, "GetGroupResponse")
	assert.Contains(t, body4, "mygroup")
	assert.Contains(t, body4, "grpuser")

	req5 := httptest.NewRequest("POST", "/", strings.NewReader("Action=RemoveUserFromGroup&GroupName=mygroup&UserName=grpuser"))
	resp5, err := p.HandleRequest(ctx, "", req5)
	require.NoError(t, err)
	assert.Equal(t, 200, resp5.StatusCode)

	req6 := httptest.NewRequest("POST", "/", strings.NewReader("Action=GetGroup&GroupName=mygroup"))
	resp6, _ := p.HandleRequest(ctx, "", req6)
	assert.NotContains(t, string(resp6.Body), "grpuser")

	req7 := httptest.NewRequest("POST", "/", strings.NewReader("Action=DeleteGroup&GroupName=mygroup"))
	resp7, err := p.HandleRequest(ctx, "", req7)
	require.NoError(t, err)
	assert.Equal(t, 200, resp7.StatusCode)

	req8 := httptest.NewRequest("POST", "/", strings.NewReader("Action=GetGroup&GroupName=mygroup"))
	resp8, _ := p.HandleRequest(ctx, "", req8)
	assert.Equal(t, 404, resp8.StatusCode)
}

func TestIAMProvider_ListGroups(t *testing.T) {
	p := newTestIAMProvider(t)
	ctx := context.Background()

	req1 := httptest.NewRequest("POST", "/", strings.NewReader("Action=CreateGroup&GroupName=grp-a"))
	p.HandleRequest(ctx, "", req1)
	req2 := httptest.NewRequest("POST", "/", strings.NewReader("Action=CreateGroup&GroupName=grp-b"))
	p.HandleRequest(ctx, "", req2)

	req := httptest.NewRequest("POST", "/", strings.NewReader("Action=ListGroups"))
	resp, err := p.HandleRequest(ctx, "", req)
	require.NoError(t, err)
	assert.Equal(t, 200, resp.StatusCode)
	body := string(resp.Body)
	assert.Contains(t, body, "grp-a")
	assert.Contains(t, body, "grp-b")
	assert.Contains(t, body, "ListGroupsResponse")
}

// Task 6: Instance profiles

func TestIAMProvider_InstanceProfileRoundtrip(t *testing.T) {
	p := newTestIAMProvider(t)
	ctx := context.Background()

	req1 := httptest.NewRequest("POST", "/", strings.NewReader("Action=CreateInstanceProfile&InstanceProfileName=myip"))
	resp1, err := p.HandleRequest(ctx, "", req1)
	require.NoError(t, err)
	assert.Equal(t, 200, resp1.StatusCode)
	assert.Contains(t, string(resp1.Body), "CreateInstanceProfileResponse")
	assert.Contains(t, string(resp1.Body), "myip")

	req2 := httptest.NewRequest("POST", "/", strings.NewReader("Action=CreateRole&RoleName=iprole&AssumeRolePolicyDocument=%7B%7D"))
	p.HandleRequest(ctx, "", req2)

	req3 := httptest.NewRequest("POST", "/", strings.NewReader("Action=AddRoleToInstanceProfile&InstanceProfileName=myip&RoleName=iprole"))
	resp3, err := p.HandleRequest(ctx, "", req3)
	require.NoError(t, err)
	assert.Equal(t, 200, resp3.StatusCode)

	req4 := httptest.NewRequest("POST", "/", strings.NewReader("Action=GetInstanceProfile&InstanceProfileName=myip"))
	resp4, err := p.HandleRequest(ctx, "", req4)
	require.NoError(t, err)
	assert.Equal(t, 200, resp4.StatusCode)
	body4 := string(resp4.Body)
	assert.Contains(t, body4, "GetInstanceProfileResponse")
	assert.Contains(t, body4, "myip")
	assert.Contains(t, body4, "iprole")

	req5 := httptest.NewRequest("POST", "/", strings.NewReader("Action=RemoveRoleFromInstanceProfile&InstanceProfileName=myip&RoleName=iprole"))
	resp5, err := p.HandleRequest(ctx, "", req5)
	require.NoError(t, err)
	assert.Equal(t, 200, resp5.StatusCode)

	req6 := httptest.NewRequest("POST", "/", strings.NewReader("Action=GetInstanceProfile&InstanceProfileName=myip"))
	resp6, _ := p.HandleRequest(ctx, "", req6)
	assert.NotContains(t, string(resp6.Body), "iprole")

	req7 := httptest.NewRequest("POST", "/", strings.NewReader("Action=DeleteInstanceProfile&InstanceProfileName=myip"))
	resp7, err := p.HandleRequest(ctx, "", req7)
	require.NoError(t, err)
	assert.Equal(t, 200, resp7.StatusCode)

	req8 := httptest.NewRequest("POST", "/", strings.NewReader("Action=GetInstanceProfile&InstanceProfileName=myip"))
	resp8, _ := p.HandleRequest(ctx, "", req8)
	assert.Equal(t, 404, resp8.StatusCode)
}

func TestIAMProvider_ListInstanceProfiles(t *testing.T) {
	p := newTestIAMProvider(t)
	ctx := context.Background()

	req1 := httptest.NewRequest("POST", "/", strings.NewReader("Action=CreateInstanceProfile&InstanceProfileName=ip-a"))
	p.HandleRequest(ctx, "", req1)
	req2 := httptest.NewRequest("POST", "/", strings.NewReader("Action=CreateInstanceProfile&InstanceProfileName=ip-b"))
	p.HandleRequest(ctx, "", req2)

	req := httptest.NewRequest("POST", "/", strings.NewReader("Action=ListInstanceProfiles"))
	resp, err := p.HandleRequest(ctx, "", req)
	require.NoError(t, err)
	assert.Equal(t, 200, resp.StatusCode)
	body := string(resp.Body)
	assert.Contains(t, body, "ip-a")
	assert.Contains(t, body, "ip-b")
}

// Task 7: Access keys & tagging

func TestIAMProvider_AccessKeyManagement(t *testing.T) {
	p := newTestIAMProvider(t)
	ctx := context.Background()

	req1 := httptest.NewRequest("POST", "/", strings.NewReader("Action=CreateUser&UserName=akuser"))
	p.HandleRequest(ctx, "", req1)

	req2 := httptest.NewRequest("POST", "/", strings.NewReader("Action=CreateAccessKey&UserName=akuser"))
	resp2, err := p.HandleRequest(ctx, "", req2)
	require.NoError(t, err)
	require.Equal(t, 200, resp2.StatusCode)

	req3 := httptest.NewRequest("POST", "/", strings.NewReader("Action=ListAccessKeys&UserName=akuser"))
	resp3, err := p.HandleRequest(ctx, "", req3)
	require.NoError(t, err)
	assert.Equal(t, 200, resp3.StatusCode)
	body3 := string(resp3.Body)
	assert.Contains(t, body3, "ListAccessKeysResponse")
	assert.Contains(t, body3, "AKIA")

	startIdx := strings.Index(body3, "<AccessKeyId>")
	endIdx := strings.Index(body3, "</AccessKeyId>")
	require.True(t, startIdx >= 0 && endIdx > startIdx)
	keyID := body3[startIdx+len("<AccessKeyId>") : endIdx]

	req4 := httptest.NewRequest("POST", "/", strings.NewReader(
		"Action=UpdateAccessKey&UserName=akuser&AccessKeyId="+keyID+"&Status=Inactive",
	))
	resp4, err := p.HandleRequest(ctx, "", req4)
	require.NoError(t, err)
	assert.Equal(t, 200, resp4.StatusCode)
	assert.Contains(t, string(resp4.Body), "UpdateAccessKeyResponse")

	req5 := httptest.NewRequest("POST", "/", strings.NewReader("Action=ListAccessKeys&UserName=akuser"))
	resp5, _ := p.HandleRequest(ctx, "", req5)
	assert.Contains(t, string(resp5.Body), "Inactive")

	req6 := httptest.NewRequest("POST", "/", strings.NewReader(
		"Action=DeleteAccessKey&UserName=akuser&AccessKeyId="+keyID,
	))
	resp6, err := p.HandleRequest(ctx, "", req6)
	require.NoError(t, err)
	assert.Equal(t, 200, resp6.StatusCode)
	assert.Contains(t, string(resp6.Body), "DeleteAccessKeyResponse")
}

func TestIAMProvider_UserTagging(t *testing.T) {
	p := newTestIAMProvider(t)
	ctx := context.Background()

	req1 := httptest.NewRequest("POST", "/", strings.NewReader("Action=CreateUser&UserName=taguser"))
	p.HandleRequest(ctx, "", req1)

	req2 := httptest.NewRequest("POST", "/", strings.NewReader(
		"Action=TagUser&UserName=taguser&Tags.member.1.Key=env&Tags.member.1.Value=prod",
	))
	resp2, err := p.HandleRequest(ctx, "", req2)
	require.NoError(t, err)
	assert.Equal(t, 200, resp2.StatusCode)
	assert.Contains(t, string(resp2.Body), "TagUserResponse")

	req3 := httptest.NewRequest("POST", "/", strings.NewReader("Action=ListUserTags&UserName=taguser"))
	resp3, err := p.HandleRequest(ctx, "", req3)
	require.NoError(t, err)
	assert.Equal(t, 200, resp3.StatusCode)
	body3 := string(resp3.Body)
	assert.Contains(t, body3, "ListUserTagsResponse")
	assert.Contains(t, body3, "env")
	assert.Contains(t, body3, "prod")

	req4 := httptest.NewRequest("POST", "/", strings.NewReader(
		"Action=UntagUser&UserName=taguser&TagKeys.member.1=env",
	))
	resp4, err := p.HandleRequest(ctx, "", req4)
	require.NoError(t, err)
	assert.Equal(t, 200, resp4.StatusCode)

	req5 := httptest.NewRequest("POST", "/", strings.NewReader("Action=ListUserTags&UserName=taguser"))
	resp5, _ := p.HandleRequest(ctx, "", req5)
	assert.NotContains(t, string(resp5.Body), "env")
}

func TestIAMProvider_RoleTagging(t *testing.T) {
	p := newTestIAMProvider(t)
	ctx := context.Background()

	req1 := httptest.NewRequest("POST", "/", strings.NewReader(
		"Action=TagRole&RoleName=myrole&Tags.member.1.Key=purpose&Tags.member.1.Value=worker",
	))
	resp1, err := p.HandleRequest(ctx, "", req1)
	require.NoError(t, err)
	assert.Equal(t, 200, resp1.StatusCode)

	req2 := httptest.NewRequest("POST", "/", strings.NewReader("Action=ListRoleTags&RoleName=myrole"))
	resp2, err := p.HandleRequest(ctx, "", req2)
	require.NoError(t, err)
	assert.Equal(t, 200, resp2.StatusCode)
	body2 := string(resp2.Body)
	assert.Contains(t, body2, "purpose")
	assert.Contains(t, body2, "worker")

	req3 := httptest.NewRequest("POST", "/", strings.NewReader(
		"Action=UntagRole&RoleName=myrole&TagKeys.member.1=purpose",
	))
	resp3, err := p.HandleRequest(ctx, "", req3)
	require.NoError(t, err)
	assert.Equal(t, 200, resp3.StatusCode)
}
