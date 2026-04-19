// SPDX-License-Identifier: Apache-2.0

package iam

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestStore(t *testing.T) *IAMStore {
	t.Helper()
	store, err := NewIAMStore(":memory:")
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })
	return store
}

func TestIAMStore_CreateAndListUsers(t *testing.T) {
	store := newTestStore(t)

	user, err := store.CreateUser("123456789012", "alice")
	require.NoError(t, err)
	assert.Equal(t, "alice", user.UserName)
	assert.Equal(t, "123456789012", user.AccountID)
	assert.Equal(t, "arn:aws:iam::123456789012:user/alice", user.Arn)
	assert.NotEmpty(t, user.UserID)

	users, err := store.ListUsers("123456789012")
	require.NoError(t, err)
	require.Len(t, users, 1)
	assert.Equal(t, "alice", users[0].UserName)
	assert.Equal(t, "arn:aws:iam::123456789012:user/alice", users[0].Arn)
}

func TestIAMStore_CreateDuplicateUser(t *testing.T) {
	store := newTestStore(t)

	_, err := store.CreateUser("123456789012", "alice")
	require.NoError(t, err)

	_, err = store.CreateUser("123456789012", "alice")
	assert.ErrorIs(t, err, ErrUserAlreadyExists)
}

func TestIAMStore_CreateAndListRoles(t *testing.T) {
	store := newTestStore(t)

	policy := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"ec2.amazonaws.com"},"Action":"sts:AssumeRole"}]}`
	role, err := store.CreateRole("123456789012", "my-role", policy)
	require.NoError(t, err)
	assert.Equal(t, "my-role", role.RoleName)
	assert.Equal(t, "123456789012", role.AccountID)
	assert.NotEmpty(t, role.RoleID)

	roles, err := store.ListRoles("123456789012")
	require.NoError(t, err)
	require.Len(t, roles, 1)
	assert.Equal(t, "my-role", roles[0].RoleName)
}

func TestIAMStore_AttachRolePolicy(t *testing.T) {
	store := newTestStore(t)

	policy := `{"Version":"2012-10-17","Statement":[]}`
	_, err := store.CreateRole("123456789012", "my-role", policy)
	require.NoError(t, err)

	err = store.AttachRolePolicy("123456789012", "my-role", "arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")
	assert.NoError(t, err)
}

func TestIAMStore_CreateAccessKey(t *testing.T) {
	store := newTestStore(t)

	_, err := store.CreateUser("123456789012", "bob")
	require.NoError(t, err)

	key, err := store.CreateAccessKey("123456789012", "bob")
	require.NoError(t, err)
	assert.True(t, strings.HasPrefix(key.AccessKeyID, "AKIA"), "access key ID should start with AKIA, got: %s", key.AccessKeyID)
	assert.NotEmpty(t, key.SecretAccessKey)
	assert.Equal(t, "bob", key.UserName)
	assert.Equal(t, "Active", key.Status)
}

func TestIAMStore_GetCallerIdentity(t *testing.T) {
	store := newTestStore(t)

	accountID, arn, userID := store.GetCallerIdentity("123456789012")
	assert.Equal(t, "123456789012", accountID)
	assert.NotEmpty(t, arn)
	assert.NotEmpty(t, userID)
}

// Task 4: Inline policies

func TestIAMStore_UserInlinePolicyRoundtrip(t *testing.T) {
	store := newTestStore(t)
	acct := "123456789012"

	err := store.PutUserInlinePolicy(acct, "alice", "mypol", `{"Version":"2012-10-17"}`)
	require.NoError(t, err)

	doc, err := store.GetUserInlinePolicy(acct, "alice", "mypol")
	require.NoError(t, err)
	assert.Equal(t, `{"Version":"2012-10-17"}`, doc)

	names, err := store.ListUserInlinePolicies(acct, "alice")
	require.NoError(t, err)
	assert.Contains(t, names, "mypol")

	err = store.DeleteUserInlinePolicy(acct, "alice", "mypol")
	require.NoError(t, err)

	_, err = store.GetUserInlinePolicy(acct, "alice", "mypol")
	assert.ErrorIs(t, err, ErrPolicyNotFound)
}

func TestIAMStore_RoleInlinePolicyRoundtrip(t *testing.T) {
	store := newTestStore(t)
	acct := "123456789012"

	err := store.PutRoleInlinePolicy(acct, "myrole", "rolepol", `{}`)
	require.NoError(t, err)

	doc, err := store.GetRoleInlinePolicy(acct, "myrole", "rolepol")
	require.NoError(t, err)
	assert.Equal(t, `{}`, doc)

	names, err := store.ListRoleInlinePolicies(acct, "myrole")
	require.NoError(t, err)
	assert.Contains(t, names, "rolepol")

	err = store.DeleteRoleInlinePolicy(acct, "myrole", "rolepol")
	require.NoError(t, err)

	names, err = store.ListRoleInlinePolicies(acct, "myrole")
	require.NoError(t, err)
	assert.NotContains(t, names, "rolepol")
}

// Task 5: Groups

func TestIAMStore_GroupRoundtrip(t *testing.T) {
	store := newTestStore(t)
	acct := "123456789012"

	g, err := store.CreateGroup(acct, "mygroup")
	require.NoError(t, err)
	assert.Equal(t, "mygroup", g.GroupName)
	assert.NotEmpty(t, g.GroupID)

	err = store.AddUserToGroup(acct, "mygroup", "alice")
	require.NoError(t, err)
	err = store.AddUserToGroup(acct, "mygroup", "bob")
	require.NoError(t, err)

	grp, members, err := store.GetGroup(acct, "mygroup")
	require.NoError(t, err)
	assert.Equal(t, "mygroup", grp.GroupName)
	assert.Contains(t, members, "alice")
	assert.Contains(t, members, "bob")

	err = store.RemoveUserFromGroup(acct, "mygroup", "alice")
	require.NoError(t, err)

	_, members2, err := store.GetGroup(acct, "mygroup")
	require.NoError(t, err)
	assert.NotContains(t, members2, "alice")
	assert.Contains(t, members2, "bob")

	err = store.DeleteGroup(acct, "mygroup")
	require.NoError(t, err)

	_, _, err = store.GetGroup(acct, "mygroup")
	assert.ErrorIs(t, err, ErrGroupNotFound)
}

func TestIAMStore_CreateDuplicateGroup(t *testing.T) {
	store := newTestStore(t)
	_, err := store.CreateGroup("123456789012", "dg")
	require.NoError(t, err)
	_, err = store.CreateGroup("123456789012", "dg")
	assert.ErrorIs(t, err, ErrGroupAlreadyExists)
}

// Task 6: Instance profiles

func TestIAMStore_InstanceProfileRoundtrip(t *testing.T) {
	store := newTestStore(t)
	acct := "123456789012"

	ip, err := store.CreateInstanceProfile(acct, "myip")
	require.NoError(t, err)
	assert.Equal(t, "myip", ip.ProfileName)
	assert.NotEmpty(t, ip.ProfileID)

	err = store.AddRoleToInstanceProfile(acct, "myip", "myrole")
	require.NoError(t, err)

	profile, roles, err := store.GetInstanceProfile(acct, "myip")
	require.NoError(t, err)
	assert.Equal(t, "myip", profile.ProfileName)
	assert.Contains(t, roles, "myrole")

	err = store.RemoveRoleFromInstanceProfile(acct, "myip", "myrole")
	require.NoError(t, err)

	_, roles2, err := store.GetInstanceProfile(acct, "myip")
	require.NoError(t, err)
	assert.NotContains(t, roles2, "myrole")

	err = store.DeleteInstanceProfile(acct, "myip")
	require.NoError(t, err)

	_, _, err = store.GetInstanceProfile(acct, "myip")
	assert.ErrorIs(t, err, ErrInstanceProfileNotFound)
}

func TestIAMStore_CreateDuplicateInstanceProfile(t *testing.T) {
	store := newTestStore(t)
	_, err := store.CreateInstanceProfile("123456789012", "dip")
	require.NoError(t, err)
	_, err = store.CreateInstanceProfile("123456789012", "dip")
	assert.ErrorIs(t, err, ErrInstanceProfileAlreadyExists)
}

// Task 7: Access keys & tagging

func TestIAMStore_AccessKeyManagement(t *testing.T) {
	store := newTestStore(t)
	acct := "123456789012"

	_, err := store.CreateUser(acct, "akuser")
	require.NoError(t, err)

	key, err := store.CreateAccessKey(acct, "akuser")
	require.NoError(t, err)

	keys, err := store.ListAccessKeys(acct, "akuser")
	require.NoError(t, err)
	require.Len(t, keys, 1)
	assert.Equal(t, "Active", keys[0].Status)
	assert.Equal(t, key.AccessKeyID, keys[0].AccessKeyID)

	err = store.UpdateAccessKey(acct, "akuser", key.AccessKeyID, "Inactive")
	require.NoError(t, err)

	keys2, err := store.ListAccessKeys(acct, "akuser")
	require.NoError(t, err)
	assert.Equal(t, "Inactive", keys2[0].Status)

	err = store.DeleteAccessKey(acct, "akuser", key.AccessKeyID)
	require.NoError(t, err)

	keys3, err := store.ListAccessKeys(acct, "akuser")
	require.NoError(t, err)
	assert.Len(t, keys3, 0)
}

func TestIAMStore_UserTagging(t *testing.T) {
	store := newTestStore(t)
	acct := "123456789012"

	err := store.TagUser(acct, "alice", map[string]string{"env": "prod", "team": "infra"})
	require.NoError(t, err)

	tags, err := store.ListUserTags(acct, "alice")
	require.NoError(t, err)
	assert.Equal(t, "prod", tags["env"])
	assert.Equal(t, "infra", tags["team"])

	err = store.UntagUser(acct, "alice", []string{"env"})
	require.NoError(t, err)

	tags2, err := store.ListUserTags(acct, "alice")
	require.NoError(t, err)
	_, hasEnv := tags2["env"]
	assert.False(t, hasEnv)
	assert.Equal(t, "infra", tags2["team"])
}

func TestIAMStore_RoleTagging(t *testing.T) {
	store := newTestStore(t)
	acct := "123456789012"

	err := store.TagRole(acct, "myrole", map[string]string{"purpose": "worker"})
	require.NoError(t, err)

	tags, err := store.ListRoleTags(acct, "myrole")
	require.NoError(t, err)
	assert.Equal(t, "worker", tags["purpose"])

	err = store.UntagRole(acct, "myrole", []string{"purpose"})
	require.NoError(t, err)

	tags2, err := store.ListRoleTags(acct, "myrole")
	require.NoError(t, err)
	assert.Empty(t, tags2)
}
