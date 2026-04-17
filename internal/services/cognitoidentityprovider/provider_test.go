// SPDX-License-Identifier: Apache-2.0

// internal/services/cognitoidentityprovider/provider_test.go
package cognitoidentityprovider

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
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
	t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
	return p
}

func cognitoReq(t *testing.T, p *Provider, op string, body any) *plugin.Response {
	t.Helper()
	b, err := json.Marshal(body)
	require.NoError(t, err)
	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(b))
	req.Header.Set("X-Amz-Target", "AmazonCognitoIdentityProvider."+op)
	resp, err := p.HandleRequest(context.Background(), op, req)
	require.NoError(t, err)
	require.NotNil(t, resp)
	return resp
}

// --- TestUserPoolCRUD ---

func TestUserPoolCRUD(t *testing.T) {
	p := newTestProvider(t)

	// Create
	resp := cognitoReq(t, p, "CreateUserPool", map[string]any{
		"PoolName": "TestPool",
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	var createOut map[string]any
	require.NoError(t, json.Unmarshal(resp.Body, &createOut))
	pool := createOut["UserPool"].(map[string]any)
	poolID := pool["Id"].(string)
	poolARNStr := pool["Arn"].(string)
	require.NotEmpty(t, poolID)
	assert.Contains(t, poolARNStr, "arn:aws:cognito-idp:")
	assert.Equal(t, "TestPool", pool["Name"])

	// Describe
	resp = cognitoReq(t, p, "DescribeUserPool", map[string]any{"UserPoolId": poolID})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	var descOut map[string]any
	require.NoError(t, json.Unmarshal(resp.Body, &descOut))
	assert.Equal(t, poolID, descOut["UserPool"].(map[string]any)["Id"])

	// List
	resp = cognitoReq(t, p, "ListUserPools", map[string]any{})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	var listOut map[string]any
	require.NoError(t, json.Unmarshal(resp.Body, &listOut))
	pools := listOut["UserPools"].([]any)
	require.Len(t, pools, 1)

	// Update
	resp = cognitoReq(t, p, "UpdateUserPool", map[string]any{
		"UserPoolId":       poolID,
		"MfaConfiguration": "OPTIONAL",
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// Verify update
	resp = cognitoReq(t, p, "DescribeUserPool", map[string]any{"UserPoolId": poolID})
	require.NoError(t, json.Unmarshal(resp.Body, &descOut))
	assert.Equal(t, "OPTIONAL", descOut["UserPool"].(map[string]any)["MfaConfiguration"])

	// Delete
	resp = cognitoReq(t, p, "DeleteUserPool", map[string]any{"UserPoolId": poolID})
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// Describe after delete → error
	resp = cognitoReq(t, p, "DescribeUserPool", map[string]any{"UserPoolId": poolID})
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

// --- TestUserPoolClientCRUD ---

func TestUserPoolClientCRUD(t *testing.T) {
	p := newTestProvider(t)

	// Create pool first
	resp := cognitoReq(t, p, "CreateUserPool", map[string]any{"PoolName": "ClientPool"})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var poolOut map[string]any
	require.NoError(t, json.Unmarshal(resp.Body, &poolOut))
	poolID := poolOut["UserPool"].(map[string]any)["Id"].(string)

	// Create client
	resp = cognitoReq(t, p, "CreateUserPoolClient", map[string]any{
		"UserPoolId": poolID,
		"ClientName": "MyClient",
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	var clientOut map[string]any
	require.NoError(t, json.Unmarshal(resp.Body, &clientOut))
	client := clientOut["UserPoolClient"].(map[string]any)
	clientID := client["ClientId"].(string)
	require.NotEmpty(t, clientID)
	assert.Equal(t, "MyClient", client["ClientName"])

	// Describe
	resp = cognitoReq(t, p, "DescribeUserPoolClient", map[string]any{
		"UserPoolId": poolID,
		"ClientId":   clientID,
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// List
	resp = cognitoReq(t, p, "ListUserPoolClients", map[string]any{"UserPoolId": poolID})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	var listOut map[string]any
	require.NoError(t, json.Unmarshal(resp.Body, &listOut))
	clients := listOut["UserPoolClients"].([]any)
	require.Len(t, clients, 1)

	// Update
	resp = cognitoReq(t, p, "UpdateUserPoolClient", map[string]any{
		"UserPoolId": poolID,
		"ClientId":   clientID,
		"ClientName": "UpdatedClient",
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	var updOut map[string]any
	require.NoError(t, json.Unmarshal(resp.Body, &updOut))
	assert.Equal(t, "UpdatedClient", updOut["UserPoolClient"].(map[string]any)["ClientName"])

	// Delete
	resp = cognitoReq(t, p, "DeleteUserPoolClient", map[string]any{
		"UserPoolId": poolID,
		"ClientId":   clientID,
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// Describe after delete → error
	resp = cognitoReq(t, p, "DescribeUserPoolClient", map[string]any{
		"UserPoolId": poolID,
		"ClientId":   clientID,
	})
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

// --- TestUserCRUD ---

func TestUserCRUD(t *testing.T) {
	p := newTestProvider(t)

	resp := cognitoReq(t, p, "CreateUserPool", map[string]any{"PoolName": "UserPool"})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var poolOut map[string]any
	require.NoError(t, json.Unmarshal(resp.Body, &poolOut))
	poolID := poolOut["UserPool"].(map[string]any)["Id"].(string)

	// AdminCreateUser
	resp = cognitoReq(t, p, "AdminCreateUser", map[string]any{
		"UserPoolId":        poolID,
		"Username":          "alice",
		"TemporaryPassword": "Temp123!",
		"UserAttributes": []map[string]string{
			{"Name": "email", "Value": "alice@example.com"},
		},
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	var userOut map[string]any
	require.NoError(t, json.Unmarshal(resp.Body, &userOut))
	user := userOut["User"].(map[string]any)
	assert.Equal(t, "alice", user["Username"])
	assert.Equal(t, "FORCE_CHANGE_PASSWORD", user["UserStatus"])

	// AdminGetUser
	resp = cognitoReq(t, p, "AdminGetUser", map[string]any{
		"UserPoolId": poolID,
		"Username":   "alice",
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	var getOut map[string]any
	require.NoError(t, json.Unmarshal(resp.Body, &getOut))
	assert.Equal(t, "alice", getOut["Username"])

	// ListUsers
	resp = cognitoReq(t, p, "ListUsers", map[string]any{"UserPoolId": poolID})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	var listOut map[string]any
	require.NoError(t, json.Unmarshal(resp.Body, &listOut))
	users := listOut["Users"].([]any)
	require.Len(t, users, 1)

	// AdminDisableUser
	resp = cognitoReq(t, p, "AdminDisableUser", map[string]any{
		"UserPoolId": poolID,
		"Username":   "alice",
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// Verify disabled
	resp = cognitoReq(t, p, "AdminGetUser", map[string]any{
		"UserPoolId": poolID,
		"Username":   "alice",
	})
	require.NoError(t, json.Unmarshal(resp.Body, &getOut))
	assert.False(t, getOut["Enabled"].(bool))

	// AdminEnableUser
	resp = cognitoReq(t, p, "AdminEnableUser", map[string]any{
		"UserPoolId": poolID,
		"Username":   "alice",
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// AdminUpdateUserAttributes
	resp = cognitoReq(t, p, "AdminUpdateUserAttributes", map[string]any{
		"UserPoolId": poolID,
		"Username":   "alice",
		"UserAttributes": []map[string]string{
			{"Name": "email", "Value": "alice2@example.com"},
			{"Name": "phone_number", "Value": "+1234567890"},
		},
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// Verify update
	resp = cognitoReq(t, p, "AdminGetUser", map[string]any{
		"UserPoolId": poolID,
		"Username":   "alice",
	})
	require.NoError(t, json.Unmarshal(resp.Body, &getOut))
	attrs := getOut["UserAttributes"].([]any)
	require.Len(t, attrs, 2)

	// AdminSetUserPassword
	resp = cognitoReq(t, p, "AdminSetUserPassword", map[string]any{
		"UserPoolId": poolID,
		"Username":   "alice",
		"Password":   "NewPass123!",
		"Permanent":  true,
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// AdminConfirmSignUp
	resp = cognitoReq(t, p, "AdminConfirmSignUp", map[string]any{
		"UserPoolId": poolID,
		"Username":   "alice",
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// AdminDeleteUserAttributes
	resp = cognitoReq(t, p, "AdminDeleteUserAttributes", map[string]any{
		"UserPoolId":         poolID,
		"Username":           "alice",
		"UserAttributeNames": []string{"phone_number"},
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// AdminDeleteUser
	resp = cognitoReq(t, p, "AdminDeleteUser", map[string]any{
		"UserPoolId": poolID,
		"Username":   "alice",
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// Get after delete → error
	resp = cognitoReq(t, p, "AdminGetUser", map[string]any{
		"UserPoolId": poolID,
		"Username":   "alice",
	})
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

// --- TestGroupCRUD ---

func TestGroupCRUD(t *testing.T) {
	p := newTestProvider(t)

	resp := cognitoReq(t, p, "CreateUserPool", map[string]any{"PoolName": "GroupPool"})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var poolOut map[string]any
	require.NoError(t, json.Unmarshal(resp.Body, &poolOut))
	poolID := poolOut["UserPool"].(map[string]any)["Id"].(string)

	// CreateGroup
	resp = cognitoReq(t, p, "CreateGroup", map[string]any{
		"UserPoolId":  poolID,
		"GroupName":   "Admins",
		"Description": "Admin group",
		"Precedence":  1,
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	var grpOut map[string]any
	require.NoError(t, json.Unmarshal(resp.Body, &grpOut))
	grp := grpOut["Group"].(map[string]any)
	assert.Equal(t, "Admins", grp["GroupName"])
	assert.Equal(t, "Admin group", grp["Description"])

	// GetGroup
	resp = cognitoReq(t, p, "GetGroup", map[string]any{
		"UserPoolId": poolID,
		"GroupName":  "Admins",
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// ListGroups
	resp = cognitoReq(t, p, "ListGroups", map[string]any{"UserPoolId": poolID})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	var listOut map[string]any
	require.NoError(t, json.Unmarshal(resp.Body, &listOut))
	groups := listOut["Groups"].([]any)
	require.Len(t, groups, 1)

	// UpdateGroup
	resp = cognitoReq(t, p, "UpdateGroup", map[string]any{
		"UserPoolId":  poolID,
		"GroupName":   "Admins",
		"Description": "Updated admins",
		"Precedence":  0,
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	var updOut map[string]any
	require.NoError(t, json.Unmarshal(resp.Body, &updOut))
	assert.Equal(t, "Updated admins", updOut["Group"].(map[string]any)["Description"])

	// DeleteGroup
	resp = cognitoReq(t, p, "DeleteGroup", map[string]any{
		"UserPoolId": poolID,
		"GroupName":  "Admins",
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// GetGroup after delete → error
	resp = cognitoReq(t, p, "GetGroup", map[string]any{
		"UserPoolId": poolID,
		"GroupName":  "Admins",
	})
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

// --- TestUserGroupMembership ---

func TestUserGroupMembership(t *testing.T) {
	p := newTestProvider(t)

	// Create pool
	resp := cognitoReq(t, p, "CreateUserPool", map[string]any{"PoolName": "MemberPool"})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var poolOut map[string]any
	require.NoError(t, json.Unmarshal(resp.Body, &poolOut))
	poolID := poolOut["UserPool"].(map[string]any)["Id"].(string)

	// Create user
	resp = cognitoReq(t, p, "AdminCreateUser", map[string]any{
		"UserPoolId": poolID,
		"Username":   "bob",
	})
	require.Equal(t, http.StatusOK, resp.StatusCode)

	// Create group
	resp = cognitoReq(t, p, "CreateGroup", map[string]any{
		"UserPoolId": poolID,
		"GroupName":  "Editors",
	})
	require.Equal(t, http.StatusOK, resp.StatusCode)

	// AdminAddUserToGroup
	resp = cognitoReq(t, p, "AdminAddUserToGroup", map[string]any{
		"UserPoolId": poolID,
		"Username":   "bob",
		"GroupName":  "Editors",
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// AdminListGroupsForUser
	resp = cognitoReq(t, p, "AdminListGroupsForUser", map[string]any{
		"UserPoolId": poolID,
		"Username":   "bob",
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	var listOut map[string]any
	require.NoError(t, json.Unmarshal(resp.Body, &listOut))
	groups := listOut["Groups"].([]any)
	require.Len(t, groups, 1)
	assert.Equal(t, "Editors", groups[0].(map[string]any)["GroupName"])

	// ListUsersInGroup
	resp = cognitoReq(t, p, "ListUsersInGroup", map[string]any{
		"UserPoolId": poolID,
		"GroupName":  "Editors",
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	var usersOut map[string]any
	require.NoError(t, json.Unmarshal(resp.Body, &usersOut))
	users := usersOut["Users"].([]any)
	require.Len(t, users, 1)
	assert.Equal(t, "bob", users[0].(map[string]any)["Username"])

	// AdminRemoveUserFromGroup
	resp = cognitoReq(t, p, "AdminRemoveUserFromGroup", map[string]any{
		"UserPoolId": poolID,
		"Username":   "bob",
		"GroupName":  "Editors",
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// Verify removed
	resp = cognitoReq(t, p, "AdminListGroupsForUser", map[string]any{
		"UserPoolId": poolID,
		"Username":   "bob",
	})
	require.NoError(t, json.Unmarshal(resp.Body, &listOut))
	groups = listOut["Groups"].([]any)
	assert.Len(t, groups, 0)
}

// --- TestIdentityProviderCRUD ---

func TestIdentityProviderCRUD(t *testing.T) {
	p := newTestProvider(t)

	resp := cognitoReq(t, p, "CreateUserPool", map[string]any{"PoolName": "IDPPool"})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var poolOut map[string]any
	require.NoError(t, json.Unmarshal(resp.Body, &poolOut))
	poolID := poolOut["UserPool"].(map[string]any)["Id"].(string)

	// CreateIdentityProvider
	resp = cognitoReq(t, p, "CreateIdentityProvider", map[string]any{
		"UserPoolId":   poolID,
		"ProviderName": "MySAML",
		"ProviderType": "SAML",
		"ProviderDetails": map[string]string{
			"MetadataURL": "https://example.com/saml/metadata",
		},
		"IdpIdentifiers": []string{"saml-idp"},
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	var idpOut map[string]any
	require.NoError(t, json.Unmarshal(resp.Body, &idpOut))
	idp := idpOut["IdentityProvider"].(map[string]any)
	assert.Equal(t, "MySAML", idp["ProviderName"])

	// DescribeIdentityProvider
	resp = cognitoReq(t, p, "DescribeIdentityProvider", map[string]any{
		"UserPoolId":   poolID,
		"ProviderName": "MySAML",
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// ListIdentityProviders
	resp = cognitoReq(t, p, "ListIdentityProviders", map[string]any{"UserPoolId": poolID})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	var listOut map[string]any
	require.NoError(t, json.Unmarshal(resp.Body, &listOut))
	providers := listOut["Providers"].([]any)
	require.Len(t, providers, 1)

	// GetIdentityProviderByIdentifier
	resp = cognitoReq(t, p, "GetIdentityProviderByIdentifier", map[string]any{
		"UserPoolId":    poolID,
		"IdpIdentifier": "saml-idp",
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	var byIDOut map[string]any
	require.NoError(t, json.Unmarshal(resp.Body, &byIDOut))
	assert.Equal(t, "MySAML", byIDOut["IdentityProvider"].(map[string]any)["ProviderName"])

	// UpdateIdentityProvider
	resp = cognitoReq(t, p, "UpdateIdentityProvider", map[string]any{
		"UserPoolId":   poolID,
		"ProviderName": "MySAML",
		"ProviderDetails": map[string]string{
			"MetadataURL": "https://example.com/saml/metadata2",
		},
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// DeleteIdentityProvider
	resp = cognitoReq(t, p, "DeleteIdentityProvider", map[string]any{
		"UserPoolId":   poolID,
		"ProviderName": "MySAML",
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// Describe after delete → error
	resp = cognitoReq(t, p, "DescribeIdentityProvider", map[string]any{
		"UserPoolId":   poolID,
		"ProviderName": "MySAML",
	})
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

// --- TestResourceServerCRUD ---

func TestResourceServerCRUD(t *testing.T) {
	p := newTestProvider(t)

	resp := cognitoReq(t, p, "CreateUserPool", map[string]any{"PoolName": "RSPool"})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var poolOut map[string]any
	require.NoError(t, json.Unmarshal(resp.Body, &poolOut))
	poolID := poolOut["UserPool"].(map[string]any)["Id"].(string)

	// CreateResourceServer
	resp = cognitoReq(t, p, "CreateResourceServer", map[string]any{
		"UserPoolId": poolID,
		"Identifier": "https://api.example.com",
		"Name":       "MyAPI",
		"Scopes": []map[string]string{
			{"ScopeName": "read", "ScopeDescription": "Read access"},
		},
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	var rsOut map[string]any
	require.NoError(t, json.Unmarshal(resp.Body, &rsOut))
	rs := rsOut["ResourceServer"].(map[string]any)
	assert.Equal(t, "MyAPI", rs["Name"])
	assert.Equal(t, "https://api.example.com", rs["Identifier"])

	// DescribeResourceServer
	resp = cognitoReq(t, p, "DescribeResourceServer", map[string]any{
		"UserPoolId": poolID,
		"Identifier": "https://api.example.com",
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// ListResourceServers
	resp = cognitoReq(t, p, "ListResourceServers", map[string]any{"UserPoolId": poolID})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	var listOut map[string]any
	require.NoError(t, json.Unmarshal(resp.Body, &listOut))
	servers := listOut["ResourceServers"].([]any)
	require.Len(t, servers, 1)

	// UpdateResourceServer
	resp = cognitoReq(t, p, "UpdateResourceServer", map[string]any{
		"UserPoolId": poolID,
		"Identifier": "https://api.example.com",
		"Name":       "UpdatedAPI",
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	var updOut map[string]any
	require.NoError(t, json.Unmarshal(resp.Body, &updOut))
	assert.Equal(t, "UpdatedAPI", updOut["ResourceServer"].(map[string]any)["Name"])

	// DeleteResourceServer
	resp = cognitoReq(t, p, "DeleteResourceServer", map[string]any{
		"UserPoolId": poolID,
		"Identifier": "https://api.example.com",
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// Describe after delete → error
	resp = cognitoReq(t, p, "DescribeResourceServer", map[string]any{
		"UserPoolId": poolID,
		"Identifier": "https://api.example.com",
	})
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

// --- TestSignUpAndAuth ---

func TestSignUpAndAuth(t *testing.T) {
	p := newTestProvider(t)

	// Create pool
	resp := cognitoReq(t, p, "CreateUserPool", map[string]any{"PoolName": "AuthPool"})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var poolOut map[string]any
	require.NoError(t, json.Unmarshal(resp.Body, &poolOut))
	poolID := poolOut["UserPool"].(map[string]any)["Id"].(string)

	// Create client
	resp = cognitoReq(t, p, "CreateUserPoolClient", map[string]any{
		"UserPoolId": poolID,
		"ClientName": "AuthClient",
	})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var clientOut map[string]any
	require.NoError(t, json.Unmarshal(resp.Body, &clientOut))
	clientID := clientOut["UserPoolClient"].(map[string]any)["ClientId"].(string)

	// SignUp
	resp = cognitoReq(t, p, "SignUp", map[string]any{
		"ClientId": clientID,
		"Username": "carol",
		"Password": "Password123!",
		"UserAttributes": []map[string]string{
			{"Name": "email", "Value": "carol@example.com"},
		},
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	var signUpOut map[string]any
	require.NoError(t, json.Unmarshal(resp.Body, &signUpOut))
	assert.False(t, signUpOut["UserConfirmed"].(bool))

	// Verify user is UNCONFIRMED
	user, err := p.store.GetUser(poolID, "carol")
	require.NoError(t, err)
	assert.Equal(t, "UNCONFIRMED", user.Status)

	// ConfirmSignUp
	resp = cognitoReq(t, p, "ConfirmSignUp", map[string]any{
		"ClientId":         clientID,
		"Username":         "carol",
		"ConfirmationCode": "123456",
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// Verify confirmed
	user, err = p.store.GetUser(poolID, "carol")
	require.NoError(t, err)
	assert.Equal(t, "CONFIRMED", user.Status)

	// InitiateAuth
	resp = cognitoReq(t, p, "InitiateAuth", map[string]any{
		"AuthFlow": "USER_PASSWORD_AUTH",
		"ClientId": clientID,
		"AuthParameters": map[string]string{
			"USERNAME": "carol",
			"PASSWORD": "Password123!",
		},
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	var authOut map[string]any
	require.NoError(t, json.Unmarshal(resp.Body, &authOut))
	authResult := authOut["AuthenticationResult"].(map[string]any)
	accessToken := authResult["AccessToken"].(string)
	assert.NotEmpty(t, accessToken)
	assert.NotEmpty(t, authResult["IdToken"])
	assert.NotEmpty(t, authResult["RefreshToken"])

	// GetUser using the access token
	resp = cognitoReq(t, p, "GetUser", map[string]any{
		"AccessToken": accessToken,
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	var getUserOut map[string]any
	require.NoError(t, json.Unmarshal(resp.Body, &getUserOut))
	assert.Equal(t, "carol", getUserOut["Username"])

	// AdminInitiateAuth
	resp = cognitoReq(t, p, "AdminInitiateAuth", map[string]any{
		"UserPoolId": poolID,
		"ClientId":   clientID,
		"AuthFlow":   "ADMIN_USER_PASSWORD_AUTH",
		"AuthParameters": map[string]string{
			"USERNAME": "carol",
			"PASSWORD": "Password123!",
		},
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	var adminAuthOut map[string]any
	require.NoError(t, json.Unmarshal(resp.Body, &adminAuthOut))
	assert.NotNil(t, adminAuthOut["AuthenticationResult"])

	// ForgotPassword
	resp = cognitoReq(t, p, "ForgotPassword", map[string]any{
		"ClientId": clientID,
		"Username": "carol",
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// ConfirmForgotPassword
	resp = cognitoReq(t, p, "ConfirmForgotPassword", map[string]any{
		"ClientId":         clientID,
		"Username":         "carol",
		"ConfirmationCode": "123456",
		"Password":         "NewPassword123!",
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// GlobalSignOut
	resp = cognitoReq(t, p, "GlobalSignOut", map[string]any{
		"AccessToken": accessToken,
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// AdminUserGlobalSignOut
	resp = cognitoReq(t, p, "AdminUserGlobalSignOut", map[string]any{
		"UserPoolId": poolID,
		"Username":   "carol",
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// RevokeToken
	resp = cognitoReq(t, p, "RevokeToken", map[string]any{
		"Token":    authResult["RefreshToken"],
		"ClientId": clientID,
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

// --- TestTags ---

func TestTags(t *testing.T) {
	p := newTestProvider(t)

	// Create pool
	resp := cognitoReq(t, p, "CreateUserPool", map[string]any{"PoolName": "TagPool"})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var poolOut map[string]any
	require.NoError(t, json.Unmarshal(resp.Body, &poolOut))
	poolARNStr := poolOut["UserPool"].(map[string]any)["Arn"].(string)

	// TagResource
	resp = cognitoReq(t, p, "TagResource", map[string]any{
		"ResourceArn": poolARNStr,
		"Tags": map[string]string{
			"env":   "test",
			"owner": "alice",
		},
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// ListTagsForResource
	resp = cognitoReq(t, p, "ListTagsForResource", map[string]any{
		"ResourceArn": poolARNStr,
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	var listOut map[string]any
	require.NoError(t, json.Unmarshal(resp.Body, &listOut))
	tags := listOut["Tags"].(map[string]any)
	assert.Equal(t, "test", tags["env"])
	assert.Equal(t, "alice", tags["owner"])

	// UntagResource
	resp = cognitoReq(t, p, "UntagResource", map[string]any{
		"ResourceArn": poolARNStr,
		"TagKeys":     []string{"owner"},
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// Verify tag removed
	resp = cognitoReq(t, p, "ListTagsForResource", map[string]any{
		"ResourceArn": poolARNStr,
	})
	require.NoError(t, json.Unmarshal(resp.Body, &listOut))
	tags = listOut["Tags"].(map[string]any)
	assert.Equal(t, "test", tags["env"])
	_, hasOwner := tags["owner"]
	assert.False(t, hasOwner)
}

// --- TestMFAConfig ---

func TestMFAConfig(t *testing.T) {
	p := newTestProvider(t)

	resp := cognitoReq(t, p, "CreateUserPool", map[string]any{"PoolName": "MFAPool"})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var poolOut map[string]any
	require.NoError(t, json.Unmarshal(resp.Body, &poolOut))
	poolID := poolOut["UserPool"].(map[string]any)["Id"].(string)

	// SetUserPoolMfaConfig
	resp = cognitoReq(t, p, "SetUserPoolMfaConfig", map[string]any{
		"UserPoolId":       poolID,
		"MfaConfiguration": "ON",
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	var mfaOut map[string]any
	require.NoError(t, json.Unmarshal(resp.Body, &mfaOut))
	assert.Equal(t, "ON", mfaOut["MfaConfiguration"])

	// GetUserPoolMfaConfig
	resp = cognitoReq(t, p, "GetUserPoolMfaConfig", map[string]any{
		"UserPoolId": poolID,
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	require.NoError(t, json.Unmarshal(resp.Body, &mfaOut))
	assert.Equal(t, "ON", mfaOut["MfaConfiguration"])
}
