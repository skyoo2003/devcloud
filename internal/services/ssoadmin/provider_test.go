// SPDX-License-Identifier: Apache-2.0

// internal/services/ssoadmin/provider_test.go
package ssoadmin

import (
	"context"
	"encoding/json"
	"net/http"
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
	require.NoError(t, p.Init(plugin.PluginConfig{DataDir: t.TempDir()}))
	t.Cleanup(func() { p.Shutdown(context.Background()) })
	return p
}

func call(t *testing.T, p *Provider, action string, body map[string]any) *plugin.Response {
	t.Helper()
	b, _ := json.Marshal(body)
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(string(b)))
	req.Header.Set("X-Amz-Target", "SWBExternalService."+action)
	resp, err := p.HandleRequest(context.Background(), action, req)
	require.NoError(t, err)
	return resp
}

func parse(t *testing.T, resp *plugin.Response) map[string]any {
	t.Helper()
	var m map[string]any
	require.NoError(t, json.Unmarshal(resp.Body, &m))
	return m
}

func TestInstanceCRUD(t *testing.T) {
	p := newTestProvider(t)

	// Create
	resp := call(t, p, "CreateInstance", map[string]any{
		"Name": "my-sso-instance",
		"Tags": []map[string]string{{"Key": "env", "Value": "test"}},
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	rb := parse(t, resp)
	instanceARN, _ := rb["InstanceArn"].(string)
	require.NotEmpty(t, instanceARN)

	// Describe
	resp2 := call(t, p, "DescribeInstance", map[string]any{"InstanceArn": instanceARN})
	assert.Equal(t, http.StatusOK, resp2.StatusCode)
	rb2 := parse(t, resp2)
	assert.Equal(t, "my-sso-instance", rb2["Name"])
	assert.Equal(t, "ACTIVE", rb2["Status"])
	assert.NotEmpty(t, rb2["IdentityStoreId"])

	// List
	resp3 := call(t, p, "ListInstances", map[string]any{})
	assert.Equal(t, http.StatusOK, resp3.StatusCode)
	rb3 := parse(t, resp3)
	instances := rb3["Instances"].([]any)
	assert.Len(t, instances, 1)

	// Update
	resp4 := call(t, p, "UpdateInstance", map[string]any{
		"InstanceArn": instanceARN,
		"Name":        "renamed-instance",
	})
	assert.Equal(t, http.StatusOK, resp4.StatusCode)

	resp5 := call(t, p, "DescribeInstance", map[string]any{"InstanceArn": instanceARN})
	rb5 := parse(t, resp5)
	assert.Equal(t, "renamed-instance", rb5["Name"])

	// Delete
	resp6 := call(t, p, "DeleteInstance", map[string]any{"InstanceArn": instanceARN})
	assert.Equal(t, http.StatusOK, resp6.StatusCode)

	// Describe after delete
	resp7 := call(t, p, "DescribeInstance", map[string]any{"InstanceArn": instanceARN})
	assert.Equal(t, http.StatusBadRequest, resp7.StatusCode)
}

func TestPermissionSetCRUD(t *testing.T) {
	p := newTestProvider(t)

	// Create instance first
	instResp := call(t, p, "CreateInstance", map[string]any{"Name": "test-instance"})
	require.Equal(t, http.StatusOK, instResp.StatusCode)
	instanceARN := parse(t, instResp)["InstanceArn"].(string)

	// Create permission set
	resp := call(t, p, "CreatePermissionSet", map[string]any{
		"Name":            "my-perm-set",
		"InstanceArn":     instanceARN,
		"Description":     "test permission set",
		"SessionDuration": "PT2H",
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	rb := parse(t, resp)
	ps := rb["PermissionSet"].(map[string]any)
	psARN, _ := ps["PermissionSetArn"].(string)
	require.NotEmpty(t, psARN)
	assert.Equal(t, "my-perm-set", ps["Name"])
	assert.Equal(t, "PT2H", ps["SessionDuration"])

	// Describe
	resp2 := call(t, p, "DescribePermissionSet", map[string]any{"PermissionSetArn": psARN})
	assert.Equal(t, http.StatusOK, resp2.StatusCode)
	rb2 := parse(t, resp2)
	ps2 := rb2["PermissionSet"].(map[string]any)
	assert.Equal(t, "my-perm-set", ps2["Name"])

	// List
	resp3 := call(t, p, "ListPermissionSets", map[string]any{"InstanceArn": instanceARN})
	assert.Equal(t, http.StatusOK, resp3.StatusCode)
	rb3 := parse(t, resp3)
	sets := rb3["PermissionSets"].([]any)
	assert.Len(t, sets, 1)
	assert.Equal(t, psARN, sets[0])

	// Update
	resp4 := call(t, p, "UpdatePermissionSet", map[string]any{
		"PermissionSetArn": psARN,
		"Description":      "updated description",
		"SessionDuration":  "PT8H",
	})
	assert.Equal(t, http.StatusOK, resp4.StatusCode)

	resp5 := call(t, p, "DescribePermissionSet", map[string]any{"PermissionSetArn": psARN})
	rb5 := parse(t, resp5)
	ps5 := rb5["PermissionSet"].(map[string]any)
	assert.Equal(t, "updated description", ps5["Description"])
	assert.Equal(t, "PT8H", ps5["SessionDuration"])

	// Inline policy
	resp6 := call(t, p, "PutInlinePolicyToPermissionSet", map[string]any{
		"PermissionSetArn": psARN,
		"InlinePolicy":     `{"Version":"2012-10-17"}`,
	})
	assert.Equal(t, http.StatusOK, resp6.StatusCode)

	resp7 := call(t, p, "GetInlinePolicyForPermissionSet", map[string]any{"PermissionSetArn": psARN})
	rb7 := parse(t, resp7)
	assert.Equal(t, `{"Version":"2012-10-17"}`, rb7["InlinePolicy"])

	resp8 := call(t, p, "DeleteInlinePolicyFromPermissionSet", map[string]any{"PermissionSetArn": psARN})
	assert.Equal(t, http.StatusOK, resp8.StatusCode)

	// Managed policies
	respAttach := call(t, p, "AttachManagedPolicyToPermissionSet", map[string]any{
		"PermissionSetArn": psARN,
		"ManagedPolicyArn": "arn:aws:iam::aws:policy/ReadOnlyAccess",
	})
	assert.Equal(t, http.StatusOK, respAttach.StatusCode)

	respList := call(t, p, "ListManagedPoliciesInPermissionSet", map[string]any{"PermissionSetArn": psARN})
	assert.Equal(t, http.StatusOK, respList.StatusCode)
	rbList := parse(t, respList)
	policies := rbList["AttachedManagedPolicies"].([]any)
	assert.Len(t, policies, 1)

	respDetach := call(t, p, "DetachManagedPolicyFromPermissionSet", map[string]any{
		"PermissionSetArn": psARN,
		"ManagedPolicyArn": "arn:aws:iam::aws:policy/ReadOnlyAccess",
	})
	assert.Equal(t, http.StatusOK, respDetach.StatusCode)

	// Provision
	respProv := call(t, p, "ProvisionPermissionSet", map[string]any{
		"PermissionSetArn": psARN,
		"TargetType":       "ALL_PROVISIONED_ACCOUNTS",
	})
	assert.Equal(t, http.StatusOK, respProv.StatusCode)

	// Delete
	resp9 := call(t, p, "DeletePermissionSet", map[string]any{"PermissionSetArn": psARN})
	assert.Equal(t, http.StatusOK, resp9.StatusCode)

	// Describe after delete
	resp10 := call(t, p, "DescribePermissionSet", map[string]any{"PermissionSetArn": psARN})
	assert.Equal(t, http.StatusBadRequest, resp10.StatusCode)
}

func TestAccountAssignmentCRUD(t *testing.T) {
	p := newTestProvider(t)

	// Create instance and permission set
	instARN := parse(t, call(t, p, "CreateInstance", map[string]any{"Name": "inst"}))["InstanceArn"].(string)
	psARN := parse(t, call(t, p, "CreatePermissionSet", map[string]any{
		"Name":        "ps",
		"InstanceArn": instARN,
	}))["PermissionSet"].(map[string]any)["PermissionSetArn"].(string)

	// Create assignment
	resp := call(t, p, "CreateAccountAssignment", map[string]any{
		"InstanceArn":      instARN,
		"PermissionSetArn": psARN,
		"TargetId":         "123456789012",
		"TargetType":       "AWS_ACCOUNT",
		"PrincipalId":      "user-001",
		"PrincipalType":    "USER",
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	rb := parse(t, resp)
	status := rb["AccountAssignmentCreationStatus"].(map[string]any)
	assert.Equal(t, "SUCCEEDED", status["Status"])

	// List assignments
	resp2 := call(t, p, "ListAccountAssignments", map[string]any{
		"InstanceArn":      instARN,
		"PermissionSetArn": psARN,
		"AccountId":        "123456789012",
	})
	assert.Equal(t, http.StatusOK, resp2.StatusCode)
	rb2 := parse(t, resp2)
	assignments := rb2["AccountAssignments"].([]any)
	assert.Len(t, assignments, 1)
	a := assignments[0].(map[string]any)
	assert.Equal(t, "user-001", a["PrincipalId"])
	assert.Equal(t, "USER", a["PrincipalType"])

	// List accounts for permission set
	resp3 := call(t, p, "ListAccountsForProvisionedPermissionSet", map[string]any{
		"InstanceArn":      instARN,
		"PermissionSetArn": psARN,
	})
	assert.Equal(t, http.StatusOK, resp3.StatusCode)
	rb3 := parse(t, resp3)
	accounts := rb3["AccountIds"].([]any)
	assert.Len(t, accounts, 1)
	assert.Equal(t, "123456789012", accounts[0])

	// List permission sets for account
	resp4 := call(t, p, "ListPermissionSetsProvisionedToAccount", map[string]any{
		"InstanceArn": instARN,
		"AccountId":   "123456789012",
	})
	assert.Equal(t, http.StatusOK, resp4.StatusCode)
	rb4 := parse(t, resp4)
	permSets := rb4["PermissionSets"].([]any)
	assert.Len(t, permSets, 1)
	assert.Equal(t, psARN, permSets[0])

	// List for principal
	resp5 := call(t, p, "ListAccountAssignmentsForPrincipal", map[string]any{
		"InstanceArn":   instARN,
		"PrincipalId":   "user-001",
		"PrincipalType": "USER",
	})
	assert.Equal(t, http.StatusOK, resp5.StatusCode)
	rb5 := parse(t, resp5)
	assignments5 := rb5["AccountAssignments"].([]any)
	assert.Len(t, assignments5, 1)

	// Delete assignment
	resp6 := call(t, p, "DeleteAccountAssignment", map[string]any{
		"InstanceArn":      instARN,
		"PermissionSetArn": psARN,
		"TargetId":         "123456789012",
		"PrincipalId":      "user-001",
	})
	assert.Equal(t, http.StatusOK, resp6.StatusCode)

	// Verify deleted
	resp7 := call(t, p, "ListAccountAssignments", map[string]any{
		"InstanceArn":      instARN,
		"PermissionSetArn": psARN,
		"AccountId":        "123456789012",
	})
	rb7 := parse(t, resp7)
	assignments7, _ := rb7["AccountAssignments"].([]any)
	assert.Len(t, assignments7, 0)
}

func TestApplicationCRUD(t *testing.T) {
	p := newTestProvider(t)

	// Create instance
	instARN := parse(t, call(t, p, "CreateInstance", map[string]any{"Name": "inst"}))["InstanceArn"].(string)

	// Create application
	resp := call(t, p, "CreateApplication", map[string]any{
		"Name":                   "my-app",
		"InstanceArn":            instARN,
		"ApplicationProviderArn": "arn:aws:sso::aws:applicationProvider/custom",
		"Description":            "test app",
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	rb := parse(t, resp)
	appARN, _ := rb["ApplicationArn"].(string)
	require.NotEmpty(t, appARN)

	// Describe
	resp2 := call(t, p, "DescribeApplication", map[string]any{"ApplicationArn": appARN})
	assert.Equal(t, http.StatusOK, resp2.StatusCode)
	rb2 := parse(t, resp2)
	assert.Equal(t, "my-app", rb2["Name"])
	assert.Equal(t, "ENABLED", rb2["Status"])

	// List
	resp3 := call(t, p, "ListApplications", map[string]any{"InstanceArn": instARN})
	assert.Equal(t, http.StatusOK, resp3.StatusCode)
	rb3 := parse(t, resp3)
	apps := rb3["Applications"].([]any)
	assert.Len(t, apps, 1)

	// Update
	resp4 := call(t, p, "UpdateApplication", map[string]any{
		"ApplicationArn": appARN,
		"Description":    "updated",
		"Status":         "DISABLED",
	})
	assert.Equal(t, http.StatusOK, resp4.StatusCode)

	resp5 := call(t, p, "DescribeApplication", map[string]any{"ApplicationArn": appARN})
	rb5 := parse(t, resp5)
	assert.Equal(t, "updated", rb5["Description"])
	assert.Equal(t, "DISABLED", rb5["Status"])

	// Delete
	resp6 := call(t, p, "DeleteApplication", map[string]any{"ApplicationArn": appARN})
	assert.Equal(t, http.StatusOK, resp6.StatusCode)

	resp7 := call(t, p, "DescribeApplication", map[string]any{"ApplicationArn": appARN})
	assert.Equal(t, http.StatusBadRequest, resp7.StatusCode)
}

func TestTrustedTokenIssuerCRUD(t *testing.T) {
	p := newTestProvider(t)

	// Create instance
	instARN := parse(t, call(t, p, "CreateInstance", map[string]any{"Name": "inst"}))["InstanceArn"].(string)

	// Create trusted token issuer
	resp := call(t, p, "CreateTrustedTokenIssuer", map[string]any{
		"Name":                   "my-tti",
		"InstanceArn":            instARN,
		"TrustedTokenIssuerType": "OIDC_JWT",
		"TrustedTokenIssuerConfiguration": map[string]any{
			"OidcJwtConfiguration": map[string]any{
				"IssuerUrl":                  "https://example.com",
				"ClaimAttributePath":         "email",
				"IdentityStoreAttributePath": "emails.value",
				"JwksRetrievalOption":        "OPEN_ID_DISCOVERY",
			},
		},
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	rb := parse(t, resp)
	ttiARN, _ := rb["TrustedTokenIssuerArn"].(string)
	require.NotEmpty(t, ttiARN)

	// Describe
	resp2 := call(t, p, "DescribeTrustedTokenIssuer", map[string]any{"TrustedTokenIssuerArn": ttiARN})
	assert.Equal(t, http.StatusOK, resp2.StatusCode)
	rb2 := parse(t, resp2)
	assert.Equal(t, "my-tti", rb2["Name"])
	assert.Equal(t, "OIDC_JWT", rb2["TrustedTokenIssuerType"])

	// List
	resp3 := call(t, p, "ListTrustedTokenIssuers", map[string]any{"InstanceArn": instARN})
	assert.Equal(t, http.StatusOK, resp3.StatusCode)
	rb3 := parse(t, resp3)
	issuers := rb3["TrustedTokenIssuers"].([]any)
	assert.Len(t, issuers, 1)

	// Update
	resp4 := call(t, p, "UpdateTrustedTokenIssuer", map[string]any{
		"TrustedTokenIssuerArn": ttiARN,
		"Name":                  "updated-tti",
	})
	assert.Equal(t, http.StatusOK, resp4.StatusCode)

	resp5 := call(t, p, "DescribeTrustedTokenIssuer", map[string]any{"TrustedTokenIssuerArn": ttiARN})
	rb5 := parse(t, resp5)
	assert.Equal(t, "updated-tti", rb5["Name"])

	// Delete
	resp6 := call(t, p, "DeleteTrustedTokenIssuer", map[string]any{"TrustedTokenIssuerArn": ttiARN})
	assert.Equal(t, http.StatusOK, resp6.StatusCode)

	resp7 := call(t, p, "DescribeTrustedTokenIssuer", map[string]any{"TrustedTokenIssuerArn": ttiARN})
	assert.Equal(t, http.StatusBadRequest, resp7.StatusCode)
}

func TestTags(t *testing.T) {
	p := newTestProvider(t)

	// Create instance with tags
	resp := call(t, p, "CreateInstance", map[string]any{
		"Name": "tagged-instance",
		"Tags": []map[string]string{{"Key": "env", "Value": "prod"}},
	})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	instanceARN := parse(t, resp)["InstanceArn"].(string)

	// ListTagsForResource
	resp2 := call(t, p, "ListTagsForResource", map[string]any{"InstanceArn": instanceARN})
	assert.Equal(t, http.StatusOK, resp2.StatusCode)
	rb2 := parse(t, resp2)
	tags := rb2["Tags"].([]any)
	assert.Len(t, tags, 1)
	assert.Equal(t, "env", tags[0].(map[string]any)["Key"])

	// TagResource
	resp3 := call(t, p, "TagResource", map[string]any{
		"InstanceArn": instanceARN,
		"Tags":        []map[string]string{{"Key": "owner", "Value": "alice"}},
	})
	assert.Equal(t, http.StatusOK, resp3.StatusCode)

	resp4 := call(t, p, "ListTagsForResource", map[string]any{"InstanceArn": instanceARN})
	rb4 := parse(t, resp4)
	tags4 := rb4["Tags"].([]any)
	assert.Len(t, tags4, 2)

	// UntagResource
	resp5 := call(t, p, "UntagResource", map[string]any{
		"InstanceArn": instanceARN,
		"TagKeys":     []string{"env"},
	})
	assert.Equal(t, http.StatusOK, resp5.StatusCode)

	resp6 := call(t, p, "ListTagsForResource", map[string]any{"InstanceArn": instanceARN})
	rb6 := parse(t, resp6)
	tags6 := rb6["Tags"].([]any)
	assert.Len(t, tags6, 1)
	assert.Equal(t, "owner", tags6[0].(map[string]any)["Key"])
}
