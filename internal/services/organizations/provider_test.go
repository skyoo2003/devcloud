// SPDX-License-Identifier: Apache-2.0

// internal/services/organizations/provider_test.go
package organizations

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/skyoo2003/devcloud/internal/plugin"
)

func newTestProvider(t *testing.T) *Provider {
	t.Helper()
	dir := t.TempDir()
	p := &Provider{}
	if err := p.Init(plugin.PluginConfig{DataDir: dir}); err != nil {
		t.Fatalf("Init: %v", err)
	}
	t.Cleanup(func() {
		p.Shutdown(context.Background()) //nolint:errcheck
		_ = os.RemoveAll(dir)
	})
	return p
}

func invoke(t *testing.T, p *Provider, action string, body map[string]any) map[string]any {
	t.Helper()
	b, _ := json.Marshal(body)
	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	resp, err := p.HandleRequest(context.Background(), action, req)
	if err != nil {
		t.Fatalf("%s: unexpected error: %v", action, err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("%s: unexpected status %d: %s", action, resp.StatusCode, resp.Body)
	}
	var out map[string]any
	if err := json.Unmarshal(resp.Body, &out); err != nil {
		t.Fatalf("%s: unmarshal: %v", action, err)
	}
	return out
}

func invokeExpectError(t *testing.T, p *Provider, action string, body map[string]any, expectStatus int) {
	t.Helper()
	b, _ := json.Marshal(body)
	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(b))
	resp, err := p.HandleRequest(context.Background(), action, req)
	if err != nil {
		t.Fatalf("%s: unexpected error: %v", action, err)
	}
	if resp.StatusCode != expectStatus {
		t.Fatalf("%s: expected status %d, got %d: %s", action, expectStatus, resp.StatusCode, resp.Body)
	}
}

// ---- TestOrganizationAndRoot ----

func TestOrganizationAndRoot(t *testing.T) {
	p := newTestProvider(t)

	// CreateOrganization
	resp := invoke(t, p, "CreateOrganization", map[string]any{"FeatureSet": "ALL"})
	org, ok := resp["Organization"].(map[string]any)
	if !ok {
		t.Fatal("expected Organization in response")
	}
	orgID, _ := org["Id"].(string)
	if orgID == "" {
		t.Fatal("expected non-empty organization ID")
	}
	if org["FeatureSet"] != "ALL" {
		t.Errorf("expected FeatureSet=ALL, got %v", org["FeatureSet"])
	}

	// DescribeOrganization
	resp2 := invoke(t, p, "DescribeOrganization", map[string]any{})
	org2, _ := resp2["Organization"].(map[string]any)
	if org2["Id"] != orgID {
		t.Errorf("DescribeOrganization: expected ID %q, got %q", orgID, org2["Id"])
	}

	// Creating again should fail
	invokeExpectError(t, p, "CreateOrganization", map[string]any{"FeatureSet": "ALL"}, http.StatusBadRequest)

	// ListRoots — should have one root
	resp3 := invoke(t, p, "ListRoots", map[string]any{})
	roots, _ := resp3["Roots"].([]any)
	if len(roots) != 1 {
		t.Fatalf("expected 1 root, got %d", len(roots))
	}
	root, _ := roots[0].(map[string]any)
	rootID, _ := root["Id"].(string)
	if rootID == "" {
		t.Fatal("expected non-empty root ID")
	}

	// EnablePolicyType
	resp4 := invoke(t, p, "EnablePolicyType", map[string]any{
		"RootId":     rootID,
		"PolicyType": "SERVICE_CONTROL_POLICY",
	})
	updatedRoot, _ := resp4["Root"].(map[string]any)
	policyTypes, _ := updatedRoot["PolicyTypes"].([]any)
	if len(policyTypes) != 1 {
		t.Errorf("expected 1 policy type, got %d", len(policyTypes))
	}

	// DisablePolicyType
	resp5 := invoke(t, p, "DisablePolicyType", map[string]any{
		"RootId":     rootID,
		"PolicyType": "SERVICE_CONTROL_POLICY",
	})
	updatedRoot2, _ := resp5["Root"].(map[string]any)
	policyTypes2, _ := updatedRoot2["PolicyTypes"].([]any)
	if len(policyTypes2) != 0 {
		t.Errorf("expected 0 policy types after disable, got %d", len(policyTypes2))
	}

	// DeleteOrganization
	invoke(t, p, "DeleteOrganization", map[string]any{})
	invokeExpectError(t, p, "DescribeOrganization", map[string]any{}, http.StatusNotFound)
}

// ---- TestAccountCRUD ----

func TestAccountCRUD(t *testing.T) {
	p := newTestProvider(t)

	// Set up org
	invoke(t, p, "CreateOrganization", map[string]any{"FeatureSet": "ALL"})

	// Get root ID
	resp := invoke(t, p, "ListRoots", map[string]any{})
	roots, _ := resp["Roots"].([]any)
	root, _ := roots[0].(map[string]any)
	rootID, _ := root["Id"].(string)

	// CreateAccount
	resp2 := invoke(t, p, "CreateAccount", map[string]any{
		"AccountName": "dev-account",
		"Email":       "dev@example.com",
	})
	status, _ := resp2["CreateAccountStatus"].(map[string]any)
	accountID, _ := status["AccountId"].(string)
	if accountID == "" {
		t.Fatal("expected non-empty account ID")
	}
	if status["State"] != "SUCCEEDED" {
		t.Errorf("expected SUCCEEDED state, got %v", status["State"])
	}

	// DescribeAccount
	resp3 := invoke(t, p, "DescribeAccount", map[string]any{"AccountId": accountID})
	account, _ := resp3["Account"].(map[string]any)
	if account["Name"] != "dev-account" {
		t.Errorf("expected Name=dev-account, got %v", account["Name"])
	}
	if account["Status"] != "ACTIVE" {
		t.Errorf("expected ACTIVE status, got %v", account["Status"])
	}

	// ListAccounts — should have master + new account
	resp4 := invoke(t, p, "ListAccounts", map[string]any{})
	accounts, _ := resp4["Accounts"].([]any)
	if len(accounts) < 2 {
		t.Errorf("expected at least 2 accounts, got %d", len(accounts))
	}

	// ListAccountsForParent (root)
	resp5 := invoke(t, p, "ListAccountsForParent", map[string]any{"ParentId": rootID})
	accountsForParent, _ := resp5["Accounts"].([]any)
	if len(accountsForParent) == 0 {
		t.Error("expected accounts for parent root")
	}

	// CloseAccount
	invoke(t, p, "CloseAccount", map[string]any{"AccountId": accountID})
	resp6 := invoke(t, p, "DescribeAccount", map[string]any{"AccountId": accountID})
	account2, _ := resp6["Account"].(map[string]any)
	if account2["Status"] != "SUSPENDED" {
		t.Errorf("expected SUSPENDED after close, got %v", account2["Status"])
	}

	// MoveAccount
	invoke(t, p, "MoveAccount", map[string]any{
		"AccountId":           accountID,
		"SourceParentId":      rootID,
		"DestinationParentId": rootID,
	})

	// RemoveAccountFromOrganization
	invoke(t, p, "RemoveAccountFromOrganization", map[string]any{"AccountId": accountID})
	invokeExpectError(t, p, "DescribeAccount", map[string]any{"AccountId": accountID}, http.StatusNotFound)
}

// ---- TestOUCRUD ----

func TestOUCRUD(t *testing.T) {
	p := newTestProvider(t)

	invoke(t, p, "CreateOrganization", map[string]any{"FeatureSet": "ALL"})

	// Get root ID
	resp := invoke(t, p, "ListRoots", map[string]any{})
	roots, _ := resp["Roots"].([]any)
	root, _ := roots[0].(map[string]any)
	rootID, _ := root["Id"].(string)

	// CreateOrganizationalUnit
	resp2 := invoke(t, p, "CreateOrganizationalUnit", map[string]any{
		"ParentId": rootID,
		"Name":     "Engineering",
	})
	ou, _ := resp2["OrganizationalUnit"].(map[string]any)
	ouID, _ := ou["Id"].(string)
	if ouID == "" {
		t.Fatal("expected non-empty OU ID")
	}
	if ou["Name"] != "Engineering" {
		t.Errorf("expected Name=Engineering, got %v", ou["Name"])
	}

	// DescribeOrganizationalUnit
	resp3 := invoke(t, p, "DescribeOrganizationalUnit", map[string]any{"OrganizationalUnitId": ouID})
	ou2, _ := resp3["OrganizationalUnit"].(map[string]any)
	if ou2["Id"] != ouID {
		t.Errorf("DescribeOrganizationalUnit: ID mismatch")
	}

	// ListOrganizationalUnitsForParent
	resp4 := invoke(t, p, "ListOrganizationalUnitsForParent", map[string]any{"ParentId": rootID})
	ous, _ := resp4["OrganizationalUnits"].([]any)
	if len(ous) != 1 {
		t.Errorf("expected 1 OU, got %d", len(ous))
	}

	// UpdateOrganizationalUnit
	resp5 := invoke(t, p, "UpdateOrganizationalUnit", map[string]any{
		"OrganizationalUnitId": ouID,
		"Name":                 "Engineering-Updated",
	})
	ou3, _ := resp5["OrganizationalUnit"].(map[string]any)
	if ou3["Name"] != "Engineering-Updated" {
		t.Errorf("expected updated name, got %v", ou3["Name"])
	}

	// ListChildren
	resp6 := invoke(t, p, "ListChildren", map[string]any{
		"ParentId":  rootID,
		"ChildType": "ORGANIZATIONAL_UNIT",
	})
	children, _ := resp6["Children"].([]any)
	if len(children) != 1 {
		t.Errorf("expected 1 OU child, got %d", len(children))
	}

	// ListParents for OU
	resp7 := invoke(t, p, "ListParents", map[string]any{"ChildId": ouID})
	parents, _ := resp7["Parents"].([]any)
	if len(parents) != 1 {
		t.Fatalf("expected 1 parent, got %d", len(parents))
	}
	parent, _ := parents[0].(map[string]any)
	if parent["Id"] != rootID {
		t.Errorf("expected parent ID %q, got %q", rootID, parent["Id"])
	}

	// DeleteOrganizationalUnit
	invoke(t, p, "DeleteOrganizationalUnit", map[string]any{"OrganizationalUnitId": ouID})
	invokeExpectError(t, p, "DescribeOrganizationalUnit", map[string]any{"OrganizationalUnitId": ouID}, http.StatusNotFound)
}

// ---- TestPolicyCRUD ----

func TestPolicyCRUD(t *testing.T) {
	p := newTestProvider(t)

	invoke(t, p, "CreateOrganization", map[string]any{"FeatureSet": "ALL"})

	content := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`

	// CreatePolicy
	resp := invoke(t, p, "CreatePolicy", map[string]any{
		"Name":        "AllowAll",
		"Type":        "SERVICE_CONTROL_POLICY",
		"Description": "Allow all",
		"Content":     content,
	})
	policy, _ := resp["Policy"].(map[string]any)
	summary, _ := policy["PolicySummary"].(map[string]any)
	policyID, _ := summary["Id"].(string)
	if policyID == "" {
		t.Fatal("expected non-empty policy ID")
	}
	if summary["Name"] != "AllowAll" {
		t.Errorf("expected Name=AllowAll, got %v", summary["Name"])
	}

	// DescribePolicy
	resp2 := invoke(t, p, "DescribePolicy", map[string]any{"PolicyId": policyID})
	policy2, _ := resp2["Policy"].(map[string]any)
	if policy2["Content"] != content {
		t.Errorf("content mismatch")
	}

	// ListPolicies
	resp3 := invoke(t, p, "ListPolicies", map[string]any{"Filter": "SERVICE_CONTROL_POLICY"})
	policies, _ := resp3["Policies"].([]any)
	if len(policies) != 1 {
		t.Errorf("expected 1 policy, got %d", len(policies))
	}

	// UpdatePolicy
	newContent := `{"Version":"2012-10-17","Statement":[]}`
	resp4 := invoke(t, p, "UpdatePolicy", map[string]any{
		"PolicyId":    policyID,
		"Name":        "AllowAll-Updated",
		"Description": "Updated",
		"Content":     newContent,
	})
	policy3, _ := resp4["Policy"].(map[string]any)
	summary3, _ := policy3["PolicySummary"].(map[string]any)
	if summary3["Name"] != "AllowAll-Updated" {
		t.Errorf("expected updated name, got %v", summary3["Name"])
	}

	// DeletePolicy
	invoke(t, p, "DeletePolicy", map[string]any{"PolicyId": policyID})
	invokeExpectError(t, p, "DescribePolicy", map[string]any{"PolicyId": policyID}, http.StatusNotFound)
}

// ---- TestPolicyAttachment ----

func TestPolicyAttachment(t *testing.T) {
	p := newTestProvider(t)

	invoke(t, p, "CreateOrganization", map[string]any{"FeatureSet": "ALL"})

	// Get root ID
	resp := invoke(t, p, "ListRoots", map[string]any{})
	roots, _ := resp["Roots"].([]any)
	root, _ := roots[0].(map[string]any)
	rootID, _ := root["Id"].(string)

	content := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`
	resp2 := invoke(t, p, "CreatePolicy", map[string]any{
		"Name":    "TestPolicy",
		"Type":    "SERVICE_CONTROL_POLICY",
		"Content": content,
	})
	policy, _ := resp2["Policy"].(map[string]any)
	summary, _ := policy["PolicySummary"].(map[string]any)
	policyID, _ := summary["Id"].(string)

	// AttachPolicy
	invoke(t, p, "AttachPolicy", map[string]any{
		"PolicyId": policyID,
		"TargetId": rootID,
	})

	// ListPoliciesForTarget
	resp3 := invoke(t, p, "ListPoliciesForTarget", map[string]any{
		"TargetId": rootID,
		"Filter":   "SERVICE_CONTROL_POLICY",
	})
	policies, _ := resp3["Policies"].([]any)
	if len(policies) != 1 {
		t.Errorf("expected 1 policy for target, got %d", len(policies))
	}

	// ListTargetsForPolicy
	resp4 := invoke(t, p, "ListTargetsForPolicy", map[string]any{"PolicyId": policyID})
	targets, _ := resp4["Targets"].([]any)
	if len(targets) != 1 {
		t.Fatalf("expected 1 target, got %d", len(targets))
	}
	tgt, _ := targets[0].(map[string]any)
	if tgt["TargetId"] != rootID {
		t.Errorf("expected targetId %q, got %q", rootID, tgt["TargetId"])
	}

	// Cannot delete a policy with attachments
	invokeExpectError(t, p, "DeletePolicy", map[string]any{"PolicyId": policyID}, http.StatusBadRequest)

	// DetachPolicy
	invoke(t, p, "DetachPolicy", map[string]any{
		"PolicyId": policyID,
		"TargetId": rootID,
	})

	// Now delete should succeed
	invoke(t, p, "DeletePolicy", map[string]any{"PolicyId": policyID})
}

// ---- TestTags ----

func TestTags(t *testing.T) {
	p := newTestProvider(t)

	invoke(t, p, "CreateOrganization", map[string]any{"FeatureSet": "ALL"})

	resp := invoke(t, p, "ListRoots", map[string]any{})
	roots, _ := resp["Roots"].([]any)
	root, _ := roots[0].(map[string]any)
	rootID, _ := root["Id"].(string)

	// TagResource
	invoke(t, p, "TagResource", map[string]any{
		"ResourceId": rootID,
		"Tags": []any{
			map[string]any{"Key": "Env", "Value": "prod"},
			map[string]any{"Key": "Team", "Value": "platform"},
		},
	})

	// ListTagsForResource
	resp2 := invoke(t, p, "ListTagsForResource", map[string]any{"ResourceId": rootID})
	tags, _ := resp2["Tags"].([]any)
	if len(tags) != 2 {
		t.Errorf("expected 2 tags, got %d", len(tags))
	}

	// UntagResource
	invoke(t, p, "UntagResource", map[string]any{
		"ResourceId": rootID,
		"TagKeys":    []any{"Env"},
	})

	resp3 := invoke(t, p, "ListTagsForResource", map[string]any{"ResourceId": rootID})
	tags2, _ := resp3["Tags"].([]any)
	if len(tags2) != 1 {
		t.Errorf("expected 1 tag after untag, got %d", len(tags2))
	}
	tag, _ := tags2[0].(map[string]any)
	if tag["Key"] != "Team" {
		t.Errorf("expected remaining tag Key=Team, got %v", tag["Key"])
	}
}
