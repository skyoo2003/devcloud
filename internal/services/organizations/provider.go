// SPDX-License-Identifier: Apache-2.0

// internal/services/organizations/provider.go
package organizations

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"strings"

	"github.com/skyoo2003/devcloud/internal/plugin"
	"github.com/skyoo2003/devcloud/internal/shared"
)

type Provider struct {
	store *Store
}

func (p *Provider) ServiceID() string             { return "organizations" }
func (p *Provider) ServiceName() string           { return "OrganizationsV20161128" }
func (p *Provider) Protocol() plugin.ProtocolType { return plugin.ProtocolJSON11 }

func (p *Provider) Init(cfg plugin.PluginConfig) error {
	dataDir := cfg.DataDir
	if dataDir == "" {
		dataDir = "."
	}
	var err error
	p.store, err = NewStore(filepath.Join(dataDir, "organizations"))
	return err
}

func (p *Provider) Shutdown(_ context.Context) error {
	if p.store != nil {
		return p.store.Close()
	}
	return nil
}

func (p *Provider) HandleRequest(_ context.Context, op string, req *http.Request) (*plugin.Response, error) {
	body, err := io.ReadAll(req.Body)
	if err != nil {
		return shared.JSONError("SerializationException", "failed to read body", http.StatusBadRequest), nil
	}
	var params map[string]any
	if len(body) > 0 {
		if err := json.Unmarshal(body, &params); err != nil {
			return shared.JSONError("SerializationException", "invalid JSON", http.StatusBadRequest), nil
		}
	} else {
		params = map[string]any{}
	}

	action := op
	if action == "" {
		target := req.Header.Get("X-Amz-Target")
		if idx := strings.LastIndex(target, "."); idx >= 0 {
			action = target[idx+1:]
		}
	}

	switch action {
	// Organization
	case "CreateOrganization":
		return p.createOrganization(params)
	case "DescribeOrganization":
		return p.describeOrganization(params)
	case "DeleteOrganization":
		return p.deleteOrganization(params)
	// Root
	case "ListRoots":
		return p.listRoots(params)
	case "EnablePolicyType":
		return p.enablePolicyType(params)
	case "DisablePolicyType":
		return p.disablePolicyType(params)
	// Account
	case "CreateAccount":
		return p.createAccount(params)
	case "DescribeAccount":
		return p.describeAccount(params)
	case "ListAccounts":
		return p.listAccounts(params)
	case "ListAccountsForParent":
		return p.listAccountsForParent(params)
	case "CloseAccount":
		return p.closeAccount(params)
	case "MoveAccount":
		return p.moveAccount(params)
	case "LeaveOrganization":
		return p.leaveOrganization(params)
	case "RemoveAccountFromOrganization":
		return p.removeAccountFromOrganization(params)
	// OU
	case "CreateOrganizationalUnit":
		return p.createOrganizationalUnit(params)
	case "DescribeOrganizationalUnit":
		return p.describeOrganizationalUnit(params)
	case "ListOrganizationalUnitsForParent":
		return p.listOrganizationalUnitsForParent(params)
	case "UpdateOrganizationalUnit":
		return p.updateOrganizationalUnit(params)
	case "DeleteOrganizationalUnit":
		return p.deleteOrganizationalUnit(params)
	// Tree navigation
	case "ListChildren":
		return p.listChildren(params)
	case "ListParents":
		return p.listParents(params)
	// Policy
	case "CreatePolicy":
		return p.createPolicy(params)
	case "DescribePolicy":
		return p.describePolicy(params)
	case "ListPolicies":
		return p.listPolicies(params)
	case "UpdatePolicy":
		return p.updatePolicy(params)
	case "DeletePolicy":
		return p.deletePolicy(params)
	case "AttachPolicy":
		return p.attachPolicy(params)
	case "DetachPolicy":
		return p.detachPolicy(params)
	case "ListPoliciesForTarget":
		return p.listPoliciesForTarget(params)
	case "ListTargetsForPolicy":
		return p.listTargetsForPolicy(params)
	// Tags
	case "TagResource":
		return p.tagResource(params)
	case "UntagResource":
		return p.untagResource(params)
	case "ListTagsForResource":
		return p.listTagsForResource(params)
	// Stub operations — return success/empty
	case "EnableAllFeatures",
		"EnableAWSServiceAccess", "DisableAWSServiceAccess", "ListAWSServiceAccessForOrganization",
		"InviteAccountToOrganization",
		"ListHandshakesForAccount", "ListHandshakesForOrganization",
		"AcceptHandshake", "DeclineHandshake", "CancelHandshake", "DescribeHandshake",
		"RegisterDelegatedAdministrator", "DeregisterDelegatedAdministrator",
		"ListDelegatedAdministrators", "ListDelegatedServicesForAccount",
		"CreateGovCloudAccount",
		"DescribeCreateAccountStatus", "ListCreateAccountStatus",
		"DescribeEffectivePolicy",
		"DescribeResourcePolicy", "PutResourcePolicy", "DeleteResourcePolicy":
		return p.stubSuccess(action)
	default:
		return shared.JSONError("InvalidAction", fmt.Sprintf("unknown action: %s", action), http.StatusBadRequest), nil
	}
}

func (p *Provider) ListResources(_ context.Context) ([]plugin.Resource, error) {
	accounts, err := p.store.ListAccounts()
	if err != nil {
		return nil, err
	}
	res := make([]plugin.Resource, 0, len(accounts))
	for _, a := range accounts {
		res = append(res, plugin.Resource{Type: "account", ID: a.ID, Name: a.Name})
	}
	return res, nil
}

func (p *Provider) GetMetrics(_ context.Context) (*plugin.ServiceMetrics, error) {
	return &plugin.ServiceMetrics{}, nil
}

// ---- Organization ----

func (p *Provider) createOrganization(params map[string]any) (*plugin.Response, error) {
	// Check if already exists
	if _, err := p.store.GetOrganization(); err == nil {
		return shared.JSONError("AlreadyInOrganizationException", "organization already exists", http.StatusBadRequest), nil
	}

	featureSet, _ := params["FeatureSet"].(string)
	if featureSet == "" {
		featureSet = "ALL"
	}

	orgID := "o-" + shared.GenerateID("", 10)
	masterID := shared.DefaultAccountID
	masterARN := fmt.Sprintf("arn:aws:organizations::%s:account/%s/%s", masterID, orgID, masterID)
	orgARN := fmt.Sprintf("arn:aws:organizations::%s:organization/%s", masterID, orgID)

	org, err := p.store.CreateOrganization(orgID, orgARN, masterID, masterARN, "admin@example.com", featureSet)
	if err != nil {
		return nil, err
	}

	// Auto-create root
	rootID := "r-" + shared.GenerateID("", 4)
	rootARN := fmt.Sprintf("arn:aws:organizations::%s:root/%s/%s", masterID, orgID, rootID)
	if _, err := p.store.CreateRoot(rootID, rootARN, "Root"); err != nil {
		return nil, err
	}

	// Auto-create master account
	accountARN := fmt.Sprintf("arn:aws:organizations::%s:account/%s/%s", masterID, orgID, masterID)
	if _, err := p.store.CreateAccount(masterID, accountARN, "master", "admin@example.com", "CREATED", rootID); err != nil {
		return nil, err
	}

	return shared.JSONResponse(http.StatusOK, map[string]any{
		"Organization": orgToMap(org),
	})
}

func (p *Provider) describeOrganization(_ map[string]any) (*plugin.Response, error) {
	org, err := p.store.GetOrganization()
	if err != nil {
		return shared.JSONError("AWSOrganizationsNotInUseException", "organization does not exist", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"Organization": orgToMap(org),
	})
}

func (p *Provider) deleteOrganization(_ map[string]any) (*plugin.Response, error) {
	if err := p.store.DeleteOrganization(); err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

// ---- Root ----

func (p *Provider) listRoots(_ map[string]any) (*plugin.Response, error) {
	roots, err := p.store.ListRoots()
	if err != nil {
		return nil, err
	}
	rootList := make([]map[string]any, 0, len(roots))
	for _, r := range roots {
		rootList = append(rootList, rootToMap(&r))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"Roots": rootList,
	})
}

func (p *Provider) enablePolicyType(params map[string]any) (*plugin.Response, error) {
	rootID, _ := params["RootId"].(string)
	policyType, _ := params["PolicyType"].(string)
	if rootID == "" || policyType == "" {
		return shared.JSONError("InvalidInputException", "RootId and PolicyType are required", http.StatusBadRequest), nil
	}
	root, err := p.store.GetRoot()
	if err != nil {
		return shared.JSONError("RootNotFoundException", "root not found", http.StatusNotFound), nil
	}
	types := parseJSONStringArray(root.PolicyTypes)
	for _, t := range types {
		if t == policyType {
			return shared.JSONError("PolicyTypeAlreadyEnabledException", "policy type already enabled", http.StatusBadRequest), nil
		}
	}
	types = append(types, policyType)
	newJSON, _ := json.Marshal(types)
	if err := p.store.UpdateRootPolicyTypes(root.ID, string(newJSON)); err != nil {
		return nil, err
	}
	root.PolicyTypes = string(newJSON)
	return shared.JSONResponse(http.StatusOK, map[string]any{"Root": rootToMap(root)})
}

func (p *Provider) disablePolicyType(params map[string]any) (*plugin.Response, error) {
	rootID, _ := params["RootId"].(string)
	policyType, _ := params["PolicyType"].(string)
	if rootID == "" || policyType == "" {
		return shared.JSONError("InvalidInputException", "RootId and PolicyType are required", http.StatusBadRequest), nil
	}
	root, err := p.store.GetRoot()
	if err != nil {
		return shared.JSONError("RootNotFoundException", "root not found", http.StatusNotFound), nil
	}
	types := parseJSONStringArray(root.PolicyTypes)
	newTypes := make([]string, 0, len(types))
	for _, t := range types {
		if t != policyType {
			newTypes = append(newTypes, t)
		}
	}
	newJSON, _ := json.Marshal(newTypes)
	if err := p.store.UpdateRootPolicyTypes(root.ID, string(newJSON)); err != nil {
		return nil, err
	}
	root.PolicyTypes = string(newJSON)
	return shared.JSONResponse(http.StatusOK, map[string]any{"Root": rootToMap(root)})
}

// ---- Account ----

func (p *Provider) createAccount(params map[string]any) (*plugin.Response, error) {
	org, err := p.store.GetOrganization()
	if err != nil {
		return shared.JSONError("AWSOrganizationsNotInUseException", "organization does not exist", http.StatusNotFound), nil
	}

	name, _ := params["AccountName"].(string)
	email, _ := params["Email"].(string)
	if name == "" || email == "" {
		return shared.JSONError("InvalidInputException", "AccountName and Email are required", http.StatusBadRequest), nil
	}

	// Find root to use as parent
	root, err := p.store.GetRoot()
	if err != nil {
		return shared.JSONError("RootNotFoundException", "root not found", http.StatusNotFound), nil
	}

	accountID := generateAccountID()
	orgID := org.ID
	accountARN := fmt.Sprintf("arn:aws:organizations::%s:account/%s/%s", org.MasterAccountID, orgID, accountID)

	account, err := p.store.CreateAccount(accountID, accountARN, name, email, "CREATED", root.ID)
	if err != nil {
		if isUniqueErr(err) {
			return shared.JSONError("DuplicateAccountException", "account already exists", http.StatusBadRequest), nil
		}
		return nil, err
	}

	statusID := shared.GenerateUUID()
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"CreateAccountStatus": map[string]any{
			"Id":                 statusID,
			"AccountId":          account.ID,
			"AccountName":        account.Name,
			"State":              "SUCCEEDED",
			"RequestedTimestamp": account.CreatedAt.Unix(),
			"CompletedTimestamp": account.CreatedAt.Unix(),
		},
	})
}

func (p *Provider) describeAccount(params map[string]any) (*plugin.Response, error) {
	id, _ := params["AccountId"].(string)
	if id == "" {
		return shared.JSONError("InvalidInputException", "AccountId is required", http.StatusBadRequest), nil
	}
	account, err := p.store.GetAccount(id)
	if err != nil {
		return shared.JSONError("AccountNotFoundException", "account not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"Account": accountToMap(account),
	})
}

func (p *Provider) listAccounts(_ map[string]any) (*plugin.Response, error) {
	accounts, err := p.store.ListAccounts()
	if err != nil {
		return nil, err
	}
	list := make([]map[string]any, 0, len(accounts))
	for i := range accounts {
		list = append(list, accountToMap(&accounts[i]))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"Accounts": list,
	})
}

func (p *Provider) listAccountsForParent(params map[string]any) (*plugin.Response, error) {
	parentID, _ := params["ParentId"].(string)
	if parentID == "" {
		return shared.JSONError("InvalidInputException", "ParentId is required", http.StatusBadRequest), nil
	}
	accounts, err := p.store.ListAccountsForParent(parentID)
	if err != nil {
		return nil, err
	}
	list := make([]map[string]any, 0, len(accounts))
	for i := range accounts {
		list = append(list, accountToMap(&accounts[i]))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"Accounts": list,
	})
}

func (p *Provider) closeAccount(params map[string]any) (*plugin.Response, error) {
	id, _ := params["AccountId"].(string)
	if id == "" {
		return shared.JSONError("InvalidInputException", "AccountId is required", http.StatusBadRequest), nil
	}
	if _, err := p.store.GetAccount(id); err != nil {
		return shared.JSONError("AccountNotFoundException", "account not found", http.StatusNotFound), nil
	}
	if err := p.store.UpdateAccountStatus(id, "SUSPENDED"); err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) moveAccount(params map[string]any) (*plugin.Response, error) {
	accountID, _ := params["AccountId"].(string)
	destParentID, _ := params["DestinationParentId"].(string)
	if accountID == "" || destParentID == "" {
		return shared.JSONError("InvalidInputException", "AccountId and DestinationParentId are required", http.StatusBadRequest), nil
	}
	if _, err := p.store.GetAccount(accountID); err != nil {
		return shared.JSONError("AccountNotFoundException", "account not found", http.StatusNotFound), nil
	}
	if err := p.store.UpdateAccountParent(accountID, destParentID); err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) leaveOrganization(_ map[string]any) (*plugin.Response, error) {
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) removeAccountFromOrganization(params map[string]any) (*plugin.Response, error) {
	id, _ := params["AccountId"].(string)
	if id == "" {
		return shared.JSONError("InvalidInputException", "AccountId is required", http.StatusBadRequest), nil
	}
	if _, err := p.store.GetAccount(id); err != nil {
		return shared.JSONError("AccountNotFoundException", "account not found", http.StatusNotFound), nil
	}
	if err := p.store.DeleteAccount(id); err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

// ---- OU ----

func (p *Provider) createOrganizationalUnit(params map[string]any) (*plugin.Response, error) {
	org, err := p.store.GetOrganization()
	if err != nil {
		return shared.JSONError("AWSOrganizationsNotInUseException", "organization does not exist", http.StatusNotFound), nil
	}

	parentID, _ := params["ParentId"].(string)
	name, _ := params["Name"].(string)
	if parentID == "" || name == "" {
		return shared.JSONError("InvalidInputException", "ParentId and Name are required", http.StatusBadRequest), nil
	}

	ouID := "ou-" + shared.GenerateID("", 14)
	orgID := org.ID
	ouARN := fmt.Sprintf("arn:aws:organizations::%s:ou/%s/%s", org.MasterAccountID, orgID, ouID)

	ou, err := p.store.CreateOU(ouID, ouARN, name, parentID)
	if err != nil {
		if isUniqueErr(err) {
			return shared.JSONError("DuplicateOrganizationalUnitException", "OU already exists", http.StatusBadRequest), nil
		}
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"OrganizationalUnit": ouToMap(ou),
	})
}

func (p *Provider) describeOrganizationalUnit(params map[string]any) (*plugin.Response, error) {
	id, _ := params["OrganizationalUnitId"].(string)
	if id == "" {
		return shared.JSONError("InvalidInputException", "OrganizationalUnitId is required", http.StatusBadRequest), nil
	}
	ou, err := p.store.GetOU(id)
	if err != nil {
		return shared.JSONError("OrganizationalUnitNotFoundException", "OU not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"OrganizationalUnit": ouToMap(ou),
	})
}

func (p *Provider) listOrganizationalUnitsForParent(params map[string]any) (*plugin.Response, error) {
	parentID, _ := params["ParentId"].(string)
	if parentID == "" {
		return shared.JSONError("InvalidInputException", "ParentId is required", http.StatusBadRequest), nil
	}
	ous, err := p.store.ListOUsForParent(parentID)
	if err != nil {
		return nil, err
	}
	list := make([]map[string]any, 0, len(ous))
	for i := range ous {
		list = append(list, ouToMap(&ous[i]))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"OrganizationalUnits": list,
	})
}

func (p *Provider) updateOrganizationalUnit(params map[string]any) (*plugin.Response, error) {
	id, _ := params["OrganizationalUnitId"].(string)
	name, _ := params["Name"].(string)
	if id == "" {
		return shared.JSONError("InvalidInputException", "OrganizationalUnitId is required", http.StatusBadRequest), nil
	}
	ou, err := p.store.GetOU(id)
	if err != nil {
		return shared.JSONError("OrganizationalUnitNotFoundException", "OU not found", http.StatusNotFound), nil
	}
	if name != "" {
		ou.Name = name
	}
	if err := p.store.UpdateOU(id, ou.Name); err != nil {
		return nil, err
	}
	ou, _ = p.store.GetOU(id)
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"OrganizationalUnit": ouToMap(ou),
	})
}

func (p *Provider) deleteOrganizationalUnit(params map[string]any) (*plugin.Response, error) {
	id, _ := params["OrganizationalUnitId"].(string)
	if id == "" {
		return shared.JSONError("InvalidInputException", "OrganizationalUnitId is required", http.StatusBadRequest), nil
	}
	hasChildren, err := p.store.OUHasChildren(id)
	if err != nil {
		return nil, err
	}
	if hasChildren {
		return shared.JSONError("OrganizationalUnitNotEmptyException", "OU is not empty", http.StatusBadRequest), nil
	}
	if err := p.store.DeleteOU(id); err != nil {
		return shared.JSONError("OrganizationalUnitNotFoundException", "OU not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

// ---- Tree navigation ----

func (p *Provider) listChildren(params map[string]any) (*plugin.Response, error) {
	parentID, _ := params["ParentId"].(string)
	childType, _ := params["ChildType"].(string)
	if parentID == "" || childType == "" {
		return shared.JSONError("InvalidInputException", "ParentId and ChildType are required", http.StatusBadRequest), nil
	}

	var children []map[string]any

	switch childType {
	case "ACCOUNT":
		accounts, err := p.store.ListAccountsForParent(parentID)
		if err != nil {
			return nil, err
		}
		for _, a := range accounts {
			children = append(children, map[string]any{"Id": a.ID, "Type": "ACCOUNT"})
		}
	case "ORGANIZATIONAL_UNIT":
		ous, err := p.store.ListOUsForParent(parentID)
		if err != nil {
			return nil, err
		}
		for _, ou := range ous {
			children = append(children, map[string]any{"Id": ou.ID, "Type": "ORGANIZATIONAL_UNIT"})
		}
	default:
		return shared.JSONError("InvalidInputException", "ChildType must be ACCOUNT or ORGANIZATIONAL_UNIT", http.StatusBadRequest), nil
	}

	if children == nil {
		children = []map[string]any{}
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"Children": children})
}

func (p *Provider) listParents(params map[string]any) (*plugin.Response, error) {
	childID, _ := params["ChildId"].(string)
	if childID == "" {
		return shared.JSONError("InvalidInputException", "ChildId is required", http.StatusBadRequest), nil
	}

	var parentID string
	var parentType string

	// Check if it's an account
	if account, err := p.store.GetAccount(childID); err == nil {
		parentID = account.ParentID
		// Determine type of parent
		if strings.HasPrefix(parentID, "r-") {
			parentType = "ROOT"
		} else {
			parentType = "ORGANIZATIONAL_UNIT"
		}
	} else if ou, err := p.store.GetOU(childID); err == nil {
		parentID = ou.ParentID
		if strings.HasPrefix(parentID, "r-") {
			parentType = "ROOT"
		} else {
			parentType = "ORGANIZATIONAL_UNIT"
		}
	} else {
		return shared.JSONError("ChildNotFoundException", "child not found", http.StatusNotFound), nil
	}

	parents := []map[string]any{}
	if parentID != "" {
		parents = append(parents, map[string]any{"Id": parentID, "Type": parentType})
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"Parents": parents})
}

// ---- Policy ----

func (p *Provider) createPolicy(params map[string]any) (*plugin.Response, error) {
	if _, err := p.store.GetOrganization(); err != nil {
		return shared.JSONError("AWSOrganizationsNotInUseException", "organization does not exist", http.StatusNotFound), nil
	}

	name, _ := params["Name"].(string)
	content, _ := params["Content"].(string)
	policyType, _ := params["Type"].(string)
	description, _ := params["Description"].(string)

	if name == "" || content == "" || policyType == "" {
		return shared.JSONError("InvalidInputException", "Name, Content, and Type are required", http.StatusBadRequest), nil
	}

	policyID := "p-" + shared.GenerateID("", 10)
	org, _ := p.store.GetOrganization()
	policyARN := fmt.Sprintf("arn:aws:organizations::%s:policy/%s/%s/%s",
		org.MasterAccountID, org.ID, strings.ToLower(policyType), policyID)

	policy, err := p.store.CreatePolicy(policyID, policyARN, name, policyType, description, content)
	if err != nil {
		if isUniqueErr(err) {
			return shared.JSONError("DuplicatePolicyException", "policy already exists", http.StatusBadRequest), nil
		}
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"Policy": policyToMap(policy),
	})
}

func (p *Provider) describePolicy(params map[string]any) (*plugin.Response, error) {
	id, _ := params["PolicyId"].(string)
	if id == "" {
		return shared.JSONError("InvalidInputException", "PolicyId is required", http.StatusBadRequest), nil
	}
	policy, err := p.store.GetPolicy(id)
	if err != nil {
		return shared.JSONError("PolicyNotFoundException", "policy not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"Policy": policyToMap(policy),
	})
}

func (p *Provider) listPolicies(params map[string]any) (*plugin.Response, error) {
	policyType, _ := params["Filter"].(string)
	policies, err := p.store.ListPolicies(policyType)
	if err != nil {
		return nil, err
	}
	list := make([]map[string]any, 0, len(policies))
	for i := range policies {
		list = append(list, policySummaryToMap(&policies[i]))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"Policies": list,
	})
}

func (p *Provider) updatePolicy(params map[string]any) (*plugin.Response, error) {
	id, _ := params["PolicyId"].(string)
	if id == "" {
		return shared.JSONError("InvalidInputException", "PolicyId is required", http.StatusBadRequest), nil
	}
	policy, err := p.store.GetPolicy(id)
	if err != nil {
		return shared.JSONError("PolicyNotFoundException", "policy not found", http.StatusNotFound), nil
	}

	name, _ := params["Name"].(string)
	description, _ := params["Description"].(string)
	content, _ := params["Content"].(string)

	if name == "" {
		name = policy.Name
	}
	if description == "" {
		description = policy.Description
	}
	if content == "" {
		content = policy.Content
	}

	if err := p.store.UpdatePolicy(id, name, description, content); err != nil {
		return nil, err
	}
	policy, _ = p.store.GetPolicy(id)
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"Policy": policyToMap(policy),
	})
}

func (p *Provider) deletePolicy(params map[string]any) (*plugin.Response, error) {
	id, _ := params["PolicyId"].(string)
	if id == "" {
		return shared.JSONError("InvalidInputException", "PolicyId is required", http.StatusBadRequest), nil
	}
	count, err := p.store.PolicyAttachmentCount(id)
	if err != nil {
		return nil, err
	}
	if count > 0 {
		return shared.JSONError("PolicyInUseException", "policy is still attached to targets", http.StatusBadRequest), nil
	}
	if err := p.store.DeletePolicy(id); err != nil {
		return shared.JSONError("PolicyNotFoundException", "policy not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) attachPolicy(params map[string]any) (*plugin.Response, error) {
	policyID, _ := params["PolicyId"].(string)
	targetID, _ := params["TargetId"].(string)
	if policyID == "" || targetID == "" {
		return shared.JSONError("InvalidInputException", "PolicyId and TargetId are required", http.StatusBadRequest), nil
	}
	if _, err := p.store.GetPolicy(policyID); err != nil {
		return shared.JSONError("PolicyNotFoundException", "policy not found", http.StatusNotFound), nil
	}
	if err := p.store.AttachPolicy(policyID, targetID); err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) detachPolicy(params map[string]any) (*plugin.Response, error) {
	policyID, _ := params["PolicyId"].(string)
	targetID, _ := params["TargetId"].(string)
	if policyID == "" || targetID == "" {
		return shared.JSONError("InvalidInputException", "PolicyId and TargetId are required", http.StatusBadRequest), nil
	}
	if err := p.store.DetachPolicy(policyID, targetID); err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) listPoliciesForTarget(params map[string]any) (*plugin.Response, error) {
	targetID, _ := params["TargetId"].(string)
	policyType, _ := params["Filter"].(string)
	if targetID == "" {
		return shared.JSONError("InvalidInputException", "TargetId is required", http.StatusBadRequest), nil
	}
	policies, err := p.store.ListPoliciesForTarget(targetID, policyType)
	if err != nil {
		return nil, err
	}
	list := make([]map[string]any, 0, len(policies))
	for i := range policies {
		list = append(list, policySummaryToMap(&policies[i]))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"Policies": list,
	})
}

func (p *Provider) listTargetsForPolicy(params map[string]any) (*plugin.Response, error) {
	policyID, _ := params["PolicyId"].(string)
	if policyID == "" {
		return shared.JSONError("InvalidInputException", "PolicyId is required", http.StatusBadRequest), nil
	}
	if _, err := p.store.GetPolicy(policyID); err != nil {
		return shared.JSONError("PolicyNotFoundException", "policy not found", http.StatusNotFound), nil
	}
	targets, err := p.store.ListTargetsForPolicy(policyID)
	if err != nil {
		return nil, err
	}
	list := make([]map[string]any, 0, len(targets))
	for _, t := range targets {
		targetType := targetTypeFromID(t)
		list = append(list, map[string]any{
			"TargetId": t,
			"Type":     targetType,
		})
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"Targets": list,
	})
}

// ---- Tags ----

func (p *Provider) tagResource(params map[string]any) (*plugin.Response, error) {
	resourceID, _ := params["ResourceId"].(string)
	if resourceID == "" {
		return shared.JSONError("InvalidInputException", "ResourceId is required", http.StatusBadRequest), nil
	}
	rawTags, _ := params["Tags"].([]any)
	if err := p.store.tags.AddTags(resourceID, parseTags(rawTags)); err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) untagResource(params map[string]any) (*plugin.Response, error) {
	resourceID, _ := params["ResourceId"].(string)
	if resourceID == "" {
		return shared.JSONError("InvalidInputException", "ResourceId is required", http.StatusBadRequest), nil
	}
	rawKeys, _ := params["TagKeys"].([]any)
	keys := make([]string, 0, len(rawKeys))
	for _, k := range rawKeys {
		if s, ok := k.(string); ok {
			keys = append(keys, s)
		}
	}
	if err := p.store.tags.RemoveTags(resourceID, keys); err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) listTagsForResource(params map[string]any) (*plugin.Response, error) {
	resourceID, _ := params["ResourceId"].(string)
	if resourceID == "" {
		return shared.JSONError("InvalidInputException", "ResourceId is required", http.StatusBadRequest), nil
	}
	tags, err := p.store.tags.ListTags(resourceID)
	if err != nil {
		return nil, err
	}
	tagList := make([]map[string]string, 0, len(tags))
	for k, v := range tags {
		tagList = append(tagList, map[string]string{"Key": k, "Value": v})
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"Tags": tagList,
	})
}

// ---- Stub ----

func (p *Provider) stubSuccess(_ string) (*plugin.Response, error) {
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

// ---- Helpers ----

func orgToMap(org *Organization) map[string]any {
	return map[string]any{
		"Id":                   org.ID,
		"Arn":                  org.ARN,
		"FeatureSet":           org.FeatureSet,
		"MasterAccountId":      org.MasterAccountID,
		"MasterAccountArn":     org.MasterAccountARN,
		"MasterAccountEmail":   org.MasterEmail,
		"AvailablePolicyTypes": []any{},
	}
}

func rootToMap(r *Root) map[string]any {
	var policyTypes []any
	json.Unmarshal([]byte(r.PolicyTypes), &policyTypes) //nolint:errcheck
	if policyTypes == nil {
		policyTypes = []any{}
	}
	return map[string]any{
		"Id":          r.ID,
		"Arn":         r.ARN,
		"Name":        r.Name,
		"PolicyTypes": policyTypes,
	}
}

func accountToMap(a *Account) map[string]any {
	return map[string]any{
		"Id":              a.ID,
		"Arn":             a.ARN,
		"Name":            a.Name,
		"Email":           a.Email,
		"Status":          a.Status,
		"JoinedMethod":    a.JoinedMethod,
		"JoinedTimestamp": a.CreatedAt.Unix(),
	}
}

func ouToMap(ou *OU) map[string]any {
	return map[string]any{
		"Id":   ou.ID,
		"Arn":  ou.ARN,
		"Name": ou.Name,
	}
}

func policyToMap(p *Policy) map[string]any {
	return map[string]any{
		"PolicySummary": policySummaryToMap(p),
		"Content":       p.Content,
	}
}

func policySummaryToMap(p *Policy) map[string]any {
	return map[string]any{
		"Id":          p.ID,
		"Arn":         p.ARN,
		"Name":        p.Name,
		"Type":        p.Type,
		"Description": p.Description,
		"AwsManaged":  false,
	}
}

func parseTags(rawTags []any) map[string]string {
	tags := make(map[string]string)
	for _, t := range rawTags {
		tag, _ := t.(map[string]any)
		k, _ := tag["Key"].(string)
		v, _ := tag["Value"].(string)
		if k != "" {
			tags[k] = v
		}
	}
	return tags
}

func parseJSONStringArray(s string) []string {
	var out []string
	json.Unmarshal([]byte(s), &out) //nolint:errcheck
	return out
}

func isUniqueErr(err error) bool {
	return err != nil && strings.Contains(err.Error(), "UNIQUE constraint failed")
}

func generateAccountID() string {
	// Generate a 12-digit numeric account ID using UUID bytes
	u := shared.GenerateUUID()
	// Use first 12 hex chars from UUID (skip hyphens) as basis
	hex := strings.ReplaceAll(u, "-", "")
	n := uint64(0)
	for i := 0; i < 12 && i < len(hex); i++ {
		c := hex[i]
		var d uint64
		if c >= '0' && c <= '9' {
			d = uint64(c - '0')
		} else {
			d = uint64(c-'a') + 10
		}
		n = n*16 + d
	}
	return fmt.Sprintf("%012d", n%1000000000000)
}

func targetTypeFromID(id string) string {
	if strings.HasPrefix(id, "r-") {
		return "ROOT"
	}
	if strings.HasPrefix(id, "ou-") {
		return "ORGANIZATIONAL_UNIT"
	}
	return "ACCOUNT"
}
