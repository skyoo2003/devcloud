// SPDX-License-Identifier: Apache-2.0

// internal/services/ssoadmin/provider.go
package ssoadmin

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

// Provider implements the SWBExternalService (SSO Admin) service.
type Provider struct {
	store *Store
}

func (p *Provider) ServiceID() string             { return "ssoadmin" }
func (p *Provider) ServiceName() string           { return "SWBExternalService" }
func (p *Provider) Protocol() plugin.ProtocolType { return plugin.ProtocolJSON11 }

func (p *Provider) Init(cfg plugin.PluginConfig) error {
	dataDir := cfg.DataDir
	if dataDir == "" {
		dataDir = "."
	}
	var err error
	p.store, err = NewStore(filepath.Join(dataDir, "ssoadmin"))
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
		return ssoError("SerializationException", "failed to read body", http.StatusBadRequest), nil
	}
	var params map[string]any
	if len(body) > 0 {
		if err := json.Unmarshal(body, &params); err != nil {
			return ssoError("SerializationException", "invalid JSON", http.StatusBadRequest), nil
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
	// Instance
	case "CreateInstance":
		return p.createInstance(params)
	case "DescribeInstance":
		return p.describeInstance(params)
	case "ListInstances":
		return p.listInstances(params)
	case "UpdateInstance":
		return p.updateInstance(params)
	case "DeleteInstance":
		return p.deleteInstance(params)
	// PermissionSet
	case "CreatePermissionSet":
		return p.createPermissionSet(params)
	case "DescribePermissionSet":
		return p.describePermissionSet(params)
	case "ListPermissionSets":
		return p.listPermissionSets(params)
	case "UpdatePermissionSet":
		return p.updatePermissionSet(params)
	case "DeletePermissionSet":
		return p.deletePermissionSet(params)
	case "ProvisionPermissionSet":
		return p.provisionPermissionSet(params)
	// AccountAssignment
	case "CreateAccountAssignment":
		return p.createAccountAssignment(params)
	case "DeleteAccountAssignment":
		return p.deleteAccountAssignment(params)
	case "ListAccountAssignments":
		return p.listAccountAssignments(params)
	case "ListAccountAssignmentsForPrincipal":
		return p.listAccountAssignmentsForPrincipal(params)
	case "ListAccountsForProvisionedPermissionSet":
		return p.listAccountsForProvisionedPermissionSet(params)
	case "ListPermissionSetsProvisionedToAccount":
		return p.listPermissionSetsProvisionedToAccount(params)
	case "DescribeAccountAssignmentCreationStatus":
		return p.describeAccountAssignmentStatus(params, "SUCCEEDED")
	case "DescribeAccountAssignmentDeletionStatus":
		return p.describeAccountAssignmentStatus(params, "SUCCEEDED")
	case "ListAccountAssignmentCreationStatus":
		return shared.JSONResponse(http.StatusOK, map[string]any{"AccountAssignmentsCreationStatus": []any{}})
	case "ListAccountAssignmentDeletionStatus":
		return shared.JSONResponse(http.StatusOK, map[string]any{"AccountAssignmentsDeletionStatus": []any{}})
	// Inline policy
	case "PutInlinePolicyToPermissionSet":
		return p.putInlinePolicy(params)
	case "GetInlinePolicyForPermissionSet":
		return p.getInlinePolicy(params)
	case "DeleteInlinePolicyFromPermissionSet":
		return p.deleteInlinePolicy(params)
	// Permissions boundary
	case "PutPermissionsBoundaryToPermissionSet":
		return p.putPermissionsBoundary(params)
	case "GetPermissionsBoundaryForPermissionSet":
		return p.getPermissionsBoundary(params)
	case "DeletePermissionsBoundaryFromPermissionSet":
		return p.deletePermissionsBoundary(params)
	// Managed policies
	case "AttachManagedPolicyToPermissionSet":
		return p.attachManagedPolicy(params)
	case "DetachManagedPolicyFromPermissionSet":
		return p.detachManagedPolicy(params)
	case "ListManagedPoliciesInPermissionSet":
		return p.listManagedPolicies(params)
	// Customer managed policies
	case "AttachCustomerManagedPolicyReferenceToPermissionSet":
		return p.attachCustomerManagedPolicy(params)
	case "DetachCustomerManagedPolicyReferenceFromPermissionSet":
		return p.detachCustomerManagedPolicy(params)
	case "ListCustomerManagedPolicyReferencesInPermissionSet":
		return p.listCustomerManagedPolicies(params)
	// Application
	case "CreateApplication":
		return p.createApplication(params)
	case "DescribeApplication":
		return p.describeApplication(params)
	case "ListApplications":
		return p.listApplications(params)
	case "UpdateApplication":
		return p.updateApplication(params)
	case "DeleteApplication":
		return p.deleteApplication(params)
	// TrustedTokenIssuer
	case "CreateTrustedTokenIssuer":
		return p.createTrustedTokenIssuer(params)
	case "DescribeTrustedTokenIssuer":
		return p.describeTrustedTokenIssuer(params)
	case "ListTrustedTokenIssuers":
		return p.listTrustedTokenIssuers(params)
	case "UpdateTrustedTokenIssuer":
		return p.updateTrustedTokenIssuer(params)
	case "DeleteTrustedTokenIssuer":
		return p.deleteTrustedTokenIssuer(params)
	// Tags
	case "TagResource":
		return p.tagResource(params)
	case "UntagResource":
		return p.untagResource(params)
	case "ListTagsForResource":
		return p.listTagsForResource(params)
	// Stub operations
	case "DescribePermissionSetProvisioningStatus",
		"ListPermissionSetProvisioningStatus":
		return shared.JSONResponse(http.StatusOK, map[string]any{
			"PermissionSetProvisioningStatus": map[string]any{
				"Status":           "SUCCEEDED",
				"RequestId":        shared.GenerateUUID(),
				"PermissionSetArn": "",
			},
		})
	case "CreateInstanceAccessControlAttributeConfiguration",
		"UpdateInstanceAccessControlAttributeConfiguration",
		"DeleteInstanceAccessControlAttributeConfiguration":
		return shared.JSONResponse(http.StatusOK, map[string]any{})
	case "DescribeInstanceAccessControlAttributeConfiguration":
		return shared.JSONResponse(http.StatusOK, map[string]any{
			"Status": "ENABLED",
			"InstanceAccessControlAttributeConfiguration": map[string]any{
				"AccessControlAttributes": []any{},
			},
		})
	case "ListApplicationAssignments":
		return shared.JSONResponse(http.StatusOK, map[string]any{"ApplicationAssignments": []any{}})
	case "ListApplicationAssignmentsForPrincipal":
		return shared.JSONResponse(http.StatusOK, map[string]any{"ApplicationAssignments": []any{}})
	case "CreateApplicationAssignment",
		"DeleteApplicationAssignment":
		return shared.JSONResponse(http.StatusOK, map[string]any{})
	case "DescribeApplicationAssignment":
		return shared.JSONResponse(http.StatusOK, map[string]any{
			"ApplicationArn": "",
			"PrincipalId":    "",
			"PrincipalType":  "USER",
		})
	case "ListApplicationProviders":
		return shared.JSONResponse(http.StatusOK, map[string]any{"ApplicationProviders": []any{}})
	case "DescribeApplicationProvider":
		return shared.JSONResponse(http.StatusOK, map[string]any{
			"ApplicationProviderArn": "",
			"DisplayData":            map[string]any{},
		})
	case "GetApplicationAssignmentConfiguration":
		return shared.JSONResponse(http.StatusOK, map[string]any{"AssignmentRequired": false})
	case "PutApplicationAssignmentConfiguration":
		return shared.JSONResponse(http.StatusOK, map[string]any{})
	case "GetApplicationSessionConfiguration":
		return shared.JSONResponse(http.StatusOK, map[string]any{"SessionConfiguration": map[string]any{}})
	case "PutApplicationSessionConfiguration":
		return shared.JSONResponse(http.StatusOK, map[string]any{})
	case "AddRegion", "RemoveRegion":
		return shared.JSONResponse(http.StatusOK, map[string]any{})
	case "DescribeRegion":
		return shared.JSONResponse(http.StatusOK, map[string]any{"RegionScopes": []any{}})
	case "ListRegions":
		return shared.JSONResponse(http.StatusOK, map[string]any{"Regions": []any{}})
	default:
		return ssoError("InvalidAction", fmt.Sprintf("unknown action: %s", action), http.StatusBadRequest), nil
	}
}

func (p *Provider) ListResources(_ context.Context) ([]plugin.Resource, error) {
	insts, err := p.store.ListInstances()
	if err != nil {
		return nil, err
	}
	res := make([]plugin.Resource, 0, len(insts))
	for _, inst := range insts {
		res = append(res, plugin.Resource{Type: "sso-instance", ID: inst.ARN, Name: inst.Name})
	}
	return res, nil
}

func (p *Provider) GetMetrics(_ context.Context) (*plugin.ServiceMetrics, error) {
	return &plugin.ServiceMetrics{}, nil
}

// --- Instance ---

func (p *Provider) createInstance(params map[string]any) (*plugin.Response, error) {
	name, _ := params["Name"].(string)
	id := shared.GenerateUUID()
	arn := ssoARN("instance", id)
	identityStoreID := "d-" + shared.GenerateID("", 10)

	inst := &Instance{
		ARN:             arn,
		Name:            name,
		IdentityStoreID: identityStoreID,
		Status:          "ACTIVE",
	}
	if err := p.store.CreateInstance(inst); err != nil {
		return nil, err
	}

	if rawTags, ok := params["Tags"].([]any); ok {
		p.store.tags.AddTags(arn, parseTagList(rawTags))
	}

	return shared.JSONResponse(http.StatusOK, map[string]any{"InstanceArn": arn})
}

func (p *Provider) describeInstance(params map[string]any) (*plugin.Response, error) {
	arn, _ := params["InstanceArn"].(string)
	if arn == "" {
		return ssoError("ValidationException", "InstanceArn is required", http.StatusBadRequest), nil
	}
	inst, err := p.store.GetInstance(arn)
	if err != nil {
		return ssoError("ResourceNotFoundException", "instance not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, instanceToMap(inst))
}

func (p *Provider) listInstances(params map[string]any) (*plugin.Response, error) {
	insts, err := p.store.ListInstances()
	if err != nil {
		return nil, err
	}
	list := make([]map[string]any, 0, len(insts))
	for i := range insts {
		list = append(list, instanceToMap(&insts[i]))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"Instances": list})
}

func (p *Provider) updateInstance(params map[string]any) (*plugin.Response, error) {
	arn, _ := params["InstanceArn"].(string)
	name, _ := params["Name"].(string)
	if arn == "" {
		return ssoError("ValidationException", "InstanceArn is required", http.StatusBadRequest), nil
	}
	if err := p.store.UpdateInstance(arn, name); err != nil {
		return ssoError("ResourceNotFoundException", "instance not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) deleteInstance(params map[string]any) (*plugin.Response, error) {
	arn, _ := params["InstanceArn"].(string)
	if arn == "" {
		return ssoError("ValidationException", "InstanceArn is required", http.StatusBadRequest), nil
	}
	p.store.tags.DeleteAllTags(arn)
	if err := p.store.DeleteInstance(arn); err != nil {
		return ssoError("ResourceNotFoundException", "instance not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

// --- PermissionSet ---

func (p *Provider) createPermissionSet(params map[string]any) (*plugin.Response, error) {
	name, _ := params["Name"].(string)
	instanceARN, _ := params["InstanceArn"].(string)
	if name == "" || instanceARN == "" {
		return ssoError("ValidationException", "Name and InstanceArn are required", http.StatusBadRequest), nil
	}
	description, _ := params["Description"].(string)
	sessionDuration, _ := params["SessionDuration"].(string)
	if sessionDuration == "" {
		sessionDuration = "PT1H"
	}
	relayState, _ := params["RelayState"].(string)

	id := shared.GenerateUUID()
	arn := ssoARN("permissionSet", id)
	ps := &PermissionSet{
		ARN:             arn,
		Name:            name,
		InstanceARN:     instanceARN,
		Description:     description,
		SessionDuration: sessionDuration,
		RelayState:      relayState,
		InlinePolicy:    "",
	}
	if err := p.store.CreatePermissionSet(ps); err != nil {
		return nil, err
	}

	if rawTags, ok := params["Tags"].([]any); ok {
		p.store.tags.AddTags(arn, parseTagList(rawTags))
	}

	return shared.JSONResponse(http.StatusOK, map[string]any{
		"PermissionSet": permissionSetToMap(ps),
	})
}

func (p *Provider) describePermissionSet(params map[string]any) (*plugin.Response, error) {
	arn, _ := params["PermissionSetArn"].(string)
	if arn == "" {
		return ssoError("ValidationException", "PermissionSetArn is required", http.StatusBadRequest), nil
	}
	ps, err := p.store.GetPermissionSet(arn)
	if err != nil {
		return ssoError("ResourceNotFoundException", "permission set not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"PermissionSet": permissionSetToMap(ps),
	})
}

func (p *Provider) listPermissionSets(params map[string]any) (*plugin.Response, error) {
	instanceARN, _ := params["InstanceArn"].(string)
	if instanceARN == "" {
		return ssoError("ValidationException", "InstanceArn is required", http.StatusBadRequest), nil
	}
	sets, err := p.store.ListPermissionSets(instanceARN)
	if err != nil {
		return nil, err
	}
	arns := make([]string, 0, len(sets))
	for _, ps := range sets {
		arns = append(arns, ps.ARN)
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"PermissionSets": arns})
}

func (p *Provider) updatePermissionSet(params map[string]any) (*plugin.Response, error) {
	arn, _ := params["PermissionSetArn"].(string)
	if arn == "" {
		return ssoError("ValidationException", "PermissionSetArn is required", http.StatusBadRequest), nil
	}
	fields := map[string]any{}
	if v, ok := params["Description"].(string); ok {
		fields["Description"] = v
	}
	if v, ok := params["SessionDuration"].(string); ok {
		fields["SessionDuration"] = v
	}
	if v, ok := params["RelayState"].(string); ok {
		fields["RelayState"] = v
	}
	if err := p.store.UpdatePermissionSet(arn, fields); err != nil {
		return ssoError("ResourceNotFoundException", "permission set not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) deletePermissionSet(params map[string]any) (*plugin.Response, error) {
	arn, _ := params["PermissionSetArn"].(string)
	if arn == "" {
		return ssoError("ValidationException", "PermissionSetArn is required", http.StatusBadRequest), nil
	}
	p.store.tags.DeleteAllTags(arn)
	if err := p.store.DeletePermissionSet(arn); err != nil {
		return ssoError("ResourceNotFoundException", "permission set not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) provisionPermissionSet(params map[string]any) (*plugin.Response, error) {
	arn, _ := params["PermissionSetArn"].(string)
	requestID := shared.GenerateUUID()
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"PermissionSetProvisioningStatus": map[string]any{
			"Status":           "SUCCEEDED",
			"RequestId":        requestID,
			"PermissionSetArn": arn,
		},
	})
}

// --- AccountAssignment ---

func (p *Provider) createAccountAssignment(params map[string]any) (*plugin.Response, error) {
	instanceARN, _ := params["InstanceArn"].(string)
	permSetARN, _ := params["PermissionSetArn"].(string)
	targetID, _ := params["TargetId"].(string)
	targetType, _ := params["TargetType"].(string)
	principalID, _ := params["PrincipalId"].(string)
	principalType, _ := params["PrincipalType"].(string)
	if instanceARN == "" || permSetARN == "" || targetID == "" || principalID == "" {
		return ssoError("ValidationException", "InstanceArn, PermissionSetArn, TargetId, PrincipalId are required", http.StatusBadRequest), nil
	}
	if targetType == "" {
		targetType = "AWS_ACCOUNT"
	}
	if principalType == "" {
		principalType = "USER"
	}

	a := &AccountAssignment{
		InstanceARN:      instanceARN,
		PermissionSetARN: permSetARN,
		TargetID:         targetID,
		TargetType:       targetType,
		PrincipalID:      principalID,
		PrincipalType:    principalType,
	}
	if err := p.store.CreateAccountAssignment(a); err != nil {
		return nil, err
	}
	requestID := shared.GenerateUUID()
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"AccountAssignmentCreationStatus": map[string]any{
			"Status":           "SUCCEEDED",
			"RequestId":        requestID,
			"TargetId":         targetID,
			"TargetType":       targetType,
			"PermissionSetArn": permSetARN,
			"PrincipalId":      principalID,
			"PrincipalType":    principalType,
		},
	})
}

func (p *Provider) deleteAccountAssignment(params map[string]any) (*plugin.Response, error) {
	instanceARN, _ := params["InstanceArn"].(string)
	permSetARN, _ := params["PermissionSetArn"].(string)
	targetID, _ := params["TargetId"].(string)
	principalID, _ := params["PrincipalId"].(string)
	if instanceARN == "" || permSetARN == "" || targetID == "" || principalID == "" {
		return ssoError("ValidationException", "InstanceArn, PermissionSetArn, TargetId, PrincipalId are required", http.StatusBadRequest), nil
	}
	if err := p.store.DeleteAccountAssignment(instanceARN, permSetARN, targetID, principalID); err != nil {
		return ssoError("ResourceNotFoundException", "account assignment not found", http.StatusBadRequest), nil
	}
	requestID := shared.GenerateUUID()
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"AccountAssignmentDeletionStatus": map[string]any{
			"Status":    "SUCCEEDED",
			"RequestId": requestID,
		},
	})
}

func (p *Provider) listAccountAssignments(params map[string]any) (*plugin.Response, error) {
	instanceARN, _ := params["InstanceArn"].(string)
	permSetARN, _ := params["PermissionSetArn"].(string)
	accountID, _ := params["AccountId"].(string)
	if instanceARN == "" || permSetARN == "" || accountID == "" {
		return ssoError("ValidationException", "InstanceArn, PermissionSetArn, AccountId are required", http.StatusBadRequest), nil
	}
	assignments, err := p.store.ListAccountAssignments(instanceARN, permSetARN, accountID)
	if err != nil {
		return nil, err
	}
	list := make([]map[string]any, 0, len(assignments))
	for i := range assignments {
		list = append(list, assignmentToMap(&assignments[i]))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"AccountAssignments": list})
}

func (p *Provider) listAccountAssignmentsForPrincipal(params map[string]any) (*plugin.Response, error) {
	instanceARN, _ := params["InstanceArn"].(string)
	principalID, _ := params["PrincipalId"].(string)
	principalType, _ := params["PrincipalType"].(string)
	if instanceARN == "" || principalID == "" {
		return ssoError("ValidationException", "InstanceArn and PrincipalId are required", http.StatusBadRequest), nil
	}
	if principalType == "" {
		principalType = "USER"
	}
	assignments, err := p.store.ListAccountAssignmentsForPrincipal(instanceARN, principalID, principalType)
	if err != nil {
		return nil, err
	}
	list := make([]map[string]any, 0, len(assignments))
	for i := range assignments {
		list = append(list, assignmentToMap(&assignments[i]))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"AccountAssignments": list})
}

func (p *Provider) listAccountsForProvisionedPermissionSet(params map[string]any) (*plugin.Response, error) {
	instanceARN, _ := params["InstanceArn"].(string)
	permSetARN, _ := params["PermissionSetArn"].(string)
	if instanceARN == "" || permSetARN == "" {
		return ssoError("ValidationException", "InstanceArn and PermissionSetArn are required", http.StatusBadRequest), nil
	}
	accounts, err := p.store.ListAccountsForPermissionSet(instanceARN, permSetARN)
	if err != nil {
		return nil, err
	}
	if accounts == nil {
		accounts = []string{}
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"AccountIds": accounts})
}

func (p *Provider) listPermissionSetsProvisionedToAccount(params map[string]any) (*plugin.Response, error) {
	instanceARN, _ := params["InstanceArn"].(string)
	accountID, _ := params["AccountId"].(string)
	if instanceARN == "" || accountID == "" {
		return ssoError("ValidationException", "InstanceArn and AccountId are required", http.StatusBadRequest), nil
	}
	arns, err := p.store.ListPermissionSetsProvisionedToAccount(instanceARN, accountID)
	if err != nil {
		return nil, err
	}
	if arns == nil {
		arns = []string{}
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"PermissionSets": arns})
}

func (p *Provider) describeAccountAssignmentStatus(params map[string]any, status string) (*plugin.Response, error) {
	requestID, _ := params["AccountAssignmentCreationRequestId"].(string)
	if requestID == "" {
		requestID, _ = params["AccountAssignmentDeletionRequestId"].(string)
	}
	if requestID == "" {
		requestID = shared.GenerateUUID()
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"AccountAssignmentCreationStatus": map[string]any{
			"Status":    status,
			"RequestId": requestID,
		},
	})
}

// --- Inline policy ---

func (p *Provider) putInlinePolicy(params map[string]any) (*plugin.Response, error) {
	arn, _ := params["PermissionSetArn"].(string)
	policy, _ := params["InlinePolicy"].(string)
	if arn == "" {
		return ssoError("ValidationException", "PermissionSetArn is required", http.StatusBadRequest), nil
	}
	if err := p.store.SetInlinePolicy(arn, policy); err != nil {
		return ssoError("ResourceNotFoundException", "permission set not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) getInlinePolicy(params map[string]any) (*plugin.Response, error) {
	arn, _ := params["PermissionSetArn"].(string)
	if arn == "" {
		return ssoError("ValidationException", "PermissionSetArn is required", http.StatusBadRequest), nil
	}
	ps, err := p.store.GetPermissionSet(arn)
	if err != nil {
		return ssoError("ResourceNotFoundException", "permission set not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"InlinePolicy": ps.InlinePolicy})
}

func (p *Provider) deleteInlinePolicy(params map[string]any) (*plugin.Response, error) {
	arn, _ := params["PermissionSetArn"].(string)
	if arn == "" {
		return ssoError("ValidationException", "PermissionSetArn is required", http.StatusBadRequest), nil
	}
	if err := p.store.DeleteInlinePolicy(arn); err != nil {
		return ssoError("ResourceNotFoundException", "permission set not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

// --- Permissions boundary ---

func (p *Provider) putPermissionsBoundary(params map[string]any) (*plugin.Response, error) {
	arn, _ := params["PermissionSetArn"].(string)
	if arn == "" {
		return ssoError("ValidationException", "PermissionSetArn is required", http.StatusBadRequest), nil
	}
	boundary := "{}"
	if pb, ok := params["PermissionsBoundary"]; ok {
		b, _ := json.Marshal(pb)
		boundary = string(b)
	}
	if err := p.store.SetPermissionsBoundary(arn, boundary); err != nil {
		return ssoError("ResourceNotFoundException", "permission set not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) getPermissionsBoundary(params map[string]any) (*plugin.Response, error) {
	arn, _ := params["PermissionSetArn"].(string)
	if arn == "" {
		return ssoError("ValidationException", "PermissionSetArn is required", http.StatusBadRequest), nil
	}
	ps, err := p.store.GetPermissionSet(arn)
	if err != nil {
		return ssoError("ResourceNotFoundException", "permission set not found", http.StatusBadRequest), nil
	}
	var pb any
	json.Unmarshal([]byte(ps.PermissionsBoundary), &pb)
	return shared.JSONResponse(http.StatusOK, map[string]any{"PermissionsBoundary": pb})
}

func (p *Provider) deletePermissionsBoundary(params map[string]any) (*plugin.Response, error) {
	arn, _ := params["PermissionSetArn"].(string)
	if arn == "" {
		return ssoError("ValidationException", "PermissionSetArn is required", http.StatusBadRequest), nil
	}
	if err := p.store.DeletePermissionsBoundary(arn); err != nil {
		return ssoError("ResourceNotFoundException", "permission set not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

// --- Managed policies ---

func (p *Provider) attachManagedPolicy(params map[string]any) (*plugin.Response, error) {
	psARN, _ := params["PermissionSetArn"].(string)
	policyARN, _ := params["ManagedPolicyArn"].(string)
	if psARN == "" || policyARN == "" {
		return ssoError("ValidationException", "PermissionSetArn and ManagedPolicyArn are required", http.StatusBadRequest), nil
	}
	if err := p.store.AttachManagedPolicy(psARN, policyARN); err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) detachManagedPolicy(params map[string]any) (*plugin.Response, error) {
	psARN, _ := params["PermissionSetArn"].(string)
	policyARN, _ := params["ManagedPolicyArn"].(string)
	if psARN == "" || policyARN == "" {
		return ssoError("ValidationException", "PermissionSetArn and ManagedPolicyArn are required", http.StatusBadRequest), nil
	}
	if err := p.store.DetachManagedPolicy(psARN, policyARN); err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) listManagedPolicies(params map[string]any) (*plugin.Response, error) {
	psARN, _ := params["PermissionSetArn"].(string)
	if psARN == "" {
		return ssoError("ValidationException", "PermissionSetArn is required", http.StatusBadRequest), nil
	}
	arns, err := p.store.ListManagedPolicies(psARN)
	if err != nil {
		return nil, err
	}
	list := make([]map[string]string, 0, len(arns))
	for _, arn := range arns {
		list = append(list, map[string]string{"Arn": arn, "Name": policyNameFromARN(arn)})
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"AttachedManagedPolicies": list})
}

// --- Customer managed policies ---

func (p *Provider) attachCustomerManagedPolicy(params map[string]any) (*plugin.Response, error) {
	psARN, _ := params["PermissionSetArn"].(string)
	ref, _ := params["CustomerManagedPolicyReference"].(map[string]any)
	name, _ := ref["Name"].(string)
	path, _ := ref["Path"].(string)
	if psARN == "" || name == "" {
		return ssoError("ValidationException", "PermissionSetArn and policy Name are required", http.StatusBadRequest), nil
	}
	if err := p.store.AttachCustomerManagedPolicy(psARN, name, path); err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) detachCustomerManagedPolicy(params map[string]any) (*plugin.Response, error) {
	psARN, _ := params["PermissionSetArn"].(string)
	ref, _ := params["CustomerManagedPolicyReference"].(map[string]any)
	name, _ := ref["Name"].(string)
	path, _ := ref["Path"].(string)
	if psARN == "" || name == "" {
		return ssoError("ValidationException", "PermissionSetArn and policy Name are required", http.StatusBadRequest), nil
	}
	if err := p.store.DetachCustomerManagedPolicy(psARN, name, path); err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) listCustomerManagedPolicies(params map[string]any) (*plugin.Response, error) {
	psARN, _ := params["PermissionSetArn"].(string)
	if psARN == "" {
		return ssoError("ValidationException", "PermissionSetArn is required", http.StatusBadRequest), nil
	}
	policies, err := p.store.ListCustomerManagedPolicies(psARN)
	if err != nil {
		return nil, err
	}
	list := make([]map[string]string, 0, len(policies))
	for _, pol := range policies {
		list = append(list, map[string]string{"Name": pol[0], "Path": pol[1]})
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"CustomerManagedPolicyReferences": list})
}

// --- Application ---

func (p *Provider) createApplication(params map[string]any) (*plugin.Response, error) {
	name, _ := params["Name"].(string)
	instanceARN, _ := params["InstanceArn"].(string)
	if name == "" || instanceARN == "" {
		return ssoError("ValidationException", "Name and InstanceArn are required", http.StatusBadRequest), nil
	}
	appProviderARN, _ := params["ApplicationProviderArn"].(string)
	description, _ := params["Description"].(string)
	status, _ := params["Status"].(string)
	if status == "" {
		status = "ENABLED"
	}

	id := shared.GenerateUUID()
	arn := ssoARN("application", id)
	app := &Application{
		ARN:            arn,
		Name:           name,
		InstanceARN:    instanceARN,
		AppProviderARN: appProviderARN,
		Description:    description,
		Status:         status,
	}
	if err := p.store.CreateApplication(app); err != nil {
		return nil, err
	}

	if rawTags, ok := params["Tags"].([]any); ok {
		p.store.tags.AddTags(arn, parseTagList(rawTags))
	}

	return shared.JSONResponse(http.StatusOK, map[string]any{"ApplicationArn": arn})
}

func (p *Provider) describeApplication(params map[string]any) (*plugin.Response, error) {
	arn, _ := params["ApplicationArn"].(string)
	if arn == "" {
		return ssoError("ValidationException", "ApplicationArn is required", http.StatusBadRequest), nil
	}
	app, err := p.store.GetApplication(arn)
	if err != nil {
		return ssoError("ResourceNotFoundException", "application not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, applicationToMap(app))
}

func (p *Provider) listApplications(params map[string]any) (*plugin.Response, error) {
	instanceARN, _ := params["InstanceArn"].(string)
	if instanceARN == "" {
		return ssoError("ValidationException", "InstanceArn is required", http.StatusBadRequest), nil
	}
	apps, err := p.store.ListApplications(instanceARN)
	if err != nil {
		return nil, err
	}
	list := make([]map[string]any, 0, len(apps))
	for i := range apps {
		list = append(list, applicationToMap(&apps[i]))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"Applications": list})
}

func (p *Provider) updateApplication(params map[string]any) (*plugin.Response, error) {
	arn, _ := params["ApplicationArn"].(string)
	if arn == "" {
		return ssoError("ValidationException", "ApplicationArn is required", http.StatusBadRequest), nil
	}
	fields := map[string]any{}
	if v, ok := params["Name"].(string); ok {
		fields["Name"] = v
	}
	if v, ok := params["Description"].(string); ok {
		fields["Description"] = v
	}
	if v, ok := params["Status"].(string); ok {
		fields["Status"] = v
	}
	if err := p.store.UpdateApplication(arn, fields); err != nil {
		return ssoError("ResourceNotFoundException", "application not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) deleteApplication(params map[string]any) (*plugin.Response, error) {
	arn, _ := params["ApplicationArn"].(string)
	if arn == "" {
		return ssoError("ValidationException", "ApplicationArn is required", http.StatusBadRequest), nil
	}
	p.store.tags.DeleteAllTags(arn)
	if err := p.store.DeleteApplication(arn); err != nil {
		return ssoError("ResourceNotFoundException", "application not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

// --- TrustedTokenIssuer ---

func (p *Provider) createTrustedTokenIssuer(params map[string]any) (*plugin.Response, error) {
	name, _ := params["Name"].(string)
	instanceARN, _ := params["InstanceArn"].(string)
	if name == "" || instanceARN == "" {
		return ssoError("ValidationException", "Name and InstanceArn are required", http.StatusBadRequest), nil
	}
	issuerType, _ := params["TrustedTokenIssuerType"].(string)
	if issuerType == "" {
		issuerType = "OIDC_JWT"
	}
	config := "{}"
	if cfg, ok := params["TrustedTokenIssuerConfiguration"]; ok {
		b, _ := json.Marshal(cfg)
		config = string(b)
	}

	id := shared.GenerateUUID()
	arn := ssoARN("trustedTokenIssuer", id)
	tti := &TrustedTokenIssuer{
		ARN:         arn,
		Name:        name,
		InstanceARN: instanceARN,
		Type:        issuerType,
		Config:      config,
	}
	if err := p.store.CreateTrustedTokenIssuer(tti); err != nil {
		return nil, err
	}

	if rawTags, ok := params["Tags"].([]any); ok {
		p.store.tags.AddTags(arn, parseTagList(rawTags))
	}

	return shared.JSONResponse(http.StatusOK, map[string]any{"TrustedTokenIssuerArn": arn})
}

func (p *Provider) describeTrustedTokenIssuer(params map[string]any) (*plugin.Response, error) {
	arn, _ := params["TrustedTokenIssuerArn"].(string)
	if arn == "" {
		return ssoError("ValidationException", "TrustedTokenIssuerArn is required", http.StatusBadRequest), nil
	}
	tti, err := p.store.GetTrustedTokenIssuer(arn)
	if err != nil {
		return ssoError("ResourceNotFoundException", "trusted token issuer not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, trustedTokenIssuerToMap(tti))
}

func (p *Provider) listTrustedTokenIssuers(params map[string]any) (*plugin.Response, error) {
	instanceARN, _ := params["InstanceArn"].(string)
	if instanceARN == "" {
		return ssoError("ValidationException", "InstanceArn is required", http.StatusBadRequest), nil
	}
	issuers, err := p.store.ListTrustedTokenIssuers(instanceARN)
	if err != nil {
		return nil, err
	}
	list := make([]map[string]any, 0, len(issuers))
	for i := range issuers {
		list = append(list, trustedTokenIssuerToMap(&issuers[i]))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"TrustedTokenIssuers": list})
}

func (p *Provider) updateTrustedTokenIssuer(params map[string]any) (*plugin.Response, error) {
	arn, _ := params["TrustedTokenIssuerArn"].(string)
	if arn == "" {
		return ssoError("ValidationException", "TrustedTokenIssuerArn is required", http.StatusBadRequest), nil
	}
	fields := map[string]any{}
	if v, ok := params["Name"].(string); ok {
		fields["Name"] = v
	}
	if cfg, ok := params["TrustedTokenIssuerConfiguration"]; ok {
		b, _ := json.Marshal(cfg)
		fields["Config"] = string(b)
	}
	if err := p.store.UpdateTrustedTokenIssuer(arn, fields); err != nil {
		return ssoError("ResourceNotFoundException", "trusted token issuer not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) deleteTrustedTokenIssuer(params map[string]any) (*plugin.Response, error) {
	arn, _ := params["TrustedTokenIssuerArn"].(string)
	if arn == "" {
		return ssoError("ValidationException", "TrustedTokenIssuerArn is required", http.StatusBadRequest), nil
	}
	p.store.tags.DeleteAllTags(arn)
	if err := p.store.DeleteTrustedTokenIssuer(arn); err != nil {
		return ssoError("ResourceNotFoundException", "trusted token issuer not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

// --- Tags ---

func (p *Provider) tagResource(params map[string]any) (*plugin.Response, error) {
	resourceARN, _ := params["InstanceArn"].(string)
	if resourceARN == "" {
		resourceARN, _ = params["ResourceArn"].(string)
	}
	if resourceARN == "" {
		return ssoError("ValidationException", "InstanceArn or ResourceArn is required", http.StatusBadRequest), nil
	}
	rawTags, _ := params["Tags"].([]any)
	if err := p.store.tags.AddTags(resourceARN, parseTagList(rawTags)); err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) untagResource(params map[string]any) (*plugin.Response, error) {
	resourceARN, _ := params["InstanceArn"].(string)
	if resourceARN == "" {
		resourceARN, _ = params["ResourceArn"].(string)
	}
	if resourceARN == "" {
		return ssoError("ValidationException", "InstanceArn or ResourceArn is required", http.StatusBadRequest), nil
	}
	rawKeys, _ := params["TagKeys"].([]any)
	keys := make([]string, 0, len(rawKeys))
	for _, k := range rawKeys {
		if s, ok := k.(string); ok {
			keys = append(keys, s)
		}
	}
	if err := p.store.tags.RemoveTags(resourceARN, keys); err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) listTagsForResource(params map[string]any) (*plugin.Response, error) {
	resourceARN, _ := params["InstanceArn"].(string)
	if resourceARN == "" {
		resourceARN, _ = params["ResourceArn"].(string)
	}
	if resourceARN == "" {
		return ssoError("ValidationException", "InstanceArn or ResourceArn is required", http.StatusBadRequest), nil
	}
	tags, err := p.store.tags.ListTags(resourceARN)
	if err != nil {
		return nil, err
	}
	list := make([]map[string]string, 0, len(tags))
	for k, v := range tags {
		list = append(list, map[string]string{"Key": k, "Value": v})
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"Tags": list})
}

// --- helpers ---

func ssoARN(resourceType, id string) string {
	return fmt.Sprintf("arn:aws:sso:::%s/%s", resourceType, id)
}

func ssoError(code, msg string, status int) *plugin.Response {
	b, _ := json.Marshal(map[string]string{"__type": code, "message": msg})
	return &plugin.Response{StatusCode: status, Body: b, ContentType: "application/x-amz-json-1.1"}
}

func instanceToMap(inst *Instance) map[string]any {
	return map[string]any{
		"InstanceArn":     inst.ARN,
		"Name":            inst.Name,
		"IdentityStoreId": inst.IdentityStoreID,
		"Status":          inst.Status,
		"CreatedDate":     inst.CreatedAt.Unix(),
	}
}

func permissionSetToMap(ps *PermissionSet) map[string]any {
	return map[string]any{
		"PermissionSetArn": ps.ARN,
		"Name":             ps.Name,
		"Description":      ps.Description,
		"SessionDuration":  ps.SessionDuration,
		"RelayState":       ps.RelayState,
		"CreatedDate":      ps.CreatedAt.Unix(),
	}
}

func assignmentToMap(a *AccountAssignment) map[string]any {
	return map[string]any{
		"AccountId":        a.TargetID,
		"PermissionSetArn": a.PermissionSetARN,
		"PrincipalId":      a.PrincipalID,
		"PrincipalType":    a.PrincipalType,
	}
}

func applicationToMap(app *Application) map[string]any {
	return map[string]any{
		"ApplicationArn":         app.ARN,
		"Name":                   app.Name,
		"InstanceArn":            app.InstanceARN,
		"ApplicationProviderArn": app.AppProviderARN,
		"Description":            app.Description,
		"Status":                 app.Status,
		"CreatedDate":            app.CreatedAt.Unix(),
	}
}

func trustedTokenIssuerToMap(tti *TrustedTokenIssuer) map[string]any {
	var config any
	json.Unmarshal([]byte(tti.Config), &config)
	return map[string]any{
		"TrustedTokenIssuerArn":           tti.ARN,
		"Name":                            tti.Name,
		"TrustedTokenIssuerType":          tti.Type,
		"TrustedTokenIssuerConfiguration": config,
	}
}

func parseTagList(rawTags []any) map[string]string {
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

func policyNameFromARN(arn string) string {
	parts := strings.Split(arn, "/")
	if len(parts) > 0 {
		return parts[len(parts)-1]
	}
	return arn
}
