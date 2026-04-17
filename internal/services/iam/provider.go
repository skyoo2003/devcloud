// SPDX-License-Identifier: Apache-2.0

package iam

import (
	"context"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	"github.com/skyoo2003/devcloud/internal/plugin"
)

const defaultAccountID = plugin.DefaultAccountID

// parseTags parses AWS-style Tags.member.N.Key / Tags.member.N.Value form parameters.
func parseTags(form url.Values) map[string]string {
	tags := make(map[string]string)
	for i := 1; ; i++ {
		key := form.Get(fmt.Sprintf("Tags.member.%d.Key", i))
		if key == "" {
			break
		}
		value := form.Get(fmt.Sprintf("Tags.member.%d.Value", i))
		tags[key] = value
	}
	return tags
}

// parseTagKeys parses AWS-style TagKeys.member.N form parameters.
func parseTagKeys(form url.Values) []string {
	var keys []string
	for i := 1; ; i++ {
		k := form.Get(fmt.Sprintf("TagKeys.member.%d", i))
		if k == "" {
			break
		}
		keys = append(keys, k)
	}
	return keys
}

// IAMProvider implements plugin.ServicePlugin for the IAM service.
type IAMProvider struct {
	store *IAMStore
}

// ServiceID returns the unique identifier for this plugin.
func (p *IAMProvider) ServiceID() string { return "iam" }

// ServiceName returns the human-readable name for this plugin.
func (p *IAMProvider) ServiceName() string { return "AWS IAM" }

// Protocol returns the wire protocol used by this plugin.
func (p *IAMProvider) Protocol() plugin.ProtocolType { return plugin.ProtocolQuery }

// Init initialises the IAMStore from cfg.
func (p *IAMProvider) Init(cfg plugin.PluginConfig) error {
	if err := os.MkdirAll(cfg.DataDir, 0o755); err != nil {
		return fmt.Errorf("init iam: %w", err)
	}

	dbPath := filepath.Join(cfg.DataDir, "iam.db")
	if v, ok := cfg.Options["db_path"]; ok {
		if s, ok := v.(string); ok && s != "" {
			dbPath = s
		}
	}

	var err error
	p.store, err = NewIAMStore(dbPath)
	return err
}

// Shutdown closes the IAMStore.
func (p *IAMProvider) Shutdown(_ context.Context) error {
	if p.store != nil {
		return p.store.Close()
	}
	return nil
}

// HandleRequest parses the Action from the form body and routes to a handler.
func (p *IAMProvider) HandleRequest(ctx context.Context, op string, req *http.Request) (*plugin.Response, error) {
	body, err := io.ReadAll(req.Body)
	if err != nil {
		return iamXMLError("InvalidRequest", "failed to read request body", http.StatusBadRequest), nil
	}

	form, err := url.ParseQuery(string(body))
	if err != nil {
		return iamXMLError("InvalidRequest", "failed to parse form body", http.StatusBadRequest), nil
	}

	action := op
	if action == "" {
		action = form.Get("Action")
	}

	switch action {
	case "CreateUser":
		return p.handleCreateUser(ctx, form)
	case "ListUsers":
		return p.handleListUsers(ctx, form)
	case "CreateRole":
		return p.handleCreateRole(ctx, form)
	case "ListRoles":
		return p.handleListRoles(ctx, form)
	case "DeleteUser":
		return p.handleDeleteUser(ctx, form)
	case "CreatePolicy":
		return p.handleCreatePolicy(ctx, form)
	case "AttachRolePolicy":
		return p.handleAttachRolePolicy(ctx, form)
	case "ListAttachedRolePolicies":
		return p.handleListAttachedRolePolicies(ctx, form)
	case "CreateAccessKey":
		return p.handleCreateAccessKey(ctx, form)
	case "GetUser":
		return p.handleGetUser(ctx, form)
	case "UpdateUser":
		return p.handleUpdateUser(ctx, form)
	case "GetRole":
		return p.handleGetRole(ctx, form)
	case "DeleteRole":
		return p.handleDeleteRole(ctx, form)
	case "UpdateAssumeRolePolicy":
		return p.handleUpdateAssumeRolePolicy(ctx, form)
	case "GetPolicy":
		return p.handleGetPolicy(ctx, form)
	case "DeletePolicy":
		return p.handleDeletePolicy(ctx, form)
	case "DetachRolePolicy":
		return p.handleDetachRolePolicy(ctx, form)
	case "AttachUserPolicy":
		return p.handleAttachUserPolicy(ctx, form)
	case "DetachUserPolicy":
		return p.handleDetachUserPolicy(ctx, form)
	case "ListAttachedUserPolicies":
		return p.handleListAttachedUserPolicies(ctx, form)
	case "GetPolicyVersion":
		return p.handleGetPolicyVersion(ctx, form)
	case "CreatePolicyVersion":
		return p.handleCreatePolicyVersion(ctx, form)
	case "ListPolicyVersions":
		return p.handleListPolicyVersions(ctx, form)
	// Inline policies
	case "PutUserPolicy":
		return p.handlePutUserPolicy(ctx, form)
	case "GetUserPolicy":
		return p.handleGetUserPolicy(ctx, form)
	case "DeleteUserPolicy":
		return p.handleDeleteUserPolicy(ctx, form)
	case "ListUserPolicies":
		return p.handleListUserPolicies(ctx, form)
	case "PutRolePolicy":
		return p.handlePutRolePolicy(ctx, form)
	case "GetRolePolicy":
		return p.handleGetRolePolicy(ctx, form)
	case "DeleteRolePolicy":
		return p.handleDeleteRolePolicy(ctx, form)
	case "ListRolePolicies":
		return p.handleListRolePolicies(ctx, form)
	// Groups
	case "CreateGroup":
		return p.handleCreateGroup(ctx, form)
	case "DeleteGroup":
		return p.handleDeleteGroup(ctx, form)
	case "GetGroup":
		return p.handleGetGroup(ctx, form)
	case "ListGroups":
		return p.handleListGroups(ctx, form)
	case "AddUserToGroup":
		return p.handleAddUserToGroup(ctx, form)
	case "RemoveUserFromGroup":
		return p.handleRemoveUserFromGroup(ctx, form)
	// Instance profiles
	case "CreateInstanceProfile":
		return p.handleCreateInstanceProfile(ctx, form)
	case "DeleteInstanceProfile":
		return p.handleDeleteInstanceProfile(ctx, form)
	case "GetInstanceProfile":
		return p.handleGetInstanceProfile(ctx, form)
	case "ListInstanceProfiles":
		return p.handleListInstanceProfiles(ctx, form)
	case "AddRoleToInstanceProfile":
		return p.handleAddRoleToInstanceProfile(ctx, form)
	case "RemoveRoleFromInstanceProfile":
		return p.handleRemoveRoleFromInstanceProfile(ctx, form)
	// Access keys & tagging
	case "ListAccessKeys":
		return p.handleListAccessKeys(ctx, form)
	case "UpdateAccessKey":
		return p.handleUpdateAccessKey(ctx, form)
	case "DeleteAccessKey":
		return p.handleDeleteAccessKey(ctx, form)
	case "TagUser":
		return p.handleTagUser(ctx, form)
	case "UntagUser":
		return p.handleUntagUser(ctx, form)
	case "ListUserTags":
		return p.handleListUserTags(ctx, form)
	case "TagRole":
		return p.handleTagRole(ctx, form)
	case "UntagRole":
		return p.handleUntagRole(ctx, form)
	case "ListRoleTags":
		return p.handleListRoleTags(ctx, form)
	default:
		return iamXMLError("InvalidAction", fmt.Sprintf("unknown action: %s", action), http.StatusBadRequest), nil
	}
}

// ListResources returns all users and roles as plugin resources.
func (p *IAMProvider) ListResources(ctx context.Context) ([]plugin.Resource, error) {
	users, err := p.store.ListUsers(defaultAccountID)
	if err != nil {
		return nil, err
	}
	roles, err := p.store.ListRoles(defaultAccountID)
	if err != nil {
		return nil, err
	}

	resources := make([]plugin.Resource, 0, len(users)+len(roles))
	for _, u := range users {
		resources = append(resources, plugin.Resource{
			Type: "user",
			ID:   u.UserID,
			Name: u.UserName,
		})
	}
	for _, r := range roles {
		resources = append(resources, plugin.Resource{
			Type: "role",
			ID:   r.RoleID,
			Name: r.RoleName,
		})
	}
	return resources, nil
}

// GetMetrics returns empty metrics.
func (p *IAMProvider) GetMetrics(_ context.Context) (*plugin.ServiceMetrics, error) {
	return &plugin.ServiceMetrics{}, nil
}

// Store returns the underlying IAMStore, allowing other providers (e.g. STS)
// to share the same database instance.
func (p *IAMProvider) Store() *IAMStore {
	return p.store
}

// --- XML response structs ---

type createUserResponse struct {
	XMLName          xml.Name         `xml:"CreateUserResponse"`
	CreateUserResult createUserResult `xml:"CreateUserResult"`
}

type createUserResult struct {
	User userXML `xml:"User"`
}

type userXML struct {
	UserName string `xml:"UserName"`
	UserID   string `xml:"UserId"`
	Arn      string `xml:"Arn"`
}

type listUsersResponse struct {
	XMLName         xml.Name        `xml:"ListUsersResponse"`
	ListUsersResult listUsersResult `xml:"ListUsersResult"`
}

type listUsersResult struct {
	Users []userXML `xml:"Users>member"`
}

type createRoleResponse struct {
	XMLName          xml.Name         `xml:"CreateRoleResponse"`
	CreateRoleResult createRoleResult `xml:"CreateRoleResult"`
}

type createRoleResult struct {
	Role roleXML `xml:"Role"`
}

type roleXML struct {
	RoleName string `xml:"RoleName"`
	RoleID   string `xml:"RoleId"`
	Arn      string `xml:"Arn"`
}

type listRolesResponse struct {
	XMLName         xml.Name        `xml:"ListRolesResponse"`
	ListRolesResult listRolesResult `xml:"ListRolesResult"`
}

type listRolesResult struct {
	Roles []roleXML `xml:"Roles>member"`
}

type deleteUserResponse struct {
	XMLName xml.Name `xml:"DeleteUserResponse"`
}

type createPolicyResponse struct {
	XMLName            xml.Name           `xml:"CreatePolicyResponse"`
	CreatePolicyResult createPolicyResult `xml:"CreatePolicyResult"`
}

type createPolicyResult struct {
	Policy policyXML `xml:"Policy"`
}

type policyXML struct {
	PolicyName string `xml:"PolicyName"`
	PolicyID   string `xml:"PolicyId"`
	Arn        string `xml:"Arn"`
}

type attachRolePolicyResponse struct {
	XMLName xml.Name `xml:"AttachRolePolicyResponse"`
}

type listAttachedRolePoliciesResponse struct {
	XMLName                        xml.Name                       `xml:"ListAttachedRolePoliciesResponse"`
	ListAttachedRolePoliciesResult listAttachedRolePoliciesResult `xml:"ListAttachedRolePoliciesResult"`
}

type listAttachedRolePoliciesResult struct {
	AttachedPolicies []attachedPolicyXML `xml:"AttachedPolicies>member"`
}

type attachedPolicyXML struct {
	PolicyArn  string `xml:"PolicyArn"`
	PolicyName string `xml:"PolicyName"`
}

type createAccessKeyResponse struct {
	XMLName               xml.Name              `xml:"CreateAccessKeyResponse"`
	CreateAccessKeyResult createAccessKeyResult `xml:"CreateAccessKeyResult"`
}

type createAccessKeyResult struct {
	AccessKey accessKeyXML `xml:"AccessKey"`
}

type accessKeyXML struct {
	AccessKeyID     string `xml:"AccessKeyId"`
	SecretAccessKey string `xml:"SecretAccessKey"`
	UserName        string `xml:"UserName"`
	Status          string `xml:"Status"`
}

type getUserResponse struct {
	XMLName       xml.Name      `xml:"GetUserResponse"`
	GetUserResult getUserResult `xml:"GetUserResult"`
}

type getUserResult struct {
	User userXML `xml:"User"`
}

type getRoleResponse struct {
	XMLName       xml.Name      `xml:"GetRoleResponse"`
	GetRoleResult getRoleResult `xml:"GetRoleResult"`
}

type getRoleResult struct {
	Role roleXML `xml:"Role"`
}

type deleteRoleResponse struct {
	XMLName xml.Name `xml:"DeleteRoleResponse"`
}

type updateAssumeRolePolicyResponse struct {
	XMLName xml.Name `xml:"UpdateAssumeRolePolicyResponse"`
}

type updateUserResponse struct {
	XMLName xml.Name `xml:"UpdateUserResponse"`
}

type getPolicyResponse struct {
	XMLName         xml.Name        `xml:"GetPolicyResponse"`
	GetPolicyResult getPolicyResult `xml:"GetPolicyResult"`
}

type getPolicyResult struct {
	Policy policyXML `xml:"Policy"`
}

type deletePolicyResponse struct {
	XMLName xml.Name `xml:"DeletePolicyResponse"`
}

type detachRolePolicyResponse struct {
	XMLName xml.Name `xml:"DetachRolePolicyResponse"`
}

type attachUserPolicyResponse struct {
	XMLName xml.Name `xml:"AttachUserPolicyResponse"`
}

type detachUserPolicyResponse struct {
	XMLName xml.Name `xml:"DetachUserPolicyResponse"`
}

type listAttachedUserPoliciesResponse struct {
	XMLName                        xml.Name                       `xml:"ListAttachedUserPoliciesResponse"`
	ListAttachedUserPoliciesResult listAttachedUserPoliciesResult `xml:"ListAttachedUserPoliciesResult"`
}

type listAttachedUserPoliciesResult struct {
	AttachedPolicies []attachedPolicyXML `xml:"AttachedPolicies>member"`
}

type getPolicyVersionResponse struct {
	XMLName                xml.Name               `xml:"GetPolicyVersionResponse"`
	GetPolicyVersionResult getPolicyVersionResult `xml:"GetPolicyVersionResult"`
}

type getPolicyVersionResult struct {
	PolicyVersion policyVersionXML `xml:"PolicyVersion"`
}

type policyVersionXML struct {
	VersionId string `xml:"VersionId"`
	Document  string `xml:"Document"`
	IsDefault bool   `xml:"IsDefaultVersion"`
}

type createPolicyVersionResponse struct {
	XMLName                   xml.Name                  `xml:"CreatePolicyVersionResponse"`
	CreatePolicyVersionResult createPolicyVersionResult `xml:"CreatePolicyVersionResult"`
}

type createPolicyVersionResult struct {
	PolicyVersion policyVersionXML `xml:"PolicyVersion"`
}

type listPolicyVersionsResponse struct {
	XMLName                  xml.Name                 `xml:"ListPolicyVersionsResponse"`
	ListPolicyVersionsResult listPolicyVersionsResult `xml:"ListPolicyVersionsResult"`
}

type listPolicyVersionsResult struct {
	Versions []policyVersionXML `xml:"Versions>member"`
}

type iamErrorResponse struct {
	XMLName xml.Name `xml:"ErrorResponse"`
	Error   iamError `xml:"Error"`
}

type iamError struct {
	Code    string `xml:"Code"`
	Message string `xml:"Message"`
}

// --- helpers ---

func iamXMLError(code, message string, status int) *plugin.Response {
	body, _ := xml.Marshal(iamErrorResponse{Error: iamError{Code: code, Message: message}})
	return &plugin.Response{
		StatusCode:  status,
		ContentType: "text/xml",
		Body:        body,
	}
}

func iamXMLResponse(status int, v any) (*plugin.Response, error) {
	body, err := xml.Marshal(v)
	if err != nil {
		return nil, err
	}
	return &plugin.Response{
		StatusCode:  status,
		ContentType: "text/xml",
		Body:        body,
	}, nil
}

// --- IAM operation implementations ---

func (p *IAMProvider) handleCreateUser(_ context.Context, form url.Values) (*plugin.Response, error) {
	userName := form.Get("UserName")
	if userName == "" {
		return iamXMLError("MissingParameter", "UserName is required", http.StatusBadRequest), nil
	}

	user, err := p.store.CreateUser(defaultAccountID, userName)
	if err != nil {
		return iamXMLError("EntityAlreadyExists", fmt.Sprintf("user %q already exists", userName), http.StatusConflict), nil
	}

	return iamXMLResponse(http.StatusOK, createUserResponse{
		CreateUserResult: createUserResult{
			User: userXML{
				UserName: user.UserName,
				UserID:   user.UserID,
				Arn:      user.Arn,
			},
		},
	})
}

func (p *IAMProvider) handleListUsers(_ context.Context, _ url.Values) (*plugin.Response, error) {
	users, err := p.store.ListUsers(defaultAccountID)
	if err != nil {
		return nil, err
	}

	members := make([]userXML, 0, len(users))
	for _, u := range users {
		members = append(members, userXML{
			UserName: u.UserName,
			UserID:   u.UserID,
			Arn:      u.Arn,
		})
	}

	return iamXMLResponse(http.StatusOK, listUsersResponse{
		ListUsersResult: listUsersResult{
			Users: members,
		},
	})
}

func (p *IAMProvider) handleCreateRole(_ context.Context, form url.Values) (*plugin.Response, error) {
	roleName := form.Get("RoleName")
	if roleName == "" {
		return iamXMLError("MissingParameter", "RoleName is required", http.StatusBadRequest), nil
	}
	assumeRolePolicy := form.Get("AssumeRolePolicyDocument")

	role, err := p.store.CreateRole(defaultAccountID, roleName, assumeRolePolicy)
	if err != nil {
		return iamXMLError("EntityAlreadyExists", fmt.Sprintf("role %q already exists", roleName), http.StatusConflict), nil
	}

	return iamXMLResponse(http.StatusOK, createRoleResponse{
		CreateRoleResult: createRoleResult{
			Role: roleXML{
				RoleName: role.RoleName,
				RoleID:   role.RoleID,
				Arn:      role.Arn,
			},
		},
	})
}

func (p *IAMProvider) handleListRoles(_ context.Context, _ url.Values) (*plugin.Response, error) {
	roles, err := p.store.ListRoles(defaultAccountID)
	if err != nil {
		return nil, err
	}

	members := make([]roleXML, 0, len(roles))
	for _, r := range roles {
		members = append(members, roleXML{
			RoleName: r.RoleName,
			RoleID:   r.RoleID,
			Arn:      r.Arn,
		})
	}

	return iamXMLResponse(http.StatusOK, listRolesResponse{
		ListRolesResult: listRolesResult{
			Roles: members,
		},
	})
}

func (p *IAMProvider) handleDeleteUser(_ context.Context, form url.Values) (*plugin.Response, error) {
	userName := form.Get("UserName")
	if userName == "" {
		return iamXMLError("MissingParameter", "UserName is required", http.StatusBadRequest), nil
	}

	err := p.store.DeleteUser(defaultAccountID, userName)
	if err != nil {
		if err == ErrUserNotFound {
			return iamXMLError("NoSuchEntity", fmt.Sprintf("The user with name %s cannot be found.", userName), http.StatusNotFound), nil
		}
		return nil, err
	}

	return iamXMLResponse(http.StatusOK, deleteUserResponse{})
}

func (p *IAMProvider) handleCreatePolicy(_ context.Context, form url.Values) (*plugin.Response, error) {
	policyName := form.Get("PolicyName")
	policyDocument := form.Get("PolicyDocument")
	if policyName == "" {
		return iamXMLError("MissingParameter", "PolicyName is required", http.StatusBadRequest), nil
	}

	policy, err := p.store.CreatePolicy(defaultAccountID, policyName, policyDocument)
	if err != nil {
		if err == ErrPolicyAlreadyExists {
			return iamXMLError("EntityAlreadyExists", fmt.Sprintf("policy %q already exists", policyName), http.StatusConflict), nil
		}
		return nil, err
	}

	return iamXMLResponse(http.StatusOK, createPolicyResponse{
		CreatePolicyResult: createPolicyResult{
			Policy: policyXML{
				PolicyName: policy.PolicyName,
				PolicyID:   policy.PolicyID,
				Arn:        policy.Arn,
			},
		},
	})
}

func (p *IAMProvider) handleListAttachedRolePolicies(_ context.Context, form url.Values) (*plugin.Response, error) {
	roleName := form.Get("RoleName")
	if roleName == "" {
		return iamXMLError("MissingParameter", "RoleName is required", http.StatusBadRequest), nil
	}

	arns, err := p.store.ListAttachedRolePolicies(defaultAccountID, roleName)
	if err != nil {
		return nil, err
	}

	members := make([]attachedPolicyXML, 0, len(arns))
	for _, arn := range arns {
		// Extract policy name from ARN: arn:aws:iam::ACCOUNT:policy/NAME
		name := arn
		if i := strings.LastIndex(arn, "policy/"); i >= 0 {
			name = arn[i+len("policy/"):]
		}
		members = append(members, attachedPolicyXML{
			PolicyArn:  arn,
			PolicyName: name,
		})
	}

	return iamXMLResponse(http.StatusOK, listAttachedRolePoliciesResponse{
		ListAttachedRolePoliciesResult: listAttachedRolePoliciesResult{
			AttachedPolicies: members,
		},
	})
}

func (p *IAMProvider) handleAttachRolePolicy(_ context.Context, form url.Values) (*plugin.Response, error) {
	roleName := form.Get("RoleName")
	policyArn := form.Get("PolicyArn")
	if roleName == "" || policyArn == "" {
		return iamXMLError("MissingParameter", "RoleName and PolicyArn are required", http.StatusBadRequest), nil
	}

	if err := p.store.AttachRolePolicy(defaultAccountID, roleName, policyArn); err != nil {
		return nil, err
	}

	return iamXMLResponse(http.StatusOK, attachRolePolicyResponse{})
}

func (p *IAMProvider) handleCreateAccessKey(_ context.Context, form url.Values) (*plugin.Response, error) {
	userName := form.Get("UserName")
	if userName == "" {
		return iamXMLError("MissingParameter", "UserName is required", http.StatusBadRequest), nil
	}

	key, err := p.store.CreateAccessKey(defaultAccountID, userName)
	if err != nil {
		return nil, err
	}

	return iamXMLResponse(http.StatusOK, createAccessKeyResponse{
		CreateAccessKeyResult: createAccessKeyResult{
			AccessKey: accessKeyXML{
				AccessKeyID:     key.AccessKeyID,
				SecretAccessKey: key.SecretAccessKey,
				UserName:        key.UserName,
				Status:          key.Status,
			},
		},
	})
}

func (p *IAMProvider) handleGetUser(_ context.Context, form url.Values) (*plugin.Response, error) {
	userName := form.Get("UserName")
	if userName == "" {
		return iamXMLError("MissingParameter", "UserName is required", http.StatusBadRequest), nil
	}

	user, err := p.store.GetUser(defaultAccountID, userName)
	if err != nil {
		if err == ErrUserNotFound {
			return iamXMLError("NoSuchEntity", fmt.Sprintf("The user with name %s cannot be found.", userName), http.StatusNotFound), nil
		}
		return nil, err
	}

	return iamXMLResponse(http.StatusOK, getUserResponse{
		GetUserResult: getUserResult{
			User: userXML{
				UserName: user.UserName,
				UserID:   user.UserID,
				Arn:      user.Arn,
			},
		},
	})
}

func (p *IAMProvider) handleUpdateUser(_ context.Context, form url.Values) (*plugin.Response, error) {
	userName := form.Get("UserName")
	newUserName := form.Get("NewUserName")
	if userName == "" {
		return iamXMLError("MissingParameter", "UserName is required", http.StatusBadRequest), nil
	}
	if newUserName == "" {
		return iamXMLError("MissingParameter", "NewUserName is required", http.StatusBadRequest), nil
	}

	err := p.store.UpdateUser(defaultAccountID, userName, newUserName)
	if err != nil {
		if err == ErrUserNotFound {
			return iamXMLError("NoSuchEntity", fmt.Sprintf("The user with name %s cannot be found.", userName), http.StatusNotFound), nil
		}
		return nil, err
	}

	return iamXMLResponse(http.StatusOK, updateUserResponse{})
}

func (p *IAMProvider) handleGetRole(_ context.Context, form url.Values) (*plugin.Response, error) {
	roleName := form.Get("RoleName")
	if roleName == "" {
		return iamXMLError("MissingParameter", "RoleName is required", http.StatusBadRequest), nil
	}

	role, err := p.store.GetRole(defaultAccountID, roleName)
	if err != nil {
		if err == ErrRoleNotFound {
			return iamXMLError("NoSuchEntity", fmt.Sprintf("The role with name %s cannot be found.", roleName), http.StatusNotFound), nil
		}
		return nil, err
	}

	return iamXMLResponse(http.StatusOK, getRoleResponse{
		GetRoleResult: getRoleResult{
			Role: roleXML{
				RoleName: role.RoleName,
				RoleID:   role.RoleID,
				Arn:      role.Arn,
			},
		},
	})
}

func (p *IAMProvider) handleDeleteRole(_ context.Context, form url.Values) (*plugin.Response, error) {
	roleName := form.Get("RoleName")
	if roleName == "" {
		return iamXMLError("MissingParameter", "RoleName is required", http.StatusBadRequest), nil
	}

	err := p.store.DeleteRole(defaultAccountID, roleName)
	if err != nil {
		if err == ErrRoleNotFound {
			return iamXMLError("NoSuchEntity", fmt.Sprintf("The role with name %s cannot be found.", roleName), http.StatusNotFound), nil
		}
		return nil, err
	}

	return iamXMLResponse(http.StatusOK, deleteRoleResponse{})
}

func (p *IAMProvider) handleUpdateAssumeRolePolicy(_ context.Context, form url.Values) (*plugin.Response, error) {
	roleName := form.Get("RoleName")
	policyDocument := form.Get("PolicyDocument")
	if roleName == "" {
		return iamXMLError("MissingParameter", "RoleName is required", http.StatusBadRequest), nil
	}
	if policyDocument == "" {
		return iamXMLError("MissingParameter", "PolicyDocument is required", http.StatusBadRequest), nil
	}

	err := p.store.UpdateAssumeRolePolicy(defaultAccountID, roleName, policyDocument)
	if err != nil {
		if err == ErrRoleNotFound {
			return iamXMLError("NoSuchEntity", fmt.Sprintf("The role with name %s cannot be found.", roleName), http.StatusNotFound), nil
		}
		return nil, err
	}

	return iamXMLResponse(http.StatusOK, updateAssumeRolePolicyResponse{})
}

func (p *IAMProvider) handleGetPolicy(_ context.Context, form url.Values) (*plugin.Response, error) {
	policyArn := form.Get("PolicyArn")
	if policyArn == "" {
		return iamXMLError("MissingParameter", "PolicyArn is required", http.StatusBadRequest), nil
	}

	policy, err := p.store.GetPolicyByArn(policyArn)
	if err != nil {
		if err == ErrPolicyNotFound {
			return iamXMLError("NoSuchEntity", fmt.Sprintf("The policy with ARN %s cannot be found.", policyArn), http.StatusNotFound), nil
		}
		return nil, err
	}

	return iamXMLResponse(http.StatusOK, getPolicyResponse{
		GetPolicyResult: getPolicyResult{
			Policy: policyXML{
				PolicyName: policy.PolicyName,
				PolicyID:   policy.PolicyID,
				Arn:        policy.Arn,
			},
		},
	})
}

func (p *IAMProvider) handleDeletePolicy(_ context.Context, form url.Values) (*plugin.Response, error) {
	policyArn := form.Get("PolicyArn")
	if policyArn == "" {
		return iamXMLError("MissingParameter", "PolicyArn is required", http.StatusBadRequest), nil
	}

	err := p.store.DeletePolicy(defaultAccountID, policyArn)
	if err != nil {
		if err == ErrPolicyNotFound {
			return iamXMLError("NoSuchEntity", fmt.Sprintf("The policy with ARN %s cannot be found.", policyArn), http.StatusNotFound), nil
		}
		return nil, err
	}

	return iamXMLResponse(http.StatusOK, deletePolicyResponse{})
}

func (p *IAMProvider) handleDetachRolePolicy(_ context.Context, form url.Values) (*plugin.Response, error) {
	roleName := form.Get("RoleName")
	policyArn := form.Get("PolicyArn")
	if roleName == "" || policyArn == "" {
		return iamXMLError("MissingParameter", "RoleName and PolicyArn are required", http.StatusBadRequest), nil
	}

	err := p.store.DetachRolePolicy(defaultAccountID, roleName, policyArn)
	if err != nil {
		if err == ErrPolicyNotFound {
			return iamXMLError("NoSuchEntity", fmt.Sprintf("The policy with ARN %s is not attached to role %s.", policyArn, roleName), http.StatusNotFound), nil
		}
		return nil, err
	}

	return iamXMLResponse(http.StatusOK, detachRolePolicyResponse{})
}

func (p *IAMProvider) handleAttachUserPolicy(_ context.Context, form url.Values) (*plugin.Response, error) {
	userName := form.Get("UserName")
	policyArn := form.Get("PolicyArn")
	if userName == "" || policyArn == "" {
		return iamXMLError("MissingParameter", "UserName and PolicyArn are required", http.StatusBadRequest), nil
	}

	if err := p.store.AttachUserPolicy(defaultAccountID, userName, policyArn); err != nil {
		return nil, err
	}

	return iamXMLResponse(http.StatusOK, attachUserPolicyResponse{})
}

func (p *IAMProvider) handleDetachUserPolicy(_ context.Context, form url.Values) (*plugin.Response, error) {
	userName := form.Get("UserName")
	policyArn := form.Get("PolicyArn")
	if userName == "" || policyArn == "" {
		return iamXMLError("MissingParameter", "UserName and PolicyArn are required", http.StatusBadRequest), nil
	}

	err := p.store.DetachUserPolicy(defaultAccountID, userName, policyArn)
	if err != nil {
		if err == ErrPolicyNotFound {
			return iamXMLError("NoSuchEntity", fmt.Sprintf("The policy with ARN %s is not attached to user %s.", policyArn, userName), http.StatusNotFound), nil
		}
		return nil, err
	}

	return iamXMLResponse(http.StatusOK, detachUserPolicyResponse{})
}

func (p *IAMProvider) handleListAttachedUserPolicies(_ context.Context, form url.Values) (*plugin.Response, error) {
	userName := form.Get("UserName")
	if userName == "" {
		return iamXMLError("MissingParameter", "UserName is required", http.StatusBadRequest), nil
	}

	arns, err := p.store.ListAttachedUserPolicies(defaultAccountID, userName)
	if err != nil {
		return nil, err
	}

	members := make([]attachedPolicyXML, 0, len(arns))
	for _, arn := range arns {
		name := arn
		if i := strings.LastIndex(arn, "policy/"); i >= 0 {
			name = arn[i+len("policy/"):]
		}
		members = append(members, attachedPolicyXML{
			PolicyArn:  arn,
			PolicyName: name,
		})
	}

	return iamXMLResponse(http.StatusOK, listAttachedUserPoliciesResponse{
		ListAttachedUserPoliciesResult: listAttachedUserPoliciesResult{
			AttachedPolicies: members,
		},
	})
}

func (p *IAMProvider) handleGetPolicyVersion(_ context.Context, form url.Values) (*plugin.Response, error) {
	policyArn := form.Get("PolicyArn")
	versionID := form.Get("VersionId")
	if policyArn == "" || versionID == "" {
		return iamXMLError("MissingParameter", "PolicyArn and VersionId are required", http.StatusBadRequest), nil
	}

	pv, err := p.store.GetPolicyVersion(policyArn, versionID)
	if err != nil {
		if err == ErrPolicyNotFound {
			return iamXMLError("NoSuchEntity", fmt.Sprintf("The policy version %s for ARN %s cannot be found.", versionID, policyArn), http.StatusNotFound), nil
		}
		return nil, err
	}

	return iamXMLResponse(http.StatusOK, getPolicyVersionResponse{
		GetPolicyVersionResult: getPolicyVersionResult{
			PolicyVersion: policyVersionXML{
				VersionId: pv.VersionID,
				Document:  pv.Document,
				IsDefault: pv.IsDefault,
			},
		},
	})
}

func (p *IAMProvider) handleCreatePolicyVersion(_ context.Context, form url.Values) (*plugin.Response, error) {
	policyArn := form.Get("PolicyArn")
	policyDocument := form.Get("PolicyDocument")
	setAsDefault := form.Get("SetAsDefault") == "true"
	if policyArn == "" || policyDocument == "" {
		return iamXMLError("MissingParameter", "PolicyArn and PolicyDocument are required", http.StatusBadRequest), nil
	}

	pv, err := p.store.CreatePolicyVersion(policyArn, policyDocument, setAsDefault)
	if err != nil {
		return nil, err
	}

	return iamXMLResponse(http.StatusOK, createPolicyVersionResponse{
		CreatePolicyVersionResult: createPolicyVersionResult{
			PolicyVersion: policyVersionXML{
				VersionId: pv.VersionID,
				Document:  pv.Document,
				IsDefault: pv.IsDefault,
			},
		},
	})
}

func (p *IAMProvider) handleListPolicyVersions(_ context.Context, form url.Values) (*plugin.Response, error) {
	policyArn := form.Get("PolicyArn")
	if policyArn == "" {
		return iamXMLError("MissingParameter", "PolicyArn is required", http.StatusBadRequest), nil
	}

	versions, err := p.store.ListPolicyVersions(policyArn)
	if err != nil {
		return nil, err
	}

	members := make([]policyVersionXML, 0, len(versions))
	for _, pv := range versions {
		members = append(members, policyVersionXML{
			VersionId: pv.VersionID,
			Document:  pv.Document,
			IsDefault: pv.IsDefault,
		})
	}

	return iamXMLResponse(http.StatusOK, listPolicyVersionsResponse{
		ListPolicyVersionsResult: listPolicyVersionsResult{
			Versions: members,
		},
	})
}

// =============================================================================
// Task 4: Inline Policies
// =============================================================================

// --- XML structs ---

type putUserPolicyResponse struct {
	XMLName xml.Name `xml:"PutUserPolicyResponse"`
}

type getUserPolicyResponse struct {
	XMLName             xml.Name            `xml:"GetUserPolicyResponse"`
	GetUserPolicyResult getUserPolicyResult `xml:"GetUserPolicyResult"`
}

type getUserPolicyResult struct {
	UserName       string `xml:"UserName"`
	PolicyName     string `xml:"PolicyName"`
	PolicyDocument string `xml:"PolicyDocument"`
}

type deleteUserPolicyResponse struct {
	XMLName xml.Name `xml:"DeleteUserPolicyResponse"`
}

type listUserPoliciesResponse struct {
	XMLName                xml.Name               `xml:"ListUserPoliciesResponse"`
	ListUserPoliciesResult listUserPoliciesResult `xml:"ListUserPoliciesResult"`
}

type listUserPoliciesResult struct {
	PolicyNames []string `xml:"PolicyNames>member"`
}

type putRolePolicyResponse struct {
	XMLName xml.Name `xml:"PutRolePolicyResponse"`
}

type getRolePolicyResponse struct {
	XMLName             xml.Name            `xml:"GetRolePolicyResponse"`
	GetRolePolicyResult getRolePolicyResult `xml:"GetRolePolicyResult"`
}

type getRolePolicyResult struct {
	RoleName       string `xml:"RoleName"`
	PolicyName     string `xml:"PolicyName"`
	PolicyDocument string `xml:"PolicyDocument"`
}

type deleteRolePolicyResponse struct {
	XMLName xml.Name `xml:"DeleteRolePolicyResponse"`
}

type listRolePoliciesResponse struct {
	XMLName                xml.Name               `xml:"ListRolePoliciesResponse"`
	ListRolePoliciesResult listRolePoliciesResult `xml:"ListRolePoliciesResult"`
}

type listRolePoliciesResult struct {
	PolicyNames []string `xml:"PolicyNames>member"`
}

// --- Handlers ---

func (p *IAMProvider) handlePutUserPolicy(_ context.Context, form url.Values) (*plugin.Response, error) {
	userName := form.Get("UserName")
	policyName := form.Get("PolicyName")
	policyDocument := form.Get("PolicyDocument")
	if userName == "" || policyName == "" {
		return iamXMLError("MissingParameter", "UserName and PolicyName are required", http.StatusBadRequest), nil
	}
	if err := p.store.PutUserInlinePolicy(defaultAccountID, userName, policyName, policyDocument); err != nil {
		return nil, err
	}
	return iamXMLResponse(http.StatusOK, putUserPolicyResponse{})
}

func (p *IAMProvider) handleGetUserPolicy(_ context.Context, form url.Values) (*plugin.Response, error) {
	userName := form.Get("UserName")
	policyName := form.Get("PolicyName")
	if userName == "" || policyName == "" {
		return iamXMLError("MissingParameter", "UserName and PolicyName are required", http.StatusBadRequest), nil
	}
	doc, err := p.store.GetUserInlinePolicy(defaultAccountID, userName, policyName)
	if err != nil {
		if err == ErrPolicyNotFound {
			return iamXMLError("NoSuchEntity", fmt.Sprintf("The policy %s cannot be found.", policyName), http.StatusNotFound), nil
		}
		return nil, err
	}
	return iamXMLResponse(http.StatusOK, getUserPolicyResponse{
		GetUserPolicyResult: getUserPolicyResult{
			UserName:       userName,
			PolicyName:     policyName,
			PolicyDocument: doc,
		},
	})
}

func (p *IAMProvider) handleDeleteUserPolicy(_ context.Context, form url.Values) (*plugin.Response, error) {
	userName := form.Get("UserName")
	policyName := form.Get("PolicyName")
	if userName == "" || policyName == "" {
		return iamXMLError("MissingParameter", "UserName and PolicyName are required", http.StatusBadRequest), nil
	}
	if err := p.store.DeleteUserInlinePolicy(defaultAccountID, userName, policyName); err != nil {
		if err == ErrPolicyNotFound {
			return iamXMLError("NoSuchEntity", fmt.Sprintf("The policy %s cannot be found.", policyName), http.StatusNotFound), nil
		}
		return nil, err
	}
	return iamXMLResponse(http.StatusOK, deleteUserPolicyResponse{})
}

func (p *IAMProvider) handleListUserPolicies(_ context.Context, form url.Values) (*plugin.Response, error) {
	userName := form.Get("UserName")
	if userName == "" {
		return iamXMLError("MissingParameter", "UserName is required", http.StatusBadRequest), nil
	}
	names, err := p.store.ListUserInlinePolicies(defaultAccountID, userName)
	if err != nil {
		return nil, err
	}
	if names == nil {
		names = []string{}
	}
	return iamXMLResponse(http.StatusOK, listUserPoliciesResponse{
		ListUserPoliciesResult: listUserPoliciesResult{PolicyNames: names},
	})
}

func (p *IAMProvider) handlePutRolePolicy(_ context.Context, form url.Values) (*plugin.Response, error) {
	roleName := form.Get("RoleName")
	policyName := form.Get("PolicyName")
	policyDocument := form.Get("PolicyDocument")
	if roleName == "" || policyName == "" {
		return iamXMLError("MissingParameter", "RoleName and PolicyName are required", http.StatusBadRequest), nil
	}
	if err := p.store.PutRoleInlinePolicy(defaultAccountID, roleName, policyName, policyDocument); err != nil {
		return nil, err
	}
	return iamXMLResponse(http.StatusOK, putRolePolicyResponse{})
}

func (p *IAMProvider) handleGetRolePolicy(_ context.Context, form url.Values) (*plugin.Response, error) {
	roleName := form.Get("RoleName")
	policyName := form.Get("PolicyName")
	if roleName == "" || policyName == "" {
		return iamXMLError("MissingParameter", "RoleName and PolicyName are required", http.StatusBadRequest), nil
	}
	doc, err := p.store.GetRoleInlinePolicy(defaultAccountID, roleName, policyName)
	if err != nil {
		if err == ErrPolicyNotFound {
			return iamXMLError("NoSuchEntity", fmt.Sprintf("The policy %s cannot be found.", policyName), http.StatusNotFound), nil
		}
		return nil, err
	}
	return iamXMLResponse(http.StatusOK, getRolePolicyResponse{
		GetRolePolicyResult: getRolePolicyResult{
			RoleName:       roleName,
			PolicyName:     policyName,
			PolicyDocument: doc,
		},
	})
}

func (p *IAMProvider) handleDeleteRolePolicy(_ context.Context, form url.Values) (*plugin.Response, error) {
	roleName := form.Get("RoleName")
	policyName := form.Get("PolicyName")
	if roleName == "" || policyName == "" {
		return iamXMLError("MissingParameter", "RoleName and PolicyName are required", http.StatusBadRequest), nil
	}
	if err := p.store.DeleteRoleInlinePolicy(defaultAccountID, roleName, policyName); err != nil {
		if err == ErrPolicyNotFound {
			return iamXMLError("NoSuchEntity", fmt.Sprintf("The policy %s cannot be found.", policyName), http.StatusNotFound), nil
		}
		return nil, err
	}
	return iamXMLResponse(http.StatusOK, deleteRolePolicyResponse{})
}

func (p *IAMProvider) handleListRolePolicies(_ context.Context, form url.Values) (*plugin.Response, error) {
	roleName := form.Get("RoleName")
	if roleName == "" {
		return iamXMLError("MissingParameter", "RoleName is required", http.StatusBadRequest), nil
	}
	names, err := p.store.ListRoleInlinePolicies(defaultAccountID, roleName)
	if err != nil {
		return nil, err
	}
	if names == nil {
		names = []string{}
	}
	return iamXMLResponse(http.StatusOK, listRolePoliciesResponse{
		ListRolePoliciesResult: listRolePoliciesResult{PolicyNames: names},
	})
}

// =============================================================================
// Task 5: Groups
// =============================================================================

// --- XML structs ---

type createGroupResponse struct {
	XMLName           xml.Name          `xml:"CreateGroupResponse"`
	CreateGroupResult createGroupResult `xml:"CreateGroupResult"`
}

type createGroupResult struct {
	Group groupXML `xml:"Group"`
}

type groupXML struct {
	GroupName string `xml:"GroupName"`
	GroupID   string `xml:"GroupId"`
	Arn       string `xml:"Arn"`
}

type deleteGroupResponse struct {
	XMLName xml.Name `xml:"DeleteGroupResponse"`
}

type getGroupResponse struct {
	XMLName        xml.Name       `xml:"GetGroupResponse"`
	GetGroupResult getGroupResult `xml:"GetGroupResult"`
}

type getGroupResult struct {
	Group groupXML       `xml:"Group"`
	Users []groupUserXML `xml:"Users>member"`
}

type groupUserXML struct {
	UserName string `xml:"UserName"`
}

type listGroupsResponse struct {
	XMLName          xml.Name         `xml:"ListGroupsResponse"`
	ListGroupsResult listGroupsResult `xml:"ListGroupsResult"`
}

type listGroupsResult struct {
	Groups []groupXML `xml:"Groups>member"`
}

type addUserToGroupResponse struct {
	XMLName xml.Name `xml:"AddUserToGroupResponse"`
}

type removeUserFromGroupResponse struct {
	XMLName xml.Name `xml:"RemoveUserFromGroupResponse"`
}

// --- Handlers ---

func (p *IAMProvider) handleCreateGroup(_ context.Context, form url.Values) (*plugin.Response, error) {
	groupName := form.Get("GroupName")
	if groupName == "" {
		return iamXMLError("MissingParameter", "GroupName is required", http.StatusBadRequest), nil
	}
	g, err := p.store.CreateGroup(defaultAccountID, groupName)
	if err != nil {
		if err == ErrGroupAlreadyExists {
			return iamXMLError("EntityAlreadyExists", fmt.Sprintf("group %q already exists", groupName), http.StatusConflict), nil
		}
		return nil, err
	}
	return iamXMLResponse(http.StatusOK, createGroupResponse{
		CreateGroupResult: createGroupResult{
			Group: groupXML{GroupName: g.GroupName, GroupID: g.GroupID, Arn: g.Arn},
		},
	})
}

func (p *IAMProvider) handleDeleteGroup(_ context.Context, form url.Values) (*plugin.Response, error) {
	groupName := form.Get("GroupName")
	if groupName == "" {
		return iamXMLError("MissingParameter", "GroupName is required", http.StatusBadRequest), nil
	}
	if err := p.store.DeleteGroup(defaultAccountID, groupName); err != nil {
		if err == ErrGroupNotFound {
			return iamXMLError("NoSuchEntity", fmt.Sprintf("The group %s cannot be found.", groupName), http.StatusNotFound), nil
		}
		return nil, err
	}
	return iamXMLResponse(http.StatusOK, deleteGroupResponse{})
}

func (p *IAMProvider) handleGetGroup(_ context.Context, form url.Values) (*plugin.Response, error) {
	groupName := form.Get("GroupName")
	if groupName == "" {
		return iamXMLError("MissingParameter", "GroupName is required", http.StatusBadRequest), nil
	}
	g, members, err := p.store.GetGroup(defaultAccountID, groupName)
	if err != nil {
		if err == ErrGroupNotFound {
			return iamXMLError("NoSuchEntity", fmt.Sprintf("The group %s cannot be found.", groupName), http.StatusNotFound), nil
		}
		return nil, err
	}
	users := make([]groupUserXML, 0, len(members))
	for _, u := range members {
		users = append(users, groupUserXML{UserName: u})
	}
	return iamXMLResponse(http.StatusOK, getGroupResponse{
		GetGroupResult: getGroupResult{
			Group: groupXML{GroupName: g.GroupName, GroupID: g.GroupID, Arn: g.Arn},
			Users: users,
		},
	})
}

func (p *IAMProvider) handleListGroups(_ context.Context, _ url.Values) (*plugin.Response, error) {
	groups, err := p.store.ListGroups(defaultAccountID)
	if err != nil {
		return nil, err
	}
	members := make([]groupXML, 0, len(groups))
	for _, g := range groups {
		members = append(members, groupXML{GroupName: g.GroupName, GroupID: g.GroupID, Arn: g.Arn})
	}
	return iamXMLResponse(http.StatusOK, listGroupsResponse{
		ListGroupsResult: listGroupsResult{Groups: members},
	})
}

func (p *IAMProvider) handleAddUserToGroup(_ context.Context, form url.Values) (*plugin.Response, error) {
	groupName := form.Get("GroupName")
	userName := form.Get("UserName")
	if groupName == "" || userName == "" {
		return iamXMLError("MissingParameter", "GroupName and UserName are required", http.StatusBadRequest), nil
	}
	if err := p.store.AddUserToGroup(defaultAccountID, groupName, userName); err != nil {
		return nil, err
	}
	return iamXMLResponse(http.StatusOK, addUserToGroupResponse{})
}

func (p *IAMProvider) handleRemoveUserFromGroup(_ context.Context, form url.Values) (*plugin.Response, error) {
	groupName := form.Get("GroupName")
	userName := form.Get("UserName")
	if groupName == "" || userName == "" {
		return iamXMLError("MissingParameter", "GroupName and UserName are required", http.StatusBadRequest), nil
	}
	if err := p.store.RemoveUserFromGroup(defaultAccountID, groupName, userName); err != nil {
		return nil, err
	}
	return iamXMLResponse(http.StatusOK, removeUserFromGroupResponse{})
}

// =============================================================================
// Task 6: Instance Profiles
// =============================================================================

// --- XML structs ---

type createInstanceProfileResponse struct {
	XMLName                     xml.Name                    `xml:"CreateInstanceProfileResponse"`
	CreateInstanceProfileResult createInstanceProfileResult `xml:"CreateInstanceProfileResult"`
}

type createInstanceProfileResult struct {
	InstanceProfile instanceProfileXML `xml:"InstanceProfile"`
}

type instanceProfileXML struct {
	InstanceProfileName string      `xml:"InstanceProfileName"`
	InstanceProfileId   string      `xml:"InstanceProfileId"`
	Arn                 string      `xml:"Arn"`
	Roles               []ipRoleXML `xml:"Roles>member"`
}

type ipRoleXML struct {
	RoleName string `xml:"RoleName"`
}

type deleteInstanceProfileResponse struct {
	XMLName xml.Name `xml:"DeleteInstanceProfileResponse"`
}

type getInstanceProfileResponse struct {
	XMLName                  xml.Name                 `xml:"GetInstanceProfileResponse"`
	GetInstanceProfileResult getInstanceProfileResult `xml:"GetInstanceProfileResult"`
}

type getInstanceProfileResult struct {
	InstanceProfile instanceProfileXML `xml:"InstanceProfile"`
}

type listInstanceProfilesResponse struct {
	XMLName                    xml.Name                   `xml:"ListInstanceProfilesResponse"`
	ListInstanceProfilesResult listInstanceProfilesResult `xml:"ListInstanceProfilesResult"`
}

type listInstanceProfilesResult struct {
	InstanceProfiles []instanceProfileXML `xml:"InstanceProfiles>member"`
}

type addRoleToInstanceProfileResponse struct {
	XMLName xml.Name `xml:"AddRoleToInstanceProfileResponse"`
}

type removeRoleFromInstanceProfileResponse struct {
	XMLName xml.Name `xml:"RemoveRoleFromInstanceProfileResponse"`
}

// --- Handlers ---

func (p *IAMProvider) handleCreateInstanceProfile(_ context.Context, form url.Values) (*plugin.Response, error) {
	profileName := form.Get("InstanceProfileName")
	if profileName == "" {
		return iamXMLError("MissingParameter", "InstanceProfileName is required", http.StatusBadRequest), nil
	}
	ip, err := p.store.CreateInstanceProfile(defaultAccountID, profileName)
	if err != nil {
		if err == ErrInstanceProfileAlreadyExists {
			return iamXMLError("EntityAlreadyExists", fmt.Sprintf("instance profile %q already exists", profileName), http.StatusConflict), nil
		}
		return nil, err
	}
	return iamXMLResponse(http.StatusOK, createInstanceProfileResponse{
		CreateInstanceProfileResult: createInstanceProfileResult{
			InstanceProfile: instanceProfileXML{
				InstanceProfileName: ip.ProfileName,
				InstanceProfileId:   ip.ProfileID,
				Arn:                 ip.Arn,
				Roles:               []ipRoleXML{},
			},
		},
	})
}

func (p *IAMProvider) handleDeleteInstanceProfile(_ context.Context, form url.Values) (*plugin.Response, error) {
	profileName := form.Get("InstanceProfileName")
	if profileName == "" {
		return iamXMLError("MissingParameter", "InstanceProfileName is required", http.StatusBadRequest), nil
	}
	if err := p.store.DeleteInstanceProfile(defaultAccountID, profileName); err != nil {
		if err == ErrInstanceProfileNotFound {
			return iamXMLError("NoSuchEntity", fmt.Sprintf("The instance profile %s cannot be found.", profileName), http.StatusNotFound), nil
		}
		return nil, err
	}
	return iamXMLResponse(http.StatusOK, deleteInstanceProfileResponse{})
}

func (p *IAMProvider) handleGetInstanceProfile(_ context.Context, form url.Values) (*plugin.Response, error) {
	profileName := form.Get("InstanceProfileName")
	if profileName == "" {
		return iamXMLError("MissingParameter", "InstanceProfileName is required", http.StatusBadRequest), nil
	}
	ip, roles, err := p.store.GetInstanceProfile(defaultAccountID, profileName)
	if err != nil {
		if err == ErrInstanceProfileNotFound {
			return iamXMLError("NoSuchEntity", fmt.Sprintf("The instance profile %s cannot be found.", profileName), http.StatusNotFound), nil
		}
		return nil, err
	}
	roleXMLs := make([]ipRoleXML, 0, len(roles))
	for _, r := range roles {
		roleXMLs = append(roleXMLs, ipRoleXML{RoleName: r})
	}
	return iamXMLResponse(http.StatusOK, getInstanceProfileResponse{
		GetInstanceProfileResult: getInstanceProfileResult{
			InstanceProfile: instanceProfileXML{
				InstanceProfileName: ip.ProfileName,
				InstanceProfileId:   ip.ProfileID,
				Arn:                 ip.Arn,
				Roles:               roleXMLs,
			},
		},
	})
}

func (p *IAMProvider) handleListInstanceProfiles(_ context.Context, _ url.Values) (*plugin.Response, error) {
	profiles, err := p.store.ListInstanceProfiles(defaultAccountID)
	if err != nil {
		return nil, err
	}
	members := make([]instanceProfileXML, 0, len(profiles))
	for _, ip := range profiles {
		members = append(members, instanceProfileXML{
			InstanceProfileName: ip.ProfileName,
			InstanceProfileId:   ip.ProfileID,
			Arn:                 ip.Arn,
			Roles:               []ipRoleXML{},
		})
	}
	return iamXMLResponse(http.StatusOK, listInstanceProfilesResponse{
		ListInstanceProfilesResult: listInstanceProfilesResult{InstanceProfiles: members},
	})
}

func (p *IAMProvider) handleAddRoleToInstanceProfile(_ context.Context, form url.Values) (*plugin.Response, error) {
	profileName := form.Get("InstanceProfileName")
	roleName := form.Get("RoleName")
	if profileName == "" || roleName == "" {
		return iamXMLError("MissingParameter", "InstanceProfileName and RoleName are required", http.StatusBadRequest), nil
	}
	if err := p.store.AddRoleToInstanceProfile(defaultAccountID, profileName, roleName); err != nil {
		return nil, err
	}
	return iamXMLResponse(http.StatusOK, addRoleToInstanceProfileResponse{})
}

func (p *IAMProvider) handleRemoveRoleFromInstanceProfile(_ context.Context, form url.Values) (*plugin.Response, error) {
	profileName := form.Get("InstanceProfileName")
	roleName := form.Get("RoleName")
	if profileName == "" || roleName == "" {
		return iamXMLError("MissingParameter", "InstanceProfileName and RoleName are required", http.StatusBadRequest), nil
	}
	if err := p.store.RemoveRoleFromInstanceProfile(defaultAccountID, profileName, roleName); err != nil {
		return nil, err
	}
	return iamXMLResponse(http.StatusOK, removeRoleFromInstanceProfileResponse{})
}

// =============================================================================
// Task 7: Access Keys & Tagging
// =============================================================================

// --- XML structs ---

type listAccessKeysResponse struct {
	XMLName              xml.Name             `xml:"ListAccessKeysResponse"`
	ListAccessKeysResult listAccessKeysResult `xml:"ListAccessKeysResult"`
}

type listAccessKeysResult struct {
	AccessKeyMetadata []accessKeyMetadataXML `xml:"AccessKeyMetadata>member"`
}

type accessKeyMetadataXML struct {
	UserName    string `xml:"UserName"`
	AccessKeyId string `xml:"AccessKeyId"`
	Status      string `xml:"Status"`
}

type updateAccessKeyResponse struct {
	XMLName xml.Name `xml:"UpdateAccessKeyResponse"`
}

type deleteAccessKeyResponse struct {
	XMLName xml.Name `xml:"DeleteAccessKeyResponse"`
}

type tagUserResponse struct {
	XMLName xml.Name `xml:"TagUserResponse"`
}

type untagUserResponse struct {
	XMLName xml.Name `xml:"UntagUserResponse"`
}

type listUserTagsResponse struct {
	XMLName            xml.Name           `xml:"ListUserTagsResponse"`
	ListUserTagsResult listUserTagsResult `xml:"ListUserTagsResult"`
}

type listUserTagsResult struct {
	Tags []tagXML `xml:"Tags>member"`
}

type tagXML struct {
	Key   string `xml:"Key"`
	Value string `xml:"Value"`
}

type tagRoleResponse struct {
	XMLName xml.Name `xml:"TagRoleResponse"`
}

type untagRoleResponse struct {
	XMLName xml.Name `xml:"UntagRoleResponse"`
}

type listRoleTagsResponse struct {
	XMLName            xml.Name           `xml:"ListRoleTagsResponse"`
	ListRoleTagsResult listRoleTagsResult `xml:"ListRoleTagsResult"`
}

type listRoleTagsResult struct {
	Tags []tagXML `xml:"Tags>member"`
}

// --- Handlers ---

func (p *IAMProvider) handleListAccessKeys(_ context.Context, form url.Values) (*plugin.Response, error) {
	userName := form.Get("UserName")
	if userName == "" {
		return iamXMLError("MissingParameter", "UserName is required", http.StatusBadRequest), nil
	}
	keys, err := p.store.ListAccessKeys(defaultAccountID, userName)
	if err != nil {
		return nil, err
	}
	metadata := make([]accessKeyMetadataXML, 0, len(keys))
	for _, k := range keys {
		metadata = append(metadata, accessKeyMetadataXML{
			UserName:    k.UserName,
			AccessKeyId: k.AccessKeyID,
			Status:      k.Status,
		})
	}
	return iamXMLResponse(http.StatusOK, listAccessKeysResponse{
		ListAccessKeysResult: listAccessKeysResult{AccessKeyMetadata: metadata},
	})
}

func (p *IAMProvider) handleUpdateAccessKey(_ context.Context, form url.Values) (*plugin.Response, error) {
	userName := form.Get("UserName")
	accessKeyID := form.Get("AccessKeyId")
	status := form.Get("Status")
	if userName == "" || accessKeyID == "" || status == "" {
		return iamXMLError("MissingParameter", "UserName, AccessKeyId and Status are required", http.StatusBadRequest), nil
	}
	if err := p.store.UpdateAccessKey(defaultAccountID, userName, accessKeyID, status); err != nil {
		return nil, err
	}
	return iamXMLResponse(http.StatusOK, updateAccessKeyResponse{})
}

func (p *IAMProvider) handleDeleteAccessKey(_ context.Context, form url.Values) (*plugin.Response, error) {
	userName := form.Get("UserName")
	accessKeyID := form.Get("AccessKeyId")
	if userName == "" || accessKeyID == "" {
		return iamXMLError("MissingParameter", "UserName and AccessKeyId are required", http.StatusBadRequest), nil
	}
	if err := p.store.DeleteAccessKey(defaultAccountID, userName, accessKeyID); err != nil {
		return nil, err
	}
	return iamXMLResponse(http.StatusOK, deleteAccessKeyResponse{})
}

func (p *IAMProvider) handleTagUser(_ context.Context, form url.Values) (*plugin.Response, error) {
	userName := form.Get("UserName")
	if userName == "" {
		return iamXMLError("MissingParameter", "UserName is required", http.StatusBadRequest), nil
	}
	tags := parseTags(form)
	if err := p.store.TagUser(defaultAccountID, userName, tags); err != nil {
		return nil, err
	}
	return iamXMLResponse(http.StatusOK, tagUserResponse{})
}

func (p *IAMProvider) handleUntagUser(_ context.Context, form url.Values) (*plugin.Response, error) {
	userName := form.Get("UserName")
	if userName == "" {
		return iamXMLError("MissingParameter", "UserName is required", http.StatusBadRequest), nil
	}
	keys := parseTagKeys(form)
	if err := p.store.UntagUser(defaultAccountID, userName, keys); err != nil {
		return nil, err
	}
	return iamXMLResponse(http.StatusOK, untagUserResponse{})
}

func (p *IAMProvider) handleListUserTags(_ context.Context, form url.Values) (*plugin.Response, error) {
	userName := form.Get("UserName")
	if userName == "" {
		return iamXMLError("MissingParameter", "UserName is required", http.StatusBadRequest), nil
	}
	tagMap, err := p.store.ListUserTags(defaultAccountID, userName)
	if err != nil {
		return nil, err
	}
	tags := make([]tagXML, 0, len(tagMap))
	for k, v := range tagMap {
		tags = append(tags, tagXML{Key: k, Value: v})
	}
	return iamXMLResponse(http.StatusOK, listUserTagsResponse{
		ListUserTagsResult: listUserTagsResult{Tags: tags},
	})
}

func (p *IAMProvider) handleTagRole(_ context.Context, form url.Values) (*plugin.Response, error) {
	roleName := form.Get("RoleName")
	if roleName == "" {
		return iamXMLError("MissingParameter", "RoleName is required", http.StatusBadRequest), nil
	}
	tags := parseTags(form)
	if err := p.store.TagRole(defaultAccountID, roleName, tags); err != nil {
		return nil, err
	}
	return iamXMLResponse(http.StatusOK, tagRoleResponse{})
}

func (p *IAMProvider) handleUntagRole(_ context.Context, form url.Values) (*plugin.Response, error) {
	roleName := form.Get("RoleName")
	if roleName == "" {
		return iamXMLError("MissingParameter", "RoleName is required", http.StatusBadRequest), nil
	}
	keys := parseTagKeys(form)
	if err := p.store.UntagRole(defaultAccountID, roleName, keys); err != nil {
		return nil, err
	}
	return iamXMLResponse(http.StatusOK, untagRoleResponse{})
}

func (p *IAMProvider) handleListRoleTags(_ context.Context, form url.Values) (*plugin.Response, error) {
	roleName := form.Get("RoleName")
	if roleName == "" {
		return iamXMLError("MissingParameter", "RoleName is required", http.StatusBadRequest), nil
	}
	tagMap, err := p.store.ListRoleTags(defaultAccountID, roleName)
	if err != nil {
		return nil, err
	}
	tags := make([]tagXML, 0, len(tagMap))
	for k, v := range tagMap {
		tags = append(tags, tagXML{Key: k, Value: v})
	}
	return iamXMLResponse(http.StatusOK, listRoleTagsResponse{
		ListRoleTagsResult: listRoleTagsResult{Tags: tags},
	})
}
