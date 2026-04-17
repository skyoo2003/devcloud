// SPDX-License-Identifier: Apache-2.0

// Package identitystore implements AWS Identity Store.
package identitystore

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

const defaultIdentityStoreID = "d-1234567890"

// IdentityStoreProvider implements plugin.ServicePlugin for Identity Store.
type IdentityStoreProvider struct {
	store *Store
}

func (p *IdentityStoreProvider) ServiceID() string             { return "identitystore" }
func (p *IdentityStoreProvider) ServiceName() string           { return "AWSIdentityStore" }
func (p *IdentityStoreProvider) Protocol() plugin.ProtocolType { return plugin.ProtocolJSON11 }

func (p *IdentityStoreProvider) Init(cfg plugin.PluginConfig) error {
	dataDir := cfg.DataDir
	if dataDir == "" {
		dataDir = "."
	}
	var err error
	p.store, err = NewStore(filepath.Join(dataDir, "identitystore"))
	return err
}

func (p *IdentityStoreProvider) Shutdown(_ context.Context) error {
	if p.store != nil {
		return p.store.Close()
	}
	return nil
}

func (p *IdentityStoreProvider) HandleRequest(_ context.Context, op string, req *http.Request) (*plugin.Response, error) {
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
	// User operations
	case "CreateUser":
		return p.createUser(params)
	case "DescribeUser":
		return p.describeUser(params)
	case "DeleteUser":
		return p.deleteUser(params)
	case "ListUsers":
		return p.listUsers(params)
	case "UpdateUser":
		return p.updateUser(params)
	case "GetUserId":
		return p.getUserID(params)
	// Group operations
	case "CreateGroup":
		return p.createGroup(params)
	case "DescribeGroup":
		return p.describeGroup(params)
	case "DeleteGroup":
		return p.deleteGroup(params)
	case "ListGroups":
		return p.listGroups(params)
	case "UpdateGroup":
		return p.updateGroup(params)
	case "GetGroupId":
		return p.getGroupID(params)
	// Group membership operations
	case "CreateGroupMembership":
		return p.createGroupMembership(params)
	case "DeleteGroupMembership":
		return p.deleteGroupMembership(params)
	case "ListGroupMemberships":
		return p.listGroupMemberships(params)
	case "DescribeGroupMembership":
		return p.describeGroupMembership(params)
	case "GetGroupMembershipId":
		return p.getGroupMembershipID(params)
	case "ListGroupMembershipsForMember":
		return p.listGroupMembershipsForMember(params)
	case "IsMemberInGroups":
		return p.isMemberInGroups(params)
	// Directory operations (stub)
	case "CreateDirectory":
		return p.createDirectory(params)
	case "DeleteDirectory":
		return p.deleteDirectory(params)
	case "DescribeDirectory":
		return p.describeDirectory(params)
	case "ListDirectories":
		return p.listDirectories(params)
	case "UpdateDirectory":
		return p.updateDirectory(params)
	// Identity Center passthroughs
	case "CreateExternalIdentityProviderConfigurationForApplication":
		return p.simpleOK()
	case "DeleteIdentityProviderConfigurationForApplication":
		return p.simpleOK()
	case "ListIdentityProvidersForApplication":
		return shared.JSONResponse(http.StatusOK, map[string]any{"IdentityProviders": []any{}})
	// Bulk / search helpers
	case "BatchCreateUsers":
		return p.batchCreateUsers(params)
	case "BatchCreateGroups":
		return p.batchCreateGroups(params)
	case "SearchUsers":
		return p.listUsers(params)
	case "SearchGroups":
		return p.listGroups(params)
	// Tag operations
	case "TagResource":
		return p.simpleOK()
	case "UntagResource":
		return p.simpleOK()
	case "ListTagsForResource":
		return shared.JSONResponse(http.StatusOK, map[string]any{"Tags": []any{}})
	// Status / lifecycle
	case "ActivateUser":
		return p.activateUser(params)
	case "DeactivateUser":
		return p.deactivateUser(params)
	case "ResetUserPassword":
		return p.simpleOK()
	default:
		return shared.JSONError("InvalidAction", fmt.Sprintf("unknown action: %s", action), http.StatusBadRequest), nil
	}
}

func (p *IdentityStoreProvider) ListResources(_ context.Context) ([]plugin.Resource, error) {
	users, err := p.store.ListUsers(defaultIdentityStoreID)
	if err != nil {
		return nil, err
	}
	res := make([]plugin.Resource, 0, len(users))
	for _, u := range users {
		res = append(res, plugin.Resource{Type: "identity-store-user", ID: u.UserID, Name: u.UserName})
	}
	return res, nil
}

func (p *IdentityStoreProvider) GetMetrics(_ context.Context) (*plugin.ServiceMetrics, error) {
	return &plugin.ServiceMetrics{}, nil
}

// --- User handlers ---

func (p *IdentityStoreProvider) createUser(params map[string]any) (*plugin.Response, error) {
	identityStoreID, _ := params["IdentityStoreId"].(string)
	if identityStoreID == "" {
		identityStoreID = defaultIdentityStoreID
	}
	userName, _ := params["UserName"].(string)
	if userName == "" {
		return shared.JSONError("ValidationException", "UserName is required", http.StatusBadRequest), nil
	}
	displayName, _ := params["DisplayName"].(string)
	email := ""
	if emails, ok := params["Emails"].([]any); ok && len(emails) > 0 {
		if em, ok := emails[0].(map[string]any); ok {
			email, _ = em["Value"].(string)
		}
	}

	user, err := p.store.CreateUser(identityStoreID, userName, displayName, email)
	if err != nil {
		if strings.Contains(err.Error(), "UNIQUE") {
			return shared.JSONError("ConflictException", "user already exists", http.StatusConflict), nil
		}
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"UserId":          user.UserID,
		"IdentityStoreId": user.IdentityStoreID,
	})
}

func (p *IdentityStoreProvider) describeUser(params map[string]any) (*plugin.Response, error) {
	identityStoreID, _ := params["IdentityStoreId"].(string)
	if identityStoreID == "" {
		identityStoreID = defaultIdentityStoreID
	}
	userID, _ := params["UserId"].(string)
	if userID == "" {
		return shared.JSONError("ValidationException", "UserId is required", http.StatusBadRequest), nil
	}
	user, err := p.store.GetUser(identityStoreID, userID)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "user not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, userToMap(user))
}

func (p *IdentityStoreProvider) deleteUser(params map[string]any) (*plugin.Response, error) {
	identityStoreID, _ := params["IdentityStoreId"].(string)
	if identityStoreID == "" {
		identityStoreID = defaultIdentityStoreID
	}
	userID, _ := params["UserId"].(string)
	if userID == "" {
		return shared.JSONError("ValidationException", "UserId is required", http.StatusBadRequest), nil
	}
	if err := p.store.DeleteUser(identityStoreID, userID); err != nil {
		return shared.JSONError("ResourceNotFoundException", "user not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *IdentityStoreProvider) listUsers(params map[string]any) (*plugin.Response, error) {
	identityStoreID, _ := params["IdentityStoreId"].(string)
	if identityStoreID == "" {
		identityStoreID = defaultIdentityStoreID
	}
	users, err := p.store.ListUsers(identityStoreID)
	if err != nil {
		return nil, err
	}
	result := make([]map[string]any, 0, len(users))
	for _, u := range users {
		result = append(result, userToMap(&u))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"Users":     result,
		"NextToken": nil,
	})
}

// --- Group handlers ---

func (p *IdentityStoreProvider) createGroup(params map[string]any) (*plugin.Response, error) {
	identityStoreID, _ := params["IdentityStoreId"].(string)
	if identityStoreID == "" {
		identityStoreID = defaultIdentityStoreID
	}
	displayName, _ := params["DisplayName"].(string)
	if displayName == "" {
		return shared.JSONError("ValidationException", "DisplayName is required", http.StatusBadRequest), nil
	}
	description, _ := params["Description"].(string)

	group, err := p.store.CreateGroup(identityStoreID, displayName, description)
	if err != nil {
		if strings.Contains(err.Error(), "UNIQUE") {
			return shared.JSONError("ConflictException", "group already exists", http.StatusConflict), nil
		}
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"GroupId":         group.GroupID,
		"IdentityStoreId": group.IdentityStoreID,
	})
}

func (p *IdentityStoreProvider) describeGroup(params map[string]any) (*plugin.Response, error) {
	identityStoreID, _ := params["IdentityStoreId"].(string)
	if identityStoreID == "" {
		identityStoreID = defaultIdentityStoreID
	}
	groupID, _ := params["GroupId"].(string)
	if groupID == "" {
		return shared.JSONError("ValidationException", "GroupId is required", http.StatusBadRequest), nil
	}
	group, err := p.store.GetGroup(identityStoreID, groupID)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "group not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, groupToMap(group))
}

func (p *IdentityStoreProvider) deleteGroup(params map[string]any) (*plugin.Response, error) {
	identityStoreID, _ := params["IdentityStoreId"].(string)
	if identityStoreID == "" {
		identityStoreID = defaultIdentityStoreID
	}
	groupID, _ := params["GroupId"].(string)
	if groupID == "" {
		return shared.JSONError("ValidationException", "GroupId is required", http.StatusBadRequest), nil
	}
	if err := p.store.DeleteGroup(identityStoreID, groupID); err != nil {
		return shared.JSONError("ResourceNotFoundException", "group not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *IdentityStoreProvider) listGroups(params map[string]any) (*plugin.Response, error) {
	identityStoreID, _ := params["IdentityStoreId"].(string)
	if identityStoreID == "" {
		identityStoreID = defaultIdentityStoreID
	}
	groups, err := p.store.ListGroups(identityStoreID)
	if err != nil {
		return nil, err
	}
	result := make([]map[string]any, 0, len(groups))
	for _, g := range groups {
		result = append(result, groupToMap(&g))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"Groups":    result,
		"NextToken": nil,
	})
}

// --- GroupMembership handlers ---

func (p *IdentityStoreProvider) createGroupMembership(params map[string]any) (*plugin.Response, error) {
	identityStoreID, _ := params["IdentityStoreId"].(string)
	if identityStoreID == "" {
		identityStoreID = defaultIdentityStoreID
	}
	groupID, _ := params["GroupId"].(string)
	if groupID == "" {
		return shared.JSONError("ValidationException", "GroupId is required", http.StatusBadRequest), nil
	}
	memberID := ""
	if member, ok := params["MemberId"].(map[string]any); ok {
		memberID, _ = member["UserId"].(string)
	}
	if memberID == "" {
		return shared.JSONError("ValidationException", "MemberId.UserId is required", http.StatusBadRequest), nil
	}

	membership, err := p.store.CreateGroupMembership(identityStoreID, groupID, memberID)
	if err != nil {
		if strings.Contains(err.Error(), "UNIQUE") {
			return shared.JSONError("ConflictException", "membership already exists", http.StatusConflict), nil
		}
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"MembershipId":    membership.MembershipID,
		"IdentityStoreId": membership.IdentityStoreID,
	})
}

func (p *IdentityStoreProvider) deleteGroupMembership(params map[string]any) (*plugin.Response, error) {
	identityStoreID, _ := params["IdentityStoreId"].(string)
	if identityStoreID == "" {
		identityStoreID = defaultIdentityStoreID
	}
	membershipID, _ := params["MembershipId"].(string)
	if membershipID == "" {
		return shared.JSONError("ValidationException", "MembershipId is required", http.StatusBadRequest), nil
	}
	if err := p.store.DeleteGroupMembership(identityStoreID, membershipID); err != nil {
		return shared.JSONError("ResourceNotFoundException", "membership not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *IdentityStoreProvider) listGroupMemberships(params map[string]any) (*plugin.Response, error) {
	identityStoreID, _ := params["IdentityStoreId"].(string)
	if identityStoreID == "" {
		identityStoreID = defaultIdentityStoreID
	}
	groupID, _ := params["GroupId"].(string)
	if groupID == "" {
		return shared.JSONError("ValidationException", "GroupId is required", http.StatusBadRequest), nil
	}
	memberships, err := p.store.ListGroupMemberships(identityStoreID, groupID)
	if err != nil {
		return nil, err
	}
	result := make([]map[string]any, 0, len(memberships))
	for _, m := range memberships {
		result = append(result, map[string]any{
			"MembershipId":    m.MembershipID,
			"IdentityStoreId": m.IdentityStoreID,
			"GroupId":         m.GroupID,
			"MemberId": map[string]any{
				"UserId": m.UserID,
			},
		})
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"GroupMemberships": result,
		"NextToken":        nil,
	})
}

// --- Extended handlers ---

func (p *IdentityStoreProvider) simpleOK() (*plugin.Response, error) {
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *IdentityStoreProvider) updateUser(params map[string]any) (*plugin.Response, error) {
	identityStoreID, _ := params["IdentityStoreId"].(string)
	if identityStoreID == "" {
		identityStoreID = defaultIdentityStoreID
	}
	userID, _ := params["UserId"].(string)
	if userID == "" {
		return shared.JSONError("ValidationException", "UserId is required", http.StatusBadRequest), nil
	}
	user, err := p.store.GetUser(identityStoreID, userID)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "user not found", http.StatusNotFound), nil
	}
	displayName := user.DisplayName
	email := user.Email
	if ops, ok := params["Operations"].([]any); ok {
		for _, op := range ops {
			m, ok := op.(map[string]any)
			if !ok {
				continue
			}
			attr, _ := m["AttributePath"].(string)
			if v, ok := m["AttributeValue"].(string); ok {
				switch attr {
				case "DisplayName":
					displayName = v
				case "Emails":
					email = v
				}
			}
		}
	}
	if err := p.store.UpdateUser(identityStoreID, userID, displayName, email); err != nil {
		return shared.JSONError("ResourceNotFoundException", "user not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *IdentityStoreProvider) getUserID(params map[string]any) (*plugin.Response, error) {
	identityStoreID, _ := params["IdentityStoreId"].(string)
	if identityStoreID == "" {
		identityStoreID = defaultIdentityStoreID
	}
	var userName string
	if alt, ok := params["AlternateIdentifier"].(map[string]any); ok {
		if id, ok := alt["UniqueAttribute"].(map[string]any); ok {
			if v, ok := id["AttributeValue"].(string); ok {
				userName = v
			}
		}
	}
	if userName == "" {
		userName, _ = params["UserName"].(string)
	}
	if userName == "" {
		return shared.JSONError("ValidationException", "UserName is required", http.StatusBadRequest), nil
	}
	user, err := p.store.GetUserByName(identityStoreID, userName)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "user not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"UserId":          user.UserID,
		"IdentityStoreId": user.IdentityStoreID,
	})
}

func (p *IdentityStoreProvider) updateGroup(params map[string]any) (*plugin.Response, error) {
	identityStoreID, _ := params["IdentityStoreId"].(string)
	if identityStoreID == "" {
		identityStoreID = defaultIdentityStoreID
	}
	groupID, _ := params["GroupId"].(string)
	if groupID == "" {
		return shared.JSONError("ValidationException", "GroupId is required", http.StatusBadRequest), nil
	}
	group, err := p.store.GetGroup(identityStoreID, groupID)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "group not found", http.StatusNotFound), nil
	}
	displayName := group.DisplayName
	description := group.Description
	if ops, ok := params["Operations"].([]any); ok {
		for _, op := range ops {
			m, ok := op.(map[string]any)
			if !ok {
				continue
			}
			attr, _ := m["AttributePath"].(string)
			if v, ok := m["AttributeValue"].(string); ok {
				switch attr {
				case "DisplayName":
					displayName = v
				case "Description":
					description = v
				}
			}
		}
	}
	if err := p.store.UpdateGroup(identityStoreID, groupID, displayName, description); err != nil {
		return shared.JSONError("ResourceNotFoundException", "group not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *IdentityStoreProvider) getGroupID(params map[string]any) (*plugin.Response, error) {
	identityStoreID, _ := params["IdentityStoreId"].(string)
	if identityStoreID == "" {
		identityStoreID = defaultIdentityStoreID
	}
	var displayName string
	if alt, ok := params["AlternateIdentifier"].(map[string]any); ok {
		if id, ok := alt["UniqueAttribute"].(map[string]any); ok {
			if v, ok := id["AttributeValue"].(string); ok {
				displayName = v
			}
		}
	}
	if displayName == "" {
		displayName, _ = params["DisplayName"].(string)
	}
	if displayName == "" {
		return shared.JSONError("ValidationException", "DisplayName is required", http.StatusBadRequest), nil
	}
	group, err := p.store.GetGroupByName(identityStoreID, displayName)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "group not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"GroupId":         group.GroupID,
		"IdentityStoreId": group.IdentityStoreID,
	})
}

func (p *IdentityStoreProvider) describeGroupMembership(params map[string]any) (*plugin.Response, error) {
	identityStoreID, _ := params["IdentityStoreId"].(string)
	if identityStoreID == "" {
		identityStoreID = defaultIdentityStoreID
	}
	membershipID, _ := params["MembershipId"].(string)
	if membershipID == "" {
		return shared.JSONError("ValidationException", "MembershipId is required", http.StatusBadRequest), nil
	}
	m, err := p.store.GetGroupMembership(identityStoreID, membershipID)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "membership not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"MembershipId":    m.MembershipID,
		"IdentityStoreId": m.IdentityStoreID,
		"GroupId":         m.GroupID,
		"MemberId": map[string]any{
			"UserId": m.UserID,
		},
	})
}

func (p *IdentityStoreProvider) getGroupMembershipID(params map[string]any) (*plugin.Response, error) {
	identityStoreID, _ := params["IdentityStoreId"].(string)
	if identityStoreID == "" {
		identityStoreID = defaultIdentityStoreID
	}
	groupID, _ := params["GroupId"].(string)
	userID := ""
	if m, ok := params["MemberId"].(map[string]any); ok {
		userID, _ = m["UserId"].(string)
	}
	if groupID == "" || userID == "" {
		return shared.JSONError("ValidationException", "GroupId and MemberId are required", http.StatusBadRequest), nil
	}
	m, err := p.store.GetGroupMembershipByGroupUser(identityStoreID, groupID, userID)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "membership not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"MembershipId":    m.MembershipID,
		"IdentityStoreId": m.IdentityStoreID,
	})
}

func (p *IdentityStoreProvider) listGroupMembershipsForMember(params map[string]any) (*plugin.Response, error) {
	identityStoreID, _ := params["IdentityStoreId"].(string)
	if identityStoreID == "" {
		identityStoreID = defaultIdentityStoreID
	}
	userID := ""
	if m, ok := params["MemberId"].(map[string]any); ok {
		userID, _ = m["UserId"].(string)
	}
	if userID == "" {
		return shared.JSONError("ValidationException", "MemberId is required", http.StatusBadRequest), nil
	}
	memberships, err := p.store.ListMembershipsForMember(identityStoreID, userID)
	if err != nil {
		return nil, err
	}
	result := make([]map[string]any, 0, len(memberships))
	for _, m := range memberships {
		result = append(result, map[string]any{
			"MembershipId":    m.MembershipID,
			"IdentityStoreId": m.IdentityStoreID,
			"GroupId":         m.GroupID,
			"MemberId": map[string]any{
				"UserId": m.UserID,
			},
		})
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"GroupMemberships": result,
		"NextToken":        nil,
	})
}

func (p *IdentityStoreProvider) isMemberInGroups(params map[string]any) (*plugin.Response, error) {
	identityStoreID, _ := params["IdentityStoreId"].(string)
	if identityStoreID == "" {
		identityStoreID = defaultIdentityStoreID
	}
	userID := ""
	if m, ok := params["MemberId"].(map[string]any); ok {
		userID, _ = m["UserId"].(string)
	}
	var groupIDs []string
	if raw, ok := params["GroupIds"].([]any); ok {
		for _, g := range raw {
			if s, ok := g.(string); ok {
				groupIDs = append(groupIDs, s)
			}
		}
	}
	results := make([]map[string]any, 0, len(groupIDs))
	for _, gid := range groupIDs {
		_, err := p.store.GetGroupMembershipByGroupUser(identityStoreID, gid, userID)
		results = append(results, map[string]any{
			"GroupId": gid,
			"MemberId": map[string]any{
				"UserId": userID,
			},
			"MembershipExists": err == nil,
		})
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"Results": results})
}

// Directory stubs
func (p *IdentityStoreProvider) createDirectory(params map[string]any) (*plugin.Response, error) {
	name, _ := params["Name"].(string)
	if name == "" {
		name = "default-directory"
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"DirectoryId": defaultIdentityStoreID,
		"Name":        name,
	})
}

func (p *IdentityStoreProvider) deleteDirectory(_ map[string]any) (*plugin.Response, error) {
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *IdentityStoreProvider) describeDirectory(params map[string]any) (*plugin.Response, error) {
	directoryID, _ := params["DirectoryId"].(string)
	if directoryID == "" {
		directoryID = defaultIdentityStoreID
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"Directory": map[string]any{
			"DirectoryId":  directoryID,
			"Name":         "dev-directory",
			"EndpointType": "DIRECTORY",
		},
	})
}

func (p *IdentityStoreProvider) listDirectories(_ map[string]any) (*plugin.Response, error) {
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"Directories": []map[string]any{
			{"DirectoryId": defaultIdentityStoreID, "Name": "dev-directory"},
		},
	})
}

func (p *IdentityStoreProvider) updateDirectory(_ map[string]any) (*plugin.Response, error) {
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *IdentityStoreProvider) batchCreateUsers(params map[string]any) (*plugin.Response, error) {
	identityStoreID, _ := params["IdentityStoreId"].(string)
	if identityStoreID == "" {
		identityStoreID = defaultIdentityStoreID
	}
	created := make([]map[string]any, 0)
	if raw, ok := params["Users"].([]any); ok {
		for _, u := range raw {
			m, ok := u.(map[string]any)
			if !ok {
				continue
			}
			userName, _ := m["UserName"].(string)
			if userName == "" {
				continue
			}
			displayName, _ := m["DisplayName"].(string)
			user, err := p.store.CreateUser(identityStoreID, userName, displayName, "")
			if err != nil {
				continue
			}
			created = append(created, map[string]any{"UserId": user.UserID, "UserName": user.UserName})
		}
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"Users": created})
}

func (p *IdentityStoreProvider) batchCreateGroups(params map[string]any) (*plugin.Response, error) {
	identityStoreID, _ := params["IdentityStoreId"].(string)
	if identityStoreID == "" {
		identityStoreID = defaultIdentityStoreID
	}
	created := make([]map[string]any, 0)
	if raw, ok := params["Groups"].([]any); ok {
		for _, g := range raw {
			m, ok := g.(map[string]any)
			if !ok {
				continue
			}
			displayName, _ := m["DisplayName"].(string)
			if displayName == "" {
				continue
			}
			description, _ := m["Description"].(string)
			group, err := p.store.CreateGroup(identityStoreID, displayName, description)
			if err != nil {
				continue
			}
			created = append(created, map[string]any{"GroupId": group.GroupID, "DisplayName": group.DisplayName})
		}
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"Groups": created})
}

func (p *IdentityStoreProvider) activateUser(params map[string]any) (*plugin.Response, error) {
	userID, _ := params["UserId"].(string)
	return shared.JSONResponse(http.StatusOK, map[string]any{"UserId": userID, "Status": "ACTIVE"})
}

func (p *IdentityStoreProvider) deactivateUser(params map[string]any) (*plugin.Response, error) {
	userID, _ := params["UserId"].(string)
	return shared.JSONResponse(http.StatusOK, map[string]any{"UserId": userID, "Status": "INACTIVE"})
}

// --- helpers ---

func userToMap(u *User) map[string]any {
	return map[string]any{
		"UserId":          u.UserID,
		"IdentityStoreId": u.IdentityStoreID,
		"UserName":        u.UserName,
		"DisplayName":     u.DisplayName,
		"Emails": []map[string]any{
			{"Value": u.Email, "Type": "work", "Primary": true},
		},
	}
}

func groupToMap(g *Group) map[string]any {
	return map[string]any{
		"GroupId":         g.GroupID,
		"IdentityStoreId": g.IdentityStoreID,
		"DisplayName":     g.DisplayName,
		"Description":     g.Description,
	}
}

func init() {
	plugin.DefaultRegistry.Register("identitystore", func(cfg plugin.PluginConfig) plugin.ServicePlugin {
		return &IdentityStoreProvider{}
	})
}
