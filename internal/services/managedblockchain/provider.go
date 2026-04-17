// SPDX-License-Identifier: Apache-2.0

// internal/services/managedblockchain/provider.go
package managedblockchain

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

// Provider implements the TaigaWebService (Managed Blockchain) service.
type Provider struct {
	store *Store
}

func (p *Provider) ServiceID() string             { return "managedblockchain" }
func (p *Provider) ServiceName() string           { return "TaigaWebService" }
func (p *Provider) Protocol() plugin.ProtocolType { return plugin.ProtocolRESTJSON }

func (p *Provider) Init(cfg plugin.PluginConfig) error {
	dataDir := cfg.DataDir
	if dataDir == "" {
		dataDir = "."
	}
	var err error
	p.store, err = NewStore(filepath.Join(dataDir, "managedblockchain"))
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

	if op == "" {
		op = resolveOp(req.Method, req.URL.Path)
	}

	switch op {
	// Networks
	case "CreateNetwork":
		return p.createNetwork(params)
	case "GetNetwork":
		networkID := extractPathSegment(req.URL.Path, "networks")
		return p.getNetwork(networkID)
	case "ListNetworks":
		return p.listNetworks()
	case "DeleteNetwork":
		networkID := extractPathSegment(req.URL.Path, "networks")
		return p.deleteNetwork(networkID)

	// Members
	case "CreateMember":
		networkID := extractPathSegment(req.URL.Path, "networks")
		return p.createMember(networkID, params)
	case "GetMember":
		networkID, memberID := extractTwoPathSegments(req.URL.Path, "networks", "members")
		return p.getMember(networkID, memberID)
	case "ListMembers":
		networkID := extractPathSegment(req.URL.Path, "networks")
		return p.listMembers(networkID)
	case "UpdateMember":
		networkID, memberID := extractTwoPathSegments(req.URL.Path, "networks", "members")
		return p.updateMember(networkID, memberID, params)
	case "DeleteMember":
		networkID, memberID := extractTwoPathSegments(req.URL.Path, "networks", "members")
		return p.deleteMember(networkID, memberID)

	// Nodes
	case "CreateNode":
		networkID, memberID := extractTwoPathSegments(req.URL.Path, "networks", "members")
		return p.createNode(networkID, memberID, params)
	case "GetNode":
		networkID, memberID, nodeID := extractThreePathSegments(req.URL.Path, "networks", "members", "nodes")
		return p.getNode(networkID, memberID, nodeID)
	case "ListNodes":
		networkID, memberID := extractTwoPathSegments(req.URL.Path, "networks", "members")
		return p.listNodes(networkID, memberID)
	case "UpdateNode":
		networkID, memberID, nodeID := extractThreePathSegments(req.URL.Path, "networks", "members", "nodes")
		return p.updateNode(networkID, memberID, nodeID, params)
	case "DeleteNode":
		networkID, memberID, nodeID := extractThreePathSegments(req.URL.Path, "networks", "members", "nodes")
		return p.deleteNode(networkID, memberID, nodeID)

	// Proposals
	case "CreateProposal":
		networkID := extractPathSegment(req.URL.Path, "networks")
		return p.createProposal(networkID, params)
	case "GetProposal":
		networkID, proposalID := extractTwoPathSegments(req.URL.Path, "networks", "proposals")
		return p.getProposal(networkID, proposalID)
	case "ListProposals":
		networkID := extractPathSegment(req.URL.Path, "networks")
		return p.listProposals(networkID)
	case "VoteOnProposal":
		networkID, proposalID := extractTwoPathSegments(req.URL.Path, "networks", "proposals")
		return p.voteOnProposal(networkID, proposalID, params)

	// Accessors
	case "CreateAccessor":
		return p.createAccessor(params)
	case "GetAccessor":
		accessorID := extractPathSegment(req.URL.Path, "accessors")
		return p.getAccessor(accessorID)
	case "ListAccessors":
		return p.listAccessors()
	case "DeleteAccessor":
		accessorID := extractPathSegment(req.URL.Path, "accessors")
		return p.deleteAccessor(accessorID)

	// Invitations (emulator stubs)
	case "ListInvitations":
		return shared.JSONResponse(http.StatusOK, map[string]any{"Invitations": []any{}})
	case "RejectInvitation":
		return shared.JSONResponse(http.StatusOK, map[string]any{})

	// Tags
	case "TagResource":
		return p.tagResource(req, params)
	case "UntagResource":
		return p.untagResource(req)
	case "ListTagsForResource":
		return p.listTagsForResource(req)

	default:
		return shared.JSONError("InvalidAction", fmt.Sprintf("unknown action: %s", op), http.StatusBadRequest), nil
	}
}

func resolveOp(method, path string) string {
	p := strings.Trim(path, "/")
	seg := strings.Split(p, "/")
	n := len(seg)

	switch {
	// Tags: /tags/{arn}
	case n >= 1 && seg[0] == "tags":
		switch method {
		case http.MethodPost:
			return "TagResource"
		case http.MethodGet:
			return "ListTagsForResource"
		case http.MethodDelete:
			return "UntagResource"
		}

	// Invitations
	case n >= 1 && seg[0] == "invitations":
		if n == 1 && method == http.MethodGet {
			return "ListInvitations"
		}
		if n == 2 && method == http.MethodDelete {
			return "RejectInvitation"
		}

	// Accessors
	case n >= 1 && seg[0] == "accessors":
		if n == 1 {
			switch method {
			case http.MethodPost:
				return "CreateAccessor"
			case http.MethodGet:
				return "ListAccessors"
			}
		}
		if n == 2 {
			switch method {
			case http.MethodGet:
				return "GetAccessor"
			case http.MethodDelete:
				return "DeleteAccessor"
			}
		}

	// Networks
	case n >= 1 && seg[0] == "networks":
		if n == 1 {
			switch method {
			case http.MethodPost:
				return "CreateNetwork"
			case http.MethodGet:
				return "ListNetworks"
			}
		}
		if n == 2 {
			switch method {
			case http.MethodGet:
				return "GetNetwork"
			case http.MethodDelete:
				return "DeleteNetwork"
			}
		}
		// /networks/{id}/members
		if n >= 3 && seg[2] == "members" {
			if n == 3 {
				switch method {
				case http.MethodPost:
					return "CreateMember"
				case http.MethodGet:
					return "ListMembers"
				}
			}
			if n == 4 {
				switch method {
				case http.MethodGet:
					return "GetMember"
				case http.MethodPatch:
					return "UpdateMember"
				case http.MethodDelete:
					return "DeleteMember"
				}
			}
			// /networks/{id}/members/{mid}/nodes
			if n >= 5 && seg[4] == "nodes" {
				if n == 5 {
					switch method {
					case http.MethodPost:
						return "CreateNode"
					case http.MethodGet:
						return "ListNodes"
					}
				}
				if n == 6 {
					switch method {
					case http.MethodGet:
						return "GetNode"
					case http.MethodPatch:
						return "UpdateNode"
					case http.MethodDelete:
						return "DeleteNode"
					}
				}
			}
		}
		// /networks/{id}/proposals
		if n >= 3 && seg[2] == "proposals" {
			if n == 3 {
				switch method {
				case http.MethodPost:
					return "CreateProposal"
				case http.MethodGet:
					return "ListProposals"
				}
			}
			if n == 4 {
				switch method {
				case http.MethodGet:
					return "GetProposal"
				}
			}
			// /networks/{id}/proposals/{pid}/votes
			if n >= 5 && seg[4] == "votes" && method == http.MethodPost {
				return "VoteOnProposal"
			}
		}
	}
	return ""
}

func (p *Provider) ListResources(_ context.Context) ([]plugin.Resource, error) {
	networks, err := p.store.ListNetworks()
	if err != nil {
		return nil, err
	}
	res := make([]plugin.Resource, 0, len(networks))
	for _, n := range networks {
		res = append(res, plugin.Resource{Type: "managedblockchain-network", ID: n.ID, Name: n.Name})
	}
	return res, nil
}

func (p *Provider) GetMetrics(_ context.Context) (*plugin.ServiceMetrics, error) {
	return &plugin.ServiceMetrics{}, nil
}

// --- Networks ---

func (p *Provider) createNetwork(params map[string]any) (*plugin.Response, error) {
	name, _ := params["Name"].(string)
	if name == "" {
		return shared.JSONError("InvalidRequestException", "network name is required", http.StatusBadRequest), nil
	}
	framework := "HYPERLEDGER_FABRIC"
	if v, ok := params["Framework"].(string); ok && v != "" {
		framework = v
	}
	frameworkVer := "2.2"
	if v, ok := params["FrameworkVersion"].(string); ok && v != "" {
		frameworkVer = v
	}
	description, _ := params["Description"].(string)

	id := shared.GenerateID("n-", 26)
	arn := shared.BuildARN("managedblockchain", "network", id)

	n := &Network{
		ID:           id,
		ARN:          arn,
		Name:         name,
		Framework:    framework,
		FrameworkVer: frameworkVer,
		Status:       "AVAILABLE",
		Description:  description,
	}
	if err := p.store.CreateNetwork(n); err != nil {
		if isUniqueErr(err) {
			return shared.JSONError("ResourceAlreadyExistsException", "network already exists", http.StatusConflict), nil
		}
		return nil, err
	}

	if rawTags, ok := params["Tags"].(map[string]any); ok {
		tags := toStringMap(rawTags)
		p.store.tags.AddTags(arn, tags)
	}

	return shared.JSONResponse(http.StatusOK, map[string]any{"NetworkId": id})
}

func (p *Provider) getNetwork(id string) (*plugin.Response, error) {
	if id == "" {
		return shared.JSONError("InvalidRequestException", "network ID is required", http.StatusBadRequest), nil
	}
	n, err := p.store.GetNetwork(id)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "network not found", http.StatusNotFound), nil
	}
	tags, _ := p.store.tags.ListTags(n.ARN)
	return shared.JSONResponse(http.StatusOK, map[string]any{"Network": networkToMap(n, tags)})
}

func (p *Provider) listNetworks() (*plugin.Response, error) {
	networks, err := p.store.ListNetworks()
	if err != nil {
		return nil, err
	}
	items := make([]map[string]any, 0, len(networks))
	for i := range networks {
		items = append(items, networkToMap(&networks[i], nil))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"Networks": items})
}

func (p *Provider) deleteNetwork(id string) (*plugin.Response, error) {
	if id == "" {
		return shared.JSONError("InvalidRequestException", "network ID is required", http.StatusBadRequest), nil
	}
	n, err := p.store.GetNetwork(id)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "network not found", http.StatusNotFound), nil
	}
	p.store.tags.DeleteAllTags(n.ARN)
	if err := p.store.DeleteNetwork(id); err != nil {
		return shared.JSONError("ResourceNotFoundException", "network not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

// --- Members ---

func (p *Provider) createMember(networkID string, params map[string]any) (*plugin.Response, error) {
	if networkID == "" {
		return shared.JSONError("InvalidRequestException", "network ID is required", http.StatusBadRequest), nil
	}
	if _, err := p.store.GetNetwork(networkID); err != nil {
		return shared.JSONError("ResourceNotFoundException", "network not found", http.StatusNotFound), nil
	}

	name, _ := params["Name"].(string)
	if name == "" {
		// check nested MemberConfiguration
		if mc, ok := params["MemberConfiguration"].(map[string]any); ok {
			name, _ = mc["Name"].(string)
		}
	}
	if name == "" {
		return shared.JSONError("InvalidRequestException", "member name is required", http.StatusBadRequest), nil
	}
	description, _ := params["Description"].(string)

	id := shared.GenerateID("m-", 26)
	arn := shared.BuildARN("managedblockchain", fmt.Sprintf("network/%s/member", networkID), id)

	m := &Member{
		ID:          id,
		ARN:         arn,
		NetworkID:   networkID,
		Name:        name,
		Status:      "AVAILABLE",
		Description: description,
	}
	if err := p.store.CreateMember(m); err != nil {
		if isUniqueErr(err) {
			return shared.JSONError("ResourceAlreadyExistsException", "member already exists", http.StatusConflict), nil
		}
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"MemberId": id})
}

func (p *Provider) getMember(networkID, memberID string) (*plugin.Response, error) {
	if networkID == "" || memberID == "" {
		return shared.JSONError("InvalidRequestException", "network ID and member ID are required", http.StatusBadRequest), nil
	}
	m, err := p.store.GetMember(networkID, memberID)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "member not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"Member": memberToMap(m)})
}

func (p *Provider) listMembers(networkID string) (*plugin.Response, error) {
	if networkID == "" {
		return shared.JSONError("InvalidRequestException", "network ID is required", http.StatusBadRequest), nil
	}
	members, err := p.store.ListMembers(networkID)
	if err != nil {
		return nil, err
	}
	items := make([]map[string]any, 0, len(members))
	for i := range members {
		items = append(items, memberToMap(&members[i]))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"Members": items})
}

func (p *Provider) updateMember(networkID, memberID string, params map[string]any) (*plugin.Response, error) {
	if networkID == "" || memberID == "" {
		return shared.JSONError("InvalidRequestException", "network ID and member ID are required", http.StatusBadRequest), nil
	}
	if err := p.store.UpdateMember(networkID, memberID, params); err != nil {
		return shared.JSONError("ResourceNotFoundException", "member not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) deleteMember(networkID, memberID string) (*plugin.Response, error) {
	if networkID == "" || memberID == "" {
		return shared.JSONError("InvalidRequestException", "network ID and member ID are required", http.StatusBadRequest), nil
	}
	if err := p.store.DeleteMember(networkID, memberID); err != nil {
		return shared.JSONError("ResourceNotFoundException", "member not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

// --- Nodes ---

func (p *Provider) createNode(networkID, memberID string, params map[string]any) (*plugin.Response, error) {
	if networkID == "" || memberID == "" {
		return shared.JSONError("InvalidRequestException", "network ID and member ID are required", http.StatusBadRequest), nil
	}
	if _, err := p.store.GetMember(networkID, memberID); err != nil {
		return shared.JSONError("ResourceNotFoundException", "member not found", http.StatusNotFound), nil
	}

	instanceType := "bc.t3.small"
	az := "us-east-1a"
	if nc, ok := params["NodeConfiguration"].(map[string]any); ok {
		if v, ok := nc["InstanceType"].(string); ok && v != "" {
			instanceType = v
		}
		if v, ok := nc["AvailabilityZone"].(string); ok && v != "" {
			az = v
		}
	}
	if v, ok := params["InstanceType"].(string); ok && v != "" {
		instanceType = v
	}

	id := shared.GenerateID("nd-", 26)
	arn := shared.BuildARN("managedblockchain", fmt.Sprintf("network/%s/member/%s/node", networkID, memberID), id)

	n := &Node{
		ID:               id,
		ARN:              arn,
		NetworkID:        networkID,
		MemberID:         memberID,
		InstanceType:     instanceType,
		Status:           "AVAILABLE",
		AvailabilityZone: az,
	}
	if err := p.store.CreateNode(n); err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"NodeId": id})
}

func (p *Provider) getNode(networkID, memberID, nodeID string) (*plugin.Response, error) {
	if networkID == "" || memberID == "" || nodeID == "" {
		return shared.JSONError("InvalidRequestException", "network ID, member ID, and node ID are required", http.StatusBadRequest), nil
	}
	n, err := p.store.GetNode(networkID, memberID, nodeID)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "node not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"Node": nodeToMap(n)})
}

func (p *Provider) listNodes(networkID, memberID string) (*plugin.Response, error) {
	if networkID == "" || memberID == "" {
		return shared.JSONError("InvalidRequestException", "network ID and member ID are required", http.StatusBadRequest), nil
	}
	nodes, err := p.store.ListNodes(networkID, memberID)
	if err != nil {
		return nil, err
	}
	items := make([]map[string]any, 0, len(nodes))
	for i := range nodes {
		items = append(items, nodeToMap(&nodes[i]))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"Nodes": items})
}

func (p *Provider) updateNode(networkID, memberID, nodeID string, params map[string]any) (*plugin.Response, error) {
	if networkID == "" || memberID == "" || nodeID == "" {
		return shared.JSONError("InvalidRequestException", "IDs are required", http.StatusBadRequest), nil
	}
	if err := p.store.UpdateNode(networkID, memberID, nodeID, params); err != nil {
		return shared.JSONError("ResourceNotFoundException", "node not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) deleteNode(networkID, memberID, nodeID string) (*plugin.Response, error) {
	if networkID == "" || memberID == "" || nodeID == "" {
		return shared.JSONError("InvalidRequestException", "IDs are required", http.StatusBadRequest), nil
	}
	if err := p.store.DeleteNode(networkID, memberID, nodeID); err != nil {
		return shared.JSONError("ResourceNotFoundException", "node not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

// --- Proposals ---

func (p *Provider) createProposal(networkID string, params map[string]any) (*plugin.Response, error) {
	if networkID == "" {
		return shared.JSONError("InvalidRequestException", "network ID is required", http.StatusBadRequest), nil
	}
	if _, err := p.store.GetNetwork(networkID); err != nil {
		return shared.JSONError("ResourceNotFoundException", "network not found", http.StatusNotFound), nil
	}

	memberID, _ := params["MemberId"].(string)
	description, _ := params["Description"].(string)
	actionsRaw, _ := json.Marshal(params["Actions"])

	id := shared.GenerateID("p-", 26)
	arn := shared.BuildARN("managedblockchain", fmt.Sprintf("network/%s/proposal", networkID), id)

	pr := &Proposal{
		ID:          id,
		ARN:         arn,
		NetworkID:   networkID,
		MemberID:    memberID,
		Status:      "IN_PROGRESS",
		Description: description,
		Actions:     string(actionsRaw),
	}
	if err := p.store.CreateProposal(pr); err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"ProposalId": id})
}

func (p *Provider) getProposal(networkID, proposalID string) (*plugin.Response, error) {
	if networkID == "" || proposalID == "" {
		return shared.JSONError("InvalidRequestException", "network ID and proposal ID are required", http.StatusBadRequest), nil
	}
	pr, err := p.store.GetProposal(networkID, proposalID)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "proposal not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"Proposal": proposalToMap(pr)})
}

func (p *Provider) listProposals(networkID string) (*plugin.Response, error) {
	if networkID == "" {
		return shared.JSONError("InvalidRequestException", "network ID is required", http.StatusBadRequest), nil
	}
	proposals, err := p.store.ListProposals(networkID)
	if err != nil {
		return nil, err
	}
	items := make([]map[string]any, 0, len(proposals))
	for i := range proposals {
		items = append(items, proposalToMap(&proposals[i]))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"Proposals": items})
}

func (p *Provider) voteOnProposal(networkID, proposalID string, params map[string]any) (*plugin.Response, error) {
	if networkID == "" || proposalID == "" {
		return shared.JSONError("InvalidRequestException", "network ID and proposal ID are required", http.StatusBadRequest), nil
	}
	vote, _ := params["Vote"].(string)
	newStatus := "APPROVED"
	if vote == "NO" {
		newStatus = "REJECTED"
	}
	if err := p.store.UpdateProposalStatus(networkID, proposalID, newStatus); err != nil {
		return shared.JSONError("ResourceNotFoundException", "proposal not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

// --- Accessors ---

func (p *Provider) createAccessor(params map[string]any) (*plugin.Response, error) {
	accessorType := "BILLING_TOKEN"
	if v, ok := params["AccessorType"].(string); ok && v != "" {
		accessorType = v
	}
	networkType := "ETHEREUM_MAINNET"
	if v, ok := params["NetworkType"].(string); ok && v != "" {
		networkType = v
	}

	id := shared.GenerateID("ac-", 26)
	arn := shared.BuildARN("managedblockchain", "accessor", id)
	billingToken := shared.GenerateID("", 64)

	a := &Accessor{
		ID:           id,
		ARN:          arn,
		Type:         accessorType,
		Status:       "AVAILABLE",
		BillingToken: billingToken,
		NetworkType:  networkType,
	}
	if err := p.store.CreateAccessor(a); err != nil {
		return nil, err
	}

	if rawTags, ok := params["Tags"].(map[string]any); ok {
		tags := toStringMap(rawTags)
		p.store.tags.AddTags(arn, tags)
	}

	return shared.JSONResponse(http.StatusOK, map[string]any{
		"AccessorId":   id,
		"Arn":          arn,
		"BillingToken": billingToken,
	})
}

func (p *Provider) getAccessor(id string) (*plugin.Response, error) {
	if id == "" {
		return shared.JSONError("InvalidRequestException", "accessor ID is required", http.StatusBadRequest), nil
	}
	a, err := p.store.GetAccessor(id)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "accessor not found", http.StatusNotFound), nil
	}
	tags, _ := p.store.tags.ListTags(a.ARN)
	return shared.JSONResponse(http.StatusOK, map[string]any{"Accessor": accessorToMap(a, tags)})
}

func (p *Provider) listAccessors() (*plugin.Response, error) {
	accessors, err := p.store.ListAccessors()
	if err != nil {
		return nil, err
	}
	items := make([]map[string]any, 0, len(accessors))
	for i := range accessors {
		items = append(items, accessorToMap(&accessors[i], nil))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"Accessors": items})
}

func (p *Provider) deleteAccessor(id string) (*plugin.Response, error) {
	if id == "" {
		return shared.JSONError("InvalidRequestException", "accessor ID is required", http.StatusBadRequest), nil
	}
	a, err := p.store.GetAccessor(id)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "accessor not found", http.StatusNotFound), nil
	}
	p.store.tags.DeleteAllTags(a.ARN)
	if err := p.store.DeleteAccessor(id); err != nil {
		return shared.JSONError("ResourceNotFoundException", "accessor not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

// --- Tags ---

func (p *Provider) tagResource(req *http.Request, params map[string]any) (*plugin.Response, error) {
	arn := extractARNFromPath(req.URL.Path, "tags")
	if arn == "" {
		return shared.JSONError("InvalidRequestException", "resource ARN is required", http.StatusBadRequest), nil
	}
	rawTags, _ := params["Tags"].(map[string]any)
	tags := toStringMap(rawTags)
	if err := p.store.tags.AddTags(arn, tags); err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) untagResource(req *http.Request) (*plugin.Response, error) {
	arn := extractARNFromPath(req.URL.Path, "tags")
	if arn == "" {
		return shared.JSONError("InvalidRequestException", "resource ARN is required", http.StatusBadRequest), nil
	}
	keys := req.URL.Query()["tagKeys"]
	if err := p.store.tags.RemoveTags(arn, keys); err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) listTagsForResource(req *http.Request) (*plugin.Response, error) {
	arn := extractARNFromPath(req.URL.Path, "tags")
	if arn == "" {
		return shared.JSONError("InvalidRequestException", "resource ARN is required", http.StatusBadRequest), nil
	}
	tags, err := p.store.tags.ListTags(arn)
	if err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"Tags": tags})
}

// --- Serializers ---

func networkToMap(n *Network, tags map[string]string) map[string]any {
	if tags == nil {
		tags = map[string]string{}
	}
	return map[string]any{
		"Id":               n.ID,
		"Arn":              n.ARN,
		"Name":             n.Name,
		"Framework":        n.Framework,
		"FrameworkVersion": n.FrameworkVer,
		"Status":           n.Status,
		"Description":      n.Description,
		"CreatedAt":        n.CreatedAt.Unix(),
		"Tags":             tags,
	}
}

func memberToMap(m *Member) map[string]any {
	return map[string]any{
		"Id":          m.ID,
		"Arn":         m.ARN,
		"NetworkId":   m.NetworkID,
		"Name":        m.Name,
		"Status":      m.Status,
		"Description": m.Description,
		"CreatedAt":   m.CreatedAt.Unix(),
	}
}

func nodeToMap(n *Node) map[string]any {
	return map[string]any{
		"Id":               n.ID,
		"Arn":              n.ARN,
		"NetworkId":        n.NetworkID,
		"MemberId":         n.MemberID,
		"InstanceType":     n.InstanceType,
		"Status":           n.Status,
		"AvailabilityZone": n.AvailabilityZone,
		"CreatedAt":        n.CreatedAt.Unix(),
	}
}

func proposalToMap(pr *Proposal) map[string]any {
	return map[string]any{
		"ProposalId":     pr.ID,
		"Arn":            pr.ARN,
		"NetworkId":      pr.NetworkID,
		"MemberId":       pr.MemberID,
		"Status":         pr.Status,
		"Description":    pr.Description,
		"CreatedAt":      pr.CreatedAt.Unix(),
		"ExpirationDate": pr.ExpiresAt.Unix(),
	}
}

func accessorToMap(a *Accessor, tags map[string]string) map[string]any {
	if tags == nil {
		tags = map[string]string{}
	}
	return map[string]any{
		"Id":           a.ID,
		"Arn":          a.ARN,
		"Type":         a.Type,
		"Status":       a.Status,
		"BillingToken": a.BillingToken,
		"NetworkType":  a.NetworkType,
		"CreatedAt":    a.CreatedAt.Unix(),
		"Tags":         tags,
	}
}

// --- Path helpers ---

// extractPathSegment returns the segment immediately after `key` in the URL path.
func extractPathSegment(path, key string) string {
	parts := strings.Split(path, "/")
	for i, p := range parts {
		if p == key && i+1 < len(parts) {
			return parts[i+1]
		}
	}
	return ""
}

// extractTwoPathSegments returns segments after key1 and key2.
func extractTwoPathSegments(path, key1, key2 string) (string, string) {
	parts := strings.Split(path, "/")
	var v1, v2 string
	for i, p := range parts {
		if p == key1 && i+1 < len(parts) {
			v1 = parts[i+1]
		}
		if p == key2 && i+1 < len(parts) {
			v2 = parts[i+1]
		}
	}
	return v1, v2
}

// extractThreePathSegments returns segments after key1, key2, and key3.
func extractThreePathSegments(path, key1, key2, key3 string) (string, string, string) {
	parts := strings.Split(path, "/")
	var v1, v2, v3 string
	for i, p := range parts {
		if p == key1 && i+1 < len(parts) {
			v1 = parts[i+1]
		}
		if p == key2 && i+1 < len(parts) {
			v2 = parts[i+1]
		}
		if p == key3 && i+1 < len(parts) {
			v3 = parts[i+1]
		}
	}
	return v1, v2, v3
}

// extractARNFromPath extracts everything after /tags/ as the ARN.
func extractARNFromPath(path, key string) string {
	idx := strings.Index(path, "/"+key+"/")
	if idx < 0 {
		return ""
	}
	return path[idx+len("/"+key+"/"):]
}

func toStringMap(m map[string]any) map[string]string {
	result := make(map[string]string, len(m))
	for k, v := range m {
		if s, ok := v.(string); ok {
			result[k] = s
		}
	}
	return result
}

func isUniqueErr(err error) bool {
	return err != nil && strings.Contains(err.Error(), "UNIQUE constraint failed")
}
