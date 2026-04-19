// SPDX-License-Identifier: Apache-2.0

// internal/services/managedblockchain/provider_test.go
package managedblockchain

import (
	"context"
	"encoding/json"
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
	err := p.Init(plugin.PluginConfig{DataDir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
	return p
}

func callREST(t *testing.T, p *Provider, method, path, op, body string) *plugin.Response {
	t.Helper()
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err := p.HandleRequest(context.Background(), op, req)
	require.NoError(t, err)
	return resp
}

func parseBody(t *testing.T, resp *plugin.Response) map[string]any {
	t.Helper()
	var m map[string]any
	require.NoError(t, json.Unmarshal(resp.Body, &m))
	return m
}

func TestNetworkCRUD(t *testing.T) {
	p := newTestProvider(t)

	// CreateNetwork
	resp := callREST(t, p, "POST", "/networks", "CreateNetwork",
		`{"Name":"test-network","Framework":"HYPERLEDGER_FABRIC","FrameworkVersion":"2.2"}`)
	assert.Equal(t, 200, resp.StatusCode)
	rb := parseBody(t, resp)
	networkID, ok := rb["NetworkId"].(string)
	require.True(t, ok)
	assert.NotEmpty(t, networkID)

	// GetNetwork
	resp2 := callREST(t, p, "GET", "/networks/"+networkID, "GetNetwork", "")
	assert.Equal(t, 200, resp2.StatusCode)
	rb2 := parseBody(t, resp2)
	net, ok := rb2["Network"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "test-network", net["Name"])
	assert.Equal(t, "AVAILABLE", net["Status"])
	assert.Equal(t, "HYPERLEDGER_FABRIC", net["Framework"])

	// ListNetworks
	callREST(t, p, "POST", "/networks", "CreateNetwork", `{"Name":"second-network"}`)
	resp3 := callREST(t, p, "GET", "/networks", "ListNetworks", "")
	assert.Equal(t, 200, resp3.StatusCode)
	rb3 := parseBody(t, resp3)
	networks, ok := rb3["Networks"].([]any)
	require.True(t, ok)
	assert.Len(t, networks, 2)

	// GetNetwork not found
	resp4 := callREST(t, p, "GET", "/networks/nonexistent", "GetNetwork", "")
	assert.Equal(t, 404, resp4.StatusCode)

	// DeleteNetwork
	resp5 := callREST(t, p, "DELETE", "/networks/"+networkID, "DeleteNetwork", "")
	assert.Equal(t, 200, resp5.StatusCode)

	// GetNetwork after delete
	resp6 := callREST(t, p, "GET", "/networks/"+networkID, "GetNetwork", "")
	assert.Equal(t, 404, resp6.StatusCode)
}

func TestMemberCRUD(t *testing.T) {
	p := newTestProvider(t)

	// Create network first
	nr := callREST(t, p, "POST", "/networks", "CreateNetwork", `{"Name":"net-for-members"}`)
	networkID := parseBody(t, nr)["NetworkId"].(string)

	// CreateMember
	resp := callREST(t, p, "POST", "/networks/"+networkID+"/members", "CreateMember",
		`{"MemberConfiguration":{"Name":"member-one"}}`)
	assert.Equal(t, 200, resp.StatusCode)
	rb := parseBody(t, resp)
	memberID, ok := rb["MemberId"].(string)
	require.True(t, ok)
	assert.NotEmpty(t, memberID)

	// GetMember
	resp2 := callREST(t, p, "GET", "/networks/"+networkID+"/members/"+memberID, "GetMember", "")
	assert.Equal(t, 200, resp2.StatusCode)
	rb2 := parseBody(t, resp2)
	member, ok := rb2["Member"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "member-one", member["Name"])
	assert.Equal(t, "AVAILABLE", member["Status"])

	// ListMembers
	callREST(t, p, "POST", "/networks/"+networkID+"/members", "CreateMember", `{"MemberConfiguration":{"Name":"member-two"}}`)
	resp3 := callREST(t, p, "GET", "/networks/"+networkID+"/members", "ListMembers", "")
	assert.Equal(t, 200, resp3.StatusCode)
	rb3 := parseBody(t, resp3)
	members, ok := rb3["Members"].([]any)
	require.True(t, ok)
	assert.Len(t, members, 2)

	// UpdateMember
	resp4 := callREST(t, p, "PATCH", "/networks/"+networkID+"/members/"+memberID, "UpdateMember",
		`{"Description":"updated-desc"}`)
	assert.Equal(t, 200, resp4.StatusCode)

	// DeleteMember
	resp5 := callREST(t, p, "DELETE", "/networks/"+networkID+"/members/"+memberID, "DeleteMember", "")
	assert.Equal(t, 200, resp5.StatusCode)

	// GetMember after delete
	resp6 := callREST(t, p, "GET", "/networks/"+networkID+"/members/"+memberID, "GetMember", "")
	assert.Equal(t, 404, resp6.StatusCode)

	// CreateMember on non-existent network
	resp7 := callREST(t, p, "POST", "/networks/nonexistent/members", "CreateMember", `{"MemberConfiguration":{"Name":"x"}}`)
	assert.Equal(t, 404, resp7.StatusCode)
}

func TestNodeCRUD(t *testing.T) {
	p := newTestProvider(t)

	// Setup
	nr := callREST(t, p, "POST", "/networks", "CreateNetwork", `{"Name":"net-for-nodes"}`)
	networkID := parseBody(t, nr)["NetworkId"].(string)
	mr := callREST(t, p, "POST", "/networks/"+networkID+"/members", "CreateMember",
		`{"MemberConfiguration":{"Name":"member-for-nodes"}}`)
	memberID := parseBody(t, mr)["MemberId"].(string)

	// CreateNode
	resp := callREST(t, p, "POST", "/networks/"+networkID+"/members/"+memberID+"/nodes", "CreateNode",
		`{"NodeConfiguration":{"InstanceType":"bc.t3.medium","AvailabilityZone":"us-east-1b"}}`)
	assert.Equal(t, 200, resp.StatusCode)
	rb := parseBody(t, resp)
	nodeID, ok := rb["NodeId"].(string)
	require.True(t, ok)
	assert.NotEmpty(t, nodeID)

	// GetNode
	resp2 := callREST(t, p, "GET", "/networks/"+networkID+"/members/"+memberID+"/nodes/"+nodeID, "GetNode", "")
	assert.Equal(t, 200, resp2.StatusCode)
	rb2 := parseBody(t, resp2)
	node, ok := rb2["Node"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "bc.t3.medium", node["InstanceType"])
	assert.Equal(t, "us-east-1b", node["AvailabilityZone"])

	// ListNodes
	callREST(t, p, "POST", "/networks/"+networkID+"/members/"+memberID+"/nodes", "CreateNode", `{}`)
	resp3 := callREST(t, p, "GET", "/networks/"+networkID+"/members/"+memberID+"/nodes", "ListNodes", "")
	assert.Equal(t, 200, resp3.StatusCode)
	rb3 := parseBody(t, resp3)
	nodes, ok := rb3["Nodes"].([]any)
	require.True(t, ok)
	assert.Len(t, nodes, 2)

	// UpdateNode
	resp4 := callREST(t, p, "PATCH", "/networks/"+networkID+"/members/"+memberID+"/nodes/"+nodeID, "UpdateNode", `{}`)
	assert.Equal(t, 200, resp4.StatusCode)

	// DeleteNode
	resp5 := callREST(t, p, "DELETE", "/networks/"+networkID+"/members/"+memberID+"/nodes/"+nodeID, "DeleteNode", "")
	assert.Equal(t, 200, resp5.StatusCode)

	// GetNode after delete
	resp6 := callREST(t, p, "GET", "/networks/"+networkID+"/members/"+memberID+"/nodes/"+nodeID, "GetNode", "")
	assert.Equal(t, 404, resp6.StatusCode)
}

func TestProposalCRUD(t *testing.T) {
	p := newTestProvider(t)

	// Setup
	nr := callREST(t, p, "POST", "/networks", "CreateNetwork", `{"Name":"net-for-proposals"}`)
	networkID := parseBody(t, nr)["NetworkId"].(string)
	mr := callREST(t, p, "POST", "/networks/"+networkID+"/members", "CreateMember",
		`{"MemberConfiguration":{"Name":"proposing-member"}}`)
	memberID := parseBody(t, mr)["MemberId"].(string)

	// CreateProposal
	body, _ := json.Marshal(map[string]any{
		"MemberId":    memberID,
		"Description": "test proposal",
		"Actions":     map[string]any{"Invitations": []any{}},
	})
	resp := callREST(t, p, "POST", "/networks/"+networkID+"/proposals", "CreateProposal", string(body))
	assert.Equal(t, 200, resp.StatusCode)
	rb := parseBody(t, resp)
	proposalID, ok := rb["ProposalId"].(string)
	require.True(t, ok)
	assert.NotEmpty(t, proposalID)

	// GetProposal
	resp2 := callREST(t, p, "GET", "/networks/"+networkID+"/proposals/"+proposalID, "GetProposal", "")
	assert.Equal(t, 200, resp2.StatusCode)
	rb2 := parseBody(t, resp2)
	proposal, ok := rb2["Proposal"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "IN_PROGRESS", proposal["Status"])
	assert.Equal(t, "test proposal", proposal["Description"])

	// ListProposals
	callREST(t, p, "POST", "/networks/"+networkID+"/proposals", "CreateProposal",
		`{"MemberId":"`+memberID+`","Description":"second"}`)
	resp3 := callREST(t, p, "GET", "/networks/"+networkID+"/proposals", "ListProposals", "")
	assert.Equal(t, 200, resp3.StatusCode)
	rb3 := parseBody(t, resp3)
	proposals, ok := rb3["Proposals"].([]any)
	require.True(t, ok)
	assert.Len(t, proposals, 2)

	// VoteOnProposal YES
	resp4 := callREST(t, p, "POST", "/networks/"+networkID+"/proposals/"+proposalID+"/votes", "VoteOnProposal",
		`{"Vote":"YES","VoterMemberId":"`+memberID+`"}`)
	assert.Equal(t, 200, resp4.StatusCode)

	// GetProposal after vote
	resp5 := callREST(t, p, "GET", "/networks/"+networkID+"/proposals/"+proposalID, "GetProposal", "")
	rb5 := parseBody(t, resp5)
	p5 := rb5["Proposal"].(map[string]any)
	assert.Equal(t, "APPROVED", p5["Status"])

	// GetProposal not found
	resp6 := callREST(t, p, "GET", "/networks/"+networkID+"/proposals/nonexistent", "GetProposal", "")
	assert.Equal(t, 404, resp6.StatusCode)
}

func TestAccessorCRUD(t *testing.T) {
	p := newTestProvider(t)

	// CreateAccessor
	resp := callREST(t, p, "POST", "/accessors", "CreateAccessor",
		`{"AccessorType":"BILLING_TOKEN","NetworkType":"ETHEREUM_MAINNET"}`)
	assert.Equal(t, 200, resp.StatusCode)
	rb := parseBody(t, resp)
	accessorID, ok := rb["AccessorId"].(string)
	require.True(t, ok)
	assert.NotEmpty(t, accessorID)
	assert.NotEmpty(t, rb["BillingToken"])
	assert.NotEmpty(t, rb["Arn"])

	// GetAccessor
	resp2 := callREST(t, p, "GET", "/accessors/"+accessorID, "GetAccessor", "")
	assert.Equal(t, 200, resp2.StatusCode)
	rb2 := parseBody(t, resp2)
	accessor, ok := rb2["Accessor"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "BILLING_TOKEN", accessor["Type"])
	assert.Equal(t, "AVAILABLE", accessor["Status"])
	assert.Equal(t, "ETHEREUM_MAINNET", accessor["NetworkType"])

	// ListAccessors
	callREST(t, p, "POST", "/accessors", "CreateAccessor", `{}`)
	resp3 := callREST(t, p, "GET", "/accessors", "ListAccessors", "")
	assert.Equal(t, 200, resp3.StatusCode)
	rb3 := parseBody(t, resp3)
	accessors, ok := rb3["Accessors"].([]any)
	require.True(t, ok)
	assert.Len(t, accessors, 2)

	// GetAccessor not found
	resp4 := callREST(t, p, "GET", "/accessors/nonexistent", "GetAccessor", "")
	assert.Equal(t, 404, resp4.StatusCode)

	// DeleteAccessor
	resp5 := callREST(t, p, "DELETE", "/accessors/"+accessorID, "DeleteAccessor", "")
	assert.Equal(t, 200, resp5.StatusCode)

	// GetAccessor after delete
	resp6 := callREST(t, p, "GET", "/accessors/"+accessorID, "GetAccessor", "")
	assert.Equal(t, 404, resp6.StatusCode)
}

func TestTags(t *testing.T) {
	p := newTestProvider(t)

	// Create network with tags
	resp := callREST(t, p, "POST", "/networks", "CreateNetwork",
		`{"Name":"tagged-net","Tags":{"Env":"prod","Team":"blockchain"}}`)
	assert.Equal(t, 200, resp.StatusCode)
	rb := parseBody(t, resp)
	networkID := rb["NetworkId"].(string)

	// Get network ARN
	resp2 := callREST(t, p, "GET", "/networks/"+networkID, "GetNetwork", "")
	rb2 := parseBody(t, resp2)
	net := rb2["Network"].(map[string]any)
	arn := net["Arn"].(string)
	require.NotEmpty(t, arn)

	// TagResource
	tagBody, _ := json.Marshal(map[string]any{
		"Tags": map[string]string{"Extra": "value"},
	})
	resp3 := callREST(t, p, "POST", "/tags/"+arn, "TagResource", string(tagBody))
	assert.Equal(t, 200, resp3.StatusCode)

	// ListTagsForResource
	resp4 := callREST(t, p, "GET", "/tags/"+arn, "ListTagsForResource", "")
	assert.Equal(t, 200, resp4.StatusCode)
	rb4 := parseBody(t, resp4)
	tags, ok := rb4["Tags"].(map[string]any)
	require.True(t, ok)
	assert.Len(t, tags, 3)
	assert.Equal(t, "prod", tags["Env"])
	assert.Equal(t, "blockchain", tags["Team"])
	assert.Equal(t, "value", tags["Extra"])

	// UntagResource
	req := httptest.NewRequest("DELETE", "/tags/"+arn+"?tagKeys=Env&tagKeys=Extra", strings.NewReader(""))
	untagResp, err := p.HandleRequest(context.Background(), "UntagResource", req)
	require.NoError(t, err)
	assert.Equal(t, 200, untagResp.StatusCode)

	// Verify 1 tag remains
	resp5 := callREST(t, p, "GET", "/tags/"+arn, "ListTagsForResource", "")
	rb5 := parseBody(t, resp5)
	tags5 := rb5["Tags"].(map[string]any)
	assert.Len(t, tags5, 1)
	assert.Equal(t, "blockchain", tags5["Team"])
}
