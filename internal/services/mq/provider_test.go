// SPDX-License-Identifier: Apache-2.0

// internal/services/mq/provider_test.go
package mq

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
	t.Cleanup(func() { p.Shutdown(context.Background()) })
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

func TestBrokerCRUD(t *testing.T) {
	p := newTestProvider(t)

	// Create broker
	createBody := `{"brokerName":"test-broker","engineType":"ACTIVEMQ","engineVersion":"5.17.6","hostInstanceType":"mq.m5.large","deploymentMode":"SINGLE_INSTANCE","publiclyAccessible":false}`
	resp := callREST(t, p, "POST", "/v1/brokers", "CreateBroker", createBody)
	assert.Equal(t, 200, resp.StatusCode)
	rb := parseBody(t, resp)
	brokerID, ok := rb["brokerId"].(string)
	require.True(t, ok)
	assert.NotEmpty(t, brokerID)
	assert.NotEmpty(t, rb["brokerArn"])

	// Describe broker
	resp2 := callREST(t, p, "GET", "/v1/brokers/"+brokerID, "DescribeBroker", "")
	assert.Equal(t, 200, resp2.StatusCode)
	rb2 := parseBody(t, resp2)
	assert.Equal(t, "test-broker", rb2["brokerName"])
	assert.Equal(t, "ACTIVEMQ", rb2["engineType"])
	assert.Equal(t, "RUNNING", rb2["brokerState"])

	// List brokers
	resp3 := callREST(t, p, "GET", "/v1/brokers", "ListBrokers", "")
	assert.Equal(t, 200, resp3.StatusCode)
	rb3 := parseBody(t, resp3)
	summaries, ok := rb3["brokerSummaries"].([]any)
	require.True(t, ok)
	assert.Len(t, summaries, 1)

	// Update broker
	resp4 := callREST(t, p, "PUT", "/v1/brokers/"+brokerID, "UpdateBroker", `{"engineVersion":"5.18.3"}`)
	assert.Equal(t, 200, resp4.StatusCode)

	// Verify update
	resp5 := callREST(t, p, "GET", "/v1/brokers/"+brokerID, "DescribeBroker", "")
	rb5 := parseBody(t, resp5)
	assert.Equal(t, "5.18.3", rb5["engineVersion"])

	// Delete broker
	resp6 := callREST(t, p, "DELETE", "/v1/brokers/"+brokerID, "DeleteBroker", "")
	assert.Equal(t, 200, resp6.StatusCode)

	// Describe after delete should 404
	resp7 := callREST(t, p, "GET", "/v1/brokers/"+brokerID, "DescribeBroker", "")
	assert.Equal(t, 404, resp7.StatusCode)

	// Delete non-existent
	resp8 := callREST(t, p, "DELETE", "/v1/brokers/nonexistent", "DeleteBroker", "")
	assert.Equal(t, 404, resp8.StatusCode)
}

func TestConfigurationCRUD(t *testing.T) {
	p := newTestProvider(t)

	// Create configuration
	createBody := `{"name":"my-config","engineType":"ACTIVEMQ","engineVersion":"5.17.6"}`
	resp := callREST(t, p, "POST", "/v1/configurations", "CreateConfiguration", createBody)
	assert.Equal(t, 200, resp.StatusCode)
	rb := parseBody(t, resp)
	cfgID, ok := rb["id"].(string)
	require.True(t, ok)
	assert.NotEmpty(t, cfgID)

	// Describe configuration
	resp2 := callREST(t, p, "GET", "/v1/configurations/"+cfgID, "DescribeConfiguration", "")
	assert.Equal(t, 200, resp2.StatusCode)
	rb2 := parseBody(t, resp2)
	assert.Equal(t, "my-config", rb2["name"])
	assert.Equal(t, "ACTIVEMQ", rb2["engineType"])

	// List configurations
	resp3 := callREST(t, p, "GET", "/v1/configurations", "ListConfigurations", "")
	assert.Equal(t, 200, resp3.StatusCode)
	rb3 := parseBody(t, resp3)
	cfgs, ok := rb3["configurations"].([]any)
	require.True(t, ok)
	assert.Len(t, cfgs, 1)

	// Update configuration
	resp4 := callREST(t, p, "PUT", "/v1/configurations/"+cfgID, "UpdateConfiguration", `{"data":"<config>test</config>"}`)
	assert.Equal(t, 200, resp4.StatusCode)
	rb4 := parseBody(t, resp4)
	latestRev, ok := rb4["latestRevision"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, float64(2), latestRev["revision"])

	// Describe revision
	resp5 := callREST(t, p, "GET", "/v1/configurations/"+cfgID+"/revisions/2", "DescribeConfigurationRevision", "")
	assert.Equal(t, 200, resp5.StatusCode)
	rb5 := parseBody(t, resp5)
	assert.Equal(t, "<config>test</config>", rb5["data"])

	// List revisions
	resp6 := callREST(t, p, "GET", "/v1/configurations/"+cfgID+"/revisions", "ListConfigurationRevisions", "")
	assert.Equal(t, 200, resp6.StatusCode)
	rb6 := parseBody(t, resp6)
	revisions, ok := rb6["revisions"].([]any)
	require.True(t, ok)
	assert.Len(t, revisions, 2)

	// Delete configuration
	resp7 := callREST(t, p, "DELETE", "/v1/configurations/"+cfgID, "DeleteConfiguration", "")
	assert.Equal(t, 200, resp7.StatusCode)

	// Describe after delete should 404
	resp8 := callREST(t, p, "GET", "/v1/configurations/"+cfgID, "DescribeConfiguration", "")
	assert.Equal(t, 404, resp8.StatusCode)
}

func TestUserCRUD(t *testing.T) {
	p := newTestProvider(t)

	// Create a broker first
	createBrokerResp := callREST(t, p, "POST", "/v1/brokers", "CreateBroker", `{"brokerName":"user-broker","engineType":"ACTIVEMQ","engineVersion":"5.17.6","hostInstanceType":"mq.m5.large","deploymentMode":"SINGLE_INSTANCE"}`)
	require.Equal(t, 200, createBrokerResp.StatusCode)
	brokerRB := parseBody(t, createBrokerResp)
	brokerID := brokerRB["brokerId"].(string)

	// Create user
	createUserBody := `{"password":"secret123","consoleAccess":true,"groups":["admin"]}`
	resp := callREST(t, p, "POST", "/v1/brokers/"+brokerID+"/users/alice", "CreateUser", createUserBody)
	assert.Equal(t, 200, resp.StatusCode)

	// Describe user
	resp2 := callREST(t, p, "GET", "/v1/brokers/"+brokerID+"/users/alice", "DescribeUser", "")
	assert.Equal(t, 200, resp2.StatusCode)
	rb2 := parseBody(t, resp2)
	assert.Equal(t, "alice", rb2["username"])
	assert.Equal(t, true, rb2["consoleAccess"])
	groups, ok := rb2["groups"].([]any)
	require.True(t, ok)
	assert.Len(t, groups, 1)
	assert.Equal(t, "admin", groups[0])

	// List users
	resp3 := callREST(t, p, "GET", "/v1/brokers/"+brokerID+"/users", "ListUsers", "")
	assert.Equal(t, 200, resp3.StatusCode)
	rb3 := parseBody(t, resp3)
	users, ok := rb3["users"].([]any)
	require.True(t, ok)
	assert.Len(t, users, 1)

	// Update user
	resp4 := callREST(t, p, "PUT", "/v1/brokers/"+brokerID+"/users/alice", "UpdateUser", `{"consoleAccess":false,"groups":["readonly"]}`)
	assert.Equal(t, 200, resp4.StatusCode)

	// Verify update
	resp5 := callREST(t, p, "GET", "/v1/brokers/"+brokerID+"/users/alice", "DescribeUser", "")
	rb5 := parseBody(t, resp5)
	assert.Equal(t, false, rb5["consoleAccess"])

	// Delete user
	resp6 := callREST(t, p, "DELETE", "/v1/brokers/"+brokerID+"/users/alice", "DeleteUser", "")
	assert.Equal(t, 200, resp6.StatusCode)

	// Describe after delete should 404
	resp7 := callREST(t, p, "GET", "/v1/brokers/"+brokerID+"/users/alice", "DescribeUser", "")
	assert.Equal(t, 404, resp7.StatusCode)
}

func TestTags(t *testing.T) {
	p := newTestProvider(t)

	// Create broker to get an ARN
	createResp := callREST(t, p, "POST", "/v1/brokers", "CreateBroker", `{"brokerName":"tagged-broker","engineType":"ACTIVEMQ","engineVersion":"5.17.6","hostInstanceType":"mq.m5.large","deploymentMode":"SINGLE_INSTANCE"}`)
	require.Equal(t, 200, createResp.StatusCode)
	rb := parseBody(t, createResp)
	arn := rb["brokerArn"].(string)
	require.NotEmpty(t, arn)

	// CreateTags
	tagBody, _ := json.Marshal(map[string]any{
		"tags": map[string]string{"Env": "prod", "Team": "platform"},
	})
	resp := callREST(t, p, "POST", "/v1/tags/"+arn, "CreateTags", string(tagBody))
	assert.Equal(t, 200, resp.StatusCode)

	// ListTags
	resp2 := callREST(t, p, "GET", "/v1/tags/"+arn, "ListTags", "")
	assert.Equal(t, 200, resp2.StatusCode)
	rb2 := parseBody(t, resp2)
	tags, ok := rb2["tags"].(map[string]any)
	require.True(t, ok)
	assert.Len(t, tags, 2)
	assert.Equal(t, "prod", tags["Env"])
	assert.Equal(t, "platform", tags["Team"])

	// DeleteTags
	req := httptest.NewRequest("DELETE", "/v1/tags/"+arn+"?tagKeys=Env", strings.NewReader(""))
	deleteResp, err := p.HandleRequest(context.Background(), "DeleteTags", req)
	require.NoError(t, err)
	assert.Equal(t, 200, deleteResp.StatusCode)

	// Verify 1 tag remains
	resp3 := callREST(t, p, "GET", "/v1/tags/"+arn, "ListTags", "")
	rb3 := parseBody(t, resp3)
	tags3, ok := rb3["tags"].(map[string]any)
	require.True(t, ok)
	assert.Len(t, tags3, 1)
	assert.Equal(t, "platform", tags3["Team"])
}

func TestRebootBroker(t *testing.T) {
	p := newTestProvider(t)

	// Create broker
	createResp := callREST(t, p, "POST", "/v1/brokers", "CreateBroker", `{"brokerName":"reboot-broker","engineType":"ACTIVEMQ","engineVersion":"5.17.6","hostInstanceType":"mq.m5.large","deploymentMode":"SINGLE_INSTANCE"}`)
	require.Equal(t, 200, createResp.StatusCode)
	rb := parseBody(t, createResp)
	brokerID := rb["brokerId"].(string)

	// Reboot broker
	resp := callREST(t, p, "POST", "/v1/brokers/"+brokerID+"/reboot", "RebootBroker", "")
	assert.Equal(t, 200, resp.StatusCode)

	// Broker should still exist and be in RUNNING state
	resp2 := callREST(t, p, "GET", "/v1/brokers/"+brokerID, "DescribeBroker", "")
	assert.Equal(t, 200, resp2.StatusCode)
	rb2 := parseBody(t, resp2)
	assert.Equal(t, "RUNNING", rb2["brokerState"])

	// Reboot non-existent broker
	resp3 := callREST(t, p, "POST", "/v1/brokers/nonexistent/reboot", "RebootBroker", "")
	assert.Equal(t, 404, resp3.StatusCode)
}
