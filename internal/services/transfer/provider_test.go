// SPDX-License-Identifier: Apache-2.0

// internal/services/transfer/provider_test.go
package transfer

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
	require.NoError(t, p.Init(plugin.PluginConfig{DataDir: t.TempDir()}))
	t.Cleanup(func() { p.Shutdown(context.Background()) })
	return p
}

func callJSON(t *testing.T, p *Provider, target, body string) *plugin.Response {
	t.Helper()
	req := httptest.NewRequest("POST", "/", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req.Header.Set("X-Amz-Target", target)
	resp, err := p.HandleRequest(context.Background(), "", req)
	require.NoError(t, err)
	return resp
}

func parseJSON(t *testing.T, resp *plugin.Response) map[string]any {
	t.Helper()
	var m map[string]any
	require.NoError(t, json.Unmarshal(resp.Body, &m))
	return m
}

const svcName = "TransferService"

func createTestServer(t *testing.T, p *Provider) string {
	t.Helper()
	resp := callJSON(t, p, svcName+".CreateServer", `{"Protocols":["SFTP"],"EndpointType":"PUBLIC"}`)
	require.Equal(t, 200, resp.StatusCode)
	m := parseJSON(t, resp)
	serverID, _ := m["ServerId"].(string)
	require.NotEmpty(t, serverID)
	return serverID
}

func TestServerCRUD(t *testing.T) {
	p := newTestProvider(t)

	// Create server
	resp := callJSON(t, p, svcName+".CreateServer",
		`{"Protocols":["SFTP"],"EndpointType":"PUBLIC","Domain":"S3","Tags":[{"Key":"env","Value":"test"}]}`)
	assert.Equal(t, 200, resp.StatusCode)
	m := parseJSON(t, resp)
	serverID, _ := m["ServerId"].(string)
	assert.NotEmpty(t, serverID)

	// Describe server
	descResp := callJSON(t, p, svcName+".DescribeServer", `{"ServerId":"`+serverID+`"}`)
	assert.Equal(t, 200, descResp.StatusCode)
	dm := parseJSON(t, descResp)
	srv := dm["Server"].(map[string]any)
	assert.Equal(t, serverID, srv["ServerId"])
	assert.Equal(t, "ONLINE", srv["State"])
	assert.Equal(t, "PUBLIC", srv["EndpointType"])

	// List servers
	listResp := callJSON(t, p, svcName+".ListServers", `{}`)
	assert.Equal(t, 200, listResp.StatusCode)
	lm := parseJSON(t, listResp)
	servers := lm["Servers"].([]any)
	assert.Len(t, servers, 1)

	// Update server
	updateResp := callJSON(t, p, svcName+".UpdateServer",
		`{"ServerId":"`+serverID+`","LoggingRole":"arn:aws:iam::123456789012:role/transfer-role"}`)
	assert.Equal(t, 200, updateResp.StatusCode)

	// Delete server
	delResp := callJSON(t, p, svcName+".DeleteServer", `{"ServerId":"`+serverID+`"}`)
	assert.Equal(t, 200, delResp.StatusCode)

	// Verify deleted
	descAfter := callJSON(t, p, svcName+".DescribeServer", `{"ServerId":"`+serverID+`"}`)
	assert.Equal(t, 400, descAfter.StatusCode)
}

func TestStartStopServer(t *testing.T) {
	p := newTestProvider(t)
	serverID := createTestServer(t, p)

	// Stop server
	stopResp := callJSON(t, p, svcName+".StopServer", `{"ServerId":"`+serverID+`"}`)
	assert.Equal(t, 200, stopResp.StatusCode)

	// Verify OFFLINE
	descResp := callJSON(t, p, svcName+".DescribeServer", `{"ServerId":"`+serverID+`"}`)
	dm := parseJSON(t, descResp)
	srv := dm["Server"].(map[string]any)
	assert.Equal(t, "OFFLINE", srv["State"])

	// Start server
	startResp := callJSON(t, p, svcName+".StartServer", `{"ServerId":"`+serverID+`"}`)
	assert.Equal(t, 200, startResp.StatusCode)

	// Verify ONLINE
	descResp2 := callJSON(t, p, svcName+".DescribeServer", `{"ServerId":"`+serverID+`"}`)
	dm2 := parseJSON(t, descResp2)
	srv2 := dm2["Server"].(map[string]any)
	assert.Equal(t, "ONLINE", srv2["State"])
}

func TestHostKeyCRUD(t *testing.T) {
	p := newTestProvider(t)
	serverID := createTestServer(t, p)

	// Import host key
	importResp := callJSON(t, p, svcName+".ImportHostKey",
		`{"ServerId":"`+serverID+`","HostKeyBody":"ssh-rsa AAAAB3Nza...","Description":"test key","Tags":[{"Key":"env","Value":"test"}]}`)
	assert.Equal(t, 200, importResp.StatusCode)
	im := parseJSON(t, importResp)
	hostKeyID, _ := im["HostKeyId"].(string)
	assert.NotEmpty(t, hostKeyID)
	assert.Equal(t, serverID, im["ServerId"])

	// Describe host key
	descResp := callJSON(t, p, svcName+".DescribeHostKey",
		`{"ServerId":"`+serverID+`","HostKeyId":"`+hostKeyID+`"}`)
	assert.Equal(t, 200, descResp.StatusCode)
	dm := parseJSON(t, descResp)
	hk := dm["HostKey"].(map[string]any)
	assert.Equal(t, hostKeyID, hk["HostKeyId"])
	assert.Equal(t, "test key", hk["Description"])

	// List host keys
	listResp := callJSON(t, p, svcName+".ListHostKeys", `{"ServerId":"`+serverID+`"}`)
	assert.Equal(t, 200, listResp.StatusCode)
	lm := parseJSON(t, listResp)
	hostKeys := lm["HostKeys"].([]any)
	assert.Len(t, hostKeys, 1)

	// Update host key
	updateResp := callJSON(t, p, svcName+".UpdateHostKey",
		`{"ServerId":"`+serverID+`","HostKeyId":"`+hostKeyID+`","Description":"updated key"}`)
	assert.Equal(t, 200, updateResp.StatusCode)
	um := parseJSON(t, updateResp)
	assert.Equal(t, hostKeyID, um["HostKeyId"])

	// Delete host key
	delResp := callJSON(t, p, svcName+".DeleteHostKey",
		`{"ServerId":"`+serverID+`","HostKeyId":"`+hostKeyID+`"}`)
	assert.Equal(t, 200, delResp.StatusCode)

	// Verify deleted
	descAfter := callJSON(t, p, svcName+".DescribeHostKey",
		`{"ServerId":"`+serverID+`","HostKeyId":"`+hostKeyID+`"}`)
	assert.Equal(t, 400, descAfter.StatusCode)
}

func TestAccessCRUD(t *testing.T) {
	p := newTestProvider(t)
	serverID := createTestServer(t, p)

	// Create access
	createResp := callJSON(t, p, svcName+".CreateAccess",
		`{"ServerId":"`+serverID+`","ExternalId":"S-1-5-21-123456","Role":"arn:aws:iam::123456789012:role/transfer-role","HomeDirectory":"/mybucket/users"}`)
	assert.Equal(t, 200, createResp.StatusCode)
	cm := parseJSON(t, createResp)
	assert.Equal(t, serverID, cm["ServerId"])
	assert.Equal(t, "S-1-5-21-123456", cm["ExternalId"])

	// Describe access
	descResp := callJSON(t, p, svcName+".DescribeAccess",
		`{"ServerId":"`+serverID+`","ExternalId":"S-1-5-21-123456"}`)
	assert.Equal(t, 200, descResp.StatusCode)
	dm := parseJSON(t, descResp)
	access := dm["Access"].(map[string]any)
	assert.Equal(t, "S-1-5-21-123456", access["ExternalId"])
	assert.Equal(t, "/mybucket/users", access["HomeDirectory"])

	// List accesses
	listResp := callJSON(t, p, svcName+".ListAccesses", `{"ServerId":"`+serverID+`"}`)
	assert.Equal(t, 200, listResp.StatusCode)
	lm := parseJSON(t, listResp)
	accesses := lm["Accesses"].([]any)
	assert.Len(t, accesses, 1)

	// Update access
	updateResp := callJSON(t, p, svcName+".UpdateAccess",
		`{"ServerId":"`+serverID+`","ExternalId":"S-1-5-21-123456","HomeDirectory":"/newbucket"}`)
	assert.Equal(t, 200, updateResp.StatusCode)
	um := parseJSON(t, updateResp)
	assert.Equal(t, "S-1-5-21-123456", um["ExternalId"])

	// Delete access
	delResp := callJSON(t, p, svcName+".DeleteAccess",
		`{"ServerId":"`+serverID+`","ExternalId":"S-1-5-21-123456"}`)
	assert.Equal(t, 200, delResp.StatusCode)

	// Verify deleted
	descAfter := callJSON(t, p, svcName+".DescribeAccess",
		`{"ServerId":"`+serverID+`","ExternalId":"S-1-5-21-123456"}`)
	assert.Equal(t, 400, descAfter.StatusCode)
}

func TestTags(t *testing.T) {
	p := newTestProvider(t)

	// Create server with initial tags
	createResp := callJSON(t, p, svcName+".CreateServer",
		`{"Tags":[{"Key":"env","Value":"prod"}]}`)
	assert.Equal(t, 200, createResp.StatusCode)
	cm := parseJSON(t, createResp)
	serverID := cm["ServerId"].(string)

	// Get server ARN via describe
	descResp := callJSON(t, p, svcName+".DescribeServer", `{"ServerId":"`+serverID+`"}`)
	dm := parseJSON(t, descResp)
	srv := dm["Server"].(map[string]any)
	arn := srv["Arn"].(string)
	assert.NotEmpty(t, arn)

	// List tags
	listResp := callJSON(t, p, svcName+".ListTagsForResource", `{"Arn":"`+arn+`"}`)
	assert.Equal(t, 200, listResp.StatusCode)
	lm := parseJSON(t, listResp)
	tags := lm["Tags"].([]any)
	assert.Len(t, tags, 1)

	// Add more tags
	callJSON(t, p, svcName+".TagResource",
		`{"Arn":"`+arn+`","Tags":[{"Key":"team","Value":"platform"}]}`)
	listResp2 := callJSON(t, p, svcName+".ListTagsForResource", `{"Arn":"`+arn+`"}`)
	lm2 := parseJSON(t, listResp2)
	tags2 := lm2["Tags"].([]any)
	assert.Len(t, tags2, 2)

	// Remove a tag
	callJSON(t, p, svcName+".UntagResource",
		`{"Arn":"`+arn+`","TagKeys":["env"]}`)
	listResp3 := callJSON(t, p, svcName+".ListTagsForResource", `{"Arn":"`+arn+`"}`)
	lm3 := parseJSON(t, listResp3)
	tags3 := lm3["Tags"].([]any)
	assert.Len(t, tags3, 1)
}
