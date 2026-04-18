// SPDX-License-Identifier: Apache-2.0

// internal/services/servicediscovery/provider_test.go
package servicediscovery

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
	t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
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

const svcName = "Route53AutoNaming_v20170314"

func TestNamespaceCRUD(t *testing.T) {
	p := newTestProvider(t)

	// Create HTTP namespace
	resp := callJSON(t, p, svcName+".CreateHttpNamespace",
		`{"Name":"my-ns","Description":"test namespace"}`)
	assert.Equal(t, 200, resp.StatusCode)
	m := parseJSON(t, resp)
	opID, _ := m["OperationId"].(string)
	assert.NotEmpty(t, opID)

	// List namespaces
	listResp := callJSON(t, p, svcName+".ListNamespaces", `{}`)
	assert.Equal(t, 200, listResp.StatusCode)
	lm := parseJSON(t, listResp)
	nsList := lm["Namespaces"].([]any)
	assert.Len(t, nsList, 1)
	nsEntry := nsList[0].(map[string]any)
	nsID := nsEntry["Id"].(string)
	assert.NotEmpty(t, nsID)
	assert.Equal(t, "my-ns", nsEntry["Name"])
	assert.Equal(t, "HTTP", nsEntry["Type"])

	// Get namespace
	getResp := callJSON(t, p, svcName+".GetNamespace", `{"Id":"`+nsID+`"}`)
	assert.Equal(t, 200, getResp.StatusCode)
	gm := parseJSON(t, getResp)
	ns := gm["Namespace"].(map[string]any)
	assert.Equal(t, "my-ns", ns["Name"])

	// Update namespace
	updateResp := callJSON(t, p, svcName+".UpdateHttpNamespace",
		`{"Id":"`+nsID+`","Namespace":{"Description":"updated"}}`)
	assert.Equal(t, 200, updateResp.StatusCode)

	// Delete namespace
	delResp := callJSON(t, p, svcName+".DeleteNamespace", `{"Id":"`+nsID+`"}`)
	assert.Equal(t, 200, delResp.StatusCode)

	// Verify deleted
	getAfterDel := callJSON(t, p, svcName+".GetNamespace", `{"Id":"`+nsID+`"}`)
	assert.Equal(t, 400, getAfterDel.StatusCode)

	// Create DNS namespaces
	resp2 := callJSON(t, p, svcName+".CreatePrivateDnsNamespace", `{"Name":"private-ns","Vpc":"vpc-123"}`)
	assert.Equal(t, 200, resp2.StatusCode)

	resp3 := callJSON(t, p, svcName+".CreatePublicDnsNamespace", `{"Name":"public-ns"}`)
	assert.Equal(t, 200, resp3.StatusCode)

	listResp2 := callJSON(t, p, svcName+".ListNamespaces", `{}`)
	lm2 := parseJSON(t, listResp2)
	nsList2 := lm2["Namespaces"].([]any)
	assert.Len(t, nsList2, 2)
}

func TestServiceCRUD(t *testing.T) {
	p := newTestProvider(t)

	// Create namespace first
	nsResp := callJSON(t, p, svcName+".CreateHttpNamespace", `{"Name":"ns1"}`)
	nsM := parseJSON(t, nsResp)
	opID := nsM["OperationId"].(string)

	// Get namespace ID via ListNamespaces
	listNsResp := callJSON(t, p, svcName+".ListNamespaces", `{}`)
	lnm := parseJSON(t, listNsResp)
	nsList := lnm["Namespaces"].([]any)
	nsID := nsList[0].(map[string]any)["Id"].(string)
	_ = opID

	// Create service
	svcResp := callJSON(t, p, svcName+".CreateService",
		`{"Name":"my-svc","NamespaceId":"`+nsID+`","Description":"test svc"}`)
	assert.Equal(t, 200, svcResp.StatusCode)
	sm := parseJSON(t, svcResp)
	svcObj := sm["Service"].(map[string]any)
	svcID := svcObj["Id"].(string)
	assert.NotEmpty(t, svcID)
	assert.Equal(t, "my-svc", svcObj["Name"])

	// Get service
	getResp := callJSON(t, p, svcName+".GetService", `{"Id":"`+svcID+`"}`)
	assert.Equal(t, 200, getResp.StatusCode)
	gm := parseJSON(t, getResp)
	assert.Equal(t, "my-svc", gm["Service"].(map[string]any)["Name"])

	// List services
	listResp := callJSON(t, p, svcName+".ListServices", `{}`)
	assert.Equal(t, 200, listResp.StatusCode)
	lm := parseJSON(t, listResp)
	svcs := lm["Services"].([]any)
	assert.Len(t, svcs, 1)

	// Update service
	updateResp := callJSON(t, p, svcName+".UpdateService",
		`{"Id":"`+svcID+`","Service":{"Description":"updated"}}`)
	assert.Equal(t, 200, updateResp.StatusCode)

	// Delete service
	delResp := callJSON(t, p, svcName+".DeleteService", `{"Id":"`+svcID+`"}`)
	assert.Equal(t, 200, delResp.StatusCode)

	// Verify deleted
	getAfter := callJSON(t, p, svcName+".GetService", `{"Id":"`+svcID+`"}`)
	assert.Equal(t, 400, getAfter.StatusCode)
}

func TestInstanceCRUD(t *testing.T) {
	p := newTestProvider(t)

	// Create namespace and service
	callJSON(t, p, svcName+".CreateHttpNamespace", `{"Name":"ns1"}`)
	listNs := parseJSON(t, callJSON(t, p, svcName+".ListNamespaces", `{}`))
	nsID := listNs["Namespaces"].([]any)[0].(map[string]any)["Id"].(string)

	svcResp := callJSON(t, p, svcName+".CreateService",
		`{"Name":"svc1","NamespaceId":"`+nsID+`"}`)
	svcM := parseJSON(t, svcResp)
	svcID := svcM["Service"].(map[string]any)["Id"].(string)

	// Register instance
	regResp := callJSON(t, p, svcName+".RegisterInstance",
		`{"ServiceId":"`+svcID+`","InstanceId":"inst-1","Attributes":{"AWS_INSTANCE_IPV4":"10.0.0.1","AWS_INSTANCE_PORT":"8080"}}`)
	assert.Equal(t, 200, regResp.StatusCode)
	regM := parseJSON(t, regResp)
	assert.NotEmpty(t, regM["OperationId"])

	// Get instance
	getResp := callJSON(t, p, svcName+".GetInstance",
		`{"ServiceId":"`+svcID+`","InstanceId":"inst-1"}`)
	assert.Equal(t, 200, getResp.StatusCode)
	gm := parseJSON(t, getResp)
	inst := gm["Instance"].(map[string]any)
	assert.Equal(t, "inst-1", inst["Id"])

	// List instances
	listResp := callJSON(t, p, svcName+".ListInstances",
		`{"ServiceId":"`+svcID+`"}`)
	assert.Equal(t, 200, listResp.StatusCode)
	lm := parseJSON(t, listResp)
	instances := lm["Instances"].([]any)
	assert.Len(t, instances, 1)

	// Discover instances
	discResp := callJSON(t, p, svcName+".DiscoverInstances",
		`{"NamespaceName":"ns1","ServiceName":"svc1"}`)
	assert.Equal(t, 200, discResp.StatusCode)
	dm := parseJSON(t, discResp)
	discovered := dm["Instances"].([]any)
	assert.Len(t, discovered, 1)

	// Get instances health status
	healthResp := callJSON(t, p, svcName+".GetInstancesHealthStatus",
		`{"ServiceId":"`+svcID+`"}`)
	assert.Equal(t, 200, healthResp.StatusCode)

	// Deregister instance
	deregResp := callJSON(t, p, svcName+".DeregisterInstance",
		`{"ServiceId":"`+svcID+`","InstanceId":"inst-1"}`)
	assert.Equal(t, 200, deregResp.StatusCode)

	// Verify deregistered
	getAfter := callJSON(t, p, svcName+".GetInstance",
		`{"ServiceId":"`+svcID+`","InstanceId":"inst-1"}`)
	assert.Equal(t, 400, getAfter.StatusCode)
}

func TestTags(t *testing.T) {
	p := newTestProvider(t)

	// Create namespace with tags
	callJSON(t, p, svcName+".CreateHttpNamespace",
		`{"Name":"tagged-ns","Tags":[{"Key":"env","Value":"prod"}]}`)
	listNs := parseJSON(t, callJSON(t, p, svcName+".ListNamespaces", `{}`))
	nsARN := listNs["Namespaces"].([]any)[0].(map[string]any)["Arn"].(string)

	// List tags
	listResp := callJSON(t, p, svcName+".ListTagsForResource",
		`{"ResourceARN":"`+nsARN+`"}`)
	assert.Equal(t, 200, listResp.StatusCode)
	lm := parseJSON(t, listResp)
	tags := lm["Tags"].([]any)
	assert.Len(t, tags, 1)

	// Add more tags
	callJSON(t, p, svcName+".TagResource",
		`{"ResourceARN":"`+nsARN+`","Tags":[{"Key":"team","Value":"platform"}]}`)
	listResp2 := callJSON(t, p, svcName+".ListTagsForResource",
		`{"ResourceARN":"`+nsARN+`"}`)
	lm2 := parseJSON(t, listResp2)
	tags2 := lm2["Tags"].([]any)
	assert.Len(t, tags2, 2)

	// Remove a tag
	callJSON(t, p, svcName+".UntagResource",
		`{"ResourceARN":"`+nsARN+`","TagKeys":["env"]}`)
	listResp3 := callJSON(t, p, svcName+".ListTagsForResource",
		`{"ResourceARN":"`+nsARN+`"}`)
	lm3 := parseJSON(t, listResp3)
	tags3 := lm3["Tags"].([]any)
	assert.Len(t, tags3, 1)
}
