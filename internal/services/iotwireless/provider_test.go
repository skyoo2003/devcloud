// SPDX-License-Identifier: Apache-2.0

// internal/services/iotwireless/provider_test.go
package iotwireless

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

func TestDestinationCRUD(t *testing.T) {
	p := newTestProvider(t)

	// Create
	resp := callREST(t, p, "POST", "/destinations", "CreateDestination",
		`{"Name":"my-dest","Expression":"rule/my-rule","ExpressionType":"RuleName","RoleArn":"arn:aws:iam::000000000000:role/myrole","Description":"test"}`)
	assert.Equal(t, 201, resp.StatusCode)
	rb := parseBody(t, resp)
	assert.Equal(t, "my-dest", rb["Name"])
	assert.NotEmpty(t, rb["Arn"])

	// Get
	resp2 := callREST(t, p, "GET", "/destinations/my-dest", "GetDestination", "")
	assert.Equal(t, 200, resp2.StatusCode)
	rb2 := parseBody(t, resp2)
	assert.Equal(t, "my-dest", rb2["Name"])
	assert.Equal(t, "RuleName", rb2["ExpressionType"])
	assert.Equal(t, "test", rb2["Description"])

	// List
	resp3 := callREST(t, p, "GET", "/destinations", "ListDestinations", "")
	assert.Equal(t, 200, resp3.StatusCode)
	rb3 := parseBody(t, resp3)
	list, ok := rb3["DestinationList"].([]any)
	require.True(t, ok)
	assert.Len(t, list, 1)

	// Update
	resp4 := callREST(t, p, "PATCH", "/destinations/my-dest", "UpdateDestination",
		`{"Description":"updated"}`)
	assert.Equal(t, 204, resp4.StatusCode)

	// Verify update
	resp5 := callREST(t, p, "GET", "/destinations/my-dest", "GetDestination", "")
	assert.Equal(t, 200, resp5.StatusCode)
	rb5 := parseBody(t, resp5)
	assert.Equal(t, "updated", rb5["Description"])

	// Delete
	resp6 := callREST(t, p, "DELETE", "/destinations/my-dest", "DeleteDestination", "")
	assert.Equal(t, 204, resp6.StatusCode)

	// Not found after delete
	resp7 := callREST(t, p, "GET", "/destinations/my-dest", "GetDestination", "")
	assert.Equal(t, 404, resp7.StatusCode)
}

func TestDeviceProfileCRUD(t *testing.T) {
	p := newTestProvider(t)

	// Create
	resp := callREST(t, p, "POST", "/device-profiles", "CreateDeviceProfile",
		`{"Name":"my-profile","LoRaWAN":{"MacVersion":"1.0.2"}}`)
	assert.Equal(t, 201, resp.StatusCode)
	rb := parseBody(t, resp)
	id, ok := rb["Id"].(string)
	require.True(t, ok)
	require.NotEmpty(t, id)
	assert.NotEmpty(t, rb["Arn"])

	// Get
	resp2 := callREST(t, p, "GET", "/device-profiles/"+id, "GetDeviceProfile", "")
	assert.Equal(t, 200, resp2.StatusCode)
	rb2 := parseBody(t, resp2)
	assert.Equal(t, id, rb2["Id"])
	assert.Equal(t, "my-profile", rb2["Name"])

	// List
	resp3 := callREST(t, p, "GET", "/device-profiles", "ListDeviceProfiles", "")
	assert.Equal(t, 200, resp3.StatusCode)
	rb3 := parseBody(t, resp3)
	list, ok := rb3["DeviceProfileList"].([]any)
	require.True(t, ok)
	assert.Len(t, list, 1)

	// Delete
	resp4 := callREST(t, p, "DELETE", "/device-profiles/"+id, "DeleteDeviceProfile", "")
	assert.Equal(t, 204, resp4.StatusCode)

	// Not found after delete
	resp5 := callREST(t, p, "GET", "/device-profiles/"+id, "GetDeviceProfile", "")
	assert.Equal(t, 404, resp5.StatusCode)
}

func TestWirelessDeviceCRUD(t *testing.T) {
	p := newTestProvider(t)

	// Create
	resp := callREST(t, p, "POST", "/wireless-devices", "CreateWirelessDevice",
		`{"Name":"my-device","Type":"LoRaWAN","DestinationName":"my-dest","Description":"a device"}`)
	assert.Equal(t, 201, resp.StatusCode)
	rb := parseBody(t, resp)
	id, ok := rb["Id"].(string)
	require.True(t, ok)
	require.NotEmpty(t, id)

	// Get
	resp2 := callREST(t, p, "GET", "/wireless-devices/"+id, "GetWirelessDevice", "")
	assert.Equal(t, 200, resp2.StatusCode)
	rb2 := parseBody(t, resp2)
	assert.Equal(t, id, rb2["Id"])
	assert.Equal(t, "my-device", rb2["Name"])
	assert.Equal(t, "LoRaWAN", rb2["Type"])
	assert.Equal(t, "my-dest", rb2["DestinationName"])
	assert.Equal(t, "a device", rb2["Description"])

	// List
	resp3 := callREST(t, p, "GET", "/wireless-devices", "ListWirelessDevices", "")
	assert.Equal(t, 200, resp3.StatusCode)
	rb3 := parseBody(t, resp3)
	list, ok := rb3["WirelessDeviceList"].([]any)
	require.True(t, ok)
	assert.Len(t, list, 1)

	// Update
	resp4 := callREST(t, p, "PATCH", "/wireless-devices/"+id, "UpdateWirelessDevice",
		`{"Name":"renamed-device","DestinationName":"new-dest"}`)
	assert.Equal(t, 204, resp4.StatusCode)

	// Verify update
	resp5 := callREST(t, p, "GET", "/wireless-devices/"+id, "GetWirelessDevice", "")
	assert.Equal(t, 200, resp5.StatusCode)
	rb5 := parseBody(t, resp5)
	assert.Equal(t, "renamed-device", rb5["Name"])
	assert.Equal(t, "new-dest", rb5["DestinationName"])

	// Delete
	resp6 := callREST(t, p, "DELETE", "/wireless-devices/"+id, "DeleteWirelessDevice", "")
	assert.Equal(t, 204, resp6.StatusCode)

	// Not found after delete
	resp7 := callREST(t, p, "GET", "/wireless-devices/"+id, "GetWirelessDevice", "")
	assert.Equal(t, 404, resp7.StatusCode)
}

func TestWirelessGatewayCRUD(t *testing.T) {
	p := newTestProvider(t)

	// Create
	resp := callREST(t, p, "POST", "/wireless-gateways", "CreateWirelessGateway",
		`{"Name":"my-gateway","Description":"a gateway","LoRaWAN":{"GatewayEui":"a1b2c3d4e5f60708"}}`)
	assert.Equal(t, 201, resp.StatusCode)
	rb := parseBody(t, resp)
	id, ok := rb["Id"].(string)
	require.True(t, ok)
	require.NotEmpty(t, id)

	// Get
	resp2 := callREST(t, p, "GET", "/wireless-gateways/"+id, "GetWirelessGateway", "")
	assert.Equal(t, 200, resp2.StatusCode)
	rb2 := parseBody(t, resp2)
	assert.Equal(t, id, rb2["Id"])
	assert.Equal(t, "my-gateway", rb2["Name"])
	assert.Equal(t, "a gateway", rb2["Description"])

	// List
	resp3 := callREST(t, p, "GET", "/wireless-gateways", "ListWirelessGateways", "")
	assert.Equal(t, 200, resp3.StatusCode)
	rb3 := parseBody(t, resp3)
	list, ok := rb3["WirelessGatewayList"].([]any)
	require.True(t, ok)
	assert.Len(t, list, 1)

	// Update
	resp4 := callREST(t, p, "PATCH", "/wireless-gateways/"+id, "UpdateWirelessGateway",
		`{"Name":"renamed-gw","Description":"updated desc"}`)
	assert.Equal(t, 204, resp4.StatusCode)

	// Verify update
	resp5 := callREST(t, p, "GET", "/wireless-gateways/"+id, "GetWirelessGateway", "")
	assert.Equal(t, 200, resp5.StatusCode)
	rb5 := parseBody(t, resp5)
	assert.Equal(t, "renamed-gw", rb5["Name"])
	assert.Equal(t, "updated desc", rb5["Description"])

	// Delete
	resp6 := callREST(t, p, "DELETE", "/wireless-gateways/"+id, "DeleteWirelessGateway", "")
	assert.Equal(t, 204, resp6.StatusCode)

	// Not found after delete
	resp7 := callREST(t, p, "GET", "/wireless-gateways/"+id, "GetWirelessGateway", "")
	assert.Equal(t, 404, resp7.StatusCode)
}

func TestFuotaTaskCRUD(t *testing.T) {
	p := newTestProvider(t)

	// Create
	resp := callREST(t, p, "POST", "/fuota-tasks", "CreateFuotaTask",
		`{"Name":"my-fuota","LoRaWAN":{"RfRegion":"US915"}}`)
	assert.Equal(t, 201, resp.StatusCode)
	rb := parseBody(t, resp)
	id, ok := rb["Id"].(string)
	require.True(t, ok)
	require.NotEmpty(t, id)

	// Get
	resp2 := callREST(t, p, "GET", "/fuota-tasks/"+id, "GetFuotaTask", "")
	assert.Equal(t, 200, resp2.StatusCode)
	rb2 := parseBody(t, resp2)
	assert.Equal(t, id, rb2["Id"])
	assert.Equal(t, "my-fuota", rb2["Name"])
	assert.Equal(t, "Pending", rb2["Status"])

	// List
	resp3 := callREST(t, p, "GET", "/fuota-tasks", "ListFuotaTasks", "")
	assert.Equal(t, 200, resp3.StatusCode)
	rb3 := parseBody(t, resp3)
	list, ok := rb3["FuotaTaskList"].([]any)
	require.True(t, ok)
	assert.Len(t, list, 1)

	// Update
	resp4 := callREST(t, p, "PATCH", "/fuota-tasks/"+id, "UpdateFuotaTask",
		`{"Name":"updated-fuota"}`)
	assert.Equal(t, 204, resp4.StatusCode)

	// StartFuotaTask
	resp5 := callREST(t, p, "PUT", "/fuota-tasks/"+id, "StartFuotaTask", "")
	assert.Equal(t, 204, resp5.StatusCode)

	// Verify status changed
	resp6 := callREST(t, p, "GET", "/fuota-tasks/"+id, "GetFuotaTask", "")
	assert.Equal(t, 200, resp6.StatusCode)
	rb6 := parseBody(t, resp6)
	assert.Equal(t, "FuotaSession_Waiting", rb6["Status"])

	// Delete
	resp7 := callREST(t, p, "DELETE", "/fuota-tasks/"+id, "DeleteFuotaTask", "")
	assert.Equal(t, 204, resp7.StatusCode)

	// Not found after delete
	resp8 := callREST(t, p, "GET", "/fuota-tasks/"+id, "GetFuotaTask", "")
	assert.Equal(t, 404, resp8.StatusCode)
}

func TestTags(t *testing.T) {
	p := newTestProvider(t)

	// Create a wireless device with tags
	resp := callREST(t, p, "POST", "/wireless-devices", "CreateWirelessDevice",
		`{"Name":"tagged-device","Type":"LoRaWAN","DestinationName":"dest","Tags":[{"Key":"env","Value":"prod"},{"Key":"owner","Value":"team-a"}]}`)
	assert.Equal(t, 201, resp.StatusCode)
	rb := parseBody(t, resp)
	arn, _ := rb["Arn"].(string)
	require.NotEmpty(t, arn)
	id, _ := rb["Id"].(string)

	// Get should include tags
	resp2 := callREST(t, p, "GET", "/wireless-devices/"+id, "GetWirelessDevice", "")
	assert.Equal(t, 200, resp2.StatusCode)
	rb2 := parseBody(t, resp2)
	tags2, ok := rb2["Tags"].([]any)
	require.True(t, ok)
	assert.Len(t, tags2, 2)

	// ListTagsForResource
	resp3 := callREST(t, p, "GET", "/tags?resourceArn="+arn, "ListTagsForResource", "")
	assert.Equal(t, 200, resp3.StatusCode)
	rb3 := parseBody(t, resp3)
	tagList, ok := rb3["Tags"].([]any)
	require.True(t, ok)
	assert.Len(t, tagList, 2)

	// TagResource - add a new tag
	resp4 := callREST(t, p, "POST", "/tags?resourceArn="+arn, "TagResource",
		`{"Tags":[{"Key":"new-key","Value":"new-val"}]}`)
	assert.Equal(t, 200, resp4.StatusCode)

	// Verify added tag
	resp5 := callREST(t, p, "GET", "/tags?resourceArn="+arn, "ListTagsForResource", "")
	assert.Equal(t, 200, resp5.StatusCode)
	rb5 := parseBody(t, resp5)
	tagList5, ok := rb5["Tags"].([]any)
	require.True(t, ok)
	assert.Len(t, tagList5, 3)

	// UntagResource
	resp6 := callREST(t, p, "DELETE", "/tags?resourceArn="+arn+"&tagKeys=env", "UntagResource", "")
	assert.Equal(t, 200, resp6.StatusCode)

	// Verify tag removed
	resp7 := callREST(t, p, "GET", "/tags?resourceArn="+arn, "ListTagsForResource", "")
	assert.Equal(t, 200, resp7.StatusCode)
	rb7 := parseBody(t, resp7)
	tagList7, ok := rb7["Tags"].([]any)
	require.True(t, ok)
	assert.Len(t, tagList7, 2)
}

func TestServiceProfileCRUD(t *testing.T) {
	p := newTestProvider(t)

	// Create
	resp := callREST(t, p, "POST", "/service-profiles", "CreateServiceProfile",
		`{"Name":"my-svc-profile"}`)
	assert.Equal(t, 201, resp.StatusCode)
	rb := parseBody(t, resp)
	id, ok := rb["Id"].(string)
	require.True(t, ok)
	require.NotEmpty(t, id)

	// Get
	resp2 := callREST(t, p, "GET", "/service-profiles/"+id, "GetServiceProfile", "")
	assert.Equal(t, 200, resp2.StatusCode)
	rb2 := parseBody(t, resp2)
	assert.Equal(t, id, rb2["Id"])
	assert.Equal(t, "my-svc-profile", rb2["Name"])

	// List
	resp3 := callREST(t, p, "GET", "/service-profiles", "ListServiceProfiles", "")
	assert.Equal(t, 200, resp3.StatusCode)
	rb3 := parseBody(t, resp3)
	list, ok := rb3["ServiceProfileList"].([]any)
	require.True(t, ok)
	assert.Len(t, list, 1)

	// Delete
	resp4 := callREST(t, p, "DELETE", "/service-profiles/"+id, "DeleteServiceProfile", "")
	assert.Equal(t, 204, resp4.StatusCode)

	// Not found after delete
	resp5 := callREST(t, p, "GET", "/service-profiles/"+id, "GetServiceProfile", "")
	assert.Equal(t, 404, resp5.StatusCode)
}

func TestStubOperations(t *testing.T) {
	p := newTestProvider(t)

	// Stub operations should return 200 with empty body
	resp := callREST(t, p, "GET", "/service-endpoint", "GetServiceEndpoint", "")
	assert.Equal(t, 200, resp.StatusCode)

	resp2 := callREST(t, p, "GET", "/log-levels", "GetLogLevelsByResourceTypes", "")
	assert.Equal(t, 200, resp2.StatusCode)
}
