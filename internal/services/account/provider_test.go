// SPDX-License-Identifier: Apache-2.0

package account

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

func newTestProvider(t *testing.T) *AccountProvider {
	t.Helper()
	p := &AccountProvider{}
	err := p.Init(plugin.PluginConfig{DataDir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
	return p
}

func call(t *testing.T, p *AccountProvider, method, path, body string) *plugin.Response {
	t.Helper()
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err := p.HandleRequest(context.Background(), "", req)
	require.NoError(t, err)
	return resp
}

func parseBody(t *testing.T, resp *plugin.Response) map[string]any {
	t.Helper()
	var m map[string]any
	require.NoError(t, json.Unmarshal(resp.Body, &m))
	return m
}

func TestContactInformationCRUD(t *testing.T) {
	p := newTestProvider(t)

	// Get - not found initially
	resp := call(t, p, "GET", "/contactInformation", "")
	assert.Equal(t, 404, resp.StatusCode)

	// Put
	body := `{"ContactInformation":{"FullName":"John Doe","CompanyName":"ACME","PhoneNumber":"+1-555-1234","AddressLine1":"123 Main St","City":"Springfield","PostalCode":"12345","CountryCode":"US"}}`
	resp2 := call(t, p, "PUT", "/contactInformation", body)
	assert.Equal(t, 204, resp2.StatusCode)

	// Get - should exist now
	resp3 := call(t, p, "GET", "/contactInformation", "")
	assert.Equal(t, 200, resp3.StatusCode)
	rb3 := parseBody(t, resp3)
	ci := rb3["ContactInformation"].(map[string]any)
	assert.Equal(t, "John Doe", ci["FullName"])
	assert.Equal(t, "ACME", ci["CompanyName"])

	// Delete
	resp4 := call(t, p, "DELETE", "/contactInformation", "")
	assert.Equal(t, 204, resp4.StatusCode)

	// Get after delete - not found
	resp5 := call(t, p, "GET", "/contactInformation", "")
	assert.Equal(t, 404, resp5.StatusCode)
}

func TestAlternateContactCRUD(t *testing.T) {
	p := newTestProvider(t)

	// Get - not found
	resp := call(t, p, "GET", "/alternateContact?AlternateContactType=BILLING", "")
	assert.Equal(t, 404, resp.StatusCode)

	// Put
	body := `{"AlternateContactType":"BILLING","Name":"Jane Smith","Title":"CFO","EmailAddress":"jane@example.com","PhoneNumber":"+1-555-5678"}`
	resp2 := call(t, p, "PUT", "/alternateContact", body)
	assert.Equal(t, 204, resp2.StatusCode)

	// Get
	resp3 := call(t, p, "GET", "/alternateContact?AlternateContactType=BILLING", "")
	assert.Equal(t, 200, resp3.StatusCode)
	rb3 := parseBody(t, resp3)
	ac := rb3["AlternateContact"].(map[string]any)
	assert.Equal(t, "Jane Smith", ac["Name"])

	// Delete
	resp4 := call(t, p, "DELETE", "/alternateContact?AlternateContactType=BILLING", "")
	assert.Equal(t, 204, resp4.StatusCode)

	// Get after delete
	resp5 := call(t, p, "GET", "/alternateContact?AlternateContactType=BILLING", "")
	assert.Equal(t, 404, resp5.StatusCode)
}

func TestListRegions(t *testing.T) {
	p := newTestProvider(t)
	resp := call(t, p, "GET", "/regions", "")
	assert.Equal(t, 200, resp.StatusCode)
	rb := parseBody(t, resp)
	regions, ok := rb["Regions"].([]any)
	require.True(t, ok)
	assert.GreaterOrEqual(t, len(regions), 10)
}

func TestGetRegionOptStatus(t *testing.T) {
	p := newTestProvider(t)
	resp := call(t, p, "GET", "/regionOptStatus?RegionName=us-east-1", "")
	assert.Equal(t, 200, resp.StatusCode)
	rb := parseBody(t, resp)
	assert.Equal(t, "us-east-1", rb["RegionName"])
	assert.Equal(t, "ENABLED", rb["RegionOptStatus"])
}

func TestEnableDisableRegion(t *testing.T) {
	p := newTestProvider(t)

	// Disable
	resp := call(t, p, "POST", "/disableRegion", `{"RegionName":"ap-northeast-1"}`)
	assert.Equal(t, 204, resp.StatusCode)

	// Verify
	resp2 := call(t, p, "GET", "/regionOptStatus?RegionName=ap-northeast-1", "")
	assert.Equal(t, 200, resp2.StatusCode)
	rb2 := parseBody(t, resp2)
	assert.Equal(t, "DISABLED", rb2["RegionOptStatus"])

	// Enable again
	resp3 := call(t, p, "POST", "/enableRegion", `{"RegionName":"ap-northeast-1"}`)
	assert.Equal(t, 204, resp3.StatusCode)

	// Verify enabled
	resp4 := call(t, p, "GET", "/regionOptStatus?RegionName=ap-northeast-1", "")
	rb4 := parseBody(t, resp4)
	assert.Equal(t, "ENABLED", rb4["RegionOptStatus"])
}

func TestPrimaryEmail(t *testing.T) {
	p := newTestProvider(t)

	// Get default
	resp := call(t, p, "GET", "/primaryEmail", "")
	assert.Equal(t, 200, resp.StatusCode)
	rb := parseBody(t, resp)
	assert.NotEmpty(t, rb["PrimaryEmail"])

	// Start update
	resp2 := call(t, p, "POST", "/primaryEmailUpdate", `{"PrimaryEmail":"new@example.com"}`)
	assert.Equal(t, 201, resp2.StatusCode)

	// Accept update
	resp3 := call(t, p, "POST", "/primaryEmailUpdate/accept", `{"Otp":"123456"}`)
	assert.Equal(t, 200, resp3.StatusCode)
	rb3 := parseBody(t, resp3)
	assert.Equal(t, "new@example.com", rb3["PrimaryEmail"])

	// Get new email
	resp4 := call(t, p, "GET", "/primaryEmail", "")
	rb4 := parseBody(t, resp4)
	assert.Equal(t, "new@example.com", rb4["PrimaryEmail"])
}
