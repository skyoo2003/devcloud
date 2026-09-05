// SPDX-License-Identifier: Apache-2.0

package gateway

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/skyoo2003/devcloud/internal/admin"
	"github.com/skyoo2003/devcloud/internal/plugin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// stubPlugin is a minimal ServicePlugin implementation for testing.
type stubPlugin struct {
	serviceID string
	response  *plugin.Response
}

func (s *stubPlugin) ServiceID() string                { return s.serviceID }
func (s *stubPlugin) ServiceName() string              { return s.serviceID }
func (s *stubPlugin) Protocol() plugin.ProtocolType    { return plugin.ProtocolRESTXML }
func (s *stubPlugin) Init(_ plugin.PluginConfig) error { return nil }
func (s *stubPlugin) Shutdown(_ context.Context) error { return nil }
func (s *stubPlugin) HandleRequest(_ context.Context, _ string, _ *http.Request) (*plugin.Response, error) {
	return s.response, nil
}
func (s *stubPlugin) ListResources(_ context.Context) ([]plugin.Resource, error) { return nil, nil }

// registerStub injects a stub plugin directly into the registry's active map
// by going through Init with a pre-registered factory.
func newRegistryWithStub(serviceID string, resp *plugin.Response) *plugin.Registry {
	reg := plugin.NewRegistry()
	stub := &stubPlugin{serviceID: serviceID, response: resp}
	reg.Register(serviceID, func() plugin.ServicePlugin {
		return stub
	})
	_, _ = reg.Init(serviceID, plugin.PluginConfig{})
	return reg
}

func TestServiceRouter_RoutesToCorrectPlugin(t *testing.T) {
	resp := &plugin.Response{
		StatusCode:  http.StatusOK,
		Headers:     map[string]string{"X-Custom": "yes"},
		Body:        []byte("<ListBucketResult/>"),
		ContentType: "application/xml",
	}
	reg := newRegistryWithStub("s3", resp)
	router := NewServiceRouter(reg, nil)

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "application/xml", w.Header().Get("Content-Type"))
	assert.Equal(t, "yes", w.Header().Get("X-Custom"))
	assert.Equal(t, "<ListBucketResult/>", w.Body.String())
}

func TestServiceRouter_UnknownService(t *testing.T) {
	// Empty registry — no plugins registered.
	reg := plugin.NewRegistry()
	router := NewServiceRouter(reg, nil)

	// Send a JSON-protocol request targeting an unknown service.
	req := httptest.NewRequest(http.MethodPost, "/", nil)
	req.Header.Set("X-Amz-Target", "UnknownService_20240101.DoSomething")
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
	assert.Contains(t, w.Body.String(), "UnknownService")
}

// unroutedJSONRequest builds a JSON-protocol call naming service, which is how
// a boto3 client reaches a service DevCloud may or may not register.
func unroutedJSONRequest(service string) *http.Request {
	req := httptest.NewRequest(http.MethodPost, "/", nil)
	req.Header.Set("X-Amz-Target", service+"_20240101.DoSomething")
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	return req
}

// TestServiceRouter_RecordsUnroutedService is the demand instrument: a call to
// a service that is not registered has to land somewhere countable, or the
// coverage roadmap has no evidence to read.
func TestServiceRouter_RecordsUnroutedService(t *testing.T) {
	uc := admin.NewUnroutedCollector(10)
	router := NewServiceRouter(plugin.NewRegistry(), uc)

	router.ServeHTTP(httptest.NewRecorder(), unroutedJSONRequest("amp"))
	router.ServeHTTP(httptest.NewRecorder(), unroutedJSONRequest("amp"))
	router.ServeHTTP(httptest.NewRecorder(), unroutedJSONRequest("appflow"))

	report := uc.Snapshot()
	require.Len(t, report.Services, 2)
	assert.Equal(t, "amp", report.Services[0].ServiceID)
	assert.Equal(t, 2, report.Services[0].Count)
}

// TestServiceRouter_DoesNotRecordRoutedService is the other half. Counting a
// served call would make every DevCloud look like it is missing everything it
// actually has.
func TestServiceRouter_DoesNotRecordRoutedService(t *testing.T) {
	uc := admin.NewUnroutedCollector(10)
	reg := newRegistryWithStub("s3", &plugin.Response{StatusCode: http.StatusOK, Body: []byte("<ok/>")})
	router := NewServiceRouter(reg, uc)

	w := httptest.NewRecorder()
	router.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/", nil))

	require.Equal(t, http.StatusOK, w.Code)
	assert.Empty(t, uc.Snapshot().Services)
}

// TestServiceRouter_NilUnroutedCollector is the admin-disabled configuration.
// The miss branch calls Add unconditionally, so nil must be safe there.
func TestServiceRouter_NilUnroutedCollector(t *testing.T) {
	router := NewServiceRouter(plugin.NewRegistry(), nil)

	w := httptest.NewRecorder()
	assert.NotPanics(t, func() { router.ServeHTTP(w, unroutedJSONRequest("amp")) })
	assert.Equal(t, http.StatusBadRequest, w.Code)
}
