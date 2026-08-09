// SPDX-License-Identifier: Apache-2.0

package admin

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/skyoo2003/devcloud/internal/plugin"
)

// mockServicePlugin is a minimal ServicePlugin used by API tests.
type mockServicePlugin struct {
	id        string
	name      string
	resources []plugin.Resource
}

func (m *mockServicePlugin) ServiceID() string   { return m.id }
func (m *mockServicePlugin) ServiceName() string { return m.name }
func (m *mockServicePlugin) Protocol() plugin.ProtocolType {
	return plugin.ProtocolRESTXML
}
func (m *mockServicePlugin) Init(cfg plugin.PluginConfig) error { return nil }
func (m *mockServicePlugin) Shutdown(ctx context.Context) error { return nil }
func (m *mockServicePlugin) HandleRequest(ctx context.Context, op string, req *http.Request) (*plugin.Response, error) {
	return &plugin.Response{StatusCode: 200}, nil
}
func (m *mockServicePlugin) ListResources(ctx context.Context) ([]plugin.Resource, error) {
	return m.resources, nil
}

// newTestRegistry creates a Registry with a single mock plugin already active.
func newTestRegistry(p *mockServicePlugin) *plugin.Registry {
	reg := plugin.NewRegistry()
	captured := p
	reg.Register(p.id, func() plugin.ServicePlugin {
		return captured
	})
	_, err := reg.Init(p.id, plugin.PluginConfig{})
	_ = err
	return reg
}

// TestAPI_Services registers a mock plugin and verifies the
// /devcloud/api/services endpoint returns it.
func TestAPI_Services(t *testing.T) {
	p := &mockServicePlugin{
		id:   "s3",
		name: "Amazon S3",
		resources: []plugin.Resource{
			{Type: "bucket", ID: "my-bucket", Name: "my-bucket"},
			{Type: "bucket", ID: "other-bucket", Name: "other-bucket"},
		},
	}
	reg := newTestRegistry(p)
	lc := NewLogCollector(10)
	api := NewAPI(reg, lc)

	req := httptest.NewRequest(http.MethodGet, "/devcloud/api/services", nil)
	w := httptest.NewRecorder()
	api.Handler().ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "application/json", w.Header().Get("Content-Type"))

	var result []serviceInfo
	require.NoError(t, json.NewDecoder(w.Body).Decode(&result))
	require.Len(t, result, 1)

	svc := result[0]
	assert.Equal(t, "s3", svc.ID)
	assert.Equal(t, "Amazon S3", svc.Name)
	assert.Equal(t, "active", svc.Status)
	assert.Equal(t, 2, svc.ResourceCount)
}

// TestAPI_Fidelity covers both shapes of the endpoint: the unfiltered summary
// carries counts but no operation list, and naming a service adds its tiers.
func TestAPI_Fidelity(t *testing.T) {
	api := NewAPI(newTestRegistry(&mockServicePlugin{id: "s3", name: "Amazon S3"}), NewLogCollector(10))

	req := httptest.NewRequest(http.MethodGet, "/devcloud/api/fidelity", nil)
	w := httptest.NewRecorder()
	api.Handler().ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "application/json", w.Header().Get("Content-Type"))

	var summary map[string]fidelityService
	require.NoError(t, json.NewDecoder(w.Body).Decode(&summary))
	require.Contains(t, summary, "s3")
	assert.True(t, summary["s3"].ModelBacked)
	assert.Greater(t, summary["s3"].Counts["hand-verified"], 0)
	assert.Empty(t, summary["s3"].Operations, "the summary must not carry every operation")

	req = httptest.NewRequest(http.MethodGet, "/devcloud/api/fidelity?service=s3", nil)
	w = httptest.NewRecorder()
	api.Handler().ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	var detail map[string]fidelityService
	require.NoError(t, json.NewDecoder(w.Body).Decode(&detail))
	assert.Equal(t, "hand-verified", detail["s3"].Operations["PutObject"])
}

func TestAPI_FidelityUnknownService(t *testing.T) {
	api := NewAPI(newTestRegistry(&mockServicePlugin{id: "s3", name: "Amazon S3"}), NewLogCollector(10))

	req := httptest.NewRequest(http.MethodGet, "/devcloud/api/fidelity?service=nosuchservice", nil)
	w := httptest.NewRecorder()
	api.Handler().ServeHTTP(w, req)

	assert.Equal(t, http.StatusNotFound, w.Code)
}

// TestAPI_Logs adds log entries to the collector and verifies the
// /devcloud/api/logs endpoint returns them newest-first.
func TestAPI_Logs(t *testing.T) {
	reg := plugin.NewRegistry()
	lc := NewLogCollector(50)

	lc.Add(RequestLog{
		Method:    "GET",
		Path:      "/s3/first",
		Status:    200,
		Duration:  "1.000ms",
		Timestamp: time.Now(),
		Service:   "s3",
	})
	lc.Add(RequestLog{
		Method:    "POST",
		Path:      "/s3/second",
		Status:    201,
		Duration:  "2.000ms",
		Timestamp: time.Now(),
		Service:   "s3",
	})

	api := NewAPI(reg, lc)

	req := httptest.NewRequest(http.MethodGet, "/devcloud/api/logs", nil)
	w := httptest.NewRecorder()
	api.Handler().ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "application/json", w.Header().Get("Content-Type"))

	var logs []RequestLog
	require.NoError(t, json.NewDecoder(w.Body).Decode(&logs))
	require.Len(t, logs, 2)

	// Newest first
	assert.Equal(t, "/s3/second", logs[0].Path)
	assert.Equal(t, "/s3/first", logs[1].Path)
}

// TestAPI_LogsLimit verifies the ?limit= query parameter.
func TestAPI_LogsLimit(t *testing.T) {
	reg := plugin.NewRegistry()
	lc := NewLogCollector(50)

	for i := 0; i < 10; i++ {
		lc.Add(RequestLog{Method: "GET", Path: "/item", Status: 200, Service: "s3"})
	}

	api := NewAPI(reg, lc)

	req := httptest.NewRequest(http.MethodGet, "/devcloud/api/logs?limit=3", nil)
	w := httptest.NewRecorder()
	api.Handler().ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var logs []RequestLog
	require.NoError(t, json.NewDecoder(w.Body).Decode(&logs))
	assert.Len(t, logs, 3)
}
