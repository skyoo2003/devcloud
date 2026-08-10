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

// getJSON issues GET path against h, asserts the status and Content-Type the
// compatibility policy guarantees, and decodes the body into v.
func getJSON(t *testing.T, h http.Handler, path string, v any) {
	t.Helper()
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, path, nil))
	require.Equal(t, http.StatusOK, w.Code, path)
	require.Equal(t, "application/json", w.Header().Get("Content-Type"), path)
	require.NoError(t, json.NewDecoder(w.Body).Decode(v), path)
}

// surfaceAPI builds an API with one service, one resource and one log entry —
// enough for every guaranteed route to return a non-empty body.
func surfaceAPI(t *testing.T) http.Handler {
	t.Helper()
	p := &mockServicePlugin{
		id:        "s3",
		name:      "Amazon S3",
		resources: []plugin.Resource{{Type: "bucket", ID: "my-bucket", Name: "my-bucket"}},
	}
	lc := NewLogCollector(10)
	lc.Add(RequestLog{
		Method: "GET", Path: "/s3/my-bucket", Status: 200,
		Duration: "1.000ms", Timestamp: time.Now(), Service: "s3",
	})
	return NewAPI(newTestRegistry(p), lc).Handler()
}

// TestGuaranteedAdminSurface_Collections locks the wire keys of the three
// list-returning routes in docs/compatibility-policy.md.
//
// It decodes into map[string]any deliberately. The other tests in this file
// decode into the internal structs (serviceInfo, RequestLog), so renaming a
// JSON tag renames both sides of the assertion and they stay green while every
// consumer breaks. Asserting key *presence* rather than the whole payload keeps
// additive change — which the policy allows — from failing the build.
func TestGuaranteedAdminSurface_Collections(t *testing.T) {
	h := surfaceAPI(t)

	for _, tc := range []struct {
		route string
		keys  []string
	}{
		{"/devcloud/api/services", []string{"id", "name", "status", "resourceCount"}},
		{"/devcloud/api/services/s3/resources", []string{"type", "id", "name"}},
		{"/devcloud/api/logs", []string{"method", "path", "status", "duration", "timestamp", "service"}},
	} {
		var got []map[string]any
		getJSON(t, h, tc.route, &got)
		require.NotEmpty(t, got, "%s returned no entries to check", tc.route)

		for _, key := range tc.keys {
			if _, ok := got[0][key]; !ok {
				t.Errorf("%s: entry is missing guaranteed key %q — guaranteed by docs/compatibility-policy.md",
					tc.route, key)
			}
		}
	}
}

// TestGuaranteedAdminSurface_Fidelity locks both shapes of the fidelity route:
// the summary carries counts only, and naming a service adds its operations.
func TestGuaranteedAdminSurface_Fidelity(t *testing.T) {
	h := surfaceAPI(t)

	var summary map[string]map[string]any
	getJSON(t, h, "/devcloud/api/fidelity", &summary)
	require.Contains(t, summary, "s3")
	for _, key := range []string{"modelBacked", "counts"} {
		if _, ok := summary["s3"][key]; !ok {
			t.Errorf("/devcloud/api/fidelity: missing guaranteed key %q — guaranteed by docs/compatibility-policy.md", key)
		}
	}
	assert.NotContains(t, summary["s3"], "operations",
		"the unfiltered summary must not carry every operation")

	var detail map[string]map[string]any
	getJSON(t, h, "/devcloud/api/fidelity?service=s3", &detail)
	require.Contains(t, detail, "s3")
	assert.Contains(t, detail["s3"], "operations",
		"?service= must add the per-operation tiers")
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
