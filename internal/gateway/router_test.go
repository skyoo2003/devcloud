// SPDX-License-Identifier: Apache-2.0

package gateway

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/skyoo2003/devcloud/internal/admin"
	"github.com/skyoo2003/devcloud/internal/plugin"
	"github.com/skyoo2003/devcloud/internal/shared/crud"
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

// --- rest-json reaching the CRUD engine ---

// decliningPlugin implements nothing and says so, which is what every
// scaffolded service does and what hands the request to the engine.
type decliningPlugin struct {
	serviceID string
	protocol  plugin.ProtocolType
}

func (d *decliningPlugin) ServiceID() string                { return d.serviceID }
func (d *decliningPlugin) ServiceName() string              { return d.serviceID }
func (d *decliningPlugin) Protocol() plugin.ProtocolType    { return d.protocol }
func (d *decliningPlugin) Init(_ plugin.PluginConfig) error { return nil }
func (d *decliningPlugin) Shutdown(_ context.Context) error { return nil }
func (d *decliningPlugin) HandleRequest(_ context.Context, _ string, _ *http.Request) (*plugin.Response, error) {
	return nil, plugin.ErrUnhandledOp
}
func (d *decliningPlugin) ListResources(_ context.Context) ([]plugin.Resource, error) {
	return nil, nil
}

func newDecliningRegistry(serviceID string, protocol plugin.ProtocolType) *plugin.Registry {
	reg := plugin.NewRegistry()
	p := &decliningPlugin{serviceID: serviceID, protocol: protocol}
	reg.Register(serviceID, func() plugin.ServicePlugin { return p })
	_, _ = reg.Init(serviceID, plugin.PluginConfig{})
	return reg
}

// restJSONRequest builds a SigV4-signed REST call, which is how DetectProtocol
// recognises a rest-json service (step 3, protocol.go).
func restJSONRequest(service, method, uri, body string) *http.Request {
	var req *http.Request
	if body == "" {
		req = httptest.NewRequest(method, uri, nil)
	} else {
		req = httptest.NewRequest(method, uri, strings.NewReader(body))
	}
	req.Header.Set("Authorization",
		"AWS4-HMAC-SHA256 Credential=AKIA/20130524/us-east-1/"+service+"/aws4_request, Signature=abc")
	return req
}

// TestServiceRouter_RESTJSONBodyReachesEngine is the gateway half of the
// rest-json change. The engine can classify the operation from the path, but a
// Create takes its parameters from the body — and the body was only buffered
// for the JSON protocols, so it arrived empty and every created resource came
// back with a generated id instead of the caller's name.
func TestServiceRouter_RESTJSONBodyReachesEngine(t *testing.T) {
	const svc = "gwrestsvc"
	crud.Register(svc, map[string]crud.OpMeta{
		"CreateGadget": {Verb: "Create", Resource: "Gadget", OutputItemKey: "Gadget",
			Method: "POST", URI: "/v1/gadgets"},
	})
	router := NewServiceRouter(newDecliningRegistry(svc, plugin.ProtocolRESTJSON), nil)

	w := httptest.NewRecorder()
	router.ServeHTTP(w, restJSONRequest(svc, http.MethodPost, "/v1/gadgets", `{"GadgetName":"g1","Colour":"red"}`))

	require.Equal(t, http.StatusOK, w.Code, "body: %s", w.Body.String())
	var out map[string]any
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &out))
	gadget, ok := out["Gadget"].(map[string]any)
	require.True(t, ok, "no Gadget in %v", out)
	assert.Equal(t, "g1", gadget["GadgetName"], "request body did not reach the engine")
	assert.Equal(t, "red", gadget["Colour"])
}

// TestServiceRouter_RESTJSONUnmatchedPathDeclinesCleanly is the guarantee the
// coverage claim rests on: a registered service that cannot serve a request
// must answer with an AWS error, never a fabricated success.
func TestServiceRouter_RESTJSONUnmatchedPathDeclinesCleanly(t *testing.T) {
	const svc = "gwmisssvc"
	crud.Register(svc, map[string]crud.OpMeta{
		"ListGadgets": {Verb: "List", Resource: "MGadget", OutputListKey: "Gadgets",
			Method: "GET", URI: "/v1/gadgets"},
	})
	router := NewServiceRouter(newDecliningRegistry(svc, plugin.ProtocolRESTJSON), nil)

	w := httptest.NewRecorder()
	router.ServeHTTP(w, restJSONRequest(svc, http.MethodPost, "/v1/nothing/here", `{}`))

	assert.Equal(t, http.StatusBadRequest, w.Code)
	assert.Contains(t, w.Body.String(), "InvalidAction")
}

// TestServiceRouter_QueryReachesEngine is the gateway half of the query change.
// It is worth its own test because query is the only protocol where the gateway
// and the engine could disagree about which operation a request is for: the
// gateway reads Action from the URL, the engine reads it from the form body,
// and every real SDK puts it in the body.
func TestServiceRouter_QueryReachesEngine(t *testing.T) {
	const svc = "gwquerysvc"
	crud.Register(svc, map[string]crud.OpMeta{
		"CreateGizmo": {Verb: "Create", Resource: "GwGizmo", OutputItemKey: "Gizmo"},
	})
	router := NewServiceRouter(newDecliningRegistry(svc, plugin.ProtocolQuery), nil)

	form := "Action=CreateGizmo&Version=2012-06-01&GwGizmoName=g1"
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(form))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Authorization",
		"AWS4-HMAC-SHA256 Credential=AKIA/20130524/us-east-1/"+svc+"/aws4_request, Signature=abc")

	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code, "body: %s", w.Body.String())
	body := w.Body.String()
	assert.Contains(t, body, "<CreateGizmoResponse>", "not the query envelope")
	assert.Contains(t, body, "<CreateGizmoResult>")
	assert.Contains(t, body, "<GwGizmoName>g1</GwGizmoName>",
		"the form body did not reach the engine")
	assert.NotContains(t, body, "<Action>", "protocol field leaked into the resource")
}

// TestServiceRouter_RESTXMLReachesEngineWithoutBuffering is the gateway half of
// the rest-xml change, and it asserts two things that have to hold together.
//
// The engine must serve the request — it can, because every rest-xml operation
// is bound to a method and URI template — and the gateway must not have read
// the body to let it. Those pull in opposite directions under one predicate,
// which is why Servable and NeedsBody are now separate: S3 speaks rest-xml, and
// buffering a multi-gigabyte PutObject to serve a provider that never reaches
// the engine would be a straight regression.
func TestServiceRouter_RESTXMLReachesEngineWithoutBuffering(t *testing.T) {
	const svc = "s3control"
	crud.Register(svc, map[string]crud.OpMeta{
		"ListAccessPoints": {Verb: "List", Resource: "GwAccessPoint", OutputListKey: "AccessPointList",
			Method: "GET", URI: "/v20180820/accesspoint"},
	})
	router := NewServiceRouter(newDecliningRegistry(svc, plugin.ProtocolRESTXML), nil)

	req := httptest.NewRequest(http.MethodGet, "/v20180820/accesspoint", &failOnReadBody{t: t})
	req.Header.Set("Authorization",
		"AWS4-HMAC-SHA256 Credential=AKIA/20130524/us-east-1/s3/aws4_request, Signature=abc")

	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code, "body: %s", w.Body.String())
	assert.Contains(t, w.Body.String(), "<ListAccessPointsResult>",
		"rest-xml response is not the engine's XML")
	assert.Equal(t, "application/xml", w.Header().Get("Content-Type"))
}

// failOnReadBody fails the test if anything reads it. It is how "the gateway did
// not buffer this" is made observable: a length check on a captured body cannot
// tell "never read" from "read and restored".
type failOnReadBody struct{ t *testing.T }

func (f *failOnReadBody) Read([]byte) (int, error) {
	f.t.Error("the gateway read a rest-xml request body; S3 uploads must keep streaming")
	return 0, io.EOF
}

// TestServiceRouter_RESTXMLBodyStillUnbuffered pins the exception for the
// service the exception exists for. S3's provider never returns ErrUnhandledOp,
// so it never reaches the engine, and its bodies must keep streaming.
func TestServiceRouter_RESTXMLBodyStillUnbuffered(t *testing.T) {
	var seen []byte
	reg := plugin.NewRegistry()
	p := &bodyCapturingPlugin{serviceID: "s3", seen: &seen}
	reg.Register("s3", func() plugin.ServicePlugin { return p })
	_, _ = reg.Init("s3", plugin.PluginConfig{})
	router := NewServiceRouter(reg, nil)

	req := httptest.NewRequest(http.MethodPut, "/bucket/key", strings.NewReader("binary-payload"))
	router.ServeHTTP(httptest.NewRecorder(), req)

	// The provider still reads the stream itself; the gateway did not consume it.
	assert.Equal(t, "binary-payload", string(seen))
}

// TestWriteAWSErrorEnvelopePerProtocol pins the three error shapes a client can
// actually parse. Getting one wrong turns a clean, actionable refusal into a
// failure with no error code, which is the worst of both outcomes: the call did
// not work and the caller cannot tell why.
func TestWriteAWSErrorEnvelopePerProtocol(t *testing.T) {
	tests := []struct {
		protocol string
		contains string
		why      string
	}{
		{"json-1.1", `"__type":"InvalidAction"`, "X-Amz-Target JSON"},
		{"json-1.0", `"__type":"InvalidAction"`, "X-Amz-Target JSON"},
		{"rest-json", `"__type":"InvalidAction"`, "rest-json bodies are JSON, and the name does not start with json"},
		{"query", "<ErrorResponse><Error>", "botocore's query parser looks for Error inside ErrorResponse"},
		{"rest-xml", "<Error><Code>InvalidAction</Code>", "S3 returns a bare Error element"},
	}
	for _, tc := range tests {
		t.Run(tc.protocol, func(t *testing.T) {
			w := httptest.NewRecorder()
			writeAWSError(w, tc.protocol, http.StatusBadRequest, "InvalidAction", "unknown action")
			assert.Contains(t, w.Body.String(), tc.contains, tc.why)
		})
	}
}

type bodyCapturingPlugin struct {
	serviceID string
	seen      *[]byte
}

func (b *bodyCapturingPlugin) ServiceID() string                { return b.serviceID }
func (b *bodyCapturingPlugin) ServiceName() string              { return b.serviceID }
func (b *bodyCapturingPlugin) Protocol() plugin.ProtocolType    { return plugin.ProtocolRESTXML }
func (b *bodyCapturingPlugin) Init(_ plugin.PluginConfig) error { return nil }
func (b *bodyCapturingPlugin) Shutdown(_ context.Context) error { return nil }
func (b *bodyCapturingPlugin) HandleRequest(_ context.Context, _ string, req *http.Request) (*plugin.Response, error) {
	*b.seen, _ = io.ReadAll(req.Body)
	return &plugin.Response{StatusCode: http.StatusOK, Body: []byte("<ok/>")}, nil
}
func (b *bodyCapturingPlugin) ListResources(_ context.Context) ([]plugin.Resource, error) {
	return nil, nil
}
