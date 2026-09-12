// SPDX-License-Identifier: Apache-2.0

// internal/services/cloudsearchdomain/provider_test.go
package cloudsearchdomain

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

// The three routes are distinguished partly by their query string, so the test
// URIs carry the same terms a real SDK sends.
const (
	searchURI  = "/2013-01-01/search?format=sdk&pretty=true"
	suggestURI = "/2013-01-01/suggest?format=sdk&pretty=true"
	uploadURI  = "/2013-01-01/documents/batch?format=sdk"
)

func newTestProvider(t *testing.T) *Provider {
	t.Helper()
	p := &Provider{}
	require.NoError(t, p.Init(plugin.PluginConfig{DataDir: t.TempDir()}))
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

func hits(t *testing.T, resp *plugin.Response) map[string]any {
	t.Helper()
	h, ok := parseBody(t, resp)["hits"].(map[string]any)
	require.True(t, ok)
	return h
}

func TestUploadThenSearch(t *testing.T) {
	p := newTestProvider(t)

	resp := callREST(t, p, "POST", uploadURI, "",
		`[{"type":"add","id":"doc1","fields":{"title":"alpha release"}}]`)
	assert.Equal(t, 200, resp.StatusCode)
	rb := parseBody(t, resp)
	assert.Equal(t, "success", rb["status"])
	assert.Equal(t, float64(1), rb["adds"])
	assert.Equal(t, float64(0), rb["deletes"])
	assert.Equal(t, []any{}, rb["warnings"])

	found := callREST(t, p, "GET", searchURI+"&q=alpha", "", "")
	assert.Equal(t, 200, found.StatusCode)
	h := hits(t, found)
	// found is a number, not a string: botocore types it as a long.
	assert.Equal(t, float64(1), h["found"])
	hit := h["hit"].([]any)[0].(map[string]any)
	assert.Equal(t, "doc1", hit["id"])
	assert.Equal(t, "alpha release", hit["fields"].(map[string]any)["title"])
	assert.Contains(t, parseBody(t, found), "facets")
	assert.Contains(t, parseBody(t, found), "stats")
}

func TestSearchWithNoMatch(t *testing.T) {
	p := newTestProvider(t)

	callREST(t, p, "POST", uploadURI, "", `[{"type":"add","id":"doc1","fields":{"title":"alpha"}}]`)

	resp := callREST(t, p, "GET", searchURI+"&q=nothinglikethis", "", "")
	assert.Equal(t, 200, resp.StatusCode)
	assert.Equal(t, float64(0), hits(t, resp)["found"])
}

// A search with no q at all is the smoke suite's shape, and must still answer.
func TestSearchWithoutQueryTerm(t *testing.T) {
	p := newTestProvider(t)

	callREST(t, p, "POST", uploadURI, "", `[{"type":"add","id":"doc1","fields":{"title":"alpha"}}]`)

	resp := callREST(t, p, "GET", searchURI, "", "")
	assert.Equal(t, 200, resp.StatusCode)
	assert.Equal(t, float64(1), hits(t, resp)["found"])
}

func TestUploadDeleteRemovesTheDocument(t *testing.T) {
	p := newTestProvider(t)

	callREST(t, p, "POST", uploadURI, "", `[{"type":"add","id":"doc1","fields":{"title":"alpha"}}]`)

	resp := callREST(t, p, "POST", uploadURI, "", `[{"type":"delete","id":"doc1"}]`)
	assert.Equal(t, 200, resp.StatusCode)
	assert.Equal(t, float64(1), parseBody(t, resp)["deletes"])

	assert.Equal(t, float64(0), hits(t, callREST(t, p, "GET", searchURI+"&q=alpha", "", ""))["found"])
}

func TestUploadInvalidBatch(t *testing.T) {
	p := newTestProvider(t)

	resp := callREST(t, p, "POST", uploadURI, "", `{"type":"add"}`)
	assert.Equal(t, 400, resp.StatusCode)
	assert.Equal(t, "DocumentServiceException", parseBody(t, resp)["__type"])
}

func TestSuggest(t *testing.T) {
	p := newTestProvider(t)

	callREST(t, p, "POST", uploadURI, "", `[{"type":"add","id":"alpha1","fields":{"title":"alpha"}}]`)

	resp := callREST(t, p, "GET", suggestURI+"&q=alpha", "", "")
	assert.Equal(t, 200, resp.StatusCode)

	s, ok := parseBody(t, resp)["suggest"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "alpha", s["query"])
	assert.Equal(t, float64(1), s["found"])
	assert.Equal(t, "alpha1", s["suggestions"].([]any)[0].(map[string]any)["id"])
}

// TestSearchOverPostForm covers what boto3 actually sends. botocore carries a
// customization for this client that converts Search from the modelled
// GET /2013-01-01/search?format=sdk&pretty=true into a POST with the same terms
// in an x-www-form-urlencoded body. A provider that knows only the modelled
// route answers nothing a real client can reach.
func TestSearchOverPostForm(t *testing.T) {
	p := newTestProvider(t)
	callREST(t, p, "POST", uploadURI, "", `[{"type":"add","id":"doc1","fields":{"title":"alpha"}}]`)

	req := httptest.NewRequest("POST", "/2013-01-01/search",
		strings.NewReader("format=sdk&pretty=true&q=alpha"))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := p.HandleRequest(context.Background(), "", req)
	require.NoError(t, err)
	require.Equal(t, 200, resp.StatusCode)

	// q came from the form body, not the query string.
	assert.Equal(t, float64(1), hits(t, resp)["found"])
}

// TestSearchOverPostFormWithNoMatch proves q is really read from the body: a
// term that matches nothing must find nothing, rather than falling through to
// the empty-q "match everything" rule.
func TestSearchOverPostFormWithNoMatch(t *testing.T) {
	p := newTestProvider(t)
	callREST(t, p, "POST", uploadURI, "", `[{"type":"add","id":"doc1","fields":{"title":"alpha"}}]`)

	req := httptest.NewRequest("POST", "/2013-01-01/search",
		strings.NewReader("format=sdk&pretty=true&q=nothinglikethis"))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := p.HandleRequest(context.Background(), "", req)
	require.NoError(t, err)
	require.Equal(t, 200, resp.StatusCode)
	assert.Equal(t, float64(0), hits(t, resp)["found"])
}

// TestProviderIdentity pins what the registry and the fidelity manifest read.
// A typo in ServiceName is invisible everywhere else.
func TestProviderIdentity(t *testing.T) {
	p := newTestProvider(t)
	assert.Equal(t, "cloudsearchdomain", p.ServiceID())
	assert.Equal(t, "AmazonCloudSearch2013", p.ServiceName())
	assert.Equal(t, plugin.ProtocolRESTJSON, p.Protocol())

	resources, err := p.ListResources(context.Background())
	require.NoError(t, err)
	assert.Empty(t, resources)
}

func TestUnknownOperationIsUnhandled(t *testing.T) {
	p := newTestProvider(t)
	req := httptest.NewRequest("GET", "/nope", strings.NewReader(""))
	_, err := p.HandleRequest(context.Background(), "NoSuchThing", req)
	assert.ErrorIs(t, err, plugin.ErrUnhandledOp)
}
