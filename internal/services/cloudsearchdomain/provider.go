// SPDX-License-Identifier: Apache-2.0

package cloudsearchdomain

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"path/filepath"
	"strings"

	generated "github.com/skyoo2003/devcloud/internal/generated/cloudsearchdomain"
	"github.com/skyoo2003/devcloud/internal/plugin"
	"github.com/skyoo2003/devcloud/internal/shared"
	"github.com/skyoo2003/devcloud/internal/shared/crud"
	"github.com/skyoo2003/devcloud/internal/shared/httproute"
)

const formContentType = "application/x-www-form-urlencoded"

// declaredRoutes is the model's table plus the one route botocore invents.
//
// The SDK does not send Search as the model declares it: botocore carries a
// customization for this client that turns
// GET /2013-01-01/search?format=sdk&pretty=true into a POST carrying those same
// terms in a form body, so a query too long for a URL still goes out. The
// generated table is derived from the Smithy model and so cannot describe that
// request — which is why it is extended here rather than regenerated.
//
// Both the provider's own resolution and the gateway's shared-signing-name split
// read this table, so the two cannot disagree about which requests this service
// claims.
var declaredRoutes = append(
	append([]httproute.Route{}, generated.OperationRoutes...),
	httproute.Route{Method: "POST", Pattern: "/2013-01-01/search", Operation: "Search"},
)

// Provider implements the AmazonCloudSearch2013 service.
type Provider struct {
	generated.BaseProvider
	store *Store
}

func (p *Provider) ServiceID() string             { return "cloudsearchdomain" }
func (p *Provider) ServiceName() string           { return "AmazonCloudSearch2013" }
func (p *Provider) Protocol() plugin.ProtocolType { return plugin.ProtocolRESTJSON }

func (p *Provider) Init(cfg plugin.PluginConfig) error {
	dataDir := cfg.DataDir
	if dataDir == "" {
		dataDir = "."
	}
	var err error
	p.store, err = NewStore(filepath.Join(dataDir, "cloudsearchdomain"))
	return err
}

func (p *Provider) Shutdown(_ context.Context) error {
	if p.store != nil {
		return p.store.Close()
	}
	return nil
}

// batchEntry is one element of the SDK's document batch: an add carries fields,
// a delete carries only the id.
type batchEntry struct {
	Type   string          `json:"type"`
	ID     string          `json:"id"`
	Fields json.RawMessage `json:"fields"`
}

func (p *Provider) HandleRequest(_ context.Context, op string, req *http.Request) (*plugin.Response, error) {
	if op == "" {
		// RequestURI, not Path: the modelled routes are distinguished partly by
		// their query string.
		op, _ = httproute.Match(declaredRoutes, req.Method, req.URL.RequestURI())
	}
	body, err := io.ReadAll(req.Body)
	if err != nil {
		return shared.JSONError("SerializationException", "failed to read body", http.StatusBadRequest), nil
	}
	q := searchTerm(req, body)

	switch op {
	case "UploadDocuments":
		return p.uploadDocuments(body)

	case "Search":
		return p.search(q)

	case "Suggest":
		return p.suggest(q)

	default:
		return nil, plugin.ErrUnhandledOp
	}
}

func (p *Provider) uploadDocuments(body []byte) (*plugin.Response, error) {
	var batch []batchEntry
	if err := json.Unmarshal(body, &batch); err != nil {
		return shared.JSONError("DocumentServiceException", "invalid document batch", http.StatusBadRequest), nil
	}

	adds, deletes := 0, 0
	for _, e := range batch {
		switch e.Type {
		case "add":
			fields := "{}"
			if len(e.Fields) > 0 {
				fields = string(e.Fields)
			}
			if err := p.store.Upsert(e.ID, fields); err != nil {
				return nil, err
			}
			adds++
		case "delete":
			if err := p.store.Delete(e.ID); err != nil {
				return nil, err
			}
			deletes++
		}
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"status":   "success",
		"adds":     adds,
		"deletes":  deletes,
		"warnings": []any{},
	})
}

func (p *Provider) search(q string) (*plugin.Response, error) {
	docs, err := p.matching(q)
	if err != nil {
		return nil, err
	}
	hits := make([]map[string]any, 0, len(docs))
	for _, d := range docs {
		hits = append(hits, map[string]any{
			"id":         d.ID,
			"fields":     decodeFields(d.Fields),
			"exprs":      map[string]any{},
			"highlights": map[string]any{},
		})
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"status": searchStatus(),
		// found is a long in the model; a string here breaks botocore's parse.
		"hits":   map[string]any{"found": len(hits), "start": 0, "cursor": "", "hit": hits},
		"facets": map[string]any{},
		"stats":  map[string]any{},
	})
}

func (p *Provider) suggest(q string) (*plugin.Response, error) {
	docs, err := p.matching(q)
	if err != nil {
		return nil, err
	}
	suggestions := make([]map[string]any, 0, len(docs))
	for _, d := range docs {
		suggestions = append(suggestions, map[string]any{
			"id":         d.ID,
			"score":      0,
			"suggestion": d.ID,
		})
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"status": searchStatus(),
		"suggest": map[string]any{
			"query":       q,
			"found":       len(suggestions),
			"suggestions": suggestions,
		},
	})
}

// matching returns the documents whose stored fields contain q. An empty q
// matches everything, which is what a bare "*" query means to a real domain.
//
// ponytail: substring match over every stored document. An inverted index is
// what a real query parser needs; swap this out if anyone asks for relevance.
func (p *Provider) matching(q string) ([]Document, error) {
	docs, err := p.store.All()
	if err != nil {
		return nil, err
	}
	if q == "" {
		return docs, nil
	}
	out := make([]Document, 0, len(docs))
	for _, d := range docs {
		if strings.Contains(d.Fields, q) || strings.Contains(d.ID, q) {
			out = append(out, d)
		}
	}
	return out, nil
}

// searchTerm reads the query term from wherever this request carries it: the
// query string for the modelled GET, the form body for the POST botocore
// rewrites Search into.
func searchTerm(req *http.Request, body []byte) string {
	if q := req.URL.Query().Get("q"); q != "" {
		return q
	}
	if len(body) == 0 || !strings.HasPrefix(req.Header.Get("Content-Type"), formContentType) {
		return ""
	}
	// ParseQuery returns what it could parse alongside any error, so a malformed
	// tail cannot discard the term before it.
	form, _ := url.ParseQuery(string(body))
	return form.Get("q")
}

func searchStatus() map[string]any {
	return map[string]any{"timems": 0, "rid": shared.GenerateID("", 24)}
}

func decodeFields(raw string) map[string]any {
	var fields map[string]any
	if err := json.Unmarshal([]byte(raw), &fields); err != nil || fields == nil {
		return map[string]any{}
	}
	return fields
}

func (p *Provider) ListResources(_ context.Context) ([]plugin.Resource, error) {
	return []plugin.Resource{}, nil
}

func init() {
	plugin.DefaultRegistry.Register("cloudsearchdomain", func() plugin.ServicePlugin {
		return &Provider{}
	})
	// This service signs as "cloudsearch", which the query-protocol control
	// plane claims and which models none of these paths. Without this the
	// gateway's shared-signing-name split has no candidate and cloudsearch keeps
	// the request. See crud.RegisterRoutes.
	crud.RegisterRoutes("cloudsearchdomain", declaredRoutes)
}
