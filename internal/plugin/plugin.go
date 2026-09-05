// SPDX-License-Identifier: Apache-2.0

package plugin

import (
	"context"
	"errors"
	"net/http"
)

// ErrUnhandledOp signals that a provider's HandleRequest did not implement the
// requested operation. The gateway treats it as a request to try the generic
// CRUD fallback engine before returning an "unknown action" error. Providers
// opt in by returning this from their dispatch default case.
var ErrUnhandledOp = errors.New("plugin: operation not handled by provider")

// ProtocolType names a wire protocol. It is an open string type on purpose:
// the constants below are AWS's protocols because AWS is what DevCloud serves,
// and a provider for another CSP declares its own values rather than being
// squeezed into these.
type ProtocolType string

const (
	ProtocolRESTXML  ProtocolType = "rest-xml"
	ProtocolRESTJSON ProtocolType = "rest-json"
	ProtocolJSON10   ProtocolType = "json-1.0"
	ProtocolJSON11   ProtocolType = "json-1.1"
	ProtocolQuery    ProtocolType = "query"

	// DefaultAccountID is the account ID used when no real auth is configured.
	DefaultAccountID = "000000000000"

	// DefaultProvider is the CSP a plugin belongs to when it does not say. Every
	// service that shipped before providers existed is an AWS service, so the
	// default is what keeps them correct without touching them.
	DefaultProvider = "aws"
)

// ProviderScoped is the optional half of ServicePlugin: a plugin implements it
// to say which CSP it serves. It is deliberately not a ServicePlugin method —
// adding one would force an edit to every service in the tree to state the
// value they already default to, and would break the v1.x interface promise in
// docs/plugin-api.md. Read it through ProviderOf, never by asserting directly.
type ProviderScoped interface {
	Provider() string
}

// ProviderOf reports the CSP a plugin serves, defaulting to DefaultProvider.
func ProviderOf(p ServicePlugin) string {
	if ps, ok := p.(ProviderScoped); ok {
		if id := ps.Provider(); id != "" {
			return id
		}
	}
	return DefaultProvider
}

type PluginConfig struct {
	DataDir string
	Options map[string]any
}

type Response struct {
	StatusCode  int
	Headers     map[string]string
	Body        []byte
	ContentType string
}

type Resource struct {
	Type string `json:"type"`
	ID   string `json:"id"`
	Name string `json:"name"`
}

type ServicePlugin interface {
	ServiceID() string
	ServiceName() string
	Protocol() ProtocolType
	Init(config PluginConfig) error
	Shutdown(ctx context.Context) error
	HandleRequest(ctx context.Context, op string, req *http.Request) (*Response, error)
	ListResources(ctx context.Context) ([]Resource, error)
}
