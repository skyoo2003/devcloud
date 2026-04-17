// SPDX-License-Identifier: Apache-2.0

package plugin

import (
	"context"
	"net/http"
)

type ProtocolType string

const (
	ProtocolRESTXML  ProtocolType = "rest-xml"
	ProtocolRESTJSON ProtocolType = "rest-json"
	ProtocolJSON10   ProtocolType = "json-1.0"
	ProtocolJSON11   ProtocolType = "json-1.1"
	ProtocolQuery    ProtocolType = "query"

	// DefaultAccountID is the account ID used when no real auth is configured.
	DefaultAccountID = "000000000000"
)

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

type ServiceMetrics struct {
	TotalRequests int64 `json:"total_requests"`
	ErrorCount    int64 `json:"error_count"`
	ResourceCount int   `json:"resource_count"`
}

type ServicePlugin interface {
	ServiceID() string
	ServiceName() string
	Protocol() ProtocolType
	Init(config PluginConfig) error
	Shutdown(ctx context.Context) error
	HandleRequest(ctx context.Context, op string, req *http.Request) (*Response, error)
	ListResources(ctx context.Context) ([]Resource, error)
	GetMetrics(ctx context.Context) (*ServiceMetrics, error)
}
