// SPDX-License-Identifier: Apache-2.0

package gateway

import (
	"encoding/json"
	"encoding/xml"
	"mime"
	"net/http"
	"strings"

	"github.com/skyoo2003/devcloud/internal/plugin"
)

// ServiceRouter dispatches incoming HTTP requests to the appropriate
// ServicePlugin based on the detected AWS protocol and service ID.
type ServiceRouter struct {
	registry *plugin.Registry
}

// NewServiceRouter creates a ServiceRouter backed by the given plugin Registry.
func NewServiceRouter(registry *plugin.Registry) *ServiceRouter {
	return &ServiceRouter{registry: registry}
}

// ServeHTTP implements http.Handler.
func (sr *ServiceRouter) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	protocol, serviceID := DetectProtocol(r)

	p, ok := sr.registry.Get(serviceID)
	if !ok {
		writeAWSError(w, protocol, http.StatusBadRequest, "UnknownService",
			"The requested service is not available: "+serviceID)
		return
	}

	op := extractOperationName(r, protocol)

	resp, err := p.HandleRequest(r.Context(), op, r)
	if err != nil {
		writeAWSError(w, protocol, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}

	// Write response headers from the plugin.
	for k, v := range resp.Headers {
		w.Header().Set(k, v)
	}
	ct := resp.ContentType
	if ct == "" {
		ct = w.Header().Get("Content-Type")
	}
	if ct == "" {
		ct = "application/octet-stream"
	}
	// Prevent XSS: this gateway serves AWS API responses only (JSON/XML),
	// never user-facing HTML. Sanitize any attempt to serve HTML-like content.
	ct = strings.TrimSpace(ct)
	htmlLike := false
	for _, p := range strings.Split(ct, ",") {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		mediaType, _, parseErr := mime.ParseMediaType(p)
		if parseErr != nil {
			continue
		}
		mtLower := strings.ToLower(mediaType)
		if mtLower == "text/html" || mtLower == "application/xhtml+xml" || strings.HasSuffix(mtLower, "+html") {
			htmlLike = true
			break
		}
	}
	if htmlLike {
		ct = "text/plain; charset=utf-8"
	}
	w.Header().Set("Content-Type", ct)
	w.WriteHeader(resp.StatusCode)
	_, _ = w.Write(resp.Body)
}

// extractOperationName derives the AWS operation name from the request.
//   - JSON protocols: X-Amz-Target header suffix after the '.'
//   - Query protocol: Action query/body parameter
//   - REST protocols: empty string (operation is implicit in URL + method)
func extractOperationName(r *http.Request, protocol string) string {
	switch {
	case strings.HasPrefix(protocol, "json"):
		target := r.Header.Get("X-Amz-Target")
		if idx := strings.LastIndex(target, "."); idx != -1 {
			return target[idx+1:]
		}
		return target
	case protocol == "query":
		// Only check URL query params — do NOT call r.FormValue() which
		// consumes the request body. The service provider will parse the
		// form body itself and extract the Action.
		return r.URL.Query().Get("Action")
	default:
		return ""
	}
}

// awsXMLError is the envelope for an XML-format AWS error response.
type awsXMLError struct {
	XMLName xml.Name `xml:"Error"`
	Code    string   `xml:"Code"`
	Message string   `xml:"Message"`
}

// awsJSONError is the envelope for a JSON-format AWS error response.
type awsJSONError struct {
	Code    string `json:"__type"`
	Message string `json:"message"`
}

// writeAWSError writes an AWS-style error response.
// JSON protocols receive a JSON body; everything else receives XML.
func writeAWSError(w http.ResponseWriter, protocol string, status int, code, message string) {
	if strings.HasPrefix(protocol, "json") {
		body, _ := json.Marshal(awsJSONError{Code: code, Message: message})
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = w.Write(body)
		return
	}

	body, _ := xml.Marshal(awsXMLError{Code: code, Message: message})
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(status)
	_, _ = w.Write([]byte(xml.Header))
	_, _ = w.Write(body)
}
