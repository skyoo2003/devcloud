// SPDX-License-Identifier: Apache-2.0

package gateway

import (
	"bytes"
	"encoding/json"
	"encoding/xml"
	"errors"
	"io"
	"mime"
	"net/http"
	"strings"

	"github.com/skyoo2003/devcloud/internal/admin"
	"github.com/skyoo2003/devcloud/internal/plugin"
	"github.com/skyoo2003/devcloud/internal/shared/crud"
)

// ServiceRouter dispatches incoming HTTP requests to the appropriate
// ServicePlugin based on the detected AWS protocol and service ID.
type ServiceRouter struct {
	registry *plugin.Registry

	// unrouted counts the misses below. It is nil when the admin API is off,
	// and UnroutedCollector.Add tolerates that, so the miss branch stays one
	// unconditional call.
	//
	// It undercounts by construction: DetectProtocol falls through to
	// ("rest-xml", "s3") for a request it cannot classify, and s3 is
	// registered, so those never reach the branch that records. A call naming a
	// real but unregistered AWS service does reach it, which is the case this
	// exists to measure.
	unrouted *admin.UnroutedCollector
}

// NewServiceRouter creates a ServiceRouter backed by the given plugin Registry.
// unrouted may be nil; see the field comment.
func NewServiceRouter(registry *plugin.Registry, unrouted *admin.UnroutedCollector) *ServiceRouter {
	return &ServiceRouter{registry: registry, unrouted: unrouted}
}

// ServeHTTP implements http.Handler.
func (sr *ServiceRouter) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	protocol, serviceID := DetectProtocol(r)

	p, ok := sr.registry.Get(serviceID)
	if !ok {
		sr.unrouted.Add(serviceID)
		writeAWSError(w, protocol, http.StatusBadRequest, "UnknownService",
			"The requested service is not available: "+serviceID)
		return
	}

	op := extractOperationName(r, protocol)

	// Buffer the body for the protocols whose parameters live in it, so the
	// engine can re-read it if the provider does not handle the operation. Cheap
	// for JSON, rest-json and query payloads.
	//
	// This asks crud.NeedsBody, not crud.Servable, and the difference is
	// rest-xml. The engine serves rest-xml — it classifies the operation from
	// the method and path like rest-json — but it must do so without the body:
	// S3 speaks rest-xml, its bodies are large binary uploads, and every
	// CRUD-shaped S3 Control operation addresses its resource with a path label
	// or a query term anyway. Asking the servable question here would buffer
	// every PutObject to serve a provider that never reaches the engine.
	var body []byte
	if crud.NeedsBody(protocol) {
		var rerr error
		if body, rerr = io.ReadAll(r.Body); rerr != nil {
			writeAWSError(w, protocol, http.StatusBadRequest, "SerializationException", "failed to read request body")
			return
		}
		r.Body = io.NopCloser(bytes.NewReader(body))
	}

	resp, err := p.HandleRequest(r.Context(), op, r)

	// A provider that returns ErrUnhandledOp is opting into the generic CRUD
	// fallback for operations it does not implement. If the engine cannot
	// classify the operation either, emit the standard "unknown action" error.
	if errors.Is(err, plugin.ErrUnhandledOp) {
		res, cerr := crud.Handle(crud.Call{
			Service:  serviceID,
			Protocol: protocol,
			Op:       op,
			Method:   r.Method,
			URI:      r.URL.RequestURI(),
			Body:     body,
		})
		if cerr != nil {
			writeAWSError(w, protocol, http.StatusBadRequest, "InvalidAction", "unknown action: "+op)
			return
		}
		resp, err = &plugin.Response{StatusCode: res.Status, Body: res.Body, ContentType: res.ContentType}, nil
	}

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

// awsXMLError is the REST-XML error envelope, which S3 returns bare.
type awsXMLError struct {
	XMLName xml.Name `xml:"Error"`
	Code    string   `xml:"Code"`
	Message string   `xml:"Message"`
}

// awsQueryError is the Query-protocol error envelope. It is not the same shape
// as REST-XML's: botocore's query parser looks for Error *inside* ErrorResponse,
// and given a bare <Error> it returns a ClientError with no error code at all —
// so the caller sees a failure with nothing to branch on. Only a registered
// Query service that serves nothing takes this path, which is why it stayed
// hidden until elb was registered.
type awsQueryError struct {
	XMLName xml.Name `xml:"ErrorResponse"`
	Type    string   `xml:"Error>Type"`
	Code    string   `xml:"Error>Code"`
	Message string   `xml:"Error>Message"`
}

// awsJSONError is the envelope for a JSON-format AWS error response.
type awsJSONError struct {
	Code    string `json:"__type"`
	Message string `json:"message"`
}

// writeAWSError writes an AWS-style error response.
// JSON-bodied protocols receive a JSON body; everything else receives XML.
//
// "rest-json" does not start with "json", so it took the XML branch and a
// rest-json client was handed an XML envelope it cannot parse — botocore
// surfaces that as a ClientError with no error Code at all, which is worse than
// the failure it is reporting. It went unnoticed while no rest-json service
// reached the engine.
func writeAWSError(w http.ResponseWriter, protocol string, status int, code, message string) {
	if strings.HasPrefix(protocol, "json") || protocol == "rest-json" {
		body, _ := json.Marshal(awsJSONError{Code: code, Message: message})
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = w.Write(body)
		return
	}

	var body []byte
	if protocol == "query" {
		body, _ = xml.Marshal(awsQueryError{Type: "Sender", Code: code, Message: message})
	} else {
		body, _ = xml.Marshal(awsXMLError{Code: code, Message: message})
	}
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(status)
	_, _ = w.Write([]byte(xml.Header))
	_, _ = w.Write(body)
}
