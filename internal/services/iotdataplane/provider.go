// SPDX-License-Identifier: Apache-2.0

// internal/services/iotdataplane/provider.go
package iotdataplane

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"strings"
	"time"

	"github.com/skyoo2003/devcloud/internal/plugin"
	"github.com/skyoo2003/devcloud/internal/shared"
)

// Provider implements the IotMoonrakerService service.
type Provider struct {
	store *Store
}

func (p *Provider) ServiceID() string             { return "iotdataplane" }
func (p *Provider) ServiceName() string           { return "IotMoonrakerService" }
func (p *Provider) Protocol() plugin.ProtocolType { return plugin.ProtocolRESTJSON }

func (p *Provider) Init(cfg plugin.PluginConfig) error {
	dataDir := cfg.DataDir
	if dataDir == "" {
		dataDir = "."
	}
	var err error
	p.store, err = NewStore(filepath.Join(dataDir, "iotdataplane"))
	return err
}

func (p *Provider) Shutdown(_ context.Context) error {
	if p.store != nil {
		return p.store.Close()
	}
	return nil
}

func (p *Provider) HandleRequest(_ context.Context, op string, req *http.Request) (*plugin.Response, error) {
	if op == "" {
		op = resolveOp(req.Method, req.URL.Path)
	}
	body, err := io.ReadAll(req.Body)
	if err != nil {
		return shared.JSONError("SerializationException", "failed to read body", http.StatusBadRequest), nil
	}

	switch op {
	case "GetThingShadow":
		thingName := extractPathParam(req.URL.Path, "things")
		shadowName := req.URL.Query().Get("name")
		if shadowName == "" {
			shadowName = "classic"
		}
		return p.getThingShadow(thingName, shadowName)

	case "UpdateThingShadow":
		thingName := extractPathParam(req.URL.Path, "things")
		shadowName := req.URL.Query().Get("name")
		if shadowName == "" {
			shadowName = "classic"
		}
		return p.updateThingShadow(thingName, shadowName, body)

	case "DeleteThingShadow":
		thingName := extractPathParam(req.URL.Path, "things")
		shadowName := req.URL.Query().Get("name")
		if shadowName == "" {
			shadowName = "classic"
		}
		return p.deleteThingShadow(thingName, shadowName)

	case "ListNamedShadowsForThing":
		thingName := extractPathParam(req.URL.Path, "ListNamedShadowsForThing")
		return p.listNamedShadowsForThing(thingName)

	case "Publish":
		topic := extractGreedyPathParam(req.URL.Path, "topics")
		var params map[string]any
		if len(body) > 0 {
			if err := json.Unmarshal(body, &params); err != nil {
				return shared.JSONError("SerializationException", "invalid JSON", http.StatusBadRequest), nil
			}
		} else {
			params = map[string]any{}
		}
		// Allow topic override from JSON body for non-retained publishes without path topic
		if topic == "" {
			if t, ok := params["topic"].(string); ok {
				topic = t
			}
		}
		return p.publish(topic, body, params)

	case "GetRetainedMessage":
		topic := extractGreedyPathParam(req.URL.Path, "retainedMessage")
		return p.getRetainedMessage(topic)

	case "ListRetainedMessages":
		return p.listRetainedMessages()

	case "DeleteConnection":
		return shared.JSONResponse(http.StatusOK, map[string]any{})

	default:
		return shared.JSONError("InvalidAction", fmt.Sprintf("unknown action: %s", op), http.StatusBadRequest), nil
	}
}

func (p *Provider) ListResources(_ context.Context) ([]plugin.Resource, error) {
	return []plugin.Resource{}, nil
}

func (p *Provider) GetMetrics(_ context.Context) (*plugin.ServiceMetrics, error) {
	return &plugin.ServiceMetrics{}, nil
}

// --- ThingShadow ---

func (p *Provider) getThingShadow(thingName, shadowName string) (*plugin.Response, error) {
	if thingName == "" {
		return shared.JSONError("InvalidRequestException", "thingName is required", http.StatusBadRequest), nil
	}
	ts, err := p.store.GetThingShadow(thingName, shadowName)
	if err != nil {
		if errors.Is(err, errShadowNotFound) {
			return shared.JSONError("ResourceNotFoundException", "shadow not found", http.StatusNotFound), nil
		}
		return nil, err
	}
	var shadowDoc map[string]any
	if err := json.Unmarshal([]byte(ts.Payload), &shadowDoc); err != nil {
		shadowDoc = map[string]any{}
	}
	shadowDoc["version"] = ts.Version
	shadowDoc["timestamp"] = ts.UpdatedAt.Unix()
	shadowDoc["payload"] = ts.Payload
	return shared.JSONResponse(http.StatusOK, shadowDoc)
}

func (p *Provider) updateThingShadow(thingName, shadowName string, body []byte) (*plugin.Response, error) {
	if thingName == "" {
		return shared.JSONError("InvalidRequestException", "thingName is required", http.StatusBadRequest), nil
	}
	payload := "{}"
	if len(body) > 0 {
		payload = string(body)
	}
	ts, err := p.store.UpsertThingShadow(thingName, shadowName, payload)
	if err != nil {
		return nil, err
	}
	var shadowDoc map[string]any
	if err := json.Unmarshal([]byte(ts.Payload), &shadowDoc); err != nil {
		shadowDoc = map[string]any{}
	}
	shadowDoc["version"] = ts.Version
	shadowDoc["timestamp"] = ts.UpdatedAt.Unix()
	shadowDoc["payload"] = ts.Payload
	return shared.JSONResponse(http.StatusOK, shadowDoc)
}

func (p *Provider) deleteThingShadow(thingName, shadowName string) (*plugin.Response, error) {
	if thingName == "" {
		return shared.JSONError("InvalidRequestException", "thingName is required", http.StatusBadRequest), nil
	}
	err := p.store.DeleteThingShadow(thingName, shadowName)
	if err != nil {
		if errors.Is(err, errShadowNotFound) {
			return shared.JSONError("ResourceNotFoundException", "shadow not found", http.StatusNotFound), nil
		}
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) listNamedShadowsForThing(thingName string) (*plugin.Response, error) {
	if thingName == "" {
		return shared.JSONError("InvalidRequestException", "thingName is required", http.StatusBadRequest), nil
	}
	names, err := p.store.ListShadowNames(thingName)
	if err != nil {
		return nil, err
	}
	if names == nil {
		names = []string{}
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"results":   names,
		"timestamp": time.Now().Unix(),
	})
}

// --- RetainedMessage / Publish ---

func (p *Provider) publish(topic string, rawBody []byte, params map[string]any) (*plugin.Response, error) {
	retain, _ := params["retain"].(bool)
	if !retain {
		// Not retained — no-op
		return shared.JSONResponse(http.StatusOK, map[string]any{})
	}
	if topic == "" {
		return shared.JSONError("InvalidRequestException", "topic is required", http.StatusBadRequest), nil
	}

	payload := ""
	if p, ok := params["payload"].(string); ok {
		payload = p
	} else if len(rawBody) > 0 {
		payload = string(rawBody)
	}

	qos := int32(0)
	if q, ok := params["qos"].(float64); ok {
		qos = int32(q)
	}

	if err := p.store.UpsertRetainedMessage(topic, payload, qos); err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) getRetainedMessage(topic string) (*plugin.Response, error) {
	if topic == "" {
		return shared.JSONError("InvalidRequestException", "topic is required", http.StatusBadRequest), nil
	}
	m, err := p.store.GetRetainedMessage(topic)
	if err != nil {
		if errors.Is(err, errRetainedMessageNotFound) {
			return shared.JSONError("ResourceNotFoundException", "retained message not found", http.StatusNotFound), nil
		}
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"topic":            m.Topic,
		"payload":          m.Payload,
		"qos":              m.Qos,
		"lastModifiedTime": m.UpdatedAt.Unix(),
	})
}

func (p *Provider) listRetainedMessages() (*plugin.Response, error) {
	msgs, err := p.store.ListRetainedMessages()
	if err != nil {
		return nil, err
	}
	topics := make([]map[string]any, 0, len(msgs))
	for _, m := range msgs {
		topics = append(topics, map[string]any{
			"topic":            m.Topic,
			"payloadSize":      int64(len(m.Payload)),
			"qos":              m.Qos,
			"lastModifiedTime": m.UpdatedAt.Unix(),
		})
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"retainedTopics": topics,
	})
}

// resolveOp maps HTTP method+path to an IoT Data Plane operation name.
func resolveOp(method, path string) string {
	segs := strings.Split(strings.Trim(path, "/"), "/")
	n := len(segs)
	if n == 0 {
		return ""
	}

	switch segs[0] {
	case "things":
		// /things/{thingName}/shadow → Get/Update/DeleteThingShadow
		if n >= 3 && segs[2] == "shadow" {
			switch method {
			case "GET":
				return "GetThingShadow"
			case "POST":
				return "UpdateThingShadow"
			case "DELETE":
				return "DeleteThingShadow"
			}
		}
	case "api":
		// /api/things/shadow/ListNamedShadowsForThing/{thingName}
		if n >= 4 && segs[1] == "things" && segs[2] == "shadow" && segs[3] == "ListNamedShadowsForThing" {
			return "ListNamedShadowsForThing"
		}
	case "topics":
		// POST /topics/{topic...} → Publish
		return "Publish"
	case "retainedMessage":
		if n == 1 {
			return "ListRetainedMessages" // GET /retainedMessage
		}
		return "GetRetainedMessage" // GET /retainedMessage/{topic...}
	case "connections":
		if method == "DELETE" {
			return "DeleteConnection"
		}
	}
	return ""
}

// --- Helpers ---

func extractPathParam(path, key string) string {
	parts := strings.Split(path, "/")
	for i, p := range parts {
		if p == key && i+1 < len(parts) {
			return parts[i+1]
		}
	}
	return ""
}

// extractGreedyPathParam returns everything after the first occurrence of key in the path.
// This handles topics like "sensor/temp" which span multiple path segments.
func extractGreedyPathParam(path, key string) string {
	prefix := "/" + key + "/"
	if idx := strings.Index(path, prefix); idx >= 0 {
		return path[idx+len(prefix):]
	}
	return ""
}
