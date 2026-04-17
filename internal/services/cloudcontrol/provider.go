// SPDX-License-Identifier: Apache-2.0

// Package cloudcontrol implements AWS Cloud Control API.
package cloudcontrol

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

// CloudControlProvider implements plugin.ServicePlugin for Cloud Control API.
type CloudControlProvider struct {
	store *Store
}

func (p *CloudControlProvider) ServiceID() string             { return "cloudcontrol" }
func (p *CloudControlProvider) ServiceName() string           { return "AWS Cloud Control API" }
func (p *CloudControlProvider) Protocol() plugin.ProtocolType { return plugin.ProtocolJSON10 }

func (p *CloudControlProvider) Init(cfg plugin.PluginConfig) error {
	dataDir := cfg.DataDir
	if dataDir == "" {
		dataDir = "."
	}
	var err error
	p.store, err = NewStore(filepath.Join(dataDir, "cloudcontrol"))
	return err
}

func (p *CloudControlProvider) Shutdown(_ context.Context) error {
	if p.store != nil {
		return p.store.Close()
	}
	return nil
}

func (p *CloudControlProvider) HandleRequest(_ context.Context, op string, req *http.Request) (*plugin.Response, error) {
	if op == "" {
		target := req.Header.Get("X-Amz-Target")
		if idx := strings.LastIndex(target, "."); idx >= 0 {
			op = target[idx+1:]
		}
	}

	body, err := io.ReadAll(req.Body)
	if err != nil {
		return shared.JSONError("SerializationException", "failed to read body", http.StatusBadRequest), nil
	}
	var params map[string]any
	if len(body) > 0 {
		if err := json.Unmarshal(body, &params); err != nil {
			return shared.JSONError("SerializationException", "invalid JSON", http.StatusBadRequest), nil
		}
	} else {
		params = map[string]any{}
	}

	switch op {
	case "CreateResource":
		return p.createResource(params)
	case "GetResource":
		return p.getResource(params)
	case "UpdateResource":
		return p.updateResource(params)
	case "DeleteResource":
		return p.deleteResource(params)
	case "ListResources":
		return p.listResources(params)
	case "GetResourceRequestStatus":
		return p.getResourceRequestStatus(params)
	case "ListResourceRequests":
		return p.listResourceRequests(params)
	case "CancelResourceRequest":
		return p.cancelResourceRequest(params)
	case "TagResource":
		return shared.JSONResponse(http.StatusOK, map[string]any{})
	case "UntagResource":
		return shared.JSONResponse(http.StatusOK, map[string]any{})
	case "ListTagsForResource":
		return shared.JSONResponse(http.StatusOK, map[string]any{"Tags": []any{}})
	case "ListResourceOperations":
		return shared.JSONResponse(http.StatusOK, map[string]any{
			"ResourceOperations": []any{},
			"NextToken":          nil,
		})
	case "ListResourceSchemas":
		return shared.JSONResponse(http.StatusOK, map[string]any{
			"ResourceSchemas": []any{},
			"NextToken":       nil,
		})
	case "GetResourceSchema":
		return shared.JSONResponse(http.StatusOK, map[string]any{
			"TypeName": params["TypeName"],
			"Schema":   "{}",
		})
	case "CloneResource":
		return p.createResource(params)
	case "ValidateResource":
		return shared.JSONResponse(http.StatusOK, map[string]any{
			"IsValid":          true,
			"ValidationErrors": []any{},
		})
	case "BatchGetResources":
		return shared.JSONResponse(http.StatusOK, map[string]any{
			"ResourceDescriptions": []any{},
		})
	case "DescribeResource":
		return p.getResource(params)
	case "WaitForResource":
		return shared.JSONResponse(http.StatusOK, map[string]any{
			"ProgressEvent": map[string]any{
				"OperationStatus": "SUCCESS",
			},
		})
	case "RefreshResource":
		return shared.JSONResponse(http.StatusOK, map[string]any{})
	case "ExportResources":
		return shared.JSONResponse(http.StatusOK, map[string]any{"Resources": []any{}})
	case "ImportResource":
		return p.createResource(params)
	case "ListResourceTypes":
		return shared.JSONResponse(http.StatusOK, map[string]any{
			"ResourceTypes": []any{},
		})
	case "GetResourceType":
		return shared.JSONResponse(http.StatusOK, map[string]any{
			"TypeName": params["TypeName"],
		})
	case "ListStackResources":
		return shared.JSONResponse(http.StatusOK, map[string]any{
			"StackResources": []any{},
		})
	case "GetHookResults":
		return shared.JSONResponse(http.StatusOK, map[string]any{
			"HookResults": []any{},
		})
	default:
		return shared.JSONError("UnsupportedOperation", fmt.Sprintf("operation not supported: %s", op), http.StatusBadRequest), nil
	}
}

func (p *CloudControlProvider) ListResources(_ context.Context) ([]plugin.Resource, error) {
	return []plugin.Resource{}, nil
}

func (p *CloudControlProvider) GetMetrics(_ context.Context) (*plugin.ServiceMetrics, error) {
	return &plugin.ServiceMetrics{}, nil
}

// --- Operations ---

func (p *CloudControlProvider) createResource(params map[string]any) (*plugin.Response, error) {
	typeName, _ := params["TypeName"].(string)
	if typeName == "" {
		return shared.JSONError("ValidationException", "TypeName is required", http.StatusBadRequest), nil
	}
	desiredState, _ := params["DesiredState"].(string)

	// Generate identifier from desired state if possible
	identifier := shared.GenerateID("", 12)
	if desiredState != "" {
		var ds map[string]any
		if err := json.Unmarshal([]byte(desiredState), &ds); err == nil {
			// Try common ID fields
			for _, field := range []string{"Id", "Name", "Arn", "ResourceId"} {
				if v, ok := ds[field].(string); ok && v != "" {
					identifier = v
					break
				}
			}
		}
	}

	resource := &CCResource{
		TypeName:   typeName,
		Identifier: identifier,
		State:      desiredState,
		AccountID:  shared.DefaultAccountID,
		CreatedAt:  time.Now(),
	}
	if err := p.store.CreateResource(resource); err != nil {
		return nil, err
	}

	requestToken := shared.GenerateUUID()
	ccReq := &CCRequest{
		RequestToken: requestToken,
		Operation:    "CREATE",
		TypeName:     typeName,
		Identifier:   identifier,
		Status:       "SUCCESS",
		AccountID:    shared.DefaultAccountID,
		CreatedAt:    time.Now(),
		CompletedAt:  time.Now(),
	}
	p.store.CreateRequest(ccReq)

	return shared.JSONResponse(http.StatusOK, map[string]any{
		"ProgressEvent": progressEvent(ccReq),
	})
}

func (p *CloudControlProvider) getResource(params map[string]any) (*plugin.Response, error) {
	typeName, _ := params["TypeName"].(string)
	identifier, _ := params["Identifier"].(string)
	if typeName == "" || identifier == "" {
		return shared.JSONError("ValidationException", "TypeName and Identifier are required", http.StatusBadRequest), nil
	}

	res, err := p.store.GetResource(typeName, identifier, shared.DefaultAccountID)
	if err != nil {
		if errors.Is(err, errResourceNotFound) {
			return shared.JSONError("ResourceNotFoundException", fmt.Sprintf("Resource of type '%s' with identifier '%s' was not found", typeName, identifier), http.StatusNotFound), nil
		}
		return nil, err
	}

	return shared.JSONResponse(http.StatusOK, map[string]any{
		"ResourceDescription": map[string]any{
			"Identifier": res.Identifier,
			"Properties": res.State,
		},
		"TypeName": typeName,
	})
}

func (p *CloudControlProvider) updateResource(params map[string]any) (*plugin.Response, error) {
	typeName, _ := params["TypeName"].(string)
	identifier, _ := params["Identifier"].(string)
	if typeName == "" || identifier == "" {
		return shared.JSONError("ValidationException", "TypeName and Identifier are required", http.StatusBadRequest), nil
	}

	patchDocument, _ := params["PatchDocument"].(string)

	if _, err := p.store.GetResource(typeName, identifier, shared.DefaultAccountID); err != nil {
		if errors.Is(err, errResourceNotFound) {
			return shared.JSONError("ResourceNotFoundException", fmt.Sprintf("Resource of type '%s' with identifier '%s' was not found", typeName, identifier), http.StatusNotFound), nil
		}
		return nil, err
	}

	if patchDocument != "" {
		p.store.UpdateResource(typeName, identifier, shared.DefaultAccountID, patchDocument)
	}

	requestToken := shared.GenerateUUID()
	ccReq := &CCRequest{
		RequestToken: requestToken,
		Operation:    "UPDATE",
		TypeName:     typeName,
		Identifier:   identifier,
		Status:       "SUCCESS",
		AccountID:    shared.DefaultAccountID,
		CreatedAt:    time.Now(),
		CompletedAt:  time.Now(),
	}
	p.store.CreateRequest(ccReq)

	return shared.JSONResponse(http.StatusOK, map[string]any{
		"ProgressEvent": progressEvent(ccReq),
	})
}

func (p *CloudControlProvider) deleteResource(params map[string]any) (*plugin.Response, error) {
	typeName, _ := params["TypeName"].(string)
	identifier, _ := params["Identifier"].(string)
	if typeName == "" || identifier == "" {
		return shared.JSONError("ValidationException", "TypeName and Identifier are required", http.StatusBadRequest), nil
	}

	if err := p.store.DeleteResource(typeName, identifier, shared.DefaultAccountID); err != nil {
		if errors.Is(err, errResourceNotFound) {
			return shared.JSONError("ResourceNotFoundException", fmt.Sprintf("Resource of type '%s' with identifier '%s' was not found", typeName, identifier), http.StatusNotFound), nil
		}
		return nil, err
	}

	requestToken := shared.GenerateUUID()
	ccReq := &CCRequest{
		RequestToken: requestToken,
		Operation:    "DELETE",
		TypeName:     typeName,
		Identifier:   identifier,
		Status:       "SUCCESS",
		AccountID:    shared.DefaultAccountID,
		CreatedAt:    time.Now(),
		CompletedAt:  time.Now(),
	}
	p.store.CreateRequest(ccReq)

	return shared.JSONResponse(http.StatusOK, map[string]any{
		"ProgressEvent": progressEvent(ccReq),
	})
}

func (p *CloudControlProvider) listResources(params map[string]any) (*plugin.Response, error) {
	typeName, _ := params["TypeName"].(string)
	if typeName == "" {
		return shared.JSONError("ValidationException", "TypeName is required", http.StatusBadRequest), nil
	}

	resources, err := p.store.ListResources(typeName, shared.DefaultAccountID)
	if err != nil {
		return nil, err
	}

	items := make([]map[string]any, 0, len(resources))
	for _, r := range resources {
		items = append(items, map[string]any{
			"Identifier": r.Identifier,
			"Properties": r.State,
		})
	}

	return shared.JSONResponse(http.StatusOK, map[string]any{
		"ResourceDescriptions": items,
		"TypeName":             typeName,
		"NextToken":            nil,
	})
}

func (p *CloudControlProvider) getResourceRequestStatus(params map[string]any) (*plugin.Response, error) {
	requestToken, _ := params["RequestToken"].(string)
	if requestToken == "" {
		return shared.JSONError("ValidationException", "RequestToken is required", http.StatusBadRequest), nil
	}

	req, err := p.store.GetRequest(requestToken)
	if err != nil {
		if errors.Is(err, errRequestNotFound) {
			return shared.JSONError("RequestTokenNotFoundException", "request token not found", http.StatusNotFound), nil
		}
		return nil, err
	}

	return shared.JSONResponse(http.StatusOK, map[string]any{
		"ProgressEvent": progressEvent(req),
	})
}

func (p *CloudControlProvider) listResourceRequests(params map[string]any) (*plugin.Response, error) {
	requests, err := p.store.ListRequests(shared.DefaultAccountID)
	if err != nil {
		return nil, err
	}

	// Apply optional filters
	var filter map[string]any
	if f, ok := params["ResourceRequestStatusFilter"].(map[string]any); ok {
		filter = f
	}
	_ = filter // For simplicity, return all requests

	items := make([]map[string]any, 0, len(requests))
	for _, r := range requests {
		items = append(items, progressEvent(&r))
	}

	return shared.JSONResponse(http.StatusOK, map[string]any{
		"ResourceRequestStatusSummaries": items,
		"NextToken":                      nil,
	})
}

func (p *CloudControlProvider) cancelResourceRequest(params map[string]any) (*plugin.Response, error) {
	requestToken, _ := params["RequestToken"].(string)
	if requestToken == "" {
		return shared.JSONError("ValidationException", "RequestToken is required", http.StatusBadRequest), nil
	}

	req, err := p.store.GetRequest(requestToken)
	if err != nil {
		if errors.Is(err, errRequestNotFound) {
			return shared.JSONError("RequestTokenNotFoundException", "request token not found", http.StatusNotFound), nil
		}
		return nil, err
	}

	p.store.CancelRequest(requestToken, shared.DefaultAccountID)
	req.Status = "CANCEL_COMPLETE"

	return shared.JSONResponse(http.StatusOK, map[string]any{
		"ProgressEvent": progressEvent(req),
	})
}

// --- Helpers ---

func progressEvent(req *CCRequest) map[string]any {
	return map[string]any{
		"RequestToken":    req.RequestToken,
		"Operation":       req.Operation,
		"TypeName":        req.TypeName,
		"Identifier":      req.Identifier,
		"OperationStatus": req.Status,
		"StatusMessage":   req.StatusMessage,
		"EventTime":       req.CompletedAt.UTC().Format(time.RFC3339),
	}
}

func init() {
	plugin.DefaultRegistry.Register("cloudcontrol", func(cfg plugin.PluginConfig) plugin.ServicePlugin {
		return &CloudControlProvider{}
	})
}
