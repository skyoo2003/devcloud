// SPDX-License-Identifier: Apache-2.0

package sagemakerruntime

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	generated "github.com/skyoo2003/devcloud/internal/generated/sagemakerruntime"
	"github.com/skyoo2003/devcloud/internal/plugin"
	"github.com/skyoo2003/devcloud/internal/shared/crud"
)

// Provider implements the SageMakerRuntime service.
type Provider struct {
	generated.BaseProvider
	dataDir string
}

func (p *Provider) ServiceID() string             { return "sagemakerruntime" }
func (p *Provider) ServiceName() string           { return "SageMakerRuntime" }
func (p *Provider) Protocol() plugin.ProtocolType { return plugin.ProtocolRESTJSON }

func (p *Provider) Init(cfg plugin.PluginConfig) error {
	p.dataDir = cfg.DataDir
	return nil
}

func (p *Provider) Shutdown(ctx context.Context) error {
	return nil
}

func (p *Provider) HandleRequest(ctx context.Context, op string, req *http.Request) (*plugin.Response, error) {
	if op == "" {
		op, _ = generated.MatchOperation(req.Method, req.URL.RequestURI())
	}

	switch op {
	case "InvokeEndpoint":
		return p.handleInvokeEndpoint(ctx, req)
	case "InvokeEndpointAsync":
		return p.handleInvokeEndpointAsync(ctx, req)
	case "InvokeEndpointWithResponseStream":
		return p.handleInvokeEndpointWithResponseStream(ctx, req)
	default:
		return nil, plugin.ErrUnhandledOp
	}
}

func (p *Provider) handleInvokeEndpoint(ctx context.Context, req *http.Request) (*plugin.Response, error) {
	_, params := generated.MatchOperation(req.Method, req.URL.RequestURI())
	endpointName := params["EndpointName"]
	if endpointName == "" {
		endpointName = "default-endpoint"
	}

	var reqBody []byte
	if req.Body != nil {
		reqBody, _ = io.ReadAll(req.Body)
	}

	contentType := req.Header.Get("Accept")
	if contentType == "" || contentType == "*/*" {
		contentType = "application/json"
	}

	respBody := []byte(fmt.Sprintf(`{"predictions":[0.0],"endpoint":"%s"}`, endpointName))
	if len(reqBody) > 0 {
		var parsed any
		if err := json.Unmarshal(reqBody, &parsed); err == nil {
			respObj := map[string]any{
				"predictions": []float64{0.0},
				"inputs":      parsed,
				"endpoint":    endpointName,
			}
			respBody, _ = json.Marshal(respObj)
		}
	}

	headers := map[string]string{
		"Content-Type":                      contentType,
		"x-Amzn-Invoked-Production-Variant": "AllTraffic",
	}
	if customAttr := req.Header.Get("X-Amzn-SageMaker-Custom-Attributes"); customAttr != "" {
		headers["X-Amzn-SageMaker-Custom-Attributes"] = customAttr
	}

	return &plugin.Response{
		StatusCode:  http.StatusOK,
		ContentType: contentType,
		Headers:     headers,
		Body:        respBody,
	}, nil
}

func (p *Provider) handleInvokeEndpointAsync(ctx context.Context, req *http.Request) (*plugin.Response, error) {
	_, params := generated.MatchOperation(req.Method, req.URL.RequestURI())
	endpointName := params["EndpointName"]
	if endpointName == "" {
		endpointName = "default-endpoint"
	}

	inferenceID := fmt.Sprintf("inf-%d", time.Now().UnixNano())
	outputLocation := fmt.Sprintf("s3://devcloud-sagemaker-output/%s/%s.out", endpointName, inferenceID)

	out := generated.InvokeEndpointAsyncOutput{
		InferenceId:    inferenceID,
		OutputLocation: outputLocation,
	}

	body, _ := json.Marshal(out)
	headers := map[string]string{
		"Content-Type":                    "application/json",
		"x-Amzn-SageMaker-OutputLocation": outputLocation,
	}

	return &plugin.Response{
		StatusCode:  http.StatusAccepted,
		ContentType: "application/json",
		Headers:     headers,
		Body:        body,
	}, nil
}

func (p *Provider) handleInvokeEndpointWithResponseStream(ctx context.Context, req *http.Request) (*plugin.Response, error) {
	_, params := generated.MatchOperation(req.Method, req.URL.RequestURI())
	endpointName := params["EndpointName"]
	if endpointName == "" {
		endpointName = "default-endpoint"
	}

	// Mock response stream chunk payload
	payload := fmt.Sprintf(`{"predictions":[0.0],"endpoint":"%s"}`, endpointName)
	return &plugin.Response{
		StatusCode:  http.StatusOK,
		ContentType: "application/json",
		Headers: map[string]string{
			"Content-Type":                      "application/json",
			"x-Amzn-Invoked-Production-Variant": "AllTraffic",
		},
		Body: []byte(payload),
	}, nil
}

func (p *Provider) ListResources(ctx context.Context) ([]plugin.Resource, error) {
	return []plugin.Resource{}, nil
}

func init() {
	plugin.DefaultRegistry.Register("sagemakerruntime", func() plugin.ServicePlugin {
		return &Provider{}
	})
	crud.RegisterRoutes("sagemakerruntime", generated.OperationRoutes)
}
