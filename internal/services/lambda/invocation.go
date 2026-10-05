// SPDX-License-Identifier: Apache-2.0
package lambda

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/skyoo2003/devcloud/internal/plugin"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"
)

var ErrInvalidFunctionReference = errors.New("invalid function reference")

func parseFunctionReference(reference, qualifier string) (name, resolvedQualifier string, err error) {
	if strings.HasPrefix(reference, "arn:") {
		parts := strings.Split(reference, ":")
		if (len(parts) != 7 && len(parts) != 8) || parts[1] != "aws" || parts[2] != "lambda" || parts[3] == "" || parts[4] != plugin.DefaultAccountID || parts[5] != "function" {
			return "", "", ErrInvalidFunctionReference
		}
		reference = strings.Join(parts[6:], ":")
	}
	parts := strings.Split(reference, ":")
	if len(parts) > 2 || !isSafePathComponent(parts[0]) {
		return "", "", ErrInvalidFunctionReference
	}
	resolvedQualifier = qualifier
	if len(parts) == 2 {
		if parts[1] == "" || (qualifier != "" && qualifier != parts[1]) {
			return "", "", fmt.Errorf("%w: conflicting qualifier", ErrInvalidFunctionReference)
		}
		resolvedQualifier = parts[1]
	}
	if resolvedQualifier != "" && !isSafePathComponent(resolvedQualifier) {
		return "", "", ErrInvalidFunctionReference
	}
	return parts[0], resolvedQualifier, nil
}

type functionEnvironment struct {
	Variables map[string]string `json:"Variables"`
}

func environmentVariables(v map[string]string) map[string]string {
	if v == nil {
		return map[string]string{}
	}
	return v
}
func validateEnvironment(v map[string]string) error {
	for k, value := range v {
		if k == "AWS_LAMBDA_RUNTIME_API" || k == "_HANDLER" || k == "DEVCLOUD_LAMBDA_HANDLER" || k == "" || strings.ContainsAny(k, "=\x00") || strings.ContainsRune(value, '\x00') {
			return fmt.Errorf("invalid or reserved environment variable: %s", k)
		}
	}
	return nil
}

type asyncInvocation struct {
	info    *FunctionInfo
	payload []byte
}

func (p *LambdaProvider) startAsyncInvocations(ctx context.Context) {
	p.asyncQueue = make(chan asyncInvocation, 64)
	p.asyncWG.Add(1)
	go func() {
		defer p.asyncWG.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case job := <-p.asyncQueue:
				if ctx.Err() != nil {
					return
				}
				result, err := p.runtime.Invoke(ctx, job.info, job.payload)
				for errors.Is(err, ErrInvocationThrottled) {
					// A queued job is already accepted. Wait for capacity without
					// retrying a handler that actually executed and failed.
					timer := time.NewTimer(50 * time.Millisecond)
					select {
					case <-ctx.Done():
						timer.Stop()
						return
					case <-timer.C:
					}
					result, err = p.runtime.Invoke(ctx, job.info, job.payload)
				}
				if err != nil || (result != nil && result.Error != nil) {
					slog.Warn("asynchronous Lambda invocation failed", "function", job.info.FunctionName, "error", err)
				}
			}
		}
	}()
}
func (p *LambdaProvider) enqueueInvocation(info *FunctionInfo, payload []byte) error {
	p.asyncMu.Lock()
	defer p.asyncMu.Unlock()
	if p.closing {
		return ErrRuntimeUnavailable
	}
	select {
	case p.asyncQueue <- asyncInvocation{info: info, payload: append([]byte(nil), payload...)}:
		return nil
	default:
		return ErrInvocationThrottled
	}
}
func invocationError(err error, name string) *plugin.Response {
	switch {
	case errors.Is(err, ErrFunctionNotFound), errors.Is(err, ErrVersionNotFound), errors.Is(err, ErrAliasNotFound):
		return notFoundError(name)
	case errors.Is(err, ErrInvalidFunctionReference), errors.Is(err, ErrInvalidFunctionCode), errors.Is(err, ErrUnsupportedRuntime):
		return lambdaError("InvalidParameterValueException", err.Error(), 400)
	case errors.Is(err, ErrInvocationThrottled):
		return lambdaError("TooManyRequestsException", err.Error(), 429)
	default:
		return lambdaError("ServiceException", err.Error(), 503)
	}
}
func (p *LambdaProvider) invoke(name string, req *http.Request) (*plugin.Response, error) {
	f, err := p.store.ResolveFunction(defaultAccountID, name, req.URL.Query().Get("Qualifier"))
	if err != nil {
		return invocationError(err, name), nil
	}
	kind := req.Header.Get("X-Amz-Invocation-Type")
	if kind == "" {
		kind = "RequestResponse"
	}
	switch kind {
	case "DryRun":
		return &plugin.Response{StatusCode: 204}, nil
	case "Event", "RequestResponse":
	default:
		return lambdaError("InvalidParameterValueException", "invalid InvocationType", 400), nil
	}
	payload, err := io.ReadAll(req.Body)
	if err != nil {
		return lambdaError("InvalidParameterValueException", "invalid request payload", 400), nil
	}
	if len(payload) == 0 {
		payload = []byte(`{}`)
	}
	if !json.Valid(payload) {
		return lambdaError("InvalidParameterValueException", "payload must be JSON", 400), nil
	}
	if _, err := runtimeImage(f.Runtime); err != nil {
		return invocationError(err, name), nil
	}
	module, method, ok := strings.Cut(f.Handler, ".")
	if !ok || module == "" || method == "" {
		return invocationError(ErrInvalidFunctionCode, name), nil
	}
	if kind == "Event" {
		if err := p.enqueueInvocation(f, payload); err != nil {
			return invocationError(err, name), nil
		}
		return &plugin.Response{StatusCode: 202}, nil
	}
	result, err := p.runtime.Invoke(req.Context(), f, payload)
	if err != nil {
		return invocationError(err, name), nil
	}
	resp := &plugin.Response{StatusCode: result.StatusCode, ContentType: "application/json", Body: result.Payload, Headers: map[string]string{}}
	if result.Error != nil {
		resp.Headers["X-Amz-Function-Error"] = "Unhandled"
	}
	if req.Header.Get("X-Amz-Log-Type") == "Tail" {
		resp.Headers["X-Amz-Log-Result"] = result.LogResult
	}
	return resp, nil
}
