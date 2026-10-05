// SPDX-License-Identifier: Apache-2.0
package lambda

import (
	"context"
	"encoding/base64"
	"github.com/skyoo2003/devcloud/internal/plugin"
	"github.com/stretchr/testify/require"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestInvokeDryRun(t *testing.T) {
	p := newTestLambdaProvider(t)
	_, err := p.store.CreateFunction(regressionFunction(), handlerZIP(t, "pass"))
	require.NoError(t, err)
	req := httptest.NewRequest("POST", "/", strings.NewReader(`{}`))
	req.Header.Set("X-Amz-Invocation-Type", "DryRun")
	resp, err := p.invoke("immutable", req)
	require.NoError(t, err)
	require.Equal(t, 204, resp.StatusCode)
}
func TestEnvironmentUpdatePreservesOrClears(t *testing.T) {
	p := newTestLambdaProvider(t)
	req := httptest.NewRequest("POST", "/", strings.NewReader(`{"FunctionName":"env","Runtime":"python3.12","Handler":"index.handler","Environment":{"Variables":{"MODE":"v1"}}}`))
	resp, err := p.createFunction(req)
	require.NoError(t, err)
	require.Contains(t, string(resp.Body), `"MODE":"v1"`)
	for _, tc := range []struct {
		body string
		want string
	}{{`{"Description":"updated"}`, `"MODE":"v1"`}, {`{"Environment":{"Variables":{}}}`, `"Variables":{}`}} {
		resp, err = p.updateFunctionConfiguration("env", httptest.NewRequest("PUT", "/", strings.NewReader(tc.body)))
		require.NoError(t, err)
		require.Contains(t, string(resp.Body), tc.want)
	}
	resp, err = p.updateFunctionConfiguration("env", httptest.NewRequest("PUT", "/", strings.NewReader(`{"Environment":{"Variables":{"_HANDLER":"wrong"}}}`)))
	require.NoError(t, err)
	require.Equal(t, 400, resp.StatusCode)
}

type testInvoker struct {
	invoke func(context.Context, *FunctionInfo, []byte) (*InvokeResult, error)
}

func (f testInvoker) Invoke(ctx context.Context, info *FunctionInfo, payload []byte) (*InvokeResult, error) {
	return f.invoke(ctx, info, payload)
}
func (f testInvoker) Close(context.Context) error { return nil }
func TestInvokeActualPayload(t *testing.T) {
	p := newTestLambdaProvider(t)
	_, err := p.store.CreateFunction(regressionFunction(), handlerZIP(t, "pass"))
	require.NoError(t, err)
	p.runtime = testInvoker{func(_ context.Context, info *FunctionInfo, payload []byte) (*InvokeResult, error) {
		require.Equal(t, "immutable", info.FunctionName)
		return &InvokeResult{StatusCode: 200, Payload: payload, LogResult: base64.StdEncoding.EncodeToString([]byte("log"))}, nil
	}}
	req := httptest.NewRequest("POST", "/", strings.NewReader(`{"key":"value"}`))
	req.Header.Set("X-Amz-Log-Type", "Tail")
	resp, err := p.invoke("immutable", req)
	require.NoError(t, err)
	require.JSONEq(t, `{"key":"value"}`, string(resp.Body))
	require.Equal(t, "bG9n", resp.Headers["X-Amz-Log-Result"])
}
func TestInvokeFunctionError(t *testing.T) {
	p := newTestLambdaProvider(t)
	_, err := p.store.CreateFunction(regressionFunction(), handlerZIP(t, "pass"))
	require.NoError(t, err)
	p.runtime = testInvoker{func(context.Context, *FunctionInfo, []byte) (*InvokeResult, error) {
		return functionFailure("ValueError", "failed"), nil
	}}
	resp, err := p.invoke("immutable", httptest.NewRequest("POST", "/", strings.NewReader(`{}`)))
	require.NoError(t, err)
	require.Equal(t, 200, resp.StatusCode)
	require.Equal(t, "Unhandled", resp.Headers["X-Amz-Function-Error"])
}
func TestInvokeRuntimeUnavailable(t *testing.T) {
	p := newTestLambdaProvider(t)
	_, err := p.store.CreateFunction(regressionFunction(), handlerZIP(t, "pass"))
	require.NoError(t, err)
	for _, tc := range []struct {
		err    error
		status int
	}{{ErrRuntimeUnavailable, 503}, {ErrUnsupportedRuntime, 400}, {ErrInvalidFunctionCode, 400}, {ErrInvocationThrottled, 429}} {
		p.runtime = testInvoker{func(context.Context, *FunctionInfo, []byte) (*InvokeResult, error) { return nil, tc.err }}
		resp, err := p.invoke("immutable", httptest.NewRequest("POST", "/", strings.NewReader(`{}`)))
		require.NoError(t, err)
		require.Equal(t, tc.status, resp.StatusCode)
	}
}
func TestInvokeAsyncRequestCancellation(t *testing.T) {
	p := newTestLambdaProvider(t)
	_, err := p.store.CreateFunction(regressionFunction(), handlerZIP(t, "pass"))
	require.NoError(t, err)
	invoked := make(chan error, 1)
	p.runtime = testInvoker{func(ctx context.Context, _ *FunctionInfo, _ []byte) (*InvokeResult, error) {
		invoked <- ctx.Err()
		return &InvokeResult{StatusCode: 200, Payload: []byte(`{}`)}, nil
	}}
	ctx, cancel := context.WithCancel(context.Background())
	req := httptest.NewRequest("POST", "/", strings.NewReader(`{}`)).WithContext(ctx)
	req.Header.Set("X-Amz-Invocation-Type", "Event")
	resp, err := p.invoke("immutable", req)
	require.NoError(t, err)
	require.Equal(t, 202, resp.StatusCode)
	cancel()
	select {
	case err := <-invoked:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("async handler never ran")
	}
}
func TestInvokeAsyncQueueFull(t *testing.T) {
	p := newTestLambdaProvider(t)
	info, err := p.store.CreateFunction(regressionFunction(), handlerZIP(t, "pass"))
	require.NoError(t, err)
	started := make(chan struct{})
	var calls atomic.Int32
	p.runtime = testInvoker{func(ctx context.Context, _ *FunctionInfo, _ []byte) (*InvokeResult, error) {
		if calls.Add(1) == 1 {
			close(started)
		}
		<-ctx.Done()
		return nil, ctx.Err()
	}}
	require.NoError(t, p.enqueueInvocation(info, []byte(`{}`)))
	<-started
	for i := 0; i < 64; i++ {
		require.NoError(t, p.enqueueInvocation(info, []byte(`{}`)))
	}
	require.ErrorIs(t, p.enqueueInvocation(info, []byte(`{}`)), ErrInvocationThrottled)
	require.NoError(t, p.Shutdown(context.Background()))
}

var _ plugin.ServicePlugin = (*LambdaProvider)(nil)

func TestAsyncSaturationRetainsAcceptedJob(t *testing.T) {
	p := newTestLambdaProvider(t)
	_, err := p.store.CreateFunction(regressionFunction(), handlerZIP(t, "pass"))
	require.NoError(t, err)
	var busy atomic.Bool
	busy.Store(true)
	attempted := make(chan struct{}, 1)
	delivered := make(chan string, 1)
	p.runtime = testInvoker{func(ctx context.Context, _ *FunctionInfo, payload []byte) (*InvokeResult, error) {
		if busy.Load() {
			select {
			case attempted <- struct{}{}:
			default:
			}
			return nil, ErrInvocationThrottled
		}
		delivered <- string(payload)
		return &InvokeResult{StatusCode: 200, Payload: []byte(`{}`)}, nil
	}}
	req := httptest.NewRequest("POST", "/", strings.NewReader(`{"accepted":true}`))
	req.Header.Set("X-Amz-Invocation-Type", "Event")
	response, err := p.invoke("immutable", req)
	require.NoError(t, err)
	require.Equal(t, 202, response.StatusCode)
	select {
	case <-attempted:
	case <-time.After(time.Second):
		t.Fatal("worker never attempted delivery")
	}
	busy.Store(false)
	select {
	case payload := <-delivered:
		require.JSONEq(t, `{"accepted":true}`, payload)
	case <-time.After(time.Second):
		t.Fatal("accepted job lost during temporary saturation")
	}
}
