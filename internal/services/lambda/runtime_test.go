// SPDX-License-Identifier: Apache-2.0
package lambda

import (
	"context"
	"errors"
	"github.com/stretchr/testify/require"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

func runtimeFunction(t *testing.T) *FunctionInfo {
	f := regressionFunction()
	f.CodePath = filepath.Join(t.TempDir(), "code.zip")
	require.NoError(t, os.WriteFile(f.CodePath, handlerZIP(t, "def handler(e,c): return e"), 0600))
	return f
}
func fakeRuntime(t *testing.T, envelope string) (*Runtime, *[]string) {
	r := NewRuntime()
	calls := []string{}
	r.docker = func(ctx context.Context, input []byte, args ...string) ([]byte, error) {
		calls = append(calls, strings.Join(args, " "))
		if args[0] == "create" {
			return []byte("test-container"), nil
		}
		if args[0] == "exec" && len(input) > 0 {
			require.JSONEq(t, `{"key":"value"}`, string(input))
			return []byte(envelope), nil
		}
		return nil, nil
	}
	return r, &calls
}
func TestRuntimeUnwrapsSuccess(t *testing.T) {
	r, calls := fakeRuntime(t, `{"success":true,"payload":"{\"executed\":true,\"input\":{\"key\":\"value\"}}"}`)
	f := runtimeFunction(t)
	f.Environment = map[string]string{"MODE": "phase1"}
	t.Setenv("DEVCLOUD_LAMBDA_NETWORK", "devcloud-test")
	result, err := r.Invoke(context.Background(), f, []byte(`{"key":"value"}`))
	require.NoError(t, err)
	require.JSONEq(t, `{"executed":true,"input":{"key":"value"}}`, string(result.Payload))
	require.Nil(t, result.Error)
	text := strings.Join(*calls, "\n")
	require.Contains(t, text, "MODE=phase1")
	require.Contains(t, text, "--network devcloud-test")
	require.Contains(t, text, "rm -f test-container")
	require.Contains(t, text, "--ready")
	require.Equal(t, 1, strings.Count(text, "--invoke"))
}
func TestRuntimeSeparatesFunctionErrorFromUserData(t *testing.T) {
	for _, tc := range []struct {
		env     string
		failure bool
	}{{`{"success":true,"payload":"{\"errorType\":\"user data\"}"}`, false}, {`{"success":false,"error":{"errorType":"ValueError","errorMessage":"failed"}}`, true}} {
		r, _ := fakeRuntime(t, tc.env)
		result, err := r.Invoke(context.Background(), runtimeFunction(t), []byte(`{"key":"value"}`))
		require.NoError(t, err)
		if tc.failure {
			require.Equal(t, "ValueError", result.Error.ErrorType)
		} else {
			require.Nil(t, result.Error)
			require.JSONEq(t, `{"errorType":"user data"}`, string(result.Payload))
		}
	}
}
func TestRuntimeDockerUnavailable(t *testing.T) {
	r := NewRuntime()
	r.docker = func(context.Context, []byte, ...string) ([]byte, error) { return nil, errors.New("daemon missing") }
	_, err := r.Invoke(context.Background(), runtimeFunction(t), []byte(`{}`))
	require.ErrorIs(t, err, ErrRuntimeUnavailable)
}
func TestRuntimeCleansAfterTimeout(t *testing.T) {
	r, calls := fakeRuntime(t, "")
	base := r.docker
	r.docker = func(ctx context.Context, input []byte, args ...string) ([]byte, error) {
		if args[0] == "exec" && len(input) > 0 {
			<-ctx.Done()
			return nil, ctx.Err()
		}
		return base(ctx, input, args...)
	}
	f := runtimeFunction(t)
	f.Timeout = 1
	result, err := r.Invoke(context.Background(), f, []byte(`{}`))
	require.NoError(t, err)
	require.Equal(t, "TimeoutError", result.Error.ErrorType)
	require.Contains(t, strings.Join(*calls, "\n"), "rm -f test-container")
}
func TestRuntimeConcurrencyLimit(t *testing.T) {
	r := NewRuntime()
	started := make(chan struct{}, 4)
	var mu sync.Mutex
	removed := 0
	r.docker = func(ctx context.Context, input []byte, args ...string) ([]byte, error) {
		if args[0] == "create" {
			return []byte("test-container"), nil
		}
		if args[0] == "exec" && len(input) > 0 {
			started <- struct{}{}
			<-ctx.Done()
			return nil, ctx.Err()
		}
		if args[0] == "rm" {
			mu.Lock()
			removed++
			mu.Unlock()
		}
		return nil, nil
	}
	f := runtimeFunction(t)
	for i := 0; i < 4; i++ {
		go func() { _, _ = r.Invoke(context.Background(), f, []byte(`{}`)) }()
	}
	for i := 0; i < 4; i++ {
		<-started
	}
	_, err := r.Invoke(context.Background(), f, []byte(`{}`))
	require.ErrorIs(t, err, ErrInvocationThrottled)
	require.NoError(t, r.Close(context.Background()))
	mu.Lock()
	require.Equal(t, 4, removed)
	mu.Unlock()
	_, err = r.Invoke(context.Background(), f, []byte(`{}`))
	require.ErrorIs(t, err, ErrRuntimeUnavailable)
}
