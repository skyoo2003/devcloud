// SPDX-License-Identifier: Apache-2.0
package lambda

import (
	"context"
	"crypto/rand"
	"embed"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

var (
	ErrRuntimeUnavailable  = errors.New("lambda Docker runtime unavailable")
	ErrUnsupportedRuntime  = errors.New("unsupported Lambda runtime")
	ErrInvalidFunctionCode = errors.New("invalid Lambda function code")
	ErrInvocationThrottled = errors.New("lambda concurrency limit reached")
)

type FunctionInvoker interface {
	Invoke(context.Context, *FunctionInfo, []byte) (*InvokeResult, error)
	Close(context.Context) error
}
type InvokeResult struct {
	StatusCode int
	Payload    []byte
	LogResult  string
	Error      *InvokeError
}
type InvokeError struct {
	ErrorType    string `json:"errorType"`
	ErrorMessage string `json:"errorMessage"`
}

//go:embed runtime_assets/*
var runtimeAssets embed.FS

type Runtime struct {
	docker func(context.Context, []byte, ...string) ([]byte, error)
	slots  chan struct{}
	ctx    context.Context
	cancel context.CancelFunc
	mu     sync.Mutex
	closed bool
	wg     sync.WaitGroup
}

func NewRuntime() *Runtime {
	ctx, cancel := context.WithCancel(context.Background())
	return &Runtime{docker: runDocker, slots: make(chan struct{}, 4), ctx: ctx, cancel: cancel}
}
func (r *Runtime) Close(ctx context.Context) error {
	r.mu.Lock()
	r.closed = true
	r.cancel()
	r.mu.Unlock()
	done := make(chan struct{})
	go func() { r.wg.Wait(); close(done) }()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
func functionFailure(kind, message string) *InvokeResult {
	e := &InvokeError{ErrorType: kind, ErrorMessage: message}
	payload, _ := json.Marshal(e)
	return &InvokeResult{StatusCode: 200, Payload: payload, Error: e}
}
func (r *Runtime) Invoke(ctx context.Context, f *FunctionInfo, payload []byte) (*InvokeResult, error) {
	image, err := runtimeImage(f.Runtime)
	if err != nil {
		return nil, err
	}
	if !json.Valid(payload) {
		return nil, fmt.Errorf("%w: payload is not JSON", ErrInvalidFunctionCode)
	}
	module, method, ok := strings.Cut(f.Handler, ".")
	if !ok || module == "" || method == "" {
		return nil, fmt.Errorf("%w: invalid handler", ErrInvalidFunctionCode)
	}
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return nil, ErrRuntimeUnavailable
	}
	select {
	case r.slots <- struct{}{}:
	default:
		r.mu.Unlock()
		return nil, ErrInvocationThrottled
	}
	r.wg.Add(1)
	r.mu.Unlock()
	defer r.wg.Done()
	defer func() { <-r.slots }()
	callCtx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(r.ctx, cancel)
	defer cancel()
	defer stop()
	dir, err := os.MkdirTemp("", "devcloud-lambda-*")
	if err != nil {
		return nil, err
	}
	defer func() { _ = os.RemoveAll(dir) }()
	if err := extractFunctionArchive(f.CodePath, dir); err != nil {
		return nil, err
	}
	extension, executable, adapter := "py", "python3", "/var/task/__devcloud_invoke__.py"
	if f.Runtime == "nodejs22.x" {
		extension, executable, adapter = "mjs", "node", "/var/task/__devcloud_invoke__.mjs"
	}
	for _, asset := range []string{"handler", "invoke"} {
		b, err := runtimeAssets.ReadFile("runtime_assets/" + asset + "." + extension)
		if err != nil {
			return nil, err
		}
		if err := os.WriteFile(filepath.Join(dir, "__devcloud_"+asset+"__."+extension), b, 0644); err != nil {
			return nil, err
		}
	}
	prepCtx, prepCancel := context.WithTimeout(callCtx, 120*time.Second)
	_, err = r.docker(prepCtx, nil, "image", "inspect", image)
	if err != nil {
		_, err = r.docker(prepCtx, nil, "pull", image)
	}
	prepCancel()
	if err != nil {
		return nil, fmt.Errorf("%w: image preparation: %v", ErrRuntimeUnavailable, err)
	}
	container := "devcloud-lambda-" + strings.ToLower(rand.Text())
	defer func() {
		cleanCtx, cleanCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanCancel()
		_, _ = r.docker(cleanCtx, nil, "rm", "-f", container)
	}()
	initCtx, initCancel := context.WithTimeout(callCtx, 30*time.Second)
	defer initCancel()
	args := []string{"create", "--name", container, "--add-host", "host.docker.internal:host-gateway"}
	if network := os.Getenv("DEVCLOUD_LAMBDA_NETWORK"); network != "" {
		args = append(args, "--network", network)
	}
	env := map[string]string{"AWS_ACCESS_KEY_ID": "test", "AWS_SECRET_ACCESS_KEY": "test", "AWS_DEFAULT_REGION": "us-east-1", "AWS_REGION": "us-east-1", "AWS_LAMBDA_FUNCTION_NAME": f.FunctionName, "AWS_LAMBDA_FUNCTION_MEMORY_SIZE": fmt.Sprint(f.MemorySize), "AWS_LAMBDA_FUNCTION_TIMEOUT": fmt.Sprint(f.Timeout)}
	for k, v := range f.Environment {
		env[k] = v
	}
	env["DEVCLOUD_LAMBDA_HANDLER"] = f.Handler
	keys := make([]string, 0, len(env))
	for k := range env {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		args = append(args, "--env", k+"="+env[k])
	}
	args = append(args, image, "__devcloud_handler__.handler")
	output, err := r.docker(initCtx, nil, args...)
	if err != nil {
		return nil, fmt.Errorf("%w: create: %v", ErrRuntimeUnavailable, err)
	}
	if id := strings.TrimSpace(string(output)); id != "" {
		container = id
	}
	if _, err = r.docker(initCtx, nil, "cp", dir+"/.", container+":/var/task"); err != nil {
		return nil, fmt.Errorf("%w: copy: %v", ErrRuntimeUnavailable, err)
	}
	if _, err = r.docker(initCtx, nil, "start", container); err != nil {
		return nil, fmt.Errorf("%w: start: %v", ErrRuntimeUnavailable, err)
	}
	if _, err = r.docker(initCtx, nil, "exec", container, executable, adapter, "--ready"); err != nil {
		return nil, fmt.Errorf("%w: initialization: %v", ErrRuntimeUnavailable, err)
	}
	timeout := f.Timeout
	if timeout <= 0 {
		timeout = 3
	}
	invokeCtx, invokeCancel := context.WithTimeout(callCtx, time.Duration(timeout)*time.Second)
	output, err = r.docker(invokeCtx, payload, "exec", "-i", container, executable, adapter, "--invoke")
	invokeCancel()
	var result *InvokeResult
	if err != nil {
		if callCtx.Err() != nil {
			return nil, callCtx.Err()
		}
		if errors.Is(invokeCtx.Err(), context.DeadlineExceeded) {
			result = functionFailure("TimeoutError", fmt.Sprintf("Task timed out after %d seconds", timeout))
		} else {
			return nil, fmt.Errorf("%w: invocation transport: %v", ErrRuntimeUnavailable, err)
		}
	} else {
		var envelope struct {
			Success *bool        `json:"success"`
			Payload string       `json:"payload"`
			Error   *InvokeError `json:"error"`
		}
		if err := json.Unmarshal(output, &envelope); err != nil || envelope.Success == nil {
			return nil, fmt.Errorf("%w: invalid runtime response", ErrRuntimeUnavailable)
		}
		if *envelope.Success {
			if !json.Valid([]byte(envelope.Payload)) {
				return nil, fmt.Errorf("%w: invalid result JSON", ErrRuntimeUnavailable)
			}
			result = &InvokeResult{StatusCode: 200, Payload: []byte(envelope.Payload)}
		} else {
			if envelope.Error == nil {
				return nil, fmt.Errorf("%w: missing function error", ErrRuntimeUnavailable)
			}
			result = functionFailure(envelope.Error.ErrorType, envelope.Error.ErrorMessage)
		}
	}
	logCtx, logCancel := context.WithTimeout(callCtx, 2*time.Second)
	logs, _ := r.docker(logCtx, nil, "logs", container)
	logCancel()
	if len(logs) > 4096 {
		logs = logs[len(logs)-4096:]
	}
	result.LogResult = base64.StdEncoding.EncodeToString(logs)
	return result, nil
}
