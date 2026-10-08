// SPDX-License-Identifier: Apache-2.0

package lambda

import (
	"encoding/json"
	"io"
	"net/http"
	"time"

	"github.com/skyoo2003/devcloud/internal/plugin"
)

// Durable Execution SQLite store methods

func (s *LambdaStore) RecordDurableExecution(executionID, functionName, status, checkpoint string) error {
	now := time.Now().UTC()
	_, err := s.store.DB().Exec(
		`INSERT INTO durable_executions (execution_id, function_name, status, checkpoint_data, updated_at)
		VALUES (?, ?, ?, ?, ?)
		ON CONFLICT(execution_id) DO UPDATE SET
			status = excluded.status,
			checkpoint_data = excluded.checkpoint_data,
			updated_at = excluded.updated_at;`,
		executionID, functionName, status, checkpoint, now,
	)
	return err
}

func (s *LambdaStore) UpdateDurableExecutionStatus(executionID, status string) error {
	now := time.Now().UTC()
	_, err := s.store.DB().Exec(
		`INSERT INTO durable_executions (execution_id, function_name, status, checkpoint_data, updated_at)
		VALUES (?, '', ?, '{}', ?)
		ON CONFLICT(execution_id) DO UPDATE SET
			status = excluded.status,
			updated_at = excluded.updated_at;`,
		executionID, status, now,
	)
	return err
}

func (s *LambdaStore) TouchDurableExecution(executionID string) error {
	now := time.Now().UTC()
	_, err := s.store.DB().Exec(
		`UPDATE durable_executions SET updated_at = ? WHERE execution_id = ?;`,
		now, executionID,
	)
	return err
}

// Durable HTTP handlers on LambdaProvider

func (p *LambdaProvider) checkpointDurableExecution(executionArn string, req *http.Request) (*plugin.Response, error) {
	var bodyBytes []byte
	if req.Body != nil {
		bodyBytes, _ = io.ReadAll(req.Body)
	}
	_ = p.store.RecordDurableExecution(executionArn, "", "RUNNING", string(bodyBytes))

	out := map[string]any{
		"DurableExecutionArn": executionArn,
		"Status":              "RUNNING",
	}
	bytes, _ := json.Marshal(out)
	return &plugin.Response{
		StatusCode:  http.StatusOK,
		ContentType: "application/json",
		Body:        bytes,
	}, nil
}

func (p *LambdaProvider) sendDurableCallbackSuccess(callbackID string, _ *http.Request) (*plugin.Response, error) {
	_ = p.store.UpdateDurableExecutionStatus(callbackID, "SUCCEEDED")
	return &plugin.Response{
		StatusCode:  http.StatusOK,
		ContentType: "application/json",
		Body:        []byte("{}"),
	}, nil
}

func (p *LambdaProvider) sendDurableCallbackFailure(callbackID string, _ *http.Request) (*plugin.Response, error) {
	_ = p.store.UpdateDurableExecutionStatus(callbackID, "FAILED")
	return &plugin.Response{
		StatusCode:  http.StatusOK,
		ContentType: "application/json",
		Body:        []byte("{}"),
	}, nil
}

func (p *LambdaProvider) sendDurableCallbackHeartbeat(callbackID string, _ *http.Request) (*plugin.Response, error) {
	_ = p.store.TouchDurableExecution(callbackID)
	return &plugin.Response{
		StatusCode:  http.StatusOK,
		ContentType: "application/json",
		Body:        []byte("{}"),
	}, nil
}

func (p *LambdaProvider) stopDurableExecution(executionArn string, _ *http.Request) (*plugin.Response, error) {
	_ = p.store.UpdateDurableExecutionStatus(executionArn, "STOPPED")
	out := map[string]any{
		"DurableExecutionArn": executionArn,
		"Status":              "STOPPED",
	}
	bytes, _ := json.Marshal(out)
	return &plugin.Response{
		StatusCode:  http.StatusOK,
		ContentType: "application/json",
		Body:        bytes,
	}, nil
}

// InvokeAsync handler
func (p *LambdaProvider) invokeAsync(name string, req *http.Request) (*plugin.Response, error) {
	// Verify function exists
	f, err := p.store.ResolveFunction(defaultAccountID, name, req.URL.Query().Get("Qualifier"))
	if err != nil {
		return invocationError(err, name), nil
	}

	if req.Body != nil {
		payload, _ := io.ReadAll(req.Body)
		_ = p.enqueueInvocation(f, payload)
	}

	out := map[string]any{
		"Status": 202,
	}
	bytes, _ := json.Marshal(out)
	return &plugin.Response{
		StatusCode:  http.StatusAccepted,
		ContentType: "application/json",
		Body:        bytes,
	}, nil
}

// InvokeWithResponseStream handler
func (p *LambdaProvider) invokeWithResponseStream(name string, req *http.Request) (*plugin.Response, error) {
	res, err := p.invoke(name, req)
	if err != nil {
		return nil, err
	}
	// Return streaming invocation response
	if res.Headers == nil {
		res.Headers = make(map[string]string)
	}
	res.Headers["Transfer-Encoding"] = "chunked"
	res.Headers["Content-Type"] = "application/vnd.amazon.eventstream"
	return res, nil
}
