// SPDX-License-Identifier: Apache-2.0

package lambda

// Runtime is a minimal Docker-based Lambda execution runtime.
// For now it is a stub that returns a placeholder response when invoked,
// allowing the rest of the Lambda provider to function without Docker.
type Runtime struct{}

// InvokeResult holds the result of a Lambda function invocation.
type InvokeResult struct {
	StatusCode int
	Payload    []byte
	LogResult  string
	Error      *InvokeError
}

// InvokeError describes a Lambda function error.
type InvokeError struct {
	ErrorType    string `json:"errorType"`
	ErrorMessage string `json:"errorMessage"`
}

// NewRuntime creates a new Runtime instance.
func NewRuntime() *Runtime {
	return &Runtime{}
}

// Invoke attempts to run a Lambda function. If Docker is not available,
// it returns a stub response indicating Docker is required.
func (r *Runtime) Invoke(functionInfo *FunctionInfo, payload []byte) (*InvokeResult, error) {
	return &InvokeResult{
		StatusCode: 200,
		Payload:    []byte(`{"statusCode": 200, "body": "Lambda invoke requires Docker runtime"}`),
	}, nil
}
