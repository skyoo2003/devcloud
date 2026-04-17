// SPDX-License-Identifier: Apache-2.0

package lambda

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	"github.com/skyoo2003/devcloud/internal/plugin"
)

const (
	defaultAccountID = plugin.DefaultAccountID
	defaultRegion    = "us-east-1"
	lambdaAPIPrefix  = "/2015-03-31/functions"
	lambdaTagsPrefix = "/2017-03-31/tags/"
	lambdaESMPrefix  = "/2015-03-31/event-source-mappings"
)

// LambdaProvider implements plugin.ServicePlugin for the Lambda REST-JSON protocol.
type LambdaProvider struct {
	store   *LambdaStore
	runtime *Runtime
	poller  *EventSourcePoller
	cancel  context.CancelFunc
}

// ServiceID returns the unique identifier for this plugin.
func (p *LambdaProvider) ServiceID() string { return "lambda" }

// ServiceName returns the human-readable name for this plugin.
func (p *LambdaProvider) ServiceName() string { return "AWS Lambda" }

// Protocol returns the wire protocol used by this plugin.
// Lambda uses rest-json but we route by service ID, not protocol.
func (p *LambdaProvider) Protocol() plugin.ProtocolType { return plugin.ProtocolJSON11 }

// Init initialises the LambdaStore and Runtime.
func (p *LambdaProvider) Init(cfg plugin.PluginConfig) error {
	if err := os.MkdirAll(cfg.DataDir, 0o755); err != nil {
		return fmt.Errorf("init lambda: %w", err)
	}

	dbPath := filepath.Join(cfg.DataDir, "lambda.db")
	codeDir := filepath.Join(cfg.DataDir, "code")

	store, err := NewLambdaStore(dbPath, codeDir)
	if err != nil {
		return fmt.Errorf("init lambda store: %w", err)
	}

	p.store = store
	p.runtime = NewRuntime()

	// Start event source poller if a server port is configured.
	serverPort := 0
	if v, ok := cfg.Options["server_port"]; ok {
		switch port := v.(type) {
		case int:
			serverPort = port
		case int64:
			serverPort = int(port)
		}
	}
	if serverPort > 0 {
		p.poller = NewEventSourcePoller(store, serverPort)
		ctx, cancel := context.WithCancel(context.Background())
		p.cancel = cancel
		go p.poller.Start(ctx)
	}

	return nil
}

// Shutdown stops the event source poller and closes the underlying store.
func (p *LambdaProvider) Shutdown(_ context.Context) error {
	if p.cancel != nil {
		p.cancel()
	}
	if p.poller != nil {
		p.poller.Stop()
	}
	if p.store != nil {
		return p.store.Close()
	}
	return nil
}

// HandleRequest routes the incoming Lambda REST request to the appropriate handler.
func (p *LambdaProvider) HandleRequest(_ context.Context, _ string, req *http.Request) (*plugin.Response, error) {
	path := req.URL.Path

	// Route: tags API /2017-03-31/tags/{arn}
	if strings.HasPrefix(path, lambdaTagsPrefix) {
		rawARN := strings.TrimPrefix(path, lambdaTagsPrefix)
		arn, err := url.PathUnescape(rawARN)
		if err != nil {
			arn = rawARN
		}
		switch req.Method {
		case http.MethodGet:
			return p.listTags(arn)
		case http.MethodPost:
			return p.tagResource(arn, req)
		case http.MethodDelete:
			return p.untagResource(arn, req)
		}
	}

	// Route: event source mappings /2015-03-31/event-source-mappings[/{uuid}]
	if strings.HasPrefix(path, lambdaESMPrefix) {
		rest := strings.TrimPrefix(path, lambdaESMPrefix)
		rest = strings.TrimPrefix(rest, "/")
		switch {
		case req.Method == http.MethodPost && rest == "":
			return p.createEventSourceMapping(req)
		case req.Method == http.MethodGet && rest == "":
			return p.listEventSourceMappings(req)
		case req.Method == http.MethodGet && rest != "":
			return p.getEventSourceMapping(rest)
		case req.Method == http.MethodPut && rest != "":
			return p.updateEventSourceMapping(rest, req)
		case req.Method == http.MethodDelete && rest != "":
			return p.deleteEventSourceMapping(rest)
		}
	}

	sub := strings.TrimPrefix(path, lambdaAPIPrefix)
	sub = strings.TrimPrefix(sub, "/")

	switch {
	// POST /2015-03-31/functions  →  CreateFunction
	case req.Method == http.MethodPost && sub == "":
		return p.createFunction(req)

	// GET /2015-03-31/functions  →  ListFunctions
	case req.Method == http.MethodGet && sub == "":
		return p.listFunctions()

	// POST /2015-03-31/functions/{name}/versions  →  PublishVersion
	case req.Method == http.MethodPost && strings.HasSuffix(sub, "/versions"):
		name := strings.TrimSuffix(sub, "/versions")
		return p.publishVersion(name)

	// GET /2015-03-31/functions/{name}/versions  →  ListVersionsByFunction
	case req.Method == http.MethodGet && strings.HasSuffix(sub, "/versions"):
		name := strings.TrimSuffix(sub, "/versions")
		return p.listVersionsByFunction(name)

	// POST /2015-03-31/functions/{name}/aliases  →  CreateAlias
	case req.Method == http.MethodPost && strings.HasSuffix(sub, "/aliases"):
		name := strings.TrimSuffix(sub, "/aliases")
		return p.createAlias(name, req)

	// GET /2015-03-31/functions/{name}/aliases  →  ListAliases
	case req.Method == http.MethodGet && strings.HasSuffix(sub, "/aliases"):
		name := strings.TrimSuffix(sub, "/aliases")
		return p.listAliases(name)

	// GET/PUT/DELETE /2015-03-31/functions/{name}/aliases/{alias}
	case (req.Method == http.MethodGet || req.Method == http.MethodPut || req.Method == http.MethodDelete) &&
		strings.Contains(sub, "/aliases/"):
		parts := strings.SplitN(sub, "/aliases/", 2)
		if len(parts) == 2 {
			name, aliasName := parts[0], parts[1]
			switch req.Method {
			case http.MethodGet:
				return p.getAlias(name, aliasName)
			case http.MethodPut:
				return p.updateAlias(name, aliasName, req)
			case http.MethodDelete:
				return p.deleteAlias(name, aliasName)
			}
		}

	// POST /2015-03-31/functions/{name}/policy  →  AddPermission
	case req.Method == http.MethodPost && strings.HasSuffix(sub, "/policy"):
		name := strings.TrimSuffix(sub, "/policy")
		return p.addPermission(name, req)

	// GET /2015-03-31/functions/{name}/policy  →  GetPolicy
	case req.Method == http.MethodGet && strings.HasSuffix(sub, "/policy"):
		name := strings.TrimSuffix(sub, "/policy")
		return p.getPolicy(name)

	// DELETE /2015-03-31/functions/{name}/policy/{statementId}  →  RemovePermission
	case req.Method == http.MethodDelete && strings.Contains(sub, "/policy/"):
		parts := strings.SplitN(sub, "/policy/", 2)
		if len(parts) == 2 {
			return p.removePermission(parts[0], parts[1])
		}

	// GET /2015-03-31/functions/{name}  →  GetFunction
	case req.Method == http.MethodGet && !strings.Contains(sub, "/"):
		return p.getFunction(sub)

	// DELETE /2015-03-31/functions/{name}  →  DeleteFunction
	case req.Method == http.MethodDelete && !strings.Contains(sub, "/"):
		return p.deleteFunction(sub)

	// PUT /2015-03-31/functions/{name}/configuration  →  UpdateFunctionConfiguration
	case req.Method == http.MethodPut && strings.HasSuffix(sub, "/configuration"):
		name := strings.TrimSuffix(sub, "/configuration")
		return p.updateFunctionConfiguration(name, req)

	// PUT /2015-03-31/functions/{name}/code  →  UpdateFunctionCode
	case req.Method == http.MethodPut && strings.HasSuffix(sub, "/code"):
		name := strings.TrimSuffix(sub, "/code")
		return p.updateFunctionCode(name, req)

	// POST /2015-03-31/functions/{name}/invocations  →  Invoke
	case req.Method == http.MethodPost && strings.HasSuffix(sub, "/invocations"):
		name := strings.TrimSuffix(sub, "/invocations")
		return p.invoke(name, req)
	}

	return lambdaError("ResourceNotFoundException",
		fmt.Sprintf("Unknown operation on path %s", path), http.StatusNotFound), nil
}

// ListResources returns all Lambda functions as plugin resources.
func (p *LambdaProvider) ListResources(_ context.Context) ([]plugin.Resource, error) {
	functions, err := p.store.ListFunctions(defaultAccountID)
	if err != nil {
		return nil, err
	}
	resources := make([]plugin.Resource, 0, len(functions))
	for _, f := range functions {
		resources = append(resources, plugin.Resource{
			Type: "function",
			ID:   f.FunctionArn,
			Name: f.FunctionName,
		})
	}
	return resources, nil
}

// GetMetrics returns basic metrics for the Lambda service.
func (p *LambdaProvider) GetMetrics(_ context.Context) (*plugin.ServiceMetrics, error) {
	functions, err := p.store.ListFunctions(defaultAccountID)
	if err != nil {
		return nil, err
	}
	return &plugin.ServiceMetrics{ResourceCount: len(functions)}, nil
}

// --- operation implementations ---

// createFunctionRequest is the JSON body sent by boto3 for CreateFunction.
type createFunctionRequest struct {
	FunctionName string `json:"FunctionName"`
	Runtime      string `json:"Runtime"`
	Handler      string `json:"Handler"`
	Role         string `json:"Role"`
	Code         struct {
		ZipFile string `json:"ZipFile"` // base64-encoded
	} `json:"Code"`
	Description string `json:"Description"`
	Timeout     int    `json:"Timeout"`
	MemorySize  int    `json:"MemorySize"`
}

func (p *LambdaProvider) createFunction(req *http.Request) (*plugin.Response, error) {
	var body createFunctionRequest
	if err := decodeJSON(req, &body); err != nil {
		return lambdaError("InvalidParameterValueException", "invalid request body", http.StatusBadRequest), nil
	}

	if body.FunctionName == "" {
		return lambdaError("InvalidParameterValueException", "FunctionName is required", http.StatusBadRequest), nil
	}

	// Decode the zip bytes from base64.
	var codeZip []byte
	if body.Code.ZipFile != "" {
		var err error
		codeZip, err = base64.StdEncoding.DecodeString(body.Code.ZipFile)
		if err != nil {
			return lambdaError("InvalidParameterValueException", "Code.ZipFile must be base64-encoded", http.StatusBadRequest), nil
		}
	}

	// Apply defaults.
	timeout := body.Timeout
	if timeout <= 0 {
		timeout = 3
	}
	memorySize := body.MemorySize
	if memorySize <= 0 {
		memorySize = 128
	}

	info := &FunctionInfo{
		FunctionName: body.FunctionName,
		FunctionArn:  functionARN(body.FunctionName),
		Runtime:      body.Runtime,
		Handler:      body.Handler,
		Role:         body.Role,
		Description:  body.Description,
		Timeout:      timeout,
		MemorySize:   memorySize,
		AccountID:    defaultAccountID,
	}

	created, err := p.store.CreateFunction(info, codeZip)
	if err != nil {
		if errors.Is(err, ErrFunctionAlreadyExists) {
			return lambdaError("ResourceConflictException",
				fmt.Sprintf("Function already exists: %s", functionARN(body.FunctionName)),
				http.StatusConflict), nil
		}
		return nil, err
	}

	return jsonResp(http.StatusCreated, functionConfig(created))
}

func (p *LambdaProvider) listFunctions() (*plugin.Response, error) {
	functions, err := p.store.ListFunctions(defaultAccountID)
	if err != nil {
		return nil, err
	}

	configs := make([]map[string]any, 0, len(functions))
	for i := range functions {
		configs = append(configs, functionConfig(&functions[i]))
	}

	return jsonResp(http.StatusOK, map[string]any{"Functions": configs})
}

func (p *LambdaProvider) getFunction(name string) (*plugin.Response, error) {
	f, err := p.store.GetFunction(defaultAccountID, name)
	if err != nil {
		if errors.Is(err, ErrFunctionNotFound) {
			return notFoundError(name), nil
		}
		return nil, err
	}

	return jsonResp(http.StatusOK, map[string]any{
		"Configuration": functionConfig(f),
		"Code":          map[string]string{"Location": ""},
	})
}

func (p *LambdaProvider) deleteFunction(name string) (*plugin.Response, error) {
	err := p.store.DeleteFunction(defaultAccountID, name)
	if err != nil {
		if errors.Is(err, ErrFunctionNotFound) {
			return notFoundError(name), nil
		}
		return nil, err
	}
	return &plugin.Response{StatusCode: http.StatusNoContent}, nil
}

func (p *LambdaProvider) updateFunctionConfiguration(name string, req *http.Request) (*plugin.Response, error) {
	var body struct {
		Handler     string `json:"Handler"`
		Runtime     string `json:"Runtime"`
		Role        string `json:"Role"`
		Description string `json:"Description"`
		Timeout     int    `json:"Timeout"`
		MemorySize  int    `json:"MemorySize"`
	}
	if err := decodeJSON(req, &body); err != nil {
		return lambdaError("InvalidParameterValueException", "invalid request body", http.StatusBadRequest), nil
	}

	updated, err := p.store.UpdateFunctionConfiguration(defaultAccountID, name, body.Handler, body.Runtime, body.Role, body.Description, body.Timeout, body.MemorySize)
	if err != nil {
		if errors.Is(err, ErrFunctionNotFound) {
			return notFoundError(name), nil
		}
		return nil, err
	}

	return jsonResp(http.StatusOK, functionConfig(updated))
}

func (p *LambdaProvider) updateFunctionCode(name string, req *http.Request) (*plugin.Response, error) {
	var body struct {
		ZipFile string `json:"ZipFile"`
	}
	if err := decodeJSON(req, &body); err != nil {
		return lambdaError("InvalidParameterValueException", "invalid request body", http.StatusBadRequest), nil
	}

	codeZip, err := base64.StdEncoding.DecodeString(body.ZipFile)
	if err != nil {
		return lambdaError("InvalidParameterValueException", "ZipFile must be base64-encoded", http.StatusBadRequest), nil
	}

	updated, err := p.store.UpdateFunctionCode(defaultAccountID, name, codeZip)
	if err != nil {
		if errors.Is(err, ErrFunctionNotFound) {
			return notFoundError(name), nil
		}
		return nil, err
	}

	return jsonResp(http.StatusOK, functionConfig(updated))
}

func (p *LambdaProvider) invoke(name string, req *http.Request) (*plugin.Response, error) {
	f, err := p.store.GetFunction(defaultAccountID, name)
	if err != nil {
		if errors.Is(err, ErrFunctionNotFound) {
			return notFoundError(name), nil
		}
		return nil, err
	}

	payload, err := io.ReadAll(req.Body)
	if err != nil {
		return nil, err
	}

	result, err := p.runtime.Invoke(f, payload)
	if err != nil {
		return nil, err
	}

	resp := &plugin.Response{
		StatusCode:  result.StatusCode,
		ContentType: "application/json",
		Body:        result.Payload,
	}

	if result.Error != nil {
		resp.Headers = map[string]string{
			"X-Amz-Function-Error": result.Error.ErrorType,
		}
	}

	return resp, nil
}

// --- Task 24: Versions and Aliases ---

func (p *LambdaProvider) publishVersion(name string) (*plugin.Response, error) {
	v, err := p.store.PublishVersion(defaultAccountID, name)
	if err != nil {
		if errors.Is(err, ErrFunctionNotFound) {
			return notFoundError(name), nil
		}
		return nil, err
	}
	resp := make(map[string]any)
	for k, val := range v.Config {
		resp[k] = val
	}
	resp["Version"] = v.Version
	return jsonResp(http.StatusCreated, resp)
}

func (p *LambdaProvider) listVersionsByFunction(name string) (*plugin.Response, error) {
	versions, err := p.store.ListVersions(defaultAccountID, name)
	if err != nil {
		return nil, err
	}
	items := make([]map[string]any, 0, len(versions))
	for _, v := range versions {
		item := make(map[string]any)
		for k, val := range v.Config {
			item[k] = val
		}
		item["Version"] = v.Version
		items = append(items, item)
	}
	return jsonResp(http.StatusOK, map[string]any{"Versions": items})
}

func (p *LambdaProvider) createAlias(functionName string, req *http.Request) (*plugin.Response, error) {
	var body struct {
		Name            string `json:"Name"`
		FunctionVersion string `json:"FunctionVersion"`
		Description     string `json:"Description"`
	}
	if err := decodeJSON(req, &body); err != nil {
		return lambdaError("InvalidParameterValueException", "invalid request body", http.StatusBadRequest), nil
	}
	a, err := p.store.CreateAlias(defaultAccountID, functionName, body.Name, body.FunctionVersion)
	if err != nil {
		if errors.Is(err, ErrAliasAlreadyExists) {
			return lambdaError("ResourceConflictException", "Alias already exists", http.StatusConflict), nil
		}
		return nil, err
	}
	return jsonResp(http.StatusCreated, aliasResponse(a, functionName))
}

func (p *LambdaProvider) getAlias(functionName, aliasName string) (*plugin.Response, error) {
	a, err := p.store.GetAlias(defaultAccountID, functionName, aliasName)
	if err != nil {
		if errors.Is(err, ErrAliasNotFound) {
			return lambdaError("ResourceNotFoundException", fmt.Sprintf("Function not found: %s:%s", functionARN(functionName), aliasName), http.StatusNotFound), nil
		}
		return nil, err
	}
	return jsonResp(http.StatusOK, aliasResponse(a, functionName))
}

func (p *LambdaProvider) updateAlias(functionName, aliasName string, req *http.Request) (*plugin.Response, error) {
	var body struct {
		FunctionVersion string `json:"FunctionVersion"`
	}
	if err := decodeJSON(req, &body); err != nil {
		return lambdaError("InvalidParameterValueException", "invalid request body", http.StatusBadRequest), nil
	}
	a, err := p.store.UpdateAlias(defaultAccountID, functionName, aliasName, body.FunctionVersion)
	if err != nil {
		if errors.Is(err, ErrAliasNotFound) {
			return lambdaError("ResourceNotFoundException", "Alias not found", http.StatusNotFound), nil
		}
		return nil, err
	}
	return jsonResp(http.StatusOK, aliasResponse(a, functionName))
}

func (p *LambdaProvider) deleteAlias(functionName, aliasName string) (*plugin.Response, error) {
	err := p.store.DeleteAlias(defaultAccountID, functionName, aliasName)
	if err != nil {
		if errors.Is(err, ErrAliasNotFound) {
			return lambdaError("ResourceNotFoundException", "Alias not found", http.StatusNotFound), nil
		}
		return nil, err
	}
	return &plugin.Response{StatusCode: http.StatusNoContent}, nil
}

func (p *LambdaProvider) listAliases(functionName string) (*plugin.Response, error) {
	aliases, err := p.store.ListAliases(defaultAccountID, functionName)
	if err != nil {
		return nil, err
	}
	items := make([]map[string]any, 0, len(aliases))
	for i := range aliases {
		items = append(items, aliasResponse(&aliases[i], functionName))
	}
	return jsonResp(http.StatusOK, map[string]any{"Aliases": items})
}

func aliasResponse(a *FunctionAlias, functionName string) map[string]any {
	return map[string]any{
		"Name":            a.AliasName,
		"FunctionVersion": a.FunctionVersion,
		"AliasArn":        fmt.Sprintf("%s:%s", functionARN(functionName), a.AliasName),
	}
}

// --- Task 27: Event Source Mappings ---

func (p *LambdaProvider) createEventSourceMapping(req *http.Request) (*plugin.Response, error) {
	var body struct {
		FunctionName   string `json:"FunctionName"`
		EventSourceArn string `json:"EventSourceArn"`
		BatchSize      int    `json:"BatchSize"`
		Enabled        *bool  `json:"Enabled"`
	}
	if err := decodeJSON(req, &body); err != nil {
		return lambdaError("InvalidParameterValueException", "invalid request body", http.StatusBadRequest), nil
	}
	enabled := true
	if body.Enabled != nil {
		enabled = *body.Enabled
	}
	batchSize := body.BatchSize
	if batchSize <= 0 {
		batchSize = 10
	}
	id := newUUID()
	m := &EventSourceMapping{
		UUID:           id,
		FunctionName:   body.FunctionName,
		EventSourceARN: body.EventSourceArn,
		BatchSize:      batchSize,
		Enabled:        enabled,
		AccountID:      defaultAccountID,
		State:          "Enabled",
	}
	if err := p.store.CreateEventSourceMapping(m); err != nil {
		return nil, err
	}
	return jsonResp(http.StatusCreated, esmResponse(m))
}

func (p *LambdaProvider) getEventSourceMapping(uuid string) (*plugin.Response, error) {
	m, err := p.store.GetEventSourceMapping(uuid)
	if err != nil {
		if errors.Is(err, ErrMappingNotFound) {
			return lambdaError("ResourceNotFoundException", "Event source mapping not found", http.StatusNotFound), nil
		}
		return nil, err
	}
	return jsonResp(http.StatusOK, esmResponse(m))
}

func (p *LambdaProvider) updateEventSourceMapping(uuid string, req *http.Request) (*plugin.Response, error) {
	var body struct {
		BatchSize int   `json:"BatchSize"`
		Enabled   *bool `json:"Enabled"`
	}
	if err := decodeJSON(req, &body); err != nil {
		return lambdaError("InvalidParameterValueException", "invalid request body", http.StatusBadRequest), nil
	}
	m, err := p.store.UpdateEventSourceMapping(uuid, body.BatchSize, body.Enabled)
	if err != nil {
		if errors.Is(err, ErrMappingNotFound) {
			return lambdaError("ResourceNotFoundException", "Event source mapping not found", http.StatusNotFound), nil
		}
		return nil, err
	}
	return jsonResp(http.StatusOK, esmResponse(m))
}

func (p *LambdaProvider) deleteEventSourceMapping(uuid string) (*plugin.Response, error) {
	m, err := p.store.GetEventSourceMapping(uuid)
	if err != nil {
		if errors.Is(err, ErrMappingNotFound) {
			return lambdaError("ResourceNotFoundException", "Event source mapping not found", http.StatusNotFound), nil
		}
		return nil, err
	}
	if err := p.store.DeleteEventSourceMapping(uuid); err != nil {
		return nil, err
	}
	return jsonResp(http.StatusAccepted, esmResponse(m))
}

func (p *LambdaProvider) listEventSourceMappings(req *http.Request) (*plugin.Response, error) {
	functionName := req.URL.Query().Get("FunctionName")
	mappings, err := p.store.ListEventSourceMappings(defaultAccountID, functionName)
	if err != nil {
		return nil, err
	}
	items := make([]map[string]any, 0, len(mappings))
	for i := range mappings {
		items = append(items, esmResponse(&mappings[i]))
	}
	return jsonResp(http.StatusOK, map[string]any{"EventSourceMappings": items})
}

func esmResponse(m *EventSourceMapping) map[string]any {
	return map[string]any{
		"UUID":           m.UUID,
		"FunctionArn":    functionARN(m.FunctionName),
		"EventSourceArn": m.EventSourceARN,
		"BatchSize":      m.BatchSize,
		"State":          m.State,
		"LastModified":   m.CreatedAt.UTC().Format("2006-01-02T15:04:05.000+0000"),
	}
}

// --- Task 26: Tags ---

func (p *LambdaProvider) tagResource(arn string, req *http.Request) (*plugin.Response, error) {
	var body struct {
		Tags map[string]string `json:"Tags"`
	}
	if err := decodeJSON(req, &body); err != nil {
		return lambdaError("InvalidParameterValueException", "invalid request body", http.StatusBadRequest), nil
	}
	if err := p.store.TagResource(arn, body.Tags); err != nil {
		return nil, err
	}
	return &plugin.Response{StatusCode: http.StatusNoContent}, nil
}

func (p *LambdaProvider) untagResource(arn string, req *http.Request) (*plugin.Response, error) {
	tagKeys := req.URL.Query()["tagKeys"]
	if err := p.store.UntagResource(arn, tagKeys); err != nil {
		return nil, err
	}
	return &plugin.Response{StatusCode: http.StatusNoContent}, nil
}

func (p *LambdaProvider) listTags(arn string) (*plugin.Response, error) {
	tags, err := p.store.ListTags(arn)
	if err != nil {
		return nil, err
	}
	return jsonResp(http.StatusOK, map[string]any{"Tags": tags})
}

// --- Task 25: Permissions ---

func (p *LambdaProvider) addPermission(functionName string, req *http.Request) (*plugin.Response, error) {
	var body struct {
		StatementId string `json:"StatementId"`
		Action      string `json:"Action"`
		Principal   string `json:"Principal"`
		SourceArn   string `json:"SourceArn"`
	}
	if err := decodeJSON(req, &body); err != nil {
		return lambdaError("InvalidParameterValueException", "invalid request body", http.StatusBadRequest), nil
	}
	perm := &FunctionPermission{
		FunctionName: functionName,
		StatementID:  body.StatementId,
		Action:       body.Action,
		Principal:    body.Principal,
		SourceARN:    body.SourceArn,
		AccountID:    defaultAccountID,
	}
	if err := p.store.AddPermission(perm); err != nil {
		return nil, err
	}
	return jsonResp(http.StatusCreated, map[string]any{"Statement": body.StatementId})
}

func (p *LambdaProvider) removePermission(functionName, statementID string) (*plugin.Response, error) {
	err := p.store.RemovePermission(defaultAccountID, functionName, statementID)
	if err != nil {
		if errors.Is(err, ErrPermissionNotFound) {
			return lambdaError("ResourceNotFoundException", "Permission not found", http.StatusNotFound), nil
		}
		return nil, err
	}
	return &plugin.Response{StatusCode: http.StatusNoContent}, nil
}

func (p *LambdaProvider) getPolicy(functionName string) (*plugin.Response, error) {
	perms, err := p.store.GetPermissions(defaultAccountID, functionName)
	if err != nil {
		return nil, err
	}
	stmts := make([]map[string]any, 0, len(perms))
	for _, perm := range perms {
		stmt := map[string]any{
			"Sid":       perm.StatementID,
			"Action":    perm.Action,
			"Principal": map[string]string{"Service": perm.Principal},
			"Effect":    "Allow",
			"Resource":  functionARN(functionName),
		}
		if perm.SourceARN != "" {
			stmt["Condition"] = map[string]any{
				"ArnLike": map[string]string{"AWS:SourceArn": perm.SourceARN},
			}
		}
		stmts = append(stmts, stmt)
	}
	policy := map[string]any{
		"Version":   "2012-10-17",
		"Statement": stmts,
	}
	policyJSON, err := json.Marshal(policy)
	if err != nil {
		return nil, err
	}
	return jsonResp(http.StatusOK, map[string]any{
		"Policy":     string(policyJSON),
		"RevisionId": "1",
	})
}

// --- helpers ---

// functionARN builds a Lambda function ARN for the default region and account.
func functionARN(name string) string {
	return fmt.Sprintf("arn:aws:lambda:%s:%s:function:%s", defaultRegion, defaultAccountID, name)
}

// functionConfig converts a FunctionInfo into the JSON map returned by the API.
func functionConfig(f *FunctionInfo) map[string]any {
	return map[string]any{
		"FunctionName": f.FunctionName,
		"FunctionArn":  f.FunctionArn,
		"Runtime":      f.Runtime,
		"Handler":      f.Handler,
		"Role":         f.Role,
		"CodeSize":     f.CodeSize,
		"Description":  f.Description,
		"Timeout":      f.Timeout,
		"MemorySize":   f.MemorySize,
		"LastModified": f.LastModified.UTC().Format("2006-01-02T15:04:05.000+0000"),
	}
}

// notFoundError returns a 404 ResourceNotFoundException response.
func notFoundError(name string) *plugin.Response {
	return lambdaError("ResourceNotFoundException",
		fmt.Sprintf("Function not found: %s", functionARN(name)),
		http.StatusNotFound)
}

// lambdaError builds a JSON error response using the Lambda error envelope.
func lambdaError(errorType, message string, status int) *plugin.Response {
	body, _ := json.Marshal(map[string]string{
		"Message": message,
		"Type":    "User",
		"Code":    errorType,
	})
	return &plugin.Response{
		StatusCode:  status,
		ContentType: "application/json",
		Body:        body,
		Headers: map[string]string{
			"X-Amzn-Errortype": errorType,
		},
	}
}

// jsonResp marshals v into a JSON response with the given status code.
func jsonResp(status int, v any) (*plugin.Response, error) {
	body, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	return &plugin.Response{
		StatusCode:  status,
		ContentType: "application/json",
		Body:        body,
	}, nil
}

// decodeJSON reads and JSON-decodes the request body into v.
func decodeJSON(req *http.Request, v any) error {
	data, err := io.ReadAll(req.Body)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, v)
}

// newUUID generates a random UUID v4 string.
func newUUID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}
