// SPDX-License-Identifier: Apache-2.0

// internal/services/transfer/provider.go
package transfer

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"strings"

	"github.com/skyoo2003/devcloud/internal/plugin"
	"github.com/skyoo2003/devcloud/internal/shared"
)

type Provider struct {
	store *Store
}

func (p *Provider) ServiceID() string             { return "transfer" }
func (p *Provider) ServiceName() string           { return "TransferService" }
func (p *Provider) Protocol() plugin.ProtocolType { return plugin.ProtocolJSON11 }

func (p *Provider) Init(cfg plugin.PluginConfig) error {
	dataDir := cfg.DataDir
	if dataDir == "" {
		dataDir = "."
	}
	var err error
	p.store, err = NewStore(filepath.Join(dataDir, "transfer"))
	return err
}

func (p *Provider) Shutdown(_ context.Context) error {
	if p.store != nil {
		return p.store.Close()
	}
	return nil
}

func (p *Provider) HandleRequest(_ context.Context, op string, req *http.Request) (*plugin.Response, error) {
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

	action := op
	if action == "" {
		target := req.Header.Get("X-Amz-Target")
		if idx := strings.LastIndex(target, "."); idx >= 0 {
			action = target[idx+1:]
		}
	}

	switch action {
	// Server operations (not in generated interface — implemented via direct CRUD)
	case "CreateServer":
		return p.createServer(params)
	case "DescribeServer":
		return p.describeServer(params)
	case "ListServers":
		return p.listServers(params)
	case "UpdateServer":
		return p.updateServer(params)
	case "DeleteServer":
		return p.deleteServer(params)
	case "StartServer":
		return p.startServer(params)
	case "StopServer":
		return p.stopServer(params)
	// HostKey operations
	case "ImportHostKey":
		return p.importHostKey(params)
	case "DescribeHostKey":
		return p.describeHostKey(params)
	case "ListHostKeys":
		return p.listHostKeys(params)
	case "UpdateHostKey":
		return p.updateHostKeyOp(params)
	case "DeleteHostKey":
		return p.deleteHostKeyOp(params)
	// Access operations
	case "CreateAccess":
		return p.createAccess(params)
	case "DescribeAccess":
		return p.describeAccess(params)
	case "ListAccesses":
		return p.listAccesses(params)
	case "UpdateAccess":
		return p.updateAccessOp(params)
	case "DeleteAccess":
		return p.deleteAccessOp(params)
	// User operations (not in generated interface)
	case "CreateUser":
		return p.createUser(params)
	case "DescribeUser":
		return p.describeUser(params)
	case "ListUsers":
		return p.listUsers(params)
	case "UpdateUser":
		return p.updateUser(params)
	case "DeleteUser":
		return p.deleteUser(params)
	// SSH public key operations
	case "ImportSshPublicKey":
		return p.importSshPublicKey(params)
	case "DeleteSshPublicKey":
		return p.deleteSshPublicKey(params)
	// Tag operations
	case "TagResource":
		return p.tagResource(params)
	case "UntagResource":
		return p.untagResource(params)
	case "ListTagsForResource":
		return p.listTagsForResource(params)
	// Stub operations
	case "DescribeExecution":
		return p.describeExecution(params)
	case "DescribeSecurityPolicy":
		return p.describeSecurityPolicy(params)
	case "ListExecutions":
		return p.listExecutions(params)
	case "ListFileTransferResults":
		return p.listFileTransferResults(params)
	case "ListSecurityPolicies":
		return p.listSecurityPolicies(params)
	case "SendWorkflowStepState":
		return p.sendWorkflowStepState(params)
	case "StartDirectoryListing":
		return p.startDirectoryListing(params)
	case "StartFileTransfer":
		return p.startFileTransfer(params)
	case "StartRemoteDelete":
		return p.startRemoteDelete(params)
	case "StartRemoteMove":
		return p.startRemoteMove(params)
	case "TestConnection":
		return p.testConnection(params)
	case "TestIdentityProvider":
		return p.testIdentityProvider(params)
	default:
		return shared.JSONError("InvalidAction", fmt.Sprintf("unknown action: %s", action), http.StatusBadRequest), nil
	}
}

func (p *Provider) ListResources(_ context.Context) ([]plugin.Resource, error) {
	servers, err := p.store.ListServers()
	if err != nil {
		return nil, err
	}
	res := make([]plugin.Resource, 0, len(servers))
	for _, s := range servers {
		res = append(res, plugin.Resource{Type: "server", ID: s.ID, Name: s.ID})
	}
	return res, nil
}

func (p *Provider) GetMetrics(_ context.Context) (*plugin.ServiceMetrics, error) {
	return &plugin.ServiceMetrics{}, nil
}

// Server operations

func (p *Provider) createServer(params map[string]any) (*plugin.Response, error) {
	id := "s-" + shared.GenerateID("", 17)
	arn := shared.BuildARN("transfer", "server", id)

	protocols := `["SFTP"]`
	if ps, ok := params["Protocols"].([]any); ok && len(ps) > 0 {
		b, _ := json.Marshal(ps)
		protocols = string(b)
	}

	endpointType, _ := params["EndpointType"].(string)
	if endpointType == "" {
		endpointType = "PUBLIC"
	}

	identityProvider := "{}"
	if ip, ok := params["IdentityProviderDetails"]; ok {
		b, _ := json.Marshal(ip)
		identityProvider = string(b)
	}

	loggingRole, _ := params["LoggingRole"].(string)

	domain, _ := params["Domain"].(string)
	if domain == "" {
		domain = "S3"
	}

	srv, err := p.store.CreateServer(id, arn, protocols, endpointType, identityProvider, loggingRole, domain)
	if err != nil {
		return nil, err
	}

	if rawTags, ok := params["Tags"].([]any); ok {
		_ = p.store.tags.AddTags(srv.ARN, parseTags(rawTags))
	}

	return shared.JSONResponse(http.StatusOK, map[string]any{"ServerId": srv.ID})
}

func (p *Provider) describeServer(params map[string]any) (*plugin.Response, error) {
	serverID, _ := params["ServerId"].(string)
	if serverID == "" {
		return shared.JSONError("ValidationException", "ServerId is required", http.StatusBadRequest), nil
	}
	srv, err := p.store.GetServer(serverID)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "server not found", http.StatusBadRequest), nil
	}
	tags, _ := p.store.tags.ListTags(srv.ARN)
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"Server": serverToMap(srv, tagsToList(tags)),
	})
}

func (p *Provider) listServers(_ map[string]any) (*plugin.Response, error) {
	servers, err := p.store.ListServers()
	if err != nil {
		return nil, err
	}
	list := make([]map[string]any, 0, len(servers))
	for _, srv := range servers {
		list = append(list, map[string]any{
			"Arn":                  srv.ARN,
			"Domain":               srv.Domain,
			"EndpointType":         srv.EndpointType,
			"IdentityProviderType": "SERVICE_MANAGED",
			"LoggingRole":          srv.LoggingRole,
			"ServerId":             srv.ID,
			"State":                srv.State,
			"UserCount":            0,
		})
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"Servers":   list,
		"NextToken": "",
	})
}

func (p *Provider) updateServer(params map[string]any) (*plugin.Response, error) {
	serverID, _ := params["ServerId"].(string)
	if serverID == "" {
		return shared.JSONError("ValidationException", "ServerId is required", http.StatusBadRequest), nil
	}
	if _, err := p.store.GetServer(serverID); err != nil {
		return shared.JSONError("ResourceNotFoundException", "server not found", http.StatusBadRequest), nil
	}
	// For simplicity, just return success (server fields stored are minimal)
	return shared.JSONResponse(http.StatusOK, map[string]any{"ServerId": serverID})
}

func (p *Provider) deleteServer(params map[string]any) (*plugin.Response, error) {
	serverID, _ := params["ServerId"].(string)
	if serverID == "" {
		return shared.JSONError("ValidationException", "ServerId is required", http.StatusBadRequest), nil
	}
	srv, err := p.store.GetServer(serverID)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "server not found", http.StatusBadRequest), nil
	}
	_ = p.store.tags.DeleteAllTags(srv.ARN)
	if err := p.store.DeleteServer(serverID); err != nil {
		return shared.JSONError("ResourceNotFoundException", "server not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) startServer(params map[string]any) (*plugin.Response, error) {
	serverID, _ := params["ServerId"].(string)
	if serverID == "" {
		return shared.JSONError("ValidationException", "ServerId is required", http.StatusBadRequest), nil
	}
	if err := p.store.UpdateServerState(serverID, "ONLINE"); err != nil {
		return shared.JSONError("ResourceNotFoundException", "server not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) stopServer(params map[string]any) (*plugin.Response, error) {
	serverID, _ := params["ServerId"].(string)
	if serverID == "" {
		return shared.JSONError("ValidationException", "ServerId is required", http.StatusBadRequest), nil
	}
	if err := p.store.UpdateServerState(serverID, "OFFLINE"); err != nil {
		return shared.JSONError("ResourceNotFoundException", "server not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

// HostKey operations

func (p *Provider) importHostKey(params map[string]any) (*plugin.Response, error) {
	serverID, _ := params["ServerId"].(string)
	if serverID == "" {
		return shared.JSONError("ValidationException", "ServerId is required", http.StatusBadRequest), nil
	}
	if _, err := p.store.GetServer(serverID); err != nil {
		return shared.JSONError("ResourceNotFoundException", "server not found", http.StatusBadRequest), nil
	}

	description, _ := params["Description"].(string)
	hostKeyBody, _ := params["HostKeyBody"].(string)

	// Derive key type from body
	keyType := "ssh-rsa"
	if strings.HasPrefix(hostKeyBody, "ecdsa-") {
		keyType = "ecdsa"
	} else if strings.HasPrefix(hostKeyBody, "ssh-ed25519") {
		keyType = "ssh-ed25519"
	}

	id := shared.GenerateID("hostkey-", 20)
	arn := shared.BuildARN("transfer", "host-key/"+serverID, id)

	hk, err := p.store.CreateHostKey(id, arn, serverID, description, "SHA256:"+shared.GenerateID("", 43), keyType)
	if err != nil {
		return nil, err
	}

	if rawTags, ok := params["Tags"].([]any); ok {
		_ = p.store.tags.AddTags(hk.ARN, parseTags(rawTags))
	}

	return shared.JSONResponse(http.StatusOK, map[string]any{
		"HostKeyId": hk.ID,
		"ServerId":  serverID,
	})
}

func (p *Provider) describeHostKey(params map[string]any) (*plugin.Response, error) {
	serverID, _ := params["ServerId"].(string)
	hostKeyID, _ := params["HostKeyId"].(string)
	if serverID == "" || hostKeyID == "" {
		return shared.JSONError("ValidationException", "ServerId and HostKeyId are required", http.StatusBadRequest), nil
	}
	hk, err := p.store.GetHostKey(hostKeyID, serverID)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "host key not found", http.StatusBadRequest), nil
	}
	tags, _ := p.store.tags.ListTags(hk.ARN)
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"HostKey": map[string]any{
			"Arn":                hk.ARN,
			"DateImported":       hk.CreatedAt,
			"Description":        hk.Description,
			"HostKeyFingerprint": hk.Fingerprint,
			"HostKeyId":          hk.ID,
			"Type":               hk.Type,
			"Tags":               tagsToList(tags),
		},
	})
}

func (p *Provider) listHostKeys(params map[string]any) (*plugin.Response, error) {
	serverID, _ := params["ServerId"].(string)
	if serverID == "" {
		return shared.JSONError("ValidationException", "ServerId is required", http.StatusBadRequest), nil
	}
	hostKeys, err := p.store.ListHostKeys(serverID)
	if err != nil {
		return nil, err
	}
	list := make([]map[string]any, 0, len(hostKeys))
	for _, hk := range hostKeys {
		list = append(list, map[string]any{
			"Arn":          hk.ARN,
			"DateImported": hk.CreatedAt,
			"Description":  hk.Description,
			"Fingerprint":  hk.Fingerprint,
			"HostKeyId":    hk.ID,
			"Type":         hk.Type,
		})
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"HostKeys":  list,
		"ServerId":  serverID,
		"NextToken": "",
	})
}

func (p *Provider) updateHostKeyOp(params map[string]any) (*plugin.Response, error) {
	serverID, _ := params["ServerId"].(string)
	hostKeyID, _ := params["HostKeyId"].(string)
	if serverID == "" || hostKeyID == "" {
		return shared.JSONError("ValidationException", "ServerId and HostKeyId are required", http.StatusBadRequest), nil
	}
	description, _ := params["Description"].(string)
	if err := p.store.UpdateHostKey(hostKeyID, serverID, description); err != nil {
		return shared.JSONError("ResourceNotFoundException", "host key not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"HostKeyId": hostKeyID,
		"ServerId":  serverID,
	})
}

func (p *Provider) deleteHostKeyOp(params map[string]any) (*plugin.Response, error) {
	serverID, _ := params["ServerId"].(string)
	hostKeyID, _ := params["HostKeyId"].(string)
	if serverID == "" || hostKeyID == "" {
		return shared.JSONError("ValidationException", "ServerId and HostKeyId are required", http.StatusBadRequest), nil
	}
	hk, err := p.store.GetHostKey(hostKeyID, serverID)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "host key not found", http.StatusBadRequest), nil
	}
	_ = p.store.tags.DeleteAllTags(hk.ARN)
	if err := p.store.DeleteHostKey(hostKeyID, serverID); err != nil {
		return shared.JSONError("ResourceNotFoundException", "host key not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

// Access operations

func (p *Provider) createAccess(params map[string]any) (*plugin.Response, error) {
	serverID, _ := params["ServerId"].(string)
	externalID, _ := params["ExternalId"].(string)
	if serverID == "" || externalID == "" {
		return shared.JSONError("ValidationException", "ServerId and ExternalId are required", http.StatusBadRequest), nil
	}
	if _, err := p.store.GetServer(serverID); err != nil {
		return shared.JSONError("ResourceNotFoundException", "server not found", http.StatusBadRequest), nil
	}

	role, _ := params["Role"].(string)
	homeDirectory, _ := params["HomeDirectory"].(string)
	if homeDirectory == "" {
		homeDirectory = "/"
	}
	policy, _ := params["Policy"].(string)

	if _, err := p.store.CreateAccess(serverID, externalID, role, homeDirectory, policy); err != nil {
		if isUnique(err) {
			return shared.JSONError("ResourceExistsException", "access already exists", http.StatusBadRequest), nil
		}
		return nil, err
	}

	return shared.JSONResponse(http.StatusOK, map[string]any{
		"ExternalId": externalID,
		"ServerId":   serverID,
	})
}

func (p *Provider) describeAccess(params map[string]any) (*plugin.Response, error) {
	serverID, _ := params["ServerId"].(string)
	externalID, _ := params["ExternalId"].(string)
	if serverID == "" || externalID == "" {
		return shared.JSONError("ValidationException", "ServerId and ExternalId are required", http.StatusBadRequest), nil
	}
	access, err := p.store.GetAccess(serverID, externalID)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "access not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"ServerId": serverID,
		"Access": map[string]any{
			"ExternalId":    access.ExternalID,
			"HomeDirectory": access.HomeDirectory,
			"Role":          access.Role,
			"Policy":        access.Policy,
		},
	})
}

func (p *Provider) listAccesses(params map[string]any) (*plugin.Response, error) {
	serverID, _ := params["ServerId"].(string)
	if serverID == "" {
		return shared.JSONError("ValidationException", "ServerId is required", http.StatusBadRequest), nil
	}
	accesses, err := p.store.ListAccesses(serverID)
	if err != nil {
		return nil, err
	}
	list := make([]map[string]any, 0, len(accesses))
	for _, a := range accesses {
		list = append(list, map[string]any{
			"ExternalId":        a.ExternalID,
			"HomeDirectory":     a.HomeDirectory,
			"HomeDirectoryType": "PATH",
			"Role":              a.Role,
		})
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"Accesses":  list,
		"ServerId":  serverID,
		"NextToken": "",
	})
}

func (p *Provider) updateAccessOp(params map[string]any) (*plugin.Response, error) {
	serverID, _ := params["ServerId"].(string)
	externalID, _ := params["ExternalId"].(string)
	if serverID == "" || externalID == "" {
		return shared.JSONError("ValidationException", "ServerId and ExternalId are required", http.StatusBadRequest), nil
	}
	access, err := p.store.GetAccess(serverID, externalID)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "access not found", http.StatusBadRequest), nil
	}

	role := access.Role
	homeDirectory := access.HomeDirectory
	policy := access.Policy

	if r, ok := params["Role"].(string); ok && r != "" {
		role = r
	}
	if h, ok := params["HomeDirectory"].(string); ok && h != "" {
		homeDirectory = h
	}
	if pol, ok := params["Policy"].(string); ok {
		policy = pol
	}

	if err := p.store.UpdateAccess(serverID, externalID, role, homeDirectory, policy); err != nil {
		return shared.JSONError("ResourceNotFoundException", "access not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"ExternalId": externalID,
		"ServerId":   serverID,
	})
}

func (p *Provider) deleteAccessOp(params map[string]any) (*plugin.Response, error) {
	serverID, _ := params["ServerId"].(string)
	externalID, _ := params["ExternalId"].(string)
	if serverID == "" || externalID == "" {
		return shared.JSONError("ValidationException", "ServerId and ExternalId are required", http.StatusBadRequest), nil
	}
	if err := p.store.DeleteAccess(serverID, externalID); err != nil {
		return shared.JSONError("ResourceNotFoundException", "access not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

// User operations (stub - not in generated interface)

func (p *Provider) createUser(params map[string]any) (*plugin.Response, error) {
	serverID, _ := params["ServerId"].(string)
	userName, _ := params["UserName"].(string)
	if serverID == "" || userName == "" {
		return shared.JSONError("ValidationException", "ServerId and UserName are required", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"ServerId": serverID,
		"UserName": userName,
	})
}

func (p *Provider) describeUser(params map[string]any) (*plugin.Response, error) {
	serverID, _ := params["ServerId"].(string)
	userName, _ := params["UserName"].(string)
	if serverID == "" || userName == "" {
		return shared.JSONError("ValidationException", "ServerId and UserName are required", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"ServerId": serverID,
		"User": map[string]any{
			"UserName":      userName,
			"HomeDirectory": "/",
			"Role":          "",
			"SshPublicKeys": []any{},
		},
	})
}

func (p *Provider) listUsers(params map[string]any) (*plugin.Response, error) {
	serverID, _ := params["ServerId"].(string)
	if serverID == "" {
		return shared.JSONError("ValidationException", "ServerId is required", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"ServerId":  serverID,
		"Users":     []any{},
		"NextToken": "",
	})
}

func (p *Provider) updateUser(params map[string]any) (*plugin.Response, error) {
	serverID, _ := params["ServerId"].(string)
	userName, _ := params["UserName"].(string)
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"ServerId": serverID,
		"UserName": userName,
	})
}

func (p *Provider) deleteUser(params map[string]any) (*plugin.Response, error) {
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

// SSH public key operations (stub)

func (p *Provider) importSshPublicKey(params map[string]any) (*plugin.Response, error) {
	serverID, _ := params["ServerId"].(string)
	userName, _ := params["UserName"].(string)
	keyID := shared.GenerateID("key-", 20)
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"ServerId":       serverID,
		"SshPublicKeyId": keyID,
		"UserName":       userName,
	})
}

func (p *Provider) deleteSshPublicKey(_ map[string]any) (*plugin.Response, error) {
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

// Tag operations

func (p *Provider) tagResource(params map[string]any) (*plugin.Response, error) {
	arn, _ := params["Arn"].(string)
	if arn == "" {
		return shared.JSONError("ValidationException", "Arn is required", http.StatusBadRequest), nil
	}
	rawTags, _ := params["Tags"].([]any)
	if err := p.store.tags.AddTags(arn, parseTags(rawTags)); err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) untagResource(params map[string]any) (*plugin.Response, error) {
	arn, _ := params["Arn"].(string)
	if arn == "" {
		return shared.JSONError("ValidationException", "Arn is required", http.StatusBadRequest), nil
	}
	rawKeys, _ := params["TagKeys"].([]any)
	keys := make([]string, 0, len(rawKeys))
	for _, k := range rawKeys {
		if s, ok := k.(string); ok {
			keys = append(keys, s)
		}
	}
	if err := p.store.tags.RemoveTags(arn, keys); err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) listTagsForResource(params map[string]any) (*plugin.Response, error) {
	arn, _ := params["Arn"].(string)
	if arn == "" {
		return shared.JSONError("ValidationException", "Arn is required", http.StatusBadRequest), nil
	}
	tags, err := p.store.tags.ListTags(arn)
	if err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"Arn":       arn,
		"Tags":      tagsToList(tags),
		"NextToken": "",
	})
}

// Stub operations

func (p *Provider) describeExecution(_ map[string]any) (*plugin.Response, error) {
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"WorkflowId": "",
		"Execution": map[string]any{
			"ExecutionId": "",
			"Status":      "COMPLETED",
		},
	})
}

func (p *Provider) describeSecurityPolicy(params map[string]any) (*plugin.Response, error) {
	name, _ := params["SecurityPolicyName"].(string)
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"SecurityPolicy": map[string]any{
			"SecurityPolicyName": name,
			"Fips":               false,
		},
	})
}

func (p *Provider) listExecutions(_ map[string]any) (*plugin.Response, error) {
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"Executions": []any{},
		"NextToken":  "",
		"WorkflowId": "",
	})
}

func (p *Provider) listFileTransferResults(_ map[string]any) (*plugin.Response, error) {
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"FileTransferResults": []any{},
		"NextToken":           "",
	})
}

func (p *Provider) listSecurityPolicies(_ map[string]any) (*plugin.Response, error) {
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"SecurityPolicyNames": []any{"TransferSecurityPolicy-2024-01"},
		"NextToken":           "",
	})
}

func (p *Provider) sendWorkflowStepState(_ map[string]any) (*plugin.Response, error) {
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) startDirectoryListing(_ map[string]any) (*plugin.Response, error) {
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"ListingId":      shared.GenerateUUID(),
		"OutputFileName": "listing.json",
	})
}

func (p *Provider) startFileTransfer(_ map[string]any) (*plugin.Response, error) {
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"TransferId": shared.GenerateUUID(),
	})
}

func (p *Provider) startRemoteDelete(_ map[string]any) (*plugin.Response, error) {
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"ListingId":      shared.GenerateUUID(),
		"OutputFileName": "delete.json",
	})
}

func (p *Provider) startRemoteMove(_ map[string]any) (*plugin.Response, error) {
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"ListingId":      shared.GenerateUUID(),
		"OutputFileName": "move.json",
	})
}

func (p *Provider) testConnection(_ map[string]any) (*plugin.Response, error) {
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"ConnectorId":   "",
		"StatusCode":    200,
		"StatusMessage": "OK",
	})
}

func (p *Provider) testIdentityProvider(params map[string]any) (*plugin.Response, error) {
	serverID, _ := params["ServerId"].(string)
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"ServerId":      serverID,
		"StatusCode":    200,
		"StatusMessage": "OK",
		"Url":           "",
	})
}

// Helpers

func serverToMap(srv *Server, tags []map[string]string) map[string]any {
	var protocols any
	_ = json.Unmarshal([]byte(srv.Protocols), &protocols)
	var identityProvider any
	_ = json.Unmarshal([]byte(srv.IdentityProvider), &identityProvider)
	return map[string]any{
		"Arn":                  srv.ARN,
		"Domain":               srv.Domain,
		"EndpointType":         srv.EndpointType,
		"IdentityProviderType": "SERVICE_MANAGED",
		"LoggingRole":          srv.LoggingRole,
		"Protocols":            protocols,
		"ServerId":             srv.ID,
		"State":                srv.State,
		"Tags":                 tags,
		"UserCount":            0,
	}
}

func tagsToList(tags map[string]string) []map[string]string {
	list := make([]map[string]string, 0, len(tags))
	for k, v := range tags {
		list = append(list, map[string]string{"Key": k, "Value": v})
	}
	return list
}

func parseTags(rawTags []any) map[string]string {
	tags := make(map[string]string)
	for _, t := range rawTags {
		tag, _ := t.(map[string]any)
		k, _ := tag["Key"].(string)
		v, _ := tag["Value"].(string)
		if k != "" {
			tags[k] = v
		}
	}
	return tags
}

func isUnique(err error) bool {
	return err != nil && strings.Contains(err.Error(), "UNIQUE constraint failed")
}
