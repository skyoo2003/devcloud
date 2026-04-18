// SPDX-License-Identifier: Apache-2.0

// internal/services/ssm/provider.go
package ssm

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"strings"

	"github.com/skyoo2003/devcloud/internal/plugin"
)

const defaultAccountID = plugin.DefaultAccountID

type Provider struct {
	store *SSMStore
}

func (p *Provider) ServiceID() string             { return "ssm" }
func (p *Provider) ServiceName() string           { return "AmazonSSM" }
func (p *Provider) Protocol() plugin.ProtocolType { return plugin.ProtocolJSON11 }

func (p *Provider) Init(cfg plugin.PluginConfig) error {
	dataDir := cfg.DataDir
	if dataDir == "" {
		dataDir = "."
	}
	var err error
	p.store, err = NewSSMStore(filepath.Join(dataDir, "ssm"))
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
		return ssmError("InvalidParameterException", "failed to read body", http.StatusBadRequest), nil
	}
	var params map[string]any
	if len(body) > 0 {
		if err := json.Unmarshal(body, &params); err != nil {
			return ssmError("InvalidParameterException", "invalid JSON", http.StatusBadRequest), nil
		}
	} else {
		params = map[string]any{}
	}

	action := op
	if action == "" {
		target := req.Header.Get("X-Amz-Target")
		if idx := strings.LastIndex(target, "."); idx >= 0 {
			action = target[idx+1:]
		} else {
			action = target
		}
	}

	switch action {
	// Parameter operations
	case "PutParameter":
		return p.putParameter(params)
	case "GetParameter":
		return p.getParameter(params)
	case "GetParameters":
		return p.getParameters(params)
	case "GetParametersByPath":
		return p.getParametersByPath(params)
	case "DeleteParameter":
		return p.deleteParameter(params)
	case "DescribeParameters":
		return p.describeParameters(params)
	case "ListTagsForResource":
		return p.listTagsForResource(params)
	// Parameter enhancements
	case "AddTagsToResource":
		return p.addTagsToResource(params)
	case "RemoveTagsFromResource":
		return p.removeTagsFromResource(params)
	case "LabelParameterVersion":
		return p.labelParameterVersion(params)
	case "GetParameterHistory":
		return p.getParameterHistory(params)
	// Document operations
	case "CreateDocument":
		return p.createDocument(params)
	case "UpdateDocument":
		return p.updateDocument(params)
	case "DeleteDocument":
		return p.deleteDocument(params)
	case "DescribeDocument":
		return p.describeDocument(params)
	case "GetDocument":
		return p.getDocumentContent(params)
	case "ListDocuments":
		return p.listDocuments(params)
	// Session stubs
	case "StartSession":
		return p.startSession(params)
	case "TerminateSession":
		return p.terminateSession(params)
	default:
		return ssmError("InvalidAction", fmt.Sprintf("unknown action: %s", action), http.StatusBadRequest), nil
	}
}

func (p *Provider) ListResources(_ context.Context) ([]plugin.Resource, error) {
	params, err := p.store.DescribeParameters(defaultAccountID)
	if err != nil {
		return nil, err
	}
	resources := make([]plugin.Resource, 0, len(params))
	for _, param := range params {
		resources = append(resources, plugin.Resource{Type: "parameter", ID: param.Name, Name: param.Name})
	}
	return resources, nil
}

func (p *Provider) GetMetrics(_ context.Context) (*plugin.ServiceMetrics, error) {
	return &plugin.ServiceMetrics{}, nil
}

func (p *Provider) putParameter(params map[string]any) (*plugin.Response, error) {
	name, _ := params["Name"].(string)
	value, _ := params["Value"].(string)
	if name == "" || value == "" {
		return ssmError("InvalidParameterException", "Name and Value are required", http.StatusBadRequest), nil
	}
	paramType, _ := params["Type"].(string)
	if paramType == "" {
		paramType = "String"
	}
	description, _ := params["Description"].(string)
	overwrite := false
	if ow, ok := params["Overwrite"].(bool); ok {
		overwrite = ow
	}
	param, err := p.store.PutParameter(name, defaultAccountID, paramType, value, description, overwrite)
	if err != nil {
		if err == ErrParameterExists {
			return ssmError("ParameterAlreadyExists", "parameter already exists, use Overwrite=true", http.StatusBadRequest), nil
		}
		return nil, err
	}
	// Handle Tags if provided.
	if tagsRaw, ok := params["Tags"].([]any); ok && len(tagsRaw) > 0 {
		for _, t := range tagsRaw {
			if m, ok := t.(map[string]any); ok {
				key, _ := m["Key"].(string)
				val, _ := m["Value"].(string)
				if key != "" {
					if err := p.store.PutTag(name, defaultAccountID, key, val); err != nil {
						return nil, err
					}
				}
			}
		}
	}
	return jsonResp(http.StatusOK, map[string]any{"Version": param.Version})
}

func (p *Provider) getParameter(params map[string]any) (*plugin.Response, error) {
	name, _ := params["Name"].(string)
	if name == "" {
		return ssmError("InvalidParameterException", "Name is required", http.StatusBadRequest), nil
	}
	param, err := p.store.GetParameter(name, defaultAccountID)
	if err != nil {
		return ssmError("ParameterNotFound", fmt.Sprintf("parameter not found: %s", name), http.StatusBadRequest), nil
	}
	return jsonResp(http.StatusOK, map[string]any{
		"Parameter": paramToMap(param),
	})
}

func (p *Provider) getParameters(params map[string]any) (*plugin.Response, error) {
	namesRaw, ok := params["Names"].([]any)
	if !ok {
		return ssmError("InvalidParameterException", "Names is required", http.StatusBadRequest), nil
	}
	names := make([]string, 0, len(namesRaw))
	for _, n := range namesRaw {
		if s, ok := n.(string); ok {
			names = append(names, s)
		}
	}
	found, invalid := p.store.GetParameters(names, defaultAccountID)
	paramList := make([]map[string]any, 0, len(found))
	for _, param := range found {
		p2 := param
		paramList = append(paramList, paramToMap(&p2))
	}
	return jsonResp(http.StatusOK, map[string]any{
		"Parameters":        paramList,
		"InvalidParameters": invalid,
	})
}

func (p *Provider) getParametersByPath(params map[string]any) (*plugin.Response, error) {
	path, _ := params["Path"].(string)
	if path == "" {
		return ssmError("InvalidParameterException", "Path is required", http.StatusBadRequest), nil
	}
	recursive := false
	if r, ok := params["Recursive"].(bool); ok {
		recursive = r
	}
	paramList, err := p.store.GetParametersByPath(path, defaultAccountID, recursive)
	if err != nil {
		return nil, err
	}
	list := make([]map[string]any, 0, len(paramList))
	for _, param := range paramList {
		p2 := param
		list = append(list, paramToMap(&p2))
	}
	return jsonResp(http.StatusOK, map[string]any{"Parameters": list})
}

func (p *Provider) deleteParameter(params map[string]any) (*plugin.Response, error) {
	name, _ := params["Name"].(string)
	if name == "" {
		return ssmError("InvalidParameterException", "Name is required", http.StatusBadRequest), nil
	}
	if err := p.store.DeleteParameter(name, defaultAccountID); err != nil {
		return ssmError("ParameterNotFound", "parameter not found", http.StatusBadRequest), nil
	}
	return jsonResp(http.StatusOK, map[string]any{})
}

func (p *Provider) describeParameters(_ map[string]any) (*plugin.Response, error) {
	paramList, err := p.store.DescribeParameters(defaultAccountID)
	if err != nil {
		return nil, err
	}
	list := make([]map[string]any, 0, len(paramList))
	for _, param := range paramList {
		p2 := param
		list = append(list, map[string]any{
			"Name":             p2.Name,
			"Type":             p2.Type,
			"Description":      p2.Description,
			"Version":          p2.Version,
			"LastModifiedDate": p2.LastModified.Unix(),
		})
	}
	return jsonResp(http.StatusOK, map[string]any{"Parameters": list})
}

func (p *Provider) listTagsForResource(params map[string]any) (*plugin.Response, error) {
	resourceID, _ := params["ResourceId"].(string)
	if resourceID == "" {
		return ssmError("InvalidParameterException", "ResourceId is required", http.StatusBadRequest), nil
	}
	tags, err := p.store.ListTags(resourceID, defaultAccountID)
	if err != nil {
		return nil, err
	}
	tagList := make([]map[string]string, 0, len(tags))
	for _, t := range tags {
		tagList = append(tagList, map[string]string{"Key": t.Key, "Value": t.Value})
	}
	return jsonResp(http.StatusOK, map[string]any{"TagList": tagList})
}

// addTagsToResource adds tags to a parameter (ResourceType=Parameter).
func (p *Provider) addTagsToResource(params map[string]any) (*plugin.Response, error) {
	resourceID, _ := params["ResourceId"].(string)
	if resourceID == "" {
		return ssmError("InvalidParameterException", "ResourceId is required", http.StatusBadRequest), nil
	}
	tagsRaw, ok := params["Tags"].([]any)
	if !ok {
		return ssmError("InvalidParameterException", "Tags is required", http.StatusBadRequest), nil
	}
	for _, t := range tagsRaw {
		if m, ok := t.(map[string]any); ok {
			key, _ := m["Key"].(string)
			val, _ := m["Value"].(string)
			if key != "" {
				if err := p.store.PutTag(resourceID, defaultAccountID, key, val); err != nil {
					return nil, err
				}
			}
		}
	}
	return jsonResp(http.StatusOK, map[string]any{})
}

// removeTagsFromResource removes tag keys from a parameter.
func (p *Provider) removeTagsFromResource(params map[string]any) (*plugin.Response, error) {
	resourceID, _ := params["ResourceId"].(string)
	if resourceID == "" {
		return ssmError("InvalidParameterException", "ResourceId is required", http.StatusBadRequest), nil
	}
	tagKeysRaw, ok := params["TagKeys"].([]any)
	if !ok {
		return ssmError("InvalidParameterException", "TagKeys is required", http.StatusBadRequest), nil
	}
	for _, k := range tagKeysRaw {
		if key, ok := k.(string); ok && key != "" {
			if err := p.store.DeleteTag(resourceID, defaultAccountID, key); err != nil {
				return nil, err
			}
		}
	}
	return jsonResp(http.StatusOK, map[string]any{})
}

// labelParameterVersion labels a specific version of a parameter.
func (p *Provider) labelParameterVersion(params map[string]any) (*plugin.Response, error) {
	name, _ := params["Name"].(string)
	if name == "" {
		return ssmError("InvalidParameterException", "Name is required", http.StatusBadRequest), nil
	}
	labelsRaw, ok := params["Labels"].([]any)
	if !ok {
		return ssmError("InvalidParameterException", "Labels is required", http.StatusBadRequest), nil
	}
	labels := make([]string, 0, len(labelsRaw))
	for _, l := range labelsRaw {
		if s, ok := l.(string); ok && s != "" {
			labels = append(labels, s)
		}
	}
	// ParameterVersion is optional; default to current version.
	version := 0
	if v, ok := params["ParameterVersion"].(float64); ok {
		version = int(v)
	}
	if version == 0 {
		param, err := p.store.GetParameter(name, defaultAccountID)
		if err != nil {
			return ssmError("ParameterNotFound", fmt.Sprintf("parameter not found: %s", name), http.StatusBadRequest), nil
		}
		version = param.Version
	}
	if err := p.store.LabelParameterVersion(name, defaultAccountID, version, labels); err != nil {
		if err == ErrParameterNotFound {
			return ssmError("ParameterNotFound", "parameter version not found", http.StatusBadRequest), nil
		}
		return nil, err
	}
	return jsonResp(http.StatusOK, map[string]any{
		"InvalidLabels":    []string{},
		"ParameterVersion": version,
	})
}

// getParameterHistory returns all versions of a parameter.
func (p *Provider) getParameterHistory(params map[string]any) (*plugin.Response, error) {
	name, _ := params["Name"].(string)
	if name == "" {
		return ssmError("InvalidParameterException", "Name is required", http.StatusBadRequest), nil
	}
	history, err := p.store.GetParameterHistory(name, defaultAccountID)
	if err != nil {
		return nil, err
	}
	list := make([]map[string]any, 0, len(history))
	for _, h := range history {
		entry := map[string]any{
			"Name":             h.Name,
			"Type":             h.Type,
			"Value":            h.Value,
			"Version":          h.Version,
			"Description":      h.Description,
			"LastModifiedDate": h.LastModified.Unix(),
			"Labels":           h.Labels,
		}
		list = append(list, entry)
	}
	return jsonResp(http.StatusOK, map[string]any{"Parameters": list})
}

// createDocument creates a new SSM document.
func (p *Provider) createDocument(params map[string]any) (*plugin.Response, error) {
	name, _ := params["Name"].(string)
	content, _ := params["Content"].(string)
	if name == "" || content == "" {
		return ssmError("InvalidParameterException", "Name and Content are required", http.StatusBadRequest), nil
	}
	docType, _ := params["DocumentType"].(string)
	if docType == "" {
		docType = "Command"
	}
	docFormat, _ := params["DocumentFormat"].(string)
	if docFormat == "" {
		docFormat = "JSON"
	}
	doc, err := p.store.CreateDocument(name, defaultAccountID, content, docType, docFormat)
	if err != nil {
		if err == ErrDocumentExists {
			return ssmError("DocumentAlreadyExists", "document already exists", http.StatusBadRequest), nil
		}
		return nil, err
	}
	return jsonResp(http.StatusOK, map[string]any{
		"DocumentDescription": documentToMap(doc),
	})
}

// updateDocument updates an existing SSM document.
func (p *Provider) updateDocument(params map[string]any) (*plugin.Response, error) {
	name, _ := params["Name"].(string)
	content, _ := params["Content"].(string)
	if name == "" || content == "" {
		return ssmError("InvalidParameterException", "Name and Content are required", http.StatusBadRequest), nil
	}
	doc, err := p.store.UpdateDocument(name, defaultAccountID, content)
	if err != nil {
		if err == ErrDocumentNotFound {
			return ssmError("InvalidDocument", "document not found", http.StatusBadRequest), nil
		}
		return nil, err
	}
	return jsonResp(http.StatusOK, map[string]any{
		"DocumentDescription": documentToMap(doc),
	})
}

// deleteDocument deletes an SSM document.
func (p *Provider) deleteDocument(params map[string]any) (*plugin.Response, error) {
	name, _ := params["Name"].(string)
	if name == "" {
		return ssmError("InvalidParameterException", "Name is required", http.StatusBadRequest), nil
	}
	if err := p.store.DeleteDocument(name, defaultAccountID); err != nil {
		if err == ErrDocumentNotFound {
			return ssmError("InvalidDocument", "document not found", http.StatusBadRequest), nil
		}
		return nil, err
	}
	return jsonResp(http.StatusOK, map[string]any{})
}

// describeDocument returns metadata for an SSM document.
func (p *Provider) describeDocument(params map[string]any) (*plugin.Response, error) {
	name, _ := params["Name"].(string)
	if name == "" {
		return ssmError("InvalidParameterException", "Name is required", http.StatusBadRequest), nil
	}
	version := 0
	if v, ok := params["DocumentVersion"].(string); ok && v != "" && v != "$LATEST" {
		_, _ = fmt.Sscanf(v, "%d", &version)
	}
	doc, err := p.store.GetDocument(name, defaultAccountID, version)
	if err != nil {
		if err == ErrDocumentNotFound {
			return ssmError("InvalidDocument", "document not found", http.StatusBadRequest), nil
		}
		return nil, err
	}
	return jsonResp(http.StatusOK, map[string]any{
		"Document": documentToMap(doc),
	})
}

// getDocumentContent returns the content of an SSM document.
func (p *Provider) getDocumentContent(params map[string]any) (*plugin.Response, error) {
	name, _ := params["Name"].(string)
	if name == "" {
		return ssmError("InvalidParameterException", "Name is required", http.StatusBadRequest), nil
	}
	version := 0
	if v, ok := params["DocumentVersion"].(string); ok && v != "" && v != "$LATEST" {
		_, _ = fmt.Sscanf(v, "%d", &version)
	}
	doc, err := p.store.GetDocument(name, defaultAccountID, version)
	if err != nil {
		if err == ErrDocumentNotFound {
			return ssmError("InvalidDocument", "document not found", http.StatusBadRequest), nil
		}
		return nil, err
	}
	docFormat, _ := params["DocumentFormat"].(string)
	if docFormat == "" {
		docFormat = doc.DocumentFormat
	}
	return jsonResp(http.StatusOK, map[string]any{
		"Name":            doc.Name,
		"Content":         doc.Content,
		"DocumentType":    doc.DocumentType,
		"DocumentFormat":  docFormat,
		"DocumentVersion": fmt.Sprintf("%d", doc.Version),
		"Status":          doc.Status,
	})
}

// listDocuments lists SSM documents.
func (p *Provider) listDocuments(_ map[string]any) (*plugin.Response, error) {
	docs, err := p.store.ListDocuments(defaultAccountID)
	if err != nil {
		return nil, err
	}
	list := make([]map[string]any, 0, len(docs))
	for _, d := range docs {
		d2 := d
		list = append(list, map[string]any{
			"Name":            d2.Name,
			"DocumentType":    d2.DocumentType,
			"DocumentFormat":  d2.DocumentFormat,
			"DocumentVersion": fmt.Sprintf("%d", d2.Version),
			"Status":          d2.Status,
			"CreatedDate":     d2.CreatedAt.Unix(),
		})
	}
	return jsonResp(http.StatusOK, map[string]any{
		"DocumentIdentifiers": list,
	})
}

// startSession creates a stub SSM session.
func (p *Provider) startSession(params map[string]any) (*plugin.Response, error) {
	target, _ := params["Target"].(string)
	if target == "" {
		return ssmError("InvalidParameterException", "Target is required", http.StatusBadRequest), nil
	}
	sessionID := "session-" + randomHex(8)
	tokenValue := randomHex(32)
	streamURL := fmt.Sprintf("wss://ssmmessages.us-east-1.amazonaws.com/v1/data-channel/%s?role=publish_subscribe", sessionID)
	return jsonResp(http.StatusOK, map[string]any{
		"SessionId":  sessionID,
		"TokenValue": tokenValue,
		"StreamUrl":  streamURL,
	})
}

// terminateSession terminates a stub SSM session.
func (p *Provider) terminateSession(params map[string]any) (*plugin.Response, error) {
	sessionID, _ := params["SessionId"].(string)
	if sessionID == "" {
		return ssmError("InvalidParameterException", "SessionId is required", http.StatusBadRequest), nil
	}
	return jsonResp(http.StatusOK, map[string]any{
		"SessionId": sessionID,
		"Status":    "Terminated",
	})
}

func paramToMap(p *Parameter) map[string]any {
	return map[string]any{
		"Name":             p.Name,
		"Type":             p.Type,
		"Value":            p.Value,
		"Version":          p.Version,
		"LastModifiedDate": p.LastModified.Unix(),
	}
}

func documentToMap(d *Document) map[string]any {
	return map[string]any{
		"Name":            d.Name,
		"DocumentType":    d.DocumentType,
		"DocumentFormat":  d.DocumentFormat,
		"DocumentVersion": fmt.Sprintf("%d", d.Version),
		"DefaultVersion":  fmt.Sprintf("%d", d.DefaultVersion),
		"Status":          d.Status,
		"CreatedDate":     d.CreatedAt.Unix(),
	}
}

func randomHex(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

func ssmError(code, message string, status int) *plugin.Response {
	body, _ := json.Marshal(map[string]any{"__type": code, "message": message})
	return &plugin.Response{StatusCode: status, ContentType: "application/x-amz-json-1.1", Body: body}
}

func jsonResp(status int, v any) (*plugin.Response, error) {
	body, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	return &plugin.Response{StatusCode: status, ContentType: "application/x-amz-json-1.1", Body: body}, nil
}
