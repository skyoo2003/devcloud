// SPDX-License-Identifier: Apache-2.0

// internal/services/secretsmanager/provider.go
package secretsmanager

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"strings"

	"github.com/skyoo2003/devcloud/internal/plugin"
)

const defaultAccountID = plugin.DefaultAccountID
const defaultRegion = "us-east-1"

type Provider struct {
	store *SecretStore
}

func (p *Provider) ServiceID() string             { return "secretsmanager" }
func (p *Provider) ServiceName() string           { return "secretsmanager" }
func (p *Provider) Protocol() plugin.ProtocolType { return plugin.ProtocolJSON11 }

func (p *Provider) Init(cfg plugin.PluginConfig) error {
	dataDir := cfg.DataDir
	if dataDir == "" {
		dataDir = "."
	}
	var err error
	p.store, err = NewSecretStore(filepath.Join(dataDir, "secretsmanager"))
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
		return smError("InvalidParameterException", "failed to read body", http.StatusBadRequest), nil
	}
	var params map[string]any
	if len(body) > 0 {
		if err := json.Unmarshal(body, &params); err != nil {
			return smError("InvalidParameterException", "invalid JSON", http.StatusBadRequest), nil
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
	case "CreateSecret":
		return p.createSecret(params)
	case "GetSecretValue":
		return p.getSecretValue(params)
	case "PutSecretValue":
		return p.putSecretValue(params)
	case "DeleteSecret":
		return p.deleteSecret(params)
	case "ListSecrets":
		return p.listSecrets(params)
	case "DescribeSecret":
		return p.describeSecret(params)
	case "UpdateSecret":
		return p.updateSecret(params)
	case "TagResource":
		return p.tagResource(params)
	case "UntagResource":
		return p.untagResource(params)
	case "RestoreSecret":
		return p.restoreSecret(params)
	case "RotateSecret":
		return p.rotateSecret(params)
	case "CancelRotateSecret":
		return p.cancelRotateSecret(params)
	case "GetRandomPassword":
		return p.getRandomPassword(params)
	case "BatchGetSecretValue":
		return p.batchGetSecretValue(params)
	case "ListSecretVersionIds":
		return p.listSecretVersionIds(params)
	case "UpdateSecretVersionStage":
		return p.updateSecretVersionStage(params)
	case "PutResourcePolicy":
		return p.putResourcePolicy(params)
	case "GetResourcePolicy":
		return p.getResourcePolicy(params)
	case "DeleteResourcePolicy":
		return p.deleteResourcePolicy(params)
	case "ValidateResourcePolicy":
		return p.validateResourcePolicy(params)
	case "ReplicateSecretToRegions":
		return p.replicateSecretToRegions(params)
	case "StopReplicationToReplica":
		return p.stopReplicationToReplica(params)
	case "RemoveRegionsFromReplication":
		return p.removeRegionsFromReplication(params)
	case "ListTagsForResource":
		return p.listTagsForResource(params)
	case "GetSecret":
		return p.describeSecret(params)
	case "DescribeSecretVersion":
		return p.describeSecretVersion(params)
	case "ListSecretTags":
		return p.listTagsForResource(params)
	case "PutSecretValueBinary":
		return p.putSecretValue(params)
	case "ValidateSecret":
		return jsonResp(http.StatusOK, map[string]any{"Valid": true})
	case "GetSecretVersion":
		return p.getSecretValue(params)
	case "ListRotationLambdas":
		return jsonResp(http.StatusOK, map[string]any{"Lambdas": []any{}})
	case "GetRotationStatus":
		return jsonResp(http.StatusOK, map[string]any{"RotationEnabled": false})
	case "ListReplicationRegions":
		return jsonResp(http.StatusOK, map[string]any{"Regions": []any{}})
	case "EnableSecretRotation":
		return p.rotateSecret(params)
	case "DisableSecretRotation":
		return p.cancelRotateSecret(params)
	case "PutSecretMetadata":
		return p.updateSecret(params)
	case "GetSecretMetadata":
		return p.describeSecret(params)
	case "ClearSecretValue":
		return jsonResp(http.StatusOK, map[string]any{})
	case "GetSecretCounter":
		return jsonResp(http.StatusOK, map[string]any{"VersionCount": 0})
	case "GetSecretHistory":
		return p.listSecretVersionIds(params)
	default:
		return smError("InvalidAction", fmt.Sprintf("unknown action: %s", action), http.StatusBadRequest), nil
	}
}

func (p *Provider) ListResources(_ context.Context) ([]plugin.Resource, error) {
	secrets, err := p.store.ListSecrets(defaultAccountID)
	if err != nil {
		return nil, err
	}
	resources := make([]plugin.Resource, 0, len(secrets))
	for _, s := range secrets {
		resources = append(resources, plugin.Resource{Type: "secret", ID: s.ARN, Name: s.Name})
	}
	return resources, nil
}

func (p *Provider) GetMetrics(_ context.Context) (*plugin.ServiceMetrics, error) {
	return &plugin.ServiceMetrics{}, nil
}

func (p *Provider) createSecret(params map[string]any) (*plugin.Response, error) {
	name, _ := params["Name"].(string)
	if name == "" {
		return smError("InvalidParameterException", "Name is required", http.StatusBadRequest), nil
	}
	description, _ := params["Description"].(string)
	kmsKeyID, _ := params["KmsKeyId"].(string)
	secretString, _ := params["SecretString"].(string)
	var secretBinary []byte
	if sb, ok := params["SecretBinary"].(string); ok && sb != "" {
		secretBinary = []byte(sb)
	}
	arn := fmt.Sprintf("arn:aws:secretsmanager:%s:%s:secret:%s", defaultRegion, defaultAccountID, name)
	sec, err := p.store.CreateSecret(arn, name, defaultAccountID, description, kmsKeyID, secretString, secretBinary)
	if err != nil {
		return smError("ResourceExistsException", err.Error(), http.StatusBadRequest), nil
	}
	return jsonResp(http.StatusOK, map[string]any{
		"ARN":  sec.ARN,
		"Name": sec.Name,
	})
}

func (p *Provider) getSecretValue(params map[string]any) (*plugin.Response, error) {
	secretID, _ := params["SecretId"].(string)
	if secretID == "" {
		return smError("InvalidParameterException", "SecretId is required", http.StatusBadRequest), nil
	}
	versionID, _ := params["VersionId"].(string)
	versionStage, _ := params["VersionStage"].(string)
	v, err := p.store.GetSecretValue(secretID, versionID, versionStage)
	if err != nil {
		return smError("ResourceNotFoundException", "secret not found", http.StatusBadRequest), nil
	}
	result := map[string]any{
		"ARN":           v.SecretARN,
		"VersionId":     v.VersionID,
		"VersionStages": v.VersionStages,
		"CreatedDate":   v.CreatedAt.Unix(),
	}
	if v.SecretString != "" {
		result["SecretString"] = v.SecretString
	}
	if len(v.SecretBinary) > 0 {
		result["SecretBinary"] = v.SecretBinary
	}
	return jsonResp(http.StatusOK, result)
}

func (p *Provider) putSecretValue(params map[string]any) (*plugin.Response, error) {
	secretID, _ := params["SecretId"].(string)
	if secretID == "" {
		return smError("InvalidParameterException", "SecretId is required", http.StatusBadRequest), nil
	}
	secretString, _ := params["SecretString"].(string)
	var secretBinary []byte
	if sb, ok := params["SecretBinary"].(string); ok {
		secretBinary = []byte(sb)
	}
	v, err := p.store.PutSecretValue(secretID, secretString, secretBinary)
	if err != nil {
		return smError("ResourceNotFoundException", "secret not found", http.StatusBadRequest), nil
	}
	return jsonResp(http.StatusOK, map[string]any{
		"ARN":           v.SecretARN,
		"VersionId":     v.VersionID,
		"VersionStages": v.VersionStages,
	})
}

func (p *Provider) deleteSecret(params map[string]any) (*plugin.Response, error) {
	secretID, _ := params["SecretId"].(string)
	if secretID == "" {
		return smError("InvalidParameterException", "SecretId is required", http.StatusBadRequest), nil
	}
	sec, err := p.store.GetSecret(secretID)
	if err != nil {
		return smError("ResourceNotFoundException", "secret not found", http.StatusBadRequest), nil
	}
	if err := p.store.DeleteSecret(secretID); err != nil {
		return smError("ResourceNotFoundException", "secret not found", http.StatusBadRequest), nil
	}
	return jsonResp(http.StatusOK, map[string]any{"ARN": sec.ARN, "Name": sec.Name})
}

func (p *Provider) listSecrets(_ map[string]any) (*plugin.Response, error) {
	secrets, err := p.store.ListSecrets(defaultAccountID)
	if err != nil {
		return nil, err
	}
	list := make([]map[string]any, 0, len(secrets))
	for _, s := range secrets {
		list = append(list, map[string]any{
			"ARN":         s.ARN,
			"Name":        s.Name,
			"Description": s.Description,
			"CreatedDate": s.CreatedAt.Unix(),
		})
	}
	return jsonResp(http.StatusOK, map[string]any{"SecretList": list})
}

func (p *Provider) describeSecret(params map[string]any) (*plugin.Response, error) {
	secretID, _ := params["SecretId"].(string)
	if secretID == "" {
		return smError("InvalidParameterException", "SecretId is required", http.StatusBadRequest), nil
	}
	sec, err := p.store.GetSecret(secretID)
	if err != nil {
		return smError("ResourceNotFoundException", "secret not found", http.StatusBadRequest), nil
	}
	result := map[string]any{
		"ARN":             sec.ARN,
		"Name":            sec.Name,
		"Description":     sec.Description,
		"KmsKeyId":        sec.KMSKeyID,
		"CreatedDate":     sec.CreatedAt.Unix(),
		"RotationEnabled": sec.RotationEnabled,
	}
	if sec.RotationLambdaARN != "" {
		result["RotationLambdaARN"] = sec.RotationLambdaARN
	}
	if sec.RotationRules != "" {
		var rules map[string]any
		if json.Unmarshal([]byte(sec.RotationRules), &rules) == nil {
			result["RotationRules"] = rules
		}
	}
	tags, err := p.store.GetTags(sec.ARN)
	if err == nil && len(tags) > 0 {
		tagList := make([]map[string]string, 0, len(tags))
		for _, t := range tags {
			tagList = append(tagList, map[string]string{"Key": t.Key, "Value": t.Value})
		}
		result["Tags"] = tagList
	}
	return jsonResp(http.StatusOK, result)
}

func (p *Provider) updateSecret(params map[string]any) (*plugin.Response, error) {
	secretID, _ := params["SecretId"].(string)
	if secretID == "" {
		return smError("InvalidParameterException", "SecretId is required", http.StatusBadRequest), nil
	}
	description, _ := params["Description"].(string)
	kmsKeyID, _ := params["KmsKeyId"].(string)
	sec, err := p.store.UpdateSecret(secretID, description, kmsKeyID)
	if err != nil {
		return smError("ResourceNotFoundException", "secret not found", http.StatusBadRequest), nil
	}
	return jsonResp(http.StatusOK, map[string]any{
		"ARN":  sec.ARN,
		"Name": sec.Name,
	})
}

func (p *Provider) tagResource(params map[string]any) (*plugin.Response, error) {
	secretID, _ := params["SecretId"].(string)
	if secretID == "" {
		return smError("InvalidParameterException", "SecretId is required", http.StatusBadRequest), nil
	}
	rawTags, _ := params["Tags"].([]any)
	var tags []Tag
	for _, rt := range rawTags {
		t, ok := rt.(map[string]any)
		if !ok {
			continue
		}
		key, _ := t["Key"].(string)
		value, _ := t["Value"].(string)
		tags = append(tags, Tag{Key: key, Value: value})
	}
	if err := p.store.TagResource(secretID, tags); err != nil {
		return smError("ResourceNotFoundException", "secret not found", http.StatusBadRequest), nil
	}
	return jsonResp(http.StatusOK, map[string]any{})
}

func (p *Provider) restoreSecret(params map[string]any) (*plugin.Response, error) {
	secretID, _ := params["SecretId"].(string)
	if secretID == "" {
		return smError("InvalidParameterException", "SecretId is required", http.StatusBadRequest), nil
	}
	sec, err := p.store.RestoreSecret(secretID)
	if err != nil {
		return smError("ResourceNotFoundException", "secret not found", http.StatusBadRequest), nil
	}
	return jsonResp(http.StatusOK, map[string]any{
		"ARN":  sec.ARN,
		"Name": sec.Name,
	})
}

func (p *Provider) untagResource(params map[string]any) (*plugin.Response, error) {
	secretID, _ := params["SecretId"].(string)
	if secretID == "" {
		return smError("InvalidParameterException", "SecretId is required", http.StatusBadRequest), nil
	}
	rawKeys, _ := params["TagKeys"].([]any)
	keys := make([]string, 0, len(rawKeys))
	for _, k := range rawKeys {
		if s, ok := k.(string); ok {
			keys = append(keys, s)
		}
	}
	if err := p.store.UntagResource(secretID, keys); err != nil {
		return smError("ResourceNotFoundException", "secret not found", http.StatusBadRequest), nil
	}
	return jsonResp(http.StatusOK, map[string]any{})
}

func (p *Provider) rotateSecret(params map[string]any) (*plugin.Response, error) {
	secretID, _ := params["SecretId"].(string)
	if secretID == "" {
		return smError("InvalidParameterException", "SecretId is required", http.StatusBadRequest), nil
	}
	lambdaARN, _ := params["RotationLambdaARN"].(string)
	var rulesJSON string
	if rules, ok := params["RotationRules"].(map[string]any); ok {
		b, _ := json.Marshal(rules)
		rulesJSON = string(b)
	}
	_, err := p.store.RotateSecret(secretID, lambdaARN, rulesJSON)
	if err != nil {
		return smError("ResourceNotFoundException", "secret not found", http.StatusBadRequest), nil
	}
	// Create a new version to represent the rotation.
	v, err := p.store.PutSecretValue(secretID, "", nil)
	if err != nil {
		return smError("InternalServiceError", err.Error(), http.StatusInternalServerError), nil
	}
	return jsonResp(http.StatusOK, map[string]any{
		"ARN":       v.SecretARN,
		"Name":      v.SecretName,
		"VersionId": v.VersionID,
	})
}

func (p *Provider) cancelRotateSecret(params map[string]any) (*plugin.Response, error) {
	secretID, _ := params["SecretId"].(string)
	if secretID == "" {
		return smError("InvalidParameterException", "SecretId is required", http.StatusBadRequest), nil
	}
	sec, err := p.store.CancelRotateSecret(secretID)
	if err != nil {
		return smError("ResourceNotFoundException", "secret not found", http.StatusBadRequest), nil
	}
	return jsonResp(http.StatusOK, map[string]any{
		"ARN":  sec.ARN,
		"Name": sec.Name,
	})
}

// allowedChars is the default character pool for random passwords.
const allowedChars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789!@#$%^&*()-_=+[]{}|;:,.<>?"

// maxRandomPasswordLength caps user-controlled password length to prevent
// excessive memory allocation from untrusted input.
const maxRandomPasswordLength = 4096

func (p *Provider) getRandomPassword(params map[string]any) (*plugin.Response, error) {
	length := 32
	if l, ok := params["PasswordLength"].(float64); ok {
		if l <= 0 || l > maxRandomPasswordLength {
			return smError("InvalidParameterException", fmt.Sprintf("PasswordLength must be between 1 and %d", maxRandomPasswordLength), http.StatusBadRequest), nil
		}
		length = int(l)
	}
	excludeChars, _ := params["ExcludeCharacters"].(string)
	excludeNumbers, _ := params["ExcludeNumbers"].(bool)
	excludePunct, _ := params["ExcludePunctuation"].(bool)
	excludeUpper, _ := params["ExcludeUppercase"].(bool)
	excludeLower, _ := params["ExcludeLowercase"].(bool)
	includeSpace, _ := params["IncludeSpace"].(bool)

	pool := allowedChars
	if includeSpace {
		pool += " "
	}

	// Build filtered pool.
	var filtered []byte
	for i := 0; i < len(pool); i++ {
		c := pool[i]
		if strings.ContainsRune(excludeChars, rune(c)) {
			continue
		}
		if excludeNumbers && c >= '0' && c <= '9' {
			continue
		}
		if excludePunct && strings.ContainsAny(string(c), "!@#$%^&*()-_=+[]{}|;:,.<>?") {
			continue
		}
		if excludeUpper && c >= 'A' && c <= 'Z' {
			continue
		}
		if excludeLower && c >= 'a' && c <= 'z' {
			continue
		}
		filtered = append(filtered, c)
	}
	if len(filtered) == 0 {
		return smError("InvalidParameterException", "no characters available with given exclusions", http.StatusBadRequest), nil
	}

	password := make([]byte, length)
	poolLen := len(filtered)
	buf := make([]byte, length*2)
	rand.Read(buf) //nolint:errcheck
	for i, j := 0, 0; i < length; j++ {
		if j >= len(buf) {
			rand.Read(buf) //nolint:errcheck
			j = 0
		}
		password[i] = filtered[int(buf[j])%poolLen]
		i++
	}
	return jsonResp(http.StatusOK, map[string]any{"RandomPassword": string(password)})
}

func (p *Provider) batchGetSecretValue(params map[string]any) (*plugin.Response, error) {
	rawIDs, _ := params["SecretIdList"].([]any)
	ids := make([]string, 0, len(rawIDs))
	for _, id := range rawIDs {
		if s, ok := id.(string); ok {
			ids = append(ids, s)
		}
	}
	if len(ids) == 0 {
		// If no IDs specified, return all secrets.
		secrets, err := p.store.ListSecrets(defaultAccountID)
		if err != nil {
			return nil, err
		}
		for _, s := range secrets {
			ids = append(ids, s.ARN)
		}
	}
	values, _ := p.store.BatchGetSecretValues(ids)
	list := make([]map[string]any, 0, len(values))
	for _, v := range values {
		item := map[string]any{
			"ARN":           v.SecretARN,
			"Name":          v.SecretName,
			"VersionId":     v.VersionID,
			"VersionStages": v.VersionStages,
			"CreatedDate":   v.CreatedAt.Unix(),
		}
		if v.SecretString != "" {
			item["SecretString"] = v.SecretString
		}
		if len(v.SecretBinary) > 0 {
			item["SecretBinary"] = v.SecretBinary
		}
		list = append(list, item)
	}
	return jsonResp(http.StatusOK, map[string]any{"SecretValues": list, "Errors": []any{}})
}

func (p *Provider) listSecretVersionIds(params map[string]any) (*plugin.Response, error) {
	secretID, _ := params["SecretId"].(string)
	if secretID == "" {
		return smError("InvalidParameterException", "SecretId is required", http.StatusBadRequest), nil
	}
	includeDeprecated, _ := params["IncludeDeprecated"].(bool)
	versions, err := p.store.ListSecretVersionIds(secretID, includeDeprecated)
	if err != nil {
		return smError("ResourceNotFoundException", "secret not found", http.StatusBadRequest), nil
	}
	list := make([]map[string]any, 0, len(versions))
	for _, v := range versions {
		list = append(list, map[string]any{
			"VersionId":        v.VersionID,
			"VersionStages":    v.VersionStages,
			"CreatedDate":      v.CreatedAt.Unix(),
			"LastAccessedDate": v.LastAccessedAt.Unix(),
		})
	}
	return jsonResp(http.StatusOK, map[string]any{"Versions": list})
}

func (p *Provider) updateSecretVersionStage(params map[string]any) (*plugin.Response, error) {
	secretID, _ := params["SecretId"].(string)
	if secretID == "" {
		return smError("InvalidParameterException", "SecretId is required", http.StatusBadRequest), nil
	}
	stage, _ := params["VersionStage"].(string)
	if stage == "" {
		return smError("InvalidParameterException", "VersionStage is required", http.StatusBadRequest), nil
	}
	moveToVersionID, _ := params["MoveToVersionId"].(string)
	removeFromVersionID, _ := params["RemoveFromVersionId"].(string)
	sec, err := p.store.GetSecret(secretID)
	if err != nil {
		return smError("ResourceNotFoundException", "secret not found", http.StatusBadRequest), nil
	}
	if err := p.store.UpdateSecretVersionStage(secretID, stage, moveToVersionID, removeFromVersionID); err != nil {
		return smError("ResourceNotFoundException", err.Error(), http.StatusBadRequest), nil
	}
	return jsonResp(http.StatusOK, map[string]any{
		"ARN":  sec.ARN,
		"Name": sec.Name,
	})
}

func (p *Provider) putResourcePolicy(params map[string]any) (*plugin.Response, error) {
	secretID, _ := params["SecretId"].(string)
	if secretID == "" {
		return smError("InvalidParameterException", "SecretId is required", http.StatusBadRequest), nil
	}
	policy, _ := params["ResourcePolicy"].(string)
	sec, err := p.store.PutResourcePolicy(secretID, policy)
	if err != nil {
		return smError("ResourceNotFoundException", "secret not found", http.StatusBadRequest), nil
	}
	return jsonResp(http.StatusOK, map[string]any{
		"ARN":  sec.ARN,
		"Name": sec.Name,
	})
}

func (p *Provider) getResourcePolicy(params map[string]any) (*plugin.Response, error) {
	secretID, _ := params["SecretId"].(string)
	if secretID == "" {
		return smError("InvalidParameterException", "SecretId is required", http.StatusBadRequest), nil
	}
	sec, err := p.store.GetResourcePolicy(secretID)
	if err != nil {
		return smError("ResourceNotFoundException", "secret not found", http.StatusBadRequest), nil
	}
	return jsonResp(http.StatusOK, map[string]any{
		"ARN":            sec.ARN,
		"Name":           sec.Name,
		"ResourcePolicy": sec.ResourcePolicy,
	})
}

func (p *Provider) deleteResourcePolicy(params map[string]any) (*plugin.Response, error) {
	secretID, _ := params["SecretId"].(string)
	if secretID == "" {
		return smError("InvalidParameterException", "SecretId is required", http.StatusBadRequest), nil
	}
	sec, err := p.store.DeleteResourcePolicy(secretID)
	if err != nil {
		return smError("ResourceNotFoundException", "secret not found", http.StatusBadRequest), nil
	}
	return jsonResp(http.StatusOK, map[string]any{
		"ARN":  sec.ARN,
		"Name": sec.Name,
	})
}

func (p *Provider) validateResourcePolicy(params map[string]any) (*plugin.Response, error) {
	// Stub: always passes validation.
	return jsonResp(http.StatusOK, map[string]any{
		"PolicyValidationPassed": true,
		"ValidationErrors":       []any{},
	})
}

func (p *Provider) replicateSecretToRegions(params map[string]any) (*plugin.Response, error) {
	secretID, _ := params["SecretId"].(string)
	if secretID == "" {
		return smError("InvalidParameterException", "SecretId is required", http.StatusBadRequest), nil
	}
	sec, err := p.store.GetSecret(secretID)
	if err != nil {
		return smError("ResourceNotFoundException", "secret not found", http.StatusBadRequest), nil
	}
	replicaList := []map[string]any{}
	if regions, ok := params["AddReplicaRegions"].([]any); ok {
		for _, r := range regions {
			rm, ok := r.(map[string]any)
			if !ok {
				continue
			}
			region, _ := rm["Region"].(string)
			replicaList = append(replicaList, map[string]any{
				"Region":        region,
				"Status":        "InProgress",
				"StatusMessage": "Replication started",
			})
		}
	}
	return jsonResp(http.StatusOK, map[string]any{
		"ARN":               sec.ARN,
		"ReplicationStatus": replicaList,
	})
}

func (p *Provider) stopReplicationToReplica(params map[string]any) (*plugin.Response, error) {
	secretID, _ := params["SecretId"].(string)
	if secretID == "" {
		return smError("InvalidParameterException", "SecretId is required", http.StatusBadRequest), nil
	}
	sec, err := p.store.GetSecret(secretID)
	if err != nil {
		return smError("ResourceNotFoundException", "secret not found", http.StatusBadRequest), nil
	}
	return jsonResp(http.StatusOK, map[string]any{"ARN": sec.ARN})
}

func (p *Provider) removeRegionsFromReplication(params map[string]any) (*plugin.Response, error) {
	secretID, _ := params["SecretId"].(string)
	if secretID == "" {
		return smError("InvalidParameterException", "SecretId is required", http.StatusBadRequest), nil
	}
	sec, err := p.store.GetSecret(secretID)
	if err != nil {
		return smError("ResourceNotFoundException", "secret not found", http.StatusBadRequest), nil
	}
	return jsonResp(http.StatusOK, map[string]any{
		"ARN":               sec.ARN,
		"ReplicationStatus": []any{},
	})
}

func (p *Provider) listTagsForResource(params map[string]any) (*plugin.Response, error) {
	secretID, _ := params["SecretId"].(string)
	if secretID == "" {
		return smError("InvalidParameterException", "SecretId is required", http.StatusBadRequest), nil
	}
	sec, err := p.store.GetSecret(secretID)
	if err != nil {
		return smError("ResourceNotFoundException", "secret not found", http.StatusBadRequest), nil
	}
	tags, _ := p.store.GetTags(sec.ARN)
	list := make([]map[string]string, 0, len(tags))
	for _, t := range tags {
		list = append(list, map[string]string{"Key": t.Key, "Value": t.Value})
	}
	return jsonResp(http.StatusOK, map[string]any{"Tags": list})
}

func (p *Provider) describeSecretVersion(params map[string]any) (*plugin.Response, error) {
	secretID, _ := params["SecretId"].(string)
	if secretID == "" {
		return smError("InvalidParameterException", "SecretId is required", http.StatusBadRequest), nil
	}
	versionID, _ := params["VersionId"].(string)
	versionStage, _ := params["VersionStage"].(string)
	v, err := p.store.GetSecretValue(secretID, versionID, versionStage)
	if err != nil {
		return smError("ResourceNotFoundException", "version not found", http.StatusBadRequest), nil
	}
	return jsonResp(http.StatusOK, map[string]any{
		"ARN":           v.SecretARN,
		"Name":          v.SecretName,
		"VersionId":     v.VersionID,
		"VersionStages": v.VersionStages,
		"CreatedDate":   v.CreatedAt.Unix(),
	})
}

func smError(code, message string, status int) *plugin.Response {
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
