// SPDX-License-Identifier: Apache-2.0

// internal/services/glacier/provider_test.go
package glacier

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http/httptest"
	"testing"

	"github.com/skyoo2003/devcloud/internal/plugin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestProvider(t *testing.T) *Provider {
	t.Helper()
	p := &Provider{}
	err := p.Init(plugin.PluginConfig{DataDir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
	return p
}

func callOp(t *testing.T, p *Provider, method, path, op string, body []byte, headers map[string]string) *plugin.Response {
	t.Helper()
	req := httptest.NewRequest(method, path, bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := p.HandleRequest(context.Background(), op, req)
	require.NoError(t, err)
	return resp
}

func parseBody(t *testing.T, resp *plugin.Response) map[string]any {
	t.Helper()
	var m map[string]any
	if len(resp.Body) == 0 {
		return m
	}
	require.NoError(t, json.Unmarshal(resp.Body, &m))
	return m
}

func TestVaultCRUD(t *testing.T) {
	p := newTestProvider(t)

	// CreateVault
	resp := callOp(t, p, "PUT", "/-/vaults/my-vault", "CreateVault", nil, nil)
	assert.Equal(t, 200, resp.StatusCode)
	rb := parseBody(t, resp)
	assert.NotEmpty(t, rb["Location"])

	// DescribeVault
	resp2 := callOp(t, p, "GET", "/-/vaults/my-vault", "DescribeVault", nil, nil)
	assert.Equal(t, 200, resp2.StatusCode)
	rb2 := parseBody(t, resp2)
	assert.Equal(t, "my-vault", rb2["VaultName"])
	assert.Equal(t, float64(0), rb2["NumberOfArchives"])

	// ListVaults
	callOp(t, p, "PUT", "/-/vaults/vault-b", "CreateVault", nil, nil)
	resp3 := callOp(t, p, "GET", "/-/vaults", "ListVaults", nil, nil)
	assert.Equal(t, 200, resp3.StatusCode)
	rb3 := parseBody(t, resp3)
	list, ok := rb3["VaultList"].([]any)
	require.True(t, ok)
	assert.Len(t, list, 2)

	// DeleteVault
	resp4 := callOp(t, p, "DELETE", "/-/vaults/vault-b", "DeleteVault", nil, nil)
	assert.Equal(t, 204, resp4.StatusCode)

	// Get non-existent
	resp5 := callOp(t, p, "GET", "/-/vaults/nonexistent", "DescribeVault", nil, nil)
	assert.Equal(t, 404, resp5.StatusCode)
}

func TestArchiveUpload(t *testing.T) {
	p := newTestProvider(t)

	// CreateVault
	callOp(t, p, "PUT", "/-/vaults/my-vault", "CreateVault", nil, nil)

	// UploadArchive
	archiveData := []byte("my archive content")
	resp := callOp(t, p, "POST", "/-/vaults/my-vault/archives", "UploadArchive",
		archiveData, map[string]string{
			"x-amz-archive-description": "my-archive",
			"x-amz-sha256-tree-hash":    "abc123",
		})
	assert.Equal(t, 200, resp.StatusCode)
	rb := parseBody(t, resp)
	archiveID, ok := rb["ArchiveId"].(string)
	require.True(t, ok)
	assert.NotEmpty(t, archiveID)
	assert.Equal(t, "abc123", rb["Checksum"])

	// Verify vault stats updated
	resp2 := callOp(t, p, "GET", "/-/vaults/my-vault", "DescribeVault", nil, nil)
	rb2 := parseBody(t, resp2)
	assert.Equal(t, float64(1), rb2["NumberOfArchives"])
	assert.Equal(t, float64(len(archiveData)), rb2["SizeInBytes"])

	// DeleteArchive
	resp3 := callOp(t, p, "DELETE", "/-/vaults/my-vault/archives/"+archiveID, "DeleteArchive", nil, nil)
	assert.Equal(t, 204, resp3.StatusCode)

	// Vault stats should decrease
	resp4 := callOp(t, p, "GET", "/-/vaults/my-vault", "DescribeVault", nil, nil)
	rb4 := parseBody(t, resp4)
	assert.Equal(t, float64(0), rb4["NumberOfArchives"])
}

func TestJobCRUD(t *testing.T) {
	p := newTestProvider(t)

	callOp(t, p, "PUT", "/-/vaults/my-vault", "CreateVault", nil, nil)

	// InitiateJob
	jobBody, _ := json.Marshal(map[string]any{
		"jobParameters": map[string]any{
			"Type":        "inventory-retrieval",
			"Description": "test job",
		},
	})
	resp := callOp(t, p, "POST", "/-/vaults/my-vault/jobs", "InitiateJob", jobBody, nil)
	assert.Equal(t, 200, resp.StatusCode)
	rb := parseBody(t, resp)
	jobID, ok := rb["JobId"].(string)
	require.True(t, ok)
	assert.NotEmpty(t, jobID)

	// DescribeJob
	resp2 := callOp(t, p, "GET", "/-/vaults/my-vault/jobs/"+jobID, "DescribeJob", nil, nil)
	assert.Equal(t, 200, resp2.StatusCode)
	rb2 := parseBody(t, resp2)
	assert.Equal(t, jobID, rb2["JobId"])
	assert.Equal(t, "inventory-retrieval", rb2["Action"])
	assert.Equal(t, "Succeeded", rb2["StatusCode"])

	// ListJobs
	resp3 := callOp(t, p, "GET", "/-/vaults/my-vault/jobs", "ListJobs", nil, nil)
	assert.Equal(t, 200, resp3.StatusCode)
	rb3 := parseBody(t, resp3)
	jobs, ok := rb3["JobList"].([]any)
	require.True(t, ok)
	assert.Len(t, jobs, 1)

	// GetJobOutput
	resp4 := callOp(t, p, "GET", "/-/vaults/my-vault/jobs/"+jobID+"/output", "GetJobOutput", nil, nil)
	assert.Equal(t, 200, resp4.StatusCode)
	// Body should be valid JSON inventory
	var inv map[string]any
	require.NoError(t, json.Unmarshal(resp4.Body, &inv))
	assert.NotNil(t, inv["ArchiveList"])

	// DescribeJob non-existent
	resp5 := callOp(t, p, "GET", "/-/vaults/my-vault/jobs/nonexistent", "DescribeJob", nil, nil)
	assert.Equal(t, 404, resp5.StatusCode)
}

func TestVaultTags(t *testing.T) {
	p := newTestProvider(t)

	callOp(t, p, "PUT", "/-/vaults/tagged-vault", "CreateVault", nil, nil)

	// AddTagsToVault
	tagBody, _ := json.Marshal(map[string]any{
		"Tags": map[string]string{"Env": "prod", "Team": "data"},
	})
	resp := callOp(t, p, "POST", "/-/vaults/tagged-vault/tags?operation=add", "AddTagsToVault", tagBody, nil)
	assert.Equal(t, 204, resp.StatusCode)

	// ListTagsForVault
	resp2 := callOp(t, p, "GET", "/-/vaults/tagged-vault/tags", "ListTagsForVault", nil, nil)
	assert.Equal(t, 200, resp2.StatusCode)
	rb2 := parseBody(t, resp2)
	tags, ok := rb2["Tags"].(map[string]any)
	require.True(t, ok)
	assert.Len(t, tags, 2)
	assert.Equal(t, "prod", tags["Env"])

	// RemoveTagsFromVault
	removeBody, _ := json.Marshal(map[string]any{"TagKeys": []string{"Env"}})
	resp3 := callOp(t, p, "POST", "/-/vaults/tagged-vault/tags?operation=remove", "RemoveTagsFromVault", removeBody, nil)
	assert.Equal(t, 204, resp3.StatusCode)

	// Verify only 1 tag remains
	resp4 := callOp(t, p, "GET", "/-/vaults/tagged-vault/tags", "ListTagsForVault", nil, nil)
	rb4 := parseBody(t, resp4)
	tags4 := rb4["Tags"].(map[string]any)
	assert.Len(t, tags4, 1)
	assert.Equal(t, "data", tags4["Team"])
}

func TestVaultPolicy(t *testing.T) {
	p := newTestProvider(t)

	callOp(t, p, "PUT", "/-/vaults/policy-vault", "CreateVault", nil, nil)

	// SetVaultAccessPolicy
	policyBody, _ := json.Marshal(map[string]any{
		"policy": map[string]any{
			"Policy": `{"Version":"2012-10-17","Statement":[]}`,
		},
	})
	resp := callOp(t, p, "PUT", "/-/vaults/policy-vault/access-policy", "SetVaultAccessPolicy", policyBody, nil)
	assert.Equal(t, 204, resp.StatusCode)

	// GetVaultAccessPolicy
	resp2 := callOp(t, p, "GET", "/-/vaults/policy-vault/access-policy", "GetVaultAccessPolicy", nil, nil)
	assert.Equal(t, 200, resp2.StatusCode)
	rb2 := parseBody(t, resp2)
	policyObj, ok := rb2["policy"].(map[string]any)
	require.True(t, ok)
	assert.NotEmpty(t, policyObj["Policy"])

	// DeleteVaultAccessPolicy
	resp3 := callOp(t, p, "DELETE", "/-/vaults/policy-vault/access-policy", "DeleteVaultAccessPolicy", nil, nil)
	assert.Equal(t, 204, resp3.StatusCode)

	// Vault Notifications
	notifBody, _ := json.Marshal(map[string]any{
		"vaultNotificationConfig": map[string]any{
			"SNSTopic": "arn:aws:sns:us-east-1:000000000000:my-topic",
			"Events":   []string{"ArchiveRetrievalCompleted"},
		},
	})
	resp4 := callOp(t, p, "PUT", "/-/vaults/policy-vault/notification-configuration",
		"SetVaultNotifications", notifBody, nil)
	assert.Equal(t, 204, resp4.StatusCode)

	resp5 := callOp(t, p, "GET", "/-/vaults/policy-vault/notification-configuration",
		"GetVaultNotifications", nil, nil)
	assert.Equal(t, 200, resp5.StatusCode)
	rb5 := parseBody(t, resp5)
	cfg, ok := rb5["vaultNotificationConfig"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "arn:aws:sns:us-east-1:000000000000:my-topic", cfg["SNSTopic"])

	// Vault Lock
	lockBody, _ := json.Marshal(map[string]any{
		"policy": map[string]any{
			"Policy": `{"Version":"2012-10-17"}`,
		},
	})
	resp6 := callOp(t, p, "POST", "/-/vaults/policy-vault/lock-policy",
		"InitiateVaultLock", lockBody, nil)
	assert.Equal(t, 200, resp6.StatusCode)
	rb6 := parseBody(t, resp6)
	lockID, ok := rb6["LockId"].(string)
	require.True(t, ok)
	assert.NotEmpty(t, lockID)

	resp7 := callOp(t, p, "POST", "/-/vaults/policy-vault/lock-policy/"+lockID,
		"CompleteVaultLock", nil, nil)
	assert.Equal(t, 204, resp7.StatusCode)

	resp8 := callOp(t, p, "GET", "/-/vaults/policy-vault/lock-policy",
		"GetVaultLock", nil, nil)
	assert.Equal(t, 200, resp8.StatusCode)
	rb8 := parseBody(t, resp8)
	assert.Equal(t, "Locked", rb8["State"])
}
