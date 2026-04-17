// SPDX-License-Identifier: Apache-2.0

// internal/services/ecr/provider_test.go
package ecr

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/skyoo2003/devcloud/internal/plugin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestECRProvider(t *testing.T) *Provider {
	t.Helper()
	dir := t.TempDir()
	p := &Provider{}
	err := p.Init(plugin.PluginConfig{DataDir: dir, Options: map[string]any{
		"db_path": filepath.Join(dir, "ecr.db"),
	}})
	require.NoError(t, err)
	t.Cleanup(func() { p.Shutdown(context.Background()) })
	return p
}

func ecrRequest(t *testing.T, p *Provider, action string, body map[string]any) (int, map[string]any) {
	t.Helper()
	raw, _ := json.Marshal(body)
	req := httptest.NewRequest("POST", "/", bytes.NewReader(raw))
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req.Header.Set("X-Amz-Target", "AmazonEC2ContainerRegistry_V20150921."+action)
	resp, err := p.HandleRequest(context.Background(), "", req)
	require.NoError(t, err)
	var result map[string]any
	json.Unmarshal(resp.Body, &result)
	return resp.StatusCode, result
}

func TestRepositoryCRUD(t *testing.T) {
	p := newTestECRProvider(t)

	status, result := ecrRequest(t, p, "CreateRepository", map[string]any{"repositoryName": "my-repo"})
	assert.Equal(t, 200, status)
	repo, ok := result["repository"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "my-repo", repo["repositoryName"])
	assert.Contains(t, repo["repositoryUri"].(string), "my-repo")
	assert.Contains(t, repo["repositoryArn"].(string), "arn:aws:ecr:")

	// Duplicate should fail
	dupStatus, _ := ecrRequest(t, p, "CreateRepository", map[string]any{"repositoryName": "my-repo"})
	assert.Equal(t, 400, dupStatus)

	// Describe
	_, descResult := ecrRequest(t, p, "DescribeRepositories", map[string]any{"repositoryNames": []string{"my-repo"}})
	repos, ok := descResult["repositories"].([]any)
	require.True(t, ok)
	assert.Len(t, repos, 1)

	// Delete
	delStatus, _ := ecrRequest(t, p, "DeleteRepository", map[string]any{"repositoryName": "my-repo"})
	assert.Equal(t, 200, delStatus)

	// Should be gone
	_, afterDel := ecrRequest(t, p, "DescribeRepositories", map[string]any{"repositoryNames": []string{"my-repo"}})
	remaining, _ := afterDel["repositories"].([]any)
	assert.Len(t, remaining, 0)
}

func TestPutAndDescribeImages(t *testing.T) {
	p := newTestECRProvider(t)

	ecrRequest(t, p, "CreateRepository", map[string]any{"repositoryName": "img-repo"})

	manifest := `{"schemaVersion":2,"mediaType":"application/vnd.docker.distribution.manifest.v2+json"}`
	status, result := ecrRequest(t, p, "PutImage", map[string]any{
		"repositoryName": "img-repo",
		"imageManifest":  manifest,
		"imageTag":       "latest",
	})
	assert.Equal(t, 200, status)
	img, ok := result["image"].(map[string]any)
	require.True(t, ok)
	imageID := img["imageId"].(map[string]any)
	assert.Contains(t, imageID["imageDigest"].(string), "sha256:")
	assert.Equal(t, "latest", imageID["imageTag"])

	_, descResult := ecrRequest(t, p, "DescribeImages", map[string]any{"repositoryName": "img-repo"})
	details, _ := descResult["imageDetails"].([]any)
	assert.Len(t, details, 1)

	_, listResult := ecrRequest(t, p, "ListImages", map[string]any{"repositoryName": "img-repo"})
	ids, _ := listResult["imageIds"].([]any)
	assert.Len(t, ids, 1)
}

func TestGetAuthorizationToken(t *testing.T) {
	p := newTestECRProvider(t)

	status, result := ecrRequest(t, p, "GetAuthorizationToken", map[string]any{})
	assert.Equal(t, 200, status)
	authData, ok := result["authorizationData"].([]any)
	require.True(t, ok)
	require.Len(t, authData, 1)
	entry := authData[0].(map[string]any)
	assert.NotEmpty(t, entry["authorizationToken"])
	assert.NotEmpty(t, entry["expiresAt"])
}

func TestLayerUpload(t *testing.T) {
	p := newTestECRProvider(t)

	ecrRequest(t, p, "CreateRepository", map[string]any{"repositoryName": "layer-repo"})

	// InitiateLayerUpload
	status, result := ecrRequest(t, p, "InitiateLayerUpload", map[string]any{"repositoryName": "layer-repo"})
	assert.Equal(t, 200, status)
	uploadID, ok := result["uploadId"].(string)
	require.True(t, ok)
	assert.NotEmpty(t, uploadID)

	// UploadLayerPart
	status, _ = ecrRequest(t, p, "UploadLayerPart", map[string]any{
		"repositoryName": "layer-repo",
		"uploadId":       uploadID,
		"partFirstByte":  float64(0),
		"partLastByte":   float64(3),
		"layerPartBlob":  "dGVzdA==", // base64("test")
	})
	assert.Equal(t, 200, status)

	// CompleteLayerUpload
	status, result = ecrRequest(t, p, "CompleteLayerUpload", map[string]any{
		"repositoryName": "layer-repo",
		"uploadId":       uploadID,
		"layerDigests":   []string{"sha256:9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08"},
	})
	assert.Equal(t, 200, status)

	// BatchCheckLayerAvailability
	status, result = ecrRequest(t, p, "BatchCheckLayerAvailability", map[string]any{
		"repositoryName": "layer-repo",
		"layerDigests":   []string{"sha256:9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08"},
	})
	assert.Equal(t, 200, status)
	layers, _ := result["layers"].([]any)
	require.Len(t, layers, 1)
	layer := layers[0].(map[string]any)
	assert.Equal(t, "AVAILABLE", layer["layerAvailability"])

	// GetDownloadUrlForLayer
	status, result = ecrRequest(t, p, "GetDownloadUrlForLayer", map[string]any{
		"repositoryName": "layer-repo",
		"layerDigest":    "sha256:9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08",
	})
	assert.Equal(t, 200, status)
	assert.NotEmpty(t, result["downloadUrl"])
}

func TestLifecyclePolicy(t *testing.T) {
	p := newTestECRProvider(t)

	ecrRequest(t, p, "CreateRepository", map[string]any{"repositoryName": "lc-repo"})

	policy := `{"rules":[{"rulePriority":1,"action":{"type":"expire"}}]}`
	status, _ := ecrRequest(t, p, "PutLifecyclePolicy", map[string]any{
		"repositoryName":      "lc-repo",
		"lifecyclePolicyText": policy,
	})
	assert.Equal(t, 200, status)

	status, result := ecrRequest(t, p, "GetLifecyclePolicy", map[string]any{"repositoryName": "lc-repo"})
	assert.Equal(t, 200, status)
	assert.Equal(t, policy, result["lifecyclePolicyText"])

	status, _ = ecrRequest(t, p, "DeleteLifecyclePolicy", map[string]any{"repositoryName": "lc-repo"})
	assert.Equal(t, 200, status)

	// After deletion, GetLifecyclePolicy should return 400.
	status, _ = ecrRequest(t, p, "GetLifecyclePolicy", map[string]any{"repositoryName": "lc-repo"})
	assert.Equal(t, 400, status)
}

func TestRepoTags(t *testing.T) {
	p := newTestECRProvider(t)

	_, createResult := ecrRequest(t, p, "CreateRepository", map[string]any{"repositoryName": "tag-repo"})
	repo := createResult["repository"].(map[string]any)
	arn := repo["repositoryArn"].(string)

	status, _ := ecrRequest(t, p, "TagResource", map[string]any{
		"resourceArn": arn,
		"tags":        []any{map[string]any{"Key": "env", "Value": "test"}},
	})
	assert.Equal(t, 200, status)

	status, result := ecrRequest(t, p, "ListTagsForResource", map[string]any{"resourceArn": arn})
	assert.Equal(t, 200, status)
	tags, _ := result["tags"].([]any)
	require.Len(t, tags, 1)
	tag := tags[0].(map[string]any)
	assert.Equal(t, "env", tag["Key"])

	status, _ = ecrRequest(t, p, "UntagResource", map[string]any{
		"resourceArn": arn,
		"tagKeys":     []string{"env"},
	})
	assert.Equal(t, 200, status)

	_, afterUntag := ecrRequest(t, p, "ListTagsForResource", map[string]any{"resourceArn": arn})
	remainingTags, _ := afterUntag["tags"].([]any)
	assert.Len(t, remainingTags, 0)
}

func TestImageScan(t *testing.T) {
	p := newTestECRProvider(t)

	ecrRequest(t, p, "CreateRepository", map[string]any{"repositoryName": "scan-repo"})
	ecrRequest(t, p, "PutImage", map[string]any{
		"repositoryName": "scan-repo",
		"imageManifest":  `{"schemaVersion":2}`,
		"imageTag":       "v1",
	})

	status, _ := ecrRequest(t, p, "StartImageScan", map[string]any{
		"repositoryName": "scan-repo",
		"imageId":        map[string]any{"imageTag": "v1"},
	})
	assert.Equal(t, 200, status)

	status, result := ecrRequest(t, p, "DescribeImageScanFindings", map[string]any{
		"repositoryName": "scan-repo",
		"imageId":        map[string]any{"imageTag": "v1"},
	})
	assert.Equal(t, 200, status)
	scanStatus, _ := result["imageScanStatus"].(map[string]any)
	assert.Equal(t, "COMPLETE", scanStatus["status"])
}

func TestPutImageScanningConfiguration(t *testing.T) {
	p := newTestECRProvider(t)

	ecrRequest(t, p, "CreateRepository", map[string]any{"repositoryName": "cfg-repo"})

	status, result := ecrRequest(t, p, "PutImageScanningConfiguration", map[string]any{
		"repositoryName":             "cfg-repo",
		"imageScanningConfiguration": map[string]any{"scanOnPush": true},
	})
	assert.Equal(t, 200, status)
	cfg, _ := result["imageScanningConfiguration"].(map[string]any)
	assert.Equal(t, true, cfg["scanOnPush"])
}

// TestECRStore_PortHandling verifies NewECRStore threads the configured HTTP
// port into the store struct and falls back to the canonical default (4747)
// when zero is passed. Regression guard for the opts-based port refactor in
// GetDownloadURLForLayer and friends.
func TestECRStore_PortHandling(t *testing.T) {
	tests := []struct {
		name      string
		inputPort int
		wantPort  int
	}{
		{"zero falls back to default 4747", 0, 4747},
		{"negative port falls back to 4747", -1, 4747},
		{"custom port 5858", 5858, 5858},
		{"custom port 8080", 8080, 8080},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			s, err := NewECRStore(dir, tt.inputPort)
			require.NoError(t, err)
			t.Cleanup(func() { _ = s.Close() })
			assert.Equal(t, tt.wantPort, s.port, "port field mismatch")
		})
	}
}

// TestECRProvider_Init_PortFromOptions verifies Provider.Init reads the
// `server_port` option from cfg.Options and threads it into the underlying
// ECRStore. Wrong types fall back to the default.
func TestECRProvider_Init_PortFromOptions(t *testing.T) {
	tests := []struct {
		name     string
		opts     map[string]any
		wantPort int
	}{
		{"nil opts falls back to 4747", nil, 4747},
		{"empty opts falls back to 4747", map[string]any{}, 4747},
		{"wrong type falls back to 4747", map[string]any{"server_port": "8080"}, 4747},
		{"explicit int 5858", map[string]any{"server_port": 5858}, 5858},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			p := &Provider{}
			require.NoError(t, p.Init(plugin.PluginConfig{
				DataDir: filepath.Join(dir, "ecr"),
				Options: tt.opts,
			}))
			t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
			assert.Equal(t, tt.wantPort, p.store.port)
		})
	}
}
