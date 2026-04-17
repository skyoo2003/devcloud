// SPDX-License-Identifier: Apache-2.0

package efs

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"strings"
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
	t.Cleanup(func() { p.Shutdown(context.Background()) })
	return p
}

func call(t *testing.T, p *Provider, method, path, op, body string) *plugin.Response {
	t.Helper()
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err := p.HandleRequest(context.Background(), op, req)
	require.NoError(t, err)
	return resp
}

func parseBody(t *testing.T, resp *plugin.Response) map[string]any {
	t.Helper()
	var m map[string]any
	require.NoError(t, json.Unmarshal(resp.Body, &m))
	return m
}

func TestCreateDescribeDeleteFileSystem(t *testing.T) {
	p := newTestProvider(t)

	// Create
	body := `{"CreationToken":"test-token","PerformanceMode":"generalPurpose"}`
	resp := call(t, p, "POST", "/2015-02-01/file-systems", "", body)
	assert.Equal(t, 201, resp.StatusCode)
	rb := parseBody(t, resp)
	fsID, ok := rb["FileSystemId"].(string)
	require.True(t, ok)
	assert.True(t, strings.HasPrefix(fsID, "fs-"))

	// Idempotent create (same token)
	resp2 := call(t, p, "POST", "/2015-02-01/file-systems", "", body)
	assert.Equal(t, 200, resp2.StatusCode)
	rb2 := parseBody(t, resp2)
	assert.Equal(t, fsID, rb2["FileSystemId"])

	// Describe all
	resp3 := call(t, p, "GET", "/2015-02-01/file-systems", "", "")
	assert.Equal(t, 200, resp3.StatusCode)
	rb3 := parseBody(t, resp3)
	fsList, ok := rb3["FileSystems"].([]any)
	require.True(t, ok)
	assert.Len(t, fsList, 1)

	// Describe by ID
	resp4 := call(t, p, "GET", fmt.Sprintf("/2015-02-01/file-systems?FileSystemId=%s", fsID), "", "")
	assert.Equal(t, 200, resp4.StatusCode)

	// Update
	resp5 := call(t, p, "PUT", fmt.Sprintf("/2015-02-01/file-systems/%s", fsID), "", `{"ThroughputMode":"provisioned"}`)
	assert.Equal(t, 202, resp5.StatusCode)

	// Delete
	resp6 := call(t, p, "DELETE", fmt.Sprintf("/2015-02-01/file-systems/%s", fsID), "", "")
	assert.Equal(t, 204, resp6.StatusCode)

	// Describe after delete - should be empty
	resp7 := call(t, p, "GET", "/2015-02-01/file-systems", "", "")
	rb7 := parseBody(t, resp7)
	fsList7, _ := rb7["FileSystems"].([]any)
	assert.Len(t, fsList7, 0)
}

func TestFileSystemPolicy(t *testing.T) {
	p := newTestProvider(t)

	// Create FS
	resp := call(t, p, "POST", "/2015-02-01/file-systems", "", `{"CreationToken":"pol-token"}`)
	require.Equal(t, 201, resp.StatusCode)
	fsID := parseBody(t, resp)["FileSystemId"].(string)

	// Put policy
	resp2 := call(t, p, "PUT", fmt.Sprintf("/2015-02-01/file-systems/%s/policy", fsID), "", `{"Policy":"{\"Version\":\"2012-10-17\"}"}`)
	assert.Equal(t, 200, resp2.StatusCode)

	// Describe policy
	resp3 := call(t, p, "GET", fmt.Sprintf("/2015-02-01/file-systems/%s/policy", fsID), "", "")
	assert.Equal(t, 200, resp3.StatusCode)
	rb3 := parseBody(t, resp3)
	assert.NotEmpty(t, rb3["Policy"])

	// Delete policy
	resp4 := call(t, p, "DELETE", fmt.Sprintf("/2015-02-01/file-systems/%s/policy", fsID), "", "")
	assert.Equal(t, 204, resp4.StatusCode)
}

func TestBackupPolicy(t *testing.T) {
	p := newTestProvider(t)

	resp := call(t, p, "POST", "/2015-02-01/file-systems", "", `{"CreationToken":"bp-token"}`)
	require.Equal(t, 201, resp.StatusCode)
	fsID := parseBody(t, resp)["FileSystemId"].(string)

	// Put backup policy
	resp2 := call(t, p, "PUT", fmt.Sprintf("/2015-02-01/file-systems/%s/backup-policy", fsID), "", `{"BackupPolicy":{"Status":"ENABLED"}}`)
	assert.Equal(t, 200, resp2.StatusCode)

	// Describe
	resp3 := call(t, p, "GET", fmt.Sprintf("/2015-02-01/file-systems/%s/backup-policy", fsID), "", "")
	assert.Equal(t, 200, resp3.StatusCode)
	rb3 := parseBody(t, resp3)
	bp := rb3["BackupPolicy"].(map[string]any)
	assert.Equal(t, "ENABLED", bp["Status"])
}

func TestLifecycleConfiguration(t *testing.T) {
	p := newTestProvider(t)

	resp := call(t, p, "POST", "/2015-02-01/file-systems", "", `{"CreationToken":"lc-token"}`)
	require.Equal(t, 201, resp.StatusCode)
	fsID := parseBody(t, resp)["FileSystemId"].(string)

	// Put lifecycle config
	resp2 := call(t, p, "PUT", fmt.Sprintf("/2015-02-01/file-systems/%s/lifecycle-configuration", fsID), "",
		`{"LifecyclePolicies":[{"TransitionToIA":"AFTER_30_DAYS"}]}`)
	assert.Equal(t, 200, resp2.StatusCode)

	// Describe
	resp3 := call(t, p, "GET", fmt.Sprintf("/2015-02-01/file-systems/%s/lifecycle-configuration", fsID), "", "")
	assert.Equal(t, 200, resp3.StatusCode)
	rb3 := parseBody(t, resp3)
	lc, ok := rb3["LifecyclePolicies"].([]any)
	require.True(t, ok)
	assert.Len(t, lc, 1)
}

func TestMountTargetCRUD(t *testing.T) {
	p := newTestProvider(t)

	// Create FS first
	resp := call(t, p, "POST", "/2015-02-01/file-systems", "", `{"CreationToken":"mt-token"}`)
	require.Equal(t, 201, resp.StatusCode)
	fsID := parseBody(t, resp)["FileSystemId"].(string)

	// Create mount target
	body := fmt.Sprintf(`{"FileSystemId":"%s","SubnetId":"subnet-12345"}`, fsID)
	resp2 := call(t, p, "POST", "/2015-02-01/mount-targets", "", body)
	assert.Equal(t, 200, resp2.StatusCode)
	mtID := parseBody(t, resp2)["MountTargetId"].(string)
	assert.True(t, strings.HasPrefix(mtID, "fsmt-"))

	// Describe by FS
	resp3 := call(t, p, "GET", fmt.Sprintf("/2015-02-01/mount-targets?FileSystemId=%s", fsID), "", "")
	assert.Equal(t, 200, resp3.StatusCode)
	rb3 := parseBody(t, resp3)
	mts := rb3["MountTargets"].([]any)
	assert.Len(t, mts, 1)

	// Describe security groups
	resp4 := call(t, p, "GET", fmt.Sprintf("/2015-02-01/mount-targets/%s/security-groups", mtID), "", "")
	assert.Equal(t, 200, resp4.StatusCode)

	// Modify security groups
	resp5 := call(t, p, "PUT", fmt.Sprintf("/2015-02-01/mount-targets/%s/security-groups", mtID), "", `{"SecurityGroups":["sg-12345"]}`)
	assert.Equal(t, 204, resp5.StatusCode)

	// Verify mount target count incremented
	resp6 := call(t, p, "GET", fmt.Sprintf("/2015-02-01/file-systems?FileSystemId=%s", fsID), "", "")
	rb6 := parseBody(t, resp6)
	fsList := rb6["FileSystems"].([]any)
	fs := fsList[0].(map[string]any)
	assert.Equal(t, float64(1), fs["NumberOfMountTargets"])

	// Delete mount target
	resp7 := call(t, p, "DELETE", fmt.Sprintf("/2015-02-01/mount-targets/%s", mtID), "", "")
	assert.Equal(t, 204, resp7.StatusCode)
}

func TestAccessPointCRUD(t *testing.T) {
	p := newTestProvider(t)

	// Create FS first
	resp := call(t, p, "POST", "/2015-02-01/file-systems", "", `{"CreationToken":"ap-token"}`)
	require.Equal(t, 201, resp.StatusCode)
	fsID := parseBody(t, resp)["FileSystemId"].(string)

	// Create access point
	body := fmt.Sprintf(`{"FileSystemId":"%s","ClientToken":"client-1","Tags":[{"Key":"Name","Value":"my-ap"}]}`, fsID)
	resp2 := call(t, p, "POST", "/2015-02-01/access-points", "", body)
	assert.Equal(t, 200, resp2.StatusCode)
	apID := parseBody(t, resp2)["AccessPointId"].(string)
	assert.True(t, strings.HasPrefix(apID, "fsap-"))

	// Describe all
	resp3 := call(t, p, "GET", "/2015-02-01/access-points", "", "")
	assert.Equal(t, 200, resp3.StatusCode)
	rb3 := parseBody(t, resp3)
	aps := rb3["AccessPoints"].([]any)
	assert.Len(t, aps, 1)

	// Delete
	resp4 := call(t, p, "DELETE", fmt.Sprintf("/2015-02-01/access-points/%s", apID), "", "")
	assert.Equal(t, 204, resp4.StatusCode)
}

func TestTagging(t *testing.T) {
	p := newTestProvider(t)

	// Create FS
	resp := call(t, p, "POST", "/2015-02-01/file-systems", "", `{"CreationToken":"tag-token","Tags":[{"Key":"env","Value":"test"}]}`)
	require.Equal(t, 201, resp.StatusCode)
	fsID := parseBody(t, resp)["FileSystemId"].(string)

	// List tags
	resp2 := call(t, p, "GET", fmt.Sprintf("/2015-02-01/resource-tags/%s", fsID), "", "")
	assert.Equal(t, 200, resp2.StatusCode)
	rb2 := parseBody(t, resp2)
	tags := rb2["Tags"].([]any)
	assert.Len(t, tags, 1)

	// Add tag
	resp3 := call(t, p, "POST", fmt.Sprintf("/2015-02-01/resource-tags/%s", fsID), "", `{"Tags":[{"Key":"tier","Value":"prod"}]}`)
	assert.Equal(t, 204, resp3.StatusCode)

	// Untag
	resp4 := call(t, p, "DELETE", fmt.Sprintf("/2015-02-01/resource-tags/%s?tagKeys=env", fsID), "", "")
	assert.Equal(t, 204, resp4.StatusCode)
}

func TestReplicationStub(t *testing.T) {
	p := newTestProvider(t)
	resp := call(t, p, "GET", "/2015-02-01/file-systems/replication-configurations", "", "")
	assert.Equal(t, 200, resp.StatusCode)
	rb := parseBody(t, resp)
	assert.Contains(t, rb, "Replications")
}
