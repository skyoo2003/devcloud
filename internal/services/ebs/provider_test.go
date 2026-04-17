// SPDX-License-Identifier: Apache-2.0

// internal/services/ebs/provider_test.go
package ebs

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
	t.Cleanup(func() { p.Shutdown(context.Background()) })
	return p
}

func callOp(t *testing.T, p *Provider, method, path, op string, body []byte, headers map[string]string) *plugin.Response {
	t.Helper()
	req := httptest.NewRequest(method, path, bytes.NewReader(body))
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
	require.NoError(t, json.Unmarshal(resp.Body, &m))
	return m
}

func TestStartAndCompleteSnapshot(t *testing.T) {
	p := newTestProvider(t)

	// StartSnapshot
	body, _ := json.Marshal(map[string]any{"VolumeSize": 8, "Description": "test"})
	resp := callOp(t, p, "POST", "/snapshots", "StartSnapshot", body, nil)
	assert.Equal(t, 200, resp.StatusCode)
	rb := parseBody(t, resp)
	snapshotID, ok := rb["SnapshotId"].(string)
	require.True(t, ok)
	assert.NotEmpty(t, snapshotID)
	assert.Equal(t, "pending", rb["Status"])
	assert.Equal(t, float64(8), rb["VolumeSize"])

	// CompleteSnapshot
	resp2 := callOp(t, p, "POST", "/snapshots/completion/"+snapshotID, "CompleteSnapshot", nil,
		map[string]string{"x-amz-ChangedBlocksCount": "0"})
	assert.Equal(t, 200, resp2.StatusCode)
	rb2 := parseBody(t, resp2)
	assert.Equal(t, "completed", rb2["Status"])

	// Complete non-existent
	resp3 := callOp(t, p, "POST", "/snapshots/completion/snap-nonexistent", "CompleteSnapshot", nil, nil)
	assert.Equal(t, 404, resp3.StatusCode)
}

func TestPutAndGetBlock(t *testing.T) {
	p := newTestProvider(t)

	// Create snapshot
	body, _ := json.Marshal(map[string]any{"VolumeSize": 1})
	startResp := callOp(t, p, "POST", "/snapshots", "StartSnapshot", body, nil)
	rb := parseBody(t, startResp)
	snapshotID := rb["SnapshotId"].(string)

	// PutSnapshotBlock
	blockData := []byte("hello block data")
	putResp := callOp(t, p, "PUT", "/snapshots/"+snapshotID+"/blocks/0", "PutSnapshotBlock",
		blockData, map[string]string{
			"x-amz-Checksum":           "abc123",
			"x-amz-Checksum-Algorithm": "SHA256",
			"x-amz-Data-Length":        "16",
		})
	assert.Equal(t, 200, putResp.StatusCode)
	rb2 := parseBody(t, putResp)
	assert.Equal(t, "abc123", rb2["Checksum"])

	// GetSnapshotBlock
	getResp := callOp(t, p, "GET", "/snapshots/"+snapshotID+"/blocks/0", "GetSnapshotBlock", nil, nil)
	assert.Equal(t, 200, getResp.StatusCode)
	rb3 := parseBody(t, getResp)
	assert.NotNil(t, rb3["BlockData"])
	assert.Equal(t, "abc123", rb3["Checksum"])

	// Get non-existent block
	resp4 := callOp(t, p, "GET", "/snapshots/"+snapshotID+"/blocks/9999", "GetSnapshotBlock", nil, nil)
	assert.Equal(t, 404, resp4.StatusCode)
}

func TestListBlocks(t *testing.T) {
	p := newTestProvider(t)

	// Create snapshot
	body, _ := json.Marshal(map[string]any{"VolumeSize": 1})
	startResp := callOp(t, p, "POST", "/snapshots", "StartSnapshot", body, nil)
	rb := parseBody(t, startResp)
	snapshotID := rb["SnapshotId"].(string)

	// Put some blocks
	for _, idx := range []string{"0", "1", "2"} {
		callOp(t, p, "PUT", "/snapshots/"+snapshotID+"/blocks/"+idx, "PutSnapshotBlock",
			[]byte("data-"+idx), map[string]string{
				"x-amz-Checksum":           "chk" + idx,
				"x-amz-Checksum-Algorithm": "SHA256",
				"x-amz-Data-Length":        "6",
			})
	}

	// ListSnapshotBlocks
	listResp := callOp(t, p, "GET", "/snapshots/"+snapshotID+"/blocks", "ListSnapshotBlocks", nil, nil)
	assert.Equal(t, 200, listResp.StatusCode)
	rb2 := parseBody(t, listResp)
	blocks, ok := rb2["Blocks"].([]any)
	require.True(t, ok)
	assert.Len(t, blocks, 3)

	// Create second snapshot and compare changed blocks
	startResp2 := callOp(t, p, "POST", "/snapshots", "StartSnapshot", body, nil)
	rb3 := parseBody(t, startResp2)
	snap2ID := rb3["SnapshotId"].(string)

	// Put block 1 with different data and new block 3
	callOp(t, p, "PUT", "/snapshots/"+snap2ID+"/blocks/1", "PutSnapshotBlock",
		[]byte("changed"), map[string]string{
			"x-amz-Checksum": "newchk1", "x-amz-Checksum-Algorithm": "SHA256", "x-amz-Data-Length": "7",
		})
	callOp(t, p, "PUT", "/snapshots/"+snap2ID+"/blocks/3", "PutSnapshotBlock",
		[]byte("new"), map[string]string{
			"x-amz-Checksum": "newchk3", "x-amz-Checksum-Algorithm": "SHA256", "x-amz-Data-Length": "3",
		})

	// ListChangedBlocks
	changedResp := callOp(t, p, "GET",
		"/snapshots/"+snap2ID+"/changedblocks?firstSnapshotId="+snapshotID,
		"ListChangedBlocks", nil, nil)
	assert.Equal(t, 200, changedResp.StatusCode)
	rb4 := parseBody(t, changedResp)
	changedBlocks, ok := rb4["ChangedBlocks"].([]any)
	require.True(t, ok)
	assert.GreaterOrEqual(t, len(changedBlocks), 1)
}
