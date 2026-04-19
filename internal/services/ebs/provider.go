// SPDX-License-Identifier: Apache-2.0

// internal/services/ebs/provider.go
package ebs

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"strings"
	"time"

	"github.com/skyoo2003/devcloud/internal/plugin"
	"github.com/skyoo2003/devcloud/internal/shared"
)

// Provider implements the Ebs service.
type Provider struct {
	store *Store
}

func (p *Provider) ServiceID() string             { return "ebs" }
func (p *Provider) ServiceName() string           { return "Ebs" }
func (p *Provider) Protocol() plugin.ProtocolType { return plugin.ProtocolRESTJSON }

func (p *Provider) Init(cfg plugin.PluginConfig) error {
	dataDir := cfg.DataDir
	if dataDir == "" {
		dataDir = "."
	}
	var err error
	p.store, err = NewStore(filepath.Join(dataDir, "ebs"))
	return err
}

func (p *Provider) Shutdown(_ context.Context) error {
	if p.store != nil {
		return p.store.Close()
	}
	return nil
}

func (p *Provider) HandleRequest(_ context.Context, op string, req *http.Request) (*plugin.Response, error) {
	if op == "" {
		op = resolveOp(req.Method, req.URL.Path)
	}
	switch op {
	case "StartSnapshot":
		return p.startSnapshot(req)
	case "CompleteSnapshot":
		snapshotID := extractPathSegment(req.URL.Path, "completion")
		return p.completeSnapshot(snapshotID, req)
	case "PutSnapshotBlock":
		snapshotID, blockIndex := extractTwoPathSegments(req.URL.Path, "blocks")
		return p.putSnapshotBlock(snapshotID, blockIndex, req)
	case "GetSnapshotBlock":
		snapshotID, blockIndex := extractTwoPathSegments(req.URL.Path, "blocks")
		return p.getSnapshotBlock(snapshotID, blockIndex)
	case "ListSnapshotBlocks":
		snapshotID := extractPathSegment(req.URL.Path, "snapshots")
		return p.listSnapshotBlocks(snapshotID, req)
	case "ListChangedBlocks":
		secondSnapshotID := extractPathSegment(req.URL.Path, "snapshots")
		return p.listChangedBlocks(secondSnapshotID, req)
	case "ModifyVolume":
		return p.modifyVolume(req)
	case "DescribeVolumesModifications":
		return p.describeVolumesModifications(req)
	case "CopySnapshot":
		return p.copySnapshot(req)
	case "ModifySnapshotAttribute":
		return p.modifySnapshotAttribute(req)
	case "EnableSnapshotTierArchival":
		return p.enableSnapshotTierArchival(req)
	case "RestoreSnapshotTier":
		return p.restoreSnapshotTier(req)
	default:
		return shared.JSONError("InvalidAction", fmt.Sprintf("unknown action: %s", op), http.StatusBadRequest), nil
	}
}

func (p *Provider) ListResources(_ context.Context) ([]plugin.Resource, error) {
	snaps, err := p.store.ListSnapshots()
	if err != nil {
		return nil, err
	}
	res := make([]plugin.Resource, 0, len(snaps))
	for _, s := range snaps {
		res = append(res, plugin.Resource{Type: "ebs-snapshot", ID: s.ID, Name: s.ID})
	}
	return res, nil
}

func (p *Provider) GetMetrics(_ context.Context) (*plugin.ServiceMetrics, error) {
	return &plugin.ServiceMetrics{}, nil
}

// --- Operations ---

func (p *Provider) startSnapshot(req *http.Request) (*plugin.Response, error) {
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

	volumeSize := int64(1)
	if v, ok := params["VolumeSize"].(float64); ok && v > 0 {
		volumeSize = int64(v)
	}
	description, _ := params["Description"].(string)
	parentSnapshotID, _ := params["ParentSnapshotId"].(string)

	id := shared.GenerateID("snap-", 21)
	arn := shared.BuildARN("ec2", "snapshot", id)

	snap := &Snapshot{
		ID:         id,
		ARN:        arn,
		VolumeSize: volumeSize,
		Status:     "pending",
		BlockSize:  524288,
	}
	if err := p.store.CreateSnapshot(snap); err != nil {
		return nil, err
	}

	return shared.JSONResponse(http.StatusOK, map[string]any{
		"SnapshotId":       id,
		"BlockSize":        snap.BlockSize,
		"VolumeSize":       volumeSize,
		"Status":           "pending",
		"Description":      description,
		"ParentSnapshotId": parentSnapshotID,
		"OwnerId":          shared.DefaultAccountID,
		"StartTime":        time.Now().UTC().Format(time.RFC3339),
	})
}

func (p *Provider) completeSnapshot(snapshotID string, req *http.Request) (*plugin.Response, error) {
	if snapshotID == "" {
		return shared.JSONError("ValidationException", "snapshot ID is required", http.StatusBadRequest), nil
	}
	if _, err := p.store.GetSnapshot(snapshotID); err != nil {
		return shared.JSONError("ResourceNotFoundException", "snapshot not found", http.StatusNotFound), nil
	}
	if err := p.store.UpdateSnapshotStatus(snapshotID, "completed"); err != nil {
		return shared.JSONError("ResourceNotFoundException", "snapshot not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"Status": "completed"})
}

func (p *Provider) putSnapshotBlock(snapshotID, blockIndexStr string, req *http.Request) (*plugin.Response, error) {
	if snapshotID == "" {
		return shared.JSONError("ValidationException", "snapshot ID is required", http.StatusBadRequest), nil
	}
	if _, err := p.store.GetSnapshot(snapshotID); err != nil {
		return shared.JSONError("ResourceNotFoundException", "snapshot not found", http.StatusNotFound), nil
	}

	var blockIndex int32
	if blockIndexStr != "" {
		var n int32
		_, _ = fmt.Sscanf(blockIndexStr, "%d", &n)
		blockIndex = n
	}

	data, err := io.ReadAll(req.Body)
	if err != nil {
		return shared.JSONError("SerializationException", "failed to read block data", http.StatusBadRequest), nil
	}

	checksum := req.Header.Get("x-amz-Checksum")
	if checksum == "" {
		checksum = req.Header.Get("x-amz-checksum")
	}
	checksumAlgorithm := req.Header.Get("x-amz-Checksum-Algorithm")
	if checksumAlgorithm == "" {
		checksumAlgorithm = "SHA256"
	}

	blockToken := shared.GenerateID("", 32)

	block := &SnapshotBlock{
		SnapshotID: snapshotID,
		BlockIndex: blockIndex,
		BlockToken: blockToken,
		Checksum:   checksum,
		Data:       data,
	}
	if err := p.store.PutBlock(block); err != nil {
		return nil, err
	}

	return shared.JSONResponse(http.StatusOK, map[string]any{
		"Checksum":          checksum,
		"ChecksumAlgorithm": checksumAlgorithm,
	})
}

func (p *Provider) getSnapshotBlock(snapshotID, blockIndexStr string) (*plugin.Response, error) {
	if snapshotID == "" {
		return shared.JSONError("ValidationException", "snapshot ID is required", http.StatusBadRequest), nil
	}

	var blockIndex int32
	if blockIndexStr != "" {
		var n int32
		_, _ = fmt.Sscanf(blockIndexStr, "%d", &n)
		blockIndex = n
	}

	block, err := p.store.GetBlock(snapshotID, blockIndex)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "block not found", http.StatusNotFound), nil
	}

	dataLen := len(block.Data)
	resp, err := shared.JSONResponse(http.StatusOK, map[string]any{
		"BlockData":         base64.StdEncoding.EncodeToString(block.Data),
		"Checksum":          block.Checksum,
		"ChecksumAlgorithm": "SHA256",
		"DataLength":        dataLen,
	})
	if err != nil {
		return nil, err
	}
	if resp.Headers == nil {
		resp.Headers = map[string]string{}
	}
	resp.Headers["x-amz-Data-Length"] = fmt.Sprintf("%d", dataLen)
	resp.Headers["x-amz-Checksum"] = block.Checksum
	resp.Headers["x-amz-Checksum-Algorithm"] = "SHA256"
	return resp, nil
}

func (p *Provider) listSnapshotBlocks(snapshotID string, req *http.Request) (*plugin.Response, error) {
	if snapshotID == "" {
		return shared.JSONError("ValidationException", "snapshot ID is required", http.StatusBadRequest), nil
	}
	snap, err := p.store.GetSnapshot(snapshotID)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "snapshot not found", http.StatusNotFound), nil
	}

	var startIndex int32
	if v := req.URL.Query().Get("startingBlockIndex"); v != "" {
		_, _ = fmt.Sscanf(v, "%d", &startIndex)
	}

	blocks, err := p.store.ListBlocks(snapshotID, startIndex)
	if err != nil {
		return nil, err
	}

	blockList := make([]map[string]any, 0, len(blocks))
	for _, b := range blocks {
		blockList = append(blockList, map[string]any{
			"BlockIndex": b.BlockIndex,
			"BlockToken": b.BlockToken,
		})
	}

	return shared.JSONResponse(http.StatusOK, map[string]any{
		"BlockSize":  snap.BlockSize,
		"Blocks":     blockList,
		"VolumeSize": snap.VolumeSize,
		"ExpiryTime": time.Now().Add(24 * time.Hour).UTC().Format(time.RFC3339),
	})
}

func (p *Provider) listChangedBlocks(secondSnapshotID string, req *http.Request) (*plugin.Response, error) {
	if secondSnapshotID == "" {
		return shared.JSONError("ValidationException", "snapshot ID is required", http.StatusBadRequest), nil
	}
	snap, err := p.store.GetSnapshot(secondSnapshotID)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "snapshot not found", http.StatusNotFound), nil
	}

	firstSnapshotID := req.URL.Query().Get("firstSnapshotId")

	var startIndex int32
	if v := req.URL.Query().Get("startingBlockIndex"); v != "" {
		_, _ = fmt.Sscanf(v, "%d", &startIndex)
	}

	blocks, err := p.store.ListChangedBlocks(firstSnapshotID, secondSnapshotID, startIndex)
	if err != nil {
		return nil, err
	}

	blockList := make([]map[string]any, 0, len(blocks))
	for _, b := range blocks {
		entry := map[string]any{
			"BlockIndex":       b.BlockIndex,
			"SecondBlockToken": b.BlockToken,
		}
		if b.Checksum != "" {
			entry["FirstBlockToken"] = b.Checksum
		}
		blockList = append(blockList, entry)
	}

	return shared.JSONResponse(http.StatusOK, map[string]any{
		"BlockSize":     snap.BlockSize,
		"ChangedBlocks": blockList,
		"VolumeSize":    snap.VolumeSize,
		"ExpiryTime":    time.Now().Add(24 * time.Hour).UTC().Format(time.RFC3339),
	})
}

func (p *Provider) modifyVolume(req *http.Request) (*plugin.Response, error) {
	body, _ := io.ReadAll(req.Body)
	var params map[string]any
	if len(body) > 0 {
		_ = json.Unmarshal(body, &params)
	} else {
		params = map[string]any{}
	}

	volumeID, _ := params["VolumeId"].(string)
	if volumeID == "" {
		return shared.JSONError("ValidationException", "VolumeId is required", http.StatusBadRequest), nil
	}

	mod := &VolumeModification{
		ID:                shared.GenerateID("volmod-", 20),
		VolumeID:          volumeID,
		ModificationState: "completed",
		Progress:          100,
		StartTime:         time.Now(),
		AccountID:         shared.DefaultAccountID,
	}
	if v, ok := params["Size"].(float64); ok {
		mod.TargetSize = int64(v)
	}
	if v, ok := params["VolumeType"].(string); ok {
		mod.TargetType = v
	}
	if v, ok := params["Iops"].(float64); ok {
		mod.TargetIops = int64(v)
	}

	if err := p.store.CreateVolumeModification(mod); err != nil {
		return nil, err
	}

	return shared.JSONResponse(http.StatusOK, map[string]any{
		"VolumeModification": map[string]any{
			"VolumeId":          volumeID,
			"ModificationState": "completed",
			"TargetSize":        mod.TargetSize,
			"TargetVolumeType":  mod.TargetType,
			"TargetIops":        mod.TargetIops,
			"Progress":          mod.Progress,
			"StartTime":         mod.StartTime.UTC().Format(time.RFC3339),
		},
	})
}

func (p *Provider) describeVolumesModifications(req *http.Request) (*plugin.Response, error) {
	volumeID := req.URL.Query().Get("VolumeId.1")
	mods, err := p.store.ListVolumeModifications(volumeID)
	if err != nil {
		return nil, err
	}

	items := make([]map[string]any, 0, len(mods))
	for _, m := range mods {
		items = append(items, map[string]any{
			"VolumeId":          m.VolumeID,
			"ModificationState": m.ModificationState,
			"TargetSize":        m.TargetSize,
			"TargetVolumeType":  m.TargetType,
			"TargetIops":        m.TargetIops,
			"Progress":          m.Progress,
			"StartTime":         m.StartTime.UTC().Format(time.RFC3339),
		})
	}

	return shared.JSONResponse(http.StatusOK, map[string]any{
		"VolumesModifications": items,
	})
}

func (p *Provider) copySnapshot(req *http.Request) (*plugin.Response, error) {
	body, _ := io.ReadAll(req.Body)
	var params map[string]any
	if len(body) > 0 {
		_ = json.Unmarshal(body, &params)
	} else {
		params = map[string]any{}
	}

	sourceSnapshotID, _ := params["SourceSnapshotId"].(string)
	if sourceSnapshotID == "" {
		sourceSnapshotID = req.URL.Query().Get("SourceSnapshotId")
	}
	if sourceSnapshotID == "" {
		return shared.JSONError("ValidationException", "SourceSnapshotId is required", http.StatusBadRequest), nil
	}

	newID := shared.GenerateID("snap-", 21)
	newARN := shared.BuildARN("ec2", "snapshot", newID)

	if err := p.store.CopySnapshot(sourceSnapshotID, newID, newARN); err != nil {
		return shared.JSONError("InvalidSnapshot.NotFound", "source snapshot not found", http.StatusNotFound), nil
	}

	return shared.JSONResponse(http.StatusOK, map[string]any{
		"SnapshotId": newID,
	})
}

func (p *Provider) modifySnapshotAttribute(req *http.Request) (*plugin.Response, error) {
	body, _ := io.ReadAll(req.Body)
	var params map[string]any
	if len(body) > 0 {
		_ = json.Unmarshal(body, &params)
	} else {
		params = map[string]any{}
	}

	snapshotID, _ := params["SnapshotId"].(string)
	if snapshotID == "" {
		snapshotID = req.URL.Query().Get("SnapshotId")
	}
	if snapshotID == "" {
		return shared.JSONError("ValidationException", "SnapshotId is required", http.StatusBadRequest), nil
	}

	if _, err := p.store.GetSnapshot(snapshotID); err != nil {
		return shared.JSONError("InvalidSnapshot.NotFound", "snapshot not found", http.StatusNotFound), nil
	}

	return shared.JSONResponse(http.StatusOK, map[string]any{
		"Return": true,
	})
}

func (p *Provider) enableSnapshotTierArchival(req *http.Request) (*plugin.Response, error) {
	body, _ := io.ReadAll(req.Body)
	var params map[string]any
	if len(body) > 0 {
		_ = json.Unmarshal(body, &params)
	} else {
		params = map[string]any{}
	}

	snapshotID, _ := params["SnapshotId"].(string)
	if snapshotID == "" {
		snapshotID = req.URL.Query().Get("SnapshotId")
	}
	if snapshotID == "" {
		return shared.JSONError("ValidationException", "SnapshotId is required", http.StatusBadRequest), nil
	}

	if _, err := p.store.GetSnapshot(snapshotID); err != nil {
		return shared.JSONError("InvalidSnapshot.NotFound", "snapshot not found", http.StatusNotFound), nil
	}

	return shared.JSONResponse(http.StatusOK, map[string]any{
		"SnapshotId": snapshotID,
		"Status":     "archival-queued",
	})
}

func (p *Provider) restoreSnapshotTier(req *http.Request) (*plugin.Response, error) {
	body, _ := io.ReadAll(req.Body)
	var params map[string]any
	if len(body) > 0 {
		_ = json.Unmarshal(body, &params)
	} else {
		params = map[string]any{}
	}

	snapshotID, _ := params["SnapshotId"].(string)
	if snapshotID == "" {
		snapshotID = req.URL.Query().Get("SnapshotId")
	}
	if snapshotID == "" {
		return shared.JSONError("ValidationException", "SnapshotId is required", http.StatusBadRequest), nil
	}

	if _, err := p.store.GetSnapshot(snapshotID); err != nil {
		return shared.JSONError("InvalidSnapshot.NotFound", "snapshot not found", http.StatusNotFound), nil
	}

	return shared.JSONResponse(http.StatusOK, map[string]any{
		"SnapshotId":         snapshotID,
		"IsPermanentRestore": false,
		"RestoreDuration":    1,
	})
}

// resolveOp maps HTTP method+path to an EBS operation name.
func resolveOp(method, path string) string {
	segs := strings.Split(strings.Trim(path, "/"), "/")
	n := len(segs)

	if n == 0 || segs[0] != "snapshots" {
		return ""
	}

	switch {
	case n == 1 && method == "POST":
		// POST /snapshots → StartSnapshot
		return "StartSnapshot"
	case n >= 3 && segs[1] == "completion":
		// POST /snapshots/completion/{snapshotId} → CompleteSnapshot
		return "CompleteSnapshot"
	case n >= 4 && segs[2] == "blocks":
		if method == "PUT" {
			return "PutSnapshotBlock"
		}
		return "GetSnapshotBlock"
	case n == 3 && segs[2] == "blocks":
		// GET /snapshots/{id}/blocks → ListSnapshotBlocks
		return "ListSnapshotBlocks"
	case n == 3 && segs[2] == "changedblocks":
		// GET /snapshots/{id}/changedblocks → ListChangedBlocks
		return "ListChangedBlocks"
	}
	return ""
}

// --- Helpers ---

func extractPathSegment(path, after string) string {
	parts := strings.Split(strings.Trim(path, "/"), "/")
	for i, p := range parts {
		if p == after && i+1 < len(parts) {
			return parts[i+1]
		}
	}
	return ""
}

// extractTwoPathSegments returns the segment before and after the key.
// For /snapshots/{id}/blocks/{blockIndex}: key="blocks", returns (id, blockIndex).
func extractTwoPathSegments(path, key string) (string, string) {
	parts := strings.Split(strings.Trim(path, "/"), "/")
	for i, p := range parts {
		if p == key {
			before := ""
			after := ""
			if i >= 2 {
				before = parts[i-1]
			}
			if i+1 < len(parts) {
				after = parts[i+1]
			}
			return before, after
		}
	}
	return "", ""
}
