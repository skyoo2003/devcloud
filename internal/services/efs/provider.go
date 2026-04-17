// SPDX-License-Identifier: Apache-2.0

package efs

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"strings"
	"time"

	generated "github.com/skyoo2003/devcloud/internal/generated/efs"
	"github.com/skyoo2003/devcloud/internal/plugin"
	"github.com/skyoo2003/devcloud/internal/shared"
)

const (
	defaultAccountID = plugin.DefaultAccountID
	defaultRegion    = "us-east-1"
)

// Provider implements the MagnolioAPIService_v20150201 service.
type Provider struct {
	generated.BaseProvider
	store *Store
}

func (p *Provider) ServiceID() string             { return "efs" }
func (p *Provider) ServiceName() string           { return "MagnolioAPIService_v20150201" }
func (p *Provider) Protocol() plugin.ProtocolType { return plugin.ProtocolRESTJSON }

func (p *Provider) Init(cfg plugin.PluginConfig) error {
	dir := cfg.DataDir
	if dir == "" {
		dir = "."
	}
	var err error
	p.store, err = NewStore(filepath.Join(dir, "efs"))
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
		var params generated.PathParams
		op, params = generated.MatchOperation(req.Method, req.URL.Path)
		_ = params
	}

	body, _ := io.ReadAll(req.Body)
	var bodyMap map[string]any
	if len(body) > 0 {
		json.Unmarshal(body, &bodyMap)
	}
	if bodyMap == nil {
		bodyMap = map[string]any{}
	}

	// Extract path params
	_, pathParams := generated.MatchOperation(req.Method, req.URL.Path)
	q := req.URL.Query()

	switch op {
	// --- FileSystems ---
	case "CreateFileSystem":
		return p.createFileSystem(bodyMap)
	case "DescribeFileSystems":
		fsID := ""
		if pathParams != nil {
			fsID = pathParams["FileSystemId"]
		}
		if fsID == "" {
			fsID = q.Get("FileSystemId")
		}
		return p.describeFileSystems(fsID)
	case "UpdateFileSystem":
		fsID := ""
		if pathParams != nil {
			fsID = pathParams["FileSystemId"]
		}
		return p.updateFileSystem(fsID, bodyMap)
	case "DeleteFileSystem":
		fsID := ""
		if pathParams != nil {
			fsID = pathParams["FileSystemId"]
		}
		return p.deleteFileSystem(fsID)

	// --- FileSystem Policy ---
	case "PutFileSystemPolicy":
		fsID := ""
		if pathParams != nil {
			fsID = pathParams["FileSystemId"]
		}
		return p.putFileSystemPolicy(fsID, bodyMap)
	case "DescribeFileSystemPolicy":
		fsID := ""
		if pathParams != nil {
			fsID = pathParams["FileSystemId"]
		}
		return p.describeFileSystemPolicy(fsID)
	case "DeleteFileSystemPolicy":
		fsID := ""
		if pathParams != nil {
			fsID = pathParams["FileSystemId"]
		}
		return p.deleteFileSystemPolicy(fsID)

	// --- Backup Policy ---
	case "PutBackupPolicy":
		fsID := ""
		if pathParams != nil {
			fsID = pathParams["FileSystemId"]
		}
		return p.putBackupPolicy(fsID, bodyMap)
	case "DescribeBackupPolicy":
		fsID := ""
		if pathParams != nil {
			fsID = pathParams["FileSystemId"]
		}
		return p.describeBackupPolicy(fsID)

	// --- Lifecycle Configuration ---
	case "PutLifecycleConfiguration":
		fsID := ""
		if pathParams != nil {
			fsID = pathParams["FileSystemId"]
		}
		return p.putLifecycleConfiguration(fsID, bodyMap)
	case "DescribeLifecycleConfiguration":
		fsID := ""
		if pathParams != nil {
			fsID = pathParams["FileSystemId"]
		}
		return p.describeLifecycleConfiguration(fsID)

	// --- MountTargets ---
	case "CreateMountTarget":
		return p.createMountTarget(bodyMap)
	case "DescribeMountTargets":
		fsID := q.Get("FileSystemId")
		mtID := q.Get("MountTargetId")
		return p.describeMountTargets(fsID, mtID)
	case "DeleteMountTarget":
		mtID := ""
		if pathParams != nil {
			mtID = pathParams["MountTargetId"]
		}
		return p.deleteMountTarget(mtID)
	case "DescribeMountTargetSecurityGroups":
		mtID := ""
		if pathParams != nil {
			mtID = pathParams["MountTargetId"]
		}
		return p.describeMountTargetSecurityGroups(mtID)
	case "ModifyMountTargetSecurityGroups":
		mtID := ""
		if pathParams != nil {
			mtID = pathParams["MountTargetId"]
		}
		return p.modifyMountTargetSecurityGroups(mtID, bodyMap)

	// --- AccessPoints ---
	case "CreateAccessPoint":
		return p.createAccessPoint(bodyMap)
	case "DescribeAccessPoints":
		apID := q.Get("AccessPointId")
		fsID := q.Get("FileSystemId")
		return p.describeAccessPoints(apID, fsID)
	case "DeleteAccessPoint":
		apID := ""
		if pathParams != nil {
			apID = pathParams["AccessPointId"]
		}
		return p.deleteAccessPoint(apID)

	// --- Tags ---
	case "TagResource":
		resID := ""
		if pathParams != nil {
			resID = pathParams["ResourceId"]
		}
		return p.tagResource(resID, bodyMap)
	case "ListTagsForResource":
		resID := ""
		if pathParams != nil {
			resID = pathParams["ResourceId"]
		}
		return p.listTagsForResource(resID)
	case "UntagResource":
		resID := ""
		if pathParams != nil {
			resID = pathParams["ResourceId"]
		}
		return p.untagResource(resID, q["tagKeys"])

	// --- Replication (stub) ---
	case "DescribeReplicationConfigurations":
		return shared.JSONResponse(http.StatusOK, map[string]any{"Replications": []any{}})

	// --- Account preferences (stub) ---
	case "DescribeAccountPreferences":
		return shared.JSONResponse(http.StatusOK, map[string]any{
			"ResourceIdPreference": map[string]any{
				"ResourceIdType": "LONG_ID",
				"Resources":      []string{"REPLICATION_CONFIGURATION", "FILE_SYSTEM"},
			},
		})

	default:
		return shared.JSONError("UnsupportedOperation", fmt.Sprintf("operation %q not implemented", op), http.StatusBadRequest), nil
	}
}

func (p *Provider) ListResources(_ context.Context) ([]plugin.Resource, error) {
	fss, err := p.store.ListFileSystems(defaultAccountID)
	if err != nil {
		return nil, err
	}
	out := make([]plugin.Resource, 0, len(fss))
	for _, fs := range fss {
		out = append(out, plugin.Resource{Type: "file-system", ID: fs.FileSystemID, Name: fs.Name})
	}
	return out, nil
}

func (p *Provider) GetMetrics(_ context.Context) (*plugin.ServiceMetrics, error) {
	return &plugin.ServiceMetrics{}, nil
}

// --- helpers ---

func efsARN(fsID string) string {
	return fmt.Sprintf("arn:aws:elasticfilesystem:%s:%s:file-system/%s", defaultRegion, defaultAccountID, fsID)
}

func apARN(apID string) string {
	return fmt.Sprintf("arn:aws:elasticfilesystem:%s:%s:access-point/%s", defaultRegion, defaultAccountID, apID)
}

func fsToDesc(fs *fileSystemRow, tags map[string]string) map[string]any {
	tagList := make([]map[string]any, 0, len(tags))
	for k, v := range tags {
		tagList = append(tagList, map[string]any{"Key": k, "Value": v})
	}
	return map[string]any{
		"FileSystemId":         fs.FileSystemID,
		"FileSystemArn":        efsARN(fs.FileSystemID),
		"CreationToken":        fs.CreationToken,
		"CreationTime":         fs.CreatedAt.Unix(),
		"LifeCycleState":       fs.LifecycleState,
		"Name":                 fs.Name,
		"NumberOfMountTargets": fs.NumberOfMountTargets,
		"SizeInBytes":          map[string]any{"Value": fs.SizeBytes, "ValueInIA": 0, "ValueInStandard": fs.SizeBytes},
		"PerformanceMode":      fs.PerformanceMode,
		"ThroughputMode":       fs.ThroughputMode,
		"Encrypted":            fs.Encrypted,
		"KmsKeyId":             fs.KMSKeyID,
		"OwnerId":              fs.AccountID,
		"Tags":                 tagList,
	}
}

func mtToDesc(mt *mountTargetRow) map[string]any {
	return map[string]any{
		"MountTargetId":      mt.MountTargetID,
		"FileSystemId":       mt.FileSystemID,
		"SubnetId":           mt.SubnetID,
		"IpAddress":          mt.IPAddress,
		"LifeCycleState":     mt.LifecycleState,
		"OwnerId":            mt.AccountID,
		"VpcId":              "vpc-00000000",
		"NetworkInterfaceId": "",
	}
}

func apToDesc(ap *accessPointRow, tags map[string]string) map[string]any {
	tagList := make([]map[string]any, 0, len(tags))
	for k, v := range tags {
		tagList = append(tagList, map[string]any{"Key": k, "Value": v})
	}
	var posixUser any
	if ap.PosixUser != "" {
		var pu map[string]any
		json.Unmarshal([]byte(ap.PosixUser), &pu)
		posixUser = pu
	}
	var rootDir any
	if ap.RootDirectory != "" {
		var rd map[string]any
		json.Unmarshal([]byte(ap.RootDirectory), &rd)
		rootDir = rd
	}
	return map[string]any{
		"AccessPointId":  ap.AccessPointID,
		"AccessPointArn": apARN(ap.AccessPointID),
		"FileSystemId":   ap.FileSystemID,
		"Name":           ap.Name,
		"ClientToken":    ap.ClientToken,
		"LifeCycleState": "available",
		"OwnerId":        ap.AccountID,
		"PosixUser":      posixUser,
		"RootDirectory":  rootDir,
		"Tags":           tagList,
	}
}

// --- FileSystem operations ---

func (p *Provider) createFileSystem(body map[string]any) (*plugin.Response, error) {
	token := strVal(body, "CreationToken")
	if token == "" {
		token = shared.GenerateUUID()
	}
	// Idempotent: return existing if same token
	if existing, err := p.store.GetFileSystemByToken(token); err == nil {
		tags, _ := p.store.ListTags(existing.FileSystemID)
		return shared.JSONResponse(http.StatusOK, fsToDesc(existing, tags))
	}

	fsID := shared.GenerateID("fs-", 11)
	perfMode := strVal(body, "PerformanceMode")
	if perfMode == "" {
		perfMode = "generalPurpose"
	}
	tpMode := strVal(body, "ThroughputMode")
	if tpMode == "" {
		tpMode = "bursting"
	}
	encrypted := boolVal(body, "Encrypted")
	kmsKey := strVal(body, "KmsKeyId")

	r := &fileSystemRow{
		FileSystemID:    fsID,
		CreationToken:   token,
		AccountID:       defaultAccountID,
		PerformanceMode: perfMode,
		ThroughputMode:  tpMode,
		Encrypted:       encrypted,
		KMSKeyID:        kmsKey,
		LifecycleState:  "available",
		BackupPolicy:    "DISABLED",
		CreatedAt:       time.Now().UTC(),
	}
	if err := p.store.CreateFileSystem(r); err != nil {
		return shared.JSONError("InternalError", err.Error(), http.StatusInternalServerError), nil
	}

	// Handle tags
	if tagsRaw, ok := body["Tags"].([]any); ok {
		tags := parseTagList(tagsRaw)
		p.store.TagResource(fsID, tags)
	}

	tags, _ := p.store.ListTags(fsID)
	return shared.JSONResponse(http.StatusCreated, fsToDesc(r, tags))
}

func (p *Provider) describeFileSystems(fsID string) (*plugin.Response, error) {
	if fsID != "" {
		fs, err := p.store.GetFileSystem(fsID)
		if err != nil {
			return shared.JSONError("FileSystemNotFound", "file system not found", http.StatusNotFound), nil
		}
		tags, _ := p.store.ListTags(fsID)
		return shared.JSONResponse(http.StatusOK, map[string]any{
			"FileSystems": []any{fsToDesc(fs, tags)},
		})
	}
	fss, err := p.store.ListFileSystems(defaultAccountID)
	if err != nil {
		return shared.JSONError("InternalError", err.Error(), http.StatusInternalServerError), nil
	}
	descs := make([]any, 0, len(fss))
	for _, fs := range fss {
		tags, _ := p.store.ListTags(fs.FileSystemID)
		descs = append(descs, fsToDesc(fs, tags))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"FileSystems": descs})
}

func (p *Provider) updateFileSystem(fsID string, body map[string]any) (*plugin.Response, error) {
	fs, err := p.store.GetFileSystem(fsID)
	if err != nil {
		return shared.JSONError("FileSystemNotFound", "file system not found", http.StatusNotFound), nil
	}
	tpMode := strVal(body, "ThroughputMode")
	if tpMode == "" {
		tpMode = fs.ThroughputMode
	}
	if err := p.store.UpdateFileSystem(fsID, tpMode); err != nil {
		return shared.JSONError("InternalError", err.Error(), http.StatusInternalServerError), nil
	}
	fs.ThroughputMode = tpMode
	tags, _ := p.store.ListTags(fsID)
	return shared.JSONResponse(http.StatusAccepted, fsToDesc(fs, tags))
}

func (p *Provider) deleteFileSystem(fsID string) (*plugin.Response, error) {
	if err := p.store.DeleteFileSystem(fsID); err != nil {
		return shared.JSONError("FileSystemNotFound", "file system not found", http.StatusNotFound), nil
	}
	return &plugin.Response{StatusCode: http.StatusNoContent, Body: []byte{}}, nil
}

func (p *Provider) putFileSystemPolicy(fsID string, body map[string]any) (*plugin.Response, error) {
	if _, err := p.store.GetFileSystem(fsID); err != nil {
		return shared.JSONError("FileSystemNotFound", "file system not found", http.StatusNotFound), nil
	}
	policy := strVal(body, "Policy")
	if err := p.store.PutFileSystemPolicy(fsID, policy); err != nil {
		return shared.JSONError("InternalError", err.Error(), http.StatusInternalServerError), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"FileSystemId": fsID, "Policy": policy})
}

func (p *Provider) describeFileSystemPolicy(fsID string) (*plugin.Response, error) {
	fs, err := p.store.GetFileSystem(fsID)
	if err != nil {
		return shared.JSONError("FileSystemNotFound", "file system not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"FileSystemId": fsID, "Policy": fs.FileSystemPolicy})
}

func (p *Provider) deleteFileSystemPolicy(fsID string) (*plugin.Response, error) {
	if err := p.store.DeleteFileSystemPolicy(fsID); err != nil {
		return shared.JSONError("FileSystemNotFound", "file system not found", http.StatusNotFound), nil
	}
	return &plugin.Response{StatusCode: http.StatusNoContent, Body: []byte{}}, nil
}

func (p *Provider) putBackupPolicy(fsID string, body map[string]any) (*plugin.Response, error) {
	if _, err := p.store.GetFileSystem(fsID); err != nil {
		return shared.JSONError("FileSystemNotFound", "file system not found", http.StatusNotFound), nil
	}
	status := "DISABLED"
	if bp, ok := body["BackupPolicy"].(map[string]any); ok {
		status = strVal(bp, "Status")
	}
	if err := p.store.PutBackupPolicy(fsID, status); err != nil {
		return shared.JSONError("InternalError", err.Error(), http.StatusInternalServerError), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"BackupPolicy": map[string]any{"Status": status}})
}

func (p *Provider) describeBackupPolicy(fsID string) (*plugin.Response, error) {
	fs, err := p.store.GetFileSystem(fsID)
	if err != nil {
		return shared.JSONError("FileSystemNotFound", "file system not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"BackupPolicy": map[string]any{"Status": fs.BackupPolicy}})
}

func (p *Provider) putLifecycleConfiguration(fsID string, body map[string]any) (*plugin.Response, error) {
	if _, err := p.store.GetFileSystem(fsID); err != nil {
		return shared.JSONError("FileSystemNotFound", "file system not found", http.StatusNotFound), nil
	}
	var policies []map[string]any
	if lc, ok := body["LifecyclePolicies"].([]any); ok {
		for _, item := range lc {
			if m, ok := item.(map[string]any); ok {
				policies = append(policies, m)
			}
		}
	}
	if err := p.store.PutLifecycleConfig(fsID, policies); err != nil {
		return shared.JSONError("InternalError", err.Error(), http.StatusInternalServerError), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"LifecyclePolicies": policies})
}

func (p *Provider) describeLifecycleConfiguration(fsID string) (*plugin.Response, error) {
	fs, err := p.store.GetFileSystem(fsID)
	if err != nil {
		return shared.JSONError("FileSystemNotFound", "file system not found", http.StatusNotFound), nil
	}
	var policies []any
	if fs.LifecycleConfig != "" {
		json.Unmarshal([]byte(fs.LifecycleConfig), &policies)
	}
	if policies == nil {
		policies = []any{}
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"LifecyclePolicies": policies})
}

// --- MountTarget operations ---

func (p *Provider) createMountTarget(body map[string]any) (*plugin.Response, error) {
	fsID := strVal(body, "FileSystemId")
	if _, err := p.store.GetFileSystem(fsID); err != nil {
		return shared.JSONError("FileSystemNotFound", "file system not found", http.StatusNotFound), nil
	}
	subnetID := strVal(body, "SubnetId")
	ipAddr := strVal(body, "IpAddress")
	if ipAddr == "" {
		ipAddr = "10.0.1.100"
	}
	var sgs []string
	if sgRaw, ok := body["SecurityGroups"].([]any); ok {
		for _, sg := range sgRaw {
			if s, ok := sg.(string); ok {
				sgs = append(sgs, s)
			}
		}
	}
	mtID := shared.GenerateID("fsmt-", 13)
	r := &mountTargetRow{
		MountTargetID:  mtID,
		FileSystemID:   fsID,
		SubnetID:       subnetID,
		IPAddress:      ipAddr,
		SecurityGroups: sgs,
		LifecycleState: "available",
		AccountID:      defaultAccountID,
		CreatedAt:      time.Now().UTC(),
	}
	if err := p.store.CreateMountTarget(r); err != nil {
		return shared.JSONError("InternalError", err.Error(), http.StatusInternalServerError), nil
	}
	return shared.JSONResponse(http.StatusOK, mtToDesc(r))
}

func (p *Provider) describeMountTargets(fsID, mtID string) (*plugin.Response, error) {
	var mts []*mountTargetRow
	var err error
	if mtID != "" {
		mt, e := p.store.GetMountTarget(mtID)
		if e != nil {
			return shared.JSONError("MountTargetNotFound", "mount target not found", http.StatusNotFound), nil
		}
		mts = []*mountTargetRow{mt}
	} else if fsID != "" {
		mts, err = p.store.ListMountTargetsByFS(fsID)
	} else {
		mts, err = p.store.ListAllMountTargets(defaultAccountID)
	}
	if err != nil {
		return shared.JSONError("InternalError", err.Error(), http.StatusInternalServerError), nil
	}
	descs := make([]any, 0, len(mts))
	for _, mt := range mts {
		descs = append(descs, mtToDesc(mt))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"MountTargets": descs})
}

func (p *Provider) deleteMountTarget(mtID string) (*plugin.Response, error) {
	if err := p.store.DeleteMountTarget(mtID); err != nil {
		return shared.JSONError("MountTargetNotFound", "mount target not found", http.StatusNotFound), nil
	}
	return &plugin.Response{StatusCode: http.StatusNoContent, Body: []byte{}}, nil
}

func (p *Provider) describeMountTargetSecurityGroups(mtID string) (*plugin.Response, error) {
	mt, err := p.store.GetMountTarget(mtID)
	if err != nil {
		return shared.JSONError("MountTargetNotFound", "mount target not found", http.StatusNotFound), nil
	}
	sgs := mt.SecurityGroups
	if sgs == nil {
		sgs = []string{}
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"SecurityGroups": sgs})
}

func (p *Provider) modifyMountTargetSecurityGroups(mtID string, body map[string]any) (*plugin.Response, error) {
	var sgs []string
	if sgRaw, ok := body["SecurityGroups"].([]any); ok {
		for _, sg := range sgRaw {
			if s, ok := sg.(string); ok {
				sgs = append(sgs, s)
			}
		}
	}
	if err := p.store.UpdateMountTargetSecurityGroups(mtID, sgs); err != nil {
		return shared.JSONError("MountTargetNotFound", "mount target not found", http.StatusNotFound), nil
	}
	return &plugin.Response{StatusCode: http.StatusNoContent, Body: []byte{}}, nil
}

// --- AccessPoint operations ---

func (p *Provider) createAccessPoint(body map[string]any) (*plugin.Response, error) {
	fsID := strVal(body, "FileSystemId")
	if _, err := p.store.GetFileSystem(fsID); err != nil {
		return shared.JSONError("FileSystemNotFound", "file system not found", http.StatusNotFound), nil
	}
	apID := shared.GenerateID("fsap-", 13)
	clientToken := strVal(body, "ClientToken")
	var posixUser, rootDir string
	if pu, ok := body["PosixUser"]; ok {
		b, _ := json.Marshal(pu)
		posixUser = string(b)
	}
	if rd, ok := body["RootDirectory"]; ok {
		b, _ := json.Marshal(rd)
		rootDir = string(b)
	}
	name := ""
	if tagsRaw, ok := body["Tags"].([]any); ok {
		for _, t := range tagsRaw {
			if m, ok := t.(map[string]any); ok {
				if k, _ := m["Key"].(string); strings.EqualFold(k, "Name") {
					name, _ = m["Value"].(string)
				}
			}
		}
	}
	r := &accessPointRow{
		AccessPointID: apID,
		FileSystemID:  fsID,
		Name:          name,
		ClientToken:   clientToken,
		PosixUser:     posixUser,
		RootDirectory: rootDir,
		AccountID:     defaultAccountID,
		CreatedAt:     time.Now().UTC(),
	}
	if err := p.store.CreateAccessPoint(r); err != nil {
		return shared.JSONError("InternalError", err.Error(), http.StatusInternalServerError), nil
	}
	if tagsRaw, ok := body["Tags"].([]any); ok {
		tags := parseTagList(tagsRaw)
		p.store.TagResource(apID, tags)
	}
	tags, _ := p.store.ListTags(apID)
	return shared.JSONResponse(http.StatusOK, apToDesc(r, tags))
}

func (p *Provider) describeAccessPoints(apID, fsID string) (*plugin.Response, error) {
	if apID != "" {
		ap, err := p.store.GetAccessPoint(apID)
		if err != nil {
			return shared.JSONError("AccessPointNotFound", "access point not found", http.StatusNotFound), nil
		}
		tags, _ := p.store.ListTags(apID)
		return shared.JSONResponse(http.StatusOK, map[string]any{
			"AccessPoints": []any{apToDesc(ap, tags)},
		})
	}
	aps, err := p.store.ListAccessPoints(defaultAccountID, fsID)
	if err != nil {
		return shared.JSONError("InternalError", err.Error(), http.StatusInternalServerError), nil
	}
	descs := make([]any, 0, len(aps))
	for _, ap := range aps {
		tags, _ := p.store.ListTags(ap.AccessPointID)
		descs = append(descs, apToDesc(ap, tags))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"AccessPoints": descs})
}

func (p *Provider) deleteAccessPoint(apID string) (*plugin.Response, error) {
	if err := p.store.DeleteAccessPoint(apID); err != nil {
		return shared.JSONError("AccessPointNotFound", "access point not found", http.StatusNotFound), nil
	}
	return &plugin.Response{StatusCode: http.StatusNoContent, Body: []byte{}}, nil
}

// --- Tag operations ---

func (p *Provider) tagResource(resourceID string, body map[string]any) (*plugin.Response, error) {
	if tagsRaw, ok := body["Tags"].([]any); ok {
		tags := parseTagList(tagsRaw)
		if err := p.store.TagResource(resourceID, tags); err != nil {
			return shared.JSONError("InternalError", err.Error(), http.StatusInternalServerError), nil
		}
	}
	return &plugin.Response{StatusCode: http.StatusNoContent, Body: []byte{}}, nil
}

func (p *Provider) listTagsForResource(resourceID string) (*plugin.Response, error) {
	tags, err := p.store.ListTags(resourceID)
	if err != nil {
		return shared.JSONError("InternalError", err.Error(), http.StatusInternalServerError), nil
	}
	tagList := make([]map[string]any, 0, len(tags))
	for k, v := range tags {
		tagList = append(tagList, map[string]any{"Key": k, "Value": v})
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"Tags": tagList})
}

func (p *Provider) untagResource(resourceID string, keys []string) (*plugin.Response, error) {
	if err := p.store.UntagResource(resourceID, keys); err != nil {
		return shared.JSONError("InternalError", err.Error(), http.StatusInternalServerError), nil
	}
	return &plugin.Response{StatusCode: http.StatusNoContent, Body: []byte{}}, nil
}

// --- helpers ---

func strVal(m map[string]any, key string) string {
	if v, ok := m[key]; ok {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}

func boolVal(m map[string]any, key string) bool {
	if v, ok := m[key]; ok {
		if b, ok := v.(bool); ok {
			return b
		}
	}
	return false
}

func parseTagList(raw []any) map[string]string {
	tags := make(map[string]string)
	for _, t := range raw {
		if m, ok := t.(map[string]any); ok {
			k, _ := m["Key"].(string)
			v, _ := m["Value"].(string)
			if k != "" {
				tags[k] = v
			}
		}
	}
	return tags
}
