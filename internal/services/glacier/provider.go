// SPDX-License-Identifier: Apache-2.0

// internal/services/glacier/provider.go
package glacier

import (
	"context"
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

// Provider implements the Glacier service.
type Provider struct {
	store *Store
}

func (p *Provider) ServiceID() string             { return "glacier" }
func (p *Provider) ServiceName() string           { return "Glacier" }
func (p *Provider) Protocol() plugin.ProtocolType { return plugin.ProtocolRESTJSON }

func (p *Provider) Init(cfg plugin.PluginConfig) error {
	dataDir := cfg.DataDir
	if dataDir == "" {
		dataDir = "."
	}
	var err error
	p.store, err = NewStore(filepath.Join(dataDir, "glacier"))
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
		if jsonErr := json.Unmarshal(body, &params); jsonErr != nil {
			params = map[string]any{}
		}
	} else {
		params = map[string]any{}
	}

	if op == "" {
		op = resolveOp(req.Method, req.URL.Path, req.URL.Query().Get("operation"))
	}

	// Extract common path params
	vaultName := extractPathParam(req.URL.Path, "vaults")
	archiveID := extractPathParam(req.URL.Path, "archives")
	jobID := extractPathParam(req.URL.Path, "jobs")
	uploadID := extractPathParam(req.URL.Path, "multipart-uploads")
	lockID := extractPathParam(req.URL.Path, "lock-policy")

	switch op {
	// Vault CRUD
	case "CreateVault":
		return p.createVault(vaultName)
	case "DescribeVault":
		return p.describeVault(vaultName)
	case "ListVaults":
		return p.listVaults()
	case "DeleteVault":
		return p.deleteVault(vaultName)

	// Archive
	case "UploadArchive":
		return p.uploadArchive(vaultName, req, body)
	case "DeleteArchive":
		return p.deleteArchive(vaultName, archiveID)

	// Jobs
	case "InitiateJob":
		return p.initiateJob(vaultName, params)
	case "DescribeJob":
		return p.describeJob(vaultName, jobID)
	case "ListJobs":
		return p.listJobs(vaultName)
	case "GetJobOutput":
		return p.getJobOutput(vaultName, jobID)

	// Multipart Upload
	case "InitiateMultipartUpload":
		return p.initiateMultipartUpload(vaultName, req)
	case "UploadMultipartPart":
		return p.uploadMultipartPart(vaultName, uploadID, req, body)
	case "CompleteMultipartUpload":
		return p.completeMultipartUpload(vaultName, uploadID, req)
	case "AbortMultipartUpload":
		return p.abortMultipartUpload(vaultName, uploadID)
	case "ListMultipartUploads":
		return p.listMultipartUploads(vaultName)
	case "ListParts":
		return p.listParts(vaultName, uploadID)

	// Tags
	case "AddTagsToVault":
		return p.addTagsToVault(vaultName, params)
	case "RemoveTagsFromVault":
		return p.removeTagsFromVault(vaultName, params)
	case "ListTagsForVault":
		return p.listTagsForVault(vaultName)

	// Vault Access Policy
	case "SetVaultAccessPolicy":
		return p.setVaultAccessPolicy(vaultName, params)
	case "GetVaultAccessPolicy":
		return p.getVaultAccessPolicy(vaultName)
	case "DeleteVaultAccessPolicy":
		return p.deleteVaultAccessPolicy(vaultName)

	// Vault Notifications
	case "SetVaultNotifications":
		return p.setVaultNotifications(vaultName, params)
	case "GetVaultNotifications":
		return p.getVaultNotifications(vaultName)
	case "DeleteVaultNotifications":
		return p.deleteVaultNotifications(vaultName)

	// Vault Lock
	case "InitiateVaultLock":
		return p.initiateVaultLock(vaultName, params)
	case "CompleteVaultLock":
		return p.completeVaultLock(vaultName, lockID)
	case "AbortVaultLock":
		return p.abortVaultLock(vaultName)
	case "GetVaultLock":
		return p.getVaultLock(vaultName)

	// Data Retrieval Policy (global, stored in-memory for simplicity)
	case "GetDataRetrievalPolicy":
		return shared.JSONResponse(http.StatusOK, map[string]any{
			"Policy": map[string]any{"Rules": []any{}},
		})
	case "SetDataRetrievalPolicy":
		return shared.JSONResponse(http.StatusNoContent, nil)

	// Provisioned Capacity
	case "ListProvisionedCapacity":
		return shared.JSONResponse(http.StatusOK, map[string]any{
			"ProvisionedCapacityList": []any{},
		})
	case "PurchaseProvisionedCapacity":
		capID := shared.GenerateID("prov-", 20)
		return shared.JSONResponse(http.StatusOK, map[string]any{
			"CapacityId": capID,
		})

	default:
		return shared.JSONError("InvalidAction", fmt.Sprintf("unknown action: %s", op), http.StatusBadRequest), nil
	}
}

func resolveOp(method, path, queryOp string) string {
	// Glacier paths: /-/vaults, /-/vaults/{name}, /-/vaults/{name}/archives, etc.
	// Strip the /-/ prefix (account ID placeholder)
	p := path
	if strings.HasPrefix(p, "/-/") {
		p = p[3:]
	} else {
		p = strings.TrimPrefix(p, "/")
		// Also handle /{accountId}/vaults
		parts := strings.SplitN(p, "/", 2)
		if len(parts) == 2 {
			p = parts[1]
		}
	}
	p = strings.Trim(p, "/")
	seg := strings.Split(p, "/")
	n := len(seg)

	switch {
	case n >= 1 && seg[0] == "vaults":
		if n == 1 {
			return "ListVaults"
		}
		if n == 2 {
			switch method {
			case http.MethodPut:
				return "CreateVault"
			case http.MethodGet:
				return "DescribeVault"
			case http.MethodDelete:
				return "DeleteVault"
			}
		}
		// /vaults/{name}/archives
		if n >= 3 && seg[2] == "archives" {
			if n == 3 && method == http.MethodPost {
				return "UploadArchive"
			}
			if n == 4 && method == http.MethodDelete {
				return "DeleteArchive"
			}
		}
		// /vaults/{name}/jobs
		if n >= 3 && seg[2] == "jobs" {
			if n == 3 {
				switch method {
				case http.MethodPost:
					return "InitiateJob"
				case http.MethodGet:
					return "ListJobs"
				}
			}
			if n == 4 && method == http.MethodGet {
				return "DescribeJob"
			}
			// /vaults/{name}/jobs/{id}/output
			if n == 5 && seg[4] == "output" && method == http.MethodGet {
				return "GetJobOutput"
			}
		}
		// /vaults/{name}/multipart-uploads
		if n >= 3 && seg[2] == "multipart-uploads" {
			if n == 3 {
				switch method {
				case http.MethodPost:
					return "InitiateMultipartUpload"
				case http.MethodGet:
					return "ListMultipartUploads"
				}
			}
			if n == 4 {
				switch method {
				case http.MethodPut:
					return "UploadMultipartPart"
				case http.MethodPost:
					return "CompleteMultipartUpload"
				case http.MethodDelete:
					return "AbortMultipartUpload"
				case http.MethodGet:
					return "ListParts"
				}
			}
		}
		// /vaults/{name}/tags
		if n >= 3 && seg[2] == "tags" {
			switch method {
			case http.MethodPost:
				if queryOp == "remove" {
					return "RemoveTagsFromVault"
				}
				return "AddTagsToVault"
			case http.MethodGet:
				return "ListTagsForVault"
			}
		}
		// /vaults/{name}/access-policy
		if n >= 3 && seg[2] == "access-policy" {
			switch method {
			case http.MethodPut:
				return "SetVaultAccessPolicy"
			case http.MethodGet:
				return "GetVaultAccessPolicy"
			case http.MethodDelete:
				return "DeleteVaultAccessPolicy"
			}
		}
		// /vaults/{name}/notification-configuration
		if n >= 3 && seg[2] == "notification-configuration" {
			switch method {
			case http.MethodPut:
				return "SetVaultNotifications"
			case http.MethodGet:
				return "GetVaultNotifications"
			case http.MethodDelete:
				return "DeleteVaultNotifications"
			}
		}
		// /vaults/{name}/lock-policy
		if n >= 3 && seg[2] == "lock-policy" {
			switch method {
			case http.MethodPost:
				return "InitiateVaultLock"
			case http.MethodGet:
				return "GetVaultLock"
			case http.MethodDelete:
				return "AbortVaultLock"
			}
			// /vaults/{name}/lock-policy/{id}
			if n == 4 && method == http.MethodPost {
				return "CompleteVaultLock"
			}
		}

	// Data retrieval policy: /-/policies/data-retrieval
	case n >= 1 && seg[0] == "policies":
		switch method {
		case http.MethodGet:
			return "GetDataRetrievalPolicy"
		case http.MethodPut:
			return "SetDataRetrievalPolicy"
		}

	// Provisioned capacity: /-/provisioned-capacity
	case n >= 1 && seg[0] == "provisioned-capacity":
		switch method {
		case http.MethodGet:
			return "ListProvisionedCapacity"
		case http.MethodPost:
			return "PurchaseProvisionedCapacity"
		}
	}
	return ""
}

func (p *Provider) ListResources(_ context.Context) ([]plugin.Resource, error) {
	vaults, err := p.store.ListVaults()
	if err != nil {
		return nil, err
	}
	res := make([]plugin.Resource, 0, len(vaults))
	for _, v := range vaults {
		res = append(res, plugin.Resource{Type: "glacier-vault", ID: v.Name, Name: v.Name})
	}
	return res, nil
}

func (p *Provider) GetMetrics(_ context.Context) (*plugin.ServiceMetrics, error) {
	return &plugin.ServiceMetrics{}, nil
}

// --- Vault CRUD ---

func (p *Provider) createVault(name string) (*plugin.Response, error) {
	if name == "" {
		return shared.JSONError("ValidationException", "vault name is required", http.StatusBadRequest), nil
	}
	arn := shared.BuildARN("glacier", "vaults", name)
	v := &Vault{Name: name, ARN: arn}
	if err := p.store.CreateVault(v); err != nil {
		if isUniqueErr(err) {
			// Vault already exists — return success per Glacier semantics
			return shared.JSONResponse(http.StatusOK, map[string]any{
				"Location": fmt.Sprintf("/-/vaults/%s", name),
			})
		}
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"Location": fmt.Sprintf("/-/vaults/%s", name),
	})
}

func (p *Provider) describeVault(name string) (*plugin.Response, error) {
	if name == "" {
		return shared.JSONError("ValidationException", "vault name is required", http.StatusBadRequest), nil
	}
	v, err := p.store.GetVault(name)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "vault not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, vaultToMap(v))
}

func (p *Provider) listVaults() (*plugin.Response, error) {
	vaults, err := p.store.ListVaults()
	if err != nil {
		return nil, err
	}
	list := make([]map[string]any, 0, len(vaults))
	for _, v := range vaults {
		list = append(list, vaultToMap(&v))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"VaultList": list,
		"Marker":    nil,
	})
}

func (p *Provider) deleteVault(name string) (*plugin.Response, error) {
	if name == "" {
		return shared.JSONError("ValidationException", "vault name is required", http.StatusBadRequest), nil
	}
	if err := p.store.DeleteVault(name); err != nil {
		return shared.JSONError("ResourceNotFoundException", "vault not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusNoContent, nil)
}

// --- Archive ---

func (p *Provider) uploadArchive(vaultName string, req *http.Request, body []byte) (*plugin.Response, error) {
	if vaultName == "" {
		return shared.JSONError("ValidationException", "vault name is required", http.StatusBadRequest), nil
	}
	if _, err := p.store.GetVault(vaultName); err != nil {
		return shared.JSONError("ResourceNotFoundException", "vault not found", http.StatusNotFound), nil
	}

	description := req.Header.Get("x-amz-archive-description")
	checksum := req.Header.Get("x-amz-sha256-tree-hash")
	if checksum == "" {
		checksum = shared.GenerateID("", 64)
	}

	id := shared.GenerateID("", 64)
	arn := shared.BuildARN("glacier", "vaults/"+vaultName+"/archives", id)
	a := &Archive{
		ID:          id,
		VaultName:   vaultName,
		Description: description,
		SizeBytes:   int64(len(body)),
		Checksum:    checksum,
	}
	if err := p.store.CreateArchive(a); err != nil {
		return nil, err
	}

	resp, err := shared.JSONResponse(http.StatusOK, map[string]any{
		"ArchiveId": id,
		"Checksum":  checksum,
		"Location":  fmt.Sprintf("/-/vaults/%s/archives/%s", vaultName, id),
	})
	if err != nil {
		return nil, err
	}
	if resp.Headers == nil {
		resp.Headers = make(map[string]string)
	}
	resp.Headers["x-amz-archive-id"] = id
	resp.Headers["x-amz-sha256-tree-hash"] = checksum
	resp.Headers["Location"] = arn
	return resp, nil
}

func (p *Provider) deleteArchive(vaultName, archiveID string) (*plugin.Response, error) {
	if vaultName == "" || archiveID == "" {
		return shared.JSONError("ValidationException", "vault name and archive ID are required", http.StatusBadRequest), nil
	}
	if err := p.store.DeleteArchive(archiveID); err != nil {
		return shared.JSONError("ResourceNotFoundException", "archive not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusNoContent, nil)
}

// --- Jobs ---

func (p *Provider) initiateJob(vaultName string, params map[string]any) (*plugin.Response, error) {
	if vaultName == "" {
		return shared.JSONError("ValidationException", "vault name is required", http.StatusBadRequest), nil
	}
	if _, err := p.store.GetVault(vaultName); err != nil {
		return shared.JSONError("ResourceNotFoundException", "vault not found", http.StatusNotFound), nil
	}

	jobType := "inventory-retrieval"
	description := ""
	archiveID := ""

	if jp, ok := params["jobParameters"].(map[string]any); ok {
		if t, ok := jp["Type"].(string); ok && t != "" {
			jobType = t
		}
		if d, ok := jp["Description"].(string); ok {
			description = d
		}
		if a, ok := jp["ArchiveId"].(string); ok {
			archiveID = a
		}
	}

	id := shared.GenerateID("job-", 60)
	job := &Job{
		ID:          id,
		VaultName:   vaultName,
		Type:        jobType,
		Description: description,
		Status:      "Succeeded",
		ArchiveID:   archiveID,
	}
	if err := p.store.CreateJob(job); err != nil {
		return nil, err
	}

	resp, err := shared.JSONResponse(http.StatusOK, map[string]any{
		"JobId":         id,
		"Location":      fmt.Sprintf("/-/vaults/%s/jobs/%s", vaultName, id),
		"JobOutputPath": fmt.Sprintf("/-/vaults/%s/jobs/%s/output", vaultName, id),
	})
	if err != nil {
		return nil, err
	}
	if resp.Headers == nil {
		resp.Headers = make(map[string]string)
	}
	resp.Headers["x-amz-job-id"] = id
	resp.Headers["Location"] = fmt.Sprintf("/-/vaults/%s/jobs/%s", vaultName, id)
	return resp, nil
}

func (p *Provider) describeJob(vaultName, jobID string) (*plugin.Response, error) {
	if jobID == "" {
		return shared.JSONError("ValidationException", "job ID is required", http.StatusBadRequest), nil
	}
	job, err := p.store.GetJob(jobID)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "job not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, jobToMap(job))
}

func (p *Provider) listJobs(vaultName string) (*plugin.Response, error) {
	if vaultName == "" {
		return shared.JSONError("ValidationException", "vault name is required", http.StatusBadRequest), nil
	}
	jobs, err := p.store.ListJobs(vaultName)
	if err != nil {
		return nil, err
	}
	list := make([]map[string]any, 0, len(jobs))
	for _, j := range jobs {
		list = append(list, jobToMap(&j))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"JobList": list,
		"Marker":  nil,
	})
}

func (p *Provider) getJobOutput(vaultName, jobID string) (*plugin.Response, error) {
	if jobID == "" {
		return shared.JSONError("ValidationException", "job ID is required", http.StatusBadRequest), nil
	}
	job, err := p.store.GetJob(jobID)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "job not found", http.StatusNotFound), nil
	}

	var output []byte
	if job.Type == "inventory-retrieval" {
		// Return a JSON inventory
		archives, _ := p.store.ListArchives(vaultName)
		archiveList := make([]map[string]any, 0, len(archives))
		for _, a := range archives {
			archiveList = append(archiveList, map[string]any{
				"ArchiveId":          a.ID,
				"ArchiveDescription": a.Description,
				"CreationDate":       a.CreatedAt.UTC().Format(time.RFC3339),
				"Size":               a.SizeBytes,
				"SHA256TreeHash":     a.Checksum,
			})
		}
		inv := map[string]any{
			"VaultARN":      shared.BuildARN("glacier", "vaults", vaultName),
			"InventoryDate": time.Now().UTC().Format(time.RFC3339),
			"ArchiveList":   archiveList,
		}
		output, _ = json.Marshal(inv)
	} else if job.Type == "archive-retrieval" && job.ArchiveID != "" {
		// Return the archive data (empty stub)
		output = []byte{}
	}

	return &plugin.Response{
		StatusCode:  http.StatusOK,
		Body:        output,
		ContentType: "application/json",
	}, nil
}

// --- Multipart Upload ---

func (p *Provider) initiateMultipartUpload(vaultName string, req *http.Request) (*plugin.Response, error) {
	if vaultName == "" {
		return shared.JSONError("ValidationException", "vault name is required", http.StatusBadRequest), nil
	}
	if _, err := p.store.GetVault(vaultName); err != nil {
		return shared.JSONError("ResourceNotFoundException", "vault not found", http.StatusNotFound), nil
	}

	description := req.Header.Get("x-amz-archive-description")
	var partSize int64
	_, _ = fmt.Sscanf(req.Header.Get("x-amz-part-size"), "%d", &partSize)

	id := shared.GenerateID("", 64)
	u := &MultipartUpload{
		ID:          id,
		VaultName:   vaultName,
		Description: description,
		PartSize:    partSize,
	}
	if err := p.store.CreateMultipartUpload(u); err != nil {
		return nil, err
	}

	resp, err := shared.JSONResponse(http.StatusOK, map[string]any{
		"UploadId": id,
		"Location": fmt.Sprintf("/-/vaults/%s/multipart-uploads/%s", vaultName, id),
	})
	if err != nil {
		return nil, err
	}
	if resp.Headers == nil {
		resp.Headers = make(map[string]string)
	}
	resp.Headers["x-amz-multipart-upload-id"] = id
	resp.Headers["Location"] = fmt.Sprintf("/-/vaults/%s/multipart-uploads/%s", vaultName, id)
	return resp, nil
}

func (p *Provider) uploadMultipartPart(vaultName, uploadID string, req *http.Request, body []byte) (*plugin.Response, error) {
	if uploadID == "" {
		return shared.JSONError("ValidationException", "upload ID is required", http.StatusBadRequest), nil
	}
	if _, err := p.store.GetMultipartUpload(uploadID); err != nil {
		return shared.JSONError("ResourceNotFoundException", "upload not found", http.StatusNotFound), nil
	}

	checksum := req.Header.Get("x-amz-sha256-tree-hash")
	rangeHdr := req.Header.Get("Content-Range")
	var rangeStart, rangeEnd int64
	if rangeHdr != "" {
		// Format: bytes start-end/*
		var start, end int64
		_, _ = fmt.Sscanf(rangeHdr, "bytes %d-%d/*", &start, &end)
		rangeStart = start
		rangeEnd = end
	}

	part := &MultipartPart{
		UploadID:   uploadID,
		RangeStart: rangeStart,
		RangeEnd:   rangeEnd,
		Checksum:   checksum,
		Data:       body,
	}
	if err := p.store.PutPart(part); err != nil {
		return nil, err
	}

	resp, err := shared.JSONResponse(http.StatusOK, map[string]any{
		"Checksum": checksum,
	})
	if err != nil {
		return nil, err
	}
	if resp.Headers == nil {
		resp.Headers = make(map[string]string)
	}
	resp.Headers["x-amz-sha256-tree-hash"] = checksum
	return resp, nil
}

func (p *Provider) completeMultipartUpload(vaultName, uploadID string, req *http.Request) (*plugin.Response, error) {
	if uploadID == "" {
		return shared.JSONError("ValidationException", "upload ID is required", http.StatusBadRequest), nil
	}
	u, err := p.store.GetMultipartUpload(uploadID)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "upload not found", http.StatusNotFound), nil
	}

	checksum := req.Header.Get("x-amz-sha256-tree-hash")
	if checksum == "" {
		checksum = shared.GenerateID("", 64)
	}

	_, size, err := p.store.AssembleMultipartUpload(uploadID)
	if err != nil {
		return nil, err
	}

	archiveID := shared.GenerateID("", 64)
	a := &Archive{
		ID:          archiveID,
		VaultName:   vaultName,
		Description: u.Description,
		SizeBytes:   size,
		Checksum:    checksum,
	}
	if err := p.store.CreateArchive(a); err != nil {
		return nil, err
	}

	p.store.DeleteMultipartParts(uploadID)        //nolint:errcheck
	p.store.DeleteMultipartUploadRecord(uploadID) //nolint:errcheck

	resp, err := shared.JSONResponse(http.StatusOK, map[string]any{
		"ArchiveId": archiveID,
		"Checksum":  checksum,
		"Location":  fmt.Sprintf("/-/vaults/%s/archives/%s", vaultName, archiveID),
	})
	if err != nil {
		return nil, err
	}
	if resp.Headers == nil {
		resp.Headers = make(map[string]string)
	}
	resp.Headers["x-amz-archive-id"] = archiveID
	resp.Headers["x-amz-sha256-tree-hash"] = checksum
	return resp, nil
}

func (p *Provider) abortMultipartUpload(vaultName, uploadID string) (*plugin.Response, error) {
	if uploadID == "" {
		return shared.JSONError("ValidationException", "upload ID is required", http.StatusBadRequest), nil
	}
	if err := p.store.AbortMultipartUpload(uploadID); err != nil {
		return shared.JSONError("ResourceNotFoundException", "upload not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusNoContent, nil)
}

func (p *Provider) listMultipartUploads(vaultName string) (*plugin.Response, error) {
	if vaultName == "" {
		return shared.JSONError("ValidationException", "vault name is required", http.StatusBadRequest), nil
	}
	uploads, err := p.store.ListMultipartUploads(vaultName)
	if err != nil {
		return nil, err
	}
	vault, _ := p.store.GetVault(vaultName)
	vaultARN := ""
	if vault != nil {
		vaultARN = vault.ARN
	}
	list := make([]map[string]any, 0, len(uploads))
	for _, u := range uploads {
		list = append(list, map[string]any{
			"MultipartUploadId":  u.ID,
			"VaultARN":           vaultARN,
			"ArchiveDescription": u.Description,
			"PartSizeInBytes":    u.PartSize,
			"CreationDate":       u.CreatedAt.UTC().Format(time.RFC3339),
		})
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"UploadsList": list,
		"Marker":      nil,
	})
}

func (p *Provider) listParts(vaultName, uploadID string) (*plugin.Response, error) {
	if uploadID == "" {
		return shared.JSONError("ValidationException", "upload ID is required", http.StatusBadRequest), nil
	}
	u, err := p.store.GetMultipartUpload(uploadID)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "upload not found", http.StatusNotFound), nil
	}
	vault, _ := p.store.GetVault(vaultName)
	vaultARN := ""
	if vault != nil {
		vaultARN = vault.ARN
	}
	parts, err := p.store.ListParts(uploadID)
	if err != nil {
		return nil, err
	}
	partList := make([]map[string]any, 0, len(parts))
	for _, part := range parts {
		partList = append(partList, map[string]any{
			"RangeInBytes":   fmt.Sprintf("%d-%d", part.RangeStart, part.RangeEnd),
			"SHA256TreeHash": part.Checksum,
		})
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"MultipartUploadId":  uploadID,
		"VaultARN":           vaultARN,
		"ArchiveDescription": u.Description,
		"PartSizeInBytes":    u.PartSize,
		"CreationDate":       u.CreatedAt.UTC().Format(time.RFC3339),
		"Parts":              partList,
		"Marker":             nil,
	})
}

// --- Tags ---

func (p *Provider) addTagsToVault(vaultName string, params map[string]any) (*plugin.Response, error) {
	if vaultName == "" {
		return shared.JSONError("ValidationException", "vault name is required", http.StatusBadRequest), nil
	}
	if _, err := p.store.GetVault(vaultName); err != nil {
		return shared.JSONError("ResourceNotFoundException", "vault not found", http.StatusNotFound), nil
	}
	tags := make(map[string]string)
	if rawTags, ok := params["Tags"].(map[string]any); ok {
		for k, v := range rawTags {
			if s, ok := v.(string); ok {
				tags[k] = s
			}
		}
	}
	if err := p.store.AddTags(vaultName, tags); err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusNoContent, nil)
}

func (p *Provider) removeTagsFromVault(vaultName string, params map[string]any) (*plugin.Response, error) {
	if vaultName == "" {
		return shared.JSONError("ValidationException", "vault name is required", http.StatusBadRequest), nil
	}
	var keys []string
	if rawKeys, ok := params["TagKeys"].([]any); ok {
		for _, k := range rawKeys {
			if s, ok := k.(string); ok {
				keys = append(keys, s)
			}
		}
	}
	if err := p.store.RemoveTags(vaultName, keys); err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusNoContent, nil)
}

func (p *Provider) listTagsForVault(vaultName string) (*plugin.Response, error) {
	if vaultName == "" {
		return shared.JSONError("ValidationException", "vault name is required", http.StatusBadRequest), nil
	}
	if _, err := p.store.GetVault(vaultName); err != nil {
		return shared.JSONError("ResourceNotFoundException", "vault not found", http.StatusNotFound), nil
	}
	tags, err := p.store.ListTags(vaultName)
	if err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"Tags": tags})
}

// --- Vault Access Policy ---

func (p *Provider) setVaultAccessPolicy(vaultName string, params map[string]any) (*plugin.Response, error) {
	if vaultName == "" {
		return shared.JSONError("ValidationException", "vault name is required", http.StatusBadRequest), nil
	}
	policy := "{}"
	if policyObj, ok := params["policy"].(map[string]any); ok {
		if pol, ok := policyObj["Policy"].(string); ok {
			policy = pol
		}
	}
	if err := p.store.SetVaultPolicy(vaultName, policy); err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusNoContent, nil)
}

func (p *Provider) getVaultAccessPolicy(vaultName string) (*plugin.Response, error) {
	if vaultName == "" {
		return shared.JSONError("ValidationException", "vault name is required", http.StatusBadRequest), nil
	}
	policy, err := p.store.GetVaultPolicy(vaultName)
	if err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"policy": map[string]any{"Policy": policy},
	})
}

func (p *Provider) deleteVaultAccessPolicy(vaultName string) (*plugin.Response, error) {
	if vaultName == "" {
		return shared.JSONError("ValidationException", "vault name is required", http.StatusBadRequest), nil
	}
	if err := p.store.DeleteVaultPolicy(vaultName); err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusNoContent, nil)
}

// --- Vault Notifications ---

func (p *Provider) setVaultNotifications(vaultName string, params map[string]any) (*plugin.Response, error) {
	if vaultName == "" {
		return shared.JSONError("ValidationException", "vault name is required", http.StatusBadRequest), nil
	}
	snsTopic := ""
	var events []string
	if cfg, ok := params["vaultNotificationConfig"].(map[string]any); ok {
		if t, ok := cfg["SNSTopic"].(string); ok {
			snsTopic = t
		}
		if ev, ok := cfg["Events"].([]any); ok {
			for _, e := range ev {
				if s, ok := e.(string); ok {
					events = append(events, s)
				}
			}
		}
	}
	if err := p.store.SetVaultNotifications(vaultName, snsTopic, events); err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusNoContent, nil)
}

func (p *Provider) getVaultNotifications(vaultName string) (*plugin.Response, error) {
	if vaultName == "" {
		return shared.JSONError("ValidationException", "vault name is required", http.StatusBadRequest), nil
	}
	snsTopic, events, err := p.store.GetVaultNotifications(vaultName)
	if err != nil {
		return nil, err
	}
	if events == nil {
		events = []string{}
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"vaultNotificationConfig": map[string]any{
			"SNSTopic": snsTopic,
			"Events":   events,
		},
	})
}

func (p *Provider) deleteVaultNotifications(vaultName string) (*plugin.Response, error) {
	if vaultName == "" {
		return shared.JSONError("ValidationException", "vault name is required", http.StatusBadRequest), nil
	}
	if err := p.store.DeleteVaultNotifications(vaultName); err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusNoContent, nil)
}

// --- Vault Lock ---

func (p *Provider) initiateVaultLock(vaultName string, params map[string]any) (*plugin.Response, error) {
	if vaultName == "" {
		return shared.JSONError("ValidationException", "vault name is required", http.StatusBadRequest), nil
	}
	if _, err := p.store.GetVault(vaultName); err != nil {
		return shared.JSONError("ResourceNotFoundException", "vault not found", http.StatusNotFound), nil
	}
	policy := "{}"
	if policyObj, ok := params["policy"].(map[string]any); ok {
		if pol, ok := policyObj["Policy"].(string); ok {
			policy = pol
		}
	}
	lockID := shared.GenerateID("", 32)
	if err := p.store.InitVaultLock(vaultName, lockID, policy); err != nil {
		return nil, err
	}
	resp, err := shared.JSONResponse(http.StatusOK, map[string]any{"LockId": lockID})
	if err != nil {
		return nil, err
	}
	if resp.Headers == nil {
		resp.Headers = make(map[string]string)
	}
	resp.Headers["x-amz-lock-id"] = lockID
	return resp, nil
}

func (p *Provider) completeVaultLock(vaultName, lockID string) (*plugin.Response, error) {
	if vaultName == "" || lockID == "" {
		return shared.JSONError("ValidationException", "vault name and lock ID are required", http.StatusBadRequest), nil
	}
	if err := p.store.CompleteVaultLock(vaultName, lockID); err != nil {
		return shared.JSONError("InvalidParameterValueException", "lock not found or lock ID mismatch", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusNoContent, nil)
}

func (p *Provider) abortVaultLock(vaultName string) (*plugin.Response, error) {
	if vaultName == "" {
		return shared.JSONError("ValidationException", "vault name is required", http.StatusBadRequest), nil
	}
	if err := p.store.AbortVaultLock(vaultName); err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusNoContent, nil)
}

func (p *Provider) getVaultLock(vaultName string) (*plugin.Response, error) {
	if vaultName == "" {
		return shared.JSONError("ValidationException", "vault name is required", http.StatusBadRequest), nil
	}
	lockID, policy, state, err := p.store.GetVaultLock(vaultName)
	if err != nil {
		return nil, err
	}
	if state == "" {
		return shared.JSONError("ResourceNotFoundException", "vault lock not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"LockId":         lockID,
		"Policy":         policy,
		"State":          state,
		"CreationDate":   time.Now().UTC().Format(time.RFC3339),
		"ExpirationDate": time.Now().Add(24 * time.Hour).UTC().Format(time.RFC3339),
	})
}

// --- Helpers ---

func vaultToMap(v *Vault) map[string]any {
	return map[string]any{
		"VaultARN":          v.ARN,
		"VaultName":         v.Name,
		"CreationDate":      v.CreatedAt.UTC().Format(time.RFC3339),
		"LastInventoryDate": "",
		"NumberOfArchives":  v.ArchiveCount,
		"SizeInBytes":       v.SizeBytes,
	}
}

func jobToMap(j *Job) map[string]any {
	return map[string]any{
		"JobId":          j.ID,
		"JobDescription": j.Description,
		"Action":         j.Type,
		"ArchiveId":      j.ArchiveID,
		"StatusCode":     j.Status,
		"StatusMessage":  j.Status,
		"Completed":      true,
		"CreationDate":   j.CreatedAt.UTC().Format(time.RFC3339),
		"CompletionDate": j.CompletedAt.UTC().Format(time.RFC3339),
	}
}

func extractPathParam(path, key string) string {
	parts := strings.Split(strings.Trim(path, "/"), "/")
	for i, p := range parts {
		// Handle query strings in the key (e.g., "tags?operation=add")
		keyBase := strings.Split(key, "?")[0]
		if p == keyBase && i+1 < len(parts) {
			return parts[i+1]
		}
	}
	return ""
}

func isUniqueErr(err error) bool {
	return err != nil && strings.Contains(err.Error(), "UNIQUE constraint failed")
}
