// SPDX-License-Identifier: Apache-2.0

package gcs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"strings"

	"github.com/skyoo2003/devcloud/internal/auth"
	generated "github.com/skyoo2003/devcloud/internal/generated/gcs"
	"github.com/skyoo2003/devcloud/internal/plugin"
)

// Provider implements Google Cloud Storage.
type Provider struct {
	generated.BaseProvider
	dataDir string
	store   *FileStore
}

func (p *Provider) ServiceID() string             { return "gcs" }
func (p *Provider) ServiceName() string           { return "Google Cloud Storage" }
func (p *Provider) Protocol() plugin.ProtocolType { return plugin.ProtocolRESTJSON }
func (p *Provider) Provider() string              { return "gcp" }

func (p *Provider) Init(cfg plugin.PluginConfig) error {
	p.dataDir = cfg.DataDir
	if p.dataDir == "" {
		p.dataDir = "./data/gcp-storage"
	}
	p.store = NewFileStore(p.dataDir)
	return nil
}

func (p *Provider) Shutdown(ctx context.Context) error {
	return nil
}

func (p *Provider) ListResources(ctx context.Context) ([]plugin.Resource, error) {
	buckets, err := p.store.ListBuckets("default")
	if err != nil {
		return nil, err
	}
	var res []plugin.Resource
	for _, b := range buckets {
		res = append(res, plugin.Resource{
			Type: "storage#bucket",
			ID:   b.ID,
			Name: b.Name,
		})
	}
	return res, nil
}

func (p *Provider) jsonReply(status int, data any) *plugin.Response {
	raw, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		return p.errorReply(http.StatusInternalServerError, "internalError", err.Error())
	}
	return &plugin.Response{
		StatusCode:  status,
		Headers:     map[string]string{"Content-Type": "application/json; charset=UTF-8"},
		Body:        raw,
		ContentType: "application/json; charset=UTF-8",
	}
}

func (p *Provider) errorReply(status int, reason, message string) *plugin.Response {
	type gcpErrItem struct {
		Message string `json:"message"`
		Domain  string `json:"domain"`
		Reason  string `json:"reason"`
	}
	type gcpErr struct {
		Error struct {
			Code    int          `json:"code"`
			Message string       `json:"message"`
			Errors  []gcpErrItem `json:"errors"`
		} `json:"error"`
	}
	var resp gcpErr
	resp.Error.Code = status
	resp.Error.Message = message
	resp.Error.Errors = []gcpErrItem{{
		Message: message,
		Domain:  "global",
		Reason:  reason,
	}}
	raw, _ := json.MarshalIndent(resp, "", "  ")
	return &plugin.Response{
		StatusCode:  status,
		Headers:     map[string]string{"Content-Type": "application/json; charset=UTF-8"},
		Body:        raw,
		ContentType: "application/json; charset=UTF-8",
	}
}

func (p *Provider) HandleRequest(ctx context.Context, _ string, req *http.Request) (*plugin.Response, error) {
	project := req.URL.Query().Get("project")
	if project == "" {
		if id, ok := auth.FromContext(ctx); ok && id.AccessKeyID != "" {
			project = id.AccessKeyID
		} else {
			project = "default"
		}
	}

	op, params := generated.MatchOperation(req.Method, req.URL.RequestURI())
	if op == "" {
		// Fallback match using path without query
		op, params = generated.MatchOperation(req.Method, req.URL.Path)
	}

	switch op {
	case "ListBuckets":
		return p.handleListBuckets(project)
	case "InsertBucket":
		return p.handleInsertBucket(project, req)
	case "GetBucket":
		return p.handleGetBucket(project, params["bucket"])
	case "DeleteBucket":
		return p.handleDeleteBucket(project, params["bucket"])
	case "ListObjects":
		return p.handleListObjects(project, params["bucket"], req.URL.Query().Get("prefix"))
	case "GetObject":
		return p.handleGetObject(project, params["bucket"], params["object"], req)
	case "InsertObject":
		return p.handleInsertObject(project, params["bucket"], req)
	case "DeleteObject":
		return p.handleDeleteObject(project, params["bucket"], params["object"])
	default:
		return p.errorReply(http.StatusBadRequest, "unknownAction", "operation not handled: "+req.Method+" "+req.URL.Path), nil
	}
}

func (p *Provider) handleListBuckets(project string) (*plugin.Response, error) {
	buckets, err := p.store.ListBuckets(project)
	if err != nil {
		return p.errorReply(http.StatusInternalServerError, "internalError", err.Error()), nil
	}
	type listBucketsResponse struct {
		Kind  string       `json:"kind"`
		Items []BucketInfo `json:"items"`
	}
	return p.jsonReply(http.StatusOK, listBucketsResponse{
		Kind:  "storage#buckets",
		Items: buckets,
	}), nil
}

func (p *Provider) handleInsertBucket(project string, req *http.Request) (*plugin.Response, error) {
	body, err := io.ReadAll(req.Body)
	if err != nil {
		return p.errorReply(http.StatusBadRequest, "badRequest", "failed to read body"), nil
	}
	var reqBody struct {
		Name         string `json:"name"`
		Location     string `json:"location"`
		StorageClass string `json:"storageClass"`
	}
	if len(body) > 0 {
		_ = json.Unmarshal(body, &reqBody)
	}
	bucketName := reqBody.Name
	if bucketName == "" {
		bucketName = req.URL.Query().Get("name")
	}
	if bucketName == "" {
		return p.errorReply(http.StatusBadRequest, "required", "bucket name is required"), nil
	}

	bucket, err := p.store.CreateBucket(project, bucketName, reqBody.Location, reqBody.StorageClass)
	if err != nil {
		if errors.Is(err, ErrBucketAlreadyExists) {
			return p.errorReply(http.StatusConflict, "conflict", "bucket already exists"), nil
		}
		if errors.Is(err, ErrInvalidName) {
			return p.errorReply(http.StatusBadRequest, "invalid", "invalid bucket name"), nil
		}
		return p.errorReply(http.StatusInternalServerError, "internalError", err.Error()), nil
	}

	return p.jsonReply(http.StatusOK, bucket), nil
}

func (p *Provider) handleGetBucket(project, bucket string) (*plugin.Response, error) {
	b, err := p.store.GetBucket(project, bucket)
	if err != nil {
		if errors.Is(err, ErrBucketNotFound) {
			return p.errorReply(http.StatusNotFound, "notFound", "bucket not found"), nil
		}
		return p.errorReply(http.StatusInternalServerError, "internalError", err.Error()), nil
	}
	return p.jsonReply(http.StatusOK, b), nil
}

func (p *Provider) handleDeleteBucket(project, bucket string) (*plugin.Response, error) {
	err := p.store.DeleteBucket(project, bucket)
	if err != nil {
		if errors.Is(err, ErrBucketNotFound) {
			return p.errorReply(http.StatusNotFound, "notFound", "bucket not found"), nil
		}
		if errors.Is(err, ErrBucketNotEmpty) {
			return p.errorReply(http.StatusConflict, "conflict", "bucket is not empty"), nil
		}
		return p.errorReply(http.StatusInternalServerError, "internalError", err.Error()), nil
	}
	return &plugin.Response{
		StatusCode: http.StatusNoContent,
		Headers:    map[string]string{},
	}, nil
}

func (p *Provider) handleListObjects(project, bucket, prefix string) (*plugin.Response, error) {
	objects, err := p.store.ListObjects(project, bucket, prefix)
	if err != nil {
		if errors.Is(err, ErrBucketNotFound) {
			return p.errorReply(http.StatusNotFound, "notFound", "bucket not found"), nil
		}
		return p.errorReply(http.StatusInternalServerError, "internalError", err.Error()), nil
	}
	type listObjectsResponse struct {
		Kind  string       `json:"kind"`
		Items []ObjectInfo `json:"items"`
	}
	return p.jsonReply(http.StatusOK, listObjectsResponse{
		Kind:  "storage#objects",
		Items: objects,
	}), nil
}

func (p *Provider) handleGetObject(project, bucket, object string, req *http.Request) (*plugin.Response, error) {
	alt := req.URL.Query().Get("alt")
	if alt == "media" {
		info, data, err := p.store.GetObject(project, bucket, object)
		if err != nil {
			if errors.Is(err, ErrObjectNotFound) || errors.Is(err, ErrBucketNotFound) {
				return p.errorReply(http.StatusNotFound, "notFound", "object not found"), nil
			}
			return p.errorReply(http.StatusInternalServerError, "internalError", err.Error()), nil
		}
		headers := map[string]string{
			"Content-Type":   info.ContentType,
			"ETag":           info.ETag,
			"Content-Length": fmt.Sprintf("%d", len(data)),
			"x-goog-hash":    fmt.Sprintf("crc32c=%s,md5=%s", info.CRC32c, info.MD5Hash),
		}
		return &plugin.Response{
			StatusCode:  http.StatusOK,
			Headers:     headers,
			Body:        data,
			ContentType: info.ContentType,
		}, nil
	}

	info, err := p.store.GetObjectMetadata(project, bucket, object)
	if err != nil {
		if errors.Is(err, ErrObjectNotFound) || errors.Is(err, ErrBucketNotFound) {
			return p.errorReply(http.StatusNotFound, "notFound", "object not found"), nil
		}
		return p.errorReply(http.StatusInternalServerError, "internalError", err.Error()), nil
	}
	return p.jsonReply(http.StatusOK, info), nil
}

func (p *Provider) handleInsertObject(project, bucket string, req *http.Request) (*plugin.Response, error) {
	contentType := req.Header.Get("Content-Type")
	mediaType, params, _ := mime.ParseMediaType(contentType)

	objectName := req.URL.Query().Get("name")
	var payload []byte
	var meta map[string]string
	objContentType := "application/octet-stream"

	if strings.HasPrefix(mediaType, "multipart/") {
		mr := multipart.NewReader(req.Body, params["boundary"])
		for {
			part, err := mr.NextPart()
			if err == io.EOF {
				break
			}
			if err != nil {
				return p.errorReply(http.StatusBadRequest, "badRequest", "invalid multipart payload: "+err.Error()), nil
			}
			partType := part.Header.Get("Content-Type")
			if strings.Contains(partType, "application/json") {
				var objMetadata struct {
					Name        string            `json:"name"`
					ContentType string            `json:"contentType"`
					Metadata    map[string]string `json:"metadata"`
				}
				_ = json.NewDecoder(part).Decode(&objMetadata)
				if objMetadata.Name != "" {
					objectName = objMetadata.Name
				}
				if objMetadata.ContentType != "" {
					objContentType = objMetadata.ContentType
				}
				if objMetadata.Metadata != nil {
					meta = objMetadata.Metadata
				}
			} else {
				if partType != "" {
					objContentType = partType
				}
				data, rerr := io.ReadAll(part)
				if rerr != nil {
					return p.errorReply(http.StatusBadRequest, "badRequest", "failed to read part data"), nil
				}
				payload = data
			}
		}
	} else {
		// Raw media or direct upload
		data, err := io.ReadAll(req.Body)
		if err != nil {
			return p.errorReply(http.StatusBadRequest, "badRequest", "failed to read body"), nil
		}
		payload = data
		if ct := req.Header.Get("X-Upload-Content-Type"); ct != "" {
			objContentType = ct
		} else if contentType != "" {
			objContentType = contentType
		}
	}

	if objectName == "" {
		return p.errorReply(http.StatusBadRequest, "required", "object name is required"), nil
	}

	info, err := p.store.PutObject(project, bucket, objectName, objContentType, payload, meta)
	if err != nil {
		if errors.Is(err, ErrBucketNotFound) {
			return p.errorReply(http.StatusNotFound, "notFound", "bucket not found"), nil
		}
		if errors.Is(err, ErrInvalidName) {
			return p.errorReply(http.StatusBadRequest, "invalid", "invalid object name"), nil
		}
		return p.errorReply(http.StatusInternalServerError, "internalError", err.Error()), nil
	}

	return p.jsonReply(http.StatusOK, info), nil
}

func (p *Provider) handleDeleteObject(project, bucket, object string) (*plugin.Response, error) {
	err := p.store.DeleteObject(project, bucket, object)
	if err != nil {
		if errors.Is(err, ErrObjectNotFound) || errors.Is(err, ErrBucketNotFound) {
			return p.errorReply(http.StatusNotFound, "notFound", "object not found"), nil
		}
		return p.errorReply(http.StatusInternalServerError, "internalError", err.Error()), nil
	}
	return &plugin.Response{
		StatusCode: http.StatusNoContent,
		Headers:    map[string]string{},
	}, nil
}

func init() {
	plugin.DefaultRegistry.Register("gcs", func() plugin.ServicePlugin {
		return &Provider{}
	})
}
