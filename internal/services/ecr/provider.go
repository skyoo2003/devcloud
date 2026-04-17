// SPDX-License-Identifier: Apache-2.0

// internal/services/ecr/provider.go
package ecr

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/skyoo2003/devcloud/internal/plugin"
)

const defaultAccountID = plugin.DefaultAccountID

// Provider implements the ECR service (JSON 1.1 protocol).
type Provider struct {
	store *ECRStore
}

func (p *Provider) ServiceID() string             { return "ecr" }
func (p *Provider) ServiceName() string           { return "Amazon ECR" }
func (p *Provider) Protocol() plugin.ProtocolType { return plugin.ProtocolJSON11 }

func (p *Provider) Init(cfg plugin.PluginConfig) error {
	if err := os.MkdirAll(cfg.DataDir, 0o755); err != nil {
		return fmt.Errorf("init ecr: %w", err)
	}
	port := 0
	if v, ok := cfg.Options["server_port"].(int); ok {
		port = v
	}
	var err error
	p.store, err = NewECRStore(cfg.DataDir, port)
	return err
}

func (p *Provider) Shutdown(_ context.Context) error {
	if p.store != nil {
		return p.store.Close()
	}
	return nil
}

func (p *Provider) HandleRequest(ctx context.Context, op string, req *http.Request) (*plugin.Response, error) {
	rawBody, err := io.ReadAll(req.Body)
	if err != nil {
		return ecrError("InvalidRequest", "failed to read body", http.StatusBadRequest), nil
	}
	var params map[string]any
	if len(rawBody) > 0 {
		if err := json.Unmarshal(rawBody, &params); err != nil {
			return ecrError("InvalidRequest", "failed to parse JSON", http.StatusBadRequest), nil
		}
	} else {
		params = make(map[string]any)
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
	case "CreateRepository":
		return p.handleCreateRepository(params)
	case "DeleteRepository":
		return p.handleDeleteRepository(params)
	case "DescribeRepositories":
		return p.handleDescribeRepositories(params)
	case "PutImage":
		return p.handlePutImage(params)
	case "BatchGetImage":
		return p.handleBatchGetImage(params)
	case "BatchDeleteImage":
		return p.handleBatchDeleteImage(params)
	case "DescribeImages":
		return p.handleDescribeImages(params)
	case "ListImages":
		return p.handleListImages(params)
	case "GetAuthorizationToken":
		return p.handleGetAuthorizationToken(params)
	case "SetRepositoryPolicy":
		return p.handleSetRepositoryPolicy(params)
	case "GetRepositoryPolicy":
		return p.handleGetRepositoryPolicy(params)
	// Layer operations
	case "InitiateLayerUpload":
		return p.handleInitiateLayerUpload(params)
	case "UploadLayerPart":
		return p.handleUploadLayerPart(params)
	case "CompleteLayerUpload":
		return p.handleCompleteLayerUpload(params)
	case "BatchCheckLayerAvailability":
		return p.handleBatchCheckLayerAvailability(params)
	case "GetDownloadUrlForLayer":
		return p.handleGetDownloadUrlForLayer(params)
	// Lifecycle policy operations
	case "PutLifecyclePolicy":
		return p.handlePutLifecyclePolicy(params)
	case "GetLifecyclePolicy":
		return p.handleGetLifecyclePolicy(params)
	case "DeleteLifecyclePolicy":
		return p.handleDeleteLifecyclePolicy(params)
	// Tag operations
	case "TagResource":
		return p.handleTagResource(params)
	case "UntagResource":
		return p.handleUntagResource(params)
	case "ListTagsForResource":
		return p.handleListTagsForResource(params)
	// Image scan operations
	case "StartImageScan":
		return p.handleStartImageScan(params)
	case "DescribeImageScanFindings":
		return p.handleDescribeImageScanFindings(params)
	case "PutImageScanningConfiguration":
		return p.handlePutImageScanningConfiguration(params)
	default:
		return ecrError("InvalidAction", fmt.Sprintf("unknown action: %s", action), http.StatusBadRequest), nil
	}
}

func (p *Provider) ListResources(ctx context.Context) ([]plugin.Resource, error) {
	repos, err := p.store.DescribeRepositories(defaultAccountID, nil)
	if err != nil {
		return nil, err
	}
	out := make([]plugin.Resource, 0, len(repos))
	for _, r := range repos {
		out = append(out, plugin.Resource{Type: "repository", ID: r.ARN, Name: r.Name})
	}
	return out, nil
}

func (p *Provider) GetMetrics(_ context.Context) (*plugin.ServiceMetrics, error) {
	return &plugin.ServiceMetrics{}, nil
}

// --- operation handlers ---

func (p *Provider) handleCreateRepository(params map[string]any) (*plugin.Response, error) {
	name := strParam(params, "repositoryName")
	if name == "" {
		return ecrError("InvalidParameterException", "repositoryName is required", http.StatusBadRequest), nil
	}
	repo, err := p.store.CreateRepository(defaultAccountID, name)
	if err != nil {
		if err == ErrRepositoryAlreadyExists {
			return ecrError("RepositoryAlreadyExistsException", fmt.Sprintf("repository %q already exists", name), http.StatusBadRequest), nil
		}
		return nil, err
	}
	return ecrJSON(http.StatusOK, map[string]any{"repository": repoToMap(repo)})
}

func (p *Provider) handleDeleteRepository(params map[string]any) (*plugin.Response, error) {
	name := strParam(params, "repositoryName")
	if name == "" {
		return ecrError("InvalidParameterException", "repositoryName is required", http.StatusBadRequest), nil
	}
	repos, _ := p.store.DescribeRepositories(defaultAccountID, []string{name})
	if err := p.store.DeleteRepository(defaultAccountID, name); err != nil {
		return ecrError("RepositoryNotFoundException", "repository not found", http.StatusBadRequest), nil
	}
	var repoMap map[string]any
	if len(repos) > 0 {
		repoMap = repoToMap(&repos[0])
	}
	return ecrJSON(http.StatusOK, map[string]any{"repository": repoMap})
}

func (p *Provider) handleDescribeRepositories(params map[string]any) (*plugin.Response, error) {
	names := strSliceParam(params, "repositoryNames")
	repos, err := p.store.DescribeRepositories(defaultAccountID, names)
	if err != nil {
		return nil, err
	}
	// If specific names were requested but none found, return RepositoryNotFoundException.
	if len(names) > 0 && len(repos) == 0 {
		return ecrError("RepositoryNotFoundException",
			fmt.Sprintf("The repository with name '%s' does not exist in the registry with id '%s'", names[0], defaultAccountID),
			http.StatusBadRequest), nil
	}
	items := make([]any, 0, len(repos))
	for i := range repos {
		items = append(items, repoToMap(&repos[i]))
	}
	return ecrJSON(http.StatusOK, map[string]any{"repositories": items})
}

func (p *Provider) handlePutImage(params map[string]any) (*plugin.Response, error) {
	repoName := strParam(params, "repositoryName")
	manifest := strParam(params, "imageManifest")
	tag := strParam(params, "imageTag")
	if repoName == "" || manifest == "" {
		return ecrError("InvalidParameterException", "repositoryName and imageManifest are required", http.StatusBadRequest), nil
	}
	img, err := p.store.PutImage(defaultAccountID, repoName, manifest, tag)
	if err != nil {
		return nil, err
	}
	return ecrJSON(http.StatusOK, map[string]any{"image": imageToMap(img)})
}

func (p *Provider) handleBatchGetImage(params map[string]any) (*plugin.Response, error) {
	repoName := strParam(params, "repositoryName")
	imageIDs := imageIDsParam(params, "imageIds")
	if repoName == "" {
		return ecrError("InvalidParameterException", "repositoryName is required", http.StatusBadRequest), nil
	}
	images, err := p.store.BatchGetImage(defaultAccountID, repoName, imageIDs)
	if err != nil {
		return nil, err
	}
	items := make([]any, 0, len(images))
	for i := range images {
		items = append(items, imageToMap(&images[i]))
	}
	return ecrJSON(http.StatusOK, map[string]any{"images": items, "failures": []any{}})
}

func (p *Provider) handleBatchDeleteImage(params map[string]any) (*plugin.Response, error) {
	repoName := strParam(params, "repositoryName")
	imageIDs := imageIDsParam(params, "imageIds")
	if repoName == "" {
		return ecrError("InvalidParameterException", "repositoryName is required", http.StatusBadRequest), nil
	}
	if err := p.store.BatchDeleteImage(defaultAccountID, repoName, imageIDs); err != nil {
		return nil, err
	}
	// Return the deleted image IDs as imageIds.
	deleted := make([]any, 0, len(imageIDs))
	for _, id := range imageIDs {
		deleted = append(deleted, id)
	}
	return ecrJSON(http.StatusOK, map[string]any{"imageIds": deleted, "failures": []any{}})
}

func (p *Provider) handleDescribeImages(params map[string]any) (*plugin.Response, error) {
	repoName := strParam(params, "repositoryName")
	if repoName == "" {
		return ecrError("InvalidParameterException", "repositoryName is required", http.StatusBadRequest), nil
	}
	images, err := p.store.DescribeImages(defaultAccountID, repoName)
	if err != nil {
		return nil, err
	}
	items := make([]any, 0, len(images))
	for i := range images {
		items = append(items, imageDetailToMap(&images[i]))
	}
	return ecrJSON(http.StatusOK, map[string]any{"imageDetails": items})
}

func (p *Provider) handleListImages(params map[string]any) (*plugin.Response, error) {
	repoName := strParam(params, "repositoryName")
	if repoName == "" {
		return ecrError("InvalidParameterException", "repositoryName is required", http.StatusBadRequest), nil
	}
	ids, err := p.store.ListImages(defaultAccountID, repoName)
	if err != nil {
		return nil, err
	}
	if ids == nil {
		ids = []map[string]string{}
	}
	items := make([]any, 0, len(ids))
	for _, id := range ids {
		items = append(items, id)
	}
	return ecrJSON(http.StatusOK, map[string]any{"imageIds": items})
}

func (p *Provider) handleGetAuthorizationToken(_ map[string]any) (*plugin.Response, error) {
	// Return a static base64-encoded token valid for 12 hours.
	token := base64.StdEncoding.EncodeToString([]byte("AWS:devcloud-token"))
	expiry := time.Now().UTC().Add(12 * time.Hour).Format(time.RFC3339)
	return ecrJSON(http.StatusOK, map[string]any{
		"authorizationData": []map[string]any{
			{
				"authorizationToken": token,
				"expiresAt":          expiry,
				"proxyEndpoint":      fmt.Sprintf("https://%s.dkr.ecr.%s.amazonaws.com", defaultAccountID, region),
			},
		},
	})
}

func (p *Provider) handleSetRepositoryPolicy(params map[string]any) (*plugin.Response, error) {
	repoName := strParam(params, "repositoryName")
	policyText := strParam(params, "policyText")
	if repoName == "" || policyText == "" {
		return ecrError("InvalidParameterException", "repositoryName and policyText are required", http.StatusBadRequest), nil
	}
	if err := p.store.SetRepositoryPolicy(defaultAccountID, repoName, policyText); err != nil {
		if err == ErrRepositoryNotFound {
			return ecrError("RepositoryNotFoundException", "repository not found", http.StatusBadRequest), nil
		}
		return nil, err
	}
	return ecrJSON(http.StatusOK, map[string]any{
		"repositoryName": repoName,
		"registryId":     defaultAccountID,
		"policyText":     policyText,
	})
}

func (p *Provider) handleGetRepositoryPolicy(params map[string]any) (*plugin.Response, error) {
	repoName := strParam(params, "repositoryName")
	if repoName == "" {
		return ecrError("InvalidParameterException", "repositoryName is required", http.StatusBadRequest), nil
	}
	policyText, err := p.store.GetRepositoryPolicy(defaultAccountID, repoName)
	if err != nil {
		if err == ErrRepositoryNotFound {
			return ecrError("RepositoryNotFoundException", "repository not found", http.StatusBadRequest), nil
		}
		if err == ErrPolicyNotFound {
			return ecrError("RepositoryPolicyNotFoundException", "repository policy does not exist", http.StatusBadRequest), nil
		}
		return nil, err
	}
	return ecrJSON(http.StatusOK, map[string]any{
		"repositoryName": repoName,
		"registryId":     defaultAccountID,
		"policyText":     policyText,
	})
}

// --- layer operation handlers ---

func (p *Provider) handleInitiateLayerUpload(params map[string]any) (*plugin.Response, error) {
	repoName := strParam(params, "repositoryName")
	if repoName == "" {
		return ecrError("InvalidParameterException", "repositoryName is required", http.StatusBadRequest), nil
	}
	uploadID, err := p.store.InitiateLayerUpload(defaultAccountID, repoName)
	if err != nil {
		return nil, err
	}
	return ecrJSON(http.StatusOK, map[string]any{
		"uploadId": uploadID,
		"partSize": int64(5 * 1024 * 1024), // 5 MiB
	})
}

func (p *Provider) handleUploadLayerPart(params map[string]any) (*plugin.Response, error) {
	repoName := strParam(params, "repositoryName")
	uploadID := strParam(params, "uploadId")
	if repoName == "" || uploadID == "" {
		return ecrError("InvalidParameterException", "repositoryName and uploadId are required", http.StatusBadRequest), nil
	}

	partFirst := int64Param(params, "partFirstByte")
	partLast := int64Param(params, "partLastByte")

	// layerPartBlob arrives as base64-encoded string in JSON.
	var blob []byte
	if v, ok := params["layerPartBlob"]; ok {
		switch bv := v.(type) {
		case string:
			decoded, err := base64.StdEncoding.DecodeString(bv)
			if err != nil {
				// Try raw string as bytes if not valid base64.
				blob = []byte(bv)
			} else {
				blob = decoded
			}
		case []byte:
			blob = bv
		}
	}

	if err := p.store.UploadLayerPart(defaultAccountID, repoName, uploadID, partFirst, partLast, blob); err != nil {
		if err == ErrLayerUploadNotFound {
			return ecrError("UploadNotFoundException", "upload not found", http.StatusBadRequest), nil
		}
		return nil, err
	}
	return ecrJSON(http.StatusOK, map[string]any{
		"uploadId":         uploadID,
		"lastByteReceived": partLast,
	})
}

func (p *Provider) handleCompleteLayerUpload(params map[string]any) (*plugin.Response, error) {
	repoName := strParam(params, "repositoryName")
	uploadID := strParam(params, "uploadId")
	if repoName == "" || uploadID == "" {
		return ecrError("InvalidParameterException", "repositoryName and uploadId are required", http.StatusBadRequest), nil
	}
	digests := strSliceParam(params, "layerDigests")
	digest := ""
	if len(digests) > 0 {
		digest = digests[0]
	}
	if err := p.store.CompleteLayerUpload(defaultAccountID, repoName, uploadID, digest); err != nil {
		if err == ErrLayerUploadNotFound {
			return ecrError("UploadNotFoundException", "upload not found", http.StatusBadRequest), nil
		}
		return nil, err
	}
	return ecrJSON(http.StatusOK, map[string]any{
		"uploadId":       uploadID,
		"layerDigest":    digest,
		"repositoryName": repoName,
		"registryId":     defaultAccountID,
	})
}

func (p *Provider) handleBatchCheckLayerAvailability(params map[string]any) (*plugin.Response, error) {
	repoName := strParam(params, "repositoryName")
	if repoName == "" {
		return ecrError("InvalidParameterException", "repositoryName is required", http.StatusBadRequest), nil
	}
	digests := strSliceParam(params, "layerDigests")
	layers, err := p.store.BatchCheckLayerAvailability(defaultAccountID, repoName, digests)
	if err != nil {
		return nil, err
	}
	items := make([]any, 0, len(layers))
	for _, l := range layers {
		items = append(items, l)
	}
	return ecrJSON(http.StatusOK, map[string]any{"layers": items, "failures": []any{}})
}

func (p *Provider) handleGetDownloadUrlForLayer(params map[string]any) (*plugin.Response, error) {
	repoName := strParam(params, "repositoryName")
	digest := strParam(params, "layerDigest")
	if repoName == "" || digest == "" {
		return ecrError("InvalidParameterException", "repositoryName and layerDigest are required", http.StatusBadRequest), nil
	}
	url, err := p.store.GetDownloadURLForLayer(defaultAccountID, repoName, digest)
	if err != nil {
		if err == ErrLayerUploadNotFound {
			return ecrError("LayerNotFoundException", "layer not found", http.StatusBadRequest), nil
		}
		return nil, err
	}
	return ecrJSON(http.StatusOK, map[string]any{
		"downloadUrl":    url,
		"layerDigest":    digest,
		"repositoryName": repoName,
		"registryId":     defaultAccountID,
	})
}

// --- lifecycle policy handlers ---

func (p *Provider) handlePutLifecyclePolicy(params map[string]any) (*plugin.Response, error) {
	repoName := strParam(params, "repositoryName")
	policyText := strParam(params, "lifecyclePolicyText")
	if repoName == "" || policyText == "" {
		return ecrError("InvalidParameterException", "repositoryName and lifecyclePolicyText are required", http.StatusBadRequest), nil
	}
	if err := p.store.PutLifecyclePolicy(defaultAccountID, repoName, policyText); err != nil {
		if err == ErrRepositoryNotFound {
			return ecrError("RepositoryNotFoundException", "repository not found", http.StatusBadRequest), nil
		}
		return nil, err
	}
	return ecrJSON(http.StatusOK, map[string]any{
		"repositoryName":      repoName,
		"registryId":          defaultAccountID,
		"lifecyclePolicyText": policyText,
	})
}

func (p *Provider) handleGetLifecyclePolicy(params map[string]any) (*plugin.Response, error) {
	repoName := strParam(params, "repositoryName")
	if repoName == "" {
		return ecrError("InvalidParameterException", "repositoryName is required", http.StatusBadRequest), nil
	}
	policyText, err := p.store.GetLifecyclePolicy(defaultAccountID, repoName)
	if err != nil {
		if err == ErrRepositoryNotFound {
			return ecrError("RepositoryNotFoundException", "repository not found", http.StatusBadRequest), nil
		}
		if err == ErrLifecyclePolicyNotFound {
			return ecrError("LifecyclePolicyNotFoundException", "lifecycle policy not found", http.StatusBadRequest), nil
		}
		return nil, err
	}
	return ecrJSON(http.StatusOK, map[string]any{
		"repositoryName":      repoName,
		"registryId":          defaultAccountID,
		"lifecyclePolicyText": policyText,
	})
}

func (p *Provider) handleDeleteLifecyclePolicy(params map[string]any) (*plugin.Response, error) {
	repoName := strParam(params, "repositoryName")
	if repoName == "" {
		return ecrError("InvalidParameterException", "repositoryName is required", http.StatusBadRequest), nil
	}
	policyText, getErr := p.store.GetLifecyclePolicy(defaultAccountID, repoName)
	if err := p.store.DeleteLifecyclePolicy(defaultAccountID, repoName); err != nil {
		if err == ErrRepositoryNotFound {
			return ecrError("RepositoryNotFoundException", "repository not found", http.StatusBadRequest), nil
		}
		if err == ErrLifecyclePolicyNotFound {
			return ecrError("LifecyclePolicyNotFoundException", "lifecycle policy not found", http.StatusBadRequest), nil
		}
		return nil, err
	}
	if getErr != nil {
		policyText = ""
	}
	return ecrJSON(http.StatusOK, map[string]any{
		"repositoryName":      repoName,
		"registryId":          defaultAccountID,
		"lifecyclePolicyText": policyText,
	})
}

// --- tag handlers ---

func (p *Provider) handleTagResource(params map[string]any) (*plugin.Response, error) {
	resourceARN := strParam(params, "resourceArn")
	if resourceARN == "" {
		return ecrError("InvalidParameterException", "resourceArn is required", http.StatusBadRequest), nil
	}
	tags := tagsParam(params, "tags")
	if err := p.store.TagResource(resourceARN, tags); err != nil {
		return nil, err
	}
	return ecrJSON(http.StatusOK, map[string]any{})
}

func (p *Provider) handleUntagResource(params map[string]any) (*plugin.Response, error) {
	resourceARN := strParam(params, "resourceArn")
	if resourceARN == "" {
		return ecrError("InvalidParameterException", "resourceArn is required", http.StatusBadRequest), nil
	}
	tagKeys := strSliceParam(params, "tagKeys")
	if err := p.store.UntagResource(resourceARN, tagKeys); err != nil {
		return nil, err
	}
	return ecrJSON(http.StatusOK, map[string]any{})
}

func (p *Provider) handleListTagsForResource(params map[string]any) (*plugin.Response, error) {
	resourceARN := strParam(params, "resourceArn")
	if resourceARN == "" {
		return ecrError("InvalidParameterException", "resourceArn is required", http.StatusBadRequest), nil
	}
	tags, err := p.store.ListTagsForResource(resourceARN)
	if err != nil {
		return nil, err
	}
	items := make([]any, 0, len(tags))
	for _, t := range tags {
		items = append(items, t)
	}
	return ecrJSON(http.StatusOK, map[string]any{"tags": items})
}

// --- image scan handlers ---

func (p *Provider) handleStartImageScan(params map[string]any) (*plugin.Response, error) {
	repoName := strParam(params, "repositoryName")
	if repoName == "" {
		return ecrError("InvalidParameterException", "repositoryName is required", http.StatusBadRequest), nil
	}
	imageID := imageIDParam(params, "imageId")
	digest := imageID["imageDigest"]
	tag := imageID["imageTag"]
	if digest == "" && tag != "" {
		// Resolve tag to digest.
		d, err := p.store.GetImageDigestByTag(defaultAccountID, repoName, tag)
		if err != nil {
			if err == ErrImageNotFound {
				return ecrError("ImageNotFoundException", "image not found", http.StatusBadRequest), nil
			}
			return nil, err
		}
		digest = d
	}
	if digest == "" {
		return ecrError("InvalidParameterException", "imageId must specify imageDigest or imageTag", http.StatusBadRequest), nil
	}
	if err := p.store.StartImageScan(defaultAccountID, repoName, digest); err != nil {
		return nil, err
	}
	return ecrJSON(http.StatusOK, map[string]any{
		"repositoryName": repoName,
		"registryId":     defaultAccountID,
		"imageId":        map[string]string{"imageDigest": digest},
		"imageScanStatus": map[string]any{
			"status": "IN_PROGRESS",
		},
	})
}

func (p *Provider) handleDescribeImageScanFindings(params map[string]any) (*plugin.Response, error) {
	repoName := strParam(params, "repositoryName")
	if repoName == "" {
		return ecrError("InvalidParameterException", "repositoryName is required", http.StatusBadRequest), nil
	}
	imageID := imageIDParam(params, "imageId")
	digest := imageID["imageDigest"]
	tag := imageID["imageTag"]
	if digest == "" && tag != "" {
		d, err := p.store.GetImageDigestByTag(defaultAccountID, repoName, tag)
		if err != nil {
			if err == ErrImageNotFound {
				return ecrError("ImageNotFoundException", "image not found", http.StatusBadRequest), nil
			}
			return nil, err
		}
		digest = d
	}
	if digest == "" {
		return ecrError("InvalidParameterException", "imageId must specify imageDigest or imageTag", http.StatusBadRequest), nil
	}
	status, findingsJSON, err := p.store.DescribeImageScanFindings(defaultAccountID, repoName, digest)
	if err != nil {
		if err == ErrImageNotFound {
			return ecrError("ScanNotFoundException", "scan not found", http.StatusBadRequest), nil
		}
		return nil, err
	}
	var findings any
	if err := json.Unmarshal([]byte(findingsJSON), &findings); err != nil {
		findings = []any{}
	}
	return ecrJSON(http.StatusOK, map[string]any{
		"repositoryName": repoName,
		"registryId":     defaultAccountID,
		"imageId":        map[string]string{"imageDigest": digest},
		"imageScanStatus": map[string]any{
			"status": status,
		},
		"imageScanFindings": map[string]any{
			"findings":              findings,
			"findingSeverityCounts": map[string]any{},
		},
	})
}

func (p *Provider) handlePutImageScanningConfiguration(params map[string]any) (*plugin.Response, error) {
	repoName := strParam(params, "repositoryName")
	if repoName == "" {
		return ecrError("InvalidParameterException", "repositoryName is required", http.StatusBadRequest), nil
	}
	scanOnPush := false
	if cfg, ok := params["imageScanningConfiguration"].(map[string]any); ok {
		if v, ok := cfg["scanOnPush"].(bool); ok {
			scanOnPush = v
		}
	}
	if err := p.store.PutImageScanningConfiguration(defaultAccountID, repoName, scanOnPush); err != nil {
		if err == ErrRepositoryNotFound {
			return ecrError("RepositoryNotFoundException", "repository not found", http.StatusBadRequest), nil
		}
		return nil, err
	}
	return ecrJSON(http.StatusOK, map[string]any{
		"repositoryName": repoName,
		"registryId":     defaultAccountID,
		"imageScanningConfiguration": map[string]any{
			"scanOnPush": scanOnPush,
		},
	})
}

// --- helpers ---

func ecrError(code, message string, status int) *plugin.Response {
	body, _ := json.Marshal(map[string]any{"__type": code, "message": message})
	return &plugin.Response{StatusCode: status, ContentType: "application/x-amz-json-1.1", Body: body}
}

func ecrJSON(status int, v any) (*plugin.Response, error) {
	body, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	return &plugin.Response{StatusCode: status, ContentType: "application/x-amz-json-1.1", Body: body}, nil
}

func strParam(params map[string]any, key string) string {
	if v, ok := params[key]; ok {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}

func strSliceParam(params map[string]any, key string) []string {
	v, ok := params[key]
	if !ok {
		return nil
	}
	arr, ok := v.([]any)
	if !ok {
		return nil
	}
	out := make([]string, 0, len(arr))
	for _, item := range arr {
		if s, ok := item.(string); ok {
			out = append(out, s)
		}
	}
	return out
}

func imageIDsParam(params map[string]any, key string) []map[string]string {
	v, ok := params[key]
	if !ok {
		return nil
	}
	arr, ok := v.([]any)
	if !ok {
		return nil
	}
	out := make([]map[string]string, 0, len(arr))
	for _, item := range arr {
		if m, ok := item.(map[string]any); ok {
			entry := map[string]string{}
			if d, ok := m["imageDigest"].(string); ok {
				entry["imageDigest"] = d
			}
			if t, ok := m["imageTag"].(string); ok {
				entry["imageTag"] = t
			}
			out = append(out, entry)
		}
	}
	return out
}

func int64Param(params map[string]any, key string) int64 {
	if v, ok := params[key]; ok {
		switch n := v.(type) {
		case float64:
			return int64(n)
		case int64:
			return n
		case int:
			return int64(n)
		}
	}
	return 0
}

func tagsParam(params map[string]any, key string) []map[string]string {
	v, ok := params[key]
	if !ok {
		return nil
	}
	arr, ok := v.([]any)
	if !ok {
		return nil
	}
	out := make([]map[string]string, 0, len(arr))
	for _, item := range arr {
		if m, ok := item.(map[string]any); ok {
			entry := map[string]string{}
			if k, ok := m["Key"].(string); ok {
				entry["Key"] = k
			}
			if val, ok := m["Value"].(string); ok {
				entry["Value"] = val
			}
			out = append(out, entry)
		}
	}
	return out
}

func imageIDParam(params map[string]any, key string) map[string]string {
	v, ok := params[key]
	if !ok {
		return map[string]string{}
	}
	m, ok := v.(map[string]any)
	if !ok {
		return map[string]string{}
	}
	out := map[string]string{}
	if d, ok := m["imageDigest"].(string); ok {
		out["imageDigest"] = d
	}
	if t, ok := m["imageTag"].(string); ok {
		out["imageTag"] = t
	}
	return out
}

func repoToMap(r *Repository) map[string]any {
	return map[string]any{
		"repositoryArn":  r.ARN,
		"repositoryName": r.Name,
		"registryId":     r.RegistryID,
		"repositoryUri":  r.URI,
		"createdAt":      r.CreatedAt.Format(time.RFC3339),
	}
}

func imageToMap(img *Image) map[string]any {
	return map[string]any{
		"repositoryName": img.RepoName,
		"registryId":     img.AccountID,
		"imageId": map[string]string{
			"imageDigest": img.ImageDigest,
			"imageTag":    img.ImageTag,
		},
	}
}

func imageDetailToMap(img *Image) map[string]any {
	return map[string]any{
		"repositoryName":   img.RepoName,
		"registryId":       img.AccountID,
		"imageDigest":      img.ImageDigest,
		"imageTags":        []string{img.ImageTag},
		"imageSizeInBytes": img.SizeBytes,
		"imagePushedAt":    img.PushedAt.Format(time.RFC3339),
	}
}
