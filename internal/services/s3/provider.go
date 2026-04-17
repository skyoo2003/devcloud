// SPDX-License-Identifier: Apache-2.0

package s3

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/rand"
	"encoding/hex"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/skyoo2003/devcloud/internal/plugin"
)

const defaultAccountID = plugin.DefaultAccountID

// S3Provider implements plugin.ServicePlugin using FileStore and MetadataStore.
type S3Provider struct {
	fileStore  *FileStore
	metaStore  *MetadataStore
	serverPort int // used to emit event notifications; 0 means disabled
}

// ServiceID returns the unique identifier for this plugin.
func (p *S3Provider) ServiceID() string { return "s3" }

// ServiceName returns the human-readable name for this plugin.
func (p *S3Provider) ServiceName() string { return "Amazon S3" }

// Protocol returns the wire protocol used by this plugin.
func (p *S3Provider) Protocol() plugin.ProtocolType { return plugin.ProtocolRESTXML }

// Init initialises the FileStore and MetadataStore from cfg.
func (p *S3Provider) Init(cfg plugin.PluginConfig) error {
	if err := os.MkdirAll(cfg.DataDir, 0o755); err != nil {
		return fmt.Errorf("init s3: %w", err)
	}

	p.fileStore = NewFileStore(cfg.DataDir)

	dbPath := filepath.Join(cfg.DataDir, "meta.db")
	if v, ok := cfg.Options["db_path"]; ok {
		if s, ok := v.(string); ok && s != "" {
			dbPath = s
		}
	}

	var err error
	p.metaStore, err = NewMetadataStore(dbPath)
	if err != nil {
		return err
	}

	if v, ok := cfg.Options["server_port"]; ok {
		switch port := v.(type) {
		case int:
			p.serverPort = port
		case int64:
			p.serverPort = int(port)
		}
	}

	return nil
}

// Shutdown closes the MetadataStore.
func (p *S3Provider) Shutdown(_ context.Context) error {
	if p.metaStore != nil {
		return p.metaStore.Close()
	}
	return nil
}

// HandleRequest routes the incoming HTTP request to the appropriate S3 operation.
func (p *S3Provider) HandleRequest(ctx context.Context, _ string, req *http.Request) (*plugin.Response, error) {
	bucket, key := splitBucketKey(req.URL.Path)
	q := req.URL.Query()

	switch req.Method {
	case http.MethodGet:
		if bucket == "" {
			return p.listBuckets(ctx)
		}
		if key == "" {
			// Bucket-level subresource checks
			if _, ok := q["uploads"]; ok {
				return p.listMultipartUploads(ctx, bucket)
			}
			if _, ok := q["policy"]; ok {
				return p.getBucketPolicy(ctx, bucket)
			}
			if _, ok := q["location"]; ok {
				return p.getBucketLocation(ctx, bucket)
			}
			if _, ok := q["versioning"]; ok {
				return p.getBucketVersioning(ctx, bucket)
			}
			if _, ok := q["cors"]; ok {
				return p.getBucketCors(ctx, bucket)
			}
			if _, ok := q["tagging"]; ok {
				return p.getBucketTagging(ctx, bucket)
			}
			if _, ok := q["acl"]; ok {
				return p.getBucketACL(ctx, bucket)
			}
			if _, ok := q["notification"]; ok {
				return p.getBucketNotification(ctx, bucket)
			}
			if q.Get("list-type") == "2" {
				return p.listObjectsV2(ctx, bucket, q)
			}
			prefix := q.Get("prefix")
			delimiter := q.Get("delimiter")
			return p.listObjects(ctx, bucket, prefix, delimiter)
		}
		// Object-level subresource checks
		if _, ok := q["tagging"]; ok {
			return p.getObjectTagging(ctx, bucket, key)
		}
		if uploadID := q.Get("uploadId"); uploadID != "" {
			return p.listParts(ctx, bucket, key, uploadID)
		}
		return p.getObject(ctx, bucket, key)

	case http.MethodPut:
		if bucket == "" {
			return xmlError("InvalidRequest", "bucket name required", http.StatusBadRequest), nil
		}
		if key == "" {
			// Bucket-level subresource checks
			if _, ok := q["policy"]; ok {
				return p.putBucketPolicy(ctx, bucket, req)
			}
			if _, ok := q["versioning"]; ok {
				return p.putBucketVersioning(ctx, bucket, req)
			}
			if _, ok := q["cors"]; ok {
				return p.putBucketCors(ctx, bucket, req)
			}
			if _, ok := q["tagging"]; ok {
				return p.putBucketTagging(ctx, bucket, req)
			}
			if _, ok := q["acl"]; ok {
				return p.putBucketACL(ctx, bucket, req)
			}
			if _, ok := q["notification"]; ok {
				return p.putBucketNotification(ctx, bucket, req)
			}
			return p.createBucket(ctx, bucket)
		}
		// Object-level subresource checks
		if _, ok := q["tagging"]; ok {
			return p.putObjectTagging(ctx, bucket, key, req)
		}
		if partNumberStr := q.Get("partNumber"); partNumberStr != "" {
			uploadID := q.Get("uploadId")
			return p.uploadPart(ctx, bucket, key, uploadID, partNumberStr, req)
		}
		if copySource := req.Header.Get("X-Amz-Copy-Source"); copySource != "" {
			return p.copyObject(ctx, bucket, key, copySource)
		}
		return p.putObject(ctx, bucket, key, req)

	case http.MethodDelete:
		if bucket == "" {
			return xmlError("InvalidRequest", "bucket name required", http.StatusBadRequest), nil
		}
		if key == "" {
			// Bucket-level subresource checks
			if _, ok := q["policy"]; ok {
				return p.deleteBucketPolicy(ctx, bucket)
			}
			if _, ok := q["cors"]; ok {
				return p.deleteBucketCors(ctx, bucket)
			}
			if _, ok := q["tagging"]; ok {
				return p.deleteBucketTagging(ctx, bucket)
			}
			return p.deleteBucket(ctx, bucket)
		}
		// Object-level subresource checks
		if _, ok := q["tagging"]; ok {
			return p.deleteObjectTagging(ctx, bucket, key)
		}
		if uploadID := q.Get("uploadId"); uploadID != "" {
			return p.abortMultipartUpload(ctx, bucket, key, uploadID)
		}
		return p.deleteObject(ctx, bucket, key)

	case http.MethodHead:
		if key == "" {
			return p.headBucket(ctx, bucket)
		}
		return p.headObject(ctx, bucket, key)

	case http.MethodPost:
		if bucket != "" && key != "" {
			if _, ok := q["uploads"]; ok {
				return p.createMultipartUpload(ctx, bucket, key)
			}
			if uploadID := q.Get("uploadId"); uploadID != "" {
				return p.completeMultipartUpload(ctx, bucket, key, uploadID, req)
			}
			if _, ok := q["delete"]; ok {
				return p.deleteObjects(ctx, bucket, req)
			}
		}
		if bucket != "" && key == "" {
			if _, ok := q["delete"]; ok {
				return p.deleteObjects(ctx, bucket, req)
			}
		}
		return xmlError("MethodNotAllowed", "method not allowed", http.StatusMethodNotAllowed), nil

	default:
		return xmlError("MethodNotAllowed", "method not allowed", http.StatusMethodNotAllowed), nil
	}
}

// ListResources returns all buckets as plugin resources.
func (p *S3Provider) ListResources(ctx context.Context) ([]plugin.Resource, error) {
	buckets, err := p.metaStore.ListBuckets(defaultAccountID)
	if err != nil {
		return nil, err
	}
	resources := make([]plugin.Resource, 0, len(buckets))
	for _, b := range buckets {
		resources = append(resources, plugin.Resource{
			Type: "bucket",
			ID:   b.Name,
			Name: b.Name,
		})
	}
	return resources, nil
}

// GetMetrics returns empty metrics.
func (p *S3Provider) GetMetrics(_ context.Context) (*plugin.ServiceMetrics, error) {
	return &plugin.ServiceMetrics{}, nil
}

// --- XML response structs ---

type listAllMyBucketsResult struct {
	XMLName xml.Name    `xml:"ListAllMyBucketsResult"`
	Buckets []bucketXML `xml:"Buckets>Bucket"`
}

type bucketXML struct {
	Name         string `xml:"Name"`
	CreationDate string `xml:"CreationDate"`
}

type listBucketResult struct {
	XMLName        xml.Name         `xml:"ListBucketResult"`
	Name           string           `xml:"Name"`
	Prefix         string           `xml:"Prefix"`
	Delimiter      string           `xml:"Delimiter,omitempty"`
	MaxKeys        int              `xml:"MaxKeys"`
	Contents       []objectXML      `xml:"Contents"`
	CommonPrefixes []commonPrefixes `xml:"CommonPrefixes,omitempty"`
}

type listBucketV2Result struct {
	XMLName               xml.Name         `xml:"ListBucketResult"`
	Xmlns                 string           `xml:"xmlns,attr"`
	Name                  string           `xml:"Name"`
	Prefix                string           `xml:"Prefix"`
	Delimiter             string           `xml:"Delimiter,omitempty"`
	MaxKeys               int              `xml:"MaxKeys"`
	KeyCount              int              `xml:"KeyCount"`
	IsTruncated           bool             `xml:"IsTruncated"`
	Contents              []objectXML      `xml:"Contents"`
	CommonPrefixes        []commonPrefixes `xml:"CommonPrefixes,omitempty"`
	ContinuationToken     string           `xml:"ContinuationToken,omitempty"`
	NextContinuationToken string           `xml:"NextContinuationToken,omitempty"`
	StartAfter            string           `xml:"StartAfter,omitempty"`
}

type commonPrefixes struct {
	Prefix string `xml:"Prefix"`
}

type copyObjectResult struct {
	XMLName      xml.Name `xml:"CopyObjectResult"`
	LastModified string   `xml:"LastModified"`
	ETag         string   `xml:"ETag"`
}

type objectXML struct {
	Key          string `xml:"Key"`
	LastModified string `xml:"LastModified"`
	ETag         string `xml:"ETag"`
	Size         int64  `xml:"Size"`
}

type errorResult struct {
	XMLName xml.Name `xml:"Error"`
	Code    string   `xml:"Code"`
	Message string   `xml:"Message"`
}

// Multipart upload XML structs

type initiateMultipartUploadResult struct {
	XMLName  xml.Name `xml:"InitiateMultipartUploadResult"`
	Bucket   string   `xml:"Bucket"`
	Key      string   `xml:"Key"`
	UploadID string   `xml:"UploadId"`
}

type completeMultipartUploadRequest struct {
	XMLName xml.Name              `xml:"CompleteMultipartUpload"`
	Parts   []completePartRequest `xml:"Part"`
}

type completePartRequest struct {
	PartNumber int    `xml:"PartNumber"`
	ETag       string `xml:"ETag"`
}

type completeMultipartUploadResult struct {
	XMLName  xml.Name `xml:"CompleteMultipartUploadResult"`
	Location string   `xml:"Location"`
	Bucket   string   `xml:"Bucket"`
	Key      string   `xml:"Key"`
	ETag     string   `xml:"ETag"`
}

type listMultipartUploadsResult struct {
	XMLName xml.Name    `xml:"ListMultipartUploadsResult"`
	Bucket  string      `xml:"Bucket"`
	Uploads []uploadXML `xml:"Upload"`
}

type uploadXML struct {
	UploadID  string `xml:"UploadId"`
	Key       string `xml:"Key"`
	Initiated string `xml:"Initiated"`
}

type listPartsResult struct {
	XMLName  xml.Name  `xml:"ListPartsResult"`
	Bucket   string    `xml:"Bucket"`
	Key      string    `xml:"Key"`
	UploadID string    `xml:"UploadId"`
	Parts    []partXML `xml:"Part"`
}

type partXML struct {
	PartNumber int    `xml:"PartNumber"`
	ETag       string `xml:"ETag"`
	Size       int64  `xml:"Size"`
}

// DeleteObjects XML structs

type deleteObjectsRequest struct {
	XMLName xml.Name            `xml:"Delete"`
	Objects []deleteObjectEntry `xml:"Object"`
}

type deleteObjectEntry struct {
	Key string `xml:"Key"`
}

type deleteObjectsResult struct {
	XMLName xml.Name           `xml:"DeleteResult"`
	Deleted []deletedObjectXML `xml:"Deleted"`
}

type deletedObjectXML struct {
	Key string `xml:"Key"`
}

// Tagging XML structs

type taggingXML struct {
	XMLName xml.Name `xml:"Tagging"`
	TagSet  []tagXML `xml:"TagSet>Tag"`
}

type tagXML struct {
	Key   string `xml:"Key"`
	Value string `xml:"Value"`
}

// Versioning XML structs

type versioningConfigurationXML struct {
	XMLName xml.Name `xml:"VersioningConfiguration"`
	Status  string   `xml:"Status"`
}

// Location XML struct

type locationConstraintXML struct {
	XMLName xml.Name `xml:"LocationConstraint"`
	Region  string   `xml:",chardata"`
}

// --- helpers ---

// splitBucketKey splits a URL path like /bucket/key/parts into (bucket, key).
// The leading slash is stripped before splitting.
func splitBucketKey(path string) (bucket, key string) {
	path = strings.TrimPrefix(path, "/")
	if path == "" {
		return "", ""
	}
	idx := strings.IndexByte(path, '/')
	if idx < 0 {
		return path, ""
	}
	return path[:idx], path[idx+1:]
}

// xmlError builds an XML error response.
func xmlError(code, message string, status int) *plugin.Response {
	body, _ := xml.Marshal(errorResult{Code: code, Message: message})
	return &plugin.Response{
		StatusCode:  status,
		ContentType: "application/xml",
		Body:        body,
	}
}

// xmlResponse marshals v into an XML response with the given status.
func xmlResponse(status int, v any) (*plugin.Response, error) {
	body, err := xml.Marshal(v)
	if err != nil {
		return nil, err
	}
	return &plugin.Response{
		StatusCode:  status,
		ContentType: "application/xml",
		Body:        body,
	}, nil
}

// generateUploadID generates a random hex string to use as an upload ID.
func generateUploadID() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}

// multipartDir returns the directory used to store parts for an upload.
func (p *S3Provider) multipartDir(uploadID string) string {
	return filepath.Join(p.fileStore.baseDir, "_multipart", uploadID)
}

// partPath returns the path to a specific part file.
func (p *S3Provider) partPath(uploadID string, partNumber int) string {
	return filepath.Join(p.multipartDir(uploadID), strconv.Itoa(partNumber))
}

// --- S3 operation implementations ---

func (p *S3Provider) listBuckets(_ context.Context) (*plugin.Response, error) {
	buckets, err := p.metaStore.ListBuckets(defaultAccountID)
	if err != nil {
		return nil, err
	}
	result := listAllMyBucketsResult{}
	for _, b := range buckets {
		result.Buckets = append(result.Buckets, bucketXML{
			Name:         b.Name,
			CreationDate: b.CreatedAt.UTC().Format(time.RFC3339),
		})
	}
	return xmlResponse(http.StatusOK, result)
}

func (p *S3Provider) createBucket(_ context.Context, bucket string) (*plugin.Response, error) {
	if err := p.metaStore.CreateBucket(bucket, "us-east-1", defaultAccountID); err != nil {
		if errors.Is(err, ErrBucketAlreadyExists) {
			// S3 allows re-creating the same bucket by the same owner — return 200.
			return &plugin.Response{StatusCode: http.StatusOK}, nil
		}
		return nil, err
	}
	if err := p.fileStore.CreateBucketDir(defaultAccountID, bucket); err != nil {
		return nil, err
	}
	return &plugin.Response{StatusCode: http.StatusOK}, nil
}

func (p *S3Provider) deleteBucket(_ context.Context, bucket string) (*plugin.Response, error) {
	if err := p.metaStore.DeleteBucket(bucket, defaultAccountID); err != nil {
		if errors.Is(err, ErrBucketNotFound) {
			return xmlError("NoSuchBucket", fmt.Sprintf("bucket %q not found", bucket), http.StatusNotFound), nil
		}
		return nil, err
	}
	if err := p.fileStore.DeleteBucketDir(defaultAccountID, bucket); err != nil {
		return nil, err
	}
	return &plugin.Response{StatusCode: http.StatusNoContent}, nil
}

func (p *S3Provider) headBucket(_ context.Context, bucket string) (*plugin.Response, error) {
	buckets, err := p.metaStore.ListBuckets(defaultAccountID)
	if err != nil {
		return nil, err
	}
	for _, b := range buckets {
		if b.Name == bucket {
			return &plugin.Response{
				StatusCode: http.StatusOK,
				Headers: map[string]string{
					"x-amz-bucket-region": b.Region,
				},
			}, nil
		}
	}
	return &plugin.Response{StatusCode: http.StatusNotFound}, nil
}

func (p *S3Provider) copyObject(_ context.Context, destBucket, destKey, copySource string) (*plugin.Response, error) {
	// copySource is "/<bucket>/<key>" or "<bucket>/<key>"
	copySource = strings.TrimPrefix(copySource, "/")
	idx := strings.IndexByte(copySource, '/')
	if idx < 0 {
		return xmlError("InvalidArgument", "invalid copy source", http.StatusBadRequest), nil
	}
	srcBucket := copySource[:idx]
	srcKey := copySource[idx+1:]

	// Read source object data and metadata
	srcMeta, err := p.metaStore.GetObjectMeta(srcBucket, srcKey, defaultAccountID)
	if err != nil {
		if errors.Is(err, ErrObjectNotFound) {
			return xmlError("NoSuchKey", fmt.Sprintf("key %q not found", srcKey), http.StatusNotFound), nil
		}
		return nil, err
	}
	data, err := p.fileStore.GetObject(defaultAccountID, srcBucket, srcKey)
	if err != nil {
		return nil, err
	}

	// Write destination object
	if err := p.fileStore.PutObject(defaultAccountID, destBucket, destKey, data); err != nil {
		return nil, err
	}
	now := time.Now()
	destMeta := ObjectMeta{
		Bucket:       destBucket,
		Key:          destKey,
		Size:         srcMeta.Size,
		ContentType:  srcMeta.ContentType,
		ETag:         srcMeta.ETag,
		AccountID:    defaultAccountID,
		LastModified: now,
	}
	if err := p.metaStore.PutObjectMeta(destMeta); err != nil {
		return nil, err
	}

	p.emitS3Event(context.Background(), destBucket, destKey, destMeta.Size, "ObjectCreated:Copy")

	result := copyObjectResult{
		LastModified: now.UTC().Format(time.RFC3339),
		ETag:         srcMeta.ETag,
	}
	return xmlResponse(http.StatusOK, result)
}

func (p *S3Provider) putObject(_ context.Context, bucket, key string, req *http.Request) (*plugin.Response, error) {
	data, err := io.ReadAll(req.Body)
	if err != nil {
		return nil, err
	}

	sum := md5.Sum(data)
	etag := fmt.Sprintf("%x", sum)

	contentType := req.Header.Get("Content-Type")
	if contentType == "" {
		contentType = "application/octet-stream"
	}

	if err := p.fileStore.PutObject(defaultAccountID, bucket, key, data); err != nil {
		return nil, err
	}

	meta := ObjectMeta{
		Bucket:       bucket,
		Key:          key,
		Size:         int64(len(data)),
		ContentType:  contentType,
		ETag:         etag,
		AccountID:    defaultAccountID,
		LastModified: time.Now(),
	}
	if err := p.metaStore.PutObjectMeta(meta); err != nil {
		return nil, err
	}

	p.emitS3Event(context.Background(), bucket, key, int64(len(data)), "ObjectCreated:Put")

	return &plugin.Response{
		StatusCode: http.StatusOK,
		Headers:    map[string]string{"ETag": etag},
	}, nil
}

func (p *S3Provider) getObject(_ context.Context, bucket, key string) (*plugin.Response, error) {
	meta, err := p.metaStore.GetObjectMeta(bucket, key, defaultAccountID)
	if err != nil {
		if errors.Is(err, ErrObjectNotFound) {
			return xmlError("NoSuchKey", fmt.Sprintf("key %q not found", key), http.StatusNotFound), nil
		}
		return nil, err
	}

	data, err := p.fileStore.GetObject(defaultAccountID, bucket, key)
	if err != nil {
		return nil, err
	}

	return &plugin.Response{
		StatusCode:  http.StatusOK,
		ContentType: meta.ContentType,
		Body:        data,
		Headers: map[string]string{
			"ETag":          meta.ETag,
			"Last-Modified": meta.LastModified.UTC().Format(time.RFC1123),
		},
	}, nil
}

func (p *S3Provider) headObject(_ context.Context, bucket, key string) (*plugin.Response, error) {
	meta, err := p.metaStore.GetObjectMeta(bucket, key, defaultAccountID)
	if err != nil {
		if errors.Is(err, ErrObjectNotFound) {
			return xmlError("NoSuchKey", fmt.Sprintf("key %q not found", key), http.StatusNotFound), nil
		}
		return nil, err
	}

	return &plugin.Response{
		StatusCode:  http.StatusOK,
		ContentType: meta.ContentType,
		Headers: map[string]string{
			"ETag":           meta.ETag,
			"Last-Modified":  meta.LastModified.UTC().Format(time.RFC1123),
			"Content-Length": fmt.Sprintf("%d", meta.Size),
		},
	}, nil
}

func (p *S3Provider) deleteObject(_ context.Context, bucket, key string) (*plugin.Response, error) {
	if err := p.metaStore.DeleteObjectMeta(bucket, key, defaultAccountID); err != nil {
		if errors.Is(err, ErrObjectNotFound) {
			return xmlError("NoSuchKey", fmt.Sprintf("key %q not found", key), http.StatusNotFound), nil
		}
		return nil, err
	}
	if err := p.fileStore.DeleteObject(defaultAccountID, bucket, key); err != nil {
		return nil, err
	}
	p.emitS3Event(context.Background(), bucket, key, 0, "ObjectRemoved:Delete")
	return &plugin.Response{StatusCode: http.StatusNoContent}, nil
}

func (p *S3Provider) listObjects(_ context.Context, bucket, prefix, delimiter string) (*plugin.Response, error) {
	objects, err := p.metaStore.ListObjects(bucket, prefix, defaultAccountID, 0)
	if err != nil {
		return nil, err
	}

	result := listBucketResult{
		Name:      bucket,
		Prefix:    prefix,
		Delimiter: delimiter,
		MaxKeys:   1000,
	}

	if delimiter != "" {
		seen := make(map[string]bool)
		for _, o := range objects {
			rest := strings.TrimPrefix(o.Key, prefix)
			if idx := strings.Index(rest, delimiter); idx >= 0 {
				cp := prefix + rest[:idx+len(delimiter)]
				if !seen[cp] {
					seen[cp] = true
					result.CommonPrefixes = append(result.CommonPrefixes, commonPrefixes{Prefix: cp})
				}
			} else {
				result.Contents = append(result.Contents, objectXML{
					Key:          o.Key,
					LastModified: o.LastModified.UTC().Format(time.RFC3339),
					ETag:         o.ETag,
					Size:         o.Size,
				})
			}
		}
	} else {
		for _, o := range objects {
			result.Contents = append(result.Contents, objectXML{
				Key:          o.Key,
				LastModified: o.LastModified.UTC().Format(time.RFC3339),
				ETag:         o.ETag,
				Size:         o.Size,
			})
		}
	}
	return xmlResponse(http.StatusOK, result)
}

// --- Task 9: ListObjectsV2 ---

func (p *S3Provider) listObjectsV2(_ context.Context, bucket string, q url.Values) (*plugin.Response, error) {
	prefix := q.Get("prefix")
	delimiter := q.Get("delimiter")
	continuationToken := q.Get("continuation-token")
	startAfter := q.Get("start-after")

	maxKeys := 1000
	if s := q.Get("max-keys"); s != "" {
		if n, err := strconv.Atoi(s); err == nil && n > 0 {
			maxKeys = n
		}
	}

	// Fetch all objects matching prefix (no limit at DB level for pagination)
	all, err := p.metaStore.ListObjects(bucket, prefix, defaultAccountID, 0)
	if err != nil {
		return nil, err
	}

	// Determine the marker: continuation-token takes precedence over start-after
	marker := startAfter
	if continuationToken != "" {
		marker = continuationToken
	}

	// Filter objects that come after the marker
	var filtered []ObjectMeta
	for _, o := range all {
		if marker == "" || o.Key > marker {
			filtered = append(filtered, o)
		}
	}

	result := listBucketV2Result{
		Xmlns:             "http://s3.amazonaws.com/doc/2006-03-01/",
		Name:              bucket,
		Prefix:            prefix,
		Delimiter:         delimiter,
		MaxKeys:           maxKeys,
		ContinuationToken: continuationToken,
		StartAfter:        startAfter,
	}

	if delimiter != "" {
		seen := make(map[string]bool)
		count := 0
		for _, o := range filtered {
			if count >= maxKeys {
				result.IsTruncated = true
				result.NextContinuationToken = o.Key
				break
			}
			rest := strings.TrimPrefix(o.Key, prefix)
			if idx := strings.Index(rest, delimiter); idx >= 0 {
				cp := prefix + rest[:idx+len(delimiter)]
				if !seen[cp] {
					seen[cp] = true
					result.CommonPrefixes = append(result.CommonPrefixes, commonPrefixes{Prefix: cp})
					count++
				}
			} else {
				result.Contents = append(result.Contents, objectXML{
					Key:          o.Key,
					LastModified: o.LastModified.UTC().Format(time.RFC3339),
					ETag:         o.ETag,
					Size:         o.Size,
				})
				count++
			}
		}
		result.KeyCount = count
	} else {
		total := len(filtered)
		if total > maxKeys {
			result.IsTruncated = true
			result.NextContinuationToken = filtered[maxKeys].Key
			filtered = filtered[:maxKeys]
		}
		result.KeyCount = len(filtered)
		for _, o := range filtered {
			result.Contents = append(result.Contents, objectXML{
				Key:          o.Key,
				LastModified: o.LastModified.UTC().Format(time.RFC3339),
				ETag:         o.ETag,
				Size:         o.Size,
			})
		}
	}

	return xmlResponse(http.StatusOK, result)
}

// --- Task 10: Multipart Upload ---

func (p *S3Provider) createMultipartUpload(_ context.Context, bucket, key string) (*plugin.Response, error) {
	uploadID, err := generateUploadID()
	if err != nil {
		return nil, err
	}
	if err := p.metaStore.CreateMultipartUpload(uploadID, bucket, key, defaultAccountID); err != nil {
		return nil, err
	}
	// Create directory for parts
	dir := p.multipartDir(uploadID)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	result := initiateMultipartUploadResult{
		Bucket:   bucket,
		Key:      key,
		UploadID: uploadID,
	}
	return xmlResponse(http.StatusOK, result)
}

func (p *S3Provider) uploadPart(_ context.Context, bucket, key, uploadID, partNumberStr string, req *http.Request) (*plugin.Response, error) {
	partNumber, err := strconv.Atoi(partNumberStr)
	if err != nil || partNumber < 1 {
		return xmlError("InvalidArgument", "invalid part number", http.StatusBadRequest), nil
	}

	if _, err := p.metaStore.GetMultipartUpload(uploadID); err != nil {
		if errors.Is(err, ErrUploadNotFound) {
			return xmlError("NoSuchUpload", "upload not found", http.StatusNotFound), nil
		}
		return nil, err
	}

	data, err := io.ReadAll(req.Body)
	if err != nil {
		return nil, err
	}

	sum := md5.Sum(data)
	etag := fmt.Sprintf("\"%x\"", sum)

	partFile := p.partPath(uploadID, partNumber)
	if err := os.WriteFile(partFile, data, 0o644); err != nil {
		return nil, err
	}

	if err := p.metaStore.PutUploadPart(UploadPartInfo{
		UploadID:   uploadID,
		PartNumber: partNumber,
		ETag:       etag,
		Size:       int64(len(data)),
	}); err != nil {
		return nil, err
	}

	return &plugin.Response{
		StatusCode: http.StatusOK,
		Headers:    map[string]string{"ETag": etag},
	}, nil
}

func (p *S3Provider) completeMultipartUpload(_ context.Context, bucket, key, uploadID string, req *http.Request) (*plugin.Response, error) {
	upload, err := p.metaStore.GetMultipartUpload(uploadID)
	if err != nil {
		if errors.Is(err, ErrUploadNotFound) {
			return xmlError("NoSuchUpload", "upload not found", http.StatusNotFound), nil
		}
		return nil, err
	}
	_ = upload

	body, err := io.ReadAll(req.Body)
	if err != nil {
		return nil, err
	}

	var completeReq completeMultipartUploadRequest
	if err := xml.Unmarshal(body, &completeReq); err != nil {
		return nil, err
	}

	// Sort parts by part number
	sort.Slice(completeReq.Parts, func(i, j int) bool {
		return completeReq.Parts[i].PartNumber < completeReq.Parts[j].PartNumber
	})

	// Concatenate parts
	var buf bytes.Buffer
	for _, part := range completeReq.Parts {
		partFile := p.partPath(uploadID, part.PartNumber)
		data, err := os.ReadFile(partFile)
		if err != nil {
			return xmlError("InvalidPart", fmt.Sprintf("part %d not found", part.PartNumber), http.StatusBadRequest), nil
		}
		buf.Write(data)
	}

	finalData := buf.Bytes()
	sum := md5.Sum(finalData)
	etag := fmt.Sprintf("\"%x\"", sum)

	if err := p.fileStore.PutObject(defaultAccountID, bucket, key, finalData); err != nil {
		return nil, err
	}

	meta := ObjectMeta{
		Bucket:       bucket,
		Key:          key,
		Size:         int64(len(finalData)),
		ContentType:  "application/octet-stream",
		ETag:         etag,
		AccountID:    defaultAccountID,
		LastModified: time.Now(),
	}
	if err := p.metaStore.PutObjectMeta(meta); err != nil {
		return nil, err
	}

	// Clean up parts directory
	os.RemoveAll(p.multipartDir(uploadID))

	// Remove upload from metadata
	if err := p.metaStore.DeleteMultipartUpload(uploadID); err != nil {
		return nil, err
	}

	p.emitS3Event(context.Background(), bucket, key, int64(len(finalData)), "ObjectCreated:CompleteMultipartUpload")

	result := completeMultipartUploadResult{
		Location: fmt.Sprintf("/%s/%s", bucket, key),
		Bucket:   bucket,
		Key:      key,
		ETag:     etag,
	}
	return xmlResponse(http.StatusOK, result)
}

func (p *S3Provider) abortMultipartUpload(_ context.Context, bucket, key, uploadID string) (*plugin.Response, error) {
	if _, err := p.metaStore.GetMultipartUpload(uploadID); err != nil {
		if errors.Is(err, ErrUploadNotFound) {
			return xmlError("NoSuchUpload", "upload not found", http.StatusNotFound), nil
		}
		return nil, err
	}
	_ = bucket
	_ = key

	// Delete part files
	os.RemoveAll(p.multipartDir(uploadID))

	if err := p.metaStore.DeleteMultipartUpload(uploadID); err != nil {
		return nil, err
	}

	return &plugin.Response{StatusCode: http.StatusNoContent}, nil
}

func (p *S3Provider) listMultipartUploads(_ context.Context, bucket string) (*plugin.Response, error) {
	uploads, err := p.metaStore.ListMultipartUploads(bucket, defaultAccountID)
	if err != nil {
		return nil, err
	}
	result := listMultipartUploadsResult{Bucket: bucket}
	for _, u := range uploads {
		result.Uploads = append(result.Uploads, uploadXML{
			UploadID:  u.UploadID,
			Key:       u.Key,
			Initiated: u.CreatedAt.UTC().Format(time.RFC3339),
		})
	}
	return xmlResponse(http.StatusOK, result)
}

func (p *S3Provider) listParts(_ context.Context, bucket, key, uploadID string) (*plugin.Response, error) {
	if _, err := p.metaStore.GetMultipartUpload(uploadID); err != nil {
		if errors.Is(err, ErrUploadNotFound) {
			return xmlError("NoSuchUpload", "upload not found", http.StatusNotFound), nil
		}
		return nil, err
	}
	parts, err := p.metaStore.ListUploadParts(uploadID)
	if err != nil {
		return nil, err
	}
	result := listPartsResult{
		Bucket:   bucket,
		Key:      key,
		UploadID: uploadID,
	}
	for _, p := range parts {
		result.Parts = append(result.Parts, partXML{
			PartNumber: p.PartNumber,
			ETag:       p.ETag,
			Size:       p.Size,
		})
	}
	return xmlResponse(http.StatusOK, result)
}

// --- Task 11: DeleteObjects ---

func (p *S3Provider) deleteObjects(_ context.Context, bucket string, req *http.Request) (*plugin.Response, error) {
	body, err := io.ReadAll(req.Body)
	if err != nil {
		return nil, err
	}

	var delReq deleteObjectsRequest
	if err := xml.Unmarshal(body, &delReq); err != nil {
		return xmlError("MalformedXML", "malformed XML in request", http.StatusBadRequest), nil
	}

	result := deleteObjectsResult{}
	for _, obj := range delReq.Objects {
		// Delete metadata (ignore not found)
		if err := p.metaStore.DeleteObjectMeta(bucket, obj.Key, defaultAccountID); err != nil && !errors.Is(err, ErrObjectNotFound) {
			return nil, err
		}
		// Delete file (ignore not found)
		if err := p.fileStore.DeleteObject(defaultAccountID, bucket, obj.Key); err != nil && !os.IsNotExist(err) {
			return nil, err
		}
		result.Deleted = append(result.Deleted, deletedObjectXML{Key: obj.Key})
	}

	return xmlResponse(http.StatusOK, result)
}

// --- Task 12: Bucket Configuration ---

func (p *S3Provider) getBucketPolicy(_ context.Context, bucket string) (*plugin.Response, error) {
	policy, err := p.metaStore.GetBucketPolicy(bucket, defaultAccountID)
	if err != nil {
		if errors.Is(err, ErrObjectNotFound) {
			return xmlError("NoSuchBucketPolicy", "no policy for bucket", http.StatusNotFound), nil
		}
		return nil, err
	}
	return &plugin.Response{
		StatusCode:  http.StatusOK,
		ContentType: "application/json",
		Body:        []byte(policy),
	}, nil
}

func (p *S3Provider) putBucketPolicy(_ context.Context, bucket string, req *http.Request) (*plugin.Response, error) {
	body, err := io.ReadAll(req.Body)
	if err != nil {
		return nil, err
	}
	if err := p.metaStore.PutBucketPolicy(bucket, defaultAccountID, string(body)); err != nil {
		return nil, err
	}
	return &plugin.Response{StatusCode: http.StatusNoContent}, nil
}

func (p *S3Provider) deleteBucketPolicy(_ context.Context, bucket string) (*plugin.Response, error) {
	if err := p.metaStore.DeleteBucketPolicy(bucket, defaultAccountID); err != nil {
		return nil, err
	}
	return &plugin.Response{StatusCode: http.StatusNoContent}, nil
}

func (p *S3Provider) getBucketLocation(_ context.Context, _ string) (*plugin.Response, error) {
	result := locationConstraintXML{Region: "us-east-1"}
	return xmlResponse(http.StatusOK, result)
}

func (p *S3Provider) getBucketVersioning(_ context.Context, bucket string) (*plugin.Response, error) {
	status, err := p.metaStore.GetBucketVersioning(bucket, defaultAccountID)
	if err != nil {
		return nil, err
	}
	result := versioningConfigurationXML{Status: status}
	return xmlResponse(http.StatusOK, result)
}

func (p *S3Provider) putBucketVersioning(_ context.Context, bucket string, req *http.Request) (*plugin.Response, error) {
	body, err := io.ReadAll(req.Body)
	if err != nil {
		return nil, err
	}
	var cfg versioningConfigurationXML
	if err := xml.Unmarshal(body, &cfg); err != nil {
		return xmlError("MalformedXML", "malformed XML", http.StatusBadRequest), nil
	}
	if err := p.metaStore.PutBucketVersioning(bucket, defaultAccountID, cfg.Status); err != nil {
		return nil, err
	}
	return &plugin.Response{StatusCode: http.StatusOK}, nil
}

func (p *S3Provider) getBucketCors(_ context.Context, bucket string) (*plugin.Response, error) {
	corsXML, err := p.metaStore.GetBucketCors(bucket, defaultAccountID)
	if err != nil {
		if errors.Is(err, ErrObjectNotFound) {
			return xmlError("NoSuchCORSConfiguration", "no CORS configuration for bucket", http.StatusNotFound), nil
		}
		return nil, err
	}
	return &plugin.Response{
		StatusCode:  http.StatusOK,
		ContentType: "application/xml",
		Body:        []byte(corsXML),
	}, nil
}

func (p *S3Provider) putBucketCors(_ context.Context, bucket string, req *http.Request) (*plugin.Response, error) {
	body, err := io.ReadAll(req.Body)
	if err != nil {
		return nil, err
	}
	if err := p.metaStore.PutBucketCors(bucket, defaultAccountID, string(body)); err != nil {
		return nil, err
	}
	return &plugin.Response{StatusCode: http.StatusOK}, nil
}

func (p *S3Provider) deleteBucketCors(_ context.Context, bucket string) (*plugin.Response, error) {
	if err := p.metaStore.DeleteBucketCors(bucket, defaultAccountID); err != nil {
		return nil, err
	}
	return &plugin.Response{StatusCode: http.StatusNoContent}, nil
}

// --- Task 13: Tagging ---

func parseTags(body []byte) (map[string]string, error) {
	var t taggingXML
	if err := xml.Unmarshal(body, &t); err != nil {
		return nil, err
	}
	tags := make(map[string]string, len(t.TagSet))
	for _, tag := range t.TagSet {
		tags[tag.Key] = tag.Value
	}
	return tags, nil
}

func tagsToXML(tags map[string]string) ([]byte, error) {
	t := taggingXML{}
	for k, v := range tags {
		t.TagSet = append(t.TagSet, tagXML{Key: k, Value: v})
	}
	// Sort for deterministic output
	sort.Slice(t.TagSet, func(i, j int) bool { return t.TagSet[i].Key < t.TagSet[j].Key })
	return xml.Marshal(t)
}

func (p *S3Provider) getBucketTagging(_ context.Context, bucket string) (*plugin.Response, error) {
	tags, err := p.metaStore.GetBucketTags(bucket, defaultAccountID)
	if err != nil {
		return nil, err
	}
	body, err := tagsToXML(tags)
	if err != nil {
		return nil, err
	}
	return &plugin.Response{
		StatusCode:  http.StatusOK,
		ContentType: "application/xml",
		Body:        body,
	}, nil
}

func (p *S3Provider) putBucketTagging(_ context.Context, bucket string, req *http.Request) (*plugin.Response, error) {
	body, err := io.ReadAll(req.Body)
	if err != nil {
		return nil, err
	}
	tags, err := parseTags(body)
	if err != nil {
		return xmlError("MalformedXML", "malformed XML", http.StatusBadRequest), nil
	}
	if err := p.metaStore.PutBucketTags(bucket, defaultAccountID, tags); err != nil {
		return nil, err
	}
	return &plugin.Response{StatusCode: http.StatusNoContent}, nil
}

func (p *S3Provider) deleteBucketTagging(_ context.Context, bucket string) (*plugin.Response, error) {
	if err := p.metaStore.DeleteBucketTags(bucket, defaultAccountID); err != nil {
		return nil, err
	}
	return &plugin.Response{StatusCode: http.StatusNoContent}, nil
}

func (p *S3Provider) getObjectTagging(_ context.Context, bucket, key string) (*plugin.Response, error) {
	tags, err := p.metaStore.GetObjectTags(bucket, key, defaultAccountID)
	if err != nil {
		return nil, err
	}
	body, err := tagsToXML(tags)
	if err != nil {
		return nil, err
	}
	return &plugin.Response{
		StatusCode:  http.StatusOK,
		ContentType: "application/xml",
		Body:        body,
	}, nil
}

func (p *S3Provider) putObjectTagging(_ context.Context, bucket, key string, req *http.Request) (*plugin.Response, error) {
	body, err := io.ReadAll(req.Body)
	if err != nil {
		return nil, err
	}
	tags, err := parseTags(body)
	if err != nil {
		return xmlError("MalformedXML", "malformed XML", http.StatusBadRequest), nil
	}
	if err := p.metaStore.PutObjectTags(bucket, key, defaultAccountID, tags); err != nil {
		return nil, err
	}
	return &plugin.Response{StatusCode: http.StatusOK}, nil
}

func (p *S3Provider) deleteObjectTagging(_ context.Context, bucket, key string) (*plugin.Response, error) {
	if err := p.metaStore.DeleteObjectTags(bucket, key, defaultAccountID); err != nil {
		return nil, err
	}
	return &plugin.Response{StatusCode: http.StatusNoContent}, nil
}

// --- Task 14: ACL & Notifications ---

const defaultACLXML = `<AccessControlPolicy><Owner><ID>default-owner</ID><DisplayName>default-owner</DisplayName></Owner><AccessControlList><Grant><Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="CanonicalUser"><ID>default-owner</ID><DisplayName>default-owner</DisplayName></Grantee><Permission>FULL_CONTROL</Permission></Grant></AccessControlList></AccessControlPolicy>`

func (p *S3Provider) getBucketACL(_ context.Context, bucket string) (*plugin.Response, error) {
	aclXML, err := p.metaStore.GetBucketACL(bucket, defaultAccountID)
	if err != nil {
		if errors.Is(err, ErrObjectNotFound) {
			// Return canned full-control ACL
			return &plugin.Response{
				StatusCode:  http.StatusOK,
				ContentType: "application/xml",
				Body:        []byte(defaultACLXML),
			}, nil
		}
		return nil, err
	}
	return &plugin.Response{
		StatusCode:  http.StatusOK,
		ContentType: "application/xml",
		Body:        []byte(aclXML),
	}, nil
}

func (p *S3Provider) putBucketACL(_ context.Context, bucket string, req *http.Request) (*plugin.Response, error) {
	body, err := io.ReadAll(req.Body)
	if err != nil {
		return nil, err
	}
	if err := p.metaStore.PutBucketACL(bucket, defaultAccountID, string(body)); err != nil {
		return nil, err
	}
	return &plugin.Response{StatusCode: http.StatusOK}, nil
}

func (p *S3Provider) getBucketNotification(_ context.Context, bucket string) (*plugin.Response, error) {
	configXML, err := p.metaStore.GetBucketNotification(bucket, defaultAccountID)
	if err != nil {
		if errors.Is(err, ErrObjectNotFound) {
			return &plugin.Response{
				StatusCode:  http.StatusOK,
				ContentType: "application/xml",
				Body:        []byte(`<NotificationConfiguration/>`),
			}, nil
		}
		return nil, err
	}
	return &plugin.Response{
		StatusCode:  http.StatusOK,
		ContentType: "application/xml",
		Body:        []byte(configXML),
	}, nil
}

func (p *S3Provider) putBucketNotification(_ context.Context, bucket string, req *http.Request) (*plugin.Response, error) {
	body, err := io.ReadAll(req.Body)
	if err != nil {
		return nil, err
	}
	if err := p.metaStore.PutBucketNotification(bucket, defaultAccountID, string(body)); err != nil {
		return nil, err
	}
	return &plugin.Response{StatusCode: http.StatusOK}, nil
}
