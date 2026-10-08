// SPDX-License-Identifier: Apache-2.0
package s3

import (
	"context"
	"crypto/md5"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/skyoo2003/devcloud/internal/plugin"
	"github.com/skyoo2003/devcloud/internal/shared"
)

// uploadPartCopy uses the existing local object and multipart stores. Validation
// finishes before replacing a part; no source object is modified.
func (p *S3Provider) uploadPartCopy(_ context.Context, bucket, key, uploadID, number string, req *http.Request) (*plugin.Response, error) {
	if key == "" {
		return xmlError("InvalidArgument", "destination key is required", http.StatusBadRequest), nil
	}
	part, err := strconv.Atoi(number)
	if err != nil || part < 1 || part > 10000 {
		return xmlError("InvalidArgument", "part number must be between 1 and 10000", http.StatusBadRequest), nil
	}
	if !shared.ValidateUploadID(uploadID) {
		return xmlError("NoSuchUpload", "upload not found", http.StatusNotFound), nil
	}
	upload, err := p.metaStore.GetMultipartUpload(uploadID)
	if errors.Is(err, ErrUploadNotFound) || err == nil && (upload.Bucket != bucket || upload.Key != key || upload.AccountID != defaultAccountID) {
		return xmlError("NoSuchUpload", "upload not found", http.StatusNotFound), nil
	}
	if err != nil {
		return nil, err
	}
	for _, header := range []string{"X-Amz-Expected-Bucket-Owner", "X-Amz-Source-Expected-Bucket-Owner"} {
		if owner := req.Header.Get(header); owner != "" && owner != defaultAccountID {
			return xmlError("AccessDenied", "bucket owner does not match", http.StatusForbidden), nil
		}
	}
	for header := range req.Header {
		lower := strings.ToLower(header)
		if strings.HasPrefix(lower, "x-amz-server-side-encryption") || strings.HasPrefix(lower, "x-amz-copy-source-server-side-encryption") {
			return xmlError("NotImplemented", "encrypted copies are not supported", http.StatusNotImplemented), nil
		}
	}
	source := req.Header.Get("X-Amz-Copy-Source")
	// Split the raw version suffix before decoding so an encoded '?' is a key.
	if strings.Contains(source, "?") {
		return xmlError("NotImplemented", "source versions are not supported", http.StatusNotImplemented), nil
	}
	source, err = url.PathUnescape(source)
	if err != nil || !utf8.ValidString(source) {
		return xmlError("InvalidArgument", "invalid copy source", http.StatusBadRequest), nil
	}
	source = strings.TrimPrefix(source, "/")
	sourceBucket, sourceKey, ok := strings.Cut(source, "/")
	if !ok || sourceBucket == "" || sourceKey == "" {
		return xmlError("InvalidArgument", "copy source requires bucket and key", http.StatusBadRequest), nil
	}
	if strings.HasPrefix(sourceBucket, "arn:") {
		return xmlError("NotImplemented", "access point copies are not supported", http.StatusNotImplemented), nil
	}
	meta, err := p.metaStore.GetObjectMeta(sourceBucket, sourceKey, defaultAccountID)
	if errors.Is(err, ErrObjectNotFound) {
		return xmlError("NoSuchKey", "source object not found", http.StatusNotFound), nil
	}
	if err != nil {
		return nil, err
	}
	if response := copyPartConditions(req.Header, meta); response != nil {
		return response, nil
	}
	path, err := p.fileStore.objectPath(defaultAccountID, sourceBucket, sourceKey)
	if err != nil {
		return xmlError("InvalidArgument", "invalid copy source path", http.StatusBadRequest), nil
	}
	file, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return xmlError("NoSuchKey", "source object not found", http.StatusNotFound), nil
	}
	if err != nil {
		return nil, err
	}
	defer func() { _ = file.Close() }()
	info, err := file.Stat()
	if err != nil {
		return nil, err
	}
	start, length := int64(0), info.Size()
	if value := req.Header.Get("X-Amz-Copy-Source-Range"); value != "" {
		if info.Size() <= 5*1024*1024 {
			return xmlError("InvalidRequest", "range source must be greater than 5 MiB", http.StatusBadRequest), nil
		}
		first, last, valid := copyPartRange(value, info.Size())
		if !valid {
			return xmlError("InvalidArgument", "invalid copy source range", http.StatusBadRequest), nil
		}
		start, length = first, last-first+1
	}
	if length > 5*1024*1024*1024 {
		return xmlError("EntityTooLarge", "copy part exceeds 5 GiB", http.StatusBadRequest), nil
	}
	data, err := io.ReadAll(io.NewSectionReader(file, start, length))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) != length {
		return nil, io.ErrUnexpectedEOF
	}
	etag, err := p.storeMultipartPart(uploadID, part, data)
	if errors.Is(err, ErrUploadNotFound) {
		return xmlError("NoSuchUpload", "upload not found", http.StatusNotFound), nil
	}
	if err != nil {
		return nil, err
	}
	return xmlResponse(http.StatusOK, struct {
		XMLName      xml.Name `xml:"CopyPartResult"`
		ETag         string   `xml:"ETag"`
		LastModified string   `xml:"LastModified"`
	}{ETag: etag, LastModified: time.Now().UTC().Format(time.RFC3339Nano)})
}

func copyPartConditions(headers http.Header, meta *ObjectMeta) *plugin.Response {
	dates := map[string]time.Time{}
	for _, header := range []string{"X-Amz-Copy-Source-If-Modified-Since", "X-Amz-Copy-Source-If-Unmodified-Since"} {
		if value := headers.Get(header); value != "" {
			date, err := http.ParseTime(value)
			if err != nil {
				return xmlError("InvalidArgument", "invalid condition date", http.StatusBadRequest)
			}
			dates[header] = date
		}
	}
	normalizeETag := func(value string) string {
		if len(value) >= 2 && value[0] == '"' && value[len(value)-1] == '"' {
			return value[1 : len(value)-1]
		}
		return value
	}
	failed := false
	if value := headers.Get("X-Amz-Copy-Source-If-Match"); value != "" {
		failed = normalizeETag(value) != normalizeETag(meta.ETag)
	} else if date, ok := dates["X-Amz-Copy-Source-If-Unmodified-Since"]; ok {
		failed = meta.LastModified.After(date)
	}
	if value := headers.Get("X-Amz-Copy-Source-If-None-Match"); value != "" {
		failed = failed || normalizeETag(value) == normalizeETag(meta.ETag)
	} else if date, ok := dates["X-Amz-Copy-Source-If-Modified-Since"]; ok {
		failed = failed || !meta.LastModified.After(date)
	}
	if failed {
		return xmlError("PreconditionFailed", "copy source condition failed", http.StatusPreconditionFailed)
	}
	return nil
}

func copyPartRange(value string, size int64) (int64, int64, bool) {
	if !strings.HasPrefix(value, "bytes=") {
		return 0, 0, false
	}
	first, last, ok := strings.Cut(strings.TrimPrefix(value, "bytes="), "-")
	if !ok || first == "" || last == "" {
		return 0, 0, false
	}
	for _, s := range []string{first, last} {
		for _, c := range s {
			if c < '0' || c > '9' {
				return 0, 0, false
			}
		}
	}
	start, e1 := strconv.ParseInt(first, 10, 64)
	end, e2 := strconv.ParseInt(last, 10, 64)
	return start, end, e1 == nil && e2 == nil && start <= end && end < size
}

// Serialize plain/copied part writes together and restore the prior bytes if
// SQLite rejects the metadata change. This is process-level rollback, not a
// crash-atomic transaction spanning the filesystem and SQLite.
func (p *S3Provider) storeMultipartPart(uploadID string, number int, data []byte) (string, error) {
	p.multipartMu.Lock()
	defer p.multipartMu.Unlock()
	if _, err := p.metaStore.GetMultipartUpload(uploadID); err != nil {
		return "", err
	}
	previous, existed, err := p.fileStore.ReadMultipartPart(uploadID, number)
	if err != nil {
		return "", err
	}
	if err = p.fileStore.WriteMultipartPartAtomic(uploadID, number, data); err != nil {
		return "", err
	}
	sum := md5.Sum(data)
	etag := fmt.Sprintf("\"%x\"", sum)
	if err = p.metaStore.PutUploadPart(UploadPartInfo{UploadID: uploadID, PartNumber: number, ETag: etag, Size: int64(len(data))}); err != nil {
		var restore error
		if existed {
			restore = p.fileStore.WriteMultipartPartAtomic(uploadID, number, previous)
		} else {
			restore = p.fileStore.DeleteMultipartPart(uploadID, number)
		}
		return "", errors.Join(err, restore)
	}
	return etag, nil
}
