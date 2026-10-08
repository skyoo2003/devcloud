// SPDX-License-Identifier: Apache-2.0
package s3

import (
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"path/filepath"
	"strings"
	"unicode/utf8"
)

type renameRequest struct {
	AccountID, Bucket, SourceKey, DestinationKey, ClientToken, Fingerprint string
	HasClientToken                                                         bool
	Conditions                                                             renameConditions
}

func parseRenameRequest(bucket, key string, req *http.Request) (renameRequest, error) {
	invalid := func(message string) (renameRequest, error) {
		return renameRequest{}, s3Error("InvalidArgument", 400, message)
	}
	q := req.URL.Query()
	for name := range q {
		if name != "renameObject" && name != "x-id" {
			return renameRequest{}, s3Error("InvalidRequest", 400, "conflicting rename subresource")
		}
	}
	if xid := q.Get("x-id"); xid != "" && xid != "RenameObject" {
		return renameRequest{}, s3Error("InvalidRequest", 400, "conflicting operation")
	}
	for name := range req.Header {
		lower := strings.ToLower(name)
		if lower == "x-amz-copy-source" {
			return renameRequest{}, s3Error("InvalidRequest", 400, "copy source is not rename source")
		}
		if strings.HasPrefix(lower, "x-amz-server-side-encryption") {
			return renameRequest{}, s3Error("NotImplemented", 501, "rename encryption is not implemented")
		}
	}
	raw := req.Header.Get("X-Amz-Rename-Source")
	if raw == "" {
		return invalid("rename source required")
	}
	if strings.Contains(raw, "?versionId=") {
		return renameRequest{}, s3Error("NotImplemented", 501, "version selection is not implemented")
	}
	source, err := url.PathUnescape(raw)
	if err != nil {
		return invalid("invalid source encoding")
	}
	if strings.HasPrefix(source, "arn:") {
		return renameRequest{}, s3Error("NotImplemented", 501, "access point source is not implemented")
	}
	if strings.HasPrefix(source, "/") {
		source = strings.TrimPrefix(source, "/")
	} else if strings.HasPrefix(source, bucket+"/") {
		source = strings.TrimPrefix(source, bucket+"/")
	} else if prefix, _, ok := strings.Cut(source, "/"); ok && strings.HasSuffix(prefix, "--x-s3") {
		return renameRequest{}, s3Error("InvalidRequest", 400, "source must be in this bucket")
	}
	for _, v := range []string{key, source} {
		if len(v) < 1 || len(v) > 1024 || !utf8.ValidString(v) || strings.ContainsRune(v, 0) {
			return invalid("invalid object key")
		}
		if strings.HasSuffix(v, "/") {
			return renameRequest{}, s3Error("InvalidRequest", 400, "slash suffix cannot be renamed")
		}
		if !filepath.IsLocal(v) || filepath.Clean(v) != v {
			return invalid("object key cannot be represented safely")
		}
		for _, part := range strings.Split(v, "/") {
			if part == "" || part == "." || part == ".." {
				return invalid("invalid object key segment")
			}
		}
	}
	token, hasToken := req.Header[http.CanonicalHeaderKey("X-Amz-Client-Token")]
	value := ""
	if hasToken {
		if len(token) != 1 {
			return invalid("invalid client token")
		}
		value = token[0]
		if len(value) < 1 || len(value) > 64 {
			return invalid("invalid client token length")
		}
		for _, b := range []byte(value) {
			if b < 33 || b > 126 {
				return invalid("invalid client token characters")
			}
		}
	}
	if req.Body != nil {
		var first [1]byte
		n, err := io.ReadFull(req.Body, first[:])
		if n > 0 {
			return renameRequest{}, s3Error("InvalidRequest", 400, "rename has no body")
		}
		if err != nil && err != io.EOF {
			return renameRequest{}, err
		}
	}
	conditions, err := parseRenameConditions(req.Header)
	if err != nil {
		return renameRequest{}, err
	}
	fingerprint, err := json.Marshal(struct {
		Source, Destination string
		Conditions          renameConditions
	}{source, key, conditions})
	if err != nil {
		return renameRequest{}, err
	}
	return renameRequest{AccountID: defaultAccountID, Bucket: bucket, SourceKey: source, DestinationKey: key, ClientToken: value, HasClientToken: hasToken, Conditions: conditions, Fingerprint: string(fingerprint)}, nil
}
