// SPDX-License-Identifier: Apache-2.0
package s3

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"encoding/xml"
	"errors"
	"github.com/skyoo2003/devcloud/internal/plugin"
	"math/big"
	"net/http"
	"strings"
	"time"
)

type directoryCredentials struct {
	AccessKeyID                   string `xml:"AccessKeyId"`
	SecretAccessKey, SessionToken string
	Expiration                    time.Time
}

func directoryRandomText(length int) (string, error) {
	const alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	out := make([]byte, length)
	for i := range out {
		n, err := rand.Int(rand.Reader, big.NewInt(int64(len(alphabet))))
		if err != nil {
			return "", err
		}
		out[i] = alphabet[n.Int64()]
	}
	return string(out), nil
}
func (p *S3Provider) createDirectorySession(_ context.Context, bucket string, req *http.Request) (*plugin.Response, error) {
	info, err := p.metaStore.GetDirectoryBucket(bucket, defaultAccountID)
	if err != nil {
		return responseForS3Error(err)
	}
	if info == nil {
		return responseForS3Error(s3Error("InvalidRequest", 400, "sessions require a directory bucket"))
	}
	for name := range req.Header {
		if strings.HasPrefix(strings.ToLower(name), "x-amz-server-side-encryption") {
			return responseForS3Error(s3Error("NotImplemented", 501, "session encryption is not implemented"))
		}
	}
	mode := req.Header.Get("X-Amz-Create-Session-Mode")
	if mode == "" {
		mode = "ReadWrite"
	}
	if mode != "ReadWrite" && mode != "ReadOnly" {
		return responseForS3Error(s3Error("InvalidArgument", 400, "invalid session mode"))
	}
	key, err := directoryRandomText(16)
	if err != nil {
		return nil, err
	}
	secret, err := directoryRandomText(40)
	if err != nil {
		return nil, err
	}
	var token [32]byte
	if _, err = rand.Read(token[:]); err != nil {
		return nil, err
	}
	c := directoryCredentials{AccessKeyID: "ASIA" + key, SecretAccessKey: secret, SessionToken: base64.StdEncoding.EncodeToString(token[:]), Expiration: time.Now().UTC().Add(5 * time.Minute).Truncate(time.Second)}
	digest := sha256.Sum256([]byte(c.SessionToken))
	err = p.metaStore.PutDirectorySession(directorySession{AccessKeyID: c.AccessKeyID, TokenDigest: hex.EncodeToString(digest[:]), AccountID: info.AccountID, Bucket: info.Name, Incarnation: info.Incarnation, Mode: mode, ExpiresAt: c.Expiration})
	if err != nil {
		return nil, err
	}
	return xmlResponse(200, struct {
		XMLName     xml.Name `xml:"CreateSessionResult"`
		Credentials directoryCredentials
	}{Credentials: c})
}
func (p *S3Provider) authorizeDirectoryOperation(info *DirectoryBucketInfo, operation string, req *http.Request, now time.Time) error {
	if info == nil || operation == "" || operation == "CreateSession" || operation == "CopyObject" || operation == "UploadPartCopy" {
		return nil
	}
	token := req.Header.Get("X-Amz-S3session-Token")
	if token == "" {
		return s3Error("AccessDenied", 403, "directory session token required")
	}
	auth := req.Header.Get("Authorization")
	_, credential, ok := strings.Cut(auth, "Credential=")
	if !ok {
		return s3Error("InvalidToken", 403, "session credentials required")
	}
	key, _, ok := strings.Cut(credential, "/")
	if !ok || key == "" {
		return s3Error("InvalidToken", 403, "invalid session credentials")
	}
	session, err := p.metaStore.GetDirectorySession(key)
	if errors.Is(err, sql.ErrNoRows) {
		return s3Error("InvalidToken", 403, "unknown directory session")
	}
	if err != nil {
		return err
	}
	digest := sha256.Sum256([]byte(token))
	actual := hex.EncodeToString(digest[:])
	if subtle.ConstantTimeCompare([]byte(actual), []byte(session.TokenDigest)) != 1 || session.AccountID != info.AccountID || session.Bucket != info.Name || session.Incarnation != info.Incarnation {
		return s3Error("InvalidToken", 403, "session does not match bucket")
	}
	if !now.Before(session.ExpiresAt) {
		return s3Error("ExpiredToken", 403, "directory session expired")
	}
	switch operation {
	case "GetObject", "HeadObject", "ListObjectsV2", "GetObjectAttributes", "ListParts", "ListMultipartUploads":
		return nil
	}
	if session.Mode != "ReadWrite" {
		return s3Error("AccessDenied", 403, "ReadWrite session required")
	}
	return nil
}
func directoryOperation(bucket, key string, req *http.Request) string {
	if bucket == "" {
		return ""
	}
	q := req.URL.Query()
	if _, ok := q["renameObject"]; ok || q.Get("x-id") == "RenameObject" {
		return "RenameObject"
	}
	if _, ok := q["session"]; ok || q.Get("x-id") == "CreateSession" {
		return "CreateSession"
	}
	// Match dispatch precedence, including explicit copy-part routing before
	// bucket or object subresources. Invalid methods are rejected by dispatch.
	if req.Method == http.MethodPut && q.Get("x-id") == "UploadPartCopy" {
		return "UploadPartCopy"
	}
	if key == "" {
		if req.Method == http.MethodGet {
			if _, ok := q["uploads"]; ok {
				return "ListMultipartUploads"
			}
			for _, control := range []string{"policy", "location", "versioning", "cors", "tagging", "acl", "notification"} {
				if _, ok := q[control]; ok {
					return ""
				}
			}
			if _, unknown := unhandledSubresource(q); unknown {
				return ""
			}
			if q.Get("list-type") == "2" {
				return "ListObjectsV2"
			}
			return "ListObjects"
		}
		if req.Method == http.MethodPost {
			if _, ok := q["delete"]; ok {
				return "DeleteObjects"
			}
		}
		return ""
	}
	switch req.Method {
	case http.MethodGet:
		if _, ok := q["tagging"]; ok {
			return "GetObjectTagging"
		}
		if q.Get("uploadId") != "" {
			return "ListParts"
		}
		return "GetObject"
	case http.MethodHead:
		return "HeadObject"
	case http.MethodDelete:
		if _, ok := q["tagging"]; ok {
			return "DeleteObjectTagging"
		}
		if q.Get("uploadId") != "" {
			return "AbortMultipartUpload"
		}
		return "DeleteObject"
	case http.MethodPut:
		if _, ok := q["tagging"]; ok {
			return "PutObjectTagging"
		}
		_, hasPart := q["partNumber"]
		_, hasUpload := q["uploadId"]
		if _, hasCopy := req.Header[http.CanonicalHeaderKey("X-Amz-Copy-Source")]; hasCopy && (hasPart || hasUpload) {
			return "UploadPartCopy"
		}
		if hasPart {
			return "UploadPart"
		}
		if req.Header.Get("X-Amz-Copy-Source") != "" {
			return "CopyObject"
		}
		return "PutObject"
	case http.MethodPost:
		if _, ok := q["uploads"]; ok {
			return "CreateMultipartUpload"
		}
		if q.Get("uploadId") != "" {
			return "CompleteMultipartUpload"
		}
		if _, ok := q["delete"]; ok {
			return "DeleteObjects"
		}
	}
	return ""
}
