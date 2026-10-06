// SPDX-License-Identifier: Apache-2.0
package s3

import (
	"bytes"
	"context"
	"encoding/xml"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/skyoo2003/devcloud/internal/plugin"
	"github.com/stretchr/testify/require"
)

func partCopyCall(t *testing.T, p *S3Provider, method, target string, body []byte, headers map[string]string) *plugin.Response {
	t.Helper()
	r := httptest.NewRequest(method, target, bytes.NewReader(body))
	for key, value := range headers {
		r.Header.Set(key, value)
	}
	response, err := p.HandleRequest(context.Background(), "", r)
	require.NoError(t, err)
	return response
}

func partCopySetup(t *testing.T) (*S3Provider, string, []byte) {
	t.Helper()
	p := newTestProvider(t)
	t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
	for _, bucket := range []string{"source", "destination"} {
		require.Equal(t, 200, partCopyCall(t, p, "PUT", "/"+bucket, nil, nil).StatusCode)
	}
	data := append([]byte("0123456789"), bytes.Repeat([]byte("x"), 6*1024*1024)...)
	require.Equal(t, 200, partCopyCall(t, p, "PUT", "/source/original", data, nil).StatusCode)
	r := partCopyCall(t, p, "POST", "/destination/result?uploads", nil, nil)
	var upload struct {
		UploadID string `xml:"UploadId"`
	}
	require.NoError(t, xml.Unmarshal(r.Body, &upload))
	require.NotEmpty(t, upload.UploadID)
	return p, upload.UploadID, data
}

func TestUploadPartCopyUsesSourceBytesAndCopyResult(t *testing.T) {
	p, id, original := partCopySetup(t)
	full := partCopyCall(t, p, "PUT", "/destination/result?uploadId="+id+"&partNumber=1", []byte("ignored request body"), map[string]string{"X-Amz-Copy-Source": "/source/original"})
	require.Equal(t, 200, full.StatusCode, string(full.Body))
	var copied struct {
		XMLName      xml.Name
		ETag         string
		LastModified string
	}
	require.NoError(t, xml.Unmarshal(full.Body, &copied))
	require.Equal(t, "CopyPartResult", copied.XMLName.Local)
	require.NotEmpty(t, copied.ETag)
	_, err := time.Parse(time.RFC3339Nano, copied.LastModified)
	require.NoError(t, err)
	ranged := partCopyCall(t, p, "PUT", "/destination/result?uploadId="+id+"&partNumber=2", nil, map[string]string{"X-Amz-Copy-Source": "source/original", "X-Amz-Copy-Source-Range": "bytes=2-5"})
	require.Equal(t, 200, ranged.StatusCode, string(ranged.Body))
	var second struct{ ETag string }
	require.NoError(t, xml.Unmarshal(ranged.Body, &second))
	parts, err := p.metaStore.ListUploadParts(id)
	require.NoError(t, err)
	require.Len(t, parts, 2)
	require.Equal(t, int64(len(original)), parts[0].Size)
	require.Equal(t, int64(4), parts[1].Size)
	complete := fmt.Sprintf(`<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>%s</ETag></Part><Part><PartNumber>2</PartNumber><ETag>%s</ETag></Part></CompleteMultipartUpload>`, copied.ETag, second.ETag)
	require.Equal(t, 200, partCopyCall(t, p, "POST", "/destination/result?uploadId="+id, []byte(complete), nil).StatusCode)
	result := partCopyCall(t, p, "GET", "/destination/result", nil, nil)
	require.Equal(t, append(append([]byte{}, original...), []byte("2345")...), result.Body)
	require.Equal(t, original, partCopyCall(t, p, "GET", "/source/original", nil, nil).Body)
}

func TestUploadPartCopyValidationPreservesExistingPart(t *testing.T) {
	p, id, _ := partCopySetup(t)
	base := "/destination/result?uploadId=" + id + "&partNumber=1"
	require.Equal(t, 200, partCopyCall(t, p, "PUT", base, []byte("keep"), nil).StatusCode)
	before, err := p.metaStore.ListUploadParts(id)
	require.NoError(t, err)
	meta, err := p.metaStore.GetObjectMeta("source", "original", defaultAccountID)
	require.NoError(t, err)
	for _, tc := range []struct {
		name, target string
		headers      map[string]string
		status       int
		code         string
	}{
		{"missing source", base, map[string]string{"X-Amz-Copy-Source": "/source/missing"}, 404, "NoSuchKey"},
		{"wrong bucket", "/other/result?uploadId=" + id + "&partNumber=1", nil, 404, "NoSuchUpload"},
		{"wrong key", "/destination/other?uploadId=" + id + "&partNumber=1", nil, 404, "NoSuchUpload"},
		{"invalid upload", "/destination/result?uploadId=invalid&partNumber=1", nil, 404, "NoSuchUpload"},
		{"part too large", "/destination/result?uploadId=" + id + "&partNumber=10001", nil, 400, "InvalidArgument"},
		{"empty part", "/destination/result?uploadId=" + id + "&partNumber=", nil, 400, "InvalidArgument"},
		{"missing part", "/destination/result?uploadId=" + id, nil, 400, "InvalidArgument"},
		{"malformed source", base, map[string]string{"X-Amz-Copy-Source": "/source/%GG"}, 400, "InvalidArgument"},
		{"source version", base, map[string]string{"X-Amz-Copy-Source": "/source/original?versionId=old"}, 501, "NotImplemented"},
		{"source encryption", base, map[string]string{"X-Amz-Copy-Source-Server-Side-Encryption-Customer-Algorithm": "AES256"}, 501, "NotImplemented"},
		{"wrong owner", base, map[string]string{"X-Amz-Expected-Bucket-Owner": "111111111111"}, 403, "AccessDenied"},
		{"source owner", base, map[string]string{"X-Amz-Source-Expected-Bucket-Owner": "111111111111"}, 403, "AccessDenied"},
		{"range reversed", base, map[string]string{"X-Amz-Copy-Source-Range": "bytes=5-2"}, 400, "InvalidArgument"},
		{"range overflow", base, map[string]string{"X-Amz-Copy-Source-Range": "bytes=0-999999999999999999999999"}, 400, "InvalidArgument"},
		{"range out of bounds", base, map[string]string{"X-Amz-Copy-Source-Range": "bytes=0-99999999"}, 400, "InvalidArgument"},
		{"range suffix", base, map[string]string{"X-Amz-Copy-Source-Range": "bytes=-4"}, 400, "InvalidArgument"},
		{"etag mismatch", base, map[string]string{"X-Amz-Copy-Source-If-Match": "\"wrong\""}, 412, "PreconditionFailed"},
		{"etag none match", base, map[string]string{"X-Amz-Copy-Source-If-None-Match": meta.ETag}, 412, "PreconditionFailed"},
		{"not modified", base, map[string]string{"X-Amz-Copy-Source-If-Modified-Since": meta.LastModified.Add(time.Hour).UTC().Format(http.TimeFormat)}, 412, "PreconditionFailed"},
		{"modified", base, map[string]string{"X-Amz-Copy-Source-If-Unmodified-Since": meta.LastModified.Add(-time.Hour).UTC().Format(http.TimeFormat)}, 412, "PreconditionFailed"},
		{"invalid date", base, map[string]string{"X-Amz-Copy-Source-If-Modified-Since": "invalid"}, 400, "InvalidArgument"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			headers := map[string]string{"X-Amz-Copy-Source": "/source/original"}
			for k, v := range tc.headers {
				headers[k] = v
			}
			r := partCopyCall(t, p, "PUT", tc.target, nil, headers)
			require.Equal(t, tc.status, r.StatusCode, string(r.Body))
			require.Contains(t, string(r.Body), "<Code>"+tc.code+"</Code>")
			after, err := p.metaStore.ListUploadParts(id)
			require.NoError(t, err)
			require.Equal(t, before, after)
			path, err := p.partPath(id, 1)
			require.NoError(t, err)
			data, err := os.ReadFile(path)
			require.NoError(t, err)
			require.Equal(t, []byte("keep"), data)
		})
	}
}

func TestUploadPartCopyConditionPrecedenceAndEncodedSource(t *testing.T) {
	p, id, _ := partCopySetup(t)
	key := "한글 + %2F ? #/name"
	require.Equal(t, 200, partCopyCall(t, p, "PUT", "/source/"+url.PathEscape(key), []byte("encoded"), nil).StatusCode)
	meta, err := p.metaStore.GetObjectMeta("source", key, defaultAccountID)
	require.NoError(t, err)
	target := "/destination/result?uploadId=" + id + "&partNumber=10000"
	headers := map[string]string{"X-Amz-Copy-Source": "/source/" + url.PathEscape(key), "X-Amz-Copy-Source-If-Match": meta.ETag, "X-Amz-Copy-Source-If-Unmodified-Since": meta.LastModified.Add(-time.Hour).UTC().Format(http.TimeFormat)}
	require.Equal(t, 200, partCopyCall(t, p, "PUT", target, nil, headers).StatusCode)
	path, err := p.partPath(id, 10000)
	require.NoError(t, err)
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, []byte("encoded"), data)
	headers["X-Amz-Copy-Source-If-None-Match"] = meta.ETag
	headers["X-Amz-Copy-Source-If-Modified-Since"] = meta.LastModified.Add(-time.Hour).UTC().Format(http.TimeFormat)
	require.Equal(t, 412, partCopyCall(t, p, "PUT", target, nil, headers).StatusCode)
}

func TestUploadPartCopyRejectsSmallRangeSource(t *testing.T) {
	p, id, _ := partCopySetup(t)
	require.Equal(t, 200, partCopyCall(t, p, "PUT", "/source/small", []byte("small"), nil).StatusCode)
	r := partCopyCall(t, p, "PUT", "/destination/result?uploadId="+id+"&partNumber=1", nil, map[string]string{"X-Amz-Copy-Source": "/source/small", "X-Amz-Copy-Source-Range": "bytes=0-1"})
	require.Equal(t, 400, r.StatusCode)
	require.Contains(t, string(r.Body), "<Code>InvalidRequest</Code>")
}

func TestUploadPartCopyAcceptsQuotedSourceETag(t *testing.T) {
	p, id, _ := partCopySetup(t)
	meta, err := p.metaStore.GetObjectMeta("source", "original", defaultAccountID)
	require.NoError(t, err)
	r := partCopyCall(t, p, "PUT", "/destination/result?uploadId="+id+"&partNumber=1", nil, map[string]string{"X-Amz-Copy-Source": "/source/original", "X-Amz-Copy-Source-If-Match": `"` + strings.Trim(meta.ETag, `"`) + `"`})
	require.Equal(t, 200, r.StatusCode, string(r.Body))
}

func TestUploadPartCopyRangesLargeSourceWithoutFullRead(t *testing.T) {
	p, id, _ := partCopySetup(t)
	path, err := p.fileStore.objectPath(defaultAccountID, "source", "original")
	require.NoError(t, err)
	require.NoError(t, os.Truncate(path, 5*1024*1024*1024+1))
	meta, err := p.metaStore.GetObjectMeta("source", "original", defaultAccountID)
	require.NoError(t, err)
	meta.Size = 5*1024*1024*1024 + 1
	require.NoError(t, p.metaStore.PutObjectMeta(*meta))
	target := "/destination/result?uploadId=" + id + "&partNumber=1"
	r := partCopyCall(t, p, "PUT", target, nil, map[string]string{"X-Amz-Copy-Source": "/source/original", "X-Amz-Copy-Source-Range": "bytes=2-5"})
	require.Equal(t, 200, r.StatusCode, string(r.Body))
	partPath, err := p.partPath(id, 1)
	require.NoError(t, err)
	data, err := os.ReadFile(partPath)
	require.NoError(t, err)
	require.Equal(t, []byte("2345"), data)
	r = partCopyCall(t, p, "PUT", target, nil, map[string]string{"X-Amz-Copy-Source": "/source/original"})
	require.Equal(t, 400, r.StatusCode)
	require.Contains(t, string(r.Body), "<Code>EntityTooLarge</Code>")
}

func TestUploadPartCopyMetadataFailurePreservesPartAndReopen(t *testing.T) {
	p, id, _ := partCopySetup(t)
	target := "/destination/result?uploadId=" + id + "&partNumber=1"
	require.Equal(t, 200, partCopyCall(t, p, "PUT", target, []byte("keep"), nil).StatusCode)
	_, err := p.metaStore.store.DB().Exec(`CREATE TRIGGER reject_copy BEFORE INSERT ON upload_parts BEGIN SELECT RAISE(ABORT,'test write failure'); END;`)
	require.NoError(t, err)
	r := httptest.NewRequest("PUT", target, nil)
	r.Header.Set("X-Amz-Copy-Source", "/source/original")
	response, err := p.HandleRequest(context.Background(), "", r)
	require.True(t, err != nil || response.StatusCode >= 500)
	path, err := p.partPath(id, 1)
	require.NoError(t, err)
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, []byte("keep"), data)
	_, err = p.metaStore.store.DB().Exec(`DROP TRIGGER reject_copy`)
	require.NoError(t, err)
	require.Equal(t, 200, partCopyCall(t, p, "PUT", target, nil, map[string]string{"X-Amz-Copy-Source": "/source/original", "X-Amz-Copy-Source-Range": "bytes=2-5"}).StatusCode)
	dir := p.fileStore.baseDir
	require.NoError(t, p.Shutdown(context.Background()))
	reopened := &S3Provider{}
	require.NoError(t, reopened.Init(plugin.PluginConfig{DataDir: dir, Options: map[string]any{"db_path": filepath.Join(dir, "meta.db")}}))
	t.Cleanup(func() { _ = reopened.Shutdown(context.Background()) })
	parts, err := reopened.metaStore.ListUploadParts(id)
	require.NoError(t, err)
	require.Len(t, parts, 1)
	require.Equal(t, int64(4), parts[0].Size)
	complete := fmt.Sprintf(`<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>%s</ETag></Part></CompleteMultipartUpload>`, parts[0].ETag)
	require.Equal(t, 200, partCopyCall(t, reopened, "POST", "/destination/result?uploadId="+id, []byte(complete), nil).StatusCode)
	require.Equal(t, "2345", strings.TrimSpace(string(partCopyCall(t, reopened, "GET", "/destination/result", nil, nil).Body)))
}
