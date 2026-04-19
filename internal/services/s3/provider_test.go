// SPDX-License-Identifier: Apache-2.0

package s3

import (
	"bytes"
	"context"
	"encoding/xml"
	"fmt"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/skyoo2003/devcloud/internal/plugin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestProvider(t *testing.T) *S3Provider {
	t.Helper()
	dir := t.TempDir()
	p := &S3Provider{}
	err := p.Init(plugin.PluginConfig{
		DataDir: dir,
		Options: map[string]any{
			"db_path": filepath.Join(dir, "meta.db"),
		},
	})
	require.NoError(t, err)
	return p
}

func TestS3Provider_CreateBucket(t *testing.T) {
	p := newTestProvider(t)
	defer func() { _ = p.Shutdown(context.Background()) }()
	req := httptest.NewRequest("PUT", "/test-bucket", nil)
	resp, err := p.HandleRequest(context.Background(), "", req)
	require.NoError(t, err)
	assert.Equal(t, 200, resp.StatusCode)
}

func TestS3Provider_ListBuckets(t *testing.T) {
	p := newTestProvider(t)
	defer func() { _ = p.Shutdown(context.Background()) }()
	req1 := httptest.NewRequest("PUT", "/bucket-a", nil)
	_, err := p.HandleRequest(context.Background(), "", req1)
	require.NoError(t, err)
	req2 := httptest.NewRequest("PUT", "/bucket-b", nil)
	_, err = p.HandleRequest(context.Background(), "", req2)
	require.NoError(t, err)
	req := httptest.NewRequest("GET", "/", nil)
	resp, err := p.HandleRequest(context.Background(), "", req)
	require.NoError(t, err)
	assert.Equal(t, 200, resp.StatusCode)
	assert.Contains(t, string(resp.Body), "bucket-a")
	assert.Contains(t, string(resp.Body), "bucket-b")
}

func TestS3Provider_PutAndGetObject(t *testing.T) {
	p := newTestProvider(t)
	defer func() { _ = p.Shutdown(context.Background()) }()
	reqCreate := httptest.NewRequest("PUT", "/test-bucket", nil)
	_, err := p.HandleRequest(context.Background(), "", reqCreate)
	require.NoError(t, err)
	body := strings.NewReader("hello world")
	reqPut := httptest.NewRequest("PUT", "/test-bucket/hello.txt", body)
	reqPut.Header.Set("Content-Type", "text/plain")
	resp, err := p.HandleRequest(context.Background(), "", reqPut)
	require.NoError(t, err)
	assert.Equal(t, 200, resp.StatusCode)
	reqGet := httptest.NewRequest("GET", "/test-bucket/hello.txt", nil)
	resp, err = p.HandleRequest(context.Background(), "", reqGet)
	require.NoError(t, err)
	assert.Equal(t, 200, resp.StatusCode)
	assert.Equal(t, "hello world", string(resp.Body))
}

func TestS3Provider_DeleteObject(t *testing.T) {
	p := newTestProvider(t)
	defer func() { _ = p.Shutdown(context.Background()) }()
	reqCreate := httptest.NewRequest("PUT", "/test-bucket", nil)
	_, err := p.HandleRequest(context.Background(), "", reqCreate)
	require.NoError(t, err)
	reqPut := httptest.NewRequest("PUT", "/test-bucket/hello.txt", strings.NewReader("data"))
	_, err = p.HandleRequest(context.Background(), "", reqPut)
	require.NoError(t, err)
	reqDel := httptest.NewRequest("DELETE", "/test-bucket/hello.txt", nil)
	resp, err := p.HandleRequest(context.Background(), "", reqDel)
	require.NoError(t, err)
	assert.Equal(t, 204, resp.StatusCode)
	reqGet := httptest.NewRequest("GET", "/test-bucket/hello.txt", nil)
	resp, err = p.HandleRequest(context.Background(), "", reqGet)
	require.NoError(t, err)
	assert.Equal(t, 404, resp.StatusCode)
}

// --- Task 9: ListObjectsV2 ---

func TestS3Provider_ListObjectsV2(t *testing.T) {
	p := newTestProvider(t)
	defer func() { _ = p.Shutdown(context.Background()) }()

	// Create bucket and put 5 objects
	_, err := p.HandleRequest(context.Background(), "", httptest.NewRequest("PUT", "/v2-bucket", nil))
	require.NoError(t, err)
	for i := 0; i < 5; i++ {
		req := httptest.NewRequest("PUT", fmt.Sprintf("/v2-bucket/key%d", i), strings.NewReader("x"))
		_, err = p.HandleRequest(context.Background(), "", req)
		require.NoError(t, err)
	}

	// First page: max-keys=2
	req := httptest.NewRequest("GET", "/v2-bucket?list-type=2&max-keys=2", nil)
	resp, err := p.HandleRequest(context.Background(), "", req)
	require.NoError(t, err)
	assert.Equal(t, 200, resp.StatusCode)

	var result1 listBucketV2Result
	require.NoError(t, xml.Unmarshal(resp.Body, &result1))
	assert.Equal(t, 2, result1.KeyCount)
	assert.True(t, result1.IsTruncated)
	assert.NotEmpty(t, result1.NextContinuationToken)

	// Second page using continuation token
	req2 := httptest.NewRequest("GET", fmt.Sprintf("/v2-bucket?list-type=2&max-keys=2&continuation-token=%s", result1.NextContinuationToken), nil)
	resp2, err := p.HandleRequest(context.Background(), "", req2)
	require.NoError(t, err)
	assert.Equal(t, 200, resp2.StatusCode)

	var result2 listBucketV2Result
	require.NoError(t, xml.Unmarshal(resp2.Body, &result2))
	assert.GreaterOrEqual(t, result2.KeyCount, 1)
}

func TestS3Provider_ListObjectsV2_WithPrefix(t *testing.T) {
	p := newTestProvider(t)
	defer func() { _ = p.Shutdown(context.Background()) }()

	_, err := p.HandleRequest(context.Background(), "", httptest.NewRequest("PUT", "/prefix-bucket", nil))
	require.NoError(t, err)
	for _, k := range []string{"foo/a", "foo/b", "bar/c"} {
		req := httptest.NewRequest("PUT", "/prefix-bucket/"+k, strings.NewReader("data"))
		_, err = p.HandleRequest(context.Background(), "", req)
		require.NoError(t, err)
	}

	req := httptest.NewRequest("GET", "/prefix-bucket?list-type=2&prefix=foo/", nil)
	resp, err := p.HandleRequest(context.Background(), "", req)
	require.NoError(t, err)
	assert.Equal(t, 200, resp.StatusCode)

	var result listBucketV2Result
	require.NoError(t, xml.Unmarshal(resp.Body, &result))
	assert.Equal(t, 2, result.KeyCount)
}

// --- Task 10: Multipart Upload ---

func TestS3Provider_MultipartUpload(t *testing.T) {
	p := newTestProvider(t)
	defer func() { _ = p.Shutdown(context.Background()) }()

	_, err := p.HandleRequest(context.Background(), "", httptest.NewRequest("PUT", "/mp-bucket", nil))
	require.NoError(t, err)

	// Create multipart upload
	reqCreate := httptest.NewRequest("POST", "/mp-bucket/big.bin?uploads", nil)
	resp, err := p.HandleRequest(context.Background(), "", reqCreate)
	require.NoError(t, err)
	assert.Equal(t, 200, resp.StatusCode)

	var initResult initiateMultipartUploadResult
	require.NoError(t, xml.Unmarshal(resp.Body, &initResult))
	uploadID := initResult.UploadID
	assert.NotEmpty(t, uploadID)

	// Upload parts
	part1Data := bytes.Repeat([]byte("a"), 5*1024*1024)
	req1 := httptest.NewRequest("PUT", fmt.Sprintf("/mp-bucket/big.bin?partNumber=1&uploadId=%s", uploadID), bytes.NewReader(part1Data))
	resp1, err := p.HandleRequest(context.Background(), "", req1)
	require.NoError(t, err)
	assert.Equal(t, 200, resp1.StatusCode)
	etag1 := resp1.Headers["ETag"]

	part2Data := bytes.Repeat([]byte("b"), 1024)
	req2 := httptest.NewRequest("PUT", fmt.Sprintf("/mp-bucket/big.bin?partNumber=2&uploadId=%s", uploadID), bytes.NewReader(part2Data))
	resp2, err := p.HandleRequest(context.Background(), "", req2)
	require.NoError(t, err)
	assert.Equal(t, 200, resp2.StatusCode)
	etag2 := resp2.Headers["ETag"]

	// Complete multipart upload
	completeBody := fmt.Sprintf(`<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>%s</ETag></Part><Part><PartNumber>2</PartNumber><ETag>%s</ETag></Part></CompleteMultipartUpload>`, etag1, etag2)
	reqComplete := httptest.NewRequest("POST", fmt.Sprintf("/mp-bucket/big.bin?uploadId=%s", uploadID), strings.NewReader(completeBody))
	respComplete, err := p.HandleRequest(context.Background(), "", reqComplete)
	require.NoError(t, err)
	assert.Equal(t, 200, respComplete.StatusCode)

	// Get object and verify size
	reqGet := httptest.NewRequest("GET", "/mp-bucket/big.bin", nil)
	respGet, err := p.HandleRequest(context.Background(), "", reqGet)
	require.NoError(t, err)
	assert.Equal(t, 200, respGet.StatusCode)
	assert.Equal(t, int64(5*1024*1024+1024), int64(len(respGet.Body)))
}

func TestS3Provider_AbortMultipartUpload(t *testing.T) {
	p := newTestProvider(t)
	defer func() { _ = p.Shutdown(context.Background()) }()

	_, err := p.HandleRequest(context.Background(), "", httptest.NewRequest("PUT", "/abort-bucket", nil))
	require.NoError(t, err)

	// Create upload
	reqCreate := httptest.NewRequest("POST", "/abort-bucket/obj?uploads", nil)
	resp, err := p.HandleRequest(context.Background(), "", reqCreate)
	require.NoError(t, err)
	var initResult initiateMultipartUploadResult
	require.NoError(t, xml.Unmarshal(resp.Body, &initResult))
	uploadID := initResult.UploadID

	// Abort
	reqAbort := httptest.NewRequest("DELETE", fmt.Sprintf("/abort-bucket/obj?uploadId=%s", uploadID), nil)
	respAbort, err := p.HandleRequest(context.Background(), "", reqAbort)
	require.NoError(t, err)
	assert.Equal(t, 204, respAbort.StatusCode)
}

func TestS3Provider_ListMultipartUploads(t *testing.T) {
	p := newTestProvider(t)
	defer func() { _ = p.Shutdown(context.Background()) }()

	_, err := p.HandleRequest(context.Background(), "", httptest.NewRequest("PUT", "/list-mp-bucket", nil))
	require.NoError(t, err)

	// Create two uploads
	for _, key := range []string{"obj1", "obj2"} {
		_, err = p.HandleRequest(context.Background(), "", httptest.NewRequest("POST", "/list-mp-bucket/"+key+"?uploads", nil))
		require.NoError(t, err)
	}

	reqList := httptest.NewRequest("GET", "/list-mp-bucket?uploads", nil)
	resp, err := p.HandleRequest(context.Background(), "", reqList)
	require.NoError(t, err)
	assert.Equal(t, 200, resp.StatusCode)
	assert.Contains(t, string(resp.Body), "obj1")
	assert.Contains(t, string(resp.Body), "obj2")
}

// --- Task 11: DeleteObjects ---

func TestS3Provider_DeleteObjects(t *testing.T) {
	p := newTestProvider(t)
	defer func() { _ = p.Shutdown(context.Background()) }()

	_, err := p.HandleRequest(context.Background(), "", httptest.NewRequest("PUT", "/del-bucket", nil))
	require.NoError(t, err)
	for _, k := range []string{"a", "b", "c"} {
		_, err = p.HandleRequest(context.Background(), "", httptest.NewRequest("PUT", "/del-bucket/"+k, strings.NewReader("data")))
		require.NoError(t, err)
	}

	delBody := `<Delete><Object><Key>a</Key></Object><Object><Key>b</Key></Object></Delete>`
	req := httptest.NewRequest("POST", "/del-bucket?delete", strings.NewReader(delBody))
	resp, err := p.HandleRequest(context.Background(), "", req)
	require.NoError(t, err)
	assert.Equal(t, 200, resp.StatusCode)

	var result deleteObjectsResult
	require.NoError(t, xml.Unmarshal(resp.Body, &result))
	assert.Len(t, result.Deleted, 2)

	// Verify a and b are gone, c remains
	respA, _ := p.HandleRequest(context.Background(), "", httptest.NewRequest("GET", "/del-bucket/a", nil))
	assert.Equal(t, 404, respA.StatusCode)
	respC, _ := p.HandleRequest(context.Background(), "", httptest.NewRequest("GET", "/del-bucket/c", nil))
	assert.Equal(t, 200, respC.StatusCode)
}

// --- Task 12: Bucket Policy ---

func TestS3Provider_BucketPolicy(t *testing.T) {
	p := newTestProvider(t)
	defer func() { _ = p.Shutdown(context.Background()) }()

	_, err := p.HandleRequest(context.Background(), "", httptest.NewRequest("PUT", "/pol-bucket", nil))
	require.NoError(t, err)

	// No policy yet — should 404
	resp, err := p.HandleRequest(context.Background(), "", httptest.NewRequest("GET", "/pol-bucket?policy", nil))
	require.NoError(t, err)
	assert.Equal(t, 404, resp.StatusCode)

	// Put policy
	policy := `{"Version":"2012-10-17","Statement":[]}`
	reqPut := httptest.NewRequest("PUT", "/pol-bucket?policy", strings.NewReader(policy))
	resp, err = p.HandleRequest(context.Background(), "", reqPut)
	require.NoError(t, err)
	assert.Equal(t, 204, resp.StatusCode)

	// Get policy
	resp, err = p.HandleRequest(context.Background(), "", httptest.NewRequest("GET", "/pol-bucket?policy", nil))
	require.NoError(t, err)
	assert.Equal(t, 200, resp.StatusCode)
	assert.Equal(t, policy, string(resp.Body))

	// Delete policy
	resp, err = p.HandleRequest(context.Background(), "", httptest.NewRequest("DELETE", "/pol-bucket?policy", nil))
	require.NoError(t, err)
	assert.Equal(t, 204, resp.StatusCode)
}

func TestS3Provider_BucketLocation(t *testing.T) {
	p := newTestProvider(t)
	defer func() { _ = p.Shutdown(context.Background()) }()

	_, err := p.HandleRequest(context.Background(), "", httptest.NewRequest("PUT", "/loc-bucket", nil))
	require.NoError(t, err)
	resp, err := p.HandleRequest(context.Background(), "", httptest.NewRequest("GET", "/loc-bucket?location", nil))
	require.NoError(t, err)
	assert.Equal(t, 200, resp.StatusCode)
	assert.Contains(t, string(resp.Body), "us-east-1")
}

func TestS3Provider_BucketVersioning(t *testing.T) {
	p := newTestProvider(t)
	defer func() { _ = p.Shutdown(context.Background()) }()

	_, err := p.HandleRequest(context.Background(), "", httptest.NewRequest("PUT", "/ver-bucket", nil))
	require.NoError(t, err)

	// Default: Suspended
	resp, err := p.HandleRequest(context.Background(), "", httptest.NewRequest("GET", "/ver-bucket?versioning", nil))
	require.NoError(t, err)
	assert.Equal(t, 200, resp.StatusCode)
	assert.Contains(t, string(resp.Body), "Suspended")

	// Enable versioning
	reqPut := httptest.NewRequest("PUT", "/ver-bucket?versioning", strings.NewReader(`<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>`))
	resp, err = p.HandleRequest(context.Background(), "", reqPut)
	require.NoError(t, err)
	assert.Equal(t, 200, resp.StatusCode)

	// Verify
	resp, err = p.HandleRequest(context.Background(), "", httptest.NewRequest("GET", "/ver-bucket?versioning", nil))
	require.NoError(t, err)
	assert.Contains(t, string(resp.Body), "Enabled")
}

func TestS3Provider_BucketCors(t *testing.T) {
	p := newTestProvider(t)
	defer func() { _ = p.Shutdown(context.Background()) }()

	_, err := p.HandleRequest(context.Background(), "", httptest.NewRequest("PUT", "/cors-bucket", nil))
	require.NoError(t, err)

	corsBody := `<CORSConfiguration><CORSRule><AllowedMethod>GET</AllowedMethod><AllowedOrigin>*</AllowedOrigin></CORSRule></CORSConfiguration>`
	reqPut := httptest.NewRequest("PUT", "/cors-bucket?cors", strings.NewReader(corsBody))
	resp, err := p.HandleRequest(context.Background(), "", reqPut)
	require.NoError(t, err)
	assert.Equal(t, 200, resp.StatusCode)

	resp, err = p.HandleRequest(context.Background(), "", httptest.NewRequest("GET", "/cors-bucket?cors", nil))
	require.NoError(t, err)
	assert.Equal(t, 200, resp.StatusCode)
	assert.Equal(t, corsBody, string(resp.Body))

	resp, err = p.HandleRequest(context.Background(), "", httptest.NewRequest("DELETE", "/cors-bucket?cors", nil))
	require.NoError(t, err)
	assert.Equal(t, 204, resp.StatusCode)
}

// --- Task 13: Tagging ---

func TestS3Provider_BucketTagging(t *testing.T) {
	p := newTestProvider(t)
	defer func() { _ = p.Shutdown(context.Background()) }()

	_, err := p.HandleRequest(context.Background(), "", httptest.NewRequest("PUT", "/tag-bucket", nil))
	require.NoError(t, err)

	tagBody := `<Tagging><TagSet><Tag><Key>env</Key><Value>test</Value></Tag></TagSet></Tagging>`
	reqPut := httptest.NewRequest("PUT", "/tag-bucket?tagging", strings.NewReader(tagBody))
	resp, err := p.HandleRequest(context.Background(), "", reqPut)
	require.NoError(t, err)
	assert.Equal(t, 204, resp.StatusCode)

	resp, err = p.HandleRequest(context.Background(), "", httptest.NewRequest("GET", "/tag-bucket?tagging", nil))
	require.NoError(t, err)
	assert.Equal(t, 200, resp.StatusCode)
	assert.Contains(t, string(resp.Body), "env")
	assert.Contains(t, string(resp.Body), "test")

	resp, err = p.HandleRequest(context.Background(), "", httptest.NewRequest("DELETE", "/tag-bucket?tagging", nil))
	require.NoError(t, err)
	assert.Equal(t, 204, resp.StatusCode)

	// After delete, tags should be empty
	resp, err = p.HandleRequest(context.Background(), "", httptest.NewRequest("GET", "/tag-bucket?tagging", nil))
	require.NoError(t, err)
	assert.Equal(t, 200, resp.StatusCode)
	assert.NotContains(t, string(resp.Body), "env")
}

func TestS3Provider_ObjectTagging(t *testing.T) {
	p := newTestProvider(t)
	defer func() { _ = p.Shutdown(context.Background()) }()

	_, err := p.HandleRequest(context.Background(), "", httptest.NewRequest("PUT", "/otag-bucket", nil))
	require.NoError(t, err)
	_, err = p.HandleRequest(context.Background(), "", httptest.NewRequest("PUT", "/otag-bucket/f.txt", strings.NewReader("data")))
	require.NoError(t, err)

	tagBody := `<Tagging><TagSet><Tag><Key>status</Key><Value>active</Value></Tag></TagSet></Tagging>`
	reqPut := httptest.NewRequest("PUT", "/otag-bucket/f.txt?tagging", strings.NewReader(tagBody))
	resp, err := p.HandleRequest(context.Background(), "", reqPut)
	require.NoError(t, err)
	assert.Equal(t, 200, resp.StatusCode)

	resp, err = p.HandleRequest(context.Background(), "", httptest.NewRequest("GET", "/otag-bucket/f.txt?tagging", nil))
	require.NoError(t, err)
	assert.Equal(t, 200, resp.StatusCode)
	assert.Contains(t, string(resp.Body), "status")
	assert.Contains(t, string(resp.Body), "active")

	resp, err = p.HandleRequest(context.Background(), "", httptest.NewRequest("DELETE", "/otag-bucket/f.txt?tagging", nil))
	require.NoError(t, err)
	assert.Equal(t, 204, resp.StatusCode)
}

// --- Task 14: ACL & Notifications ---

func TestS3Provider_BucketACL(t *testing.T) {
	p := newTestProvider(t)
	defer func() { _ = p.Shutdown(context.Background()) }()

	_, err := p.HandleRequest(context.Background(), "", httptest.NewRequest("PUT", "/acl-bucket", nil))
	require.NoError(t, err)

	// Default ACL should return FULL_CONTROL canned ACL
	resp, err := p.HandleRequest(context.Background(), "", httptest.NewRequest("GET", "/acl-bucket?acl", nil))
	require.NoError(t, err)
	assert.Equal(t, 200, resp.StatusCode)
	assert.Contains(t, string(resp.Body), "FULL_CONTROL")

	// Put custom ACL
	customACL := `<AccessControlPolicy><Owner><ID>custom-owner</ID></Owner></AccessControlPolicy>`
	reqPut := httptest.NewRequest("PUT", "/acl-bucket?acl", strings.NewReader(customACL))
	resp, err = p.HandleRequest(context.Background(), "", reqPut)
	require.NoError(t, err)
	assert.Equal(t, 200, resp.StatusCode)

	// Verify custom ACL stored
	resp, err = p.HandleRequest(context.Background(), "", httptest.NewRequest("GET", "/acl-bucket?acl", nil))
	require.NoError(t, err)
	assert.Equal(t, 200, resp.StatusCode)
	assert.Contains(t, string(resp.Body), "custom-owner")
}

func TestS3Provider_BucketNotification(t *testing.T) {
	p := newTestProvider(t)
	defer func() { _ = p.Shutdown(context.Background()) }()

	_, err := p.HandleRequest(context.Background(), "", httptest.NewRequest("PUT", "/notif-bucket", nil))
	require.NoError(t, err)

	// Default: empty config
	resp, err := p.HandleRequest(context.Background(), "", httptest.NewRequest("GET", "/notif-bucket?notification", nil))
	require.NoError(t, err)
	assert.Equal(t, 200, resp.StatusCode)
	assert.Contains(t, string(resp.Body), "NotificationConfiguration")

	// Put config
	config := `<NotificationConfiguration><TopicConfiguration><Id>test</Id><Topic>arn:aws:sns:us-east-1:123:MyTopic</Topic></TopicConfiguration></NotificationConfiguration>`
	reqPut := httptest.NewRequest("PUT", "/notif-bucket?notification", strings.NewReader(config))
	resp, err = p.HandleRequest(context.Background(), "", reqPut)
	require.NoError(t, err)
	assert.Equal(t, 200, resp.StatusCode)

	// Verify
	resp, err = p.HandleRequest(context.Background(), "", httptest.NewRequest("GET", "/notif-bucket?notification", nil))
	require.NoError(t, err)
	assert.Equal(t, 200, resp.StatusCode)
	assert.Contains(t, string(resp.Body), "MyTopic")
}
