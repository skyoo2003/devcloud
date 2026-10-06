// SPDX-License-Identifier: Apache-2.0
package s3

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"github.com/skyoo2003/devcloud/internal/plugin"
	"github.com/stretchr/testify/require"
	"net/http/httptest"
	"testing"
	"time"
)

type testDirectoryCredentials struct {
	AccessKeyID                   string `xml:"AccessKeyId"`
	SecretAccessKey, SessionToken string
	Expiration                    time.Time
}

func directoryIssue(t *testing.T, p *S3Provider, bucket, mode string) testDirectoryCredentials {
	t.Helper()
	h := map[string]string{}
	if mode != "" {
		h["X-Amz-Create-Session-Mode"] = mode
	}
	r := partCopyCall(t, p, "GET", "/"+bucket+"?session", nil, h)
	require.Equal(t, 200, r.StatusCode)
	var out struct {
		XMLName     xml.Name
		Credentials testDirectoryCredentials
	}
	require.NoError(t, xml.Unmarshal(r.Body, &out))
	require.Equal(t, "CreateSessionResult", out.XMLName.Local)
	return out.Credentials
}
func directoryAuth(c testDirectoryCredentials) map[string]string {
	return map[string]string{"X-Amz-S3session-Token": c.SessionToken, "Authorization": fmt.Sprintf("AWS4-HMAC-SHA256 Credential=%s/20261007/us-east-1/s3express/aws4_request, SignedHeaders=host, Signature=local", c.AccessKeyID)}
}
func directoryCall(t *testing.T, p *S3Provider, method, target string, body []byte, c testDirectoryCredentials) *plugin.Response {
	t.Helper()
	return partCopyCall(t, p, method, target, body, directoryAuth(c))
}

func TestDirectorySessionCredentialShapeAndReopen(t *testing.T) {
	p := directorySetup(t)
	before := time.Now()
	c := directoryIssue(t, p, directoryTestBucket, "")
	after := time.Now()
	require.Len(t, c.AccessKeyID, 20)
	require.Len(t, c.SecretAccessKey, 40)
	require.NotEmpty(t, c.SessionToken)
	require.True(t, c.Expiration.After(before.Add(5*time.Minute-time.Second)))
	require.False(t, c.Expiration.After(after.Add(5*time.Minute)))
	next := directoryIssue(t, p, directoryTestBucket, "")
	require.NotEqual(t, c.AccessKeyID, next.AccessKeyID)
	var digest, mode string
	require.NoError(t, p.metaStore.store.DB().QueryRow(`SELECT token_digest,mode FROM directory_sessions WHERE access_key_id=?`, c.AccessKeyID).Scan(&digest, &mode))
	sum := sha256.Sum256([]byte(c.SessionToken))
	require.Equal(t, hex.EncodeToString(sum[:]), digest)
	require.Equal(t, "ReadWrite", mode)
	var secrets int
	require.NoError(t, p.metaStore.store.DB().QueryRow(`SELECT count(*) FROM pragma_table_info('directory_sessions') WHERE name LIKE '%secret%'`).Scan(&secrets))
	require.Zero(t, secrets)
	root := p.fileStore.baseDir
	require.NoError(t, p.Shutdown(context.Background()))
	reopen := &S3Provider{}
	require.NoError(t, reopen.Init(plugin.PluginConfig{DataDir: root}))
	defer func() { _ = reopen.Shutdown(context.Background()) }()
	require.Equal(t, 200, directoryCall(t, reopen, "PUT", "/"+directoryTestBucket+"/valid", []byte("data"), c).StatusCode)
}
func TestDirectorySessionModeScopeExpiryAndAccessKey(t *testing.T) {
	p := directorySetup(t)
	rw := directoryIssue(t, p, directoryTestBucket, "ReadWrite")
	ro := directoryIssue(t, p, directoryTestBucket, "ReadOnly")
	require.Equal(t, 200, directoryCall(t, p, "PUT", "/"+directoryTestBucket+"/key", []byte("keep"), rw).StatusCode)
	require.Equal(t, 200, directoryCall(t, p, "GET", "/"+directoryTestBucket+"/key", nil, ro).StatusCode)
	require.Equal(t, 403, directoryCall(t, p, "PUT", "/"+directoryTestBucket+"/key", []byte("bad"), ro).StatusCode)
	require.Equal(t, 403, partCopyCall(t, p, "GET", "/"+directoryTestBucket+"/key", nil, nil).StatusCode)
	for _, change := range []func(*testDirectoryCredentials){func(c *testDirectoryCredentials) { c.SessionToken = "invalid" }, func(c *testDirectoryCredentials) { c.AccessKeyID = "wrong" }} {
		bad := rw
		change(&bad)
		r := directoryCall(t, p, "GET", "/"+directoryTestBucket+"/key", nil, bad)
		require.Equal(t, 403, r.StatusCode)
		require.Contains(t, string(r.Body), "InvalidToken")
	}
	other := "other--use1-az1--x-s3"
	require.Equal(t, 200, partCopyCall(t, p, "PUT", "/"+other, directoryConfig("use1-az1", "AvailabilityZone"), nil).StatusCode)
	require.Equal(t, 403, directoryCall(t, p, "GET", "/"+other+"?list-type=2", nil, rw).StatusCode)
	_, err := p.metaStore.store.DB().Exec(`UPDATE directory_sessions SET expires_at=? WHERE access_key_id=?`, time.Now().Add(-time.Second).Unix(), rw.AccessKeyID)
	require.NoError(t, err)
	r := directoryCall(t, p, "GET", "/"+directoryTestBucket+"/key", nil, rw)
	require.Equal(t, 403, r.StatusCode)
	require.Contains(t, string(r.Body), "ExpiredToken")
	require.Equal(t, []byte("keep"), directoryCall(t, p, "GET", "/"+directoryTestBucket+"/key", nil, ro).Body)
}
func TestDirectorySessionDatabaseFailureIsInternalError(t *testing.T) {
	p := directorySetup(t)
	c := directoryIssue(t, p, directoryTestBucket, "")
	require.NoError(t, p.metaStore.Close())
	r := httptest.NewRequest("GET", "/"+directoryTestBucket+"?list-type=2", nil)
	for k, v := range directoryAuth(c) {
		r.Header.Set(k, v)
	}
	resp, err := p.HandleRequest(context.Background(), "", r)
	require.Error(t, err)
	require.Nil(t, resp)
}
func TestDirectorySessionEncryptionAndGeneralBucketErrors(t *testing.T) {
	p := directorySetup(t)
	require.Equal(t, 200, partCopyCall(t, p, "PUT", "/general", nil, nil).StatusCode)
	for _, tc := range []struct {
		target string
		h      map[string]string
		status int
		code   string
	}{
		{"/general?session", nil, 400, "InvalidRequest"}, {"/missing?session", nil, 404, "NoSuchBucket"}, {"/" + directoryTestBucket + "?session", map[string]string{"X-Amz-Create-Session-Mode": "bad"}, 400, "InvalidArgument"},
		{"/" + directoryTestBucket + "?session", map[string]string{"X-Amz-Server-Side-Encryption": "AES256"}, 501, "NotImplemented"},
	} {
		r := partCopyCall(t, p, "GET", tc.target, nil, tc.h)
		require.Equal(t, tc.status, r.StatusCode)
		require.Contains(t, string(r.Body), tc.code)
		require.Empty(t, r.Headers["x-amz-server-side-encryption"])
	}
}
