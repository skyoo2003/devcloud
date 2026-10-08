// SPDX-License-Identifier: Apache-2.0
package s3

import (
	"bytes"
	"context"
	"github.com/skyoo2003/devcloud/internal/plugin"
	"github.com/stretchr/testify/require"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"
)

func renameSetup(t *testing.T) (*S3Provider, testDirectoryCredentials) {
	t.Helper()
	p := directorySetup(t)
	c := directoryIssue(t, p, directoryTestBucket, "")
	require.Equal(t, 200, directoryCall(t, p, "PUT", "/"+directoryTestBucket+"/source", []byte("original bytes"), c).StatusCode)
	require.Equal(t, 200, directoryCall(t, p, "PUT", "/"+directoryTestBucket+"/destination", []byte("previous destination"), c).StatusCode)
	meta, err := p.metaStore.GetObjectMeta(directoryTestBucket, "source", defaultAccountID)
	require.NoError(t, err)
	meta.LastModified = time.Unix(123, 0)
	meta.ContentType = "text/custom"
	require.NoError(t, p.metaStore.PutObjectMeta(*meta))
	require.NoError(t, p.metaStore.PutObjectTags(directoryTestBucket, "source", defaultAccountID, map[string]string{"source": "keep"}))
	require.NoError(t, p.metaStore.PutObjectTags(directoryTestBucket, "destination", defaultAccountID, map[string]string{"destination": "replace"}))
	return p, c
}
func renameHTTP(bucket, source, destination, token string, c testDirectoryCredentials) *http.Request {
	r := httptest.NewRequest("PUT", "/"+bucket+"/"+url.PathEscape(destination)+"?renameObject", nil)
	for k, v := range directoryAuth(c) {
		r.Header.Set(k, v)
	}
	r.Header.Set("X-Amz-Rename-Source", source)
	if token != "" {
		r.Header.Set("X-Amz-Client-Token", token)
	}
	return r
}
func renameRun(t *testing.T, p *S3Provider, r *http.Request) *plugin.Response {
	t.Helper()
	response, err := p.HandleRequest(context.Background(), "", r)
	require.NoError(t, err)
	return response
}
func renameReceiptCount(t *testing.T, p *S3Provider) int {
	t.Helper()
	var n int
	require.NoError(t, p.metaStore.store.DB().QueryRow(`SELECT count(*) FROM rename_receipts`).Scan(&n))
	return n
}

func TestRenameMovesBytesAndPreservesMetadata(t *testing.T) {
	p, c := renameSetup(t)
	p.metaStore.store.DB().SetMaxOpenConns(1)
	before, err := p.metaStore.GetObjectMeta(directoryTestBucket, "source", defaultAccountID)
	require.NoError(t, err)
	r := renameRun(t, p, renameHTTP(directoryTestBucket, "source", "destination", "move", c))
	require.Equal(t, 200, r.StatusCode)
	require.Empty(t, r.Body)
	require.Equal(t, 404, directoryCall(t, p, "GET", "/"+directoryTestBucket+"/source", nil, c).StatusCode)
	require.Equal(t, []byte("original bytes"), directoryCall(t, p, "GET", "/"+directoryTestBucket+"/destination", nil, c).Body)
	after, err := p.metaStore.GetObjectMeta(directoryTestBucket, "destination", defaultAccountID)
	require.NoError(t, err)
	before.Key = "destination"
	require.Equal(t, before, after)
	tags, err := p.metaStore.GetObjectTags(directoryTestBucket, "destination", defaultAccountID)
	require.NoError(t, err)
	require.Equal(t, map[string]string{"source": "keep"}, tags)
	oldTags, err := p.metaStore.GetObjectTags(directoryTestBucket, "source", defaultAccountID)
	require.NoError(t, err)
	require.Empty(t, oldTags)
	require.Equal(t, 1, renameReceiptCount(t, p))
	nested := renameRun(t, p, renameHTTP(directoryTestBucket, "destination", "folder/deep/result", "nested", c))
	require.Equal(t, 200, nested.StatusCode)
	require.Equal(t, []byte("original bytes"), directoryCall(t, p, "GET", "/"+directoryTestBucket+"/folder/deep/result", nil, c).Body)
	list := directoryCall(t, p, "GET", "/"+directoryTestBucket+"?list-type=2", nil, c)
	require.Contains(t, string(list.Body), "folder/deep/result")
	require.NotContains(t, string(list.Body), ".rename-staging")
}

func TestRenameExplicitMalformedRequestsDoNotMutate(t *testing.T) {
	p, c := renameSetup(t)
	for _, target := range []string{"/" + directoryTestBucket + "?x-id=RenameObject", "/" + directoryTestBucket + "/destination?x-id=RenameObject", "/" + directoryTestBucket + "/destination?renameObject&partNumber=1", "/" + directoryTestBucket + "/destination?renameObject&tagging"} {
		r := httptest.NewRequest("PUT", target, nil)
		for k, v := range directoryAuth(c) {
			r.Header.Set(k, v)
		}
		if !bytes.Contains([]byte(target), []byte("x-id=RenameObject")) {
			r.Header.Set("X-Amz-Rename-Source", "source")
		}
		response := renameRun(t, p, r)
		require.Equal(t, 400, response.StatusCode)
	}
	for _, method := range []string{"GET", "HEAD", "POST", "DELETE"} {
		r := renameHTTP(directoryTestBucket, "source", "destination", "", c)
		r.Method = method
		require.Equal(t, 405, renameRun(t, p, r).StatusCode)
	}
	general := partCopyCall(t, p, "PUT", "/general", nil, nil)
	require.Equal(t, 200, general.StatusCode)
	require.Equal(t, 400, renameRun(t, p, renameHTTP("general", "source", "destination", "", c)).StatusCode)
	require.Equal(t, 404, renameRun(t, p, renameHTTP("missing--use1-az1--x-s3", "source", "destination", "", c)).StatusCode)
	require.Equal(t, []byte("original bytes"), directoryCall(t, p, "GET", "/"+directoryTestBucket+"/source", nil, c).Body)
	require.Equal(t, []byte("previous destination"), directoryCall(t, p, "GET", "/"+directoryTestBucket+"/destination", nil, c).Body)
}

func TestRenameSelfAndNoToken(t *testing.T) {
	p, c := renameSetup(t)
	require.Equal(t, 200, renameRun(t, p, renameHTTP(directoryTestBucket, "source", "source", "", c)).StatusCode)
	require.Zero(t, renameReceiptCount(t, p))
	require.Equal(t, []byte("original bytes"), directoryCall(t, p, "GET", "/"+directoryTestBucket+"/source", nil, c).Body)
	r := renameHTTP(directoryTestBucket, "source", "source", "", c)
	r.Header.Set("If-None-Match", "*")
	require.Equal(t, 412, renameRun(t, p, r).StatusCode)
	r = renameHTTP(directoryTestBucket, "source", "destination", "", c)
	r.Header.Set("X-Amz-Rename-Source-If-Match", "wrong")
	require.Equal(t, 412, renameRun(t, p, r).StatusCode)
	require.Equal(t, 404, renameRun(t, p, renameHTTP(directoryTestBucket, "absent", "destination", "", c)).StatusCode)
	require.Equal(t, 200, renameRun(t, p, renameHTTP(directoryTestBucket, "source", "destination", "", c)).StatusCode)
	require.Zero(t, renameReceiptCount(t, p))
}
