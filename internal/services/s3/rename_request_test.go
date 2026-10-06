// SPDX-License-Identifier: Apache-2.0
package s3

import (
	"github.com/stretchr/testify/require"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
)

func renameParsingRequest(source string) *http.Request {
	r := httptest.NewRequest("PUT", "/"+directoryTestBucket+"/dest?renameObject", nil)
	r.Header.Set("X-Amz-Rename-Source", source)
	return r
}
func renameErrorCode(t *testing.T, err error) string {
	t.Helper()
	var api *s3OperationError
	require.ErrorAs(t, err, &api)
	return api.Code
}
func TestRenameRequestEncodedSourceAndForms(t *testing.T) {
	key := "한글 + %2F ? #/원본"
	for _, source := range []string{url.PathEscape(key), "/" + url.PathEscape(key), directoryTestBucket + "/" + url.PathEscape(key)} {
		value, err := parseRenameRequest(directoryTestBucket, "dest", renameParsingRequest(source))
		require.NoError(t, err)
		require.Equal(t, key, value.SourceKey)
		require.Equal(t, "dest", value.DestinationKey)
	}
	literal := directoryTestBucket + "/literal"
	value, err := parseRenameRequest(directoryTestBucket, "dest", renameParsingRequest("/"+literal))
	require.NoError(t, err)
	require.Equal(t, literal, value.SourceKey)
}

type renameFailingReader struct{}

func (renameFailingReader) Read([]byte) (int, error) { return 0, io.ErrUnexpectedEOF }

type renameInitiallyEmptyReader struct {
	first     bool
	remaining io.Reader
}

func (r *renameInitiallyEmptyReader) Read(out []byte) (int, error) {
	if !r.first {
		r.first = true
		return 0, nil
	}
	return r.remaining.Read(out)
}
func TestRenameRequestInitiallyEmptyReadStillRejectsBody(t *testing.T) {
	r := renameParsingRequest("source")
	r.Body = io.NopCloser(&renameInitiallyEmptyReader{remaining: strings.NewReader("body")})
	_, err := parseRenameRequest(directoryTestBucket, "dest", r)
	require.Equal(t, "InvalidRequest", renameErrorCode(t, err))
}
func TestRenameRequestKeyTokenAndSubresourceValidation(t *testing.T) {
	for _, tc := range []struct {
		name, key, source, query, token string
		hasToken                        bool
		body                            io.Reader
		code                            string
	}{
		{name: "empty-key", source: "source", code: "InvalidArgument"},
		{name: "large-key", key: strings.Repeat("a", 1025), source: "source", code: "InvalidArgument"},
		{name: "invalid-utf8", key: string([]byte{255}), source: "source", code: "InvalidArgument"},
		{name: "missing-source", key: "dest", code: "InvalidArgument"},
		{name: "large-source", key: "dest", source: strings.Repeat("a", 1025), code: "InvalidArgument"},
		{name: "malformed-encoding", key: "dest", source: "%zz", code: "InvalidArgument"},
		{name: "traversal", key: "dest", source: "../source", code: "InvalidArgument"},
		{name: "source-dot", key: "dest", source: "a/./b", code: "InvalidArgument"},
		{name: "destination-dot", key: "a/../b", source: "source", code: "InvalidArgument"},
		{name: "double-slash", key: "dest", source: "a//b", code: "InvalidArgument"},
		{name: "absolute", key: "/dest", source: "source", code: "InvalidArgument"},
		{name: "source-slash", key: "dest", source: "source/", code: "InvalidRequest"},
		{name: "destination-slash", key: "dest/", source: "source", code: "InvalidRequest"},
		{name: "foreign-bucket", key: "dest", source: "other--use1-az1--x-s3/source", code: "InvalidRequest"},
		{name: "version", key: "dest", source: "source?versionId=old", code: "NotImplemented"},
		{name: "access-point", key: "dest", source: "arn:aws:s3:us-east-1:000000000000:accesspoint/ap/key", code: "NotImplemented"},
		{name: "empty-token", key: "dest", source: "source", hasToken: true, code: "InvalidArgument"},
		{name: "large-token", key: "dest", source: "source", token: strings.Repeat("t", 65), hasToken: true, code: "InvalidArgument"},
		{name: "space-token", key: "dest", source: "source", token: " ", hasToken: true, code: "InvalidArgument"},
		{name: "del-token", key: "dest", source: "source", token: string([]byte{127}), hasToken: true, code: "InvalidArgument"},
		{name: "part", key: "dest", source: "source", query: "renameObject&partNumber=1", code: "InvalidRequest"},
		{name: "upload", key: "dest", source: "source", query: "renameObject&uploadId=x", code: "InvalidRequest"},
		{name: "tagging", key: "dest", source: "source", query: "renameObject&tagging", code: "InvalidRequest"},
		{name: "body", key: "dest", source: "source", body: strings.NewReader("data"), code: "InvalidRequest"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := renameParsingRequest(tc.source)
			if tc.source == "" {
				r.Header.Del("X-Amz-Rename-Source")
			}
			if tc.query != "" {
				r.URL.RawQuery = tc.query
			}
			if tc.hasToken {
				r.Header.Set("X-Amz-Client-Token", tc.token)
			}
			if tc.body != nil {
				r.Body = io.NopCloser(tc.body)
			}
			_, err := parseRenameRequest(directoryTestBucket, tc.key, r)
			require.Equal(t, tc.code, renameErrorCode(t, err))
		})
	}
	for _, key := range []string{"a", strings.Repeat("a", 1024), strings.Repeat("한", 341) + "a"} {
		_, err := parseRenameRequest(directoryTestBucket, key, renameParsingRequest("source"))
		require.NoError(t, err)
	}
	for _, token := range []string{"!", "~", strings.Repeat("t", 64)} {
		r := renameParsingRequest("source")
		r.Header.Set("X-Amz-Client-Token", token)
		v, err := parseRenameRequest(directoryTestBucket, "dest", r)
		require.NoError(t, err)
		require.True(t, v.HasClientToken)
		require.Equal(t, token, v.ClientToken)
	}
	r := renameParsingRequest("source")
	r.Body = io.NopCloser(renameFailingReader{})
	_, err := parseRenameRequest(directoryTestBucket, "dest", r)
	require.ErrorIs(t, err, io.ErrUnexpectedEOF)
	r = renameParsingRequest("source")
	r.Header.Set("X-Amz-Copy-Source", "copy")
	_, err = parseRenameRequest(directoryTestBucket, "dest", r)
	require.Equal(t, "InvalidRequest", renameErrorCode(t, err))
}
func TestRenameCanonicalParameters(t *testing.T) {
	a := renameParsingRequest("source")
	a.Header.Set("X-Amz-Rename-Source-If-Match", `"two","one"`)
	a.Header.Set("If-Unmodified-Since", "Wed, 07 Oct 2026 01:00:00 GMT")
	b := renameParsingRequest(directoryTestBucket + "/source")
	b.Header.Set("X-Amz-Rename-Source-If-Match", `one,two,one`)
	b.Header.Set("If-Unmodified-Since", "Wednesday, 07-Oct-26 01:00:00 GMT")
	one, err := parseRenameRequest(directoryTestBucket, "dest", a)
	require.NoError(t, err)
	two, err := parseRenameRequest(directoryTestBucket, "dest", b)
	require.NoError(t, err)
	require.Equal(t, one.Fingerprint, two.Fingerprint)
	b.Header.Del("If-Unmodified-Since")
	two, err = parseRenameRequest(directoryTestBucket, "dest", b)
	require.NoError(t, err)
	require.NotEqual(t, one.Fingerprint, two.Fingerprint)
	a = renameParsingRequest("source")
	one, err = parseRenameRequest(directoryTestBucket, "dest", a)
	require.NoError(t, err)
	require.False(t, one.HasClientToken)
	a.Header.Set("Authorization", "irrelevant")
	a.Header.Set("X-Amz-S3session-Token", "other-session")
	two, err = parseRenameRequest(directoryTestBucket, "dest", a)
	require.NoError(t, err)
	require.Equal(t, one.Fingerprint, two.Fingerprint)
}
