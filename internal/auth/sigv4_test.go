// SPDX-License-Identifier: Apache-2.0

package auth

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSigV4_AuthorizationHeader(t *testing.T) {
	r := httptest.NewRequest(http.MethodPost, "/", nil)
	r.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/eu-west-1/lambda/aws4_request, "+
		"SignedHeaders=host;x-amz-date, Signature=deadbeef")
	r.Header.Set("X-Amz-Security-Token", "session-token")

	id, ok := SigV4{}.Authenticate(r)
	require.True(t, ok)
	assert.Equal(t, "aws", id.Provider)
	assert.Equal(t, "AKIAIOSFODNN7EXAMPLE", id.AccessKeyID)
	assert.Equal(t, "eu-west-1", id.Region)
	assert.Equal(t, "lambda", id.Service)
	assert.Equal(t, "session-token", id.SessionToken)
}

// A presigned URL carries no Authorization header; the same scope arrives
// percent-encoded in the query string.
func TestSigV4_PresignedQuery(t *testing.T) {
	r := httptest.NewRequest(http.MethodGet,
		"/my-bucket/key?X-Amz-Algorithm=AWS4-HMAC-SHA256"+
			"&X-Amz-Credential=AKIAIOSFODNN7EXAMPLE%2F20130524%2Fus-west-2%2Fs3%2Faws4_request"+
			"&X-Amz-Security-Token=presigned-token", nil)

	id, ok := SigV4{}.Authenticate(r)
	require.True(t, ok)
	assert.Equal(t, "us-west-2", id.Region)
	assert.Equal(t, "s3", id.Service)
	assert.Equal(t, "presigned-token", id.SessionToken)
}

func TestSigV4_Unrecognized(t *testing.T) {
	for name, setup := range map[string]func(*http.Request){
		"no credentials": func(*http.Request) {},
		"sigv2 header": func(r *http.Request) {
			r.Header.Set("Authorization", "AWS AKIAIOSFODNN7EXAMPLE:signature")
		},
		"sigv4 without credential": func(r *http.Request) {
			r.Header.Set("Authorization", "AWS4-HMAC-SHA256 SignedHeaders=host, Signature=deadbeef")
		},
	} {
		t.Run(name, func(t *testing.T) {
			r := httptest.NewRequest(http.MethodGet, "/", nil)
			setup(r)
			_, ok := SigV4{}.Authenticate(r)
			assert.False(t, ok)
		})
	}
}

// A truncated scope still yields the access key. Callers read the field they
// need, so a partial identity beats discarding the whole header.
func TestSigV4_TruncatedScope(t *testing.T) {
	r := httptest.NewRequest(http.MethodGet, "/", nil)
	r.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524, Signature=deadbeef")

	id, ok := SigV4{}.Authenticate(r)
	require.True(t, ok)
	assert.Equal(t, "AKIAIOSFODNN7EXAMPLE", id.AccessKeyID)
	assert.Empty(t, id.Region)
	assert.Empty(t, id.Service)
}

func TestIdentifyAndContext(t *testing.T) {
	r := httptest.NewRequest(http.MethodGet, "/", nil)
	r.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=AKIA/20130524/ap-northeast-2/s3/aws4_request, Signature=x")

	id, ok := Identify(r)
	require.True(t, ok)

	ctx := NewContext(context.Background(), id)
	got, ok := FromContext(ctx)
	require.True(t, ok)
	assert.Equal(t, "ap-northeast-2", got.Region)

	_, ok = FromContext(context.Background())
	assert.False(t, ok)
}
