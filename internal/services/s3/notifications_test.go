// SPDX-License-Identifier: Apache-2.0
package s3

import (
	"context"
	"github.com/stretchr/testify/require"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"testing"
	"time"
)

func TestS3LambdaFunctionErrorReported(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Amz-Function-Error", "Unhandled")
		_, _ = w.Write([]byte(`{}`))
	}))
	defer s.Close()
	require.Error(t, invokeLambda(context.Background(), s.URL, "function", []byte(`{}`)))
}
func TestS3QualifiedLambdaARN(t *testing.T) {
	require.Equal(t, "function:published", extractNameFromARN("arn:aws:lambda:us-east-1:000000000000:function:function:published"))
}
func TestS3NotificationOutlivesRequest(t *testing.T) {
	delivered := make(chan string, 1)
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { delivered <- r.URL.Path; _, _ = w.Write([]byte(`{}`)) }))
	defer s.Close()
	u, err := url.Parse(s.URL)
	require.NoError(t, err)
	port, err := strconv.Atoi(u.Port())
	require.NoError(t, err)
	p := newTestProvider(t)
	defer func() { require.NoError(t, p.Shutdown(context.Background())) }()
	p.serverPort = port
	require.NoError(t, p.metaStore.CreateBucket("notify-bucket", "us-east-1", defaultAccountID))
	require.NoError(t, p.metaStore.PutBucketNotification("notify-bucket", defaultAccountID, `<NotificationConfiguration><CloudFunctionConfiguration><CloudFunction>arn:aws:lambda:us-east-1:000000000000:function:function:published</CloudFunction><Event>s3:ObjectCreated:*</Event></CloudFunctionConfiguration></NotificationConfiguration>`))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	p.emitS3Event(ctx, "notify-bucket", "test.txt", 12, "ObjectCreated:Put")
	select {
	case path := <-delivered:
		require.Equal(t, "/2015-03-31/functions/function:published/invocations", path)
	case <-time.After(time.Second):
		t.Fatal("notification canceled with request")
	}
}
