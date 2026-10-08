// SPDX-License-Identifier: Apache-2.0
package gateway

import (
	"context"
	"github.com/skyoo2003/devcloud/internal/plugin"
	"github.com/skyoo2003/devcloud/internal/services/s3"
	"github.com/stretchr/testify/require"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestDetectProtocolS3Express(t *testing.T) {
	for _, path := range []string{"/bucket/key?renameObject", "/bucket?session", "/v20180820/accesspoint/x"} {
		r := httptest.NewRequest("GET", path, nil)
		r.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=ASIA/20261007/us-east-1/s3express/aws4_request, Signature=local")
		proto, svc := DetectProtocol(r)
		require.Equal(t, "rest-xml", proto)
		require.Equal(t, "s3", svc)
	}
}
func TestDirectoryCanonicalErrorsStayTerminal(t *testing.T) {
	p := &s3.S3Provider{}
	registry := plugin.NewRegistry()
	registry.Register("s3", func() plugin.ServicePlugin { return p })
	_, err := registry.Init("s3", plugin.PluginConfig{DataDir: t.TempDir()})
	require.NoError(t, err)
	defer func() { _ = p.Shutdown(context.Background()) }()
	router := NewServiceRouter(registry, nil)
	config := `<CreateBucketConfiguration><Location><Type>AvailabilityZone</Type><Name>use1-az1</Name></Location><Bucket><Type>Directory</Type><DataRedundancy>SingleAvailabilityZone</DataRedundancy></Bucket></CreateBucketConfiguration>`
	req := httptest.NewRequest("PUT", "/terminal--use1-az1--x-s3", strings.NewReader(config))
	out := httptest.NewRecorder()
	router.ServeHTTP(out, req)
	require.Equal(t, 200, out.Code)
	for _, tc := range []struct {
		method, target, body string
		status               int
		encrypt              bool
	}{
		{"GET", "/terminal--use1-az1--x-s3?session", "", 501, true},
		{"PUT", "/zone--use1-az1--x-s3", strings.ReplaceAll(config, "AvailabilityZone", "LocalZone"), 501, false},
		{"PUT", "/terminal--use1-az1--x-s3?session", "", 405, false},
		{"GET", "/terminal--use1-az1--x-s3/key?renameObject", "", 405, false},
	} {
		r := httptest.NewRequest(tc.method, tc.target, strings.NewReader(tc.body))
		if tc.encrypt {
			r.Header.Set("X-Amz-Server-Side-Encryption", "AES256")
		}
		w := httptest.NewRecorder()
		router.ServeHTTP(w, r)
		require.Equal(t, tc.status, w.Code, w.Body.String())
	}
}
