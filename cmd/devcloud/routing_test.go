// SPDX-License-Identifier: Apache-2.0

// cmd/devcloud/routing_test.go
package main

import (
	"net/http/httptest"
	"testing"

	"github.com/skyoo2003/devcloud/internal/gateway"
	"github.com/stretchr/testify/assert"
)

// TestContestedDataPlanesResolveToThemselves covers the three Phase 2 services
// that sign with a name an already-registered neighbour claims. Nothing else in
// the tree can catch this: the fidelity manifest is derived from dispatch
// literals, so it reports all three as fully hand-verified whether or not the
// gateway ever routes to them.
//
// It lives here rather than in internal/gateway because this package already
// blank-imports every service and the generated crudregistry, so the tables
// under test are the real ones.
func TestContestedDataPlanesResolveToThemselves(t *testing.T) {
	cases := []struct{ name, signingName, method, uri, want string }{
		{"payment_crypto_encrypt", "payment-cryptography", "POST", "/keys/k1/encrypt", "paymentcryptographydata"},
		{"payment_crypto_verify_mac", "payment-cryptography", "POST", "/mac/verify", "paymentcryptographydata"},
		{"cloudsearch_search", "cloudsearch", "GET", "/2013-01-01/search?format=sdk&pretty=true&q=x", "cloudsearchdomain"},
		// botocore converts Search to a POST with a form body, so this — not the
		// modelled GET above — is the request a real client sends. Asserting only
		// the model's shape is how the gateway came to hand this to cloudsearch.
		{"cloudsearch_search_as_boto3_sends_it", "cloudsearch", "POST", "/2013-01-01/search", "cloudsearchdomain"},
		{"cloudsearch_upload", "cloudsearch", "POST", "/2013-01-01/documents/batch?format=sdk", "cloudsearchdomain"},
		{"kvs_join", "kinesisvideo", "POST", "/joinStorageSession", "kinesisvideowebrtcstorage"},
		{"kvs_join_as_viewer", "kinesisvideo", "POST", "/joinStorageSessionAsViewer", "kinesisvideowebrtcstorage"},
		// The parents keep everything they model. A split that stole these would
		// be a regression dressed as a fix.
		{"kinesisvideo_keeps_its_own", "kinesisvideo", "POST", "/createStream", "kinesisvideo"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			req := httptest.NewRequest(c.method, c.uri, nil)
			req.Header.Set("Authorization",
				"AWS4-HMAC-SHA256 Credential=AKIA/20130524/us-east-1/"+c.signingName+"/aws4_request, Signature=abc")
			proto, service := gateway.DetectProtocol(req)
			assert.Equal(t, "rest-json", proto)
			assert.Equal(t, c.want, service)
		})
	}
}
