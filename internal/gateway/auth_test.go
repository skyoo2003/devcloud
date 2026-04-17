// SPDX-License-Identifier: Apache-2.0

package gateway

import (
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestExtractAccountID(t *testing.T) {
	req := httptest.NewRequest("GET", "/", nil)
	req.Header.Set("Authorization",
		"AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request, "+
			"SignedHeaders=host;range;x-amz-date, Signature=abc123")
	accountID := ExtractAccountID(req)
	assert.Equal(t, "000000000000", accountID)
}

func TestExtractAccountID_NoAuth(t *testing.T) {
	req := httptest.NewRequest("GET", "/", nil)
	accountID := ExtractAccountID(req)
	assert.Equal(t, "000000000000", accountID)
}
