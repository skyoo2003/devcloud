// SPDX-License-Identifier: Apache-2.0

package acmpca

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/skyoo2003/devcloud/internal/plugin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestProvider(t *testing.T) *Provider {
	t.Helper()
	p := &Provider{}
	err := p.Init(plugin.PluginConfig{DataDir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
	return p
}

func pcaReq(t *testing.T, p *Provider, op string, body any) *plugin.Response {
	t.Helper()
	b, err := json.Marshal(body)
	require.NoError(t, err)
	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(b))
	req.Header.Set("X-Amz-Target", "ACMPrivateCA."+op)
	resp, err := p.HandleRequest(context.Background(), op, req)
	require.NoError(t, err)
	return resp
}

func TestCertificateAuthorityCRUD(t *testing.T) {
	p := newTestProvider(t)

	// CreateCertificateAuthority
	resp := pcaReq(t, p, "CreateCertificateAuthority", map[string]any{
		"CertificateAuthorityType": "ROOT",
		"UsageMode":                "SHORT_LIVED_CERTIFICATE",
		"CertificateAuthorityConfiguration": map[string]any{
			"KeyAlgorithm":     "RSA_2048",
			"SigningAlgorithm": "SHA256WITHRSA",
			"Subject": map[string]string{
				"commonName": "DevCloud Root CA",
			},
		},
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var createOut map[string]string
	require.NoError(t, json.Unmarshal(resp.Body, &createOut))
	caARN := createOut["CertificateAuthorityArn"]
	require.NotEmpty(t, caARN)
	assert.True(t, strings.HasPrefix(caARN, "arn:aws:acm-pca:"))

	// DescribeCertificateAuthority
	resp = pcaReq(t, p, "DescribeCertificateAuthority", map[string]string{
		"CertificateAuthorityArn": caARN,
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	var descOut map[string]any
	require.NoError(t, json.Unmarshal(resp.Body, &descOut))
	ca := descOut["CertificateAuthority"].(map[string]any)
	assert.Equal(t, caARN, ca["Arn"])
	assert.Equal(t, "ACTIVE", ca["Status"])
	assert.Equal(t, "ROOT", ca["Type"])

	// ListCertificateAuthorities
	resp = pcaReq(t, p, "ListCertificateAuthorities", map[string]any{})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	var listOut map[string]any
	require.NoError(t, json.Unmarshal(resp.Body, &listOut))
	authorities := listOut["CertificateAuthorities"].([]any)
	require.Len(t, authorities, 1)

	// UpdateCertificateAuthority
	resp = pcaReq(t, p, "UpdateCertificateAuthority", map[string]any{
		"CertificateAuthorityArn": caARN,
		"Status":                  "DISABLED",
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	resp = pcaReq(t, p, "DescribeCertificateAuthority", map[string]string{
		"CertificateAuthorityArn": caARN,
	})
	require.NoError(t, json.Unmarshal(resp.Body, &descOut))
	ca = descOut["CertificateAuthority"].(map[string]any)
	assert.Equal(t, "DISABLED", ca["Status"])

	// GetCertificateAuthorityCsr
	resp = pcaReq(t, p, "GetCertificateAuthorityCsr", map[string]string{
		"CertificateAuthorityArn": caARN,
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	var csrOut map[string]string
	require.NoError(t, json.Unmarshal(resp.Body, &csrOut))
	assert.Contains(t, csrOut["Csr"], "CERTIFICATE REQUEST")

	// GetCertificateAuthorityCertificate
	resp = pcaReq(t, p, "GetCertificateAuthorityCertificate", map[string]string{
		"CertificateAuthorityArn": caARN,
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	var caCertOut map[string]string
	require.NoError(t, json.Unmarshal(resp.Body, &caCertOut))
	assert.Contains(t, caCertOut["Certificate"], "BEGIN CERTIFICATE")

	// DeleteCertificateAuthority
	resp = pcaReq(t, p, "DeleteCertificateAuthority", map[string]string{
		"CertificateAuthorityArn": caARN,
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// Verify it is marked DELETED
	resp = pcaReq(t, p, "DescribeCertificateAuthority", map[string]string{
		"CertificateAuthorityArn": caARN,
	})
	require.NoError(t, json.Unmarshal(resp.Body, &descOut))
	ca = descOut["CertificateAuthority"].(map[string]any)
	assert.Equal(t, "DELETED", ca["Status"])

	// RestoreCertificateAuthority
	resp = pcaReq(t, p, "RestoreCertificateAuthority", map[string]string{
		"CertificateAuthorityArn": caARN,
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	resp = pcaReq(t, p, "DescribeCertificateAuthority", map[string]string{
		"CertificateAuthorityArn": caARN,
	})
	require.NoError(t, json.Unmarshal(resp.Body, &descOut))
	ca = descOut["CertificateAuthority"].(map[string]any)
	assert.Equal(t, "ACTIVE", ca["Status"])
}

func TestIssueCertificate(t *testing.T) {
	p := newTestProvider(t)

	// Create CA
	resp := pcaReq(t, p, "CreateCertificateAuthority", map[string]any{
		"CertificateAuthorityType": "ROOT",
		"CertificateAuthorityConfiguration": map[string]any{
			"KeyAlgorithm":     "RSA_2048",
			"SigningAlgorithm": "SHA256WITHRSA",
			"Subject":          map[string]string{"commonName": "Test CA"},
		},
	})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var caOut map[string]string
	require.NoError(t, json.Unmarshal(resp.Body, &caOut))
	caARN := caOut["CertificateAuthorityArn"]

	// IssueCertificate
	resp = pcaReq(t, p, "IssueCertificate", map[string]any{
		"CertificateAuthorityArn": caARN,
		"SigningAlgorithm":        "SHA256WITHRSA",
		"Validity": map[string]any{
			"Type":  "DAYS",
			"Value": 365,
		},
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	var issueOut map[string]string
	require.NoError(t, json.Unmarshal(resp.Body, &issueOut))
	certARN := issueOut["CertificateArn"]
	require.NotEmpty(t, certARN)

	// GetCertificate
	resp = pcaReq(t, p, "GetCertificate", map[string]any{
		"CertificateAuthorityArn": caARN,
		"CertificateArn":          certARN,
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	var certOut map[string]string
	require.NoError(t, json.Unmarshal(resp.Body, &certOut))
	assert.Contains(t, certOut["Certificate"], "BEGIN CERTIFICATE")
	assert.Contains(t, certOut["CertificateChain"], "BEGIN CERTIFICATE")

	// RevokeCertificate
	resp = pcaReq(t, p, "RevokeCertificate", map[string]any{
		"CertificateAuthorityArn": caARN,
		"CertificateSerial":       "deadbeef",
		"RevocationReason":        "KEY_COMPROMISE",
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// CreateCertificateAuthorityAuditReport
	resp = pcaReq(t, p, "CreateCertificateAuthorityAuditReport", map[string]any{
		"CertificateAuthorityArn":   caARN,
		"S3BucketName":              "my-audit-bucket",
		"AuditReportResponseFormat": "JSON",
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	var auditOut map[string]string
	require.NoError(t, json.Unmarshal(resp.Body, &auditOut))
	assert.NotEmpty(t, auditOut["AuditReportId"])

	// DescribeCertificateAuthorityAuditReport
	resp = pcaReq(t, p, "DescribeCertificateAuthorityAuditReport", map[string]any{
		"CertificateAuthorityArn": caARN,
		"AuditReportId":           auditOut["AuditReportId"],
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	var auditDescOut map[string]any
	require.NoError(t, json.Unmarshal(resp.Body, &auditDescOut))
	assert.Equal(t, "SUCCESS", auditDescOut["AuditReportStatus"])
}

func TestPermissionCRUD(t *testing.T) {
	p := newTestProvider(t)

	// Create CA
	resp := pcaReq(t, p, "CreateCertificateAuthority", map[string]any{
		"CertificateAuthorityType": "ROOT",
		"CertificateAuthorityConfiguration": map[string]any{
			"KeyAlgorithm":     "RSA_2048",
			"SigningAlgorithm": "SHA256WITHRSA",
			"Subject":          map[string]string{},
		},
	})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var caOut map[string]string
	require.NoError(t, json.Unmarshal(resp.Body, &caOut))
	caARN := caOut["CertificateAuthorityArn"]

	// CreatePermission
	resp = pcaReq(t, p, "CreatePermission", map[string]any{
		"CertificateAuthorityArn": caARN,
		"Principal":               "acm.amazonaws.com",
		"Actions":                 []string{"IssueCertificate", "GetCertificate", "ListPermissions"},
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// ListPermissions
	resp = pcaReq(t, p, "ListPermissions", map[string]any{
		"CertificateAuthorityArn": caARN,
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	var listOut map[string]any
	require.NoError(t, json.Unmarshal(resp.Body, &listOut))
	perms := listOut["Permissions"].([]any)
	require.Len(t, perms, 1)
	perm := perms[0].(map[string]any)
	assert.Equal(t, "acm.amazonaws.com", perm["Principal"])

	// DeletePermission
	resp = pcaReq(t, p, "DeletePermission", map[string]any{
		"CertificateAuthorityArn": caARN,
		"Principal":               "acm.amazonaws.com",
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// ListPermissions — empty
	resp = pcaReq(t, p, "ListPermissions", map[string]any{
		"CertificateAuthorityArn": caARN,
	})
	require.NoError(t, json.Unmarshal(resp.Body, &listOut))
	perms = listOut["Permissions"].([]any)
	assert.Len(t, perms, 0)

	// GetPolicy — no policy yet
	resp = pcaReq(t, p, "GetPolicy", map[string]any{"ResourceArn": caARN})
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)

	// PutPolicy
	policy := `{"Version":"2012-10-17","Statement":[]}`
	resp = pcaReq(t, p, "PutPolicy", map[string]any{
		"ResourceArn": caARN,
		"Policy":      policy,
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// GetPolicy
	resp = pcaReq(t, p, "GetPolicy", map[string]any{"ResourceArn": caARN})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	var policyOut map[string]string
	require.NoError(t, json.Unmarshal(resp.Body, &policyOut))
	assert.Equal(t, policy, policyOut["Policy"])

	// DeletePolicy
	resp = pcaReq(t, p, "DeletePolicy", map[string]any{"ResourceArn": caARN})
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	resp = pcaReq(t, p, "GetPolicy", map[string]any{"ResourceArn": caARN})
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

func TestTags(t *testing.T) {
	p := newTestProvider(t)

	// Create CA
	resp := pcaReq(t, p, "CreateCertificateAuthority", map[string]any{
		"CertificateAuthorityType": "ROOT",
		"CertificateAuthorityConfiguration": map[string]any{
			"KeyAlgorithm":     "RSA_2048",
			"SigningAlgorithm": "SHA256WITHRSA",
			"Subject":          map[string]string{},
		},
	})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var caOut map[string]string
	require.NoError(t, json.Unmarshal(resp.Body, &caOut))
	caARN := caOut["CertificateAuthorityArn"]

	// TagCertificateAuthority
	resp = pcaReq(t, p, "TagCertificateAuthority", map[string]any{
		"CertificateAuthorityArn": caARN,
		"Tags": []map[string]string{
			{"Key": "env", "Value": "prod"},
			{"Key": "owner", "Value": "team-a"},
		},
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// ListTags
	resp = pcaReq(t, p, "ListTags", map[string]any{
		"CertificateAuthorityArn": caARN,
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	var listOut map[string]any
	require.NoError(t, json.Unmarshal(resp.Body, &listOut))
	tags := listOut["Tags"].([]any)
	require.Len(t, tags, 2)
	keys := make([]string, 0, 2)
	for _, tag := range tags {
		keys = append(keys, tag.(map[string]any)["Key"].(string))
	}
	assert.ElementsMatch(t, []string{"env", "owner"}, keys)

	// UntagCertificateAuthority
	resp = pcaReq(t, p, "UntagCertificateAuthority", map[string]any{
		"CertificateAuthorityArn": caARN,
		"Tags":                    []map[string]string{{"Key": "owner", "Value": "team-a"}},
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	resp = pcaReq(t, p, "ListTags", map[string]any{
		"CertificateAuthorityArn": caARN,
	})
	require.NoError(t, json.Unmarshal(resp.Body, &listOut))
	tags = listOut["Tags"].([]any)
	require.Len(t, tags, 1)
	assert.Equal(t, "env", tags[0].(map[string]any)["Key"])
}
