// SPDX-License-Identifier: Apache-2.0

// internal/services/acm/provider_test.go
package acm

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

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

func acmReq(t *testing.T, p *Provider, op string, body any) *plugin.Response {
	t.Helper()
	b, err := json.Marshal(body)
	require.NoError(t, err)
	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(b))
	req.Header.Set("X-Amz-Target", "CertificateManager."+op)
	resp, err := p.HandleRequest(context.Background(), op, req)
	require.NoError(t, err)
	return resp
}

func TestRequestAndDescribeCertificate(t *testing.T) {
	p := newTestProvider(t)

	// RequestCertificate
	resp := acmReq(t, p, "RequestCertificate", map[string]string{
		"DomainName": "example.com",
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var reqOut map[string]string
	require.NoError(t, json.Unmarshal(resp.Body, &reqOut))
	arn := reqOut["CertificateArn"]
	require.NotEmpty(t, arn)
	assert.True(t, strings.HasPrefix(arn, "arn:aws:acm:"))

	// DescribeCertificate
	resp = acmReq(t, p, "DescribeCertificate", map[string]string{"CertificateArn": arn})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	var descOut map[string]any
	require.NoError(t, json.Unmarshal(resp.Body, &descOut))
	cert := descOut["Certificate"].(map[string]any)
	assert.Equal(t, "example.com", cert["DomainName"])
	assert.Equal(t, "ISSUED", cert["Status"])

	// GetCertificate — should return PEM
	resp = acmReq(t, p, "GetCertificate", map[string]string{"CertificateArn": arn})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	var getOut map[string]string
	require.NoError(t, json.Unmarshal(resp.Body, &getOut))
	assert.Contains(t, getOut["Certificate"], "BEGIN CERTIFICATE")

	// ListCertificates
	resp = acmReq(t, p, "ListCertificates", map[string]any{})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	var listOut map[string]any
	require.NoError(t, json.Unmarshal(resp.Body, &listOut))
	summaries := listOut["CertificateSummaryList"].([]any)
	require.Len(t, summaries, 1)
	assert.Equal(t, arn, summaries[0].(map[string]any)["CertificateArn"])

	// DeleteCertificate
	resp = acmReq(t, p, "DeleteCertificate", map[string]string{"CertificateArn": arn})
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// DescribeCertificate after delete → error
	resp = acmReq(t, p, "DescribeCertificate", map[string]string{"CertificateArn": arn})
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

func makeTestCertPEM(t *testing.T, domain string) (certPEM, keyPEM string) {
	t.Helper()
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	require.NoError(t, err)
	tmpl := &x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{CommonName: domain},
		DNSNames:     []string{domain},
		NotBefore:    time.Now(),
		NotAfter:     time.Now().Add(365 * 24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	derBytes, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &priv.PublicKey, priv)
	require.NoError(t, err)
	certPEM = string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: derBytes}))
	privDER, err := x509.MarshalECPrivateKey(priv)
	require.NoError(t, err)
	keyPEM = string(pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: privDER}))
	return
}

func TestImportCertificate(t *testing.T) {
	p := newTestProvider(t)
	certPEM, keyPEM := makeTestCertPEM(t, "import.example.com")

	resp := acmReq(t, p, "ImportCertificate", map[string]any{
		"Certificate": []byte(certPEM),
		"PrivateKey":  []byte(keyPEM),
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var out map[string]string
	require.NoError(t, json.Unmarshal(resp.Body, &out))
	arn := out["CertificateArn"]
	require.NotEmpty(t, arn)

	// describe the imported cert
	resp = acmReq(t, p, "DescribeCertificate", map[string]string{"CertificateArn": arn})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	var descOut map[string]any
	require.NoError(t, json.Unmarshal(resp.Body, &descOut))
	cert := descOut["Certificate"].(map[string]any)
	assert.Equal(t, "IMPORTED", cert["Type"])
	assert.Equal(t, "import.example.com", cert["DomainName"])
}

func TestCertificateTags(t *testing.T) {
	p := newTestProvider(t)

	resp := acmReq(t, p, "RequestCertificate", map[string]string{
		"DomainName": "tags.example.com",
	})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var out map[string]string
	require.NoError(t, json.Unmarshal(resp.Body, &out))
	arn := out["CertificateArn"]

	// AddTags
	resp = acmReq(t, p, "AddTagsToCertificate", map[string]any{
		"CertificateArn": arn,
		"Tags": []map[string]string{
			{"Key": "env", "Value": "test"},
			{"Key": "owner", "Value": "alice"},
		},
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// ListTags
	resp = acmReq(t, p, "ListTagsForCertificate", map[string]string{"CertificateArn": arn})
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

	// RemoveTags
	resp = acmReq(t, p, "RemoveTagsFromCertificate", map[string]any{
		"CertificateArn": arn,
		"Tags":           []map[string]string{{"Key": "owner", "Value": "alice"}},
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	resp = acmReq(t, p, "ListTagsForCertificate", map[string]string{"CertificateArn": arn})
	require.NoError(t, json.Unmarshal(resp.Body, &listOut))
	tags = listOut["Tags"].([]any)
	require.Len(t, tags, 1)
	assert.Equal(t, "env", tags[0].(map[string]any)["Key"])
}
