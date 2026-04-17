// SPDX-License-Identifier: Apache-2.0

// internal/services/acm/provider.go
package acm

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"math/big"
	"net/http"
	"path/filepath"
	"strings"
	"time"

	"github.com/skyoo2003/devcloud/internal/plugin"
)

const defaultAccountID = plugin.DefaultAccountID
const defaultRegion = "us-east-1"

// Provider implements ACM using the JSON 1.1 protocol.
// Operations are dispatched via X-Amz-Target header (value passed as op).
type Provider struct {
	store *ACMStore
}

func (p *Provider) ServiceID() string             { return "acm" }
func (p *Provider) ServiceName() string           { return "CertificateManager" }
func (p *Provider) Protocol() plugin.ProtocolType { return plugin.ProtocolJSON11 }

func (p *Provider) Init(cfg plugin.PluginConfig) error {
	dataDir := cfg.DataDir
	if dataDir == "" {
		dataDir = "."
	}
	var err error
	p.store, err = NewACMStore(filepath.Join(dataDir, "acm"))
	return err
}

func (p *Provider) Shutdown(_ context.Context) error {
	if p.store != nil {
		return p.store.Close()
	}
	return nil
}

func (p *Provider) HandleRequest(_ context.Context, op string, req *http.Request) (*plugin.Response, error) {
	if op == "" {
		target := req.Header.Get("X-Amz-Target")
		op = strings.TrimPrefix(target, "CertificateManager.")
	}
	switch op {
	case "RequestCertificate":
		return p.requestCertificate(req)
	case "DescribeCertificate":
		return p.describeCertificate(req)
	case "ListCertificates":
		return p.listCertificates(req)
	case "DeleteCertificate":
		return p.deleteCertificate(req)
	case "ImportCertificate":
		return p.importCertificate(req)
	case "GetCertificate":
		return p.getCertificate(req)
	case "AddTagsToCertificate":
		return p.addTagsToCertificate(req)
	case "ListTagsForCertificate":
		return p.listTagsForCertificate(req)
	case "RemoveTagsFromCertificate":
		return p.removeTagsFromCertificate(req)
	case "RenewCertificate":
		return p.renewCertificate(req)
	case "UpdateCertificateOptions":
		return p.updateCertificateOptions(req)
	case "ResendValidationEmail":
		return p.resendValidationEmail(req)
	case "ExportCertificate":
		return p.exportCertificate(req)
	case "GetAccountConfiguration":
		return p.getAccountConfiguration(req)
	case "PutAccountConfiguration":
		return p.putAccountConfiguration(req)
	case "RevokeCertificate":
		return p.revokeCertificate(req)
	case "UpdateCertificate":
		return p.updateCertificate(req)
	case "DescribeCertificateAuthority":
		return jsonResp(http.StatusOK, map[string]any{})
	case "GetCertificateAuthorityCertificate":
		return jsonResp(http.StatusOK, map[string]any{})
	case "GetCertificateAuthorityCsr":
		return jsonResp(http.StatusOK, map[string]any{})
	case "ListCertificateAuthorities":
		return jsonResp(http.StatusOK, map[string]any{"CertificateAuthorities": []any{}})
	case "ListPermissions":
		return jsonResp(http.StatusOK, map[string]any{"Permissions": []any{}})
	case "CreatePermission":
		return jsonResp(http.StatusOK, map[string]any{})
	case "DeletePermission":
		return jsonResp(http.StatusOK, map[string]any{})
	case "StartCertificateIssuance":
		return jsonResp(http.StatusOK, map[string]any{})
	case "CancelCertificateIssuance":
		return jsonResp(http.StatusOK, map[string]any{})
	case "ListCertificateTransparency":
		return jsonResp(http.StatusOK, map[string]any{"Entries": []any{}})
	case "PutConfigurationRecorder":
		return jsonResp(http.StatusOK, map[string]any{})
	case "DescribeCertificateValidation":
		return jsonResp(http.StatusOK, map[string]any{"DomainValidationOptions": []any{}})
	case "WaitForValidation":
		return jsonResp(http.StatusOK, map[string]any{"Status": "SUCCESS"})
	case "GetCertificateRenewal":
		return jsonResp(http.StatusOK, map[string]any{"RenewalStatus": "SUCCESS"})
	case "ListIssuers":
		return jsonResp(http.StatusOK, map[string]any{"Issuers": []any{}})
	case "GetIssuerCertificate":
		return jsonResp(http.StatusOK, map[string]any{})
	case "GetCertificateChain":
		return jsonResp(http.StatusOK, map[string]any{"Chain": ""})
	case "GetCSR":
		return jsonResp(http.StatusOK, map[string]any{"Csr": ""})
	case "DescribeCertificateEvents":
		return jsonResp(http.StatusOK, map[string]any{"Events": []any{}})
	case "ListRenewalEvents":
		return jsonResp(http.StatusOK, map[string]any{"Events": []any{}})
	default:
		return acmError("InvalidAction", fmt.Sprintf("operation not implemented: %s", op), http.StatusBadRequest), nil
	}
}

func (p *Provider) ListResources(_ context.Context) ([]plugin.Resource, error) {
	certs, err := p.store.ListCertificates(defaultAccountID)
	if err != nil {
		return nil, err
	}
	out := make([]plugin.Resource, 0, len(certs))
	for _, c := range certs {
		out = append(out, plugin.Resource{Type: "certificate", ID: c.ARN, Name: c.DomainName})
	}
	return out, nil
}

func (p *Provider) GetMetrics(_ context.Context) (*plugin.ServiceMetrics, error) {
	return &plugin.ServiceMetrics{}, nil
}

// --- helpers ---

func arnForCert(accountID, region, id string) string {
	return fmt.Sprintf("arn:aws:acm:%s:%s:certificate/%s", region, accountID, id)
}

func acmError(code, msg string, status int) *plugin.Response {
	body, _ := json.Marshal(map[string]string{"__type": code, "message": msg})
	return &plugin.Response{
		StatusCode:  status,
		Body:        body,
		ContentType: "application/x-amz-json-1.1",
	}
}

func jsonResp(status int, v any) (*plugin.Response, error) {
	body, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	return &plugin.Response{
		StatusCode:  status,
		Body:        body,
		ContentType: "application/x-amz-json-1.1",
	}, nil
}

// generateSelfSigned creates a self-signed ECDSA P-256 certificate and returns
// certPEM, keyPEM, serial, notBefore, notAfter.
func generateSelfSigned(domain string) (certPEM, keyPEM, serial string, notBefore, notAfter time.Time, err error) {
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return
	}
	serialNumber, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return
	}
	notBefore = time.Now().UTC()
	notAfter = notBefore.Add(365 * 24 * time.Hour)
	serial = serialNumber.Text(16)

	tmpl := &x509.Certificate{
		SerialNumber: serialNumber,
		Subject:      pkix.Name{CommonName: domain},
		DNSNames:     []string{domain},
		NotBefore:    notBefore,
		NotAfter:     notAfter,
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	derBytes, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &priv.PublicKey, priv)
	if err != nil {
		return
	}
	certPEM = string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: derBytes}))

	privDER, err := x509.MarshalECPrivateKey(priv)
	if err != nil {
		return
	}
	keyPEM = string(pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: privDER}))
	return
}

// --- operation handlers ---

func (p *Provider) requestCertificate(req *http.Request) (*plugin.Response, error) {
	var in struct {
		DomainName   string `json:"DomainName"`
		KeyAlgorithm string `json:"KeyAlgorithm"`
	}
	if err := json.NewDecoder(req.Body).Decode(&in); err != nil {
		return acmError("ValidationException", "failed to parse request body", http.StatusBadRequest), nil
	}
	if in.DomainName == "" {
		return acmError("ValidationException", "DomainName is required", http.StatusBadRequest), nil
	}
	certPEM, keyPEM, serial, notBefore, notAfter, err := generateSelfSigned(in.DomainName)
	if err != nil {
		return nil, fmt.Errorf("generate certificate: %w", err)
	}
	// ensure UUID format: use rand for uniqueness
	idBytes := make([]byte, 16)
	_, _ = rand.Read(idBytes)
	id := fmt.Sprintf("%x-%x-%x-%x-%x",
		idBytes[0:4], idBytes[4:6], idBytes[6:8], idBytes[8:10], idBytes[10:16])

	arn := arnForCert(defaultAccountID, defaultRegion, id)
	keyAlgo := in.KeyAlgorithm
	if keyAlgo == "" {
		keyAlgo = "EC_prime256v1"
	}
	cert := &Certificate{
		ARN:        arn,
		DomainName: in.DomainName,
		AccountID:  defaultAccountID,
		Status:     "ISSUED",
		Type:       "AMAZON_ISSUED",
		KeyAlgo:    keyAlgo,
		Serial:     serial,
		NotBefore:  notBefore,
		NotAfter:   notAfter,
		CertPEM:    certPEM,
		KeyPEM:     keyPEM,
		CreatedAt:  time.Now(),
	}
	if err := p.store.PutCertificate(cert); err != nil {
		return nil, err
	}
	return jsonResp(http.StatusOK, map[string]string{"CertificateArn": arn})
}

func (p *Provider) describeCertificate(req *http.Request) (*plugin.Response, error) {
	var in struct {
		CertificateArn string `json:"CertificateArn"`
	}
	if err := json.NewDecoder(req.Body).Decode(&in); err != nil {
		return acmError("ValidationException", "failed to parse request", http.StatusBadRequest), nil
	}
	cert, err := p.store.GetCertificate(in.CertificateArn, defaultAccountID)
	if err != nil {
		if err == ErrCertNotFound {
			return acmError("ResourceNotFoundException", "certificate not found", http.StatusBadRequest), nil
		}
		return nil, err
	}
	detail := map[string]any{
		"CertificateArn":          cert.ARN,
		"DomainName":              cert.DomainName,
		"Status":                  cert.Status,
		"Type":                    cert.Type,
		"KeyAlgorithm":            cert.KeyAlgo,
		"Serial":                  cert.Serial,
		"NotBefore":               cert.NotBefore.Unix(),
		"NotAfter":                cert.NotAfter.Unix(),
		"IssuedAt":                cert.CreatedAt.Unix(),
		"CreatedAt":               cert.CreatedAt.Unix(),
		"SubjectAlternativeNames": []string{cert.DomainName},
		"Subject":                 "CN=" + cert.DomainName,
		"Issuer":                  "DevCloud",
		"SignatureAlgorithm":      "SHA256WITHECDSA",
	}
	return jsonResp(http.StatusOK, map[string]any{"Certificate": detail})
}

func (p *Provider) listCertificates(req *http.Request) (*plugin.Response, error) {
	certs, err := p.store.ListCertificates(defaultAccountID)
	if err != nil {
		return nil, err
	}
	summaries := make([]map[string]any, 0, len(certs))
	for _, c := range certs {
		summaries = append(summaries, map[string]any{
			"CertificateArn": c.ARN,
			"DomainName":     c.DomainName,
			"Status":         c.Status,
			"Type":           c.Type,
			"KeyAlgorithm":   c.KeyAlgo,
			"NotBefore":      c.NotBefore.Unix(),
			"NotAfter":       c.NotAfter.Unix(),
			"CreatedAt":      c.CreatedAt.Unix(),
		})
	}
	return jsonResp(http.StatusOK, map[string]any{"CertificateSummaryList": summaries})
}

func (p *Provider) deleteCertificate(req *http.Request) (*plugin.Response, error) {
	var in struct {
		CertificateArn string `json:"CertificateArn"`
	}
	if err := json.NewDecoder(req.Body).Decode(&in); err != nil {
		return acmError("ValidationException", "failed to parse request", http.StatusBadRequest), nil
	}
	if err := p.store.DeleteCertificate(in.CertificateArn, defaultAccountID); err != nil {
		if err == ErrCertNotFound {
			return acmError("ResourceNotFoundException", "certificate not found", http.StatusBadRequest), nil
		}
		return nil, err
	}
	return &plugin.Response{StatusCode: http.StatusOK, Body: []byte("{}"), ContentType: "application/x-amz-json-1.1"}, nil
}

func (p *Provider) importCertificate(req *http.Request) (*plugin.Response, error) {
	var in struct {
		CertificateArn   string `json:"CertificateArn"`
		Certificate      []byte `json:"Certificate"`
		PrivateKey       []byte `json:"PrivateKey"`
		CertificateChain []byte `json:"CertificateChain"`
	}
	if err := json.NewDecoder(req.Body).Decode(&in); err != nil {
		return acmError("ValidationException", "failed to parse request", http.StatusBadRequest), nil
	}
	certPEM := string(in.Certificate)
	keyPEM := string(in.PrivateKey)

	// parse to extract metadata
	var serial, domain string
	var notBefore, notAfter time.Time
	block, _ := pem.Decode(in.Certificate)
	if block != nil {
		if parsed, err := x509.ParseCertificate(block.Bytes); err == nil {
			serial = parsed.SerialNumber.Text(16)
			domain = parsed.Subject.CommonName
			if len(parsed.DNSNames) > 0 {
				domain = parsed.DNSNames[0]
			}
			notBefore = parsed.NotBefore
			notAfter = parsed.NotAfter
		}
	}

	arn := in.CertificateArn
	if arn == "" {
		idBytes := make([]byte, 16)
		_, _ = rand.Read(idBytes)
		id := fmt.Sprintf("%x-%x-%x-%x-%x",
			idBytes[0:4], idBytes[4:6], idBytes[6:8], idBytes[8:10], idBytes[10:16])
		arn = arnForCert(defaultAccountID, defaultRegion, id)
	}

	cert := &Certificate{
		ARN:        arn,
		DomainName: domain,
		AccountID:  defaultAccountID,
		Status:     "ISSUED",
		Type:       "IMPORTED",
		KeyAlgo:    "EC_prime256v1",
		Serial:     serial,
		NotBefore:  notBefore,
		NotAfter:   notAfter,
		CertPEM:    certPEM,
		KeyPEM:     keyPEM,
		CreatedAt:  time.Now(),
	}
	if err := p.store.PutCertificate(cert); err != nil {
		return nil, err
	}
	return jsonResp(http.StatusOK, map[string]string{"CertificateArn": arn})
}

func (p *Provider) getCertificate(req *http.Request) (*plugin.Response, error) {
	var in struct {
		CertificateArn string `json:"CertificateArn"`
	}
	if err := json.NewDecoder(req.Body).Decode(&in); err != nil {
		return acmError("ValidationException", "failed to parse request", http.StatusBadRequest), nil
	}
	cert, err := p.store.GetCertificate(in.CertificateArn, defaultAccountID)
	if err != nil {
		if err == ErrCertNotFound {
			return acmError("ResourceNotFoundException", "certificate not found", http.StatusBadRequest), nil
		}
		return nil, err
	}
	return jsonResp(http.StatusOK, map[string]string{
		"Certificate":      cert.CertPEM,
		"CertificateChain": cert.CertPEM, // self-signed: chain = cert itself
	})
}

func (p *Provider) addTagsToCertificate(req *http.Request) (*plugin.Response, error) {
	var in struct {
		CertificateArn string `json:"CertificateArn"`
		Tags           []struct {
			Key   string `json:"Key"`
			Value string `json:"Value"`
		} `json:"Tags"`
	}
	if err := json.NewDecoder(req.Body).Decode(&in); err != nil {
		return acmError("ValidationException", "failed to parse request", http.StatusBadRequest), nil
	}
	if _, err := p.store.GetCertificate(in.CertificateArn, defaultAccountID); err != nil {
		return acmError("ResourceNotFoundException", "certificate not found", http.StatusBadRequest), nil
	}
	tags := make([]CertTag, 0, len(in.Tags))
	for _, t := range in.Tags {
		tags = append(tags, CertTag{Key: t.Key, Value: t.Value})
	}
	if err := p.store.AddTags(in.CertificateArn, tags); err != nil {
		return nil, err
	}
	return &plugin.Response{StatusCode: http.StatusOK, Body: []byte("{}"), ContentType: "application/x-amz-json-1.1"}, nil
}

func (p *Provider) listTagsForCertificate(req *http.Request) (*plugin.Response, error) {
	var in struct {
		CertificateArn string `json:"CertificateArn"`
	}
	if err := json.NewDecoder(req.Body).Decode(&in); err != nil {
		return acmError("ValidationException", "failed to parse request", http.StatusBadRequest), nil
	}
	if _, err := p.store.GetCertificate(in.CertificateArn, defaultAccountID); err != nil {
		return acmError("ResourceNotFoundException", "certificate not found", http.StatusBadRequest), nil
	}
	tags, err := p.store.ListTags(in.CertificateArn)
	if err != nil {
		return nil, err
	}
	out := make([]map[string]string, 0, len(tags))
	for _, t := range tags {
		out = append(out, map[string]string{"Key": t.Key, "Value": t.Value})
	}
	return jsonResp(http.StatusOK, map[string]any{"Tags": out})
}

func (p *Provider) renewCertificate(req *http.Request) (*plugin.Response, error) {
	var in struct {
		CertificateArn string `json:"CertificateArn"`
	}
	if err := json.NewDecoder(req.Body).Decode(&in); err != nil {
		return acmError("ValidationException", "failed to parse request", http.StatusBadRequest), nil
	}
	if _, err := p.store.GetCertificate(in.CertificateArn, defaultAccountID); err != nil {
		if err == ErrCertNotFound {
			return acmError("ResourceNotFoundException", "certificate not found", http.StatusBadRequest), nil
		}
		return nil, err
	}
	// In the emulator, renew is a no-op since certificates are already self-signed and valid.
	return &plugin.Response{StatusCode: http.StatusOK, Body: []byte("{}"), ContentType: "application/x-amz-json-1.1"}, nil
}

func (p *Provider) removeTagsFromCertificate(req *http.Request) (*plugin.Response, error) {
	var in struct {
		CertificateArn string `json:"CertificateArn"`
		Tags           []struct {
			Key   string `json:"Key"`
			Value string `json:"Value"`
		} `json:"Tags"`
	}
	if err := json.NewDecoder(req.Body).Decode(&in); err != nil {
		return acmError("ValidationException", "failed to parse request", http.StatusBadRequest), nil
	}
	tags := make([]CertTag, 0, len(in.Tags))
	for _, t := range in.Tags {
		tags = append(tags, CertTag{Key: t.Key, Value: t.Value})
	}
	if err := p.store.RemoveTags(in.CertificateArn, tags); err != nil {
		return nil, err
	}
	return &plugin.Response{StatusCode: http.StatusOK, Body: []byte("{}"), ContentType: "application/x-amz-json-1.1"}, nil
}

func (p *Provider) updateCertificateOptions(req *http.Request) (*plugin.Response, error) {
	var in struct {
		CertificateArn string `json:"CertificateArn"`
		Options        struct {
			CertificateTransparencyLoggingPreference string `json:"CertificateTransparencyLoggingPreference"`
		} `json:"Options"`
	}
	if err := json.NewDecoder(req.Body).Decode(&in); err != nil {
		return acmError("ValidationException", "failed to parse request", http.StatusBadRequest), nil
	}
	if _, err := p.store.GetCertificate(in.CertificateArn, defaultAccountID); err != nil {
		if err == ErrCertNotFound {
			return acmError("ResourceNotFoundException", "certificate not found", http.StatusBadRequest), nil
		}
		return nil, err
	}
	return &plugin.Response{StatusCode: http.StatusOK, Body: []byte("{}"), ContentType: "application/x-amz-json-1.1"}, nil
}

func (p *Provider) resendValidationEmail(req *http.Request) (*plugin.Response, error) {
	var in struct {
		CertificateArn   string `json:"CertificateArn"`
		Domain           string `json:"Domain"`
		ValidationDomain string `json:"ValidationDomain"`
	}
	if err := json.NewDecoder(req.Body).Decode(&in); err != nil {
		return acmError("ValidationException", "failed to parse request", http.StatusBadRequest), nil
	}
	if _, err := p.store.GetCertificate(in.CertificateArn, defaultAccountID); err != nil {
		if err == ErrCertNotFound {
			return acmError("ResourceNotFoundException", "certificate not found", http.StatusBadRequest), nil
		}
		return nil, err
	}
	return &plugin.Response{StatusCode: http.StatusOK, Body: []byte("{}"), ContentType: "application/x-amz-json-1.1"}, nil
}

func (p *Provider) exportCertificate(req *http.Request) (*plugin.Response, error) {
	var in struct {
		CertificateArn string `json:"CertificateArn"`
		Passphrase     []byte `json:"Passphrase"`
	}
	if err := json.NewDecoder(req.Body).Decode(&in); err != nil {
		return acmError("ValidationException", "failed to parse request", http.StatusBadRequest), nil
	}
	cert, err := p.store.GetCertificate(in.CertificateArn, defaultAccountID)
	if err != nil {
		if err == ErrCertNotFound {
			return acmError("ResourceNotFoundException", "certificate not found", http.StatusBadRequest), nil
		}
		return nil, err
	}
	certPEM := cert.CertPEM
	keyPEM := cert.KeyPEM
	if certPEM == "" {
		certPEM = "-----BEGIN CERTIFICATE-----\nMOCK_CERT\n-----END CERTIFICATE-----"
	}
	if keyPEM == "" {
		keyPEM = "-----BEGIN RSA PRIVATE KEY-----\nMOCK_KEY\n-----END RSA PRIVATE KEY-----"
	}
	return jsonResp(http.StatusOK, map[string]string{
		"Certificate":      certPEM,
		"CertificateChain": "-----BEGIN CERTIFICATE-----\nMOCK_CHAIN\n-----END CERTIFICATE-----",
		"PrivateKey":       keyPEM,
	})
}

func (p *Provider) getAccountConfiguration(_ *http.Request) (*plugin.Response, error) {
	days, err := p.store.GetAccountConfig(defaultAccountID)
	if err != nil {
		return nil, err
	}
	return jsonResp(http.StatusOK, map[string]any{
		"ExpiryEvents": map[string]any{
			"DaysBeforeExpiry": days,
		},
	})
}

func (p *Provider) revokeCertificate(req *http.Request) (*plugin.Response, error) {
	var in struct {
		CertificateArn   string `json:"CertificateArn"`
		RevocationReason string `json:"RevocationReason"`
	}
	if err := json.NewDecoder(req.Body).Decode(&in); err != nil {
		return acmError("ValidationException", "failed to parse request", http.StatusBadRequest), nil
	}
	if in.CertificateArn == "" {
		return acmError("ValidationException", "CertificateArn is required", http.StatusBadRequest), nil
	}
	if _, err := p.store.GetCertificate(in.CertificateArn, defaultAccountID); err != nil {
		if err == ErrCertNotFound {
			return acmError("ResourceNotFoundException", "certificate not found", http.StatusBadRequest), nil
		}
		return nil, err
	}
	// Mark certificate as REVOKED by updating status.
	cert, _ := p.store.GetCertificate(in.CertificateArn, defaultAccountID)
	cert.Status = "REVOKED"
	if err := p.store.PutCertificate(cert); err != nil {
		return nil, err
	}
	return jsonResp(http.StatusOK, map[string]string{"CertificateArn": in.CertificateArn})
}

func (p *Provider) updateCertificate(req *http.Request) (*plugin.Response, error) {
	var in struct {
		CertificateArn string `json:"CertificateArn"`
	}
	if err := json.NewDecoder(req.Body).Decode(&in); err != nil {
		return acmError("ValidationException", "failed to parse request", http.StatusBadRequest), nil
	}
	if _, err := p.store.GetCertificate(in.CertificateArn, defaultAccountID); err != nil {
		if err == ErrCertNotFound {
			return acmError("ResourceNotFoundException", "certificate not found", http.StatusBadRequest), nil
		}
		return nil, err
	}
	return &plugin.Response{StatusCode: http.StatusOK, Body: []byte("{}"), ContentType: "application/x-amz-json-1.1"}, nil
}

func (p *Provider) putAccountConfiguration(req *http.Request) (*plugin.Response, error) {
	var in struct {
		ExpiryEvents struct {
			DaysBeforeExpiry int `json:"DaysBeforeExpiry"`
		} `json:"ExpiryEvents"`
		IdempotencyToken string `json:"IdempotencyToken"`
	}
	if err := json.NewDecoder(req.Body).Decode(&in); err != nil {
		return acmError("ValidationException", "failed to parse request", http.StatusBadRequest), nil
	}
	days := in.ExpiryEvents.DaysBeforeExpiry
	if days == 0 {
		days = 45
	}
	if err := p.store.PutAccountConfig(defaultAccountID, days); err != nil {
		return nil, err
	}
	return &plugin.Response{StatusCode: http.StatusOK, Body: []byte("{}"), ContentType: "application/x-amz-json-1.1"}, nil
}
