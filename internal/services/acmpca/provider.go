// SPDX-License-Identifier: Apache-2.0

package acmpca

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

// Provider implements the ACMPrivateCA service using JSON 1.1 protocol.
type Provider struct {
	store *ACMPCAStore
}

func (p *Provider) ServiceID() string             { return "acmpca" }
func (p *Provider) ServiceName() string           { return "ACMPrivateCA" }
func (p *Provider) Protocol() plugin.ProtocolType { return plugin.ProtocolJSON11 }

func (p *Provider) Init(cfg plugin.PluginConfig) error {
	dataDir := cfg.DataDir
	if dataDir == "" {
		dataDir = "."
	}
	var err error
	p.store, err = NewACMPCAStore(filepath.Join(dataDir, "acmpca"))
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
		op = strings.TrimPrefix(target, "ACMPrivateCA.")
	}
	switch op {
	case "CreateCertificateAuthority":
		return p.createCertificateAuthority(req)
	case "DescribeCertificateAuthority":
		return p.describeCertificateAuthority(req)
	case "ListCertificateAuthorities":
		return p.listCertificateAuthorities(req)
	case "UpdateCertificateAuthority":
		return p.updateCertificateAuthority(req)
	case "DeleteCertificateAuthority":
		return p.deleteCertificateAuthority(req)
	case "RestoreCertificateAuthority":
		return p.restoreCertificateAuthority(req)
	case "GetCertificateAuthorityCsr":
		return p.getCertificateAuthorityCsr(req)
	case "GetCertificateAuthorityCertificate":
		return p.getCertificateAuthorityCertificate(req)
	case "ImportCertificateAuthorityCertificate":
		return p.importCertificateAuthorityCertificate(req)
	case "IssueCertificate":
		return p.issueCertificate(req)
	case "GetCertificate":
		return p.getCertificate(req)
	case "RevokeCertificate":
		return p.revokeCertificate(req)
	case "CreateCertificateAuthorityAuditReport":
		return p.createCertificateAuthorityAuditReport(req)
	case "DescribeCertificateAuthorityAuditReport":
		return p.describeCertificateAuthorityAuditReport(req)
	case "CreatePermission":
		return p.createPermission(req)
	case "ListPermissions":
		return p.listPermissions(req)
	case "DeletePermission":
		return p.deletePermission(req)
	case "GetPolicy":
		return p.getPolicy(req)
	case "PutPolicy":
		return p.putPolicy(req)
	case "DeletePolicy":
		return p.deletePolicy(req)
	case "TagCertificateAuthority":
		return p.tagCertificateAuthority(req)
	case "UntagCertificateAuthority":
		return p.untagCertificateAuthority(req)
	case "ListTags":
		return p.listTags(req)
	default:
		return pcaError("InvalidAction", fmt.Sprintf("operation not implemented: %s", op), http.StatusBadRequest), nil
	}
}

func (p *Provider) ListResources(_ context.Context) ([]plugin.Resource, error) {
	cas, err := p.store.ListCAs()
	if err != nil {
		return nil, err
	}
	out := make([]plugin.Resource, 0, len(cas))
	for _, ca := range cas {
		out = append(out, plugin.Resource{Type: "certificate-authority", ID: ca.ARN, Name: ca.ARN})
	}
	return out, nil
}

func (p *Provider) GetMetrics(_ context.Context) (*plugin.ServiceMetrics, error) {
	return &plugin.ServiceMetrics{}, nil
}

// --- helpers ---

func newID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

func arnForCA(accountID, region, id string) string {
	return fmt.Sprintf("arn:aws:acm-pca:%s:%s:certificate-authority/%s", region, accountID, id)
}

func arnForCert(caARN, id string) string {
	// extract the CA id portion
	parts := strings.Split(caARN, "/")
	caID := parts[len(parts)-1]
	return fmt.Sprintf("%s/certificate/%s", strings.Join(parts[:len(parts)-1], "/"), caID+"-"+id)
}

func pcaError(code, msg string, status int) *plugin.Response {
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

func emptyOK() (*plugin.Response, error) {
	return &plugin.Response{
		StatusCode:  http.StatusOK,
		Body:        []byte("{}"),
		ContentType: "application/x-amz-json-1.1",
	}, nil
}

// generateSelfSignedCA creates a self-signed root CA certificate.
func generateSelfSignedCA(subject map[string]string) (certPEM, csrPEM, serial string, err error) {
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return
	}
	serialNumber, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return
	}
	serial = serialNumber.Text(16)

	cn := subject["commonName"]
	if cn == "" {
		cn = "DevCloud Private CA"
	}
	tmpl := &x509.Certificate{
		SerialNumber:          serialNumber,
		Subject:               pkix.Name{CommonName: cn},
		NotBefore:             time.Now().UTC(),
		NotAfter:              time.Now().UTC().Add(10 * 365 * 24 * time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
	}
	derBytes, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &priv.PublicKey, priv)
	if err != nil {
		return
	}
	certPEM = string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: derBytes}))

	// Generate a dummy CSR
	csrBytes, err := x509.CreateCertificateRequest(rand.Reader, &x509.CertificateRequest{
		Subject: pkix.Name{CommonName: cn},
	}, priv)
	if err != nil {
		return
	}
	csrPEM = string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE REQUEST", Bytes: csrBytes}))
	return
}

// issueDummyCert generates a dummy self-signed leaf certificate.
// A real CA would sign this; here we produce a standalone cert for emulation.
func issueDummyCert(_ *CertificateAuthority) (certPEM string, err error) {
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return
	}
	serialNumber, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return
	}

	tmpl := &x509.Certificate{
		SerialNumber: serialNumber,
		Subject:      pkix.Name{CommonName: "issued-by-devcloud"},
		NotBefore:    time.Now().UTC(),
		NotAfter:     time.Now().UTC().Add(365 * 24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}

	derBytes, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &priv.PublicKey, priv)
	if err != nil {
		return
	}
	certPEM = string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: derBytes}))
	return
}

func caToMap(ca *CertificateAuthority) map[string]any {
	return map[string]any{
		"Arn":       ca.ARN,
		"Serial":    ca.Serial,
		"Status":    ca.Status,
		"Type":      ca.Type,
		"UsageMode": ca.UsageMode,
		"CreatedAt": ca.CreatedAt.Unix(),
		"CertificateAuthorityConfiguration": map[string]any{
			"KeyAlgorithm":     ca.KeyAlgo,
			"SigningAlgorithm": ca.SigningAlgo,
			"Subject":          ca.Subject,
		},
	}
}

// --- operation handlers ---

func (p *Provider) createCertificateAuthority(req *http.Request) (*plugin.Response, error) {
	var in struct {
		CertificateAuthorityType          string `json:"CertificateAuthorityType"`
		UsageMode                         string `json:"UsageMode"`
		CertificateAuthorityConfiguration *struct {
			KeyAlgorithm     string            `json:"KeyAlgorithm"`
			SigningAlgorithm string            `json:"SigningAlgorithm"`
			Subject          map[string]string `json:"Subject"`
		} `json:"CertificateAuthorityConfiguration"`
	}
	if err := json.NewDecoder(req.Body).Decode(&in); err != nil {
		return pcaError("ValidationException", "failed to parse request body", http.StatusBadRequest), nil
	}

	id := newID()
	arn := arnForCA(defaultAccountID, defaultRegion, id)

	caType := in.CertificateAuthorityType
	if caType == "" {
		caType = "ROOT"
	}
	usageMode := in.UsageMode
	if usageMode == "" {
		usageMode = "SHORT_LIVED_CERTIFICATE"
	}
	keyAlgo := "RSA_2048"
	signingAlgo := "SHA256WITHRSA"
	subject := map[string]string{}
	if in.CertificateAuthorityConfiguration != nil {
		if in.CertificateAuthorityConfiguration.KeyAlgorithm != "" {
			keyAlgo = in.CertificateAuthorityConfiguration.KeyAlgorithm
		}
		if in.CertificateAuthorityConfiguration.SigningAlgorithm != "" {
			signingAlgo = in.CertificateAuthorityConfiguration.SigningAlgorithm
		}
		if in.CertificateAuthorityConfiguration.Subject != nil {
			subject = in.CertificateAuthorityConfiguration.Subject
		}
	}

	certPEM, csrPEM, serial, err := generateSelfSignedCA(subject)
	if err != nil {
		return nil, fmt.Errorf("generate CA: %w", err)
	}

	ca := &CertificateAuthority{
		ARN:         arn,
		Serial:      serial,
		Type:        caType,
		KeyAlgo:     keyAlgo,
		SigningAlgo: signingAlgo,
		Subject:     subject,
		Status:      "ACTIVE",
		UsageMode:   usageMode,
		CSR:         csrPEM,
		CertPEM:     certPEM,
		CertChain:   certPEM,
		CreatedAt:   time.Now(),
	}
	if err := p.store.PutCA(ca); err != nil {
		return nil, err
	}
	return jsonResp(http.StatusOK, map[string]string{"CertificateAuthorityArn": arn})
}

func (p *Provider) describeCertificateAuthority(req *http.Request) (*plugin.Response, error) {
	var in struct {
		CertificateAuthorityArn string `json:"CertificateAuthorityArn"`
	}
	if err := json.NewDecoder(req.Body).Decode(&in); err != nil {
		return pcaError("ValidationException", "failed to parse request", http.StatusBadRequest), nil
	}
	ca, err := p.store.GetCA(in.CertificateAuthorityArn)
	if err != nil {
		if err == ErrCANotFound {
			return pcaError("ResourceNotFoundException", "certificate authority not found", http.StatusBadRequest), nil
		}
		return nil, err
	}
	return jsonResp(http.StatusOK, map[string]any{"CertificateAuthority": caToMap(ca)})
}

func (p *Provider) listCertificateAuthorities(req *http.Request) (*plugin.Response, error) {
	cas, err := p.store.ListCAs()
	if err != nil {
		return nil, err
	}
	items := make([]map[string]any, 0, len(cas))
	for _, ca := range cas {
		items = append(items, caToMap(&ca))
	}
	return jsonResp(http.StatusOK, map[string]any{"CertificateAuthorities": items})
}

func (p *Provider) updateCertificateAuthority(req *http.Request) (*plugin.Response, error) {
	var in struct {
		CertificateAuthorityArn string `json:"CertificateAuthorityArn"`
		Status                  string `json:"Status"`
	}
	if err := json.NewDecoder(req.Body).Decode(&in); err != nil {
		return pcaError("ValidationException", "failed to parse request", http.StatusBadRequest), nil
	}
	ca, err := p.store.GetCA(in.CertificateAuthorityArn)
	if err != nil {
		if err == ErrCANotFound {
			return pcaError("ResourceNotFoundException", "certificate authority not found", http.StatusBadRequest), nil
		}
		return nil, err
	}
	if in.Status != "" {
		ca.Status = in.Status
	}
	if err := p.store.PutCA(ca); err != nil {
		return nil, err
	}
	return emptyOK()
}

func (p *Provider) deleteCertificateAuthority(req *http.Request) (*plugin.Response, error) {
	var in struct {
		CertificateAuthorityArn string `json:"CertificateAuthorityArn"`
	}
	if err := json.NewDecoder(req.Body).Decode(&in); err != nil {
		return pcaError("ValidationException", "failed to parse request", http.StatusBadRequest), nil
	}
	ca, err := p.store.GetCA(in.CertificateAuthorityArn)
	if err != nil {
		if err == ErrCANotFound {
			return pcaError("ResourceNotFoundException", "certificate authority not found", http.StatusBadRequest), nil
		}
		return nil, err
	}
	ca.Status = "DELETED"
	if err := p.store.PutCA(ca); err != nil {
		return nil, err
	}
	return emptyOK()
}

func (p *Provider) restoreCertificateAuthority(req *http.Request) (*plugin.Response, error) {
	var in struct {
		CertificateAuthorityArn string `json:"CertificateAuthorityArn"`
	}
	if err := json.NewDecoder(req.Body).Decode(&in); err != nil {
		return pcaError("ValidationException", "failed to parse request", http.StatusBadRequest), nil
	}
	ca, err := p.store.GetCA(in.CertificateAuthorityArn)
	if err != nil {
		if err == ErrCANotFound {
			return pcaError("ResourceNotFoundException", "certificate authority not found", http.StatusBadRequest), nil
		}
		return nil, err
	}
	ca.Status = "ACTIVE"
	if err := p.store.PutCA(ca); err != nil {
		return nil, err
	}
	return emptyOK()
}

func (p *Provider) getCertificateAuthorityCsr(req *http.Request) (*plugin.Response, error) {
	var in struct {
		CertificateAuthorityArn string `json:"CertificateAuthorityArn"`
	}
	if err := json.NewDecoder(req.Body).Decode(&in); err != nil {
		return pcaError("ValidationException", "failed to parse request", http.StatusBadRequest), nil
	}
	ca, err := p.store.GetCA(in.CertificateAuthorityArn)
	if err != nil {
		if err == ErrCANotFound {
			return pcaError("ResourceNotFoundException", "certificate authority not found", http.StatusBadRequest), nil
		}
		return nil, err
	}
	return jsonResp(http.StatusOK, map[string]string{"Csr": ca.CSR})
}

func (p *Provider) getCertificateAuthorityCertificate(req *http.Request) (*plugin.Response, error) {
	var in struct {
		CertificateAuthorityArn string `json:"CertificateAuthorityArn"`
	}
	if err := json.NewDecoder(req.Body).Decode(&in); err != nil {
		return pcaError("ValidationException", "failed to parse request", http.StatusBadRequest), nil
	}
	ca, err := p.store.GetCA(in.CertificateAuthorityArn)
	if err != nil {
		if err == ErrCANotFound {
			return pcaError("ResourceNotFoundException", "certificate authority not found", http.StatusBadRequest), nil
		}
		return nil, err
	}
	return jsonResp(http.StatusOK, map[string]string{
		"Certificate":      ca.CertPEM,
		"CertificateChain": ca.CertChain,
	})
}

func (p *Provider) importCertificateAuthorityCertificate(req *http.Request) (*plugin.Response, error) {
	var in struct {
		CertificateAuthorityArn string `json:"CertificateAuthorityArn"`
		Certificate             []byte `json:"Certificate"`
		CertificateChain        []byte `json:"CertificateChain"`
	}
	if err := json.NewDecoder(req.Body).Decode(&in); err != nil {
		return pcaError("ValidationException", "failed to parse request", http.StatusBadRequest), nil
	}
	ca, err := p.store.GetCA(in.CertificateAuthorityArn)
	if err != nil {
		if err == ErrCANotFound {
			return pcaError("ResourceNotFoundException", "certificate authority not found", http.StatusBadRequest), nil
		}
		return nil, err
	}
	if len(in.Certificate) > 0 {
		ca.CertPEM = string(in.Certificate)
	}
	if len(in.CertificateChain) > 0 {
		ca.CertChain = string(in.CertificateChain)
	}
	if err := p.store.PutCA(ca); err != nil {
		return nil, err
	}
	return emptyOK()
}

func (p *Provider) issueCertificate(req *http.Request) (*plugin.Response, error) {
	var in struct {
		CertificateAuthorityArn string `json:"CertificateAuthorityArn"`
		SigningAlgorithm        string `json:"SigningAlgorithm"`
	}
	if err := json.NewDecoder(req.Body).Decode(&in); err != nil {
		return pcaError("ValidationException", "failed to parse request", http.StatusBadRequest), nil
	}
	ca, err := p.store.GetCA(in.CertificateAuthorityArn)
	if err != nil {
		if err == ErrCANotFound {
			return pcaError("ResourceNotFoundException", "certificate authority not found", http.StatusBadRequest), nil
		}
		return nil, err
	}

	certPEM, err := issueDummyCert(ca)
	if err != nil {
		return nil, fmt.Errorf("issue certificate: %w", err)
	}

	certID := newID()
	certARN := arnForCert(ca.ARN, certID)
	cert := &IssuedCertificate{
		ARN:       certARN,
		CAARN:     ca.ARN,
		CertPEM:   certPEM,
		CertChain: ca.CertPEM,
		CreatedAt: time.Now(),
	}
	if err := p.store.PutCert(cert); err != nil {
		return nil, err
	}
	return jsonResp(http.StatusOK, map[string]string{"CertificateArn": certARN})
}

func (p *Provider) getCertificate(req *http.Request) (*plugin.Response, error) {
	var in struct {
		CertificateAuthorityArn string `json:"CertificateAuthorityArn"`
		CertificateArn          string `json:"CertificateArn"`
	}
	if err := json.NewDecoder(req.Body).Decode(&in); err != nil {
		return pcaError("ValidationException", "failed to parse request", http.StatusBadRequest), nil
	}
	cert, err := p.store.GetCert(in.CertificateArn)
	if err != nil {
		if err == ErrCertNotFound {
			return pcaError("ResourceNotFoundException", "certificate not found", http.StatusBadRequest), nil
		}
		return nil, err
	}
	return jsonResp(http.StatusOK, map[string]string{
		"Certificate":      cert.CertPEM,
		"CertificateChain": cert.CertChain,
	})
}

func (p *Provider) revokeCertificate(req *http.Request) (*plugin.Response, error) {
	var in struct {
		CertificateAuthorityArn string `json:"CertificateAuthorityArn"`
		CertificateSerial       string `json:"CertificateSerial"`
		RevocationReason        string `json:"RevocationReason"`
	}
	if err := json.NewDecoder(req.Body).Decode(&in); err != nil {
		return pcaError("ValidationException", "failed to parse request", http.StatusBadRequest), nil
	}
	// Verify the CA exists
	if _, err := p.store.GetCA(in.CertificateAuthorityArn); err != nil {
		if err == ErrCANotFound {
			return pcaError("ResourceNotFoundException", "certificate authority not found", http.StatusBadRequest), nil
		}
		return nil, err
	}
	return emptyOK()
}

func (p *Provider) createCertificateAuthorityAuditReport(req *http.Request) (*plugin.Response, error) {
	var in struct {
		CertificateAuthorityArn   string `json:"CertificateAuthorityArn"`
		S3BucketName              string `json:"S3BucketName"`
		AuditReportResponseFormat string `json:"AuditReportResponseFormat"`
	}
	if err := json.NewDecoder(req.Body).Decode(&in); err != nil {
		return pcaError("ValidationException", "failed to parse request", http.StatusBadRequest), nil
	}
	if _, err := p.store.GetCA(in.CertificateAuthorityArn); err != nil {
		if err == ErrCANotFound {
			return pcaError("ResourceNotFoundException", "certificate authority not found", http.StatusBadRequest), nil
		}
		return nil, err
	}
	auditID := newID()
	return jsonResp(http.StatusOK, map[string]string{
		"AuditReportId": auditID,
		"S3Key":         "audit/" + auditID + ".json",
	})
}

func (p *Provider) describeCertificateAuthorityAuditReport(req *http.Request) (*plugin.Response, error) {
	var in struct {
		CertificateAuthorityArn string `json:"CertificateAuthorityArn"`
		AuditReportId           string `json:"AuditReportId"`
	}
	if err := json.NewDecoder(req.Body).Decode(&in); err != nil {
		return pcaError("ValidationException", "failed to parse request", http.StatusBadRequest), nil
	}
	return jsonResp(http.StatusOK, map[string]any{
		"AuditReportStatus": "SUCCESS",
		"S3BucketName":      "devcloud-audit-bucket",
		"S3Key":             "audit/" + in.AuditReportId + ".json",
		"CreatedAt":         time.Now().Unix(),
	})
}

func (p *Provider) createPermission(req *http.Request) (*plugin.Response, error) {
	var in struct {
		CertificateAuthorityArn string   `json:"CertificateAuthorityArn"`
		Principal               string   `json:"Principal"`
		Actions                 []string `json:"Actions"`
		SourceAccount           string   `json:"SourceAccount"`
	}
	if err := json.NewDecoder(req.Body).Decode(&in); err != nil {
		return pcaError("ValidationException", "failed to parse request", http.StatusBadRequest), nil
	}
	if _, err := p.store.GetCA(in.CertificateAuthorityArn); err != nil {
		if err == ErrCANotFound {
			return pcaError("ResourceNotFoundException", "certificate authority not found", http.StatusBadRequest), nil
		}
		return nil, err
	}
	perm := &Permission{
		CAARN:     in.CertificateAuthorityArn,
		Principal: in.Principal,
		Actions:   in.Actions,
	}
	if err := p.store.PutPermission(perm); err != nil {
		return nil, err
	}
	return emptyOK()
}

func (p *Provider) listPermissions(req *http.Request) (*plugin.Response, error) {
	var in struct {
		CertificateAuthorityArn string `json:"CertificateAuthorityArn"`
	}
	if err := json.NewDecoder(req.Body).Decode(&in); err != nil {
		return pcaError("ValidationException", "failed to parse request", http.StatusBadRequest), nil
	}
	perms, err := p.store.ListPermissions(in.CertificateAuthorityArn)
	if err != nil {
		return nil, err
	}
	items := make([]map[string]any, 0, len(perms))
	for _, perm := range perms {
		items = append(items, map[string]any{
			"CertificateAuthorityArn": perm.CAARN,
			"Principal":               perm.Principal,
			"Actions":                 perm.Actions,
		})
	}
	return jsonResp(http.StatusOK, map[string]any{"Permissions": items})
}

func (p *Provider) deletePermission(req *http.Request) (*plugin.Response, error) {
	var in struct {
		CertificateAuthorityArn string `json:"CertificateAuthorityArn"`
		Principal               string `json:"Principal"`
	}
	if err := json.NewDecoder(req.Body).Decode(&in); err != nil {
		return pcaError("ValidationException", "failed to parse request", http.StatusBadRequest), nil
	}
	if err := p.store.DeletePermission(in.CertificateAuthorityArn, in.Principal); err != nil {
		if err == ErrPermNotFound {
			return pcaError("ResourceNotFoundException", "permission not found", http.StatusBadRequest), nil
		}
		return nil, err
	}
	return emptyOK()
}

func (p *Provider) getPolicy(req *http.Request) (*plugin.Response, error) {
	var in struct {
		ResourceArn string `json:"ResourceArn"`
	}
	if err := json.NewDecoder(req.Body).Decode(&in); err != nil {
		return pcaError("ValidationException", "failed to parse request", http.StatusBadRequest), nil
	}
	ca, err := p.store.GetCA(in.ResourceArn)
	if err != nil {
		if err == ErrCANotFound {
			return pcaError("ResourceNotFoundException", "certificate authority not found", http.StatusBadRequest), nil
		}
		return nil, err
	}
	if ca.Policy == "" {
		return pcaError("ResourceNotFoundException", "no policy found", http.StatusBadRequest), nil
	}
	return jsonResp(http.StatusOK, map[string]string{"Policy": ca.Policy})
}

func (p *Provider) putPolicy(req *http.Request) (*plugin.Response, error) {
	var in struct {
		ResourceArn string `json:"ResourceArn"`
		Policy      string `json:"Policy"`
	}
	if err := json.NewDecoder(req.Body).Decode(&in); err != nil {
		return pcaError("ValidationException", "failed to parse request", http.StatusBadRequest), nil
	}
	ca, err := p.store.GetCA(in.ResourceArn)
	if err != nil {
		if err == ErrCANotFound {
			return pcaError("ResourceNotFoundException", "certificate authority not found", http.StatusBadRequest), nil
		}
		return nil, err
	}
	ca.Policy = in.Policy
	if err := p.store.PutCA(ca); err != nil {
		return nil, err
	}
	return emptyOK()
}

func (p *Provider) deletePolicy(req *http.Request) (*plugin.Response, error) {
	var in struct {
		ResourceArn string `json:"ResourceArn"`
	}
	if err := json.NewDecoder(req.Body).Decode(&in); err != nil {
		return pcaError("ValidationException", "failed to parse request", http.StatusBadRequest), nil
	}
	ca, err := p.store.GetCA(in.ResourceArn)
	if err != nil {
		if err == ErrCANotFound {
			return pcaError("ResourceNotFoundException", "certificate authority not found", http.StatusBadRequest), nil
		}
		return nil, err
	}
	ca.Policy = ""
	if err := p.store.PutCA(ca); err != nil {
		return nil, err
	}
	return emptyOK()
}

func (p *Provider) tagCertificateAuthority(req *http.Request) (*plugin.Response, error) {
	var in struct {
		CertificateAuthorityArn string `json:"CertificateAuthorityArn"`
		Tags                    []struct {
			Key   string `json:"Key"`
			Value string `json:"Value"`
		} `json:"Tags"`
	}
	if err := json.NewDecoder(req.Body).Decode(&in); err != nil {
		return pcaError("ValidationException", "failed to parse request", http.StatusBadRequest), nil
	}
	if _, err := p.store.GetCA(in.CertificateAuthorityArn); err != nil {
		return pcaError("ResourceNotFoundException", "certificate authority not found", http.StatusBadRequest), nil
	}
	tags := make([]Tag, 0, len(in.Tags))
	for _, t := range in.Tags {
		tags = append(tags, Tag{Key: t.Key, Value: t.Value})
	}
	if err := p.store.AddTags(in.CertificateAuthorityArn, tags); err != nil {
		return nil, err
	}
	return emptyOK()
}

func (p *Provider) untagCertificateAuthority(req *http.Request) (*plugin.Response, error) {
	var in struct {
		CertificateAuthorityArn string `json:"CertificateAuthorityArn"`
		Tags                    []struct {
			Key   string `json:"Key"`
			Value string `json:"Value"`
		} `json:"Tags"`
	}
	if err := json.NewDecoder(req.Body).Decode(&in); err != nil {
		return pcaError("ValidationException", "failed to parse request", http.StatusBadRequest), nil
	}
	tags := make([]Tag, 0, len(in.Tags))
	for _, t := range in.Tags {
		tags = append(tags, Tag{Key: t.Key, Value: t.Value})
	}
	if err := p.store.RemoveTags(in.CertificateAuthorityArn, tags); err != nil {
		return nil, err
	}
	return emptyOK()
}

func (p *Provider) listTags(req *http.Request) (*plugin.Response, error) {
	var in struct {
		CertificateAuthorityArn string `json:"CertificateAuthorityArn"`
	}
	if err := json.NewDecoder(req.Body).Decode(&in); err != nil {
		return pcaError("ValidationException", "failed to parse request", http.StatusBadRequest), nil
	}
	tags, err := p.store.ListTags(in.CertificateAuthorityArn)
	if err != nil {
		return nil, err
	}
	items := make([]map[string]string, 0, len(tags))
	for _, t := range tags {
		items = append(items, map[string]string{"Key": t.Key, "Value": t.Value})
	}
	return jsonResp(http.StatusOK, map[string]any{"Tags": items})
}
