// SPDX-License-Identifier: Apache-2.0

package iam

import (
	"context"
	"net/http"
	"net/url"
	"strings"
	"testing"
)

func TestIAM_MFADeviceLifecycle(t *testing.T) {
	p := newTestIAMProvider(t)
	ctx := context.Background()

	// Enable MFA
	form := url.Values{
		"Action":              {"EnableMFADevice"},
		"UserName":            {"alice"},
		"SerialNumber":        {"arn:aws:iam::000000000000:mfa/alice-token"},
		"AuthenticationCode1": {"123456"},
		"AuthenticationCode2": {"654321"},
	}
	req, _ := http.NewRequestWithContext(ctx, "POST", "/", strings.NewReader(form.Encode()))
	resp, err := p.HandleRequest(ctx, "EnableMFADevice", req)
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("EnableMFADevice failed: %v, resp: %+v", err, resp)
	}

	// Resync MFA
	form.Set("Action", "ResyncMFADevice")
	req, _ = http.NewRequestWithContext(ctx, "POST", "/", strings.NewReader(form.Encode()))
	resp, err = p.HandleRequest(ctx, "ResyncMFADevice", req)
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("ResyncMFADevice failed: %v, resp: %+v", err, resp)
	}

	// Deactivate MFA
	form.Set("Action", "DeactivateMFADevice")
	req, _ = http.NewRequestWithContext(ctx, "POST", "/", strings.NewReader(form.Encode()))
	resp, err = p.HandleRequest(ctx, "DeactivateMFADevice", req)
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("DeactivateMFADevice failed: %v, resp: %+v", err, resp)
	}
}

func TestIAM_CredentialsAndCertificates(t *testing.T) {
	p := newTestIAMProvider(t)
	ctx := context.Background()

	// Change password
	form := url.Values{
		"OldPassword": {"old12345"},
		"NewPassword": {"new12345"},
	}
	req, _ := http.NewRequestWithContext(ctx, "POST", "/", strings.NewReader(form.Encode()))
	resp, err := p.HandleRequest(ctx, "ChangePassword", req)
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("ChangePassword failed: %v", err)
	}

	// Reset service specific cred
	form = url.Values{"UserName": {"alice"}}
	req, _ = http.NewRequestWithContext(ctx, "POST", "/", strings.NewReader(form.Encode()))
	resp, err = p.HandleRequest(ctx, "ResetServiceSpecificCredential", req)
	if err != nil || resp.StatusCode != http.StatusOK || !strings.Contains(string(resp.Body), "ServiceSpecificCredentialId") {
		t.Fatalf("ResetServiceSpecificCredential failed: %v, body: %s", err, string(resp.Body))
	}

	// Upload SSH key
	form = url.Values{
		"UserName":         {"alice"},
		"SSHPublicKeyBody": {"ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABAQC..."},
	}
	req, _ = http.NewRequestWithContext(ctx, "POST", "/", strings.NewReader(form.Encode()))
	resp, err = p.HandleRequest(ctx, "UploadSSHPublicKey", req)
	if err != nil || resp.StatusCode != http.StatusOK || !strings.Contains(string(resp.Body), "SSHPublicKeyId") {
		t.Fatalf("UploadSSHPublicKey failed: %v, body: %s", err, string(resp.Body))
	}

	// Upload Server Certificate
	form = url.Values{
		"ServerCertificateName": {"my-cert"},
		"CertificateBody":       {"-----BEGIN CERTIFICATE-----\nMIID...-----END CERTIFICATE-----"},
	}
	req, _ = http.NewRequestWithContext(ctx, "POST", "/", strings.NewReader(form.Encode()))
	resp, err = p.HandleRequest(ctx, "UploadServerCertificate", req)
	if err != nil || resp.StatusCode != http.StatusOK || !strings.Contains(string(resp.Body), "ServerCertificateId") {
		t.Fatalf("UploadServerCertificate failed: %v, body: %s", err, string(resp.Body))
	}

	// Upload Signing Certificate
	form = url.Values{
		"UserName":        {"alice"},
		"CertificateBody": {"-----BEGIN CERTIFICATE-----\nMIID...-----END CERTIFICATE-----"},
	}
	req, _ = http.NewRequestWithContext(ctx, "POST", "/", strings.NewReader(form.Encode()))
	resp, err = p.HandleRequest(ctx, "UploadSigningCertificate", req)
	if err != nil || resp.StatusCode != http.StatusOK || !strings.Contains(string(resp.Body), "CertificateId") {
		t.Fatalf("UploadSigningCertificate failed: %v, body: %s", err, string(resp.Body))
	}
}

func TestIAM_PolicyVersionsAndPreferences(t *testing.T) {
	p := newTestIAMProvider(t)
	ctx := context.Background()

	// SetDefaultPolicyVersion
	form := url.Values{
		"PolicyArn": {"arn:aws:iam::000000000000:policy/test"},
		"VersionId": {"v1"},
	}
	req, _ := http.NewRequestWithContext(ctx, "POST", "/", strings.NewReader(form.Encode()))
	resp, err := p.HandleRequest(ctx, "SetDefaultPolicyVersion", req)
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("SetDefaultPolicyVersion failed: %v", err)
	}

	// SetSecurityTokenServicePreferences
	form = url.Values{"GlobalEndpointTokenVersion": {"v2Token"}}
	req, _ = http.NewRequestWithContext(ctx, "POST", "/", strings.NewReader(form.Encode()))
	resp, err = p.HandleRequest(ctx, "SetSecurityTokenServicePreferences", req)
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("SetSecurityTokenServicePreferences failed: %v", err)
	}
}

func TestIAM_OIDCAndDelegation(t *testing.T) {
	p := newTestIAMProvider(t)
	ctx := context.Background()

	// AddClientIDToOpenIDConnectProvider
	form := url.Values{
		"OpenIDConnectProviderArn": {"arn:aws:iam::000000000000:oidc-provider/oidc.devcloud.local"},
		"ClientID":                 {"my-client-app"},
	}
	req, _ := http.NewRequestWithContext(ctx, "POST", "/", strings.NewReader(form.Encode()))
	resp, err := p.HandleRequest(ctx, "AddClientIDToOpenIDConnectProvider", req)
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("AddClientIDToOpenIDConnectProvider failed: %v", err)
	}

	// RemoveClientIDFromOpenIDConnectProvider
	req, _ = http.NewRequestWithContext(ctx, "POST", "/", strings.NewReader(form.Encode()))
	resp, err = p.HandleRequest(ctx, "RemoveClientIDFromOpenIDConnectProvider", req)
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("RemoveClientIDFromOpenIDConnectProvider failed: %v", err)
	}

	// AcceptDelegationRequest
	form = url.Values{"DelegationRequestId": {"del-12345"}}
	req, _ = http.NewRequestWithContext(ctx, "POST", "/", strings.NewReader(form.Encode()))
	resp, err = p.HandleRequest(ctx, "AcceptDelegationRequest", req)
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("AcceptDelegationRequest failed: %v", err)
	}

	// AssociateDelegationRequest
	form = url.Values{"DelegationRequestId": {"del-12345"}, "TargetAccount": {"111122223333"}}
	req, _ = http.NewRequestWithContext(ctx, "POST", "/", strings.NewReader(form.Encode()))
	resp, err = p.HandleRequest(ctx, "AssociateDelegationRequest", req)
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("AssociateDelegationRequest failed: %v", err)
	}

	// RejectDelegationRequest
	form = url.Values{"DelegationRequestId": {"del-12345"}}
	req, _ = http.NewRequestWithContext(ctx, "POST", "/", strings.NewReader(form.Encode()))
	resp, err = p.HandleRequest(ctx, "RejectDelegationRequest", req)
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("RejectDelegationRequest failed: %v", err)
	}

	// SendDelegationToken
	form = url.Values{"TargetAccount": {"111122223333"}, "Token": {"token-abc"}}
	req, _ = http.NewRequestWithContext(ctx, "POST", "/", strings.NewReader(form.Encode()))
	resp, err = p.HandleRequest(ctx, "SendDelegationToken", req)
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("SendDelegationToken failed: %v", err)
	}
}

func TestIAM_ReportsAndSimulation(t *testing.T) {
	p := newTestIAMProvider(t)
	ctx := context.Background()

	// GenerateCredentialReport
	req, _ := http.NewRequestWithContext(ctx, "POST", "/", strings.NewReader(""))
	resp, err := p.HandleRequest(ctx, "GenerateCredentialReport", req)
	if err != nil || resp.StatusCode != http.StatusOK || !strings.Contains(string(resp.Body), "COMPLETE") {
		t.Fatalf("GenerateCredentialReport failed: %v, body: %s", err, string(resp.Body))
	}

	// GenerateOrganizationsAccessReport
	resp, err = p.HandleRequest(ctx, "GenerateOrganizationsAccessReport", req)
	if err != nil || resp.StatusCode != http.StatusOK || !strings.Contains(string(resp.Body), "JobId") {
		t.Fatalf("GenerateOrganizationsAccessReport failed: %v, body: %s", err, string(resp.Body))
	}

	// GenerateServiceLastAccessedDetails
	resp, err = p.HandleRequest(ctx, "GenerateServiceLastAccessedDetails", req)
	if err != nil || resp.StatusCode != http.StatusOK || !strings.Contains(string(resp.Body), "JobId") {
		t.Fatalf("GenerateServiceLastAccessedDetails failed: %v, body: %s", err, string(resp.Body))
	}

	// SimulateCustomPolicy
	form := url.Values{
		"ActionNames.member.1": {"s3:GetObject"},
		"ActionNames.member.2": {"s3:PutObject"},
	}
	req, _ = http.NewRequestWithContext(ctx, "POST", "/", strings.NewReader(form.Encode()))
	resp, err = p.HandleRequest(ctx, "SimulateCustomPolicy", req)
	if err != nil || resp.StatusCode != http.StatusOK || !strings.Contains(string(resp.Body), "allowed") {
		t.Fatalf("SimulateCustomPolicy failed: %v, body: %s", err, string(resp.Body))
	}

	// SimulatePrincipalPolicy
	form.Set("PolicySourceArn", "arn:aws:iam::000000000000:role/my-role")
	req, _ = http.NewRequestWithContext(ctx, "POST", "/", strings.NewReader(form.Encode()))
	resp, err = p.HandleRequest(ctx, "SimulatePrincipalPolicy", req)
	if err != nil || resp.StatusCode != http.StatusOK || !strings.Contains(string(resp.Body), "allowed") {
		t.Fatalf("SimulatePrincipalPolicy failed: %v, body: %s", err, string(resp.Body))
	}
}

func TestIAM_OrganizationsRootSettings(t *testing.T) {
	p := newTestIAMProvider(t)
	ctx := context.Background()

	ops := []string{
		"EnableOrganizationsRootCredentialsManagement",
		"DisableOrganizationsRootCredentialsManagement",
		"EnableOrganizationsRootSessions",
		"DisableOrganizationsRootSessions",
		"EnableOutboundWebIdentityFederation",
		"DisableOutboundWebIdentityFederation",
	}

	for _, op := range ops {
		req, _ := http.NewRequestWithContext(ctx, "POST", "/", strings.NewReader(""))
		resp, err := p.HandleRequest(ctx, op, req)
		if err != nil || resp.StatusCode != http.StatusOK {
			t.Fatalf("%s failed: %v, resp: %+v", op, err, resp)
		}
	}
}

func TestSTS_FederationAndAdvancedOperations(t *testing.T) {
	stsP := newTestSTSProvider(t)
	ctx := context.Background()

	// AssumeRoleWithWebIdentity
	form := url.Values{
		"RoleArn":          {"arn:aws:iam::000000000000:role/k8s-role"},
		"RoleSessionName":  {"k8s-session"},
		"WebIdentityToken": {"eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.token"},
	}
	req, _ := http.NewRequestWithContext(ctx, "POST", "/", strings.NewReader(form.Encode()))
	resp, err := stsP.HandleRequest(ctx, "AssumeRoleWithWebIdentity", req)
	if err != nil || resp.StatusCode != http.StatusOK || !strings.Contains(string(resp.Body), "SubjectFromWebIdentityToken") {
		t.Fatalf("AssumeRoleWithWebIdentity failed: %v, body: %s", err, string(resp.Body))
	}

	// AssumeRoleWithSAML
	form = url.Values{
		"RoleArn":       {"arn:aws:iam::000000000000:role/saml-role"},
		"PrincipalArn":  {"arn:aws:iam::000000000000:saml-provider/okta"},
		"SAMLAssertion": {"PHNhbWxwOlJlc3BvbnNlPjwvc2FtbHA6UmVzcG9uc2U+"},
	}
	req, _ = http.NewRequestWithContext(ctx, "POST", "/", strings.NewReader(form.Encode()))
	resp, err = stsP.HandleRequest(ctx, "AssumeRoleWithSAML", req)
	if err != nil || resp.StatusCode != http.StatusOK || !strings.Contains(string(resp.Body), "Subject") {
		t.Fatalf("AssumeRoleWithSAML failed: %v, body: %s", err, string(resp.Body))
	}

	// AssumeRoot
	form = url.Values{"TargetPrincipal": {"000000000000"}}
	req, _ = http.NewRequestWithContext(ctx, "POST", "/", strings.NewReader(form.Encode()))
	resp, err = stsP.HandleRequest(ctx, "AssumeRoot", req)
	if err != nil || resp.StatusCode != http.StatusOK || !strings.Contains(string(resp.Body), "SourceIdentity") {
		t.Fatalf("AssumeRoot failed: %v, body: %s", err, string(resp.Body))
	}

	// DecodeAuthorizationMessage
	form = url.Values{"EncodedMessage": {"secret-auth-msg"}}
	req, _ = http.NewRequestWithContext(ctx, "POST", "/", strings.NewReader(form.Encode()))
	resp, err = stsP.HandleRequest(ctx, "DecodeAuthorizationMessage", req)
	if err != nil || resp.StatusCode != http.StatusOK || !strings.Contains(string(resp.Body), "DecodedMessage") {
		t.Fatalf("DecodeAuthorizationMessage failed: %v, body: %s", err, string(resp.Body))
	}

	// GetDelegatedAccessToken
	form = url.Values{"RoleArn": {"arn:aws:iam::000000000000:role/app-role"}}
	req, _ = http.NewRequestWithContext(ctx, "POST", "/", strings.NewReader(form.Encode()))
	resp, err = stsP.HandleRequest(ctx, "GetDelegatedAccessToken", req)
	if err != nil || resp.StatusCode != http.StatusOK || !strings.Contains(string(resp.Body), "DelegatedAccessToken") {
		t.Fatalf("GetDelegatedAccessToken failed: %v, body: %s", err, string(resp.Body))
	}

	// GetFederationToken
	form = url.Values{"Name": {"fed-user"}}
	req, _ = http.NewRequestWithContext(ctx, "POST", "/", strings.NewReader(form.Encode()))
	resp, err = stsP.HandleRequest(ctx, "GetFederationToken", req)
	if err != nil || resp.StatusCode != http.StatusOK || !strings.Contains(string(resp.Body), "FederatedUser") {
		t.Fatalf("GetFederationToken failed: %v, body: %s", err, string(resp.Body))
	}

	// GetWebIdentityToken
	req, _ = http.NewRequestWithContext(ctx, "POST", "/", strings.NewReader(""))
	resp, err = stsP.HandleRequest(ctx, "GetWebIdentityToken", req)
	if err != nil || resp.StatusCode != http.StatusOK || !strings.Contains(string(resp.Body), "WebIdentityToken") {
		t.Fatalf("GetWebIdentityToken failed: %v, body: %s", err, string(resp.Body))
	}
}
