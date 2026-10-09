// SPDX-License-Identifier: Apache-2.0

package iam

import (
	"context"
	"encoding/xml"
	"net/http"
	"net/url"
	"time"

	"github.com/skyoo2003/devcloud/internal/plugin"
)

// --- Response structs ---

type enableMFADeviceResponse struct {
	XMLName xml.Name `xml:"EnableMFADeviceResponse"`
}

type deactivateMFADeviceResponse struct {
	XMLName xml.Name `xml:"DeactivateMFADeviceResponse"`
}

type resyncMFADeviceResponse struct {
	XMLName xml.Name `xml:"ResyncMFADeviceResponse"`
}

type changePasswordResponse struct {
	XMLName xml.Name `xml:"ChangePasswordResponse"`
}

type resetServiceSpecificCredentialResponse struct {
	XMLName                              xml.Name                             `xml:"ResetServiceSpecificCredentialResponse"`
	ResetServiceSpecificCredentialResult resetServiceSpecificCredentialResult `xml:"ResetServiceSpecificCredentialResult"`
}

type resetServiceSpecificCredentialResult struct {
	ServiceSpecificCredential serviceSpecificCredentialXML `xml:"ServiceSpecificCredential"`
}

type serviceSpecificCredentialXML struct {
	UserName                    string `xml:"UserName"`
	Status                      string `xml:"Status"`
	ServiceUserName             string `xml:"ServiceUserName"`
	CreateDate                  string `xml:"CreateDate"`
	ServiceSpecificCredentialID string `xml:"ServiceSpecificCredentialId"`
	ServiceName                 string `xml:"ServiceName"`
}

type uploadSSHPublicKeyResponse struct {
	XMLName                  xml.Name                 `xml:"UploadSSHPublicKeyResponse"`
	UploadSSHPublicKeyResult uploadSSHPublicKeyResult `xml:"UploadSSHPublicKeyResult"`
}

type uploadSSHPublicKeyResult struct {
	SSHPublicKey sshPublicKeyXML `xml:"SSHPublicKey"`
}

type sshPublicKeyXML struct {
	UserName         string `xml:"UserName"`
	SSHPublicKeyID   string `xml:"SSHPublicKeyId"`
	SSHPublicKeyBody string `xml:"SSHPublicKeyBody"`
	Status           string `xml:"Status"`
	UploadDate       string `xml:"UploadDate"`
}

type uploadServerCertificateResponse struct {
	XMLName                       xml.Name                      `xml:"UploadServerCertificateResponse"`
	UploadServerCertificateResult uploadServerCertificateResult `xml:"UploadServerCertificateResult"`
}

type uploadServerCertificateResult struct {
	ServerCertificateMetadata serverCertificateMetadataXML `xml:"ServerCertificateMetadata"`
}

type serverCertificateMetadataXML struct {
	Path                  string `xml:"Path"`
	ServerCertificateName string `xml:"ServerCertificateName"`
	ServerCertificateID   string `xml:"ServerCertificateId"`
	Arn                   string `xml:"Arn"`
	UploadDate            string `xml:"UploadDate"`
}

type uploadSigningCertificateResponse struct {
	XMLName                        xml.Name                       `xml:"UploadSigningCertificateResponse"`
	UploadSigningCertificateResult uploadSigningCertificateResult `xml:"UploadSigningCertificateResult"`
}

type uploadSigningCertificateResult struct {
	Certificate signingCertificateXML `xml:"Certificate"`
}

type signingCertificateXML struct {
	UserName        string `xml:"UserName"`
	CertificateID   string `xml:"CertificateId"`
	CertificateBody string `xml:"CertificateBody"`
	Status          string `xml:"Status"`
	UploadDate      string `xml:"UploadDate"`
}

type setDefaultPolicyVersionResponse struct {
	XMLName xml.Name `xml:"SetDefaultPolicyVersionResponse"`
}

type setSecurityTokenServicePreferencesResponse struct {
	XMLName xml.Name `xml:"SetSecurityTokenServicePreferencesResponse"`
}

// --- Handlers ---

func (p *IAMProvider) handleEnableMFADevice(_ context.Context, form url.Values) (*plugin.Response, error) {
	userName := form.Get("UserName")
	serial := form.Get("SerialNumber")
	if userName == "" || serial == "" {
		return iamXMLError("MissingParameter", "UserName and SerialNumber are required", http.StatusBadRequest), nil
	}
	if err := p.store.EnableMFADevice(defaultAccountID, userName, serial); err != nil {
		return iamXMLError("InternalFailure", err.Error(), http.StatusInternalServerError), nil
	}
	return iamXMLResponse(http.StatusOK, enableMFADeviceResponse{})
}

func (p *IAMProvider) handleDeactivateMFADevice(_ context.Context, form url.Values) (*plugin.Response, error) {
	userName := form.Get("UserName")
	serial := form.Get("SerialNumber")
	if userName == "" || serial == "" {
		return iamXMLError("MissingParameter", "UserName and SerialNumber are required", http.StatusBadRequest), nil
	}
	if err := p.store.DeactivateMFADevice(defaultAccountID, userName, serial); err != nil {
		return iamXMLError("InternalFailure", err.Error(), http.StatusInternalServerError), nil
	}
	return iamXMLResponse(http.StatusOK, deactivateMFADeviceResponse{})
}

func (p *IAMProvider) handleResyncMFADevice(_ context.Context, form url.Values) (*plugin.Response, error) {
	userName := form.Get("UserName")
	serial := form.Get("SerialNumber")
	if userName == "" || serial == "" {
		return iamXMLError("MissingParameter", "UserName and SerialNumber are required", http.StatusBadRequest), nil
	}
	if err := p.store.ResyncMFADevice(defaultAccountID, userName, serial); err != nil {
		return iamXMLError("InternalFailure", err.Error(), http.StatusInternalServerError), nil
	}
	return iamXMLResponse(http.StatusOK, resyncMFADeviceResponse{})
}

func (p *IAMProvider) handleChangePassword(_ context.Context, form url.Values) (*plugin.Response, error) {
	oldPass := form.Get("OldPassword")
	newPass := form.Get("NewPassword")
	if oldPass == "" || newPass == "" {
		return iamXMLError("MissingParameter", "OldPassword and NewPassword are required", http.StatusBadRequest), nil
	}
	return iamXMLResponse(http.StatusOK, changePasswordResponse{})
}

func (p *IAMProvider) handleResetServiceSpecificCredential(_ context.Context, form url.Values) (*plugin.Response, error) {
	userName := form.Get("UserName")
	credID := form.Get("ServiceSpecificCredentialId")
	if credID == "" {
		var err error
		credID, err = generateID()
		if err != nil {
			return nil, err
		}
	}
	now := time.Now().UTC().Format(time.RFC3339)
	return iamXMLResponse(http.StatusOK, resetServiceSpecificCredentialResponse{
		ResetServiceSpecificCredentialResult: resetServiceSpecificCredentialResult{
			ServiceSpecificCredential: serviceSpecificCredentialXML{
				UserName:                    userName,
				Status:                      "Active",
				ServiceUserName:             userName,
				CreateDate:                  now,
				ServiceSpecificCredentialID: credID,
				ServiceName:                 "codecommit.amazonaws.com",
			},
		},
	})
}

func (p *IAMProvider) handleUploadSSHPublicKey(_ context.Context, form url.Values) (*plugin.Response, error) {
	userName := form.Get("UserName")
	keyBody := form.Get("SSHPublicKeyBody")
	if userName == "" || keyBody == "" {
		return iamXMLError("MissingParameter", "UserName and SSHPublicKeyBody are required", http.StatusBadRequest), nil
	}
	keyID, uploadDate, err := p.store.UploadSSHPublicKey(defaultAccountID, userName, keyBody)
	if err != nil {
		return iamXMLError("InternalFailure", err.Error(), http.StatusInternalServerError), nil
	}
	return iamXMLResponse(http.StatusOK, uploadSSHPublicKeyResponse{
		UploadSSHPublicKeyResult: uploadSSHPublicKeyResult{
			SSHPublicKey: sshPublicKeyXML{
				UserName:         userName,
				SSHPublicKeyID:   keyID,
				SSHPublicKeyBody: keyBody,
				Status:           "Active",
				UploadDate:       uploadDate.Format(time.RFC3339),
			},
		},
	})
}

func (p *IAMProvider) handleUploadServerCertificate(_ context.Context, form url.Values) (*plugin.Response, error) {
	name := form.Get("ServerCertificateName")
	certBody := form.Get("CertificateBody")
	path := form.Get("Path")
	if name == "" || certBody == "" {
		return iamXMLError("MissingParameter", "ServerCertificateName and CertificateBody are required", http.StatusBadRequest), nil
	}
	certID, arn, uploadDate, err := p.store.UploadServerCertificate(defaultAccountID, name, path, certBody)
	if err != nil {
		return iamXMLError("InternalFailure", err.Error(), http.StatusInternalServerError), nil
	}
	if path == "" {
		path = "/"
	}
	return iamXMLResponse(http.StatusOK, uploadServerCertificateResponse{
		UploadServerCertificateResult: uploadServerCertificateResult{
			ServerCertificateMetadata: serverCertificateMetadataXML{
				Path:                  path,
				ServerCertificateName: name,
				ServerCertificateID:   certID,
				Arn:                   arn,
				UploadDate:            uploadDate.Format(time.RFC3339),
			},
		},
	})
}

func (p *IAMProvider) handleUploadSigningCertificate(_ context.Context, form url.Values) (*plugin.Response, error) {
	userName := form.Get("UserName")
	certBody := form.Get("CertificateBody")
	if userName == "" || certBody == "" {
		return iamXMLError("MissingParameter", "UserName and CertificateBody are required", http.StatusBadRequest), nil
	}
	certID, uploadDate, err := p.store.UploadSigningCertificate(defaultAccountID, userName, certBody)
	if err != nil {
		return iamXMLError("InternalFailure", err.Error(), http.StatusInternalServerError), nil
	}
	return iamXMLResponse(http.StatusOK, uploadSigningCertificateResponse{
		UploadSigningCertificateResult: uploadSigningCertificateResult{
			Certificate: signingCertificateXML{
				UserName:        userName,
				CertificateID:   certID,
				CertificateBody: certBody,
				Status:          "Active",
				UploadDate:      uploadDate.Format(time.RFC3339),
			},
		},
	})
}

func (p *IAMProvider) handleSetDefaultPolicyVersion(_ context.Context, form url.Values) (*plugin.Response, error) {
	policyArn := form.Get("PolicyArn")
	versionID := form.Get("VersionId")
	if policyArn == "" || versionID == "" {
		return iamXMLError("MissingParameter", "PolicyArn and VersionId are required", http.StatusBadRequest), nil
	}
	if err := p.store.SetDefaultPolicyVersion(defaultAccountID, policyArn, versionID); err != nil {
		return iamXMLError("InternalFailure", err.Error(), http.StatusInternalServerError), nil
	}
	return iamXMLResponse(http.StatusOK, setDefaultPolicyVersionResponse{})
}

func (p *IAMProvider) handleSetSecurityTokenServicePreferences(_ context.Context, form url.Values) (*plugin.Response, error) {
	version := form.Get("GlobalEndpointTokenVersion")
	if version == "" {
		version = "v1Token"
	}
	if err := p.store.SetSTSPreferences(defaultAccountID, version); err != nil {
		return iamXMLError("InternalFailure", err.Error(), http.StatusInternalServerError), nil
	}
	return iamXMLResponse(http.StatusOK, setSecurityTokenServicePreferencesResponse{})
}
