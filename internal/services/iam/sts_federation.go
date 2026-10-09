// SPDX-License-Identifier: Apache-2.0

package iam

import (
	"context"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"net/http"
	"net/url"
	"time"

	"github.com/skyoo2003/devcloud/internal/plugin"
)

// --- Response structs ---

type assumeRoleWithWebIdentityResponse struct {
	XMLName                         xml.Name                        `xml:"AssumeRoleWithWebIdentityResponse"`
	AssumeRoleWithWebIdentityResult assumeRoleWithWebIdentityResult `xml:"AssumeRoleWithWebIdentityResult"`
}

type assumeRoleWithWebIdentityResult struct {
	Credentials                 stsCredentials  `xml:"Credentials"`
	SubjectFromWebIdentityToken string          `xml:"SubjectFromWebIdentityToken"`
	AssumedRoleUser             assumedRoleUser `xml:"AssumedRoleUser"`
	PackedPolicySize            int             `xml:"PackedPolicySize"`
	Provider                    string          `xml:"Provider"`
	Audience                    string          `xml:"Audience"`
}

type assumeRoleWithSAMLResponse struct {
	XMLName                  xml.Name                 `xml:"AssumeRoleWithSAMLResponse"`
	AssumeRoleWithSAMLResult assumeRoleWithSAMLResult `xml:"AssumeRoleWithSAMLResult"`
}

type assumeRoleWithSAMLResult struct {
	Credentials      stsCredentials  `xml:"Credentials"`
	AssumedRoleUser  assumedRoleUser `xml:"AssumedRoleUser"`
	Subject          string          `xml:"Subject"`
	SubjectType      string          `xml:"SubjectType"`
	Issuer           string          `xml:"Issuer"`
	Audience         string          `xml:"Audience"`
	PackedPolicySize int             `xml:"PackedPolicySize"`
}

type assumeRootResponse struct {
	XMLName          xml.Name         `xml:"AssumeRootResponse"`
	AssumeRootResult assumeRootResult `xml:"AssumeRootResult"`
}

type assumeRootResult struct {
	Credentials    stsCredentials `xml:"Credentials"`
	SourceIdentity string         `xml:"SourceIdentity"`
}

type decodeAuthorizationMessageResponse struct {
	XMLName                          xml.Name                         `xml:"DecodeAuthorizationMessageResponse"`
	DecodeAuthorizationMessageResult decodeAuthorizationMessageResult `xml:"DecodeAuthorizationMessageResult"`
}

type decodeAuthorizationMessageResult struct {
	DecodedMessage string `xml:"DecodedMessage"`
}

type getDelegatedAccessTokenResponse struct {
	XMLName                       xml.Name                      `xml:"GetDelegatedAccessTokenResponse"`
	GetDelegatedAccessTokenResult getDelegatedAccessTokenResult `xml:"GetDelegatedAccessTokenResult"`
}

type getDelegatedAccessTokenResult struct {
	DelegatedAccessToken string `xml:"DelegatedAccessToken"`
	Expiration           string `xml:"Expiration"`
}

type getFederationTokenResponse struct {
	XMLName                  xml.Name                 `xml:"GetFederationTokenResponse"`
	GetFederationTokenResult getFederationTokenResult `xml:"GetFederationTokenResult"`
}

type getFederationTokenResult struct {
	Credentials      stsCredentials `xml:"Credentials"`
	FederatedUser    federatedUser  `xml:"FederatedUser"`
	PackedPolicySize int            `xml:"PackedPolicySize"`
}

type federatedUser struct {
	Arn             string `xml:"Arn"`
	FederatedUserID string `xml:"FederatedUserId"`
}

type getWebIdentityTokenResponse struct {
	XMLName                   xml.Name                  `xml:"GetWebIdentityTokenResponse"`
	GetWebIdentityTokenResult getWebIdentityTokenResult `xml:"GetWebIdentityTokenResult"`
}

type getWebIdentityTokenResult struct {
	WebIdentityToken string `xml:"WebIdentityToken"`
}

// --- Handlers ---

func (p *STSProvider) handleAssumeRoleWithWebIdentity(_ context.Context, form url.Values) (*plugin.Response, error) {
	roleArn := form.Get("RoleArn")
	sessionName := form.Get("RoleSessionName")
	webToken := form.Get("WebIdentityToken")
	if roleArn == "" {
		return stsXMLError("MissingParameter", "RoleArn is required", http.StatusBadRequest), nil
	}
	if sessionName == "" {
		sessionName = "web-session"
	}
	keyID, err := generateAsiaKeyID()
	if err != nil {
		return nil, err
	}
	secret, err := generateSecret()
	if err != nil {
		return nil, err
	}
	token, err := generateSessionToken()
	if err != nil {
		return nil, err
	}
	roaID, err := generateARoaID()
	if err != nil {
		return nil, err
	}
	expiration := time.Now().UTC().Add(time.Hour).Format(time.RFC3339)
	assumedArn := deriveAssumedRoleArn(roleArn, sessionName)

	subject := "sub-devcloud-user"
	if len(webToken) > 0 {
		end := min(len(webToken), 16)
		subject = "sub-" + webToken[:end]
	}

	return stsXMLResponse(http.StatusOK, assumeRoleWithWebIdentityResponse{
		AssumeRoleWithWebIdentityResult: assumeRoleWithWebIdentityResult{
			Credentials: stsCredentials{
				AccessKeyID:     keyID,
				SecretAccessKey: secret,
				SessionToken:    token,
				Expiration:      expiration,
			},
			SubjectFromWebIdentityToken: subject,
			AssumedRoleUser: assumedRoleUser{
				Arn:           assumedArn,
				AssumedRoleID: fmt.Sprintf("%s:%s", roaID, sessionName),
			},
			PackedPolicySize: 1,
			Provider:         "https://sts.amazonaws.com",
			Audience:         "sts.devcloud",
		},
	})
}

func (p *STSProvider) handleAssumeRoleWithSAML(_ context.Context, form url.Values) (*plugin.Response, error) {
	roleArn := form.Get("RoleArn")
	principalArn := form.Get("PrincipalArn")
	if roleArn == "" || principalArn == "" {
		return stsXMLError("MissingParameter", "RoleArn and PrincipalArn are required", http.StatusBadRequest), nil
	}
	sessionName := "saml-session"
	keyID, err := generateAsiaKeyID()
	if err != nil {
		return nil, err
	}
	secret, err := generateSecret()
	if err != nil {
		return nil, err
	}
	token, err := generateSessionToken()
	if err != nil {
		return nil, err
	}
	roaID, err := generateARoaID()
	if err != nil {
		return nil, err
	}
	expiration := time.Now().UTC().Add(time.Hour).Format(time.RFC3339)
	assumedArn := deriveAssumedRoleArn(roleArn, sessionName)

	return stsXMLResponse(http.StatusOK, assumeRoleWithSAMLResponse{
		AssumeRoleWithSAMLResult: assumeRoleWithSAMLResult{
			Credentials: stsCredentials{
				AccessKeyID:     keyID,
				SecretAccessKey: secret,
				SessionToken:    token,
				Expiration:      expiration,
			},
			AssumedRoleUser: assumedRoleUser{
				Arn:           assumedArn,
				AssumedRoleID: fmt.Sprintf("%s:%s", roaID, sessionName),
			},
			Subject:          "saml-user",
			SubjectType:      "persistent",
			Issuer:           "https://saml.devcloud.local",
			Audience:         "https://signin.aws.amazon.com/saml",
			PackedPolicySize: 1,
		},
	})
}

func (p *STSProvider) handleAssumeRoot(_ context.Context, form url.Values) (*plugin.Response, error) {
	target := form.Get("TargetPrincipal")
	if target == "" {
		target = defaultAccountID
	}
	keyID, err := generateAsiaKeyID()
	if err != nil {
		return nil, err
	}
	secret, err := generateSecret()
	if err != nil {
		return nil, err
	}
	token, err := generateSessionToken()
	if err != nil {
		return nil, err
	}
	expiration := time.Now().UTC().Add(15 * time.Minute).Format(time.RFC3339)

	return stsXMLResponse(http.StatusOK, assumeRootResponse{
		AssumeRootResult: assumeRootResult{
			Credentials: stsCredentials{
				AccessKeyID:     keyID,
				SecretAccessKey: secret,
				SessionToken:    token,
				Expiration:      expiration,
			},
			SourceIdentity: target,
		},
	})
}

func (p *STSProvider) handleDecodeAuthorizationMessage(_ context.Context, form url.Values) (*plugin.Response, error) {
	msg := form.Get("EncodedMessage")
	if msg == "" {
		return stsXMLError("MissingParameter", "EncodedMessage is required", http.StatusBadRequest), nil
	}
	rawJSON, err := json.Marshal(map[string]any{
		"allowed":           false,
		"explicitDeny":      false,
		"matchedStatements": map[string]any{"items": []any{}},
		"failures":          map[string]any{"items": []any{}},
		"context": map[string]any{
			"action":     "unknown",
			"resource":   "unknown",
			"conditions": map[string]any{"items": []any{}},
		},
		"raw": msg,
	})
	if err != nil {
		return nil, err
	}
	decoded := string(rawJSON)
	return stsXMLResponse(http.StatusOK, decodeAuthorizationMessageResponse{
		DecodeAuthorizationMessageResult: decodeAuthorizationMessageResult{
			DecodedMessage: decoded,
		},
	})
}

func (p *STSProvider) handleGetDelegatedAccessToken(_ context.Context, form url.Values) (*plugin.Response, error) {
	token, err := generateSessionToken()
	if err != nil {
		return nil, err
	}
	expiration := time.Now().UTC().Add(time.Hour).Format(time.RFC3339)
	return stsXMLResponse(http.StatusOK, getDelegatedAccessTokenResponse{
		GetDelegatedAccessTokenResult: getDelegatedAccessTokenResult{
			DelegatedAccessToken: fmt.Sprintf("dat-%s", token),
			Expiration:           expiration,
		},
	})
}

func (p *STSProvider) handleGetFederationToken(_ context.Context, form url.Values) (*plugin.Response, error) {
	name := form.Get("Name")
	if name == "" {
		return stsXMLError("MissingParameter", "Name is required", http.StatusBadRequest), nil
	}
	keyID, err := generateAsiaKeyID()
	if err != nil {
		return nil, err
	}
	secret, err := generateSecret()
	if err != nil {
		return nil, err
	}
	token, err := generateSessionToken()
	if err != nil {
		return nil, err
	}
	expiration := time.Now().UTC().Add(time.Hour).Format(time.RFC3339)
	return stsXMLResponse(http.StatusOK, getFederationTokenResponse{
		GetFederationTokenResult: getFederationTokenResult{
			Credentials: stsCredentials{
				AccessKeyID:     keyID,
				SecretAccessKey: secret,
				SessionToken:    token,
				Expiration:      expiration,
			},
			FederatedUser: federatedUser{
				Arn:             fmt.Sprintf("arn:aws:sts::%s:federated-user/%s", defaultAccountID, name),
				FederatedUserID: fmt.Sprintf("%s:%s", defaultAccountID, name),
			},
			PackedPolicySize: 1,
		},
	})
}

func (p *STSProvider) handleGetWebIdentityToken(_ context.Context, _ url.Values) (*plugin.Response, error) {
	token, err := generateSessionToken()
	if err != nil {
		return nil, err
	}
	return stsXMLResponse(http.StatusOK, getWebIdentityTokenResponse{
		GetWebIdentityTokenResult: getWebIdentityTokenResult{
			WebIdentityToken: fmt.Sprintf("eyJhbGciOiJSUzI1NiJ9.%s.signature", token),
		},
	})
}
