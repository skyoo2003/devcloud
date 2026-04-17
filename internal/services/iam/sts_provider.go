// SPDX-License-Identifier: Apache-2.0

package iam

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/skyoo2003/devcloud/internal/plugin"
	"github.com/skyoo2003/devcloud/internal/storage/sqlite"
)

// STSProvider implements plugin.ServicePlugin for the STS service.
type STSProvider struct {
	store       *IAMStore
	sharedStore bool // true if store is shared with IAM (don't close it)
}

// ServiceID returns the unique identifier for this plugin.
func (p *STSProvider) ServiceID() string { return "sts" }

// ServiceName returns the human-readable name for this plugin.
func (p *STSProvider) ServiceName() string { return "AWS STS" }

// Protocol returns the wire protocol used by this plugin.
func (p *STSProvider) Protocol() plugin.ProtocolType { return plugin.ProtocolQuery }

// Init initialises the IAMStore from cfg.
// If cfg.Options contains an "iam_store" key holding an *IAMStore, STS will
// share that store instead of opening a separate database.
func (p *STSProvider) Init(cfg plugin.PluginConfig) error {
	// Prefer shared IAM store if provided.
	if sqliteStore, ok := cfg.Options["iam_store"].(*sqlite.Store); ok {
		p.store = NewIAMStoreFromDB(sqliteStore)
		p.sharedStore = true
		return nil
	}

	if err := os.MkdirAll(cfg.DataDir, 0o755); err != nil {
		return fmt.Errorf("init sts: %w", err)
	}

	dbPath := filepath.Join(cfg.DataDir, "iam.db")
	if v, ok := cfg.Options["db_path"]; ok {
		if s, ok := v.(string); ok && s != "" {
			dbPath = s
		}
	}

	var err error
	p.store, err = NewIAMStore(dbPath)
	return err
}

// Shutdown closes the IAMStore (unless it's shared with IAM).
func (p *STSProvider) Shutdown(_ context.Context) error {
	if p.store != nil && !p.sharedStore {
		return p.store.Close()
	}
	return nil
}

// HandleRequest parses the Action from the form body and routes to a handler.
func (p *STSProvider) HandleRequest(ctx context.Context, op string, req *http.Request) (*plugin.Response, error) {
	body, err := io.ReadAll(req.Body)
	if err != nil {
		return stsXMLError("InvalidRequest", "failed to read request body", http.StatusBadRequest), nil
	}

	form, err := url.ParseQuery(string(body))
	if err != nil {
		return stsXMLError("InvalidRequest", "failed to parse form body", http.StatusBadRequest), nil
	}

	action := op
	if action == "" {
		action = form.Get("Action")
	}

	switch action {
	case "GetCallerIdentity":
		return p.handleGetCallerIdentity(ctx, form)
	case "AssumeRole":
		return p.handleAssumeRole(ctx, form)
	case "GetSessionToken":
		return p.handleGetSessionToken(ctx, form)
	case "GetAccessKeyInfo":
		return p.handleGetAccessKeyInfo(ctx, form)
	default:
		return stsXMLError("InvalidAction", fmt.Sprintf("unknown action: %s", action), http.StatusBadRequest), nil
	}
}

// ListResources returns no resources for STS.
func (p *STSProvider) ListResources(_ context.Context) ([]plugin.Resource, error) {
	return nil, nil
}

// GetMetrics returns empty metrics.
func (p *STSProvider) GetMetrics(_ context.Context) (*plugin.ServiceMetrics, error) {
	return &plugin.ServiceMetrics{}, nil
}

// --- XML response structs ---

type getCallerIdentityResponse struct {
	XMLName                 xml.Name                `xml:"GetCallerIdentityResponse"`
	GetCallerIdentityResult getCallerIdentityResult `xml:"GetCallerIdentityResult"`
}

type getCallerIdentityResult struct {
	Account string `xml:"Account"`
	Arn     string `xml:"Arn"`
	UserID  string `xml:"UserId"`
}

type assumeRoleResponse struct {
	XMLName          xml.Name         `xml:"AssumeRoleResponse"`
	AssumeRoleResult assumeRoleResult `xml:"AssumeRoleResult"`
}

type assumeRoleResult struct {
	Credentials     stsCredentials  `xml:"Credentials"`
	AssumedRoleUser assumedRoleUser `xml:"AssumedRoleUser"`
}

type stsCredentials struct {
	AccessKeyID     string `xml:"AccessKeyId"`
	SecretAccessKey string `xml:"SecretAccessKey"`
	SessionToken    string `xml:"SessionToken"`
	Expiration      string `xml:"Expiration"`
}

type assumedRoleUser struct {
	Arn           string `xml:"Arn"`
	AssumedRoleID string `xml:"AssumedRoleId"`
}

type getSessionTokenResponse struct {
	XMLName               xml.Name              `xml:"GetSessionTokenResponse"`
	GetSessionTokenResult getSessionTokenResult `xml:"GetSessionTokenResult"`
}

type getSessionTokenResult struct {
	Credentials stsCredentials `xml:"Credentials"`
}

type getAccessKeyInfoResponse struct {
	XMLName                xml.Name               `xml:"GetAccessKeyInfoResponse"`
	GetAccessKeyInfoResult getAccessKeyInfoResult `xml:"GetAccessKeyInfoResult"`
}

type getAccessKeyInfoResult struct {
	Account string `xml:"Account"`
}

type stsErrorResponse struct {
	XMLName xml.Name `xml:"ErrorResponse"`
	Error   stsError `xml:"Error"`
}

type stsError struct {
	Code    string `xml:"Code"`
	Message string `xml:"Message"`
}

// --- helpers ---

func stsXMLError(code, message string, status int) *plugin.Response {
	body, _ := xml.Marshal(stsErrorResponse{Error: stsError{Code: code, Message: message}})
	return &plugin.Response{
		StatusCode:  status,
		ContentType: "text/xml",
		Body:        body,
	}
}

func stsXMLResponse(status int, v any) (*plugin.Response, error) {
	body, err := xml.Marshal(v)
	if err != nil {
		return nil, err
	}
	return &plugin.Response{
		StatusCode:  status,
		ContentType: "text/xml",
		Body:        body,
	}, nil
}

func generateSessionToken() (string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}

func generateAsiaKeyID() (string, error) {
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return "ASIA" + strings.ToUpper(hex.EncodeToString(b)), nil
}

func generateARoaID() (string, error) {
	b := make([]byte, 10)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return "AROA" + strings.ToUpper(hex.EncodeToString(b)), nil
}

// --- STS operation implementations ---

func (p *STSProvider) handleGetCallerIdentity(_ context.Context, _ url.Values) (*plugin.Response, error) {
	accountID, arn, userID := p.store.GetCallerIdentity(defaultAccountID)

	return stsXMLResponse(http.StatusOK, getCallerIdentityResponse{
		GetCallerIdentityResult: getCallerIdentityResult{
			Account: accountID,
			Arn:     arn,
			UserID:  userID,
		},
	})
}

func (p *STSProvider) handleAssumeRole(_ context.Context, form url.Values) (*plugin.Response, error) {
	roleArn := form.Get("RoleArn")
	sessionName := form.Get("RoleSessionName")
	if roleArn == "" {
		return stsXMLError("MissingParameter", "RoleArn is required", http.StatusBadRequest), nil
	}
	if sessionName == "" {
		sessionName = "session"
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

	// Derive assumed-role ARN from the role ARN:
	// arn:aws:iam::ACCOUNT:role/ROLENAME -> arn:aws:sts::ACCOUNT:assumed-role/ROLENAME/SESSION
	assumedArn := deriveAssumedRoleArn(roleArn, sessionName)

	return stsXMLResponse(http.StatusOK, assumeRoleResponse{
		AssumeRoleResult: assumeRoleResult{
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
		},
	})
}

func (p *STSProvider) handleGetSessionToken(_ context.Context, _ url.Values) (*plugin.Response, error) {
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

	return stsXMLResponse(http.StatusOK, getSessionTokenResponse{
		GetSessionTokenResult: getSessionTokenResult{
			Credentials: stsCredentials{
				AccessKeyID:     keyID,
				SecretAccessKey: secret,
				SessionToken:    token,
				Expiration:      expiration,
			},
		},
	})
}

func (p *STSProvider) handleGetAccessKeyInfo(_ context.Context, form url.Values) (*plugin.Response, error) {
	accessKeyID := form.Get("AccessKeyId")
	if accessKeyID == "" {
		return stsXMLError("MissingParameter", "AccessKeyId is required", http.StatusBadRequest), nil
	}
	return stsXMLResponse(http.StatusOK, getAccessKeyInfoResponse{
		GetAccessKeyInfoResult: getAccessKeyInfoResult{
			Account: defaultAccountID,
		},
	})
}

// deriveAssumedRoleArn converts an IAM role ARN to an STS assumed-role ARN.
// e.g. arn:aws:iam::123456789012:role/myrole -> arn:aws:sts::123456789012:assumed-role/myrole/session
func deriveAssumedRoleArn(roleArn, sessionName string) string {
	// Split on ":role/" to extract account and role name
	parts := strings.SplitN(roleArn, ":role/", 2)
	if len(parts) != 2 {
		return fmt.Sprintf("arn:aws:sts::%s:assumed-role/unknown/%s", defaultAccountID, sessionName)
	}
	// parts[0] = "arn:aws:iam::ACCOUNT"
	// Extract account ID from parts[0]
	prefix := strings.TrimPrefix(parts[0], "arn:aws:iam::")
	accountID := prefix
	roleName := parts[1]
	return fmt.Sprintf("arn:aws:sts::%s:assumed-role/%s/%s", accountID, roleName, sessionName)
}
