// SPDX-License-Identifier: Apache-2.0

// internal/services/cognitoidentityprovider/provider.go
package cognitoidentityprovider

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"path/filepath"
	"strings"
	"time"

	"github.com/skyoo2003/devcloud/internal/plugin"
)

const defaultAccountID = plugin.DefaultAccountID
const defaultRegion = "us-east-1"

// Provider implements the CognitoIdentityProviderService service.
type Provider struct {
	store *CognitoStore
}

func (p *Provider) ServiceID() string             { return "cognitoidentityprovider" }
func (p *Provider) ServiceName() string           { return "CognitoIdentityProviderService" }
func (p *Provider) Protocol() plugin.ProtocolType { return plugin.ProtocolJSON11 }

func (p *Provider) Init(cfg plugin.PluginConfig) error {
	dataDir := cfg.DataDir
	if dataDir == "" {
		dataDir = "."
	}
	var err error
	p.store, err = NewCognitoStore(filepath.Join(dataDir, "cognitoidentityprovider"))
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
		op = strings.TrimPrefix(target, "AmazonCognitoIdentityProvider.")
	}
	switch op {
	// UserPool
	case "CreateUserPool":
		return p.createUserPool(req)
	case "DescribeUserPool":
		return p.describeUserPool(req)
	case "ListUserPools":
		return p.listUserPools(req)
	case "UpdateUserPool":
		return p.updateUserPool(req)
	case "DeleteUserPool":
		return p.deleteUserPool(req)
	// UserPoolClient
	case "CreateUserPoolClient":
		return p.createUserPoolClient(req)
	case "DescribeUserPoolClient":
		return p.describeUserPoolClient(req)
	case "ListUserPoolClients":
		return p.listUserPoolClients(req)
	case "UpdateUserPoolClient":
		return p.updateUserPoolClient(req)
	case "DeleteUserPoolClient":
		return p.deleteUserPoolClient(req)
	// User management
	case "AdminCreateUser":
		return p.adminCreateUser(req)
	case "AdminGetUser":
		return p.adminGetUser(req)
	case "ListUsers":
		return p.listUsers(req)
	case "AdminDeleteUser":
		return p.adminDeleteUser(req)
	case "AdminDisableUser":
		return p.adminDisableUser(req)
	case "AdminEnableUser":
		return p.adminEnableUser(req)
	case "AdminUpdateUserAttributes":
		return p.adminUpdateUserAttributes(req)
	case "AdminDeleteUserAttributes":
		return p.adminDeleteUserAttributes(req)
	case "AdminSetUserPassword":
		return p.adminSetUserPassword(req)
	case "AdminResetUserPassword":
		return p.adminResetUserPassword(req)
	case "AdminConfirmSignUp":
		return p.adminConfirmSignUp(req)
	// Group management
	case "CreateGroup":
		return p.createGroup(req)
	case "GetGroup":
		return p.getGroup(req)
	case "ListGroups":
		return p.listGroups(req)
	case "UpdateGroup":
		return p.updateGroup(req)
	case "DeleteGroup":
		return p.deleteGroup(req)
	// User-Group membership
	case "AdminAddUserToGroup":
		return p.adminAddUserToGroup(req)
	case "AdminRemoveUserFromGroup":
		return p.adminRemoveUserFromGroup(req)
	case "AdminListGroupsForUser":
		return p.adminListGroupsForUser(req)
	case "ListUsersInGroup":
		return p.listUsersInGroup(req)
	// IdentityProvider
	case "CreateIdentityProvider":
		return p.createIdentityProvider(req)
	case "DescribeIdentityProvider":
		return p.describeIdentityProvider(req)
	case "ListIdentityProviders":
		return p.listIdentityProviders(req)
	case "UpdateIdentityProvider":
		return p.updateIdentityProvider(req)
	case "DeleteIdentityProvider":
		return p.deleteIdentityProvider(req)
	case "GetIdentityProviderByIdentifier":
		return p.getIdentityProviderByIdentifier(req)
	// ResourceServer
	case "CreateResourceServer":
		return p.createResourceServer(req)
	case "DescribeResourceServer":
		return p.describeResourceServer(req)
	case "ListResourceServers":
		return p.listResourceServers(req)
	case "UpdateResourceServer":
		return p.updateResourceServer(req)
	case "DeleteResourceServer":
		return p.deleteResourceServer(req)
	// Auth flows
	case "SignUp":
		return p.signUp(req)
	case "ConfirmSignUp":
		return p.confirmSignUp(req)
	case "InitiateAuth":
		return p.initiateAuth(req)
	case "AdminInitiateAuth":
		return p.adminInitiateAuth(req)
	case "RespondToAuthChallenge":
		return p.respondToAuthChallenge(req)
	case "AdminRespondToAuthChallenge":
		return p.adminRespondToAuthChallenge(req)
	case "ForgotPassword":
		return p.forgotPassword(req)
	case "ConfirmForgotPassword":
		return p.confirmForgotPassword(req)
	case "ChangePassword":
		return p.changePassword(req)
	case "GlobalSignOut":
		return p.globalSignOut(req)
	case "AdminUserGlobalSignOut":
		return p.adminUserGlobalSignOut(req)
	case "RevokeToken":
		return p.revokeToken(req)
	case "GetUser":
		return p.getUser(req)
	// MFA config
	case "SetUserPoolMfaConfig":
		return p.setUserPoolMfaConfig(req)
	case "GetUserPoolMfaConfig":
		return p.getUserPoolMfaConfig(req)
	// Tags
	case "TagResource":
		return p.tagResource(req)
	case "UntagResource":
		return p.untagResource(req)
	case "ListTagsForResource":
		return p.listTagsForResource(req)
	default:
		// Return success/empty for unimplemented ops
		return emptyOK(), nil
	}
}

func (p *Provider) ListResources(_ context.Context) ([]plugin.Resource, error) {
	pools, err := p.store.ListUserPools()
	if err != nil {
		return nil, err
	}
	out := make([]plugin.Resource, 0, len(pools))
	for _, pool := range pools {
		out = append(out, plugin.Resource{Type: "user-pool", ID: pool.ID, Name: pool.Name})
	}
	return out, nil
}

func (p *Provider) GetMetrics(_ context.Context) (*plugin.ServiceMetrics, error) {
	return &plugin.ServiceMetrics{}, nil
}

// --- helpers ---

func cognitoError(code, msg string, status int) *plugin.Response {
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

func emptyOK() *plugin.Response {
	return &plugin.Response{StatusCode: http.StatusOK, Body: []byte("{}"), ContentType: "application/x-amz-json-1.1"}
}

func newID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

func poolARN(id string) string {
	return fmt.Sprintf("arn:aws:cognito-idp:%s:%s:userpool/%s", defaultRegion, defaultAccountID, id)
}

func now() int64 { return time.Now().Unix() }

func marshalJSON(v any) string {
	b, _ := json.Marshal(v)
	return string(b)
}

// makeDummyTokens creates base64-encoded JSON tokens for auth simulation.
func makeDummyTokens(poolID, username string) map[string]any {
	payload := map[string]any{
		"sub":     username,
		"pool_id": poolID,
		"iss":     "devcloud",
		"iat":     now(),
		"exp":     now() + 3600,
	}
	accessToken := base64.StdEncoding.EncodeToString([]byte(marshalJSON(payload)))
	idPayload := map[string]any{
		"sub":     username,
		"pool_id": poolID,
		"email":   username + "@devcloud.local",
		"iat":     now(),
		"exp":     now() + 3600,
	}
	idToken := base64.StdEncoding.EncodeToString([]byte(marshalJSON(idPayload)))
	refreshPayload := map[string]any{
		"sub":     username,
		"pool_id": poolID,
		"type":    "refresh",
	}
	refreshToken := base64.StdEncoding.EncodeToString([]byte(marshalJSON(refreshPayload)))
	return map[string]any{
		"AccessToken":  accessToken,
		"IdToken":      idToken,
		"RefreshToken": refreshToken,
		"TokenType":    "Bearer",
		"ExpiresIn":    3600,
	}
}

// userToMap converts a User to an API-shaped map.
func userToMap(u *User) map[string]any {
	var attrs []any
	_ = json.Unmarshal([]byte(u.Attributes), &attrs)
	if attrs == nil {
		attrs = []any{}
	}
	enabled := u.Enabled != 0
	return map[string]any{
		"Username":             u.Username,
		"UserStatus":           u.Status,
		"Enabled":              enabled,
		"UserAttributes":       attrs,
		"UserCreateDate":       u.CreatedAt,
		"UserLastModifiedDate": u.UpdatedAt,
	}
}

// groupToMap converts a Group to an API-shaped map.
func groupToMap(g *Group) map[string]any {
	return map[string]any{
		"GroupName":        g.Name,
		"UserPoolId":       g.PoolID,
		"Description":      g.Description,
		"RoleArn":          g.RoleARN,
		"Precedence":       g.Precedence,
		"CreationDate":     g.CreatedAt,
		"LastModifiedDate": g.CreatedAt,
	}
}

// idpToMap converts an IdentityProvider to an API-shaped map.
func idpToMap(idp *IdentityProvider) map[string]any {
	var details map[string]string
	_ = json.Unmarshal([]byte(idp.Details), &details)
	if details == nil {
		details = map[string]string{}
	}
	var attrMapping map[string]string
	_ = json.Unmarshal([]byte(idp.AttributeMapping), &attrMapping)
	if attrMapping == nil {
		attrMapping = map[string]string{}
	}
	var identifiers []string
	_ = json.Unmarshal([]byte(idp.IDPIdentifiers), &identifiers)
	if identifiers == nil {
		identifiers = []string{}
	}
	return map[string]any{
		"UserPoolId":       idp.PoolID,
		"ProviderName":     idp.Name,
		"ProviderType":     idp.Type,
		"ProviderDetails":  details,
		"AttributeMapping": attrMapping,
		"IdpIdentifiers":   identifiers,
		"CreationDate":     idp.CreatedAt,
		"LastModifiedDate": idp.CreatedAt,
	}
}

// rsToMap converts a ResourceServer to API-shaped map.
func rsToMap(rs *ResourceServer) map[string]any {
	var scopes []any
	_ = json.Unmarshal([]byte(rs.Scopes), &scopes)
	if scopes == nil {
		scopes = []any{}
	}
	return map[string]any{
		"UserPoolId": rs.PoolID,
		"Identifier": rs.Identifier,
		"Name":       rs.Name,
		"Scopes":     scopes,
	}
}

// poolToMap converts a UserPool to API-shaped map.
func poolToMap(pool *UserPool) map[string]any {
	var policies map[string]any
	_ = json.Unmarshal([]byte(pool.Policies), &policies)
	if policies == nil {
		policies = map[string]any{}
	}
	var schemaAttrs []any
	_ = json.Unmarshal([]byte(pool.SchemaAttrs), &schemaAttrs)
	if schemaAttrs == nil {
		schemaAttrs = []any{}
	}
	return map[string]any{
		"Id":               pool.ID,
		"Arn":              pool.ARN,
		"Name":             pool.Name,
		"Status":           pool.Status,
		"Policies":         policies,
		"MfaConfiguration": pool.MFAConfig,
		"SchemaAttributes": schemaAttrs,
		"CreationDate":     pool.CreatedAt,
		"LastModifiedDate": pool.UpdatedAt,
	}
}

// clientToMap converts a UserPoolClient to API-shaped map.
func clientToMap(c *UserPoolClient) map[string]any {
	var authFlows []string
	_ = json.Unmarshal([]byte(c.AuthFlows), &authFlows)
	if authFlows == nil {
		authFlows = []string{}
	}
	var scopes []string
	_ = json.Unmarshal([]byte(c.Scopes), &scopes)
	if scopes == nil {
		scopes = []string{}
	}
	var callbackURLs []string
	_ = json.Unmarshal([]byte(c.CallbackURLs), &callbackURLs)
	if callbackURLs == nil {
		callbackURLs = []string{}
	}
	var logoutURLs []string
	_ = json.Unmarshal([]byte(c.LogoutURLs), &logoutURLs)
	if logoutURLs == nil {
		logoutURLs = []string{}
	}
	return map[string]any{
		"ClientId":           c.ID,
		"UserPoolId":         c.PoolID,
		"ClientName":         c.Name,
		"ClientSecret":       c.Secret,
		"ExplicitAuthFlows":  authFlows,
		"AllowedOAuthScopes": scopes,
		"CallbackURLs":       callbackURLs,
		"LogoutURLs":         logoutURLs,
		"CreationDate":       c.CreatedAt,
		"LastModifiedDate":   c.CreatedAt,
	}
}

// mergeAttributes merges new attributes into existing JSON attribute array.
func mergeAttributes(existing string, updates []map[string]string) string {
	var attrs []map[string]string
	_ = json.Unmarshal([]byte(existing), &attrs)
	if attrs == nil {
		attrs = []map[string]string{}
	}
	for _, upd := range updates {
		name := upd["Name"]
		found := false
		for i, attr := range attrs {
			if attr["Name"] == name {
				attrs[i]["Value"] = upd["Value"]
				found = true
				break
			}
		}
		if !found {
			attrs = append(attrs, upd)
		}
	}
	return marshalJSON(attrs)
}

// deleteAttributes removes named attributes from JSON attribute array.
func deleteAttributes(existing string, names []string) string {
	var attrs []map[string]string
	_ = json.Unmarshal([]byte(existing), &attrs)
	nameSet := map[string]bool{}
	for _, n := range names {
		nameSet[n] = true
	}
	var result []map[string]string
	for _, attr := range attrs {
		if !nameSet[attr["Name"]] {
			result = append(result, attr)
		}
	}
	return marshalJSON(result)
}

// --- UserPool handlers ---

func (p *Provider) createUserPool(req *http.Request) (*plugin.Response, error) {
	var in struct {
		PoolName  string         `json:"PoolName"`
		Policies  map[string]any `json:"Policies"`
		MFAConfig string         `json:"MfaConfiguration"`
		Schema    []any          `json:"Schema"`
	}
	if err := json.NewDecoder(req.Body).Decode(&in); err != nil {
		return cognitoError("InvalidParameterException", "failed to parse request", http.StatusBadRequest), nil
	}
	if in.PoolName == "" {
		return cognitoError("InvalidParameterException", "PoolName is required", http.StatusBadRequest), nil
	}
	id := "us-east-1_" + strings.ReplaceAll(newID()[:8], "-", "")
	mfa := in.MFAConfig
	if mfa == "" {
		mfa = "OFF"
	}
	pool := &UserPool{
		ID:          id,
		ARN:         poolARN(id),
		Name:        in.PoolName,
		Status:      "Active",
		Policies:    marshalJSON(in.Policies),
		MFAConfig:   mfa,
		SchemaAttrs: marshalJSON(in.Schema),
		CreatedAt:   now(),
		UpdatedAt:   now(),
	}
	if err := p.store.CreateUserPool(pool); err != nil {
		return nil, err
	}
	return jsonResp(http.StatusOK, map[string]any{"UserPool": poolToMap(pool)})
}

func (p *Provider) describeUserPool(req *http.Request) (*plugin.Response, error) {
	var in struct {
		UserPoolId string `json:"UserPoolId"`
	}
	if err := json.NewDecoder(req.Body).Decode(&in); err != nil {
		return cognitoError("InvalidParameterException", "failed to parse request", http.StatusBadRequest), nil
	}
	pool, err := p.store.GetUserPool(in.UserPoolId)
	if err != nil {
		if err == ErrPoolNotFound {
			return cognitoError("ResourceNotFoundException", "user pool not found", http.StatusBadRequest), nil
		}
		return nil, err
	}
	return jsonResp(http.StatusOK, map[string]any{"UserPool": poolToMap(pool)})
}

func (p *Provider) listUserPools(req *http.Request) (*plugin.Response, error) {
	pools, err := p.store.ListUserPools()
	if err != nil {
		return nil, err
	}
	summaries := make([]any, 0, len(pools))
	for _, pool := range pools {
		summaries = append(summaries, poolToMap(&pool))
	}
	return jsonResp(http.StatusOK, map[string]any{"UserPools": summaries})
}

func (p *Provider) updateUserPool(req *http.Request) (*plugin.Response, error) {
	var in struct {
		UserPoolId string         `json:"UserPoolId"`
		Policies   map[string]any `json:"Policies"`
		MFAConfig  string         `json:"MfaConfiguration"`
	}
	if err := json.NewDecoder(req.Body).Decode(&in); err != nil {
		return cognitoError("InvalidParameterException", "failed to parse request", http.StatusBadRequest), nil
	}
	pool, err := p.store.GetUserPool(in.UserPoolId)
	if err != nil {
		if err == ErrPoolNotFound {
			return cognitoError("ResourceNotFoundException", "user pool not found", http.StatusBadRequest), nil
		}
		return nil, err
	}
	if in.Policies != nil {
		pool.Policies = marshalJSON(in.Policies)
	}
	if in.MFAConfig != "" {
		pool.MFAConfig = in.MFAConfig
	}
	pool.UpdatedAt = now()
	if err := p.store.UpdateUserPool(pool); err != nil {
		return nil, err
	}
	return emptyOK(), nil
}

func (p *Provider) deleteUserPool(req *http.Request) (*plugin.Response, error) {
	var in struct {
		UserPoolId string `json:"UserPoolId"`
	}
	if err := json.NewDecoder(req.Body).Decode(&in); err != nil {
		return cognitoError("InvalidParameterException", "failed to parse request", http.StatusBadRequest), nil
	}
	if err := p.store.DeleteUserPool(in.UserPoolId); err != nil {
		if err == ErrPoolNotFound {
			return cognitoError("ResourceNotFoundException", "user pool not found", http.StatusBadRequest), nil
		}
		return nil, err
	}
	return emptyOK(), nil
}

// --- UserPoolClient handlers ---

func (p *Provider) createUserPoolClient(req *http.Request) (*plugin.Response, error) {
	var in struct {
		UserPoolId         string   `json:"UserPoolId"`
		ClientName         string   `json:"ClientName"`
		GenerateSecret     bool     `json:"GenerateSecret"`
		ExplicitAuthFlows  []string `json:"ExplicitAuthFlows"`
		AllowedOAuthScopes []string `json:"AllowedOAuthScopes"`
		CallbackURLs       []string `json:"CallbackURLs"`
		LogoutURLs         []string `json:"LogoutURLs"`
	}
	if err := json.NewDecoder(req.Body).Decode(&in); err != nil {
		return cognitoError("InvalidParameterException", "failed to parse request", http.StatusBadRequest), nil
	}
	if in.UserPoolId == "" || in.ClientName == "" {
		return cognitoError("InvalidParameterException", "UserPoolId and ClientName are required", http.StatusBadRequest), nil
	}
	// Ensure pool exists
	if _, err := p.store.GetUserPool(in.UserPoolId); err != nil {
		return cognitoError("ResourceNotFoundException", "user pool not found", http.StatusBadRequest), nil
	}
	secret := ""
	if in.GenerateSecret {
		sb := make([]byte, 32)
		_, _ = rand.Read(sb)
		secret = base64.StdEncoding.EncodeToString(sb)
	}
	client := &UserPoolClient{
		ID:           newID(),
		PoolID:       in.UserPoolId,
		Name:         in.ClientName,
		Secret:       secret,
		AuthFlows:    marshalJSON(in.ExplicitAuthFlows),
		Scopes:       marshalJSON(in.AllowedOAuthScopes),
		CallbackURLs: marshalJSON(in.CallbackURLs),
		LogoutURLs:   marshalJSON(in.LogoutURLs),
		CreatedAt:    now(),
	}
	if err := p.store.CreateUserPoolClient(client); err != nil {
		return nil, err
	}
	return jsonResp(http.StatusOK, map[string]any{"UserPoolClient": clientToMap(client)})
}

func (p *Provider) describeUserPoolClient(req *http.Request) (*plugin.Response, error) {
	var in struct {
		UserPoolId string `json:"UserPoolId"`
		ClientId   string `json:"ClientId"`
	}
	if err := json.NewDecoder(req.Body).Decode(&in); err != nil {
		return cognitoError("InvalidParameterException", "failed to parse request", http.StatusBadRequest), nil
	}
	client, err := p.store.GetUserPoolClient(in.UserPoolId, in.ClientId)
	if err != nil {
		if err == ErrClientNotFound {
			return cognitoError("ResourceNotFoundException", "client not found", http.StatusBadRequest), nil
		}
		return nil, err
	}
	return jsonResp(http.StatusOK, map[string]any{"UserPoolClient": clientToMap(client)})
}

func (p *Provider) listUserPoolClients(req *http.Request) (*plugin.Response, error) {
	var in struct {
		UserPoolId string `json:"UserPoolId"`
	}
	if err := json.NewDecoder(req.Body).Decode(&in); err != nil {
		return cognitoError("InvalidParameterException", "failed to parse request", http.StatusBadRequest), nil
	}
	clients, err := p.store.ListUserPoolClients(in.UserPoolId)
	if err != nil {
		return nil, err
	}
	summaries := make([]any, 0, len(clients))
	for _, c := range clients {
		summaries = append(summaries, map[string]any{
			"ClientId":   c.ID,
			"ClientName": c.Name,
			"UserPoolId": c.PoolID,
		})
	}
	return jsonResp(http.StatusOK, map[string]any{"UserPoolClients": summaries})
}

func (p *Provider) updateUserPoolClient(req *http.Request) (*plugin.Response, error) {
	var in struct {
		UserPoolId         string   `json:"UserPoolId"`
		ClientId           string   `json:"ClientId"`
		ClientName         string   `json:"ClientName"`
		ExplicitAuthFlows  []string `json:"ExplicitAuthFlows"`
		AllowedOAuthScopes []string `json:"AllowedOAuthScopes"`
		CallbackURLs       []string `json:"CallbackURLs"`
		LogoutURLs         []string `json:"LogoutURLs"`
	}
	if err := json.NewDecoder(req.Body).Decode(&in); err != nil {
		return cognitoError("InvalidParameterException", "failed to parse request", http.StatusBadRequest), nil
	}
	client, err := p.store.GetUserPoolClient(in.UserPoolId, in.ClientId)
	if err != nil {
		if err == ErrClientNotFound {
			return cognitoError("ResourceNotFoundException", "client not found", http.StatusBadRequest), nil
		}
		return nil, err
	}
	if in.ClientName != "" {
		client.Name = in.ClientName
	}
	if in.ExplicitAuthFlows != nil {
		client.AuthFlows = marshalJSON(in.ExplicitAuthFlows)
	}
	if in.AllowedOAuthScopes != nil {
		client.Scopes = marshalJSON(in.AllowedOAuthScopes)
	}
	if in.CallbackURLs != nil {
		client.CallbackURLs = marshalJSON(in.CallbackURLs)
	}
	if in.LogoutURLs != nil {
		client.LogoutURLs = marshalJSON(in.LogoutURLs)
	}
	if err := p.store.UpdateUserPoolClient(client); err != nil {
		return nil, err
	}
	return jsonResp(http.StatusOK, map[string]any{"UserPoolClient": clientToMap(client)})
}

func (p *Provider) deleteUserPoolClient(req *http.Request) (*plugin.Response, error) {
	var in struct {
		UserPoolId string `json:"UserPoolId"`
		ClientId   string `json:"ClientId"`
	}
	if err := json.NewDecoder(req.Body).Decode(&in); err != nil {
		return cognitoError("InvalidParameterException", "failed to parse request", http.StatusBadRequest), nil
	}
	if err := p.store.DeleteUserPoolClient(in.UserPoolId, in.ClientId); err != nil {
		if err == ErrClientNotFound {
			return cognitoError("ResourceNotFoundException", "client not found", http.StatusBadRequest), nil
		}
		return nil, err
	}
	return emptyOK(), nil
}

// --- User handlers ---

func (p *Provider) adminCreateUser(req *http.Request) (*plugin.Response, error) {
	var in struct {
		UserPoolId        string              `json:"UserPoolId"`
		Username          string              `json:"Username"`
		TemporaryPassword string              `json:"TemporaryPassword"`
		UserAttributes    []map[string]string `json:"UserAttributes"`
	}
	if err := json.NewDecoder(req.Body).Decode(&in); err != nil {
		return cognitoError("InvalidParameterException", "failed to parse request", http.StatusBadRequest), nil
	}
	if in.UserPoolId == "" || in.Username == "" {
		return cognitoError("InvalidParameterException", "UserPoolId and Username are required", http.StatusBadRequest), nil
	}
	if _, err := p.store.GetUserPool(in.UserPoolId); err != nil {
		return cognitoError("ResourceNotFoundException", "user pool not found", http.StatusBadRequest), nil
	}
	// Check if user already exists
	if _, err := p.store.GetUser(in.UserPoolId, in.Username); err == nil {
		return cognitoError("UsernameExistsException", "user already exists", http.StatusBadRequest), nil
	}
	t := now()
	user := &User{
		PoolID:      in.UserPoolId,
		Username:    in.Username,
		Status:      "FORCE_CHANGE_PASSWORD",
		Enabled:     1,
		Attributes:  marshalJSON(in.UserAttributes),
		Password:    in.TemporaryPassword,
		MFASettings: "{}",
		CreatedAt:   t,
		UpdatedAt:   t,
	}
	if err := p.store.CreateUser(user); err != nil {
		return nil, err
	}
	return jsonResp(http.StatusOK, map[string]any{"User": userToMap(user)})
}

func (p *Provider) adminGetUser(req *http.Request) (*plugin.Response, error) {
	var in struct {
		UserPoolId string `json:"UserPoolId"`
		Username   string `json:"Username"`
	}
	if err := json.NewDecoder(req.Body).Decode(&in); err != nil {
		return cognitoError("InvalidParameterException", "failed to parse request", http.StatusBadRequest), nil
	}
	user, err := p.store.GetUser(in.UserPoolId, in.Username)
	if err != nil {
		if err == ErrUserNotFound {
			return cognitoError("UserNotFoundException", "user not found", http.StatusBadRequest), nil
		}
		return nil, err
	}
	return jsonResp(http.StatusOK, userToMap(user))
}

func (p *Provider) listUsers(req *http.Request) (*plugin.Response, error) {
	var in struct {
		UserPoolId string `json:"UserPoolId"`
	}
	if err := json.NewDecoder(req.Body).Decode(&in); err != nil {
		return cognitoError("InvalidParameterException", "failed to parse request", http.StatusBadRequest), nil
	}
	users, err := p.store.ListUsers(in.UserPoolId)
	if err != nil {
		return nil, err
	}
	out := make([]any, 0, len(users))
	for _, u := range users {
		out = append(out, userToMap(&u))
	}
	return jsonResp(http.StatusOK, map[string]any{"Users": out})
}

func (p *Provider) adminDeleteUser(req *http.Request) (*plugin.Response, error) {
	var in struct {
		UserPoolId string `json:"UserPoolId"`
		Username   string `json:"Username"`
	}
	if err := json.NewDecoder(req.Body).Decode(&in); err != nil {
		return cognitoError("InvalidParameterException", "failed to parse request", http.StatusBadRequest), nil
	}
	if err := p.store.DeleteUser(in.UserPoolId, in.Username); err != nil {
		if err == ErrUserNotFound {
			return cognitoError("UserNotFoundException", "user not found", http.StatusBadRequest), nil
		}
		return nil, err
	}
	return emptyOK(), nil
}

func (p *Provider) adminDisableUser(req *http.Request) (*plugin.Response, error) {
	var in struct {
		UserPoolId string `json:"UserPoolId"`
		Username   string `json:"Username"`
	}
	if err := json.NewDecoder(req.Body).Decode(&in); err != nil {
		return cognitoError("InvalidParameterException", "failed to parse request", http.StatusBadRequest), nil
	}
	user, err := p.store.GetUser(in.UserPoolId, in.Username)
	if err != nil {
		if err == ErrUserNotFound {
			return cognitoError("UserNotFoundException", "user not found", http.StatusBadRequest), nil
		}
		return nil, err
	}
	user.Enabled = 0
	user.UpdatedAt = now()
	if err := p.store.UpdateUser(user); err != nil {
		return nil, err
	}
	return emptyOK(), nil
}

func (p *Provider) adminEnableUser(req *http.Request) (*plugin.Response, error) {
	var in struct {
		UserPoolId string `json:"UserPoolId"`
		Username   string `json:"Username"`
	}
	if err := json.NewDecoder(req.Body).Decode(&in); err != nil {
		return cognitoError("InvalidParameterException", "failed to parse request", http.StatusBadRequest), nil
	}
	user, err := p.store.GetUser(in.UserPoolId, in.Username)
	if err != nil {
		if err == ErrUserNotFound {
			return cognitoError("UserNotFoundException", "user not found", http.StatusBadRequest), nil
		}
		return nil, err
	}
	user.Enabled = 1
	user.UpdatedAt = now()
	if err := p.store.UpdateUser(user); err != nil {
		return nil, err
	}
	return emptyOK(), nil
}

func (p *Provider) adminUpdateUserAttributes(req *http.Request) (*plugin.Response, error) {
	var in struct {
		UserPoolId     string              `json:"UserPoolId"`
		Username       string              `json:"Username"`
		UserAttributes []map[string]string `json:"UserAttributes"`
	}
	if err := json.NewDecoder(req.Body).Decode(&in); err != nil {
		return cognitoError("InvalidParameterException", "failed to parse request", http.StatusBadRequest), nil
	}
	user, err := p.store.GetUser(in.UserPoolId, in.Username)
	if err != nil {
		if err == ErrUserNotFound {
			return cognitoError("UserNotFoundException", "user not found", http.StatusBadRequest), nil
		}
		return nil, err
	}
	user.Attributes = mergeAttributes(user.Attributes, in.UserAttributes)
	user.UpdatedAt = now()
	if err := p.store.UpdateUser(user); err != nil {
		return nil, err
	}
	return emptyOK(), nil
}

func (p *Provider) adminDeleteUserAttributes(req *http.Request) (*plugin.Response, error) {
	var in struct {
		UserPoolId         string   `json:"UserPoolId"`
		Username           string   `json:"Username"`
		UserAttributeNames []string `json:"UserAttributeNames"`
	}
	if err := json.NewDecoder(req.Body).Decode(&in); err != nil {
		return cognitoError("InvalidParameterException", "failed to parse request", http.StatusBadRequest), nil
	}
	user, err := p.store.GetUser(in.UserPoolId, in.Username)
	if err != nil {
		if err == ErrUserNotFound {
			return cognitoError("UserNotFoundException", "user not found", http.StatusBadRequest), nil
		}
		return nil, err
	}
	user.Attributes = deleteAttributes(user.Attributes, in.UserAttributeNames)
	user.UpdatedAt = now()
	if err := p.store.UpdateUser(user); err != nil {
		return nil, err
	}
	return emptyOK(), nil
}

func (p *Provider) adminSetUserPassword(req *http.Request) (*plugin.Response, error) {
	var in struct {
		UserPoolId string `json:"UserPoolId"`
		Username   string `json:"Username"`
		Password   string `json:"Password"`
		Permanent  bool   `json:"Permanent"`
	}
	if err := json.NewDecoder(req.Body).Decode(&in); err != nil {
		return cognitoError("InvalidParameterException", "failed to parse request", http.StatusBadRequest), nil
	}
	user, err := p.store.GetUser(in.UserPoolId, in.Username)
	if err != nil {
		if err == ErrUserNotFound {
			return cognitoError("UserNotFoundException", "user not found", http.StatusBadRequest), nil
		}
		return nil, err
	}
	user.Password = in.Password
	if in.Permanent {
		user.Status = "CONFIRMED"
	}
	user.UpdatedAt = now()
	if err := p.store.UpdateUser(user); err != nil {
		return nil, err
	}
	return emptyOK(), nil
}

func (p *Provider) adminResetUserPassword(req *http.Request) (*plugin.Response, error) {
	var in struct {
		UserPoolId string `json:"UserPoolId"`
		Username   string `json:"Username"`
	}
	if err := json.NewDecoder(req.Body).Decode(&in); err != nil {
		return cognitoError("InvalidParameterException", "failed to parse request", http.StatusBadRequest), nil
	}
	user, err := p.store.GetUser(in.UserPoolId, in.Username)
	if err != nil {
		if err == ErrUserNotFound {
			return cognitoError("UserNotFoundException", "user not found", http.StatusBadRequest), nil
		}
		return nil, err
	}
	user.Status = "RESET_REQUIRED"
	user.UpdatedAt = now()
	if err := p.store.UpdateUser(user); err != nil {
		return nil, err
	}
	return emptyOK(), nil
}

func (p *Provider) adminConfirmSignUp(req *http.Request) (*plugin.Response, error) {
	var in struct {
		UserPoolId string `json:"UserPoolId"`
		Username   string `json:"Username"`
	}
	if err := json.NewDecoder(req.Body).Decode(&in); err != nil {
		return cognitoError("InvalidParameterException", "failed to parse request", http.StatusBadRequest), nil
	}
	user, err := p.store.GetUser(in.UserPoolId, in.Username)
	if err != nil {
		if err == ErrUserNotFound {
			return cognitoError("UserNotFoundException", "user not found", http.StatusBadRequest), nil
		}
		return nil, err
	}
	user.Status = "CONFIRMED"
	user.UpdatedAt = now()
	if err := p.store.UpdateUser(user); err != nil {
		return nil, err
	}
	return emptyOK(), nil
}

// --- Group handlers ---

func (p *Provider) createGroup(req *http.Request) (*plugin.Response, error) {
	var in struct {
		UserPoolId  string `json:"UserPoolId"`
		GroupName   string `json:"GroupName"`
		Description string `json:"Description"`
		RoleArn     string `json:"RoleArn"`
		Precedence  int    `json:"Precedence"`
	}
	if err := json.NewDecoder(req.Body).Decode(&in); err != nil {
		return cognitoError("InvalidParameterException", "failed to parse request", http.StatusBadRequest), nil
	}
	if in.UserPoolId == "" || in.GroupName == "" {
		return cognitoError("InvalidParameterException", "UserPoolId and GroupName are required", http.StatusBadRequest), nil
	}
	// Check pool exists
	if _, err := p.store.GetUserPool(in.UserPoolId); err != nil {
		return cognitoError("ResourceNotFoundException", "user pool not found", http.StatusBadRequest), nil
	}
	g := &Group{
		PoolID:      in.UserPoolId,
		Name:        in.GroupName,
		Description: in.Description,
		RoleARN:     in.RoleArn,
		Precedence:  in.Precedence,
		CreatedAt:   now(),
	}
	if err := p.store.CreateGroup(g); err != nil {
		return nil, err
	}
	return jsonResp(http.StatusOK, map[string]any{"Group": groupToMap(g)})
}

func (p *Provider) getGroup(req *http.Request) (*plugin.Response, error) {
	var in struct {
		UserPoolId string `json:"UserPoolId"`
		GroupName  string `json:"GroupName"`
	}
	if err := json.NewDecoder(req.Body).Decode(&in); err != nil {
		return cognitoError("InvalidParameterException", "failed to parse request", http.StatusBadRequest), nil
	}
	g, err := p.store.GetGroup(in.UserPoolId, in.GroupName)
	if err != nil {
		if err == ErrGroupNotFound {
			return cognitoError("ResourceNotFoundException", "group not found", http.StatusBadRequest), nil
		}
		return nil, err
	}
	return jsonResp(http.StatusOK, map[string]any{"Group": groupToMap(g)})
}

func (p *Provider) listGroups(req *http.Request) (*plugin.Response, error) {
	var in struct {
		UserPoolId string `json:"UserPoolId"`
	}
	if err := json.NewDecoder(req.Body).Decode(&in); err != nil {
		return cognitoError("InvalidParameterException", "failed to parse request", http.StatusBadRequest), nil
	}
	groups, err := p.store.ListGroups(in.UserPoolId)
	if err != nil {
		return nil, err
	}
	out := make([]any, 0, len(groups))
	for _, g := range groups {
		out = append(out, groupToMap(&g))
	}
	return jsonResp(http.StatusOK, map[string]any{"Groups": out})
}

func (p *Provider) updateGroup(req *http.Request) (*plugin.Response, error) {
	var in struct {
		UserPoolId  string `json:"UserPoolId"`
		GroupName   string `json:"GroupName"`
		Description string `json:"Description"`
		RoleArn     string `json:"RoleArn"`
		Precedence  int    `json:"Precedence"`
	}
	if err := json.NewDecoder(req.Body).Decode(&in); err != nil {
		return cognitoError("InvalidParameterException", "failed to parse request", http.StatusBadRequest), nil
	}
	g, err := p.store.GetGroup(in.UserPoolId, in.GroupName)
	if err != nil {
		if err == ErrGroupNotFound {
			return cognitoError("ResourceNotFoundException", "group not found", http.StatusBadRequest), nil
		}
		return nil, err
	}
	if in.Description != "" {
		g.Description = in.Description
	}
	if in.RoleArn != "" {
		g.RoleARN = in.RoleArn
	}
	g.Precedence = in.Precedence
	if err := p.store.UpdateGroup(g); err != nil {
		return nil, err
	}
	return jsonResp(http.StatusOK, map[string]any{"Group": groupToMap(g)})
}

func (p *Provider) deleteGroup(req *http.Request) (*plugin.Response, error) {
	var in struct {
		UserPoolId string `json:"UserPoolId"`
		GroupName  string `json:"GroupName"`
	}
	if err := json.NewDecoder(req.Body).Decode(&in); err != nil {
		return cognitoError("InvalidParameterException", "failed to parse request", http.StatusBadRequest), nil
	}
	if err := p.store.DeleteGroup(in.UserPoolId, in.GroupName); err != nil {
		if err == ErrGroupNotFound {
			return cognitoError("ResourceNotFoundException", "group not found", http.StatusBadRequest), nil
		}
		return nil, err
	}
	return emptyOK(), nil
}

// --- User-Group membership handlers ---

func (p *Provider) adminAddUserToGroup(req *http.Request) (*plugin.Response, error) {
	var in struct {
		UserPoolId string `json:"UserPoolId"`
		Username   string `json:"Username"`
		GroupName  string `json:"GroupName"`
	}
	if err := json.NewDecoder(req.Body).Decode(&in); err != nil {
		return cognitoError("InvalidParameterException", "failed to parse request", http.StatusBadRequest), nil
	}
	if err := p.store.AddUserToGroup(in.UserPoolId, in.Username, in.GroupName); err != nil {
		return nil, err
	}
	return emptyOK(), nil
}

func (p *Provider) adminRemoveUserFromGroup(req *http.Request) (*plugin.Response, error) {
	var in struct {
		UserPoolId string `json:"UserPoolId"`
		Username   string `json:"Username"`
		GroupName  string `json:"GroupName"`
	}
	if err := json.NewDecoder(req.Body).Decode(&in); err != nil {
		return cognitoError("InvalidParameterException", "failed to parse request", http.StatusBadRequest), nil
	}
	if err := p.store.RemoveUserFromGroup(in.UserPoolId, in.Username, in.GroupName); err != nil {
		return nil, err
	}
	return emptyOK(), nil
}

func (p *Provider) adminListGroupsForUser(req *http.Request) (*plugin.Response, error) {
	var in struct {
		UserPoolId string `json:"UserPoolId"`
		Username   string `json:"Username"`
	}
	if err := json.NewDecoder(req.Body).Decode(&in); err != nil {
		return cognitoError("InvalidParameterException", "failed to parse request", http.StatusBadRequest), nil
	}
	groups, err := p.store.ListGroupsForUser(in.UserPoolId, in.Username)
	if err != nil {
		return nil, err
	}
	out := make([]any, 0, len(groups))
	for _, g := range groups {
		out = append(out, groupToMap(&g))
	}
	return jsonResp(http.StatusOK, map[string]any{"Groups": out})
}

func (p *Provider) listUsersInGroup(req *http.Request) (*plugin.Response, error) {
	var in struct {
		UserPoolId string `json:"UserPoolId"`
		GroupName  string `json:"GroupName"`
	}
	if err := json.NewDecoder(req.Body).Decode(&in); err != nil {
		return cognitoError("InvalidParameterException", "failed to parse request", http.StatusBadRequest), nil
	}
	users, err := p.store.ListUsersInGroup(in.UserPoolId, in.GroupName)
	if err != nil {
		return nil, err
	}
	out := make([]any, 0, len(users))
	for _, u := range users {
		out = append(out, userToMap(&u))
	}
	return jsonResp(http.StatusOK, map[string]any{"Users": out})
}

// --- IdentityProvider handlers ---

func (p *Provider) createIdentityProvider(req *http.Request) (*plugin.Response, error) {
	var in struct {
		UserPoolId       string            `json:"UserPoolId"`
		ProviderName     string            `json:"ProviderName"`
		ProviderType     string            `json:"ProviderType"`
		ProviderDetails  map[string]string `json:"ProviderDetails"`
		AttributeMapping map[string]string `json:"AttributeMapping"`
		IdpIdentifiers   []string          `json:"IdpIdentifiers"`
	}
	if err := json.NewDecoder(req.Body).Decode(&in); err != nil {
		return cognitoError("InvalidParameterException", "failed to parse request", http.StatusBadRequest), nil
	}
	if in.UserPoolId == "" || in.ProviderName == "" {
		return cognitoError("InvalidParameterException", "UserPoolId and ProviderName are required", http.StatusBadRequest), nil
	}
	idp := &IdentityProvider{
		PoolID:           in.UserPoolId,
		Name:             in.ProviderName,
		Type:             in.ProviderType,
		Details:          marshalJSON(in.ProviderDetails),
		AttributeMapping: marshalJSON(in.AttributeMapping),
		IDPIdentifiers:   marshalJSON(in.IdpIdentifiers),
		CreatedAt:        now(),
	}
	if err := p.store.CreateIdentityProvider(idp); err != nil {
		return nil, err
	}
	return jsonResp(http.StatusOK, map[string]any{"IdentityProvider": idpToMap(idp)})
}

func (p *Provider) describeIdentityProvider(req *http.Request) (*plugin.Response, error) {
	var in struct {
		UserPoolId   string `json:"UserPoolId"`
		ProviderName string `json:"ProviderName"`
	}
	if err := json.NewDecoder(req.Body).Decode(&in); err != nil {
		return cognitoError("InvalidParameterException", "failed to parse request", http.StatusBadRequest), nil
	}
	idp, err := p.store.GetIdentityProvider(in.UserPoolId, in.ProviderName)
	if err != nil {
		if err == ErrIDPNotFound {
			return cognitoError("ResourceNotFoundException", "identity provider not found", http.StatusBadRequest), nil
		}
		return nil, err
	}
	return jsonResp(http.StatusOK, map[string]any{"IdentityProvider": idpToMap(idp)})
}

func (p *Provider) listIdentityProviders(req *http.Request) (*plugin.Response, error) {
	var in struct {
		UserPoolId string `json:"UserPoolId"`
	}
	if err := json.NewDecoder(req.Body).Decode(&in); err != nil {
		return cognitoError("InvalidParameterException", "failed to parse request", http.StatusBadRequest), nil
	}
	idps, err := p.store.ListIdentityProviders(in.UserPoolId)
	if err != nil {
		return nil, err
	}
	out := make([]any, 0, len(idps))
	for _, idp := range idps {
		out = append(out, map[string]any{
			"ProviderName":     idp.Name,
			"ProviderType":     idp.Type,
			"LastModifiedDate": idp.CreatedAt,
			"CreationDate":     idp.CreatedAt,
		})
	}
	return jsonResp(http.StatusOK, map[string]any{"Providers": out})
}

func (p *Provider) updateIdentityProvider(req *http.Request) (*plugin.Response, error) {
	var in struct {
		UserPoolId       string            `json:"UserPoolId"`
		ProviderName     string            `json:"ProviderName"`
		ProviderDetails  map[string]string `json:"ProviderDetails"`
		AttributeMapping map[string]string `json:"AttributeMapping"`
		IdpIdentifiers   []string          `json:"IdpIdentifiers"`
	}
	if err := json.NewDecoder(req.Body).Decode(&in); err != nil {
		return cognitoError("InvalidParameterException", "failed to parse request", http.StatusBadRequest), nil
	}
	idp, err := p.store.GetIdentityProvider(in.UserPoolId, in.ProviderName)
	if err != nil {
		if err == ErrIDPNotFound {
			return cognitoError("ResourceNotFoundException", "identity provider not found", http.StatusBadRequest), nil
		}
		return nil, err
	}
	if in.ProviderDetails != nil {
		idp.Details = marshalJSON(in.ProviderDetails)
	}
	if in.AttributeMapping != nil {
		idp.AttributeMapping = marshalJSON(in.AttributeMapping)
	}
	if in.IdpIdentifiers != nil {
		idp.IDPIdentifiers = marshalJSON(in.IdpIdentifiers)
	}
	if err := p.store.UpdateIdentityProvider(idp); err != nil {
		return nil, err
	}
	return jsonResp(http.StatusOK, map[string]any{"IdentityProvider": idpToMap(idp)})
}

func (p *Provider) deleteIdentityProvider(req *http.Request) (*plugin.Response, error) {
	var in struct {
		UserPoolId   string `json:"UserPoolId"`
		ProviderName string `json:"ProviderName"`
	}
	if err := json.NewDecoder(req.Body).Decode(&in); err != nil {
		return cognitoError("InvalidParameterException", "failed to parse request", http.StatusBadRequest), nil
	}
	if err := p.store.DeleteIdentityProvider(in.UserPoolId, in.ProviderName); err != nil {
		if err == ErrIDPNotFound {
			return cognitoError("ResourceNotFoundException", "identity provider not found", http.StatusBadRequest), nil
		}
		return nil, err
	}
	return emptyOK(), nil
}

func (p *Provider) getIdentityProviderByIdentifier(req *http.Request) (*plugin.Response, error) {
	var in struct {
		UserPoolId    string `json:"UserPoolId"`
		IdpIdentifier string `json:"IdpIdentifier"`
	}
	if err := json.NewDecoder(req.Body).Decode(&in); err != nil {
		return cognitoError("InvalidParameterException", "failed to parse request", http.StatusBadRequest), nil
	}
	idp, err := p.store.GetIdentityProviderByIdentifier(in.UserPoolId, in.IdpIdentifier)
	if err != nil {
		if err == ErrIDPNotFound {
			return cognitoError("ResourceNotFoundException", "identity provider not found", http.StatusBadRequest), nil
		}
		return nil, err
	}
	return jsonResp(http.StatusOK, map[string]any{"IdentityProvider": idpToMap(idp)})
}

// --- ResourceServer handlers ---

func (p *Provider) createResourceServer(req *http.Request) (*plugin.Response, error) {
	var in struct {
		UserPoolId string `json:"UserPoolId"`
		Identifier string `json:"Identifier"`
		Name       string `json:"Name"`
		Scopes     []any  `json:"Scopes"`
	}
	if err := json.NewDecoder(req.Body).Decode(&in); err != nil {
		return cognitoError("InvalidParameterException", "failed to parse request", http.StatusBadRequest), nil
	}
	if in.UserPoolId == "" || in.Identifier == "" {
		return cognitoError("InvalidParameterException", "UserPoolId and Identifier are required", http.StatusBadRequest), nil
	}
	rs := &ResourceServer{
		PoolID:     in.UserPoolId,
		Identifier: in.Identifier,
		Name:       in.Name,
		Scopes:     marshalJSON(in.Scopes),
	}
	if err := p.store.CreateResourceServer(rs); err != nil {
		return nil, err
	}
	return jsonResp(http.StatusOK, map[string]any{"ResourceServer": rsToMap(rs)})
}

func (p *Provider) describeResourceServer(req *http.Request) (*plugin.Response, error) {
	var in struct {
		UserPoolId string `json:"UserPoolId"`
		Identifier string `json:"Identifier"`
	}
	if err := json.NewDecoder(req.Body).Decode(&in); err != nil {
		return cognitoError("InvalidParameterException", "failed to parse request", http.StatusBadRequest), nil
	}
	rs, err := p.store.GetResourceServer(in.UserPoolId, in.Identifier)
	if err != nil {
		if err == ErrResourceServerNotFound {
			return cognitoError("ResourceNotFoundException", "resource server not found", http.StatusBadRequest), nil
		}
		return nil, err
	}
	return jsonResp(http.StatusOK, map[string]any{"ResourceServer": rsToMap(rs)})
}

func (p *Provider) listResourceServers(req *http.Request) (*plugin.Response, error) {
	var in struct {
		UserPoolId string `json:"UserPoolId"`
	}
	if err := json.NewDecoder(req.Body).Decode(&in); err != nil {
		return cognitoError("InvalidParameterException", "failed to parse request", http.StatusBadRequest), nil
	}
	servers, err := p.store.ListResourceServers(in.UserPoolId)
	if err != nil {
		return nil, err
	}
	out := make([]any, 0, len(servers))
	for _, rs := range servers {
		out = append(out, rsToMap(&rs))
	}
	return jsonResp(http.StatusOK, map[string]any{"ResourceServers": out})
}

func (p *Provider) updateResourceServer(req *http.Request) (*plugin.Response, error) {
	var in struct {
		UserPoolId string `json:"UserPoolId"`
		Identifier string `json:"Identifier"`
		Name       string `json:"Name"`
		Scopes     []any  `json:"Scopes"`
	}
	if err := json.NewDecoder(req.Body).Decode(&in); err != nil {
		return cognitoError("InvalidParameterException", "failed to parse request", http.StatusBadRequest), nil
	}
	rs, err := p.store.GetResourceServer(in.UserPoolId, in.Identifier)
	if err != nil {
		if err == ErrResourceServerNotFound {
			return cognitoError("ResourceNotFoundException", "resource server not found", http.StatusBadRequest), nil
		}
		return nil, err
	}
	if in.Name != "" {
		rs.Name = in.Name
	}
	if in.Scopes != nil {
		rs.Scopes = marshalJSON(in.Scopes)
	}
	if err := p.store.UpdateResourceServer(rs); err != nil {
		return nil, err
	}
	return jsonResp(http.StatusOK, map[string]any{"ResourceServer": rsToMap(rs)})
}

func (p *Provider) deleteResourceServer(req *http.Request) (*plugin.Response, error) {
	var in struct {
		UserPoolId string `json:"UserPoolId"`
		Identifier string `json:"Identifier"`
	}
	if err := json.NewDecoder(req.Body).Decode(&in); err != nil {
		return cognitoError("InvalidParameterException", "failed to parse request", http.StatusBadRequest), nil
	}
	if err := p.store.DeleteResourceServer(in.UserPoolId, in.Identifier); err != nil {
		if err == ErrResourceServerNotFound {
			return cognitoError("ResourceNotFoundException", "resource server not found", http.StatusBadRequest), nil
		}
		return nil, err
	}
	return emptyOK(), nil
}

// --- Auth flow handlers ---

func (p *Provider) signUp(req *http.Request) (*plugin.Response, error) {
	var in struct {
		ClientId       string              `json:"ClientId"`
		Username       string              `json:"Username"`
		Password       string              `json:"Password"`
		UserAttributes []map[string]string `json:"UserAttributes"`
	}
	if err := json.NewDecoder(req.Body).Decode(&in); err != nil {
		return cognitoError("InvalidParameterException", "failed to parse request", http.StatusBadRequest), nil
	}
	// Find pool by client
	clients, err := p.store.db().Query(`SELECT pool_id FROM user_pool_clients WHERE id=?`, in.ClientId)
	if err != nil {
		return nil, err
	}
	var poolID string
	if clients.Next() {
		_ = clients.Scan(&poolID)
	}
	clients.Close() //nolint:errcheck
	if poolID == "" {
		return cognitoError("ResourceNotFoundException", "client not found", http.StatusBadRequest), nil
	}
	if _, err := p.store.GetUser(poolID, in.Username); err == nil {
		return cognitoError("UsernameExistsException", "user already exists", http.StatusBadRequest), nil
	}
	t := now()
	user := &User{
		PoolID:      poolID,
		Username:    in.Username,
		Status:      "UNCONFIRMED",
		Enabled:     1,
		Attributes:  marshalJSON(in.UserAttributes),
		Password:    in.Password,
		MFASettings: "{}",
		CreatedAt:   t,
		UpdatedAt:   t,
	}
	if err := p.store.CreateUser(user); err != nil {
		return nil, err
	}
	return jsonResp(http.StatusOK, map[string]any{
		"UserConfirmed": false,
		"UserSub":       in.Username,
	})
}

func (p *Provider) confirmSignUp(req *http.Request) (*plugin.Response, error) {
	var in struct {
		ClientId         string `json:"ClientId"`
		Username         string `json:"Username"`
		ConfirmationCode string `json:"ConfirmationCode"`
	}
	if err := json.NewDecoder(req.Body).Decode(&in); err != nil {
		return cognitoError("InvalidParameterException", "failed to parse request", http.StatusBadRequest), nil
	}
	// Find pool by client
	row := p.store.db().QueryRow(`SELECT pool_id FROM user_pool_clients WHERE id=?`, in.ClientId)
	var poolID string
	if err := row.Scan(&poolID); err != nil {
		return cognitoError("ResourceNotFoundException", "client not found", http.StatusBadRequest), nil
	}
	user, err := p.store.GetUser(poolID, in.Username)
	if err != nil {
		if err == ErrUserNotFound {
			return cognitoError("UserNotFoundException", "user not found", http.StatusBadRequest), nil
		}
		return nil, err
	}
	user.Status = "CONFIRMED"
	user.UpdatedAt = now()
	if err := p.store.UpdateUser(user); err != nil {
		return nil, err
	}
	return emptyOK(), nil
}

func (p *Provider) initiateAuth(req *http.Request) (*plugin.Response, error) {
	var in struct {
		AuthFlow       string            `json:"AuthFlow"`
		ClientId       string            `json:"ClientId"`
		AuthParameters map[string]string `json:"AuthParameters"`
	}
	if err := json.NewDecoder(req.Body).Decode(&in); err != nil {
		return cognitoError("InvalidParameterException", "failed to parse request", http.StatusBadRequest), nil
	}
	// Find pool by client
	row := p.store.db().QueryRow(`SELECT pool_id FROM user_pool_clients WHERE id=?`, in.ClientId)
	var poolID string
	if err := row.Scan(&poolID); err != nil {
		return cognitoError("ResourceNotFoundException", "client not found", http.StatusBadRequest), nil
	}
	username := in.AuthParameters["USERNAME"]
	if username == "" {
		username = "unknown"
	}
	tokens := makeDummyTokens(poolID, username)
	return jsonResp(http.StatusOK, map[string]any{
		"AuthenticationResult": tokens,
		"ChallengeName":        nil,
	})
}

func (p *Provider) adminInitiateAuth(req *http.Request) (*plugin.Response, error) {
	var in struct {
		UserPoolId     string            `json:"UserPoolId"`
		ClientId       string            `json:"ClientId"`
		AuthFlow       string            `json:"AuthFlow"`
		AuthParameters map[string]string `json:"AuthParameters"`
	}
	if err := json.NewDecoder(req.Body).Decode(&in); err != nil {
		return cognitoError("InvalidParameterException", "failed to parse request", http.StatusBadRequest), nil
	}
	username := in.AuthParameters["USERNAME"]
	if username == "" {
		username = "unknown"
	}
	tokens := makeDummyTokens(in.UserPoolId, username)
	return jsonResp(http.StatusOK, map[string]any{
		"AuthenticationResult": tokens,
		"ChallengeName":        nil,
	})
}

func (p *Provider) respondToAuthChallenge(req *http.Request) (*plugin.Response, error) {
	var in struct {
		ClientId           string            `json:"ClientId"`
		ChallengeName      string            `json:"ChallengeName"`
		ChallengeResponses map[string]string `json:"ChallengeResponses"`
	}
	if err := json.NewDecoder(req.Body).Decode(&in); err != nil {
		return cognitoError("InvalidParameterException", "failed to parse request", http.StatusBadRequest), nil
	}
	row := p.store.db().QueryRow(`SELECT pool_id FROM user_pool_clients WHERE id=?`, in.ClientId)
	var poolID string
	if err := row.Scan(&poolID); err != nil {
		poolID = "unknown"
	}
	username := in.ChallengeResponses["USERNAME"]
	if username == "" {
		username = "unknown"
	}
	tokens := makeDummyTokens(poolID, username)
	return jsonResp(http.StatusOK, map[string]any{"AuthenticationResult": tokens})
}

func (p *Provider) adminRespondToAuthChallenge(req *http.Request) (*plugin.Response, error) {
	var in struct {
		UserPoolId         string            `json:"UserPoolId"`
		ClientId           string            `json:"ClientId"`
		ChallengeName      string            `json:"ChallengeName"`
		ChallengeResponses map[string]string `json:"ChallengeResponses"`
	}
	if err := json.NewDecoder(req.Body).Decode(&in); err != nil {
		return cognitoError("InvalidParameterException", "failed to parse request", http.StatusBadRequest), nil
	}
	username := in.ChallengeResponses["USERNAME"]
	if username == "" {
		username = "unknown"
	}
	tokens := makeDummyTokens(in.UserPoolId, username)
	return jsonResp(http.StatusOK, map[string]any{"AuthenticationResult": tokens})
}

func (p *Provider) forgotPassword(req *http.Request) (*plugin.Response, error) {
	var in struct {
		ClientId string `json:"ClientId"`
		Username string `json:"Username"`
	}
	if err := json.NewDecoder(req.Body).Decode(&in); err != nil {
		return cognitoError("InvalidParameterException", "failed to parse request", http.StatusBadRequest), nil
	}
	return jsonResp(http.StatusOK, map[string]any{
		"CodeDeliveryDetails": map[string]string{
			"Destination":    "devcloud@local",
			"DeliveryMedium": "EMAIL",
			"AttributeName":  "email",
		},
	})
}

func (p *Provider) confirmForgotPassword(req *http.Request) (*plugin.Response, error) {
	var in struct {
		ClientId         string `json:"ClientId"`
		Username         string `json:"Username"`
		ConfirmationCode string `json:"ConfirmationCode"`
		Password         string `json:"Password"`
	}
	if err := json.NewDecoder(req.Body).Decode(&in); err != nil {
		return cognitoError("InvalidParameterException", "failed to parse request", http.StatusBadRequest), nil
	}
	// Find pool by client
	row := p.store.db().QueryRow(`SELECT pool_id FROM user_pool_clients WHERE id=?`, in.ClientId)
	var poolID string
	if err := row.Scan(&poolID); err == nil {
		user, err := p.store.GetUser(poolID, in.Username)
		if err == nil {
			user.Password = in.Password
			user.Status = "CONFIRMED"
			user.UpdatedAt = now()
			_ = p.store.UpdateUser(user)
		}
	}
	return emptyOK(), nil
}

func (p *Provider) changePassword(req *http.Request) (*plugin.Response, error) {
	// Simplified: just return success
	return emptyOK(), nil
}

func (p *Provider) globalSignOut(req *http.Request) (*plugin.Response, error) {
	return emptyOK(), nil
}

func (p *Provider) adminUserGlobalSignOut(req *http.Request) (*plugin.Response, error) {
	return emptyOK(), nil
}

func (p *Provider) revokeToken(req *http.Request) (*plugin.Response, error) {
	return emptyOK(), nil
}

func (p *Provider) getUser(req *http.Request) (*plugin.Response, error) {
	// GetUser uses AccessToken (not pool/username). We decode the token.
	var in struct {
		AccessToken string `json:"AccessToken"`
	}
	if err := json.NewDecoder(req.Body).Decode(&in); err != nil {
		return cognitoError("InvalidParameterException", "failed to parse request", http.StatusBadRequest), nil
	}
	// Decode our dummy token
	payload, err := base64.StdEncoding.DecodeString(in.AccessToken)
	if err != nil {
		return cognitoError("NotAuthorizedException", "invalid token", http.StatusBadRequest), nil
	}
	var claims map[string]any
	if err := json.Unmarshal(payload, &claims); err != nil {
		return cognitoError("NotAuthorizedException", "invalid token", http.StatusBadRequest), nil
	}
	poolID, _ := claims["pool_id"].(string)
	username, _ := claims["sub"].(string)
	if poolID == "" || username == "" {
		return cognitoError("NotAuthorizedException", "invalid token claims", http.StatusBadRequest), nil
	}
	user, err := p.store.GetUser(poolID, username)
	if err != nil {
		if err == ErrUserNotFound {
			return cognitoError("UserNotFoundException", "user not found", http.StatusBadRequest), nil
		}
		return nil, err
	}
	return jsonResp(http.StatusOK, userToMap(user))
}

// --- MFA config ---

func (p *Provider) setUserPoolMfaConfig(req *http.Request) (*plugin.Response, error) {
	var in struct {
		UserPoolId       string `json:"UserPoolId"`
		MfaConfiguration string `json:"MfaConfiguration"`
	}
	if err := json.NewDecoder(req.Body).Decode(&in); err != nil {
		return cognitoError("InvalidParameterException", "failed to parse request", http.StatusBadRequest), nil
	}
	pool, err := p.store.GetUserPool(in.UserPoolId)
	if err != nil {
		if err == ErrPoolNotFound {
			return cognitoError("ResourceNotFoundException", "user pool not found", http.StatusBadRequest), nil
		}
		return nil, err
	}
	pool.MFAConfig = in.MfaConfiguration
	pool.UpdatedAt = now()
	if err := p.store.UpdateUserPool(pool); err != nil {
		return nil, err
	}
	return jsonResp(http.StatusOK, map[string]any{"MfaConfiguration": pool.MFAConfig})
}

func (p *Provider) getUserPoolMfaConfig(req *http.Request) (*plugin.Response, error) {
	var in struct {
		UserPoolId string `json:"UserPoolId"`
	}
	if err := json.NewDecoder(req.Body).Decode(&in); err != nil {
		return cognitoError("InvalidParameterException", "failed to parse request", http.StatusBadRequest), nil
	}
	pool, err := p.store.GetUserPool(in.UserPoolId)
	if err != nil {
		if err == ErrPoolNotFound {
			return cognitoError("ResourceNotFoundException", "user pool not found", http.StatusBadRequest), nil
		}
		return nil, err
	}
	return jsonResp(http.StatusOK, map[string]any{"MfaConfiguration": pool.MFAConfig})
}

// --- Tag handlers ---

func (p *Provider) tagResource(req *http.Request) (*plugin.Response, error) {
	var in struct {
		ResourceArn string            `json:"ResourceArn"`
		Tags        map[string]string `json:"Tags"`
	}
	if err := json.NewDecoder(req.Body).Decode(&in); err != nil {
		return cognitoError("InvalidParameterException", "failed to parse request", http.StatusBadRequest), nil
	}
	if err := p.store.TagResource(in.ResourceArn, in.Tags); err != nil {
		return nil, err
	}
	return emptyOK(), nil
}

func (p *Provider) untagResource(req *http.Request) (*plugin.Response, error) {
	var in struct {
		ResourceArn string   `json:"ResourceArn"`
		TagKeys     []string `json:"TagKeys"`
	}
	if err := json.NewDecoder(req.Body).Decode(&in); err != nil {
		return cognitoError("InvalidParameterException", "failed to parse request", http.StatusBadRequest), nil
	}
	if err := p.store.UntagResource(in.ResourceArn, in.TagKeys); err != nil {
		return nil, err
	}
	return emptyOK(), nil
}

func (p *Provider) listTagsForResource(req *http.Request) (*plugin.Response, error) {
	var in struct {
		ResourceArn string `json:"ResourceArn"`
	}
	if err := json.NewDecoder(req.Body).Decode(&in); err != nil {
		return cognitoError("InvalidParameterException", "failed to parse request", http.StatusBadRequest), nil
	}
	tags, err := p.store.ListTagsForResource(in.ResourceArn)
	if err != nil {
		return nil, err
	}
	return jsonResp(http.StatusOK, map[string]any{"Tags": tags})
}
