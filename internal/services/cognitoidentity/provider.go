// SPDX-License-Identifier: Apache-2.0

package cognitoidentity

import (
	"context"
	"crypto/rand"
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

// Provider implements the CognitoIdentityService using JSON 1.1 protocol.
type Provider struct {
	store *CognitoIdentityStore
}

func (p *Provider) ServiceID() string             { return "cognitoidentity" }
func (p *Provider) ServiceName() string           { return "CognitoIdentityService" }
func (p *Provider) Protocol() plugin.ProtocolType { return plugin.ProtocolJSON11 }

func (p *Provider) Init(cfg plugin.PluginConfig) error {
	dataDir := cfg.DataDir
	if dataDir == "" {
		dataDir = "."
	}
	var err error
	p.store, err = NewCognitoIdentityStore(filepath.Join(dataDir, "cognitoidentity"))
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
		op = strings.TrimPrefix(target, "AmazonCognitoIdentity.")
	}
	switch op {
	case "CreateIdentityPool":
		return p.createIdentityPool(req)
	case "DescribeIdentityPool":
		return p.describeIdentityPool(req)
	case "ListIdentityPools":
		return p.listIdentityPools(req)
	case "UpdateIdentityPool":
		return p.updateIdentityPool(req)
	case "DeleteIdentityPool":
		return p.deleteIdentityPool(req)
	case "GetId":
		return p.getId(req)
	case "DescribeIdentity":
		return p.describeIdentity(req)
	case "ListIdentities":
		return p.listIdentities(req)
	case "DeleteIdentities":
		return p.deleteIdentities(req)
	case "GetCredentialsForIdentity":
		return p.getCredentialsForIdentity(req)
	case "GetOpenIdToken":
		return p.getOpenIdToken(req)
	case "GetOpenIdTokenForDeveloperIdentity":
		return p.getOpenIdTokenForDeveloperIdentity(req)
	case "SetIdentityPoolRoles":
		return p.setIdentityPoolRoles(req)
	case "GetIdentityPoolRoles":
		return p.getIdentityPoolRoles(req)
	case "LookupDeveloperIdentity":
		return p.lookupDeveloperIdentity(req)
	case "MergeDeveloperIdentities":
		return p.mergeDeveloperIdentities(req)
	case "UnlinkIdentity":
		return p.unlinkIdentity(req)
	case "UnlinkDeveloperIdentity":
		return p.unlinkDeveloperIdentity(req)
	case "SetPrincipalTagAttributeMap":
		return p.setPrincipalTagAttributeMap(req)
	case "GetPrincipalTagAttributeMap":
		return p.getPrincipalTagAttributeMap(req)
	case "TagResource":
		return p.tagResource(req)
	case "UntagResource":
		return p.untagResource(req)
	case "ListTagsForResource":
		return p.listTagsForResource(req)
	default:
		return ciError("InvalidAction", fmt.Sprintf("operation not implemented: %s", op), http.StatusBadRequest), nil
	}
}

func (p *Provider) ListResources(_ context.Context) ([]plugin.Resource, error) {
	pools, err := p.store.ListPools()
	if err != nil {
		return nil, err
	}
	out := make([]plugin.Resource, 0, len(pools))
	for _, pool := range pools {
		out = append(out, plugin.Resource{Type: "identity-pool", ID: pool.ARN, Name: pool.Name})
	}
	return out, nil
}

func (p *Provider) GetMetrics(_ context.Context) (*plugin.ServiceMetrics, error) {
	return &plugin.ServiceMetrics{}, nil
}

// --- helpers ---

func arnForPool(accountID, region, id string) string {
	return fmt.Sprintf("arn:aws:cognito-identity:%s:%s:identitypool/%s", region, accountID, id)
}

func newID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

func ciError(code, msg string, status int) *plugin.Response {
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

func poolToMap(pool *IdentityPool) map[string]any {
	return map[string]any{
		"IdentityPoolId":                 pool.ID,
		"IdentityPoolName":               pool.Name,
		"AllowUnauthenticatedIdentities": pool.AllowUnauthenticated,
		"DeveloperProviderName":          pool.DeveloperProvider,
		"SupportedLoginProviders":        pool.SupportedLogins,
		"CognitoIdentityProviders":       pool.CognitoIDPs,
	}
}

// --- operation handlers ---

func (p *Provider) createIdentityPool(req *http.Request) (*plugin.Response, error) {
	var in struct {
		IdentityPoolName               string            `json:"IdentityPoolName"`
		AllowUnauthenticatedIdentities bool              `json:"AllowUnauthenticatedIdentities"`
		DeveloperProviderName          string            `json:"DeveloperProviderName"`
		SupportedLoginProviders        map[string]string `json:"SupportedLoginProviders"`
		CognitoIdentityProviders       []map[string]any  `json:"CognitoIdentityProviders"`
	}
	if err := json.NewDecoder(req.Body).Decode(&in); err != nil {
		return ciError("ValidationException", "failed to parse request body", http.StatusBadRequest), nil
	}
	if in.IdentityPoolName == "" {
		return ciError("ValidationException", "IdentityPoolName is required", http.StatusBadRequest), nil
	}

	id := "us-east-1:" + newID()
	arn := arnForPool(defaultAccountID, defaultRegion, id)

	supportedLogins := in.SupportedLoginProviders
	if supportedLogins == nil {
		supportedLogins = map[string]string{}
	}
	cognitoIDPs := in.CognitoIdentityProviders
	if cognitoIDPs == nil {
		cognitoIDPs = []map[string]any{}
	}

	pool := &IdentityPool{
		ID:                   id,
		ARN:                  arn,
		Name:                 in.IdentityPoolName,
		AllowUnauthenticated: in.AllowUnauthenticatedIdentities,
		DeveloperProvider:    in.DeveloperProviderName,
		SupportedLogins:      supportedLogins,
		CognitoIDPs:          cognitoIDPs,
		Roles:                map[string]string{},
		CreatedAt:            time.Now(),
	}
	if err := p.store.PutPool(pool); err != nil {
		return nil, err
	}
	return jsonResp(http.StatusOK, poolToMap(pool))
}

func (p *Provider) describeIdentityPool(req *http.Request) (*plugin.Response, error) {
	var in struct {
		IdentityPoolId string `json:"IdentityPoolId"`
	}
	if err := json.NewDecoder(req.Body).Decode(&in); err != nil {
		return ciError("ValidationException", "failed to parse request", http.StatusBadRequest), nil
	}
	pool, err := p.store.GetPool(in.IdentityPoolId)
	if err != nil {
		if err == ErrPoolNotFound {
			return ciError("ResourceNotFoundException", "identity pool not found", http.StatusBadRequest), nil
		}
		return nil, err
	}
	return jsonResp(http.StatusOK, poolToMap(pool))
}

func (p *Provider) listIdentityPools(req *http.Request) (*plugin.Response, error) {
	pools, err := p.store.ListPools()
	if err != nil {
		return nil, err
	}
	items := make([]map[string]any, 0, len(pools))
	for _, pool := range pools {
		items = append(items, map[string]any{
			"IdentityPoolId":   pool.ID,
			"IdentityPoolName": pool.Name,
		})
	}
	return jsonResp(http.StatusOK, map[string]any{"IdentityPools": items})
}

func (p *Provider) updateIdentityPool(req *http.Request) (*plugin.Response, error) {
	var in struct {
		IdentityPoolId                 string            `json:"IdentityPoolId"`
		IdentityPoolName               string            `json:"IdentityPoolName"`
		AllowUnauthenticatedIdentities bool              `json:"AllowUnauthenticatedIdentities"`
		DeveloperProviderName          string            `json:"DeveloperProviderName"`
		SupportedLoginProviders        map[string]string `json:"SupportedLoginProviders"`
		CognitoIdentityProviders       []map[string]any  `json:"CognitoIdentityProviders"`
	}
	if err := json.NewDecoder(req.Body).Decode(&in); err != nil {
		return ciError("ValidationException", "failed to parse request", http.StatusBadRequest), nil
	}
	pool, err := p.store.GetPool(in.IdentityPoolId)
	if err != nil {
		if err == ErrPoolNotFound {
			return ciError("ResourceNotFoundException", "identity pool not found", http.StatusBadRequest), nil
		}
		return nil, err
	}
	if in.IdentityPoolName != "" {
		pool.Name = in.IdentityPoolName
	}
	pool.AllowUnauthenticated = in.AllowUnauthenticatedIdentities
	if in.DeveloperProviderName != "" {
		pool.DeveloperProvider = in.DeveloperProviderName
	}
	if in.SupportedLoginProviders != nil {
		pool.SupportedLogins = in.SupportedLoginProviders
	}
	if in.CognitoIdentityProviders != nil {
		pool.CognitoIDPs = in.CognitoIdentityProviders
	}
	if err := p.store.PutPool(pool); err != nil {
		return nil, err
	}
	return jsonResp(http.StatusOK, poolToMap(pool))
}

func (p *Provider) deleteIdentityPool(req *http.Request) (*plugin.Response, error) {
	var in struct {
		IdentityPoolId string `json:"IdentityPoolId"`
	}
	if err := json.NewDecoder(req.Body).Decode(&in); err != nil {
		return ciError("ValidationException", "failed to parse request", http.StatusBadRequest), nil
	}
	if err := p.store.DeletePool(in.IdentityPoolId); err != nil {
		if err == ErrPoolNotFound {
			return ciError("ResourceNotFoundException", "identity pool not found", http.StatusBadRequest), nil
		}
		return nil, err
	}
	return emptyOK()
}

func (p *Provider) getId(req *http.Request) (*plugin.Response, error) {
	var in struct {
		IdentityPoolId string            `json:"IdentityPoolId"`
		AccountId      string            `json:"AccountId"`
		Logins         map[string]string `json:"Logins"`
	}
	if err := json.NewDecoder(req.Body).Decode(&in); err != nil {
		return ciError("ValidationException", "failed to parse request", http.StatusBadRequest), nil
	}
	if _, err := p.store.GetPool(in.IdentityPoolId); err != nil {
		if err == ErrPoolNotFound {
			return ciError("ResourceNotFoundException", "identity pool not found", http.StatusBadRequest), nil
		}
		return nil, err
	}

	id := in.IdentityPoolId + ":" + newID()
	logins := make([]string, 0, len(in.Logins))
	for k := range in.Logins {
		logins = append(logins, k)
	}
	identity := &Identity{
		ID:        id,
		PoolID:    in.IdentityPoolId,
		Logins:    logins,
		CreatedAt: time.Now(),
	}
	if err := p.store.PutIdentity(identity); err != nil {
		return nil, err
	}
	return jsonResp(http.StatusOK, map[string]string{"IdentityId": id})
}

func (p *Provider) describeIdentity(req *http.Request) (*plugin.Response, error) {
	var in struct {
		IdentityId string `json:"IdentityId"`
	}
	if err := json.NewDecoder(req.Body).Decode(&in); err != nil {
		return ciError("ValidationException", "failed to parse request", http.StatusBadRequest), nil
	}
	identity, err := p.store.GetIdentity(in.IdentityId)
	if err != nil {
		if err == ErrIdentityNotFound {
			return ciError("ResourceNotFoundException", "identity not found", http.StatusBadRequest), nil
		}
		return nil, err
	}
	return jsonResp(http.StatusOK, map[string]any{
		"IdentityId":       identity.ID,
		"Logins":           identity.Logins,
		"CreationDate":     identity.CreatedAt.Unix(),
		"LastModifiedDate": identity.CreatedAt.Unix(),
	})
}

func (p *Provider) listIdentities(req *http.Request) (*plugin.Response, error) {
	var in struct {
		IdentityPoolId string `json:"IdentityPoolId"`
		MaxResults     int    `json:"MaxResults"`
	}
	if err := json.NewDecoder(req.Body).Decode(&in); err != nil {
		return ciError("ValidationException", "failed to parse request", http.StatusBadRequest), nil
	}
	identities, err := p.store.ListIdentities(in.IdentityPoolId)
	if err != nil {
		return nil, err
	}
	items := make([]map[string]any, 0, len(identities))
	for _, i := range identities {
		items = append(items, map[string]any{
			"IdentityId":       i.ID,
			"Logins":           i.Logins,
			"CreationDate":     i.CreatedAt.Unix(),
			"LastModifiedDate": i.CreatedAt.Unix(),
		})
	}
	return jsonResp(http.StatusOK, map[string]any{
		"IdentityPoolId": in.IdentityPoolId,
		"Identities":     items,
	})
}

func (p *Provider) deleteIdentities(req *http.Request) (*plugin.Response, error) {
	var in struct {
		IdentityIdsToDelete []string `json:"IdentityIdsToDelete"`
	}
	if err := json.NewDecoder(req.Body).Decode(&in); err != nil {
		return ciError("ValidationException", "failed to parse request", http.StatusBadRequest), nil
	}
	if err := p.store.DeleteIdentities(in.IdentityIdsToDelete); err != nil {
		return nil, err
	}
	return jsonResp(http.StatusOK, map[string]any{"UnprocessedIdentityIds": []any{}})
}

func (p *Provider) getCredentialsForIdentity(req *http.Request) (*plugin.Response, error) {
	var in struct {
		IdentityId string `json:"IdentityId"`
	}
	if err := json.NewDecoder(req.Body).Decode(&in); err != nil {
		return ciError("ValidationException", "failed to parse request", http.StatusBadRequest), nil
	}
	if _, err := p.store.GetIdentity(in.IdentityId); err != nil {
		if err == ErrIdentityNotFound {
			return ciError("ResourceNotFoundException", "identity not found", http.StatusBadRequest), nil
		}
		return nil, err
	}
	expiry := time.Now().Add(time.Hour).UTC()
	return jsonResp(http.StatusOK, map[string]any{
		"IdentityId": in.IdentityId,
		"Credentials": map[string]any{
			"AccessKeyId":  "ASIADEVCLOUDCOGNITO00",
			"SecretKey":    "devcloud-dummy-secret-key",
			"SessionToken": "devcloud-dummy-session-token",
			"Expiration":   expiry.Unix(),
		},
	})
}

func (p *Provider) getOpenIdToken(req *http.Request) (*plugin.Response, error) {
	var in struct {
		IdentityId string `json:"IdentityId"`
	}
	if err := json.NewDecoder(req.Body).Decode(&in); err != nil {
		return ciError("ValidationException", "failed to parse request", http.StatusBadRequest), nil
	}
	if _, err := p.store.GetIdentity(in.IdentityId); err != nil {
		if err == ErrIdentityNotFound {
			return ciError("ResourceNotFoundException", "identity not found", http.StatusBadRequest), nil
		}
		return nil, err
	}
	return jsonResp(http.StatusOK, map[string]string{
		"IdentityId": in.IdentityId,
		"Token":      "devcloud-dummy-openid-token-" + in.IdentityId,
	})
}

func (p *Provider) getOpenIdTokenForDeveloperIdentity(req *http.Request) (*plugin.Response, error) {
	var in struct {
		IdentityPoolId string `json:"IdentityPoolId"`
		IdentityId     string `json:"IdentityId"`
	}
	if err := json.NewDecoder(req.Body).Decode(&in); err != nil {
		return ciError("ValidationException", "failed to parse request", http.StatusBadRequest), nil
	}
	identityId := in.IdentityId
	if identityId == "" {
		identityId = in.IdentityPoolId + ":" + newID()
	}
	return jsonResp(http.StatusOK, map[string]string{
		"IdentityId": identityId,
		"Token":      "devcloud-dummy-developer-token-" + identityId,
	})
}

func (p *Provider) setIdentityPoolRoles(req *http.Request) (*plugin.Response, error) {
	var in struct {
		IdentityPoolId string            `json:"IdentityPoolId"`
		Roles          map[string]string `json:"Roles"`
	}
	if err := json.NewDecoder(req.Body).Decode(&in); err != nil {
		return ciError("ValidationException", "failed to parse request", http.StatusBadRequest), nil
	}
	pool, err := p.store.GetPool(in.IdentityPoolId)
	if err != nil {
		if err == ErrPoolNotFound {
			return ciError("ResourceNotFoundException", "identity pool not found", http.StatusBadRequest), nil
		}
		return nil, err
	}
	if in.Roles != nil {
		pool.Roles = in.Roles
	}
	if err := p.store.PutPool(pool); err != nil {
		return nil, err
	}
	return emptyOK()
}

func (p *Provider) getIdentityPoolRoles(req *http.Request) (*plugin.Response, error) {
	var in struct {
		IdentityPoolId string `json:"IdentityPoolId"`
	}
	if err := json.NewDecoder(req.Body).Decode(&in); err != nil {
		return ciError("ValidationException", "failed to parse request", http.StatusBadRequest), nil
	}
	pool, err := p.store.GetPool(in.IdentityPoolId)
	if err != nil {
		if err == ErrPoolNotFound {
			return ciError("ResourceNotFoundException", "identity pool not found", http.StatusBadRequest), nil
		}
		return nil, err
	}
	return jsonResp(http.StatusOK, map[string]any{
		"IdentityPoolId": pool.ID,
		"Roles":          pool.Roles,
	})
}

func (p *Provider) lookupDeveloperIdentity(req *http.Request) (*plugin.Response, error) {
	var in struct {
		IdentityPoolId string `json:"IdentityPoolId"`
		IdentityId     string `json:"IdentityId"`
	}
	if err := json.NewDecoder(req.Body).Decode(&in); err != nil {
		return ciError("ValidationException", "failed to parse request", http.StatusBadRequest), nil
	}
	return jsonResp(http.StatusOK, map[string]any{
		"IdentityId":                  in.IdentityId,
		"DeveloperUserIdentifierList": []string{},
	})
}

func (p *Provider) mergeDeveloperIdentities(req *http.Request) (*plugin.Response, error) {
	var in struct {
		IdentityPoolId            string `json:"IdentityPoolId"`
		SourceUserIdentifier      string `json:"SourceUserIdentifier"`
		DestinationUserIdentifier string `json:"DestinationUserIdentifier"`
		DeveloperProviderName     string `json:"DeveloperProviderName"`
	}
	if err := json.NewDecoder(req.Body).Decode(&in); err != nil {
		return ciError("ValidationException", "failed to parse request", http.StatusBadRequest), nil
	}
	mergedID := in.IdentityPoolId + ":" + newID()
	return jsonResp(http.StatusOK, map[string]string{"IdentityId": mergedID})
}

func (p *Provider) unlinkIdentity(req *http.Request) (*plugin.Response, error) {
	var in struct {
		IdentityId     string            `json:"IdentityId"`
		Logins         map[string]string `json:"Logins"`
		LoginsToRemove []string          `json:"LoginsToRemove"`
	}
	if err := json.NewDecoder(req.Body).Decode(&in); err != nil {
		return ciError("ValidationException", "failed to parse request", http.StatusBadRequest), nil
	}
	return emptyOK()
}

func (p *Provider) unlinkDeveloperIdentity(req *http.Request) (*plugin.Response, error) {
	var in struct {
		IdentityId              string `json:"IdentityId"`
		IdentityPoolId          string `json:"IdentityPoolId"`
		DeveloperProviderName   string `json:"DeveloperProviderName"`
		DeveloperUserIdentifier string `json:"DeveloperUserIdentifier"`
	}
	if err := json.NewDecoder(req.Body).Decode(&in); err != nil {
		return ciError("ValidationException", "failed to parse request", http.StatusBadRequest), nil
	}
	return emptyOK()
}

func (p *Provider) setPrincipalTagAttributeMap(req *http.Request) (*plugin.Response, error) {
	var in struct {
		IdentityPoolId       string            `json:"IdentityPoolId"`
		IdentityProviderName string            `json:"IdentityProviderName"`
		PrincipalTags        map[string]string `json:"PrincipalTags"`
		UseDefaults          bool              `json:"UseDefaults"`
	}
	if err := json.NewDecoder(req.Body).Decode(&in); err != nil {
		return ciError("ValidationException", "failed to parse request", http.StatusBadRequest), nil
	}
	return jsonResp(http.StatusOK, map[string]any{
		"IdentityPoolId":       in.IdentityPoolId,
		"IdentityProviderName": in.IdentityProviderName,
		"PrincipalTags":        in.PrincipalTags,
		"UseDefaults":          in.UseDefaults,
	})
}

func (p *Provider) getPrincipalTagAttributeMap(req *http.Request) (*plugin.Response, error) {
	var in struct {
		IdentityPoolId       string `json:"IdentityPoolId"`
		IdentityProviderName string `json:"IdentityProviderName"`
	}
	if err := json.NewDecoder(req.Body).Decode(&in); err != nil {
		return ciError("ValidationException", "failed to parse request", http.StatusBadRequest), nil
	}
	return jsonResp(http.StatusOK, map[string]any{
		"IdentityPoolId":       in.IdentityPoolId,
		"IdentityProviderName": in.IdentityProviderName,
		"PrincipalTags":        map[string]string{},
		"UseDefaults":          true,
	})
}

func (p *Provider) tagResource(req *http.Request) (*plugin.Response, error) {
	var in struct {
		ResourceArn string            `json:"ResourceArn"`
		Tags        map[string]string `json:"Tags"`
	}
	if err := json.NewDecoder(req.Body).Decode(&in); err != nil {
		return ciError("ValidationException", "failed to parse request", http.StatusBadRequest), nil
	}
	if _, err := p.store.GetPoolByARN(in.ResourceArn); err != nil {
		return ciError("ResourceNotFoundException", "resource not found", http.StatusBadRequest), nil
	}
	if err := p.store.AddTags(in.ResourceArn, in.Tags); err != nil {
		return nil, err
	}
	return emptyOK()
}

func (p *Provider) untagResource(req *http.Request) (*plugin.Response, error) {
	var in struct {
		ResourceArn string   `json:"ResourceArn"`
		TagKeys     []string `json:"TagKeys"`
	}
	if err := json.NewDecoder(req.Body).Decode(&in); err != nil {
		return ciError("ValidationException", "failed to parse request", http.StatusBadRequest), nil
	}
	if err := p.store.RemoveTags(in.ResourceArn, in.TagKeys); err != nil {
		return nil, err
	}
	return emptyOK()
}

func (p *Provider) listTagsForResource(req *http.Request) (*plugin.Response, error) {
	var in struct {
		ResourceArn string `json:"ResourceArn"`
	}
	if err := json.NewDecoder(req.Body).Decode(&in); err != nil {
		return ciError("ValidationException", "failed to parse request", http.StatusBadRequest), nil
	}
	tags, err := p.store.ListTags(in.ResourceArn)
	if err != nil {
		return nil, err
	}
	return jsonResp(http.StatusOK, map[string]any{"Tags": tags})
}
