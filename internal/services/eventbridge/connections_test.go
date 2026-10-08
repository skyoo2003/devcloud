// SPDX-License-Identifier: Apache-2.0
package eventbridge

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestConnectionOperationDeauthorizeAndReauthorize(t *testing.T) {
	for _, auth := range []struct{ kind, params string }{
		{"API_KEY", `{"ApiKeyAuthParameters":{"ApiKeyName":"key","ApiKeyValue":"secret-key"}}`},
		{"BASIC", `{"BasicAuthParameters":{"Username":"user","Password":"secret-password"}}`},
	} {
		t.Run(auth.kind, func(t *testing.T) {
			p := newTestProvider(t)
			response := call(t, p, "CreateConnection", `{"Name":"local","Description":"keep","AuthorizationType":"`+auth.kind+`","AuthParameters":`+auth.params+`}`)
			require.Equal(t, 200, response.StatusCode)
			created := parseJSON(t, response)
			require.Equal(t, "AUTHORIZED", created["ConnectionState"])
			require.Equal(t, 400, call(t, p, "CreateConnection", `{"Name":"local","AuthorizationType":"`+auth.kind+`","AuthParameters":`+auth.params+`}`).StatusCode)
			before, err := p.store.GetConnection("local", defaultAccountID)
			require.NoError(t, err)
			describe := call(t, p, "DescribeConnection", `{"Name":"local"}`)
			require.Equal(t, 200, describe.StatusCode)
			require.NotContains(t, string(describe.Body), "secret-")
			require.NotContains(t, string(describe.Body), "Password")
			require.NotContains(t, string(describe.Body), "ApiKeyValue")
			for i := 0; i < 2; i++ {
				response = call(t, p, "DeauthorizeConnection", `{"Name":"local"}`)
				require.Equal(t, 200, response.StatusCode)
				require.Equal(t, "DEAUTHORIZED", parseJSON(t, response)["ConnectionState"])
			}
			after, err := p.store.GetConnection("local", defaultAccountID)
			require.NoError(t, err)
			require.Empty(t, after.AuthParameters)
			require.Equal(t, before.ARN, after.ARN)
			require.Equal(t, before.CreationTime, after.CreationTime)
			require.Equal(t, before.LastAuthorizedTime, after.LastAuthorizedTime)
			require.False(t, after.LastModifiedTime.Before(before.LastModifiedTime))
			require.Equal(t, "keep", after.Description)
			described := parseJSON(t, call(t, p, "DescribeConnection", `{"Name":"local"}`))
			require.Equal(t, "DEAUTHORIZED", described["ConnectionState"])
			require.NotContains(t, described, "AuthParameters")
			response = call(t, p, "UpdateConnection", `{"Name":"local","AuthParameters":`+auth.params+`}`)
			require.Equal(t, 200, response.StatusCode)
			require.Equal(t, "AUTHORIZED", parseJSON(t, response)["ConnectionState"])
			response = call(t, p, "DeleteConnection", `{"Name":"local"}`)
			require.Equal(t, 200, response.StatusCode)
			require.Equal(t, "DELETING", parseJSON(t, response)["ConnectionState"])
			require.Equal(t, 400, call(t, p, "DescribeConnection", `{"Name":"local"}`).StatusCode)
		})
	}
}

func TestConnectionOperationValidationAndUpdatePreservation(t *testing.T) {
	p := newTestProvider(t)
	for _, body := range []string{`{"Name":"bad/name","AuthorizationType":"BASIC","AuthParameters":{}}`, `{"Name":"valid","AuthorizationType":"UNKNOWN","AuthParameters":{}}`, `{"Name":"valid","AuthorizationType":"API_KEY","AuthParameters":{"ApiKeyAuthParameters":{"ApiKeyName":"key"}}}`} {
		require.Equal(t, 400, call(t, p, "CreateConnection", body).StatusCode)
	}
	require.Equal(t, 200, call(t, p, "CreateConnection", `{"Name":"valid","AuthorizationType":"API_KEY","AuthParameters":{"ApiKeyAuthParameters":{"ApiKeyName":"key","ApiKeyValue":"secret"}},"KmsKeyIdentifier":"local-metadata"}`).StatusCode)
	before, err := p.store.GetConnection("valid", defaultAccountID)
	require.NoError(t, err)
	require.Equal(t, 200, call(t, p, "UpdateConnection", `{"Name":"valid","Description":"updated"}`).StatusCode)
	updated, err := p.store.GetConnection("valid", defaultAccountID)
	require.NoError(t, err)
	require.Equal(t, before.AuthParameters, updated.AuthParameters)
	require.Equal(t, "AUTHORIZED", updated.State)
	require.Equal(t, 400, call(t, p, "UpdateConnection", `{"Name":"valid","Description":"must-not-stick","AuthParameters":{}}`).StatusCode)
	loaded, err := p.store.GetConnection("valid", defaultAccountID)
	require.NoError(t, err)
	require.Equal(t, updated, loaded)
	require.Equal(t, 400, call(t, p, "UpdateConnection", `{"Name":"valid","AuthorizationType":"BASIC"}`).StatusCode)
	listed := parseJSON(t, call(t, p, "ListConnections", `{"NamePrefix":"val","ConnectionState":"AUTHORIZED","Limit":1}`))
	require.Len(t, listed["Connections"], 1)
	require.Equal(t, 400, call(t, p, "ListConnections", `{"Limit":0}`).StatusCode)
	described := parseJSON(t, call(t, p, "DescribeConnection", `{"Name":"valid"}`))
	require.Equal(t, "local-metadata", described["KmsKeyIdentifier"])
	require.IsType(t, float64(0), described["CreationTime"])
	require.Equal(t, before.CreationTime.Unix(), int64(described["CreationTime"].(float64)))
}

func TestConnectionOperationOAuthRemainsDeauthorized(t *testing.T) {
	p := newTestProvider(t)
	response := call(t, p, "CreateConnection", `{"Name":"oauth","AuthorizationType":"OAUTH_CLIENT_CREDENTIALS","AuthParameters":{"OAuthParameters":{"AuthorizationEndpoint":"https://example.invalid/token","HttpMethod":"POST","ClientParameters":{"ClientID":"id","ClientSecret":"secret"}}}}`)
	require.Equal(t, 200, response.StatusCode)
	require.Equal(t, "DEAUTHORIZED", parseJSON(t, response)["ConnectionState"])
	described := call(t, p, "DescribeConnection", `{"Name":"oauth"}`)
	require.Equal(t, 200, described.StatusCode)
	require.NotContains(t, string(described.Body), "ClientSecret")
	require.NotContains(t, string(described.Body), "secret")
	loaded, err := p.store.GetConnection("oauth", defaultAccountID)
	require.NoError(t, err)
	require.Nil(t, loaded.LastAuthorizedTime)
	require.WithinDuration(t, time.Now(), loaded.CreationTime, time.Second)
}

func TestConnectionOperationStorageFailure(t *testing.T) {
	p := newTestProvider(t)
	require.NoError(t, p.store.Close())
	for _, op := range []string{"DescribeConnection", "ListConnections", "DeauthorizeConnection", "DeleteConnection"} {
		response := call(t, p, op, `{"Name":"valid"}`)
		require.Equal(t, 500, response.StatusCode, op)
	}
}

func TestConnectionOperationPartialAuthUpdates(t *testing.T) {
	p := newTestProvider(t)
	require.Equal(t, 200, call(t, p, "CreateConnection", `{"Name":"partial","AuthorizationType":"API_KEY","AuthParameters":{"ApiKeyAuthParameters":{"ApiKeyName":"keep-name","ApiKeyValue":"old-secret"}}}`).StatusCode)
	require.Equal(t, 200, call(t, p, "UpdateConnection", `{"Name":"partial","AuthParameters":{"ApiKeyAuthParameters":{"ApiKeyValue":"rotated-secret"}}}`).StatusCode)
	loaded, err := p.store.GetConnection("partial", defaultAccountID)
	require.NoError(t, err)
	require.Equal(t, map[string]any{"ApiKeyName": "keep-name", "ApiKeyValue": "rotated-secret"}, loaded.AuthParameters["ApiKeyAuthParameters"])
	authorized := loaded.LastAuthorizedTime
	require.Equal(t, 200, call(t, p, "UpdateConnection", `{"Name":"partial","AuthParameters":{"InvocationHttpParameters":{"HeaderParameters":[{"Key":"visible","Value":"public","IsValueSecret":false},{"Key":"private","Value":"header-secret","IsValueSecret":true}]}}}`).StatusCode)
	loaded, err = p.store.GetConnection("partial", defaultAccountID)
	require.NoError(t, err)
	require.Equal(t, authorized, loaded.LastAuthorizedTime)
	require.Equal(t, "AUTHORIZED", loaded.State)
	describe := call(t, p, "DescribeConnection", `{"Name":"partial"}`)
	require.NotContains(t, string(describe.Body), "rotated-secret")
	require.NotContains(t, string(describe.Body), "header-secret")
	require.Contains(t, string(describe.Body), "public")
	before := loaded
	require.Equal(t, 400, call(t, p, "UpdateConnection", `{"Name":"partial","AuthorizationType":"BASIC","AuthParameters":{"BasicAuthParameters":{"Username":"user"}}}`).StatusCode)
	loaded, err = p.store.GetConnection("partial", defaultAccountID)
	require.NoError(t, err)
	require.Equal(t, before, loaded)
	require.Equal(t, 200, call(t, p, "DeauthorizeConnection", `{"Name":"partial"}`).StatusCode)
	require.Equal(t, 400, call(t, p, "UpdateConnection", `{"Name":"partial","AuthParameters":{"ApiKeyAuthParameters":{"ApiKeyValue":"rotated-secret"}}}`).StatusCode)
	loaded, err = p.store.GetConnection("partial", defaultAccountID)
	require.NoError(t, err)
	require.Empty(t, loaded.AuthParameters)
	require.Equal(t, "DEAUTHORIZED", loaded.State)
}
