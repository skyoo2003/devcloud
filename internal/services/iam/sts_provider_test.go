// SPDX-License-Identifier: Apache-2.0

package iam

import (
	"context"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/skyoo2003/devcloud/internal/plugin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestSTSProvider(t *testing.T) *STSProvider {
	t.Helper()
	dir := t.TempDir()
	p := &STSProvider{}
	err := p.Init(plugin.PluginConfig{
		DataDir: dir,
		Options: map[string]any{
			"db_path": filepath.Join(dir, "iam.db"),
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { p.Shutdown(context.Background()) })
	return p
}

func TestSTSProvider_GetCallerIdentity(t *testing.T) {
	p := newTestSTSProvider(t)

	req := httptest.NewRequest("POST", "/", strings.NewReader("Action=GetCallerIdentity"))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := p.HandleRequest(context.Background(), "", req)
	require.NoError(t, err)
	assert.Equal(t, 200, resp.StatusCode)
	body := string(resp.Body)
	assert.Contains(t, body, "GetCallerIdentityResponse")
	assert.Contains(t, body, "000000000000")
	assert.Contains(t, body, "arn:aws:iam::000000000000:root")
}

func TestSTSProvider_AssumeRole(t *testing.T) {
	p := newTestSTSProvider(t)

	form := "Action=AssumeRole&RoleArn=arn%3Aaws%3Aiam%3A%3A000000000000%3Arole%2Fmyrole&RoleSessionName=mysession"
	req := httptest.NewRequest("POST", "/", strings.NewReader(form))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := p.HandleRequest(context.Background(), "", req)
	require.NoError(t, err)
	assert.Equal(t, 200, resp.StatusCode)
	body := string(resp.Body)
	assert.Contains(t, body, "AssumeRoleResponse")
	assert.Contains(t, body, "Credentials")
	assert.Contains(t, body, "ASIA")
	assert.Contains(t, body, "SecretAccessKey")
	assert.Contains(t, body, "SessionToken")
	assert.Contains(t, body, "Expiration")
	assert.Contains(t, body, "assumed-role/myrole/mysession")
}

func TestSTSProvider_GetSessionToken(t *testing.T) {
	p := newTestSTSProvider(t)

	req := httptest.NewRequest("POST", "/", strings.NewReader("Action=GetSessionToken"))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := p.HandleRequest(context.Background(), "", req)
	require.NoError(t, err)
	assert.Equal(t, 200, resp.StatusCode)
	body := string(resp.Body)
	assert.Contains(t, body, "GetSessionTokenResponse")
	assert.Contains(t, body, "ASIA")
	assert.Contains(t, body, "SessionToken")
	assert.Contains(t, body, "Expiration")
}

func TestSTSProvider_GetAccessKeyInfo(t *testing.T) {
	p := newTestSTSProvider(t)

	form := "Action=GetAccessKeyInfo&AccessKeyId=AKIAIOSFODNN7EXAMPLE"
	req := httptest.NewRequest("POST", "/", strings.NewReader(form))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := p.HandleRequest(context.Background(), "", req)
	require.NoError(t, err)
	assert.Equal(t, 200, resp.StatusCode)
	body := string(resp.Body)
	assert.Contains(t, body, "GetAccessKeyInfoResponse")
	assert.Contains(t, body, "000000000000")
}

func TestSTSProvider_UnknownAction(t *testing.T) {
	p := newTestSTSProvider(t)

	req := httptest.NewRequest("POST", "/", strings.NewReader("Action=DeleteEverything"))
	resp, err := p.HandleRequest(context.Background(), "", req)
	require.NoError(t, err)
	assert.Equal(t, 400, resp.StatusCode)
}
