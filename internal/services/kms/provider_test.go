// SPDX-License-Identifier: Apache-2.0

package kms

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/skyoo2003/devcloud/internal/plugin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestProvider(t *testing.T) *Provider {
	t.Helper()
	p := &Provider{}
	require.NoError(t, p.Init(plugin.PluginConfig{DataDir: t.TempDir()}))
	t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
	return p
}

func callJSON(t *testing.T, p *Provider, target, body string) *plugin.Response {
	t.Helper()
	req := httptest.NewRequest("POST", "/", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req.Header.Set("X-Amz-Target", target)
	resp, err := p.HandleRequest(context.Background(), "", req)
	require.NoError(t, err)
	return resp
}

func parseJSON(t *testing.T, resp *plugin.Response) map[string]any {
	t.Helper()
	var m map[string]any
	require.NoError(t, json.Unmarshal(resp.Body, &m))
	return m
}

func TestKMS_CreateKey(t *testing.T) {
	p := newTestProvider(t)
	resp := callJSON(t, p, "TrentService.CreateKey", `{"Description":"test key"}`)
	assert.Equal(t, 200, resp.StatusCode)
	m := parseJSON(t, resp)
	meta := m["KeyMetadata"].(map[string]any)
	assert.NotEmpty(t, meta["KeyId"])
	assert.Equal(t, "Enabled", meta["KeyState"])
}

func TestKMS_DescribeKey(t *testing.T) {
	p := newTestProvider(t)
	cr := parseJSON(t, callJSON(t, p, "TrentService.CreateKey", `{}`))
	keyID := cr["KeyMetadata"].(map[string]any)["KeyId"].(string)

	resp := callJSON(t, p, "TrentService.DescribeKey", `{"KeyId":"`+keyID+`"}`)
	assert.Equal(t, 200, resp.StatusCode)
	m := parseJSON(t, resp)
	assert.Equal(t, keyID, m["KeyMetadata"].(map[string]any)["KeyId"])
}

func TestKMS_EncryptDecrypt(t *testing.T) {
	p := newTestProvider(t)
	cr := parseJSON(t, callJSON(t, p, "TrentService.CreateKey", `{}`))
	keyID := cr["KeyMetadata"].(map[string]any)["KeyId"].(string)

	plaintext := base64.StdEncoding.EncodeToString([]byte("secret-data"))
	encResp := callJSON(t, p, "TrentService.Encrypt",
		`{"KeyId":"`+keyID+`","Plaintext":"`+plaintext+`"}`)
	assert.Equal(t, 200, encResp.StatusCode)
	encM := parseJSON(t, encResp)
	ciphertext := encM["CiphertextBlob"].(string)
	assert.NotEmpty(t, ciphertext)

	decResp := callJSON(t, p, "TrentService.Decrypt",
		`{"KeyId":"`+keyID+`","CiphertextBlob":"`+ciphertext+`"}`)
	assert.Equal(t, 200, decResp.StatusCode)
	decM := parseJSON(t, decResp)
	recovered, _ := base64.StdEncoding.DecodeString(decM["Plaintext"].(string))
	assert.Equal(t, "secret-data", string(recovered))
}

func TestKMS_GenerateDataKey(t *testing.T) {
	p := newTestProvider(t)
	cr := parseJSON(t, callJSON(t, p, "TrentService.CreateKey", `{}`))
	keyID := cr["KeyMetadata"].(map[string]any)["KeyId"].(string)

	resp := callJSON(t, p, "TrentService.GenerateDataKey", `{"KeyId":"`+keyID+`"}`)
	assert.Equal(t, 200, resp.StatusCode)
	m := parseJSON(t, resp)
	assert.NotEmpty(t, m["Plaintext"])
	assert.NotEmpty(t, m["CiphertextBlob"])

	// Encrypted data key should decrypt back to the same plaintext.
	keyPlainB64 := m["Plaintext"].(string)
	cipherB64 := m["CiphertextBlob"].(string)
	decResp := callJSON(t, p, "TrentService.Decrypt",
		`{"KeyId":"`+keyID+`","CiphertextBlob":"`+cipherB64+`"}`)
	decM := parseJSON(t, decResp)
	assert.Equal(t, keyPlainB64, decM["Plaintext"])
}

func TestKMS_CreateAlias_And_ListAliases(t *testing.T) {
	p := newTestProvider(t)
	cr := parseJSON(t, callJSON(t, p, "TrentService.CreateKey", `{}`))
	keyID := cr["KeyMetadata"].(map[string]any)["KeyId"].(string)

	resp := callJSON(t, p, "TrentService.CreateAlias",
		`{"AliasName":"alias/my-key","TargetKeyId":"`+keyID+`"}`)
	assert.Equal(t, 200, resp.StatusCode)

	listResp := callJSON(t, p, "TrentService.ListAliases", `{}`)
	assert.Equal(t, 200, listResp.StatusCode)
	lm := parseJSON(t, listResp)
	aliases := lm["Aliases"].([]any)
	require.Len(t, aliases, 1)
	assert.Equal(t, "alias/my-key", aliases[0].(map[string]any)["AliasName"])
}

func TestKMS_ListKeys(t *testing.T) {
	p := newTestProvider(t)
	callJSON(t, p, "TrentService.CreateKey", `{"Description":"k1"}`)
	callJSON(t, p, "TrentService.CreateKey", `{"Description":"k2"}`)

	resp := callJSON(t, p, "TrentService.ListKeys", `{}`)
	assert.Equal(t, 200, resp.StatusCode)
	m := parseJSON(t, resp)
	assert.Len(t, m["Keys"], 2)
}

func TestKMS_GenerateDataKeyWithoutPlaintext(t *testing.T) {
	p := newTestProvider(t)
	cr := parseJSON(t, callJSON(t, p, "TrentService.CreateKey", `{}`))
	keyID := cr["KeyMetadata"].(map[string]any)["KeyId"].(string)

	resp := callJSON(t, p, "TrentService.GenerateDataKeyWithoutPlaintext", `{"KeyId":"`+keyID+`","KeySpec":"AES_256"}`)
	assert.Equal(t, 200, resp.StatusCode)
	m := parseJSON(t, resp)
	assert.NotEmpty(t, m["CiphertextBlob"])
	_, hasPlaintext := m["Plaintext"]
	assert.False(t, hasPlaintext)
}

func TestKMS_GenerateDataKeyPair(t *testing.T) {
	p := newTestProvider(t)
	cr := parseJSON(t, callJSON(t, p, "TrentService.CreateKey", `{}`))
	keyID := cr["KeyMetadata"].(map[string]any)["KeyId"].(string)

	resp := callJSON(t, p, "TrentService.GenerateDataKeyPair", `{"KeyId":"`+keyID+`","KeyPairSpec":"RSA_2048"}`)
	assert.Equal(t, 200, resp.StatusCode)
	m := parseJSON(t, resp)
	assert.NotEmpty(t, m["PublicKey"])
	assert.NotEmpty(t, m["PrivateKeyPlaintext"])
	assert.NotEmpty(t, m["PrivateKeyCiphertext"])
}

func TestKMS_GenerateDataKeyPairWithoutPlaintext(t *testing.T) {
	p := newTestProvider(t)
	cr := parseJSON(t, callJSON(t, p, "TrentService.CreateKey", `{}`))
	keyID := cr["KeyMetadata"].(map[string]any)["KeyId"].(string)

	resp := callJSON(t, p, "TrentService.GenerateDataKeyPairWithoutPlaintext", `{"KeyId":"`+keyID+`","KeyPairSpec":"RSA_2048"}`)
	assert.Equal(t, 200, resp.StatusCode)
	m := parseJSON(t, resp)
	assert.NotEmpty(t, m["PublicKey"])
	assert.NotEmpty(t, m["PrivateKeyCiphertext"])
	_, hasPlaintext := m["PrivateKeyPlaintext"]
	assert.False(t, hasPlaintext)
}

func TestKMS_ReEncrypt(t *testing.T) {
	p := newTestProvider(t)
	k1 := parseJSON(t, callJSON(t, p, "TrentService.CreateKey", `{"Description":"src"}`))
	k2 := parseJSON(t, callJSON(t, p, "TrentService.CreateKey", `{"Description":"dst"}`))
	k1ID := k1["KeyMetadata"].(map[string]any)["KeyId"].(string)
	k2ID := k2["KeyMetadata"].(map[string]any)["KeyId"].(string)

	plaintext := base64.StdEncoding.EncodeToString([]byte("secret"))
	encResp := parseJSON(t, callJSON(t, p, "TrentService.Encrypt", `{"KeyId":"`+k1ID+`","Plaintext":"`+plaintext+`"}`))
	ciphertext := encResp["CiphertextBlob"].(string)

	reResp := callJSON(t, p, "TrentService.ReEncrypt", `{"CiphertextBlob":"`+ciphertext+`","DestinationKeyId":"`+k2ID+`"}`)
	assert.Equal(t, 200, reResp.StatusCode)
	m := parseJSON(t, reResp)
	assert.NotEmpty(t, m["CiphertextBlob"])

	// Verify new ciphertext decrypts with k2.
	newCipher := m["CiphertextBlob"].(string)
	decResp := callJSON(t, p, "TrentService.Decrypt", `{"KeyId":"`+k2ID+`","CiphertextBlob":"`+newCipher+`"}`)
	assert.Equal(t, 200, decResp.StatusCode)
	decM := parseJSON(t, decResp)
	recovered, _ := base64.StdEncoding.DecodeString(decM["Plaintext"].(string))
	assert.Equal(t, "secret", string(recovered))
}

func TestKMS_SignVerify(t *testing.T) {
	p := newTestProvider(t)
	cr := parseJSON(t, callJSON(t, p, "TrentService.CreateKey", `{}`))
	keyID := cr["KeyMetadata"].(map[string]any)["KeyId"].(string)

	message := base64.StdEncoding.EncodeToString([]byte("hello world"))
	sigResp := callJSON(t, p, "TrentService.Sign",
		`{"KeyId":"`+keyID+`","Message":"`+message+`","MessageType":"RAW","SigningAlgorithm":"RSASSA_PKCS1_V1_5_SHA_256"}`)
	assert.Equal(t, 200, sigResp.StatusCode)
	sigM := parseJSON(t, sigResp)
	sig := sigM["Signature"].(string)
	assert.NotEmpty(t, sig)

	verResp := callJSON(t, p, "TrentService.Verify",
		`{"KeyId":"`+keyID+`","Message":"`+message+`","Signature":"`+sig+`","SigningAlgorithm":"RSASSA_PKCS1_V1_5_SHA_256"}`)
	assert.Equal(t, 200, verResp.StatusCode)
	verM := parseJSON(t, verResp)
	assert.True(t, verM["SignatureValid"].(bool))
}

func TestKMS_GetPublicKey(t *testing.T) {
	p := newTestProvider(t)
	cr := parseJSON(t, callJSON(t, p, "TrentService.CreateKey", `{}`))
	keyID := cr["KeyMetadata"].(map[string]any)["KeyId"].(string)

	resp := callJSON(t, p, "TrentService.GetPublicKey", `{"KeyId":"`+keyID+`"}`)
	assert.Equal(t, 200, resp.StatusCode)
	m := parseJSON(t, resp)
	assert.NotEmpty(t, m["PublicKey"])
}

func TestKMS_ScheduleAndCancelKeyDeletion(t *testing.T) {
	p := newTestProvider(t)
	cr := parseJSON(t, callJSON(t, p, "TrentService.CreateKey", `{}`))
	keyID := cr["KeyMetadata"].(map[string]any)["KeyId"].(string)

	schedResp := callJSON(t, p, "TrentService.ScheduleKeyDeletion",
		`{"KeyId":"`+keyID+`","PendingWindowInDays":7}`)
	assert.Equal(t, 200, schedResp.StatusCode)
	m := parseJSON(t, schedResp)
	assert.NotEmpty(t, m["DeletionDate"])

	cancelResp := callJSON(t, p, "TrentService.CancelKeyDeletion", `{"KeyId":"`+keyID+`"}`)
	assert.Equal(t, 200, cancelResp.StatusCode)
	cm := parseJSON(t, cancelResp)
	assert.Equal(t, "Enabled", cm["KeyState"])
}

func TestKMS_UpdateKeyDescription(t *testing.T) {
	p := newTestProvider(t)
	cr := parseJSON(t, callJSON(t, p, "TrentService.CreateKey", `{"Description":"original"}`))
	keyID := cr["KeyMetadata"].(map[string]any)["KeyId"].(string)

	resp := callJSON(t, p, "TrentService.UpdateKeyDescription", `{"KeyId":"`+keyID+`","Description":"updated"}`)
	assert.Equal(t, 200, resp.StatusCode)

	descResp := callJSON(t, p, "TrentService.DescribeKey", `{"KeyId":"`+keyID+`"}`)
	dm := parseJSON(t, descResp)
	assert.Equal(t, "updated", dm["KeyMetadata"].(map[string]any)["Description"])
}

func TestKMS_GrantLifecycle(t *testing.T) {
	p := newTestProvider(t)
	cr := parseJSON(t, callJSON(t, p, "TrentService.CreateKey", `{"Description":"grant test"}`))
	keyID := cr["KeyMetadata"].(map[string]any)["KeyId"].(string)

	grantResp := callJSON(t, p, "TrentService.CreateGrant",
		`{"KeyId":"`+keyID+`","GranteePrincipal":"arn:aws:iam::000000000000:user/alice","Operations":["Encrypt","Decrypt"],"Name":"my-grant"}`)
	assert.Equal(t, 200, grantResp.StatusCode)
	gm := parseJSON(t, grantResp)
	grantID := gm["GrantId"].(string)
	assert.NotEmpty(t, grantID)
	assert.NotEmpty(t, gm["GrantToken"])

	listResp := callJSON(t, p, "TrentService.ListGrants", `{"KeyId":"`+keyID+`"}`)
	assert.Equal(t, 200, listResp.StatusCode)
	lm := parseJSON(t, listResp)
	grants := lm["Grants"].([]any)
	require.Len(t, grants, 1)
	assert.Equal(t, grantID, grants[0].(map[string]any)["GrantId"])

	revokeResp := callJSON(t, p, "TrentService.RevokeGrant", `{"KeyId":"`+keyID+`","GrantId":"`+grantID+`"}`)
	assert.Equal(t, 200, revokeResp.StatusCode)

	listResp2 := callJSON(t, p, "TrentService.ListGrants", `{"KeyId":"`+keyID+`"}`)
	lm2 := parseJSON(t, listResp2)
	assert.Empty(t, lm2["Grants"])
}

func TestKMS_RetireGrant(t *testing.T) {
	p := newTestProvider(t)
	cr := parseJSON(t, callJSON(t, p, "TrentService.CreateKey", `{"Description":"retire test"}`))
	keyID := cr["KeyMetadata"].(map[string]any)["KeyId"].(string)

	grantResp := callJSON(t, p, "TrentService.CreateGrant",
		`{"KeyId":"`+keyID+`","GranteePrincipal":"arn:aws:iam::000000000000:user/bob","RetiringPrincipal":"arn:aws:iam::000000000000:user/admin","Operations":["Encrypt"]}`)
	gm := parseJSON(t, grantResp)
	grantToken := gm["GrantToken"].(string)

	retireResp := callJSON(t, p, "TrentService.RetireGrant", `{"GrantToken":"`+grantToken+`"}`)
	assert.Equal(t, 200, retireResp.StatusCode)
}

func TestKMS_ListRetirableGrants(t *testing.T) {
	p := newTestProvider(t)
	cr := parseJSON(t, callJSON(t, p, "TrentService.CreateKey", `{}`))
	keyID := cr["KeyMetadata"].(map[string]any)["KeyId"].(string)

	callJSON(t, p, "TrentService.CreateGrant",
		`{"KeyId":"`+keyID+`","GranteePrincipal":"arn:aws:iam::000000000000:user/bob","RetiringPrincipal":"arn:aws:iam::000000000000:user/admin","Operations":["Encrypt"]}`)

	resp := callJSON(t, p, "TrentService.ListRetirableGrants",
		`{"RetiringPrincipal":"arn:aws:iam::000000000000:user/admin"}`)
	assert.Equal(t, 200, resp.StatusCode)
	m := parseJSON(t, resp)
	grants := m["Grants"].([]any)
	require.Len(t, grants, 1)
}

func TestKMS_KeyPolicies(t *testing.T) {
	p := newTestProvider(t)
	cr := parseJSON(t, callJSON(t, p, "TrentService.CreateKey", `{"Description":"policy test"}`))
	keyID := cr["KeyMetadata"].(map[string]any)["KeyId"].(string)

	policy := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"*"},"Action":"kms:*","Resource":"*"}]}`
	putResp := callJSON(t, p, "TrentService.PutKeyPolicy",
		`{"KeyId":"`+keyID+`","PolicyName":"default","Policy":`+string(mustMarshal(policy))+`}`)
	assert.Equal(t, 200, putResp.StatusCode)

	getResp := callJSON(t, p, "TrentService.GetKeyPolicy", `{"KeyId":"`+keyID+`","PolicyName":"default"}`)
	assert.Equal(t, 200, getResp.StatusCode)
	gm := parseJSON(t, getResp)
	assert.Contains(t, gm["Policy"].(string), "Statement")

	listResp := callJSON(t, p, "TrentService.ListKeyPolicies", `{"KeyId":"`+keyID+`"}`)
	assert.Equal(t, 200, listResp.StatusCode)
	lm := parseJSON(t, listResp)
	names := lm["PolicyNames"].([]any)
	require.Len(t, names, 1)
	assert.Equal(t, "default", names[0])
}

func mustMarshal(s string) []byte {
	b, _ := json.Marshal(s)
	return b
}
