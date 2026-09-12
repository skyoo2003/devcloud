// SPDX-License-Identifier: Apache-2.0

// internal/services/paymentcryptographydata/provider_test.go
package paymentcryptographydata

import (
	"context"
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

func callREST(t *testing.T, p *Provider, method, path, op, body string) *plugin.Response {
	t.Helper()
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err := p.HandleRequest(context.Background(), op, req)
	require.NoError(t, err)
	return resp
}

func parseBody(t *testing.T, resp *plugin.Response) map[string]any {
	t.Helper()
	var m map[string]any
	require.NoError(t, json.Unmarshal(resp.Body, &m))
	return m
}

// TestEveryOperation calls all 15 by path, so the generated route table is
// exercised alongside the dispatch. Every wire member is PascalCase.
func TestEveryOperation(t *testing.T) {
	p := newTestProvider(t)
	const body = `{"KeyIdentifier":"key-1","PlainText":"4111111111111111",` +
		`"CipherText":"3431","MessageData":"abcd","PrimaryAccountNumber":"4111111111111111",` +
		`"EncryptedPinBlock":"AAAA1111","GenerationKeyIdentifier":"gen-1",` +
		`"EncryptionKeyIdentifier":"enc-1","VerificationKeyIdentifier":"ver-1",` +
		`"NewPinPekIdentifier":"pek-1",` +
		`"SecureMessagingConfidentialityKeyIdentifier":"conf-1",` +
		`"SecureMessagingIntegrityKeyIdentifier":"int-1"}`

	cases := []struct {
		op      string
		path    string
		members []string
	}{
		{"EncryptData", "/keys/key-1/encrypt", []string{"KeyArn", "KeyCheckValue", "CipherText"}},
		{"DecryptData", "/keys/key-1/decrypt", []string{"KeyArn", "KeyCheckValue", "PlainText"}},
		{"ReEncryptData", "/keys/key-1/reencrypt", []string{"KeyArn", "KeyCheckValue", "CipherText"}},
		{"GenerateMac", "/mac/generate", []string{"KeyArn", "KeyCheckValue", "Mac"}},
		{"VerifyMac", "/mac/verify", []string{"KeyArn", "KeyCheckValue"}},
		{"GenerateCardValidationData", "/cardvalidationdata/generate",
			[]string{"KeyArn", "KeyCheckValue", "ValidationData"}},
		{"VerifyCardValidationData", "/cardvalidationdata/verify", []string{"KeyArn", "KeyCheckValue"}},
		{"GenerateAuthRequestCryptogram", "/cryptogram/generate",
			[]string{"KeyArn", "KeyCheckValue", "AuthRequestCryptogram"}},
		{"VerifyAuthRequestCryptogram", "/cryptogram/verify",
			[]string{"KeyArn", "KeyCheckValue", "AuthResponseValue"}},
		{"GenerateAs2805KekValidation", "/as2805kekvalidation/generate",
			[]string{"KeyArn", "KeyCheckValue", "RandomKeyReceive", "RandomKeySend"}},
		{"GeneratePinData", "/pindata/generate",
			[]string{"EncryptionKeyArn", "EncryptionKeyCheckValue", "GenerationKeyArn",
				"GenerationKeyCheckValue", "EncryptedPinBlock", "PinData"}},
		{"TranslatePinData", "/pindata/translate", []string{"KeyArn", "KeyCheckValue", "PinBlock"}},
		{"VerifyPinData", "/pindata/verify",
			[]string{"EncryptionKeyArn", "EncryptionKeyCheckValue", "VerificationKeyArn",
				"VerificationKeyCheckValue"}},
		{"TranslateKeyMaterial", "/keymaterial/translate", []string{"WrappedKey"}},
		{"GenerateMacEmvPinChange", "/macemvpinchange/generate",
			[]string{"EncryptedPinBlock", "Mac", "NewPinPekArn", "NewPinPekKeyCheckValue",
				"SecureMessagingConfidentialityKeyArn", "SecureMessagingConfidentialityKeyCheckValue",
				"SecureMessagingIntegrityKeyArn", "SecureMessagingIntegrityKeyCheckValue",
				"VisaAmexDerivationOutputs"}},
	}
	require.Len(t, cases, 15, "every operation the model declares must be called")

	for _, c := range cases {
		t.Run(c.op, func(t *testing.T) {
			resp := callREST(t, p, "POST", c.path, "", body)
			assert.Equal(t, 200, resp.StatusCode)

			rb := parseBody(t, resp)
			for _, m := range c.members {
				assert.Contains(t, rb, m)
			}
		})
	}
}

// TestEncryptDecryptRoundTrip is the one property a caller can assert locally.
func TestEncryptDecryptRoundTrip(t *testing.T) {
	p := newTestProvider(t)
	const plainText = "4111111111111111"

	enc := callREST(t, p, "POST", "/keys/key-1/encrypt", "", `{"PlainText":"`+plainText+`"}`)
	require.Equal(t, 200, enc.StatusCode)
	cipherText, ok := parseBody(t, enc)["CipherText"].(string)
	require.True(t, ok)
	assert.NotEqual(t, plainText, cipherText)

	dec := callREST(t, p, "POST", "/keys/key-1/decrypt", "", `{"CipherText":"`+cipherText+`"}`)
	require.Equal(t, 200, dec.StatusCode)
	assert.Equal(t, plainText, parseBody(t, dec)["PlainText"])

	// Re-encryption preserves the content, so the round-trip survives it.
	re := callREST(t, p, "POST", "/keys/key-2/reencrypt", "", `{"CipherText":"`+cipherText+`"}`)
	require.Equal(t, 200, re.StatusCode)
	reCipher, ok := parseBody(t, re)["CipherText"].(string)
	require.True(t, ok)
	dec2 := callREST(t, p, "POST", "/keys/key-2/decrypt", "", `{"CipherText":"`+reCipher+`"}`)
	assert.Equal(t, plainText, parseBody(t, dec2)["PlainText"])
}

func TestDecryptRejectsNonHexCipherText(t *testing.T) {
	p := newTestProvider(t)

	resp := callREST(t, p, "POST", "/keys/key-1/decrypt", "", `{"CipherText":"zz"}`)
	assert.Equal(t, 400, resp.StatusCode)
	assert.Equal(t, "ValidationException", parseBody(t, resp)["__type"])
}

// The key check value must be a function of the key id alone, or a caller has
// nothing stable to compare across calls.
func TestKeyCheckValueIsStable(t *testing.T) {
	p := newTestProvider(t)

	first := parseBody(t, callREST(t, p, "POST", "/mac/verify", "", `{"KeyIdentifier":"key-1"}`))
	second := parseBody(t, callREST(t, p, "POST", "/mac/verify", "", `{"KeyIdentifier":"key-1"}`))
	other := parseBody(t, callREST(t, p, "POST", "/mac/verify", "", `{"KeyIdentifier":"key-2"}`))

	assert.Equal(t, first["KeyCheckValue"], second["KeyCheckValue"])
	assert.NotEqual(t, first["KeyCheckValue"], other["KeyCheckValue"])
}

// The key identifier comes from the path for the three operations that bind it
// there, and from the body for every other one.
func TestKeyIdentifierSource(t *testing.T) {
	p := newTestProvider(t)

	fromPath := parseBody(t, callREST(t, p, "POST", "/keys/path-key/encrypt", "",
		`{"KeyIdentifier":"body-key","PlainText":"x"}`))
	assert.Contains(t, fromPath["KeyArn"], "path-key")

	fromBody := parseBody(t, callREST(t, p, "POST", "/mac/verify", "", `{"KeyIdentifier":"body-key"}`))
	assert.Contains(t, fromBody["KeyArn"], "body-key")
}

// TestGeneratePinDataIsATaggedUnion guards the one union-typed response member
// in this service. botocore refuses a union with no member set — "PinData must
// have one and only one member set" — so an empty object is a 200 the SDK
// cannot parse, which is worse than an error.
func TestGeneratePinDataIsATaggedUnion(t *testing.T) {
	p := newTestProvider(t)

	cases := []struct {
		name       string
		attributes string
		wantMember string
	}{
		{"visa", `{"VisaPin":{"PinVerificationKeyIndex":1}}`, "VerificationValue"},
		{"ibm3624", `{"Ibm3624PinOffset":{"EncryptedPinBlock":"AABB"}}`, "PinOffset"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			resp := callREST(t, p, "POST", "/pindata/generate", "",
				`{"GenerationKeyIdentifier":"genkey-01","EncryptionKeyIdentifier":"enckey-01",`+
					`"GenerationAttributes":`+c.attributes+`}`)
			require.Equal(t, 200, resp.StatusCode)

			pinData, ok := parseBody(t, resp)["PinData"].(map[string]any)
			require.True(t, ok)
			require.Len(t, pinData, 1, "a tagged union carries exactly one member")
			assert.Contains(t, pinData, c.wantMember)
		})
	}
}

func TestInvalidJSONBody(t *testing.T) {
	p := newTestProvider(t)

	resp := callREST(t, p, "POST", "/mac/verify", "", "not json")
	assert.Equal(t, 400, resp.StatusCode)
	assert.Equal(t, "SerializationException", parseBody(t, resp)["__type"])
}

// TestProviderIdentity pins what the registry and the fidelity manifest read.
// A typo in ServiceName is invisible everywhere else.
func TestProviderIdentity(t *testing.T) {
	p := newTestProvider(t)
	assert.Equal(t, "paymentcryptographydata", p.ServiceID())
	assert.Equal(t, "PaymentCryptographyDataPlane", p.ServiceName())
	assert.Equal(t, plugin.ProtocolRESTJSON, p.Protocol())

	resources, err := p.ListResources(context.Background())
	require.NoError(t, err)
	assert.Empty(t, resources)
}

func TestUnknownOperationIsUnhandled(t *testing.T) {
	p := newTestProvider(t)
	req := httptest.NewRequest("POST", "/nope", strings.NewReader("{}"))
	_, err := p.HandleRequest(context.Background(), "NoSuchThing", req)
	assert.ErrorIs(t, err, plugin.ErrUnhandledOp)
}
