// SPDX-License-Identifier: Apache-2.0

package paymentcryptographydata

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"strings"

	generated "github.com/skyoo2003/devcloud/internal/generated/paymentcryptographydata"
	"github.com/skyoo2003/devcloud/internal/plugin"
	"github.com/skyoo2003/devcloud/internal/shared"
	"github.com/skyoo2003/devcloud/internal/shared/crud"
)

// Provider implements the PaymentCryptographyDataPlane service.
type Provider struct {
	generated.BaseProvider
}

func (p *Provider) ServiceID() string             { return "paymentcryptographydata" }
func (p *Provider) ServiceName() string           { return "PaymentCryptographyDataPlane" }
func (p *Provider) Protocol() plugin.ProtocolType { return plugin.ProtocolRESTJSON }

func (p *Provider) Init(_ plugin.PluginConfig) error { return nil }

func (p *Provider) Shutdown(_ context.Context) error { return nil }

// HandleRequest dispatches all 15 operations. Every wire member is PascalCase.
//
// The switch is kept flat and literal on purpose: codegen's hand-verified scan
// reads the `case` clauses out of this function, so collapsing it into a map
// lookup would delete all 15 operations from the fidelity manifest.
func (p *Provider) HandleRequest(_ context.Context, op string, req *http.Request) (*plugin.Response, error) {
	matched, labels := generated.MatchOperation(req.Method, req.URL.RequestURI())
	if op == "" {
		op = matched
	}

	raw, err := io.ReadAll(req.Body)
	if err != nil {
		return shared.JSONError("SerializationException", "failed to read body", http.StatusBadRequest), nil
	}
	in := map[string]any{}
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &in); err != nil {
			return shared.JSONError("SerializationException", "invalid JSON", http.StatusBadRequest), nil
		}
	}

	// Three operations address the key in the path; every other one carries it
	// in the body as KeyIdentifier.
	keyID := labels["KeyIdentifier"]
	if keyID == "" {
		keyID = labels["IncomingKeyIdentifier"]
	}
	if keyID == "" {
		keyID, _ = in["KeyIdentifier"].(string)
	}

	switch op {
	case "EncryptData":
		plainText, _ := in["PlainText"].(string)
		return shared.JSONResponse(http.StatusOK, map[string]any{
			"KeyArn":        keyARN(keyID),
			"KeyCheckValue": keyCheckValue(keyID),
			"CipherText":    cipher(plainText),
		})

	case "DecryptData":
		cipherText, _ := in["CipherText"].(string)
		plainText, ok := plain(cipherText)
		if !ok {
			return shared.JSONError("ValidationException", "cipherText is not valid", http.StatusBadRequest), nil
		}
		return shared.JSONResponse(http.StatusOK, map[string]any{
			"KeyArn":        keyARN(keyID),
			"KeyCheckValue": keyCheckValue(keyID),
			"PlainText":     plainText,
		})

	case "ReEncryptData":
		// Re-encryption is keyed on the incoming key but the content survives,
		// so decode and re-encode rather than echoing the input: a caller that
		// then decrypts must get its plaintext back.
		cipherText, _ := in["CipherText"].(string)
		plainText, ok := plain(cipherText)
		if !ok {
			return shared.JSONError("ValidationException", "cipherText is not valid", http.StatusBadRequest), nil
		}
		return shared.JSONResponse(http.StatusOK, map[string]any{
			"KeyArn":        keyARN(keyID),
			"KeyCheckValue": keyCheckValue(keyID),
			"CipherText":    cipher(plainText),
		})

	case "GenerateMac":
		messageData, _ := in["MessageData"].(string)
		return shared.JSONResponse(http.StatusOK, map[string]any{
			"KeyArn":        keyARN(keyID),
			"KeyCheckValue": keyCheckValue(keyID),
			"Mac":           derive(keyID+messageData, 16),
		})

	case "VerifyMac":
		return shared.JSONResponse(http.StatusOK, map[string]any{
			"KeyArn":        keyARN(keyID),
			"KeyCheckValue": keyCheckValue(keyID),
		})

	case "GenerateCardValidationData":
		pan, _ := in["PrimaryAccountNumber"].(string)
		return shared.JSONResponse(http.StatusOK, map[string]any{
			"KeyArn":         keyARN(keyID),
			"KeyCheckValue":  keyCheckValue(keyID),
			"ValidationData": digits(pan, 3),
		})

	case "VerifyCardValidationData":
		return shared.JSONResponse(http.StatusOK, map[string]any{
			"KeyArn":        keyARN(keyID),
			"KeyCheckValue": keyCheckValue(keyID),
		})

	case "GenerateAuthRequestCryptogram":
		return shared.JSONResponse(http.StatusOK, map[string]any{
			"KeyArn":                keyARN(keyID),
			"KeyCheckValue":         keyCheckValue(keyID),
			"AuthRequestCryptogram": derive(keyID+"arqc", 16),
		})

	case "VerifyAuthRequestCryptogram":
		return shared.JSONResponse(http.StatusOK, map[string]any{
			"KeyArn":            keyARN(keyID),
			"KeyCheckValue":     keyCheckValue(keyID),
			"AuthResponseValue": derive(keyID+"arpc", 8),
		})

	case "GenerateAs2805KekValidation":
		return shared.JSONResponse(http.StatusOK, map[string]any{
			"KeyArn":           keyARN(keyID),
			"KeyCheckValue":    keyCheckValue(keyID),
			"RandomKeyReceive": derive(keyID+"receive", 16),
			"RandomKeySend":    derive(keyID+"send", 16),
		})

	case "GeneratePinData":
		generationKeyID, _ := in["GenerationKeyIdentifier"].(string)
		encryptionKeyID, _ := in["EncryptionKeyIdentifier"].(string)
		generationAttributes, _ := in["GenerationAttributes"].(map[string]any)
		return shared.JSONResponse(http.StatusOK, map[string]any{
			"EncryptionKeyArn":        keyARN(encryptionKeyID),
			"EncryptionKeyCheckValue": keyCheckValue(encryptionKeyID),
			"GenerationKeyArn":        keyARN(generationKeyID),
			"GenerationKeyCheckValue": keyCheckValue(generationKeyID),
			"EncryptedPinBlock":       derive(generationKeyID+"pin", 16),
			"PinData":                 pinData(generationKeyID, generationAttributes),
		})

	case "TranslatePinData":
		// The PIN block comes back unchanged: a translation re-wraps under a new
		// key, and with no key material the block itself is the only stable
		// thing to hand back.
		pinBlock, _ := in["EncryptedPinBlock"].(string)
		return shared.JSONResponse(http.StatusOK, map[string]any{
			"KeyArn":        keyARN(keyID),
			"KeyCheckValue": keyCheckValue(keyID),
			"PinBlock":      pinBlock,
		})

	case "VerifyPinData":
		encryptionKeyID, _ := in["EncryptionKeyIdentifier"].(string)
		verificationKeyID, _ := in["VerificationKeyIdentifier"].(string)
		return shared.JSONResponse(http.StatusOK, map[string]any{
			"EncryptionKeyArn":          keyARN(encryptionKeyID),
			"EncryptionKeyCheckValue":   keyCheckValue(encryptionKeyID),
			"VerificationKeyArn":        keyARN(verificationKeyID),
			"VerificationKeyCheckValue": keyCheckValue(verificationKeyID),
		})

	case "TranslateKeyMaterial":
		return shared.JSONResponse(http.StatusOK, map[string]any{
			"WrappedKey": map[string]any{},
		})

	case "GenerateMacEmvPinChange":
		newPinPekID, _ := in["NewPinPekIdentifier"].(string)
		confidentialityKeyID, _ := in["SecureMessagingConfidentialityKeyIdentifier"].(string)
		integrityKeyID, _ := in["SecureMessagingIntegrityKeyIdentifier"].(string)
		return shared.JSONResponse(http.StatusOK, map[string]any{
			"EncryptedPinBlock":                    derive(newPinPekID+"pin", 16),
			"Mac":                                  derive(integrityKeyID+"mac", 16),
			"NewPinPekArn":                         keyARN(newPinPekID),
			"NewPinPekKeyCheckValue":               keyCheckValue(newPinPekID),
			"SecureMessagingConfidentialityKeyArn": keyARN(confidentialityKeyID),
			"SecureMessagingConfidentialityKeyCheckValue": keyCheckValue(confidentialityKeyID),
			"SecureMessagingIntegrityKeyArn":              keyARN(integrityKeyID),
			"SecureMessagingIntegrityKeyCheckValue":       keyCheckValue(integrityKeyID),
			"VisaAmexDerivationOutputs":                   map[string]any{},
		})

	default:
		return nil, plugin.ErrUnhandledOp
	}
}

func (p *Provider) ListResources(_ context.Context) ([]plugin.Resource, error) {
	return []plugin.Resource{}, nil
}

// keyARN and keyCheckValue are what every response in this service carries. The
// KCV is derived from the key identifier rather than random so repeated calls
// against the same key agree, which is the only property a caller can
// reasonably assert locally.
func keyARN(keyID string) string {
	return shared.BuildARN("payment-cryptography", "key", keyID)
}

func keyCheckValue(keyID string) string {
	sum := sha256.Sum256([]byte(keyID))
	return strings.ToUpper(hex.EncodeToString(sum[:3]))
}

// pinData builds the GeneratePinData response's PinData member.
//
// It is a tagged union, so it must carry exactly one member: botocore refuses a
// union that arrives empty ("PinData must have one and only one member set") and
// raises instead of returning, which turns a 200 into an error the caller cannot
// read. Which member is right depends on the scheme asked for — an IBM 3624
// generation yields an offset, every other scheme a verification value.
func pinData(keyID string, generationAttributes map[string]any) map[string]any {
	for name := range generationAttributes {
		if strings.HasPrefix(name, "Ibm3624") {
			return map[string]any{"PinOffset": digits(keyID+"offset", 4)}
		}
	}
	return map[string]any{"VerificationValue": digits(keyID+"verification", 4)}
}

// derive produces a stable upper-hex string of n characters from a seed, so two
// identical requests agree and two different ones do not.
func derive(seed string, n int) string {
	sum := sha256.Sum256([]byte(seed))
	return strings.ToUpper(hex.EncodeToString(sum[:]))[:n]
}

// digits produces n decimal digits from a seed, for the card validation values a
// caller may feed straight back into a numeric field.
func digits(seed string, n int) string {
	sum := sha256.Sum256([]byte(seed))
	out := make([]byte, 0, n)
	for i := 0; len(out) < n; i++ {
		out = append(out, '0'+sum[i%len(sum)]%10)
	}
	return string(out)
}

// ponytail: hex, not cryptography. The property that matters locally is that
// DecryptData returns what EncryptData was given; a real cipher would need key
// material this service does not hold.
func cipher(plainText string) string { return hex.EncodeToString([]byte(plainText)) }

func plain(cipherText string) (string, bool) {
	b, err := hex.DecodeString(cipherText)
	return string(b), err == nil
}

func init() {
	plugin.DefaultRegistry.Register("paymentcryptographydata", func() plugin.ServicePlugin {
		return &Provider{}
	})
	// This service signs as "payment-cryptography", which the control plane
	// claims. Neither side's operations are CRUD-classifiable at these paths, so
	// without this the gateway's shared-signing-name split has no candidate and
	// the control plane answers with InvalidAction. See crud.RegisterRoutes.
	crud.RegisterRoutes("paymentcryptographydata", generated.OperationRoutes)
}
