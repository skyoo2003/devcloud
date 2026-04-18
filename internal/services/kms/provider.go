// SPDX-License-Identifier: Apache-2.0

package kms

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"strings"
	"time"

	"github.com/skyoo2003/devcloud/internal/plugin"
)

const defaultAccountID = plugin.DefaultAccountID
const defaultRegion = "us-east-1"

type Provider struct {
	store *KMSStore
}

func (p *Provider) ServiceID() string             { return "kms" }
func (p *Provider) ServiceName() string           { return "TrentService" }
func (p *Provider) Protocol() plugin.ProtocolType { return plugin.ProtocolJSON11 }

func (p *Provider) Init(cfg plugin.PluginConfig) error {
	dataDir := cfg.DataDir
	if dataDir == "" {
		dataDir = "."
	}
	var err error
	p.store, err = NewKMSStore(filepath.Join(dataDir, "kms"))
	return err
}

func (p *Provider) Shutdown(_ context.Context) error {
	if p.store != nil {
		return p.store.Close()
	}
	return nil
}

func (p *Provider) HandleRequest(_ context.Context, op string, req *http.Request) (*plugin.Response, error) {
	body, err := io.ReadAll(req.Body)
	if err != nil {
		return kmsError("InvalidParameterValue", "failed to read body", http.StatusBadRequest), nil
	}
	var params map[string]any
	if len(body) > 0 {
		if err := json.Unmarshal(body, &params); err != nil {
			return kmsError("InvalidParameterValue", "invalid JSON", http.StatusBadRequest), nil
		}
	} else {
		params = map[string]any{}
	}

	action := op
	if action == "" {
		target := req.Header.Get("X-Amz-Target")
		if idx := strings.LastIndex(target, "."); idx >= 0 {
			action = target[idx+1:]
		} else {
			action = target
		}
	}

	switch action {
	case "CreateKey":
		return p.createKey(params)
	case "DescribeKey":
		return p.describeKey(params)
	case "ListKeys":
		return p.listKeys(params)
	case "Encrypt":
		return p.encrypt(params)
	case "Decrypt":
		return p.decrypt(params)
	case "GenerateDataKey":
		return p.generateDataKey(params)
	case "GenerateDataKeyWithoutPlaintext":
		return p.generateDataKeyWithoutPlaintext(params)
	case "GenerateDataKeyPair":
		return p.generateDataKeyPair(params)
	case "GenerateDataKeyPairWithoutPlaintext":
		return p.generateDataKeyPairWithoutPlaintext(params)
	case "ReEncrypt":
		return p.reEncrypt(params)
	case "Sign":
		return p.sign(params)
	case "Verify":
		return p.verify(params)
	case "GetPublicKey":
		return p.getPublicKey(params)
	case "CreateAlias":
		return p.createAlias(params)
	case "ListAliases":
		return p.listAliases(params)
	case "DisableKey":
		return p.disableKey(params)
	case "EnableKey":
		return p.enableKey(params)
	case "ScheduleKeyDeletion":
		return p.scheduleKeyDeletion(params)
	case "CancelKeyDeletion":
		return p.cancelKeyDeletion(params)
	case "UpdateKeyDescription":
		return p.updateKeyDescription(params)
	case "CreateGrant":
		return p.createGrant(params)
	case "ListGrants":
		return p.listGrants(params)
	case "RetireGrant":
		return p.retireGrant(params)
	case "RevokeGrant":
		return p.revokeGrant(params)
	case "ListRetirableGrants":
		return p.listRetirableGrants(params)
	case "GetKeyPolicy":
		return p.getKeyPolicy(params)
	case "PutKeyPolicy":
		return p.putKeyPolicy(params)
	case "ListKeyPolicies":
		return p.listKeyPolicies(params)
	default:
		return kmsError("InvalidAction", fmt.Sprintf("unknown action: %s", action), http.StatusBadRequest), nil
	}
}

func (p *Provider) ListResources(_ context.Context) ([]plugin.Resource, error) {
	keys, err := p.store.ListKeys(defaultAccountID)
	if err != nil {
		return nil, err
	}
	resources := make([]plugin.Resource, 0, len(keys))
	for _, k := range keys {
		resources = append(resources, plugin.Resource{Type: "key", ID: k.KeyID, Name: k.Description})
	}
	return resources, nil
}

func (p *Provider) GetMetrics(_ context.Context) (*plugin.ServiceMetrics, error) {
	return &plugin.ServiceMetrics{}, nil
}

func (p *Provider) createKey(params map[string]any) (*plugin.Response, error) {
	description, _ := params["Description"].(string)
	keySpec, _ := params["KeySpec"].(string)
	if keySpec == "" {
		keySpec = "SYMMETRIC_DEFAULT"
	}
	keyUsage, _ := params["KeyUsage"].(string)
	if keyUsage == "" {
		keyUsage = "ENCRYPT_DECRYPT"
	}
	keyID := randomHex(16)
	arn := fmt.Sprintf("arn:aws:kms:%s:%s:key/%s", defaultRegion, defaultAccountID, keyID)
	k, err := p.store.CreateKey(keyID, arn, defaultAccountID, description, keySpec, keyUsage)
	if err != nil {
		return nil, err
	}
	return jsonResp(http.StatusOK, map[string]any{
		"KeyMetadata": keyMetadata(k),
	})
}

func (p *Provider) describeKey(params map[string]any) (*plugin.Response, error) {
	keyID, _ := params["KeyId"].(string)
	if keyID == "" {
		return kmsError("ValidationException", "KeyId is required", http.StatusBadRequest), nil
	}
	k, err := p.resolveKey(keyID)
	if err != nil {
		return kmsError("NotFoundException", "key not found", http.StatusBadRequest), nil
	}
	return jsonResp(http.StatusOK, map[string]any{"KeyMetadata": keyMetadata(k)})
}

func (p *Provider) listKeys(_ map[string]any) (*plugin.Response, error) {
	keys, err := p.store.ListKeys(defaultAccountID)
	if err != nil {
		return nil, err
	}
	list := make([]map[string]string, 0, len(keys))
	for _, k := range keys {
		list = append(list, map[string]string{"KeyId": k.KeyID, "KeyArn": k.ARN})
	}
	return jsonResp(http.StatusOK, map[string]any{"Keys": list})
}

func (p *Provider) encrypt(params map[string]any) (*plugin.Response, error) {
	keyID, _ := params["KeyId"].(string)
	plaintextB64, _ := params["Plaintext"].(string)
	if keyID == "" || plaintextB64 == "" {
		return kmsError("ValidationException", "KeyId and Plaintext are required", http.StatusBadRequest), nil
	}
	plaintext, err := base64.StdEncoding.DecodeString(plaintextB64)
	if err != nil {
		return kmsError("ValidationException", "Plaintext must be base64", http.StatusBadRequest), nil
	}
	k, err := p.resolveKey(keyID)
	if err != nil {
		return kmsError("NotFoundException", "key not found", http.StatusBadRequest), nil
	}
	if k.KeyState != "Enabled" {
		return kmsError("DisabledException", "key is disabled", http.StatusBadRequest), nil
	}
	ciphertext, err := Encrypt(k, plaintext)
	if err != nil {
		return nil, err
	}
	return jsonResp(http.StatusOK, map[string]any{
		"CiphertextBlob": base64.StdEncoding.EncodeToString(ciphertext),
		"KeyId":          k.ARN,
	})
}

func (p *Provider) decrypt(params map[string]any) (*plugin.Response, error) {
	ciphertextB64, _ := params["CiphertextBlob"].(string)
	keyID, _ := params["KeyId"].(string)
	if ciphertextB64 == "" {
		return kmsError("ValidationException", "CiphertextBlob is required", http.StatusBadRequest), nil
	}
	ciphertext, err := base64.StdEncoding.DecodeString(ciphertextB64)
	if err != nil {
		return kmsError("ValidationException", "CiphertextBlob must be base64", http.StatusBadRequest), nil
	}
	if keyID != "" {
		k, err := p.resolveKey(keyID)
		if err != nil {
			return kmsError("NotFoundException", "key not found", http.StatusBadRequest), nil
		}
		plaintext, err := Decrypt(k, ciphertext)
		if err != nil {
			return kmsError("InvalidCiphertextException", "decryption failed", http.StatusBadRequest), nil
		}
		return jsonResp(http.StatusOK, map[string]any{
			"Plaintext": base64.StdEncoding.EncodeToString(plaintext),
			"KeyId":     k.ARN,
		})
	}
	// No key ID provided — try all keys.
	keys, _ := p.store.ListKeys(defaultAccountID)
	for _, candidate := range keys {
		c := candidate
		if plaintext, e := Decrypt(&c, ciphertext); e == nil {
			return jsonResp(http.StatusOK, map[string]any{
				"Plaintext": base64.StdEncoding.EncodeToString(plaintext),
				"KeyId":     c.ARN,
			})
		}
	}
	return kmsError("InvalidCiphertextException", "decryption failed", http.StatusBadRequest), nil
}

func (p *Provider) generateDataKey(params map[string]any) (*plugin.Response, error) {
	keyID, _ := params["KeyId"].(string)
	if keyID == "" {
		return kmsError("ValidationException", "KeyId is required", http.StatusBadRequest), nil
	}
	k, err := p.resolveKey(keyID)
	if err != nil {
		return kmsError("NotFoundException", "key not found", http.StatusBadRequest), nil
	}
	// Generate a 32-byte data key.
	dataKey := make([]byte, 32)
	if _, err := rand.Read(dataKey); err != nil {
		return nil, err
	}
	ciphertext, err := Encrypt(k, dataKey)
	if err != nil {
		return nil, err
	}
	return jsonResp(http.StatusOK, map[string]any{
		"Plaintext":      base64.StdEncoding.EncodeToString(dataKey),
		"CiphertextBlob": base64.StdEncoding.EncodeToString(ciphertext),
		"KeyId":          k.ARN,
	})
}

func (p *Provider) createAlias(params map[string]any) (*plugin.Response, error) {
	aliasName, _ := params["AliasName"].(string)
	keyID, _ := params["TargetKeyId"].(string)
	if aliasName == "" || keyID == "" {
		return kmsError("ValidationException", "AliasName and TargetKeyId are required", http.StatusBadRequest), nil
	}
	if err := p.store.CreateAlias(aliasName, keyID, defaultAccountID); err != nil {
		return nil, err
	}
	return jsonResp(http.StatusOK, map[string]any{})
}

func (p *Provider) listAliases(_ map[string]any) (*plugin.Response, error) {
	aliases, err := p.store.ListAliases(defaultAccountID)
	if err != nil {
		return nil, err
	}
	list := make([]map[string]string, 0, len(aliases))
	for _, a := range aliases {
		arn := fmt.Sprintf("arn:aws:kms:%s:%s:%s", defaultRegion, defaultAccountID, a.AliasName)
		list = append(list, map[string]string{
			"AliasName":   a.AliasName,
			"AliasArn":    arn,
			"TargetKeyId": a.KeyID,
		})
	}
	return jsonResp(http.StatusOK, map[string]any{"Aliases": list})
}

func (p *Provider) disableKey(params map[string]any) (*plugin.Response, error) {
	keyID, _ := params["KeyId"].(string)
	if keyID == "" {
		return kmsError("ValidationException", "KeyId is required", http.StatusBadRequest), nil
	}
	k, err := p.resolveKey(keyID)
	if err != nil {
		return kmsError("NotFoundException", "key not found", http.StatusBadRequest), nil
	}
	if err := p.store.UpdateKeyState(k.KeyID, "Disabled"); err != nil {
		return nil, err
	}
	return jsonResp(http.StatusOK, map[string]any{})
}

func (p *Provider) enableKey(params map[string]any) (*plugin.Response, error) {
	keyID, _ := params["KeyId"].(string)
	if keyID == "" {
		return kmsError("ValidationException", "KeyId is required", http.StatusBadRequest), nil
	}
	k, err := p.resolveKey(keyID)
	if err != nil {
		return kmsError("NotFoundException", "key not found", http.StatusBadRequest), nil
	}
	if err := p.store.UpdateKeyState(k.KeyID, "Enabled"); err != nil {
		return nil, err
	}
	return jsonResp(http.StatusOK, map[string]any{})
}

// --- Data Key Variants ---

func (p *Provider) generateDataKeyWithoutPlaintext(params map[string]any) (*plugin.Response, error) {
	keyID, _ := params["KeyId"].(string)
	if keyID == "" {
		return kmsError("ValidationException", "KeyId is required", http.StatusBadRequest), nil
	}
	k, err := p.resolveKey(keyID)
	if err != nil {
		return kmsError("NotFoundException", "key not found", http.StatusBadRequest), nil
	}
	dataKey := make([]byte, 32)
	if _, err := rand.Read(dataKey); err != nil {
		return nil, err
	}
	ciphertext, err := Encrypt(k, dataKey)
	if err != nil {
		return nil, err
	}
	return jsonResp(http.StatusOK, map[string]any{
		"CiphertextBlob": base64.StdEncoding.EncodeToString(ciphertext),
		"KeyId":          k.ARN,
	})
}

func (p *Provider) generateDataKeyPair(params map[string]any) (*plugin.Response, error) {
	keyID, _ := params["KeyId"].(string)
	if keyID == "" {
		return kmsError("ValidationException", "KeyId is required", http.StatusBadRequest), nil
	}
	k, err := p.resolveKey(keyID)
	if err != nil {
		return kmsError("NotFoundException", "key not found", http.StatusBadRequest), nil
	}
	// Stub: generate fake RSA key pair bytes for dev mode.
	privateKeyBytes := make([]byte, 256)
	if _, err := rand.Read(privateKeyBytes); err != nil {
		return nil, err
	}
	publicKeyBytes := make([]byte, 64)
	if _, err := rand.Read(publicKeyBytes); err != nil {
		return nil, err
	}
	encryptedPrivateKey, err := Encrypt(k, privateKeyBytes)
	if err != nil {
		return nil, err
	}
	keyPairSpec, _ := params["KeyPairSpec"].(string)
	if keyPairSpec == "" {
		keyPairSpec = "RSA_2048"
	}
	return jsonResp(http.StatusOK, map[string]any{
		"PublicKey":            base64.StdEncoding.EncodeToString(publicKeyBytes),
		"PrivateKeyPlaintext":  base64.StdEncoding.EncodeToString(privateKeyBytes),
		"PrivateKeyCiphertext": base64.StdEncoding.EncodeToString(encryptedPrivateKey),
		"KeyId":                k.ARN,
		"KeyPairSpec":          keyPairSpec,
	})
}

func (p *Provider) generateDataKeyPairWithoutPlaintext(params map[string]any) (*plugin.Response, error) {
	keyID, _ := params["KeyId"].(string)
	if keyID == "" {
		return kmsError("ValidationException", "KeyId is required", http.StatusBadRequest), nil
	}
	k, err := p.resolveKey(keyID)
	if err != nil {
		return kmsError("NotFoundException", "key not found", http.StatusBadRequest), nil
	}
	privateKeyBytes := make([]byte, 256)
	if _, err := rand.Read(privateKeyBytes); err != nil {
		return nil, err
	}
	publicKeyBytes := make([]byte, 64)
	if _, err := rand.Read(publicKeyBytes); err != nil {
		return nil, err
	}
	encryptedPrivateKey, err := Encrypt(k, privateKeyBytes)
	if err != nil {
		return nil, err
	}
	keyPairSpec, _ := params["KeyPairSpec"].(string)
	if keyPairSpec == "" {
		keyPairSpec = "RSA_2048"
	}
	return jsonResp(http.StatusOK, map[string]any{
		"PublicKey":            base64.StdEncoding.EncodeToString(publicKeyBytes),
		"PrivateKeyCiphertext": base64.StdEncoding.EncodeToString(encryptedPrivateKey),
		"KeyId":                k.ARN,
		"KeyPairSpec":          keyPairSpec,
	})
}

// --- ReEncrypt ---

func (p *Provider) reEncrypt(params map[string]any) (*plugin.Response, error) {
	ciphertextB64, _ := params["CiphertextBlob"].(string)
	destKeyID, _ := params["DestinationKeyId"].(string)
	if ciphertextB64 == "" || destKeyID == "" {
		return kmsError("ValidationException", "CiphertextBlob and DestinationKeyId are required", http.StatusBadRequest), nil
	}
	ciphertext, err := base64.StdEncoding.DecodeString(ciphertextB64)
	if err != nil {
		return kmsError("ValidationException", "CiphertextBlob must be base64", http.StatusBadRequest), nil
	}

	// Decrypt with source key (try explicit SourceKeyId first, then all keys).
	var plaintext []byte
	var srcKey *Key
	sourceKeyID, _ := params["SourceKeyId"].(string)
	if sourceKeyID != "" {
		srcKey, err = p.resolveKey(sourceKeyID)
		if err != nil {
			return kmsError("NotFoundException", "source key not found", http.StatusBadRequest), nil
		}
		plaintext, err = Decrypt(srcKey, ciphertext)
		if err != nil {
			return kmsError("InvalidCiphertextException", "decryption failed", http.StatusBadRequest), nil
		}
	} else {
		keys, _ := p.store.ListKeys(defaultAccountID)
		for _, candidate := range keys {
			c := candidate
			if pt, e := Decrypt(&c, ciphertext); e == nil {
				plaintext = pt
				srcKey = &c
				break
			}
		}
		if plaintext == nil {
			return kmsError("InvalidCiphertextException", "decryption failed", http.StatusBadRequest), nil
		}
	}

	destKey, err := p.resolveKey(destKeyID)
	if err != nil {
		return kmsError("NotFoundException", "destination key not found", http.StatusBadRequest), nil
	}
	newCiphertext, err := Encrypt(destKey, plaintext)
	if err != nil {
		return nil, err
	}
	return jsonResp(http.StatusOK, map[string]any{
		"CiphertextBlob":                 base64.StdEncoding.EncodeToString(newCiphertext),
		"SourceKeyId":                    srcKey.ARN,
		"DestinationKeyId":               destKey.ARN,
		"SourceEncryptionAlgorithm":      "SYMMETRIC_DEFAULT",
		"DestinationEncryptionAlgorithm": "SYMMETRIC_DEFAULT",
	})
}

// --- Sign / Verify / GetPublicKey ---

func (p *Provider) sign(params map[string]any) (*plugin.Response, error) {
	keyID, _ := params["KeyId"].(string)
	messageB64, _ := params["Message"].(string)
	if keyID == "" || messageB64 == "" {
		return kmsError("ValidationException", "KeyId and Message are required", http.StatusBadRequest), nil
	}
	message, err := base64.StdEncoding.DecodeString(messageB64)
	if err != nil {
		return kmsError("ValidationException", "Message must be base64", http.StatusBadRequest), nil
	}
	k, err := p.resolveKey(keyID)
	if err != nil {
		return kmsError("NotFoundException", "key not found", http.StatusBadRequest), nil
	}
	signingAlgorithm, _ := params["SigningAlgorithm"].(string)
	if signingAlgorithm == "" {
		signingAlgorithm = "RSASSA_PKCS1_V1_5_SHA_256"
	}
	sig := HMACSign(k, message)
	return jsonResp(http.StatusOK, map[string]any{
		"Signature":        base64.StdEncoding.EncodeToString(sig),
		"KeyId":            k.ARN,
		"SigningAlgorithm": signingAlgorithm,
	})
}

func (p *Provider) verify(params map[string]any) (*plugin.Response, error) {
	keyID, _ := params["KeyId"].(string)
	messageB64, _ := params["Message"].(string)
	signatureB64, _ := params["Signature"].(string)
	if keyID == "" || messageB64 == "" || signatureB64 == "" {
		return kmsError("ValidationException", "KeyId, Message, and Signature are required", http.StatusBadRequest), nil
	}
	message, err := base64.StdEncoding.DecodeString(messageB64)
	if err != nil {
		return kmsError("ValidationException", "Message must be base64", http.StatusBadRequest), nil
	}
	signature, err := base64.StdEncoding.DecodeString(signatureB64)
	if err != nil {
		return kmsError("ValidationException", "Signature must be base64", http.StatusBadRequest), nil
	}
	k, err := p.resolveKey(keyID)
	if err != nil {
		return kmsError("NotFoundException", "key not found", http.StatusBadRequest), nil
	}
	signingAlgorithm, _ := params["SigningAlgorithm"].(string)
	if signingAlgorithm == "" {
		signingAlgorithm = "RSASSA_PKCS1_V1_5_SHA_256"
	}
	valid := HMACVerify(k, message, signature)
	return jsonResp(http.StatusOK, map[string]any{
		"SignatureValid":   valid,
		"KeyId":            k.ARN,
		"SigningAlgorithm": signingAlgorithm,
	})
}

func (p *Provider) getPublicKey(params map[string]any) (*plugin.Response, error) {
	keyID, _ := params["KeyId"].(string)
	if keyID == "" {
		return kmsError("ValidationException", "KeyId is required", http.StatusBadRequest), nil
	}
	k, err := p.resolveKey(keyID)
	if err != nil {
		return kmsError("NotFoundException", "key not found", http.StatusBadRequest), nil
	}
	// Stub: return key material as public key for dev mode.
	return jsonResp(http.StatusOK, map[string]any{
		"PublicKey": base64.StdEncoding.EncodeToString(k.KeyMaterial),
		"KeyId":     k.ARN,
		"KeySpec":   k.KeySpec,
		"KeyUsage":  k.KeyUsage,
	})
}

// --- Key Management ---

func (p *Provider) scheduleKeyDeletion(params map[string]any) (*plugin.Response, error) {
	keyID, _ := params["KeyId"].(string)
	if keyID == "" {
		return kmsError("ValidationException", "KeyId is required", http.StatusBadRequest), nil
	}
	k, err := p.resolveKey(keyID)
	if err != nil {
		return kmsError("NotFoundException", "key not found", http.StatusBadRequest), nil
	}
	days := int64(30)
	if v, ok := params["PendingWindowInDays"].(float64); ok {
		days = int64(v)
	}
	deletionDate := time.Now().AddDate(0, 0, int(days)).Unix()
	if err := p.store.SetDeletionDate(k.KeyID, deletionDate); err != nil {
		return nil, err
	}
	return jsonResp(http.StatusOK, map[string]any{
		"KeyId":        k.ARN,
		"DeletionDate": deletionDate,
		"KeyState":     "PendingDeletion",
	})
}

func (p *Provider) cancelKeyDeletion(params map[string]any) (*plugin.Response, error) {
	keyID, _ := params["KeyId"].(string)
	if keyID == "" {
		return kmsError("ValidationException", "KeyId is required", http.StatusBadRequest), nil
	}
	k, err := p.resolveKey(keyID)
	if err != nil {
		return kmsError("NotFoundException", "key not found", http.StatusBadRequest), nil
	}
	if err := p.store.CancelDeletion(k.KeyID); err != nil {
		return nil, err
	}
	return jsonResp(http.StatusOK, map[string]any{
		"KeyId":    k.ARN,
		"KeyState": "Enabled",
	})
}

func (p *Provider) updateKeyDescription(params map[string]any) (*plugin.Response, error) {
	keyID, _ := params["KeyId"].(string)
	description, _ := params["Description"].(string)
	if keyID == "" {
		return kmsError("ValidationException", "KeyId is required", http.StatusBadRequest), nil
	}
	k, err := p.resolveKey(keyID)
	if err != nil {
		return kmsError("NotFoundException", "key not found", http.StatusBadRequest), nil
	}
	if err := p.store.UpdateKeyDescription(k.KeyID, description); err != nil {
		return nil, err
	}
	return jsonResp(http.StatusOK, map[string]any{})
}

// --- Grants ---

func (p *Provider) createGrant(params map[string]any) (*plugin.Response, error) {
	keyID, _ := params["KeyId"].(string)
	granteePrincipal, _ := params["GranteePrincipal"].(string)
	if keyID == "" || granteePrincipal == "" {
		return kmsError("ValidationException", "KeyId and GranteePrincipal are required", http.StatusBadRequest), nil
	}
	k, err := p.resolveKey(keyID)
	if err != nil {
		return kmsError("NotFoundException", "key not found", http.StatusBadRequest), nil
	}

	// Serialize Operations list.
	var opsJSON []byte
	if ops, ok := params["Operations"].([]any); ok {
		opsJSON, _ = json.Marshal(ops)
	} else {
		opsJSON = []byte("[]")
	}

	retiringPrincipal, _ := params["RetiringPrincipal"].(string)
	name, _ := params["Name"].(string)
	grantID := randomHex(16)
	grantToken := randomHex(32)

	grant, err := p.store.CreateGrant(grantID, grantToken, k.KeyID, granteePrincipal, retiringPrincipal, string(opsJSON), name, defaultAccountID)
	if err != nil {
		return nil, err
	}
	return jsonResp(http.StatusOK, map[string]any{
		"GrantId":    grant.GrantID,
		"GrantToken": grant.GrantToken,
	})
}

func (p *Provider) listGrants(params map[string]any) (*plugin.Response, error) {
	keyID, _ := params["KeyId"].(string)
	if keyID == "" {
		return kmsError("ValidationException", "KeyId is required", http.StatusBadRequest), nil
	}
	k, err := p.resolveKey(keyID)
	if err != nil {
		return kmsError("NotFoundException", "key not found", http.StatusBadRequest), nil
	}
	grants, err := p.store.ListGrants(k.KeyID, defaultAccountID)
	if err != nil {
		return nil, err
	}
	return jsonResp(http.StatusOK, map[string]any{"Grants": grantsToList(grants)})
}

func (p *Provider) retireGrant(params map[string]any) (*plugin.Response, error) {
	grantToken, _ := params["GrantToken"].(string)
	if grantToken != "" {
		if err := p.store.DeleteGrantByToken(grantToken); err != nil {
			return kmsError("NotFoundException", "grant not found", http.StatusBadRequest), nil
		}
		return jsonResp(http.StatusOK, map[string]any{})
	}
	// Fallback: KeyId + GrantId
	keyID, _ := params["KeyId"].(string)
	grantID, _ := params["GrantId"].(string)
	if keyID == "" || grantID == "" {
		return kmsError("ValidationException", "GrantToken or KeyId+GrantId required", http.StatusBadRequest), nil
	}
	k, err := p.resolveKey(keyID)
	if err != nil {
		return kmsError("NotFoundException", "key not found", http.StatusBadRequest), nil
	}
	if err := p.store.DeleteGrantByID(k.KeyID, grantID, defaultAccountID); err != nil {
		return kmsError("NotFoundException", "grant not found", http.StatusBadRequest), nil
	}
	return jsonResp(http.StatusOK, map[string]any{})
}

func (p *Provider) revokeGrant(params map[string]any) (*plugin.Response, error) {
	keyID, _ := params["KeyId"].(string)
	grantID, _ := params["GrantId"].(string)
	if keyID == "" || grantID == "" {
		return kmsError("ValidationException", "KeyId and GrantId are required", http.StatusBadRequest), nil
	}
	k, err := p.resolveKey(keyID)
	if err != nil {
		return kmsError("NotFoundException", "key not found", http.StatusBadRequest), nil
	}
	if err := p.store.DeleteGrantByID(k.KeyID, grantID, defaultAccountID); err != nil {
		return kmsError("NotFoundException", "grant not found", http.StatusBadRequest), nil
	}
	return jsonResp(http.StatusOK, map[string]any{})
}

func (p *Provider) listRetirableGrants(params map[string]any) (*plugin.Response, error) {
	retiringPrincipal, _ := params["RetiringPrincipal"].(string)
	if retiringPrincipal == "" {
		return kmsError("ValidationException", "RetiringPrincipal is required", http.StatusBadRequest), nil
	}
	grants, err := p.store.ListRetirableGrants(retiringPrincipal, defaultAccountID)
	if err != nil {
		return nil, err
	}
	return jsonResp(http.StatusOK, map[string]any{"Grants": grantsToList(grants)})
}

func grantsToList(grants []Grant) []map[string]any {
	list := make([]map[string]any, 0, len(grants))
	for _, g := range grants {
		var ops []string
		_ = json.Unmarshal([]byte(g.Operations), &ops) //nolint:errcheck
		entry := map[string]any{
			"GrantId":          g.GrantID,
			"GrantToken":       g.GrantToken,
			"KeyId":            g.KeyID,
			"GranteePrincipal": g.GranteePrincipal,
			"Operations":       ops,
			"CreationDate":     g.CreatedAt.Unix(),
		}
		if g.Name != "" {
			entry["Name"] = g.Name
		}
		if g.RetiringPrincipal != "" {
			entry["RetiringPrincipal"] = g.RetiringPrincipal
		}
		list = append(list, entry)
	}
	return list
}

// --- Key Policies ---

func (p *Provider) getKeyPolicy(params map[string]any) (*plugin.Response, error) {
	keyID, _ := params["KeyId"].(string)
	policyName, _ := params["PolicyName"].(string)
	if keyID == "" {
		return kmsError("ValidationException", "KeyId is required", http.StatusBadRequest), nil
	}
	if policyName == "" {
		policyName = "default"
	}
	k, err := p.resolveKey(keyID)
	if err != nil {
		return kmsError("NotFoundException", "key not found", http.StatusBadRequest), nil
	}
	policy, err := p.store.GetKeyPolicy(k.KeyID, policyName, defaultAccountID)
	if err != nil {
		return nil, err
	}
	if policy == "" {
		// Return a default policy.
		policy = fmt.Sprintf(`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"arn:aws:iam::%s:root"},"Action":"kms:*","Resource":"*"}]}`, defaultAccountID)
	}
	return jsonResp(http.StatusOK, map[string]any{"Policy": policy})
}

func (p *Provider) putKeyPolicy(params map[string]any) (*plugin.Response, error) {
	keyID, _ := params["KeyId"].(string)
	policyName, _ := params["PolicyName"].(string)
	policy, _ := params["Policy"].(string)
	if keyID == "" || policyName == "" || policy == "" {
		return kmsError("ValidationException", "KeyId, PolicyName, and Policy are required", http.StatusBadRequest), nil
	}
	k, err := p.resolveKey(keyID)
	if err != nil {
		return kmsError("NotFoundException", "key not found", http.StatusBadRequest), nil
	}
	if err := p.store.PutKeyPolicy(k.KeyID, policyName, policy, defaultAccountID); err != nil {
		return nil, err
	}
	return jsonResp(http.StatusOK, map[string]any{})
}

func (p *Provider) listKeyPolicies(params map[string]any) (*plugin.Response, error) {
	keyID, _ := params["KeyId"].(string)
	if keyID == "" {
		return kmsError("ValidationException", "KeyId is required", http.StatusBadRequest), nil
	}
	_, err := p.resolveKey(keyID)
	if err != nil {
		return kmsError("NotFoundException", "key not found", http.StatusBadRequest), nil
	}
	return jsonResp(http.StatusOK, map[string]any{"PolicyNames": []string{"default"}})
}

// resolveKey accepts a key ID, ARN, or alias name.
func (p *Provider) resolveKey(keyID string) (*Key, error) {
	if strings.HasPrefix(keyID, "alias/") {
		return p.store.GetKeyByAlias(keyID)
	}
	if strings.Contains(keyID, ":key/") {
		parts := strings.Split(keyID, "/")
		keyID = parts[len(parts)-1]
	}
	return p.store.GetKey(keyID)
}

func keyMetadata(k *Key) map[string]any {
	return map[string]any{
		"KeyId":        k.KeyID,
		"Arn":          k.ARN,
		"Description":  k.Description,
		"KeyState":     k.KeyState,
		"KeySpec":      k.KeySpec,
		"KeyUsage":     k.KeyUsage,
		"CreationDate": k.CreatedAt.Unix(),
		"Enabled":      k.KeyState == "Enabled",
	}
}

func jsonResp(status int, v any) (*plugin.Response, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	return &plugin.Response{
		StatusCode:  status,
		Body:        b,
		ContentType: "application/x-amz-json-1.1",
	}, nil
}

func kmsError(code, message string, status int) *plugin.Response {
	b, _ := json.Marshal(map[string]string{
		"__type":  code,
		"message": message,
	})
	return &plugin.Response{
		StatusCode:  status,
		Body:        b,
		ContentType: "application/x-amz-json-1.1",
	}
}
