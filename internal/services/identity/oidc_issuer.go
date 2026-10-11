// SPDX-License-Identifier: Apache-2.0

package identity

import (
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math/big"
	"strings"
	"sync"
	"time"
)

type OIDCIssuer struct {
	mu         sync.RWMutex
	privateKey *rsa.PrivateKey
	keyID      string
}

func NewOIDCIssuer() (*OIDCIssuer, error) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return nil, fmt.Errorf("failed to generate rsa key: %w", err)
	}
	return &OIDCIssuer{
		privateKey: key,
		keyID:      "devcloud-key-1",
	}, nil
}

type JWKKey struct {
	Kty string `json:"kty"`
	Use string `json:"use"`
	Alg string `json:"alg"`
	Kid string `json:"kid"`
	N   string `json:"n"`
	E   string `json:"e"`
}

type JWKSResponse struct {
	Keys []JWKKey `json:"keys"`
}

func (iss *OIDCIssuer) JWKS() JWKSResponse {
	iss.mu.RLock()
	defer iss.mu.RUnlock()

	pub := &iss.privateKey.PublicKey
	nBytes := pub.N.Bytes()
	eBytes := big.NewInt(int64(pub.E)).Bytes()

	return JWKSResponse{
		Keys: []JWKKey{
			{
				Kty: "RSA",
				Use: "sig",
				Alg: "RS256",
				Kid: iss.keyID,
				N:   base64.RawURLEncoding.EncodeToString(nBytes),
				E:   base64.RawURLEncoding.EncodeToString(eBytes),
			},
		},
	}
}

func (iss *OIDCIssuer) OpenIDConfiguration(issuerURL string) map[string]any {
	if issuerURL == "" {
		issuerURL = "http://localhost:4747/devcloud/identity"
	}
	issuerURL = strings.TrimRight(issuerURL, "/")
	return map[string]any{
		"issuer":                                issuerURL,
		"jwks_uri":                              issuerURL + "/jwks.json",
		"token_endpoint":                        issuerURL + "/token",
		"response_types_supported":              []string{"id_token", "token"},
		"subject_types_supported":               []string{"public"},
		"id_token_signing_alg_values_supported": []string{"RS256"},
		"scopes_supported":                      []string{"openid", "email", "profile"},
		"claims_supported":                      []string{"sub", "iss", "aud", "exp", "iat"},
	}
}

type MintTokenOptions struct {
	Subject   string
	Issuer    string
	Audience  any // string or []string
	ExpiresIn time.Duration
	Claims    map[string]any
}

func (iss *OIDCIssuer) MintToken(opts MintTokenOptions) (string, error) {
	iss.mu.RLock()
	defer iss.mu.RUnlock()

	now := time.Now().UTC()
	expDur := opts.ExpiresIn
	if expDur <= 0 {
		expDur = time.Hour
	}
	if opts.Subject == "" {
		opts.Subject = "devcloud-user"
	}
	if opts.Issuer == "" {
		opts.Issuer = "http://localhost:4747/devcloud/identity"
	}
	if opts.Audience == nil {
		opts.Audience = "sts.devcloud"
	}

	header := map[string]string{
		"alg": "RS256",
		"typ": "JWT",
		"kid": iss.keyID,
	}
	headerJSON, err := json.Marshal(header)
	if err != nil {
		return "", err
	}

	claims := map[string]any{
		"sub": opts.Subject,
		"iss": opts.Issuer,
		"aud": opts.Audience,
		"iat": now.Unix(),
		"exp": now.Add(expDur).Unix(),
	}
	for k, v := range opts.Claims {
		claims[k] = v
	}
	claimsJSON, err := json.Marshal(claims)
	if err != nil {
		return "", err
	}

	headerB64 := base64.RawURLEncoding.EncodeToString(headerJSON)
	payloadB64 := base64.RawURLEncoding.EncodeToString(claimsJSON)
	sigInput := headerB64 + "." + payloadB64

	hashed := sha256.Sum256([]byte(sigInput))
	sig, err := rsa.SignPKCS1v15(rand.Reader, iss.privateKey, crypto.SHA256, hashed[:])
	if err != nil {
		return "", err
	}
	sigB64 := base64.RawURLEncoding.EncodeToString(sig)

	return sigInput + "." + sigB64, nil
}

type ParsedIdentity struct {
	Subject  string
	Issuer   string
	Audience string
	Claims   map[string]any
}

// ParseWebIdentityToken parses an identity token (OIDC JWT, Google Access Token, Azure token, or raw string)
// and extracts the subject, issuer, and audience claims.
func ParseWebIdentityToken(token string) ParsedIdentity {
	result := ParsedIdentity{
		Subject:  "sub-devcloud-user",
		Issuer:   "https://sts.amazonaws.com",
		Audience: "sts.devcloud",
		Claims:   make(map[string]any),
	}
	if token == "" {
		return result
	}

	// 1. Google OAuth2 / STS Access Token (ya29...)
	if strings.HasPrefix(token, "ya29.") {
		sub := strings.TrimPrefix(token, "ya29.")
		if len(sub) > 32 {
			sub = sub[:32]
		}
		result.Subject = "gcp:" + sub
		result.Issuer = "https://accounts.google.com"
		result.Audience = "sts.amazonaws.com"
		return result
	}

	// 2. Azure AD token simulation
	if strings.HasPrefix(token, "azure-bearer-") {
		sub := strings.TrimPrefix(token, "azure-bearer-")
		result.Subject = "azure:" + sub
		result.Issuer = "https://sts.windows.net/"
		result.Audience = "https://management.azure.com"
		return result
	}

	// 3. Standard 3-part JWT
	parts := strings.Split(token, ".")
	if len(parts) == 3 {
		payloadBytes, err := base64.RawURLEncoding.DecodeString(parts[1])
		if err != nil {
			payloadBytes, err = base64.StdEncoding.DecodeString(parts[1])
		}
		if err == nil {
			var claims map[string]any
			if json.Unmarshal(payloadBytes, &claims) == nil {
				result.Claims = claims
				if sub, ok := claims["sub"].(string); ok && sub != "" {
					result.Subject = sub
				}
				if iss, ok := claims["iss"].(string); ok && iss != "" {
					result.Issuer = iss
				}
				if aud, ok := claims["aud"].(string); ok && aud != "" {
					result.Audience = aud
				} else if auds, ok := claims["aud"].([]any); ok && len(auds) > 0 {
					if firstAud, ok := auds[0].(string); ok {
						result.Audience = firstAud
					}
				}
				return result
			}
		}
	}

	// 4. Fallback for non-JWT arbitrary strings
	end := min(len(token), 16)
	result.Subject = "sub-" + token[:end]
	return result
}
