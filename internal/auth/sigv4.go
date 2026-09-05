// SPDX-License-Identifier: Apache-2.0

package auth

import (
	"net/http"
	"strings"

	"github.com/skyoo2003/devcloud/internal/plugin"
)

// sigV4Algorithm prefixes every SigV4 credential presentation, in both the
// Authorization header and the presigned-URL query form.
const sigV4Algorithm = "AWS4-HMAC-SHA256"

// credentialParam is the presigned-URL query parameter carrying the credential
// scope. Named here because Authenticate scans the raw query for it before
// paying for a full query parse.
const credentialParam = "X-Amz-Credential"

const sessionTokenParam = "X-Amz-Security-Token"

// SigV4 reads AWS Signature Version 4 credentials. It parses the credential
// scope and stops there: the signature is never recomputed or compared. See the
// package comment for why.
type SigV4 struct{}

func (SigV4) Provider() string { return plugin.DefaultProvider }

// Authenticate reads the credential scope from the Authorization header, then
// from the presigned-URL query string. Both carry the same scope:
//
//	<accessKeyID>/<YYYYMMDD>/<region>/<signingName>/aws4_request
//
// A scope shorter than four elements yields an identity with only the access
// key filled in, which is what a malformed or truncated header deserves —
// callers check the field they need rather than trusting ok to mean "complete".
func (s SigV4) Authenticate(r *http.Request) (Identity, bool) {
	scope := credentialFromHeader(r.Header.Get("Authorization"))
	token := r.Header.Get(sessionTokenParam)

	// Presigned URLs carry no Authorization header. Scanning RawQuery first
	// keeps the common signed-header path off url.Values allocation.
	if scope == "" && strings.Contains(r.URL.RawQuery, credentialParam) {
		q := r.URL.Query()
		scope = q.Get(credentialParam)
		if token == "" {
			token = q.Get(sessionTokenParam)
		}
	}
	if scope == "" {
		return Identity{}, false
	}

	id := Identity{Provider: s.Provider(), SessionToken: token}
	parts := strings.Split(scope, "/")
	id.AccessKeyID = parts[0]
	if len(parts) >= 4 {
		id.Region, id.Service = parts[2], parts[3]
	}
	return id, true
}

// credentialFromHeader extracts the credential scope from an Authorization
// header of the form:
//
//	AWS4-HMAC-SHA256 Credential=<scope>, SignedHeaders=<...>, Signature=<...>
func credentialFromHeader(authz string) string {
	if !strings.HasPrefix(authz, sigV4Algorithm) {
		return ""
	}
	idx := strings.Index(authz, "Credential=")
	if idx < 0 {
		return ""
	}
	scope := authz[idx+len("Credential="):]
	if end := strings.IndexByte(scope, ','); end >= 0 {
		scope = scope[:end]
	}
	return strings.TrimSpace(scope)
}
