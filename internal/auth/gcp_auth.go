// SPDX-License-Identifier: Apache-2.0

package auth

import (
	"net/http"
	"strings"
)

// GCPAuth authenticates Google Cloud requests carrying Bearer tokens or Google headers.
type GCPAuth struct{}

func (GCPAuth) Provider() string { return "gcp" }

func (g GCPAuth) Authenticate(r *http.Request) (Identity, bool) {
	authz := r.Header.Get("Authorization")
	hasBearer := strings.HasPrefix(authz, "Bearer ")

	if isGCPContext(r) {
		keyID := "devcloud-gcp-sa"
		if hasBearer {
			token := strings.TrimSpace(strings.TrimPrefix(authz, "Bearer "))
			if token != "" {
				keyID = token
			}
		}
		return Identity{
			Provider:    g.Provider(),
			AccessKeyID: keyID,
			Service:     "gcs",
		}, true
	}

	return Identity{}, false
}

func isGCPContext(r *http.Request) bool {
	if strings.HasPrefix(r.URL.Path, "/storage/v1/") ||
		strings.HasPrefix(r.URL.Path, "/upload/storage/v1/") ||
		strings.HasPrefix(r.URL.Path, "/devcloud/identity/gcp") {
		return true
	}
	if r.Header.Get("x-goog-api-client") != "" ||
		r.Header.Get("x-goog-project-id") != "" ||
		r.Header.Get("x-goog-bucket-name") != "" {
		return true
	}
	ua := r.Header.Get("User-Agent")
	if strings.Contains(ua, "google-cloud") || strings.Contains(ua, "gl-python") {
		return true
	}
	return false
}
