// SPDX-License-Identifier: Apache-2.0

package auth

import (
	"net/http"
	"strings"
)

type AzureSharedKey struct{}

func (AzureSharedKey) Provider() string { return "azure" }

func (s AzureSharedKey) Authenticate(r *http.Request) (Identity, bool) {
	authz := r.Header.Get("Authorization")
	if strings.HasPrefix(authz, "SharedKey ") {
		parts := strings.SplitN(authz[10:], ":", 2)
		if len(parts) == 2 {
			return Identity{
				Provider:    s.Provider(),
				AccessKeyID: parts[0],
				Service:     "blob",
			}, true
		}
	}
	if strings.HasPrefix(authz, "SharedKeyLite ") {
		parts := strings.SplitN(authz[14:], ":", 2)
		if len(parts) == 2 {
			return Identity{
				Provider:    s.Provider(),
				AccessKeyID: parts[0],
				Service:     "blob",
			}, true
		}
	}

	q := r.URL.Query()
	if q.Get("sig") != "" && (q.Get("se") != "" || q.Get("sp") != "" || q.Get("sv") != "") {
		account := "devstoreaccount1"
		trimmed := strings.Trim(r.URL.Path, "/")
		if parts := strings.Split(trimmed, "/"); len(parts) > 0 && parts[0] != "" {
			account = parts[0]
		}
		return Identity{
			Provider:    s.Provider(),
			AccessKeyID: account,
			Service:     "blob",
		}, true
	}

	if r.Header.Get("x-ms-version") != "" || strings.HasPrefix(r.URL.Path, "/devstoreaccount1") {
		account := "devstoreaccount1"
		trimmed := strings.Trim(r.URL.Path, "/")
		if parts := strings.Split(trimmed, "/"); len(parts) > 0 && parts[0] != "" {
			account = parts[0]
		}
		return Identity{
			Provider:    s.Provider(),
			AccessKeyID: account,
			Service:     "blob",
		}, true
	}

	return Identity{}, false
}
