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
	if !strings.HasPrefix(authz, "SharedKey ") {
		return Identity{}, false
	}

	parts := strings.SplitN(authz[10:], ":", 2)
	if len(parts) != 2 {
		return Identity{}, false
	}

	return Identity{
		Provider:    s.Provider(),
		AccessKeyID: parts[0],
		Service:     "blob",
	}, true
}
