// SPDX-License-Identifier: Apache-2.0

package gateway

import (
	"net/http"

	"github.com/skyoo2003/devcloud/internal/plugin"
)

// defaultAccountID is the account ID used for all requests until real
// account-to-credential mapping is implemented.
const defaultAccountID = plugin.DefaultAccountID

// ExtractAccountID attempts to derive an AWS account ID from the incoming
// request (e.g. by inspecting the Authorization header). Real account mapping
// is deferred; the function always returns the default account ID for now.
func ExtractAccountID(r *http.Request) string {
	// The Authorization header is available for future implementation of
	// access-key → account-ID resolution.
	_ = r.Header.Get("Authorization")
	return defaultAccountID
}
