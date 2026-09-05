// SPDX-License-Identifier: Apache-2.0

// Package auth reads the credentials a request carries and reports who the
// caller claimed to be — one adapter per cloud provider.
//
// DevCloud does not verify signatures and no adapter rejects a request. That is
// deliberate and documented: a local development server that refused calls
// because a signature did not match would break every SDK pointed at it with
// placeholder credentials (see docs/compatibility-policy.md, "credentials are
// accepted without signature verification"). An adapter's job is to *read* the
// claim, not to judge it — which is why Authenticate returns an Identity and a
// "did we recognize anything" bool rather than an error.
//
// The interface exists so a second CSP is an added file, not a rewrite: SigV4
// is AWS's credential form, Azure presents AAD bearer tokens or SAS query
// strings, and Google Cloud presents OAuth2. Each becomes an Adapter in
// Adapters; the gateway does not change.
package auth

import (
	"context"
	"net/http"
)

// Identity is what a request claimed about its caller. Fields the request did
// not carry are empty, and nothing here has been verified.
type Identity struct {
	// Provider is the CSP whose credential form was recognized, e.g. "aws".
	Provider string
	// AccessKeyID is the key the caller signed with. For AWS it is the first
	// element of the SigV4 credential scope.
	AccessKeyID string
	// Region is the region the caller addressed. DevCloud is regionless, so
	// this is what the SDK believed, not where anything lives.
	Region string
	// Service is the signing name exactly as presented. It is not a DevCloud
	// service ID — mapping the two is the gateway's job (normalizeServiceID).
	Service string
	// SessionToken is the temporary-credential token, when one was sent.
	SessionToken string
}

// Adapter reads one provider's credential presentation off a request.
type Adapter interface {
	// Provider is the CSP this adapter speaks for, matching
	// plugin.DefaultProvider's vocabulary.
	Provider() string
	// Authenticate reports the identity the request claims. ok is false when
	// the request carries no credential this adapter recognizes; that is the
	// normal case for an unsigned client, not an error.
	Authenticate(r *http.Request) (id Identity, ok bool)
}

// Adapters is the ordered set Identify consults. Adding a provider means
// appending its adapter here.
var Adapters = []Adapter{SigV4{}}

// Identify returns the first identity any adapter recognizes.
func Identify(r *http.Request) (Identity, bool) {
	for _, a := range Adapters {
		if id, ok := a.Authenticate(r); ok {
			return id, true
		}
	}
	return Identity{}, false
}

// ctxKey is unexported so nothing outside this package can plant an Identity
// that did not come from an adapter.
type ctxKey struct{}

// NewContext returns a copy of ctx carrying id.
func NewContext(ctx context.Context, id Identity) context.Context {
	return context.WithValue(ctx, ctxKey{}, id)
}

// FromContext returns the identity the gateway attached to the request, if the
// request carried a credential at all. Service plugins can call it from
// HandleRequest to read the caller's claimed region or access key.
func FromContext(ctx context.Context) (Identity, bool) {
	id, ok := ctx.Value(ctxKey{}).(Identity)
	return id, ok
}
