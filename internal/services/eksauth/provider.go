// SPDX-License-Identifier: Apache-2.0

package eksauth

import (
	"context"
	"net/http"
	"time"

	generated "github.com/skyoo2003/devcloud/internal/generated/eksauth"
	"github.com/skyoo2003/devcloud/internal/plugin"
	"github.com/skyoo2003/devcloud/internal/shared"
	"github.com/skyoo2003/devcloud/internal/shared/crud"
)

// Provider implements the EKSAuthFrontend service.
type Provider struct {
	generated.BaseProvider
}

func (p *Provider) ServiceID() string             { return "eksauth" }
func (p *Provider) ServiceName() string           { return "EKSAuthFrontend" }
func (p *Provider) Protocol() plugin.ProtocolType { return plugin.ProtocolRESTJSON }

func (p *Provider) Init(_ plugin.PluginConfig) error { return nil }

func (p *Provider) Shutdown(_ context.Context) error { return nil }

func (p *Provider) HandleRequest(_ context.Context, op string, req *http.Request) (*plugin.Response, error) {
	// rest-json carries the operation in the method and path, and
	// gateway.extractOperationName returns "" for every REST protocol, so the
	// provider resolves it from the generated table. The match is run
	// regardless, because the path labels are needed either way.
	matched, params := generated.MatchOperation(req.Method, req.URL.RequestURI())
	if op == "" {
		op = matched
	}

	switch op {
	case "AssumeRoleForPodIdentity":
		return assumeRoleForPodIdentity(params["clusterName"])

	default:
		return nil, plugin.ErrUnhandledOp
	}
}

// assumeRoleForPodIdentity mints a credential set for a pod identity
// association. DevCloud signs nothing: the credentials are well-formed and
// short-lived so an SDK's own validation is satisfied, and they authorize
// nothing outside this process.
func assumeRoleForPodIdentity(clusterName string) (*plugin.Response, error) {
	assocID := shared.GenerateID("a-", 19)
	assumeRoleID := shared.GenerateID("AROA", 21)
	now := time.Now().UTC()

	return shared.JSONResponse(http.StatusOK, map[string]any{
		"subject":  map[string]any{"namespace": "default", "serviceAccount": "default"},
		"audience": "pods.eks.amazonaws.com",
		"podIdentityAssociation": map[string]any{
			"associationArn": shared.BuildARN("eks", "podidentityassociation/"+clusterName, assocID),
			"associationId":  assocID,
		},
		"assumedRoleUser": map[string]any{
			"arn":          shared.BuildARN("sts", "assumed-role/devcloud-pod-identity", "devcloud"),
			"assumeRoleId": assumeRoleID,
		},
		"credentials": map[string]any{
			"accessKeyId":     "ASIADEVCLOUDPODIDENTITY",
			"secretAccessKey": shared.GenerateID("", 40),
			"sessionToken":    shared.GenerateID("", 64),
			"expiration":      now.Add(15 * time.Minute).Format(time.RFC3339),
		},
	})
}

func (p *Provider) ListResources(_ context.Context) ([]plugin.Resource, error) {
	return []plugin.Resource{}, nil
}

func init() {
	plugin.DefaultRegistry.Register("eksauth", func() plugin.ServicePlugin {
		return &Provider{}
	})
	// The signing name is unique, so routing does not need this — but every
	// hand-written rest-json provider in this set states its routes the same
	// way, and crud.Route is documented as a fallback a path resolver can end
	// with.
	crud.RegisterRoutes("eksauth", generated.OperationRoutes)
}
