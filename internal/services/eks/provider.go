// SPDX-License-Identifier: Apache-2.0

// internal/services/eks/provider.go
package eks

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"strings"

	"github.com/skyoo2003/devcloud/internal/plugin"
	"github.com/skyoo2003/devcloud/internal/shared"
	"github.com/skyoo2003/devcloud/internal/storage/sqlite"
)

// Provider implements the EKS service.
type Provider struct {
	store *Store
}

func (p *Provider) ServiceID() string             { return "eks" }
func (p *Provider) ServiceName() string           { return "WesleyFrontend" }
func (p *Provider) Protocol() plugin.ProtocolType { return plugin.ProtocolRESTJSON }

func (p *Provider) Init(cfg plugin.PluginConfig) error {
	dataDir := cfg.DataDir
	if dataDir == "" {
		dataDir = "."
	}
	var err error
	p.store, err = NewStore(filepath.Join(dataDir, "eks"))
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
		return shared.JSONError("SerializationException", "failed to read body", http.StatusBadRequest), nil
	}
	var params map[string]any
	if len(body) > 0 {
		if err := json.Unmarshal(body, &params); err != nil {
			return shared.JSONError("SerializationException", "invalid JSON", http.StatusBadRequest), nil
		}
	} else {
		params = map[string]any{}
	}

	path := req.URL.Path

	if op == "" {
		op = resolveOp(req.Method, path)
	}

	switch op {
	// ── Cluster ──────────────────────────────────────────────────────────────
	case "CreateCluster":
		return p.createCluster(params)
	case "DescribeCluster":
		name := extractSegment(path, "clusters", 1)
		return p.describeCluster(name)
	case "ListClusters":
		return p.listClusters()
	case "DeleteCluster":
		name := extractSegment(path, "clusters", 1)
		return p.deleteCluster(name)
	case "UpdateClusterConfig":
		name := extractSegment(path, "clusters", 1)
		return p.updateClusterConfig(name, params)
	case "UpdateClusterVersion":
		name := extractSegment(path, "clusters", 1)
		return p.updateClusterVersion(name, params)

	// ── Nodegroup ─────────────────────────────────────────────────────────────
	case "CreateNodegroup":
		clusterName := extractSegment(path, "clusters", 1)
		return p.createNodegroup(clusterName, params)
	case "DescribeNodegroup":
		clusterName, ngName := extractTwoSegments(path, "clusters", "node-groups")
		return p.describeNodegroup(clusterName, ngName)
	case "ListNodegroups":
		clusterName := extractSegment(path, "clusters", 1)
		return p.listNodegroups(clusterName)
	case "DeleteNodegroup":
		clusterName, ngName := extractTwoSegments(path, "clusters", "node-groups")
		return p.deleteNodegroup(clusterName, ngName)
	case "UpdateNodegroupConfig":
		clusterName, ngName := extractTwoSegments(path, "clusters", "node-groups")
		return p.updateNodegroupConfig(clusterName, ngName, params)
	case "UpdateNodegroupVersion":
		clusterName, ngName := extractTwoSegments(path, "clusters", "node-groups")
		return p.updateNodegroupVersion(clusterName, ngName, params)

	// ── FargateProfile ────────────────────────────────────────────────────────
	case "CreateFargateProfile":
		clusterName := extractSegment(path, "clusters", 1)
		return p.createFargateProfile(clusterName, params)
	case "DescribeFargateProfile":
		clusterName, fpName := extractTwoSegments(path, "clusters", "fargate-profiles")
		return p.describeFargateProfile(clusterName, fpName)
	case "ListFargateProfiles":
		clusterName := extractSegment(path, "clusters", 1)
		return p.listFargateProfiles(clusterName)
	case "DeleteFargateProfile":
		clusterName, fpName := extractTwoSegments(path, "clusters", "fargate-profiles")
		return p.deleteFargateProfile(clusterName, fpName)

	// ── Addon ─────────────────────────────────────────────────────────────────
	case "CreateAddon":
		clusterName := extractSegment(path, "clusters", 1)
		return p.createAddon(clusterName, params)
	case "DescribeAddon":
		clusterName, addonName := extractTwoSegments(path, "clusters", "addons")
		return p.describeAddon(clusterName, addonName)
	case "ListAddons":
		clusterName := extractSegment(path, "clusters", 1)
		return p.listAddons(clusterName)
	case "DeleteAddon":
		clusterName, addonName := extractTwoSegments(path, "clusters", "addons")
		return p.deleteAddon(clusterName, addonName)
	case "UpdateAddon":
		clusterName, addonName := extractTwoSegments(path, "clusters", "addons")
		return p.updateAddon(clusterName, addonName, params)
	case "DescribeAddonVersions":
		return shared.JSONResponse(http.StatusOK, map[string]any{"addons": []any{}, "nextToken": ""})
	case "DescribeAddonConfiguration":
		return shared.JSONResponse(http.StatusOK, map[string]any{"addonName": "", "addonVersion": "", "configurationSchema": ""})

	// ── Tags ──────────────────────────────────────────────────────────────────
	case "TagResource":
		resourceARN := extractTagARN(path)
		return p.tagResource(resourceARN, params)
	case "UntagResource":
		resourceARN := extractTagARN(path)
		tagKeys := req.URL.Query()["tagKeys"]
		return p.untagResource(resourceARN, tagKeys)
	case "ListTagsForResource":
		resourceARN := extractTagARN(path)
		return p.listTagsForResource(resourceARN)

	// ── AccessEntry ───────────────────────────────────────────────────────────
	case "CreateAccessEntry":
		clusterName := extractSegment(path, "clusters", 1)
		return p.createAccessEntry(clusterName, params)
	case "DescribeAccessEntry":
		clusterName, principalARN := extractTwoSegments(path, "clusters", "access-entries")
		return p.describeAccessEntry(clusterName, principalARN)
	case "ListAccessEntries":
		clusterName := extractSegment(path, "clusters", 1)
		return p.listAccessEntries(clusterName)
	case "DeleteAccessEntry":
		clusterName, principalARN := extractTwoSegments(path, "clusters", "access-entries")
		return p.deleteAccessEntry(clusterName, principalARN)
	case "UpdateAccessEntry":
		clusterName, principalARN := extractTwoSegments(path, "clusters", "access-entries")
		return p.updateAccessEntry(clusterName, principalARN, params)

	// ── PodIdentityAssociation ────────────────────────────────────────────────
	case "CreatePodIdentityAssociation":
		clusterName := extractSegment(path, "clusters", 1)
		return p.createPodIdentityAssociation(clusterName, params)
	case "DescribePodIdentityAssociation":
		clusterName, assocID := extractTwoSegments(path, "clusters", "pod-identity-associations")
		return p.describePodIdentityAssociation(clusterName, assocID)
	case "ListPodIdentityAssociations":
		clusterName := extractSegment(path, "clusters", 1)
		return p.listPodIdentityAssociations(clusterName)
	case "DeletePodIdentityAssociation":
		clusterName, assocID := extractTwoSegments(path, "clusters", "pod-identity-associations")
		return p.deletePodIdentityAssociation(clusterName, assocID)
	case "UpdatePodIdentityAssociation":
		clusterName, assocID := extractTwoSegments(path, "clusters", "pod-identity-associations")
		return p.updatePodIdentityAssociation(clusterName, assocID, params)

	// ── Stub operations ───────────────────────────────────────────────────────
	case "AssociateAccessPolicy",
		"DisassociateAccessPolicy",
		"ListAssociatedAccessPolicies",
		"ListAccessPolicies":
		return shared.JSONResponse(http.StatusOK, map[string]any{"accessPolicies": []any{}, "nextToken": ""})

	case "AssociateEncryptionConfig",
		"AssociateIdentityProviderConfig",
		"DisassociateIdentityProviderConfig":
		return shared.JSONResponse(http.StatusOK, map[string]any{"update": map[string]any{}})

	case "DescribeIdentityProviderConfig":
		return shared.JSONResponse(http.StatusOK, map[string]any{"identityProviderConfig": map[string]any{}})
	case "ListIdentityProviderConfigs":
		return shared.JSONResponse(http.StatusOK, map[string]any{"identityProviderConfigs": []any{}, "nextToken": ""})

	case "DescribeUpdate",
		"ListUpdates":
		return shared.JSONResponse(http.StatusOK, map[string]any{"update": map[string]any{}, "updateIds": []any{}, "nextToken": ""})

	case "DescribeInsight":
		return shared.JSONResponse(http.StatusOK, map[string]any{"insight": map[string]any{}})
	case "DescribeInsightsRefresh":
		return shared.JSONResponse(http.StatusOK, map[string]any{"insightsRefresh": map[string]any{}})
	case "ListInsights":
		return shared.JSONResponse(http.StatusOK, map[string]any{"insights": []any{}, "nextToken": ""})
	case "StartInsightsRefresh":
		return shared.JSONResponse(http.StatusOK, map[string]any{"insightsRefreshId": shared.GenerateUUID()})

	case "CreateCapability",
		"UpdateCapability":
		return shared.JSONResponse(http.StatusOK, map[string]any{"capability": map[string]any{}})
	case "DeleteCapability":
		return shared.JSONResponse(http.StatusOK, map[string]any{})
	case "DescribeCapability":
		return shared.JSONResponse(http.StatusOK, map[string]any{"capability": map[string]any{}})
	case "ListCapabilities":
		return shared.JSONResponse(http.StatusOK, map[string]any{"capabilities": []any{}, "nextToken": ""})

	case "CreateEksAnywhereSubscription",
		"UpdateEksAnywhereSubscription":
		return shared.JSONResponse(http.StatusOK, map[string]any{"subscription": map[string]any{}})
	case "DeleteEksAnywhereSubscription":
		return shared.JSONResponse(http.StatusOK, map[string]any{"subscription": map[string]any{}})
	case "DescribeEksAnywhereSubscription":
		return shared.JSONResponse(http.StatusOK, map[string]any{"subscription": map[string]any{}})
	case "ListEksAnywhereSubscriptions":
		return shared.JSONResponse(http.StatusOK, map[string]any{"subscriptions": []any{}, "nextToken": ""})

	case "RegisterCluster":
		return shared.JSONResponse(http.StatusOK, map[string]any{"cluster": map[string]any{}})
	case "DeregisterCluster":
		return shared.JSONResponse(http.StatusOK, map[string]any{"cluster": map[string]any{}})

	case "DescribeClusterVersions":
		return shared.JSONResponse(http.StatusOK, map[string]any{"clusterVersions": []any{}, "nextToken": ""})

	default:
		return shared.JSONError("InvalidAction", fmt.Sprintf("unknown action: %s", op), http.StatusBadRequest), nil
	}
}

func resolveOp(method, path string) string {
	p := strings.Trim(path, "/")
	seg := strings.Split(p, "/")
	n := len(seg)

	switch {
	// Tags: /tags/{arn}
	case n >= 1 && seg[0] == "tags":
		switch method {
		case http.MethodPost:
			return "TagResource"
		case http.MethodGet:
			return "ListTagsForResource"
		case http.MethodDelete:
			return "UntagResource"
		}

	// Clusters
	case n >= 1 && seg[0] == "clusters":
		if n == 1 {
			switch method {
			case http.MethodPost:
				return "CreateCluster"
			case http.MethodGet:
				return "ListClusters"
			}
		}
		if n == 2 {
			switch method {
			case http.MethodGet:
				return "DescribeCluster"
			case http.MethodDelete:
				return "DeleteCluster"
			}
		}
		// /clusters/{name}/update-config
		if n == 3 && seg[2] == "update-config" && method == http.MethodPost {
			return "UpdateClusterConfig"
		}
		// /clusters/{name}/update-version
		if n == 3 && seg[2] == "update-version" && method == http.MethodPost {
			return "UpdateClusterVersion"
		}
		// /clusters/{name}/node-groups
		if n >= 3 && seg[2] == "node-groups" {
			if n == 3 {
				switch method {
				case http.MethodPost:
					return "CreateNodegroup"
				case http.MethodGet:
					return "ListNodegroups"
				}
			}
			if n == 4 {
				switch method {
				case http.MethodGet:
					return "DescribeNodegroup"
				case http.MethodDelete:
					return "DeleteNodegroup"
				}
			}
			if n == 5 && seg[4] == "update-config" && method == http.MethodPost {
				return "UpdateNodegroupConfig"
			}
			if n == 5 && seg[4] == "update-version" && method == http.MethodPost {
				return "UpdateNodegroupVersion"
			}
		}
		// /clusters/{name}/fargate-profiles
		if n >= 3 && seg[2] == "fargate-profiles" {
			if n == 3 {
				switch method {
				case http.MethodPost:
					return "CreateFargateProfile"
				case http.MethodGet:
					return "ListFargateProfiles"
				}
			}
			if n == 4 {
				switch method {
				case http.MethodGet:
					return "DescribeFargateProfile"
				case http.MethodDelete:
					return "DeleteFargateProfile"
				}
			}
		}
		// /clusters/{name}/addons
		if n >= 3 && seg[2] == "addons" {
			if n == 3 {
				switch method {
				case http.MethodPost:
					return "CreateAddon"
				case http.MethodGet:
					return "ListAddons"
				}
			}
			if n == 4 {
				switch method {
				case http.MethodGet:
					return "DescribeAddon"
				case http.MethodDelete:
					return "DeleteAddon"
				case http.MethodPost:
					return "UpdateAddon"
				}
			}
		}
		// /clusters/{name}/access-entries
		if n >= 3 && seg[2] == "access-entries" {
			if n == 3 {
				switch method {
				case http.MethodPost:
					return "CreateAccessEntry"
				case http.MethodGet:
					return "ListAccessEntries"
				}
			}
			if n == 4 {
				switch method {
				case http.MethodGet:
					return "DescribeAccessEntry"
				case http.MethodDelete:
					return "DeleteAccessEntry"
				case http.MethodPost:
					return "UpdateAccessEntry"
				}
			}
		}
		// /clusters/{name}/pod-identity-associations
		if n >= 3 && seg[2] == "pod-identity-associations" {
			if n == 3 {
				switch method {
				case http.MethodPost:
					return "CreatePodIdentityAssociation"
				case http.MethodGet:
					return "ListPodIdentityAssociations"
				}
			}
			if n == 4 {
				switch method {
				case http.MethodGet:
					return "DescribePodIdentityAssociation"
				case http.MethodDelete:
					return "DeletePodIdentityAssociation"
				case http.MethodPost:
					return "UpdatePodIdentityAssociation"
				}
			}
		}
		// /clusters/{name}/updates
		if n >= 3 && seg[2] == "updates" {
			if n == 3 && method == http.MethodGet {
				return "ListUpdates"
			}
			if n == 4 && method == http.MethodGet {
				return "DescribeUpdate"
			}
		}
		// /clusters/{name}/insights
		if n >= 3 && seg[2] == "insights" {
			if n == 3 && method == http.MethodPost {
				return "ListInsights"
			}
			if n == 4 && method == http.MethodGet {
				return "DescribeInsight"
			}
		}

	// Addon versions
	case n >= 2 && seg[0] == "addons" && seg[1] == "supported-versions" && method == http.MethodGet:
		return "DescribeAddonVersions"
	case n >= 2 && seg[0] == "addons" && seg[1] == "configuration-schema" && method == http.MethodGet:
		return "DescribeAddonConfiguration"

	// Cluster versions
	case n >= 2 && seg[0] == "cluster-versions" && method == http.MethodGet:
		return "DescribeClusterVersions"

	// Register/deregister
	case n == 2 && seg[0] == "register-cluster" && method == http.MethodPost:
		return "RegisterCluster"
	case n == 2 && seg[0] == "deregister-cluster" && method == http.MethodDelete:
		return "DeregisterCluster"
	}
	return ""
}

func (p *Provider) ListResources(_ context.Context) ([]plugin.Resource, error) {
	clusters, err := p.store.ListClusters()
	if err != nil {
		return nil, err
	}
	res := make([]plugin.Resource, 0, len(clusters))
	for _, c := range clusters {
		res = append(res, plugin.Resource{Type: "eks-cluster", ID: c.ARN, Name: c.Name})
	}
	return res, nil
}

func (p *Provider) GetMetrics(_ context.Context) (*plugin.ServiceMetrics, error) {
	return &plugin.ServiceMetrics{}, nil
}

// ── Cluster operations ──────────────────────────────────────────────────────

func (p *Provider) createCluster(params map[string]any) (*plugin.Response, error) {
	name, _ := params["name"].(string)
	if name == "" {
		return shared.JSONError("ValidationException", "name is required", http.StatusBadRequest), nil
	}
	version, _ := params["version"].(string)
	if version == "" {
		version = "1.29"
	}
	roleARN, _ := params["roleArn"].(string)

	arn := shared.BuildARN("eks", "cluster", name)
	c, err := p.store.CreateCluster(name, arn, version, roleARN)
	if err != nil {
		if sqlite.IsUniqueConstraintError(err) {
			return shared.JSONError("ResourceInUseException", "cluster already exists", http.StatusConflict), nil
		}
		return nil, err
	}

	if rawTags, ok := params["tags"].(map[string]any); ok {
		tags := toStringMap(rawTags)
		p.store.tags.AddTags(c.ARN, tags)
	}

	return shared.JSONResponse(http.StatusOK, map[string]any{"cluster": clusterToMap(c)})
}

func (p *Provider) describeCluster(name string) (*plugin.Response, error) {
	if name == "" {
		return shared.JSONError("ValidationException", "name is required", http.StatusBadRequest), nil
	}
	c, err := p.store.GetCluster(name)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "cluster not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"cluster": clusterToMap(c)})
}

func (p *Provider) listClusters() (*plugin.Response, error) {
	clusters, err := p.store.ListClusters()
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, len(clusters))
	for _, c := range clusters {
		names = append(names, c.Name)
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"clusters": names, "nextToken": ""})
}

func (p *Provider) deleteCluster(name string) (*plugin.Response, error) {
	if name == "" {
		return shared.JSONError("ValidationException", "name is required", http.StatusBadRequest), nil
	}
	c, err := p.store.GetCluster(name)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "cluster not found", http.StatusNotFound), nil
	}
	p.store.tags.DeleteAllTags(c.ARN)
	if err := p.store.DeleteCluster(name); err != nil {
		return shared.JSONError("ResourceNotFoundException", "cluster not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"cluster": clusterToMap(c)})
}

func (p *Provider) updateClusterConfig(name string, params map[string]any) (*plugin.Response, error) {
	if name == "" {
		return shared.JSONError("ValidationException", "name is required", http.StatusBadRequest), nil
	}
	c, err := p.store.GetCluster(name)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "cluster not found", http.StatusNotFound), nil
	}
	config := c.Config
	if rc, ok := params["resourcesVpcConfig"].(map[string]any); ok {
		b, _ := json.Marshal(rc)
		config = string(b)
	}
	if err := p.store.UpdateClusterConfig(name, config); err != nil {
		return nil, err
	}
	c.Config = config
	return shared.JSONResponse(http.StatusOK, map[string]any{"update": map[string]any{"id": shared.GenerateUUID(), "status": "InProgress"}})
}

func (p *Provider) updateClusterVersion(name string, params map[string]any) (*plugin.Response, error) {
	if name == "" {
		return shared.JSONError("ValidationException", "name is required", http.StatusBadRequest), nil
	}
	_, err := p.store.GetCluster(name)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "cluster not found", http.StatusNotFound), nil
	}
	version, _ := params["version"].(string)
	if version == "" {
		return shared.JSONError("ValidationException", "version is required", http.StatusBadRequest), nil
	}
	if err := p.store.UpdateClusterVersion(name, version); err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"update": map[string]any{"id": shared.GenerateUUID(), "status": "InProgress"}})
}

// ── Nodegroup operations ────────────────────────────────────────────────────

func (p *Provider) createNodegroup(clusterName string, params map[string]any) (*plugin.Response, error) {
	if clusterName == "" {
		return shared.JSONError("ValidationException", "clusterName is required", http.StatusBadRequest), nil
	}
	ngName, _ := params["nodegroupName"].(string)
	if ngName == "" {
		return shared.JSONError("ValidationException", "nodegroupName is required", http.StatusBadRequest), nil
	}
	if _, err := p.store.GetCluster(clusterName); err != nil {
		return shared.JSONError("ResourceNotFoundException", "cluster not found", http.StatusNotFound), nil
	}

	nodeRole, _ := params["nodeRole"].(string)
	amiType, _ := params["amiType"].(string)
	if amiType == "" {
		amiType = "AL2_x86_64"
	}
	desiredSize := 2
	minSize := 1
	maxSize := 3
	if sc, ok := params["scalingConfig"].(map[string]any); ok {
		if v, ok := sc["desiredSize"].(float64); ok {
			desiredSize = int(v)
		}
		if v, ok := sc["minSize"].(float64); ok {
			minSize = int(v)
		}
		if v, ok := sc["maxSize"].(float64); ok {
			maxSize = int(v)
		}
	}
	instanceTypes := `["t3.medium"]`
	if it, ok := params["instanceTypes"].([]any); ok {
		b, _ := json.Marshal(it)
		instanceTypes = string(b)
	}
	subnets := "[]"
	if sn, ok := params["subnets"].([]any); ok {
		b, _ := json.Marshal(sn)
		subnets = string(b)
	}

	arn := shared.BuildARN("eks", "nodegroup", clusterName+"/"+ngName+"/"+shared.GenerateID("", 16))
	ng, err := p.store.CreateNodegroup(ngName, clusterName, arn, nodeRole, amiType, desiredSize, minSize, maxSize, instanceTypes, subnets)
	if err != nil {
		if sqlite.IsUniqueConstraintError(err) {
			return shared.JSONError("ResourceInUseException", "nodegroup already exists", http.StatusConflict), nil
		}
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"nodegroup": nodegroupToMap(ng)})
}

func (p *Provider) describeNodegroup(clusterName, ngName string) (*plugin.Response, error) {
	ng, err := p.store.GetNodegroup(clusterName, ngName)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "nodegroup not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"nodegroup": nodegroupToMap(ng)})
}

func (p *Provider) listNodegroups(clusterName string) (*plugin.Response, error) {
	ngs, err := p.store.ListNodegroups(clusterName)
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, len(ngs))
	for _, ng := range ngs {
		names = append(names, ng.Name)
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"nodegroups": names, "nextToken": ""})
}

func (p *Provider) deleteNodegroup(clusterName, ngName string) (*plugin.Response, error) {
	ng, err := p.store.GetNodegroup(clusterName, ngName)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "nodegroup not found", http.StatusNotFound), nil
	}
	if err := p.store.DeleteNodegroup(clusterName, ngName); err != nil {
		return shared.JSONError("ResourceNotFoundException", "nodegroup not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"nodegroup": nodegroupToMap(ng)})
}

func (p *Provider) updateNodegroupConfig(clusterName, ngName string, params map[string]any) (*plugin.Response, error) {
	ng, err := p.store.GetNodegroup(clusterName, ngName)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "nodegroup not found", http.StatusNotFound), nil
	}
	desiredSize := ng.DesiredSize
	minSize := ng.MinSize
	maxSize := ng.MaxSize
	if sc, ok := params["scalingConfig"].(map[string]any); ok {
		if v, ok := sc["desiredSize"].(float64); ok {
			desiredSize = int(v)
		}
		if v, ok := sc["minSize"].(float64); ok {
			minSize = int(v)
		}
		if v, ok := sc["maxSize"].(float64); ok {
			maxSize = int(v)
		}
	}
	if err := p.store.UpdateNodegroupConfig(clusterName, ngName, desiredSize, minSize, maxSize); err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"update": map[string]any{"id": shared.GenerateUUID(), "status": "InProgress"}})
}

func (p *Provider) updateNodegroupVersion(clusterName, ngName string, params map[string]any) (*plugin.Response, error) {
	_, err := p.store.GetNodegroup(clusterName, ngName)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "nodegroup not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"update": map[string]any{"id": shared.GenerateUUID(), "status": "InProgress"}})
}

// ── FargateProfile operations ───────────────────────────────────────────────

func (p *Provider) createFargateProfile(clusterName string, params map[string]any) (*plugin.Response, error) {
	if clusterName == "" {
		return shared.JSONError("ValidationException", "clusterName is required", http.StatusBadRequest), nil
	}
	fpName, _ := params["fargateProfileName"].(string)
	if fpName == "" {
		return shared.JSONError("ValidationException", "fargateProfileName is required", http.StatusBadRequest), nil
	}
	if _, err := p.store.GetCluster(clusterName); err != nil {
		return shared.JSONError("ResourceNotFoundException", "cluster not found", http.StatusNotFound), nil
	}
	podExecRole, _ := params["podExecutionRoleArn"].(string)
	selectors := "[]"
	if sl, ok := params["selectors"].([]any); ok {
		b, _ := json.Marshal(sl)
		selectors = string(b)
	}
	subnets := "[]"
	if sn, ok := params["subnets"].([]any); ok {
		b, _ := json.Marshal(sn)
		subnets = string(b)
	}
	arn := shared.BuildARN("eks", "fargateprofile", clusterName+"/"+fpName+"/"+shared.GenerateID("", 16))
	fp, err := p.store.CreateFargateProfile(fpName, clusterName, arn, podExecRole, selectors, subnets)
	if err != nil {
		if sqlite.IsUniqueConstraintError(err) {
			return shared.JSONError("ResourceInUseException", "fargate profile already exists", http.StatusConflict), nil
		}
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"fargateProfile": fargateProfileToMap(fp)})
}

func (p *Provider) describeFargateProfile(clusterName, fpName string) (*plugin.Response, error) {
	fp, err := p.store.GetFargateProfile(clusterName, fpName)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "fargate profile not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"fargateProfile": fargateProfileToMap(fp)})
}

func (p *Provider) listFargateProfiles(clusterName string) (*plugin.Response, error) {
	fps, err := p.store.ListFargateProfiles(clusterName)
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, len(fps))
	for _, fp := range fps {
		names = append(names, fp.Name)
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"fargateProfileNames": names, "nextToken": ""})
}

func (p *Provider) deleteFargateProfile(clusterName, fpName string) (*plugin.Response, error) {
	fp, err := p.store.GetFargateProfile(clusterName, fpName)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "fargate profile not found", http.StatusNotFound), nil
	}
	if err := p.store.DeleteFargateProfile(clusterName, fpName); err != nil {
		return shared.JSONError("ResourceNotFoundException", "fargate profile not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"fargateProfile": fargateProfileToMap(fp)})
}

// ── Addon operations ────────────────────────────────────────────────────────

func (p *Provider) createAddon(clusterName string, params map[string]any) (*plugin.Response, error) {
	if clusterName == "" {
		return shared.JSONError("ValidationException", "clusterName is required", http.StatusBadRequest), nil
	}
	addonName, _ := params["addonName"].(string)
	if addonName == "" {
		return shared.JSONError("ValidationException", "addonName is required", http.StatusBadRequest), nil
	}
	if _, err := p.store.GetCluster(clusterName); err != nil {
		return shared.JSONError("ResourceNotFoundException", "cluster not found", http.StatusNotFound), nil
	}
	addonVersion, _ := params["addonVersion"].(string)
	serviceRole, _ := params["serviceAccountRoleArn"].(string)
	configJSON := "{}"
	if cfg, ok := params["configurationValues"].(map[string]any); ok {
		b, _ := json.Marshal(cfg)
		configJSON = string(b)
	} else if cfg, ok := params["configurationValues"].(string); ok {
		configJSON = cfg
	}
	arn := shared.BuildARN("eks", "addon", clusterName+"/"+addonName+"/"+shared.GenerateID("", 16))
	a, err := p.store.CreateAddon(addonName, clusterName, arn, addonVersion, serviceRole, configJSON)
	if err != nil {
		if sqlite.IsUniqueConstraintError(err) {
			return shared.JSONError("ResourceInUseException", "addon already exists", http.StatusConflict), nil
		}
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"addon": addonToMap(a)})
}

func (p *Provider) describeAddon(clusterName, addonName string) (*plugin.Response, error) {
	a, err := p.store.GetAddon(clusterName, addonName)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "addon not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"addon": addonToMap(a)})
}

func (p *Provider) listAddons(clusterName string) (*plugin.Response, error) {
	addons, err := p.store.ListAddons(clusterName)
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, len(addons))
	for _, a := range addons {
		names = append(names, a.Name)
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"addons": names, "nextToken": ""})
}

func (p *Provider) deleteAddon(clusterName, addonName string) (*plugin.Response, error) {
	a, err := p.store.GetAddon(clusterName, addonName)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "addon not found", http.StatusNotFound), nil
	}
	if err := p.store.DeleteAddon(clusterName, addonName); err != nil {
		return shared.JSONError("ResourceNotFoundException", "addon not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"addon": addonToMap(a)})
}

func (p *Provider) updateAddon(clusterName, addonName string, params map[string]any) (*plugin.Response, error) {
	a, err := p.store.GetAddon(clusterName, addonName)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "addon not found", http.StatusNotFound), nil
	}
	addonVersion := a.AddonVersion
	if v, ok := params["addonVersion"].(string); ok && v != "" {
		addonVersion = v
	}
	serviceRole := a.ServiceRole
	if v, ok := params["serviceAccountRoleArn"].(string); ok {
		serviceRole = v
	}
	configJSON := a.Config
	if cfg, ok := params["configurationValues"].(string); ok {
		configJSON = cfg
	}
	if err := p.store.UpdateAddon(clusterName, addonName, addonVersion, serviceRole, configJSON); err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"update": map[string]any{"id": shared.GenerateUUID(), "status": "InProgress"}})
}

// ── Tag operations ──────────────────────────────────────────────────────────

func (p *Provider) tagResource(resourceARN string, params map[string]any) (*plugin.Response, error) {
	if resourceARN == "" {
		return shared.JSONError("ValidationException", "resourceArn is required", http.StatusBadRequest), nil
	}
	if rawTags, ok := params["tags"].(map[string]any); ok {
		if err := p.store.tags.AddTags(resourceARN, toStringMap(rawTags)); err != nil {
			return nil, err
		}
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) untagResource(resourceARN string, tagKeys []string) (*plugin.Response, error) {
	if resourceARN == "" {
		return shared.JSONError("ValidationException", "resourceArn is required", http.StatusBadRequest), nil
	}
	if err := p.store.tags.RemoveTags(resourceARN, tagKeys); err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) listTagsForResource(resourceARN string) (*plugin.Response, error) {
	if resourceARN == "" {
		return shared.JSONError("ValidationException", "resourceArn is required", http.StatusBadRequest), nil
	}
	tags, err := p.store.tags.ListTags(resourceARN)
	if err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"tags": tags})
}

// ── AccessEntry operations ──────────────────────────────────────────────────

func (p *Provider) createAccessEntry(clusterName string, params map[string]any) (*plugin.Response, error) {
	if clusterName == "" {
		return shared.JSONError("ValidationException", "clusterName is required", http.StatusBadRequest), nil
	}
	principalARN, _ := params["principalArn"].(string)
	if principalARN == "" {
		return shared.JSONError("ValidationException", "principalArn is required", http.StatusBadRequest), nil
	}
	if _, err := p.store.GetCluster(clusterName); err != nil {
		return shared.JSONError("ResourceNotFoundException", "cluster not found", http.StatusNotFound), nil
	}
	entryType, _ := params["type"].(string)
	if entryType == "" {
		entryType = "STANDARD"
	}
	kubernetesGroups := "[]"
	if kg, ok := params["kubernetesGroups"].([]any); ok {
		b, _ := json.Marshal(kg)
		kubernetesGroups = string(b)
	}
	username, _ := params["username"].(string)
	arn := shared.BuildARN("eks", "access-entry", clusterName+"/"+shared.GenerateID("", 16))
	ae, err := p.store.CreateAccessEntry(principalARN, clusterName, arn, entryType, kubernetesGroups, username)
	if err != nil {
		if sqlite.IsUniqueConstraintError(err) {
			return shared.JSONError("ResourceInUseException", "access entry already exists", http.StatusConflict), nil
		}
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"accessEntry": accessEntryToMap(ae)})
}

func (p *Provider) describeAccessEntry(clusterName, principalARN string) (*plugin.Response, error) {
	ae, err := p.store.GetAccessEntry(clusterName, principalARN)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "access entry not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"accessEntry": accessEntryToMap(ae)})
}

func (p *Provider) listAccessEntries(clusterName string) (*plugin.Response, error) {
	entries, err := p.store.ListAccessEntries(clusterName)
	if err != nil {
		return nil, err
	}
	arns := make([]string, 0, len(entries))
	for _, ae := range entries {
		arns = append(arns, ae.ARN)
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"accessEntries": arns, "nextToken": ""})
}

func (p *Provider) deleteAccessEntry(clusterName, principalARN string) (*plugin.Response, error) {
	if err := p.store.DeleteAccessEntry(clusterName, principalARN); err != nil {
		return shared.JSONError("ResourceNotFoundException", "access entry not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) updateAccessEntry(clusterName, principalARN string, params map[string]any) (*plugin.Response, error) {
	ae, err := p.store.GetAccessEntry(clusterName, principalARN)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "access entry not found", http.StatusNotFound), nil
	}
	kubernetesGroups := ae.KubernetesGroups
	if kg, ok := params["kubernetesGroups"].([]any); ok {
		b, _ := json.Marshal(kg)
		kubernetesGroups = string(b)
	}
	username := ae.Username
	if u, ok := params["username"].(string); ok {
		username = u
	}
	if err := p.store.UpdateAccessEntry(clusterName, principalARN, kubernetesGroups, username); err != nil {
		return nil, err
	}
	ae.KubernetesGroups = kubernetesGroups
	ae.Username = username
	return shared.JSONResponse(http.StatusOK, map[string]any{"accessEntry": accessEntryToMap(ae)})
}

// ── PodIdentityAssociation operations ──────────────────────────────────────

func (p *Provider) createPodIdentityAssociation(clusterName string, params map[string]any) (*plugin.Response, error) {
	if clusterName == "" {
		return shared.JSONError("ValidationException", "clusterName is required", http.StatusBadRequest), nil
	}
	if _, err := p.store.GetCluster(clusterName); err != nil {
		return shared.JSONError("ResourceNotFoundException", "cluster not found", http.StatusNotFound), nil
	}
	namespace, _ := params["namespace"].(string)
	serviceAccount, _ := params["serviceAccount"].(string)
	roleARN, _ := params["roleArn"].(string)
	assocID := shared.GenerateUUID()
	arn := shared.BuildARN("eks", "podidentityassociation", clusterName+"/"+assocID)
	pa, err := p.store.CreatePodIdentityAssociation(assocID, clusterName, arn, namespace, serviceAccount, roleARN)
	if err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"association": podIdentityToMap(pa)})
}

func (p *Provider) describePodIdentityAssociation(clusterName, assocID string) (*plugin.Response, error) {
	pa, err := p.store.GetPodIdentityAssociation(clusterName, assocID)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "association not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"association": podIdentityToMap(pa)})
}

func (p *Provider) listPodIdentityAssociations(clusterName string) (*plugin.Response, error) {
	assocs, err := p.store.ListPodIdentityAssociations(clusterName)
	if err != nil {
		return nil, err
	}
	list := make([]map[string]any, 0, len(assocs))
	for _, pa := range assocs {
		list = append(list, map[string]any{
			"associationId":  pa.AssociationID,
			"associationArn": pa.ARN,
			"clusterName":    pa.ClusterName,
		})
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"associations": list, "nextToken": ""})
}

func (p *Provider) deletePodIdentityAssociation(clusterName, assocID string) (*plugin.Response, error) {
	pa, err := p.store.GetPodIdentityAssociation(clusterName, assocID)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "association not found", http.StatusNotFound), nil
	}
	if err := p.store.DeletePodIdentityAssociation(clusterName, assocID); err != nil {
		return shared.JSONError("ResourceNotFoundException", "association not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"association": podIdentityToMap(pa)})
}

func (p *Provider) updatePodIdentityAssociation(clusterName, assocID string, params map[string]any) (*plugin.Response, error) {
	pa, err := p.store.GetPodIdentityAssociation(clusterName, assocID)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "association not found", http.StatusNotFound), nil
	}
	roleARN := pa.RoleARN
	if r, ok := params["roleArn"].(string); ok && r != "" {
		roleARN = r
	}
	if err := p.store.UpdatePodIdentityAssociation(clusterName, assocID, roleARN); err != nil {
		return nil, err
	}
	pa.RoleARN = roleARN
	return shared.JSONResponse(http.StatusOK, map[string]any{"association": podIdentityToMap(pa)})
}

// ── Path helpers ────────────────────────────────────────────────────────────

// extractSegment returns the segment after the given anchor in the URL path.
// offset=1 means the segment immediately after anchor, offset=2 means two after, etc.
func extractSegment(path, anchor string, offset int) string {
	parts := strings.Split(strings.Trim(path, "/"), "/")
	for i, p := range parts {
		if p == anchor && i+offset < len(parts) {
			return parts[i+offset]
		}
	}
	return ""
}

// extractTwoSegments returns the segments after firstAnchor and secondAnchor.
func extractTwoSegments(path, firstAnchor, secondAnchor string) (string, string) {
	parts := strings.Split(strings.Trim(path, "/"), "/")
	first := ""
	second := ""
	for i, p := range parts {
		if p == firstAnchor && i+1 < len(parts) {
			first = parts[i+1]
		}
		if p == secondAnchor && i+1 < len(parts) {
			second = parts[i+1]
		}
	}
	return first, second
}

// extractTagARN extracts the ARN from /tags/{resourceArn}
func extractTagARN(path string) string {
	idx := strings.Index(path, "/tags/")
	if idx < 0 {
		return ""
	}
	return path[idx+6:]
}

// ── Model helpers ───────────────────────────────────────────────────────────

func clusterToMap(c *Cluster) map[string]any {
	return map[string]any{
		"name":            c.Name,
		"arn":             c.ARN,
		"status":          c.Status,
		"version":         c.Version,
		"roleArn":         c.RoleARN,
		"endpoint":        c.Endpoint,
		"platformVersion": c.PlatformVersion,
		"createdAt":       c.CreatedAt.Unix(),
	}
}

func nodegroupToMap(ng *Nodegroup) map[string]any {
	return map[string]any{
		"nodegroupName": ng.Name,
		"clusterName":   ng.ClusterName,
		"nodegroupArn":  ng.ARN,
		"status":        ng.Status,
		"amiType":       ng.AMIType,
		"nodeRole":      ng.NodeRole,
		"scalingConfig": map[string]any{
			"desiredSize": ng.DesiredSize,
			"minSize":     ng.MinSize,
			"maxSize":     ng.MaxSize,
		},
		"createdAt": ng.CreatedAt.Unix(),
	}
}

func fargateProfileToMap(fp *FargateProfile) map[string]any {
	return map[string]any{
		"fargateProfileName":  fp.Name,
		"clusterName":         fp.ClusterName,
		"fargateProfileArn":   fp.ARN,
		"status":              fp.Status,
		"podExecutionRoleArn": fp.PodExecutionRole,
		"createdAt":           fp.CreatedAt.Unix(),
	}
}

func addonToMap(a *Addon) map[string]any {
	return map[string]any{
		"addonName":    a.Name,
		"clusterName":  a.ClusterName,
		"addonArn":     a.ARN,
		"status":       a.Status,
		"addonVersion": a.AddonVersion,
		"createdAt":    a.CreatedAt.Unix(),
	}
}

func accessEntryToMap(ae *AccessEntry) map[string]any {
	return map[string]any{
		"principalArn":   ae.PrincipalARN,
		"clusterName":    ae.ClusterName,
		"accessEntryArn": ae.ARN,
		"type":           ae.EntryType,
		"username":       ae.Username,
		"createdAt":      ae.CreatedAt.Unix(),
	}
}

func podIdentityToMap(pa *PodIdentityAssociation) map[string]any {
	return map[string]any{
		"associationId":  pa.AssociationID,
		"associationArn": pa.ARN,
		"clusterName":    pa.ClusterName,
		"namespace":      pa.Namespace,
		"serviceAccount": pa.ServiceAccount,
		"roleArn":        pa.RoleARN,
		"createdAt":      pa.CreatedAt.Unix(),
	}
}

func toStringMap(raw map[string]any) map[string]string {
	out := make(map[string]string, len(raw))
	for k, v := range raw {
		if s, ok := v.(string); ok {
			out[k] = s
		}
	}
	return out
}
