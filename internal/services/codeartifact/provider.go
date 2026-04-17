// SPDX-License-Identifier: Apache-2.0

// internal/services/codeartifact/provider.go
package codeartifact

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"strings"
	"time"

	"github.com/skyoo2003/devcloud/internal/plugin"
	"github.com/skyoo2003/devcloud/internal/shared"
)

// Provider implements the CodeArtifact service.
type Provider struct {
	store *Store
}

func (p *Provider) ServiceID() string             { return "codeartifact" }
func (p *Provider) ServiceName() string           { return "CodeArtifactControlPlaneService" }
func (p *Provider) Protocol() plugin.ProtocolType { return plugin.ProtocolRESTJSON }

func (p *Provider) Init(cfg plugin.PluginConfig) error {
	dataDir := cfg.DataDir
	if dataDir == "" {
		dataDir = "."
	}
	var err error
	p.store, err = NewStore(filepath.Join(dataDir, "codeartifact"))
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

	if op == "" {
		op = resolveOp(req.Method, req.URL.Path)
	}

	switch op {
	// Domain CRUD
	case "CreateDomain":
		return p.createDomain(req, params)
	case "DescribeDomain":
		return p.describeDomain(req)
	case "ListDomains":
		return p.listDomains()
	case "DeleteDomain":
		return p.deleteDomain(req)

	// Repository CRUD
	case "CreateRepository":
		return p.createRepository(req, params)
	case "DescribeRepository":
		return p.describeRepository(req)
	case "ListRepositories":
		return p.listRepositories()
	case "ListRepositoriesInDomain":
		return p.listRepositoriesInDomain(req)
	case "UpdateRepository":
		return p.updateRepository(req, params)
	case "DeleteRepository":
		return p.deleteRepository(req)

	// Package ops
	case "DescribePackage":
		return p.describePackage(req)
	case "ListPackages":
		return p.listPackages(req)
	case "DeletePackage":
		return p.deletePackage(req)
	case "PutPackageOriginConfiguration":
		return p.putPackageOriginConfiguration(req, params)

	// Package version ops (stub)
	case "ListPackageVersions":
		return shared.JSONResponse(http.StatusOK, map[string]any{"versions": []any{}, "format": "", "namespace": "", "package": ""})
	case "ListPackageVersionAssets":
		return shared.JSONResponse(http.StatusOK, map[string]any{"assets": []any{}, "format": "", "namespace": "", "package": "", "version": "", "versionRevision": ""})
	case "ListPackageVersionDependencies":
		return shared.JSONResponse(http.StatusOK, map[string]any{"dependencies": []any{}, "format": "", "namespace": "", "package": "", "version": "", "versionRevision": ""})
	case "DescribePackageVersion":
		return shared.JSONResponse(http.StatusOK, map[string]any{"packageVersion": map[string]any{}})
	case "DeletePackageVersions":
		return shared.JSONResponse(http.StatusOK, map[string]any{"failedVersions": map[string]any{}, "successfulVersions": map[string]any{}})
	case "CopyPackageVersions":
		return shared.JSONResponse(http.StatusOK, map[string]any{"failedVersions": map[string]any{}, "successfulVersions": map[string]any{}})
	case "DisposePackageVersions":
		return shared.JSONResponse(http.StatusOK, map[string]any{"failedVersions": map[string]any{}, "successfulVersions": map[string]any{}})
	case "UpdatePackageVersionsStatus":
		return shared.JSONResponse(http.StatusOK, map[string]any{"failedVersions": map[string]any{}, "successfulVersions": map[string]any{}})
	case "GetPackageVersionReadme":
		return shared.JSONResponse(http.StatusOK, map[string]any{"readme": "", "format": "", "namespace": "", "package": "", "version": "", "versionRevision": ""})
	case "GetPackageVersionAsset":
		return shared.JSONResponse(http.StatusOK, map[string]any{"assetName": "", "packageVersion": "", "packageVersionRevision": ""})
	case "PublishPackageVersion":
		return shared.JSONResponse(http.StatusOK, map[string]any{"format": "", "namespace": "", "package": "", "status": "Published", "version": "", "versionRevision": ""})

	// PackageGroup CRUD
	case "CreatePackageGroup":
		return p.createPackageGroup(req, params)
	case "DescribePackageGroup":
		return p.describePackageGroup(req)
	case "ListPackageGroups":
		return p.listPackageGroups(req)
	case "ListSubPackageGroups":
		return p.listSubPackageGroups(req)
	case "UpdatePackageGroup":
		return p.updatePackageGroup(req, params)
	case "UpdatePackageGroupOriginConfiguration":
		return p.updatePackageGroupOriginConfiguration(req, params)
	case "DeletePackageGroup":
		return p.deletePackageGroup(req)
	case "GetAssociatedPackageGroup":
		return shared.JSONResponse(http.StatusOK, map[string]any{"packageGroup": nil, "associationType": "STRONG"})
	case "ListAllowedRepositoriesForGroup":
		return shared.JSONResponse(http.StatusOK, map[string]any{"allowedRepositories": []any{}})
	case "ListAssociatedPackages":
		return shared.JSONResponse(http.StatusOK, map[string]any{"packages": []any{}})

	// Auth token
	case "GetAuthorizationToken":
		return p.getAuthorizationToken(req)

	// Repository endpoint
	case "GetRepositoryEndpoint":
		return p.getRepositoryEndpoint(req)

	// Domain permissions policy
	case "GetDomainPermissionsPolicy":
		return p.getDomainPermissionsPolicy(req)
	case "PutDomainPermissionsPolicy":
		return p.putDomainPermissionsPolicy(req, params)
	case "DeleteDomainPermissionsPolicy":
		return p.deleteDomainPermissionsPolicy(req)

	// Repository permissions policy
	case "GetRepositoryPermissionsPolicy":
		return p.getRepositoryPermissionsPolicy(req)
	case "PutRepositoryPermissionsPolicy":
		return p.putRepositoryPermissionsPolicy(req, params)
	case "DeleteRepositoryPermissionsPolicy":
		return p.deleteRepositoryPermissionsPolicy(req)

	// External connections
	case "AssociateExternalConnection":
		return p.associateExternalConnection(req, params)
	case "DisassociateExternalConnection":
		return p.disassociateExternalConnection(req)

	// Tags
	case "TagResource":
		return p.tagResource(req, params)
	case "UntagResource":
		return p.untagResource(req)
	case "ListTagsForResource":
		return p.listTagsForResource(req)

	default:
		return shared.JSONError("InvalidAction", fmt.Sprintf("unknown action: %s", op), http.StatusBadRequest), nil
	}
}

func resolveOp(method, path string) string {
	p := strings.TrimPrefix(path, "/v1")
	p = strings.Trim(p, "/")

	switch {
	// Tags
	case p == "tag":
		return "TagResource"
	case p == "untag":
		return "UntagResource"
	case strings.HasPrefix(p, "tags"):
		return "ListTagsForResource"

	// Domain
	case p == "domain":
		switch method {
		case http.MethodPost:
			return "CreateDomain"
		case http.MethodGet:
			return "DescribeDomain"
		case http.MethodDelete:
			return "DeleteDomain"
		}
	case p == "domains":
		return "ListDomains"

	// Repository
	case p == "repository":
		switch method {
		case http.MethodPost:
			return "CreateRepository"
		case http.MethodGet:
			return "DescribeRepository"
		case http.MethodPut:
			return "UpdateRepository"
		case http.MethodDelete:
			return "DeleteRepository"
		}
	case p == "repositories":
		return "ListRepositories"
	case p == "repositories/in-domain":
		return "ListRepositoriesInDomain"

	// Package
	case p == "package":
		switch method {
		case http.MethodGet:
			return "DescribePackage"
		case http.MethodDelete:
			return "DeletePackage"
		}
	case p == "packages":
		return "ListPackages"
	case p == "package/origin/configuration":
		return "PutPackageOriginConfiguration"
	case p == "package/versions":
		return "ListPackageVersions"
	case p == "package/version/assets":
		return "ListPackageVersionAssets"
	case p == "package/version/dependencies":
		return "ListPackageVersionDependencies"
	case p == "package/version":
		switch method {
		case http.MethodGet:
			return "DescribePackageVersion"
		case http.MethodPost:
			return "DeletePackageVersions"
		}
	case p == "package/versions/copy":
		return "CopyPackageVersions"
	case p == "package/versions/dispose":
		return "DisposePackageVersions"
	case p == "package/versions/update_status":
		return "UpdatePackageVersionsStatus"
	case p == "package/version/readme":
		return "GetPackageVersionReadme"
	case p == "package/version/asset":
		return "GetPackageVersionAsset"
	case p == "package/version/publish":
		return "PublishPackageVersion"

	// PackageGroup
	case p == "package-group":
		switch method {
		case http.MethodPost:
			return "CreatePackageGroup"
		case http.MethodGet:
			return "DescribePackageGroup"
		case http.MethodPut:
			return "UpdatePackageGroup"
		case http.MethodDelete:
			return "DeletePackageGroup"
		}
	case p == "package-groups":
		return "ListPackageGroups"
	case p == "package-group/sub-groups":
		return "ListSubPackageGroups"
	case p == "package-group/origin-configuration":
		return "UpdatePackageGroupOriginConfiguration"
	case p == "package-group/associated-package":
		return "GetAssociatedPackageGroup"
	case p == "package-group/allowed-repositories":
		return "ListAllowedRepositoriesForGroup"
	case p == "package-group/associated-packages":
		return "ListAssociatedPackages"

	// Auth token
	case p == "authorization-token":
		return "GetAuthorizationToken"

	// Repository endpoint
	case p == "repository/endpoint":
		return "GetRepositoryEndpoint"

	// Domain permissions policy
	case p == "domain/permissions/policy":
		switch method {
		case http.MethodGet:
			return "GetDomainPermissionsPolicy"
		case http.MethodPut:
			return "PutDomainPermissionsPolicy"
		case http.MethodDelete:
			return "DeleteDomainPermissionsPolicy"
		}

	// Repository permissions policy
	case p == "repository/permissions/policy":
		switch method {
		case http.MethodGet:
			return "GetRepositoryPermissionsPolicy"
		case http.MethodPut:
			return "PutRepositoryPermissionsPolicy"
		case http.MethodDelete:
			return "DeleteRepositoryPermissionsPolicy"
		}

	// External connections
	case p == "repository/external-connection":
		switch method {
		case http.MethodPost:
			return "AssociateExternalConnection"
		case http.MethodDelete:
			return "DisassociateExternalConnection"
		}
	}
	return ""
}

func (p *Provider) ListResources(_ context.Context) ([]plugin.Resource, error) {
	domains, err := p.store.ListDomains()
	if err != nil {
		return nil, err
	}
	res := make([]plugin.Resource, 0, len(domains))
	for _, d := range domains {
		res = append(res, plugin.Resource{Type: "codeartifact-domain", ID: d.Name, Name: d.Name})
	}
	return res, nil
}

func (p *Provider) GetMetrics(_ context.Context) (*plugin.ServiceMetrics, error) {
	return &plugin.ServiceMetrics{}, nil
}

// --- Domain ---

func (p *Provider) createDomain(req *http.Request, params map[string]any) (*plugin.Response, error) {
	name := req.URL.Query().Get("domain")
	if name == "" {
		name, _ = params["domain"].(string)
	}
	if name == "" {
		return shared.JSONError("ValidationException", "domain is required", http.StatusBadRequest), nil
	}
	encryptionKey, _ := params["encryptionKey"].(string)
	arn := shared.BuildARN("codeartifact", "domain", name)

	d := &Domain{
		Name:          name,
		ARN:           arn,
		Owner:         shared.DefaultAccountID,
		Status:        "Active",
		EncryptionKey: encryptionKey,
		RepoCount:     0,
	}
	if err := p.store.CreateDomain(d); err != nil {
		if isUniqueErr(err) {
			return shared.JSONError("ConflictException", "domain already exists", http.StatusConflict), nil
		}
		return nil, err
	}

	if rawTags, ok := params["tags"].([]any); ok {
		tags := tagsListToMap(rawTags)
		p.store.tags.AddTags(arn, tags) //nolint:errcheck
	}

	stored, err := p.store.GetDomain(name)
	if err != nil {
		return nil, err
	}
	tags, _ := p.store.tags.ListTags(arn)
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"domain": domainToMap(stored, tags),
	})
}

func (p *Provider) describeDomain(req *http.Request) (*plugin.Response, error) {
	name := req.URL.Query().Get("domain")
	if name == "" {
		return shared.JSONError("ValidationException", "domain is required", http.StatusBadRequest), nil
	}
	d, err := p.store.GetDomain(name)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "domain not found", http.StatusNotFound), nil
	}
	tags, _ := p.store.tags.ListTags(d.ARN)
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"domain": domainToMap(d, tags),
	})
}

func (p *Provider) listDomains() (*plugin.Response, error) {
	domains, err := p.store.ListDomains()
	if err != nil {
		return nil, err
	}
	summaries := make([]map[string]any, 0, len(domains))
	for _, d := range domains {
		summaries = append(summaries, domainSummaryToMap(&d))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"domains": summaries,
	})
}

func (p *Provider) deleteDomain(req *http.Request) (*plugin.Response, error) {
	name := req.URL.Query().Get("domain")
	if name == "" {
		return shared.JSONError("ValidationException", "domain is required", http.StatusBadRequest), nil
	}
	d, err := p.store.GetDomain(name)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "domain not found", http.StatusNotFound), nil
	}
	p.store.tags.DeleteAllTags(d.ARN) //nolint:errcheck
	if err := p.store.DeleteDomain(name); err != nil {
		return shared.JSONError("ResourceNotFoundException", "domain not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"domain": domainToMap(d, nil),
	})
}

// --- Repository ---

func (p *Provider) createRepository(req *http.Request, params map[string]any) (*plugin.Response, error) {
	domainName := req.URL.Query().Get("domain")
	if domainName == "" {
		domainName, _ = params["domain"].(string)
	}
	repoName, _ := params["repository"].(string)
	if domainName == "" || repoName == "" {
		return shared.JSONError("ValidationException", "domain and repository are required", http.StatusBadRequest), nil
	}

	if _, err := p.store.GetDomain(domainName); err != nil {
		return shared.JSONError("ResourceNotFoundException", "domain not found", http.StatusNotFound), nil
	}

	description, _ := params["description"].(string)
	upstreamsJSON := "[]"
	if v, ok := params["upstreams"]; ok {
		if b, err := json.Marshal(v); err == nil {
			upstreamsJSON = string(b)
		}
	}

	arn := shared.BuildARN("codeartifact", "repository", domainName+"/"+repoName)
	r := &Repository{
		Name:          repoName,
		DomainName:    domainName,
		ARN:           arn,
		Description:   description,
		Upstreams:     upstreamsJSON,
		ExternalConns: "[]",
	}
	if err := p.store.CreateRepository(r); err != nil {
		if isUniqueErr(err) {
			return shared.JSONError("ConflictException", "repository already exists", http.StatusConflict), nil
		}
		return nil, err
	}
	p.store.IncrementDomainRepoCount(domainName, 1) //nolint:errcheck

	if rawTags, ok := params["tags"].([]any); ok {
		tags := tagsListToMap(rawTags)
		p.store.tags.AddTags(arn, tags) //nolint:errcheck
	}

	stored, err := p.store.GetRepository(repoName, domainName)
	if err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"repository": repositoryToMap(stored),
	})
}

func (p *Provider) describeRepository(req *http.Request) (*plugin.Response, error) {
	domainName := req.URL.Query().Get("domain")
	repoName := req.URL.Query().Get("repository")
	if domainName == "" || repoName == "" {
		return shared.JSONError("ValidationException", "domain and repository are required", http.StatusBadRequest), nil
	}
	r, err := p.store.GetRepository(repoName, domainName)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "repository not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"repository": repositoryToMap(r),
	})
}

func (p *Provider) listRepositories() (*plugin.Response, error) {
	repos, err := p.store.ListRepositories()
	if err != nil {
		return nil, err
	}
	summaries := make([]map[string]any, 0, len(repos))
	for _, r := range repos {
		summaries = append(summaries, repositorySummaryToMap(&r))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"repositories": summaries,
	})
}

func (p *Provider) listRepositoriesInDomain(req *http.Request) (*plugin.Response, error) {
	domainName := req.URL.Query().Get("domain")
	if domainName == "" {
		return shared.JSONError("ValidationException", "domain is required", http.StatusBadRequest), nil
	}
	repos, err := p.store.ListRepositoriesInDomain(domainName)
	if err != nil {
		return nil, err
	}
	summaries := make([]map[string]any, 0, len(repos))
	for _, r := range repos {
		summaries = append(summaries, repositorySummaryToMap(&r))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"repositories": summaries,
	})
}

func (p *Provider) updateRepository(req *http.Request, params map[string]any) (*plugin.Response, error) {
	domainName := req.URL.Query().Get("domain")
	if domainName == "" {
		domainName, _ = params["domain"].(string)
	}
	repoName := req.URL.Query().Get("repository")
	if repoName == "" {
		repoName, _ = params["repository"].(string)
	}
	if domainName == "" || repoName == "" {
		return shared.JSONError("ValidationException", "domain and repository are required", http.StatusBadRequest), nil
	}

	existing, err := p.store.GetRepository(repoName, domainName)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "repository not found", http.StatusNotFound), nil
	}

	description := existing.Description
	if v, ok := params["description"].(string); ok {
		description = v
	}
	upstreams := existing.Upstreams
	if v, ok := params["upstreams"]; ok {
		if b, e := json.Marshal(v); e == nil {
			upstreams = string(b)
		}
	}

	if err := p.store.UpdateRepository(repoName, domainName, description, upstreams); err != nil {
		return shared.JSONError("ResourceNotFoundException", "repository not found", http.StatusNotFound), nil
	}
	updated, err := p.store.GetRepository(repoName, domainName)
	if err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"repository": repositoryToMap(updated),
	})
}

func (p *Provider) deleteRepository(req *http.Request) (*plugin.Response, error) {
	domainName := req.URL.Query().Get("domain")
	repoName := req.URL.Query().Get("repository")
	if domainName == "" || repoName == "" {
		return shared.JSONError("ValidationException", "domain and repository are required", http.StatusBadRequest), nil
	}
	r, err := p.store.GetRepository(repoName, domainName)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "repository not found", http.StatusNotFound), nil
	}
	p.store.tags.DeleteAllTags(r.ARN) //nolint:errcheck
	if err := p.store.DeleteRepository(repoName, domainName); err != nil {
		return shared.JSONError("ResourceNotFoundException", "repository not found", http.StatusNotFound), nil
	}
	p.store.IncrementDomainRepoCount(domainName, -1) //nolint:errcheck
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"repository": repositoryToMap(r),
	})
}

// --- Package ---

func (p *Provider) describePackage(req *http.Request) (*plugin.Response, error) {
	q := req.URL.Query()
	domainName := q.Get("domain")
	repoName := q.Get("repository")
	format := q.Get("format")
	namespace := q.Get("namespace")
	pkgName := q.Get("package")
	if domainName == "" || repoName == "" || format == "" || pkgName == "" {
		return shared.JSONError("ValidationException", "domain, repository, format, and package are required", http.StatusBadRequest), nil
	}
	pkg, err := p.store.GetPackage(pkgName, namespace, format, domainName, repoName)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "package not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"package": packageToMap(pkg),
	})
}

func (p *Provider) listPackages(req *http.Request) (*plugin.Response, error) {
	q := req.URL.Query()
	domainName := q.Get("domain")
	repoName := q.Get("repository")
	format := q.Get("format")
	namespace := q.Get("namespace")
	if domainName == "" || repoName == "" {
		return shared.JSONError("ValidationException", "domain and repository are required", http.StatusBadRequest), nil
	}
	pkgs, err := p.store.ListPackages(domainName, repoName, format, namespace)
	if err != nil {
		return nil, err
	}
	summaries := make([]map[string]any, 0, len(pkgs))
	for _, pkg := range pkgs {
		summaries = append(summaries, packageSummaryToMap(&pkg))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"packages": summaries,
	})
}

func (p *Provider) deletePackage(req *http.Request) (*plugin.Response, error) {
	q := req.URL.Query()
	domainName := q.Get("domain")
	repoName := q.Get("repository")
	format := q.Get("format")
	namespace := q.Get("namespace")
	pkgName := q.Get("package")
	if domainName == "" || repoName == "" || format == "" || pkgName == "" {
		return shared.JSONError("ValidationException", "domain, repository, format, and package are required", http.StatusBadRequest), nil
	}
	pkg, err := p.store.GetPackage(pkgName, namespace, format, domainName, repoName)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "package not found", http.StatusNotFound), nil
	}
	if err := p.store.DeletePackage(pkgName, namespace, format, domainName, repoName); err != nil {
		return shared.JSONError("ResourceNotFoundException", "package not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"deletedPackage": packageSummaryToMap(pkg),
	})
}

func (p *Provider) putPackageOriginConfiguration(req *http.Request, params map[string]any) (*plugin.Response, error) {
	q := req.URL.Query()
	domainName := q.Get("domain")
	if domainName == "" {
		domainName, _ = params["domain"].(string)
	}
	repoName := q.Get("repository")
	if repoName == "" {
		repoName, _ = params["repository"].(string)
	}
	format := q.Get("format")
	if format == "" {
		format, _ = params["format"].(string)
	}
	namespace := q.Get("namespace")
	if namespace == "" {
		namespace, _ = params["namespace"].(string)
	}
	pkgName := q.Get("package")
	if pkgName == "" {
		pkgName, _ = params["package"].(string)
	}
	if domainName == "" || repoName == "" || format == "" || pkgName == "" {
		return shared.JSONError("ValidationException", "domain, repository, format, and package are required", http.StatusBadRequest), nil
	}

	originConfigJSON := "{}"
	if v, ok := params["restrictions"]; ok {
		if b, err := json.Marshal(map[string]any{"restrictions": v}); err == nil {
			originConfigJSON = string(b)
		}
	}

	pkg := &Package{
		Name:         pkgName,
		Namespace:    namespace,
		Format:       format,
		DomainName:   domainName,
		RepoName:     repoName,
		OriginConfig: originConfigJSON,
	}
	if err := p.store.UpsertPackage(pkg); err != nil {
		return nil, err
	}

	var originConfig any
	json.Unmarshal([]byte(originConfigJSON), &originConfig) //nolint:errcheck
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"originConfiguration": originConfig,
	})
}

// --- PackageGroup ---

func (p *Provider) createPackageGroup(req *http.Request, params map[string]any) (*plugin.Response, error) {
	domainName := req.URL.Query().Get("domain")
	if domainName == "" {
		domainName, _ = params["domain"].(string)
	}
	pattern, _ := params["packageGroup"].(string)
	if domainName == "" || pattern == "" {
		return shared.JSONError("ValidationException", "domain and packageGroup are required", http.StatusBadRequest), nil
	}

	if _, err := p.store.GetDomain(domainName); err != nil {
		return shared.JSONError("ResourceNotFoundException", "domain not found", http.StatusNotFound), nil
	}

	description, _ := params["description"].(string)
	arn := shared.BuildARN("codeartifact", "package-group", domainName+"/"+pattern)

	pg := &PackageGroup{
		ARN:          arn,
		Pattern:      pattern,
		DomainName:   domainName,
		Description:  description,
		OriginConfig: "{}",
	}
	if err := p.store.CreatePackageGroup(pg); err != nil {
		if isUniqueErr(err) {
			return shared.JSONError("ConflictException", "package group already exists", http.StatusConflict), nil
		}
		return nil, err
	}

	if rawTags, ok := params["tags"].([]any); ok {
		tags := tagsListToMap(rawTags)
		p.store.tags.AddTags(arn, tags) //nolint:errcheck
	}

	stored, err := p.store.GetPackageGroup(arn)
	if err != nil {
		return nil, err
	}
	tags, _ := p.store.tags.ListTags(arn)
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"packageGroup": packageGroupToMap(stored, tags),
	})
}

func (p *Provider) describePackageGroup(req *http.Request) (*plugin.Response, error) {
	q := req.URL.Query()
	domainName := q.Get("domain")
	pattern := q.Get("packageGroup")
	if domainName == "" || pattern == "" {
		return shared.JSONError("ValidationException", "domain and packageGroup are required", http.StatusBadRequest), nil
	}
	pg, err := p.store.GetPackageGroupByPattern(pattern, domainName)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "package group not found", http.StatusNotFound), nil
	}
	tags, _ := p.store.tags.ListTags(pg.ARN)
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"packageGroup": packageGroupToMap(pg, tags),
	})
}

func (p *Provider) listPackageGroups(req *http.Request) (*plugin.Response, error) {
	domainName := req.URL.Query().Get("domain")
	if domainName == "" {
		return shared.JSONError("ValidationException", "domain is required", http.StatusBadRequest), nil
	}
	groups, err := p.store.ListPackageGroups(domainName)
	if err != nil {
		return nil, err
	}
	summaries := make([]map[string]any, 0, len(groups))
	for _, pg := range groups {
		tags, _ := p.store.tags.ListTags(pg.ARN)
		summaries = append(summaries, packageGroupToMap(&pg, tags))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"packageGroups": summaries,
	})
}

func (p *Provider) listSubPackageGroups(req *http.Request) (*plugin.Response, error) {
	// Stub: return empty list
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"packageGroups": []any{},
	})
}

func (p *Provider) updatePackageGroup(req *http.Request, params map[string]any) (*plugin.Response, error) {
	domainName := req.URL.Query().Get("domain")
	if domainName == "" {
		domainName, _ = params["domain"].(string)
	}
	pattern, _ := params["packageGroup"].(string)
	if domainName == "" || pattern == "" {
		return shared.JSONError("ValidationException", "domain and packageGroup are required", http.StatusBadRequest), nil
	}
	pg, err := p.store.GetPackageGroupByPattern(pattern, domainName)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "package group not found", http.StatusNotFound), nil
	}
	description := pg.Description
	if v, ok := params["description"].(string); ok {
		description = v
	}
	if err := p.store.UpdatePackageGroup(pg.ARN, description); err != nil {
		return shared.JSONError("ResourceNotFoundException", "package group not found", http.StatusNotFound), nil
	}
	updated, err := p.store.GetPackageGroup(pg.ARN)
	if err != nil {
		return nil, err
	}
	tags, _ := p.store.tags.ListTags(updated.ARN)
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"packageGroup": packageGroupToMap(updated, tags),
	})
}

func (p *Provider) updatePackageGroupOriginConfiguration(req *http.Request, params map[string]any) (*plugin.Response, error) {
	domainName := req.URL.Query().Get("domain")
	if domainName == "" {
		domainName, _ = params["domain"].(string)
	}
	pattern, _ := params["packageGroup"].(string)
	if domainName == "" || pattern == "" {
		return shared.JSONError("ValidationException", "domain and packageGroup are required", http.StatusBadRequest), nil
	}
	pg, err := p.store.GetPackageGroupByPattern(pattern, domainName)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "package group not found", http.StatusNotFound), nil
	}

	originConfigJSON := pg.OriginConfig
	if v, ok := params["restrictions"]; ok {
		if b, err := json.Marshal(v); err == nil {
			originConfigJSON = string(b)
		}
	}

	if err := p.store.UpdatePackageGroupOriginConfig(pg.ARN, originConfigJSON); err != nil {
		return shared.JSONError("ResourceNotFoundException", "package group not found", http.StatusNotFound), nil
	}
	updated, err := p.store.GetPackageGroup(pg.ARN)
	if err != nil {
		return nil, err
	}
	tags, _ := p.store.tags.ListTags(updated.ARN)
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"packageGroup":             packageGroupToMap(updated, tags),
		"allowedRepositoryUpdates": map[string]any{},
	})
}

func (p *Provider) deletePackageGroup(req *http.Request) (*plugin.Response, error) {
	q := req.URL.Query()
	domainName := q.Get("domain")
	pattern := q.Get("packageGroup")
	if domainName == "" || pattern == "" {
		return shared.JSONError("ValidationException", "domain and packageGroup are required", http.StatusBadRequest), nil
	}
	pg, err := p.store.GetPackageGroupByPattern(pattern, domainName)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "package group not found", http.StatusNotFound), nil
	}
	p.store.tags.DeleteAllTags(pg.ARN) //nolint:errcheck
	tags, _ := p.store.tags.ListTags(pg.ARN)
	if err := p.store.DeletePackageGroup(pg.ARN); err != nil {
		return shared.JSONError("ResourceNotFoundException", "package group not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"packageGroup": packageGroupToMap(pg, tags),
	})
}

// --- Auth Token ---

func (p *Provider) getAuthorizationToken(req *http.Request) (*plugin.Response, error) {
	domainName := req.URL.Query().Get("domain")
	if domainName == "" {
		return shared.JSONError("ValidationException", "domain is required", http.StatusBadRequest), nil
	}
	token := shared.GenerateID("token-", 32)
	expiration := time.Now().Add(12 * time.Hour)
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"authorizationToken": token,
		"expiration":         expiration.Unix(),
	})
}

// --- Repository Endpoint ---

func (p *Provider) getRepositoryEndpoint(req *http.Request) (*plugin.Response, error) {
	q := req.URL.Query()
	domainName := q.Get("domain")
	repoName := q.Get("repository")
	format := q.Get("format")
	if domainName == "" || repoName == "" || format == "" {
		return shared.JSONError("ValidationException", "domain, repository, and format are required", http.StatusBadRequest), nil
	}
	endpoint := fmt.Sprintf("https://%s-%s.d.codeartifact.us-east-1.amazonaws.com/%s/%s/%s/",
		domainName, shared.DefaultAccountID, format, domainName, repoName)
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"repositoryEndpoint": endpoint,
	})
}

// --- Domain Permissions Policy ---

func (p *Provider) getDomainPermissionsPolicy(req *http.Request) (*plugin.Response, error) {
	domainName := req.URL.Query().Get("domain")
	if domainName == "" {
		return shared.JSONError("ValidationException", "domain is required", http.StatusBadRequest), nil
	}
	d, err := p.store.GetDomain(domainName)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "domain not found", http.StatusNotFound), nil
	}
	// Check if there's a policy tag
	policyDoc, _ := p.store.tags.ListTags(d.ARN + "/policy")
	if len(policyDoc) == 0 {
		return shared.JSONError("ResourceNotFoundException", "policy not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"policy": map[string]any{
			"document":    policyDoc["document"],
			"resourceArn": d.ARN,
			"revision":    "1",
		},
	})
}

func (p *Provider) putDomainPermissionsPolicy(req *http.Request, params map[string]any) (*plugin.Response, error) {
	domainName := req.URL.Query().Get("domain")
	if domainName == "" {
		domainName, _ = params["domain"].(string)
	}
	if domainName == "" {
		return shared.JSONError("ValidationException", "domain is required", http.StatusBadRequest), nil
	}
	d, err := p.store.GetDomain(domainName)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "domain not found", http.StatusNotFound), nil
	}
	policyDoc, _ := params["policyDocument"].(string)
	p.store.tags.AddTags(d.ARN+"/policy", map[string]string{"document": policyDoc}) //nolint:errcheck
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"policy": map[string]any{
			"document":    policyDoc,
			"resourceArn": d.ARN,
			"revision":    "1",
		},
	})
}

func (p *Provider) deleteDomainPermissionsPolicy(req *http.Request) (*plugin.Response, error) {
	domainName := req.URL.Query().Get("domain")
	if domainName == "" {
		return shared.JSONError("ValidationException", "domain is required", http.StatusBadRequest), nil
	}
	d, err := p.store.GetDomain(domainName)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "domain not found", http.StatusNotFound), nil
	}
	policyDoc, _ := p.store.tags.ListTags(d.ARN + "/policy")
	p.store.tags.DeleteAllTags(d.ARN + "/policy") //nolint:errcheck
	doc := ""
	if policyDoc != nil {
		doc = policyDoc["document"]
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"policy": map[string]any{
			"document":    doc,
			"resourceArn": d.ARN,
			"revision":    "1",
		},
	})
}

// --- Repository Permissions Policy ---

func (p *Provider) getRepositoryPermissionsPolicy(req *http.Request) (*plugin.Response, error) {
	q := req.URL.Query()
	domainName := q.Get("domain")
	repoName := q.Get("repository")
	if domainName == "" || repoName == "" {
		return shared.JSONError("ValidationException", "domain and repository are required", http.StatusBadRequest), nil
	}
	r, err := p.store.GetRepository(repoName, domainName)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "repository not found", http.StatusNotFound), nil
	}
	policyDoc, _ := p.store.tags.ListTags(r.ARN + "/policy")
	if len(policyDoc) == 0 {
		return shared.JSONError("ResourceNotFoundException", "policy not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"policy": map[string]any{
			"document":    policyDoc["document"],
			"resourceArn": r.ARN,
			"revision":    "1",
		},
	})
}

func (p *Provider) putRepositoryPermissionsPolicy(req *http.Request, params map[string]any) (*plugin.Response, error) {
	domainName := req.URL.Query().Get("domain")
	if domainName == "" {
		domainName, _ = params["domain"].(string)
	}
	repoName := req.URL.Query().Get("repository")
	if repoName == "" {
		repoName, _ = params["repository"].(string)
	}
	if domainName == "" || repoName == "" {
		return shared.JSONError("ValidationException", "domain and repository are required", http.StatusBadRequest), nil
	}
	r, err := p.store.GetRepository(repoName, domainName)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "repository not found", http.StatusNotFound), nil
	}
	policyDoc, _ := params["policyDocument"].(string)
	p.store.tags.AddTags(r.ARN+"/policy", map[string]string{"document": policyDoc}) //nolint:errcheck
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"policy": map[string]any{
			"document":    policyDoc,
			"resourceArn": r.ARN,
			"revision":    "1",
		},
	})
}

func (p *Provider) deleteRepositoryPermissionsPolicy(req *http.Request) (*plugin.Response, error) {
	q := req.URL.Query()
	domainName := q.Get("domain")
	repoName := q.Get("repository")
	if domainName == "" || repoName == "" {
		return shared.JSONError("ValidationException", "domain and repository are required", http.StatusBadRequest), nil
	}
	r, err := p.store.GetRepository(repoName, domainName)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "repository not found", http.StatusNotFound), nil
	}
	policyDoc, _ := p.store.tags.ListTags(r.ARN + "/policy")
	p.store.tags.DeleteAllTags(r.ARN + "/policy") //nolint:errcheck
	doc := ""
	if policyDoc != nil {
		doc = policyDoc["document"]
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"policy": map[string]any{
			"document":    doc,
			"resourceArn": r.ARN,
			"revision":    "1",
		},
	})
}

// --- External Connections ---

func (p *Provider) associateExternalConnection(req *http.Request, params map[string]any) (*plugin.Response, error) {
	domainName := req.URL.Query().Get("domain")
	if domainName == "" {
		domainName, _ = params["domain"].(string)
	}
	repoName := req.URL.Query().Get("repository")
	if repoName == "" {
		repoName, _ = params["repository"].(string)
	}
	extConn, _ := params["externalConnection"].(string)
	if domainName == "" || repoName == "" || extConn == "" {
		return shared.JSONError("ValidationException", "domain, repository, and externalConnection are required", http.StatusBadRequest), nil
	}
	r, err := p.store.GetRepository(repoName, domainName)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "repository not found", http.StatusNotFound), nil
	}

	var conns []any
	json.Unmarshal([]byte(r.ExternalConns), &conns) //nolint:errcheck
	conns = append(conns, map[string]any{
		"externalConnectionName": extConn,
		"packageFormat":          "npm",
		"status":                 "Available",
	})
	b, _ := json.Marshal(conns)
	if err := p.store.UpdateRepositoryExternalConns(repoName, domainName, string(b)); err != nil {
		return nil, err
	}
	updated, err := p.store.GetRepository(repoName, domainName)
	if err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"repository": repositoryToMap(updated),
	})
}

func (p *Provider) disassociateExternalConnection(req *http.Request) (*plugin.Response, error) {
	q := req.URL.Query()
	domainName := q.Get("domain")
	repoName := q.Get("repository")
	extConn := q.Get("externalConnection")
	if domainName == "" || repoName == "" || extConn == "" {
		return shared.JSONError("ValidationException", "domain, repository, and externalConnection are required", http.StatusBadRequest), nil
	}
	r, err := p.store.GetRepository(repoName, domainName)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "repository not found", http.StatusNotFound), nil
	}

	var conns []any
	json.Unmarshal([]byte(r.ExternalConns), &conns) //nolint:errcheck
	newConns := make([]any, 0, len(conns))
	for _, c := range conns {
		if cm, ok := c.(map[string]any); ok {
			if cm["externalConnectionName"] != extConn {
				newConns = append(newConns, c)
			}
		}
	}
	b, _ := json.Marshal(newConns)
	if err := p.store.UpdateRepositoryExternalConns(repoName, domainName, string(b)); err != nil {
		return nil, err
	}
	updated, err := p.store.GetRepository(repoName, domainName)
	if err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"repository": repositoryToMap(updated),
	})
}

// --- Tags ---

func (p *Provider) tagResource(req *http.Request, params map[string]any) (*plugin.Response, error) {
	arn := req.URL.Query().Get("resourceArn")
	if arn == "" {
		arn, _ = params["resourceArn"].(string)
	}
	if arn == "" {
		return shared.JSONError("ValidationException", "resourceArn is required", http.StatusBadRequest), nil
	}
	if rawTags, ok := params["tags"].([]any); ok {
		tags := tagsListToMap(rawTags)
		if err := p.store.tags.AddTags(arn, tags); err != nil {
			return nil, err
		}
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) untagResource(req *http.Request) (*plugin.Response, error) {
	arn := req.URL.Query().Get("resourceArn")
	if arn == "" {
		return shared.JSONError("ValidationException", "resourceArn is required", http.StatusBadRequest), nil
	}
	keys := req.URL.Query()["tagKeys"]
	if err := p.store.tags.RemoveTags(arn, keys); err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) listTagsForResource(req *http.Request) (*plugin.Response, error) {
	arn := req.URL.Query().Get("resourceArn")
	if arn == "" {
		return shared.JSONError("ValidationException", "resourceArn is required", http.StatusBadRequest), nil
	}
	tags, err := p.store.tags.ListTags(arn)
	if err != nil {
		return nil, err
	}
	// Convert to tag list format
	tagList := make([]map[string]string, 0, len(tags))
	for k, v := range tags {
		tagList = append(tagList, map[string]string{"key": k, "value": v})
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"tags": tagList})
}

// --- Helpers ---

func domainToMap(d *Domain, tags map[string]string) map[string]any {
	if tags == nil {
		tags = map[string]string{}
	}
	return map[string]any{
		"name":            d.Name,
		"arn":             d.ARN,
		"owner":           d.Owner,
		"status":          d.Status,
		"encryptionKey":   d.EncryptionKey,
		"repositoryCount": d.RepoCount,
		"createdTime":     d.CreatedAt.Unix(),
		"tags":            tags,
	}
}

func domainSummaryToMap(d *Domain) map[string]any {
	return map[string]any{
		"name":          d.Name,
		"arn":           d.ARN,
		"owner":         d.Owner,
		"status":        d.Status,
		"encryptionKey": d.EncryptionKey,
		"createdTime":   d.CreatedAt.Unix(),
	}
}

func repositoryToMap(r *Repository) map[string]any {
	var upstreams, externalConns any
	json.Unmarshal([]byte(r.Upstreams), &upstreams)         //nolint:errcheck
	json.Unmarshal([]byte(r.ExternalConns), &externalConns) //nolint:errcheck
	if upstreams == nil {
		upstreams = []any{}
	}
	if externalConns == nil {
		externalConns = []any{}
	}
	return map[string]any{
		"name":                 r.Name,
		"domainName":           r.DomainName,
		"domainOwner":          shared.DefaultAccountID,
		"administratorAccount": shared.DefaultAccountID,
		"arn":                  r.ARN,
		"description":          r.Description,
		"upstreams":            upstreams,
		"externalConnections":  externalConns,
		"createdTime":          r.CreatedAt.Unix(),
	}
}

func repositorySummaryToMap(r *Repository) map[string]any {
	return map[string]any{
		"name":                 r.Name,
		"domainName":           r.DomainName,
		"domainOwner":          shared.DefaultAccountID,
		"administratorAccount": shared.DefaultAccountID,
		"arn":                  r.ARN,
		"description":          r.Description,
		"createdTime":          r.CreatedAt.Unix(),
	}
}

func packageToMap(pkg *Package) map[string]any {
	var originConfig any
	json.Unmarshal([]byte(pkg.OriginConfig), &originConfig) //nolint:errcheck
	return map[string]any{
		"name":                pkg.Name,
		"namespace":           pkg.Namespace,
		"format":              pkg.Format,
		"originConfiguration": originConfig,
	}
}

func packageSummaryToMap(pkg *Package) map[string]any {
	var originConfig any
	json.Unmarshal([]byte(pkg.OriginConfig), &originConfig) //nolint:errcheck
	return map[string]any{
		"package":             pkg.Name,
		"namespace":           pkg.Namespace,
		"format":              pkg.Format,
		"originConfiguration": originConfig,
	}
}

func packageGroupToMap(pg *PackageGroup, tags map[string]string) map[string]any {
	if tags == nil {
		tags = map[string]string{}
	}
	var originConfig any
	json.Unmarshal([]byte(pg.OriginConfig), &originConfig) //nolint:errcheck
	return map[string]any{
		"arn":                 pg.ARN,
		"pattern":             pg.Pattern,
		"domainName":          pg.DomainName,
		"domainOwner":         shared.DefaultAccountID,
		"description":         pg.Description,
		"originConfiguration": originConfig,
		"createdTime":         pg.CreatedAt.Unix(),
		"tags":                tags,
	}
}

func isUniqueErr(err error) bool {
	return err != nil && strings.Contains(err.Error(), "UNIQUE constraint failed")
}

// tagsListToMap converts a CodeArtifact tags list [{key:..., value:...}] to map[string]string.
func tagsListToMap(rawTags []any) map[string]string {
	m := make(map[string]string)
	for _, item := range rawTags {
		if t, ok := item.(map[string]any); ok {
			k, _ := t["key"].(string)
			v, _ := t["value"].(string)
			if k != "" {
				m[k] = v
			}
		}
	}
	return m
}
