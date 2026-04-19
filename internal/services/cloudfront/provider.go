// SPDX-License-Identifier: Apache-2.0

// internal/services/cloudfront/provider.go
package cloudfront

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"net/http"
	"path/filepath"
	"strings"
	"time"

	generated "github.com/skyoo2003/devcloud/internal/generated/cloudfront"
	"github.com/skyoo2003/devcloud/internal/plugin"
)

const defaultAccountID = plugin.DefaultAccountID

// Provider implements the Cloudfront2020_05_31 service.
type Provider struct {
	store *CloudFrontStore
}

func (p *Provider) ServiceID() string             { return "cloudfront" }
func (p *Provider) ServiceName() string           { return "Cloudfront2020_05_31" }
func (p *Provider) Protocol() plugin.ProtocolType { return plugin.ProtocolRESTXML }

func (p *Provider) Init(cfg plugin.PluginConfig) error {
	dataDir := cfg.DataDir
	if dataDir == "" {
		dataDir = "."
	}
	var err error
	p.store, err = NewCloudFrontStore(filepath.Join(dataDir, "cloudfront"))
	return err
}

func (p *Provider) Shutdown(_ context.Context) error {
	if p.store != nil {
		return p.store.Close()
	}
	return nil
}

func (p *Provider) HandleRequest(_ context.Context, op string, req *http.Request) (*plugin.Response, error) {
	if op == "" {
		var params generated.PathParams
		op, params = generated.MatchOperation(req.Method, req.URL.Path)
		_ = params
	}
	switch op {
	// Distribution
	case "CreateDistribution", "CreateDistributionWithTags":
		return p.createDistribution(req)
	case "GetDistribution":
		return p.getDistribution(req)
	case "ListDistributions":
		return p.listDistributions(req)
	case "UpdateDistribution":
		return p.updateDistribution(req)
	case "DeleteDistribution":
		return p.deleteDistribution(req)
	case "GetDistributionConfig":
		return p.getDistributionConfig(req)

	// Cache Policy
	case "CreateCachePolicy":
		return p.createCachePolicy(req)
	case "GetCachePolicy":
		return p.getCachePolicy(req)
	case "ListCachePolicies":
		return p.listCachePolicies(req)
	case "UpdateCachePolicy":
		return p.updateCachePolicy(req)
	case "DeleteCachePolicy":
		return p.deleteCachePolicy(req)
	case "GetCachePolicyConfig":
		return p.getCachePolicyConfig(req)

	// Origin Request Policy
	case "CreateOriginRequestPolicy":
		return p.createOriginRequestPolicy(req)
	case "GetOriginRequestPolicy":
		return p.getOriginRequestPolicy(req)
	case "ListOriginRequestPolicies":
		return p.listOriginRequestPolicies(req)
	case "UpdateOriginRequestPolicy":
		return p.updateOriginRequestPolicy(req)
	case "DeleteOriginRequestPolicy":
		return p.deleteOriginRequestPolicy(req)
	case "GetOriginRequestPolicyConfig":
		return p.getOriginRequestPolicyConfig(req)

	// Response Headers Policy
	case "CreateResponseHeadersPolicy":
		return p.createResponseHeadersPolicy(req)
	case "GetResponseHeadersPolicy":
		return p.getResponseHeadersPolicy(req)
	case "ListResponseHeadersPolicies":
		return p.listResponseHeadersPolicies(req)
	case "UpdateResponseHeadersPolicy":
		return p.updateResponseHeadersPolicy(req)
	case "DeleteResponseHeadersPolicy":
		return p.deleteResponseHeadersPolicy(req)
	case "GetResponseHeadersPolicyConfig":
		return p.getResponseHeadersPolicyConfig(req)

	// Origin Access Control
	case "CreateOriginAccessControl":
		return p.createOriginAccessControl(req)
	case "GetOriginAccessControl":
		return p.getOriginAccessControl(req)
	case "ListOriginAccessControls":
		return p.listOriginAccessControls(req)
	case "UpdateOriginAccessControl":
		return p.updateOriginAccessControl(req)
	case "DeleteOriginAccessControl":
		return p.deleteOriginAccessControl(req)
	case "GetOriginAccessControlConfig":
		return p.getOriginAccessControlConfig(req)

	// Functions
	case "CreateFunction":
		return p.createFunction(req)
	case "DescribeFunction":
		return p.describeFunction(req)
	case "GetFunction":
		return p.describeFunction(req)
	case "ListFunctions":
		return p.listFunctions(req)
	case "UpdateFunction":
		return p.updateFunction(req)
	case "DeleteFunction":
		return p.deleteFunction(req)
	case "PublishFunction":
		return p.publishFunction(req)
	case "TestFunction":
		return p.testFunction(req)

	// Invalidations
	case "CreateInvalidation":
		return p.createInvalidation(req)
	case "GetInvalidation":
		return p.getInvalidation(req)
	case "ListInvalidations":
		return p.listInvalidations(req)

	// Tags
	case "TagResource":
		return p.tagResource(req)
	case "UntagResource":
		return p.untagResource(req)
	case "ListTagsForResource":
		return p.listTagsForResource(req)

	default:
		return &plugin.Response{
			StatusCode:  200,
			ContentType: "application/xml",
			Body:        []byte(`<?xml version="1.0"?>`),
		}, nil
	}
}

func (p *Provider) ListResources(_ context.Context) ([]plugin.Resource, error) {
	dists, err := p.store.ListDistributions()
	if err != nil {
		return nil, err
	}
	out := make([]plugin.Resource, 0, len(dists))
	for _, d := range dists {
		out = append(out, plugin.Resource{Type: "distribution", ID: d.ID, Name: d.DomainName})
	}
	return out, nil
}

func (p *Provider) GetMetrics(_ context.Context) (*plugin.ServiceMetrics, error) {
	return &plugin.ServiceMetrics{}, nil
}

// --- helpers ---

func cfError(code, msg string, status int) *plugin.Response {
	type errBody struct {
		XMLName xml.Name `xml:"ErrorResponse"`
		Error   struct {
			Type    string `xml:"Type"`
			Code    string `xml:"Code"`
			Message string `xml:"Message"`
		} `xml:"Error"`
	}
	body, _ := xml.Marshal(errBody{
		Error: struct {
			Type    string `xml:"Type"`
			Code    string `xml:"Code"`
			Message string `xml:"Message"`
		}{Type: "Sender", Code: code, Message: msg},
	})
	return &plugin.Response{
		StatusCode:  status,
		Body:        append([]byte(xml.Header), body...),
		ContentType: "application/xml",
	}
}

func xmlResp(status int, v any) (*plugin.Response, error) {
	body, err := xml.Marshal(v)
	if err != nil {
		return nil, err
	}
	return &plugin.Response{
		StatusCode:  status,
		Body:        append([]byte(xml.Header), body...),
		ContentType: "application/xml",
	}, nil
}

func xmlRespWithHeader(status int, v any, headers map[string]string) (*plugin.Response, error) {
	resp, err := xmlResp(status, v)
	if err != nil {
		return nil, err
	}
	resp.Headers = headers
	return resp, nil
}

func generateID() string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return strings.ToUpper(hex.EncodeToString(b))
}

func generateETag() string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return "E" + strings.ToUpper(hex.EncodeToString(b))
}

func idFromPath(path, segment string) string {
	parts := strings.Split(strings.Trim(path, "/"), "/")
	for i, p := range parts {
		if p == segment && i+1 < len(parts) {
			return parts[i+1]
		}
	}
	return ""
}

func distIDFromPath(path string) string {
	// /2020-05-31/distribution/{Id}[/...]
	return idFromPath(path, "distribution")
}

func cachePolicyIDFromPath(path string) string {
	return idFromPath(path, "cache-policy")
}

func originRequestPolicyIDFromPath(path string) string {
	return idFromPath(path, "origin-request-policy")
}

func responseHeadersPolicyIDFromPath(path string) string {
	return idFromPath(path, "response-headers-policy")
}

func oacIDFromPath(path string) string {
	return idFromPath(path, "origin-access-control")
}

func functionNameFromPath(path string) string {
	// /2020-05-31/function/{Name}[/...]
	return idFromPath(path, "function")
}

func invalidationIDFromPath(path string) string {
	return idFromPath(path, "invalidation")
}

// --- Distribution operations ---

func (p *Provider) createDistribution(req *http.Request) (*plugin.Response, error) {
	type input struct {
		XMLName   xml.Name `xml:"DistributionConfig"`
		Comment   string   `xml:"Comment"`
		Enabled   bool     `xml:"Enabled"`
		CallerRef string   `xml:"CallerReference"`
	}
	// body may be wrapped in CreateDistributionRequest
	type wrapper struct {
		XMLName            xml.Name `xml:"CreateDistributionRequest"`
		DistributionConfig input    `xml:"DistributionConfig"`
	}
	// try both shapes
	var cfg input
	var w wrapper
	data, err := readBody(req)
	if err != nil {
		return cfError("MalformedInput", "failed to read body", http.StatusBadRequest), nil
	}
	if xmlErr := xml.Unmarshal(data, &w); xmlErr == nil && w.DistributionConfig.CallerRef != "" {
		cfg = w.DistributionConfig
	} else {
		_ = xml.Unmarshal(data, &cfg)
	}

	id := generateID()
	etag := generateETag()
	now := time.Now()
	d := &Distribution{
		ID:         id,
		ARN:        fmt.Sprintf("arn:aws:cloudfront::%s:distribution/%s", defaultAccountID, id),
		DomainName: id + ".cloudfront.net",
		Status:     "Deployed",
		ETag:       etag,
		Config:     string(data),
		Comment:    cfg.Comment,
		Enabled:    cfg.Enabled,
		CreatedAt:  now,
		UpdatedAt:  now,
	}
	if err := p.store.CreateDistribution(d); err != nil {
		return nil, err
	}

	type response struct {
		XMLName      xml.Name `xml:"Distribution"`
		Id           string   `xml:"Id"`
		ARN          string   `xml:"ARN"`
		Status       string   `xml:"Status"`
		DomainName   string   `xml:"DomainName"`
		Comment      string   `xml:"Comment"`
		Enabled      bool     `xml:"DistributionConfig>Enabled"`
		LastModified string   `xml:"LastModifiedTime"`
	}
	resp := response{
		Id:           d.ID,
		ARN:          d.ARN,
		Status:       d.Status,
		DomainName:   d.DomainName,
		Comment:      d.Comment,
		Enabled:      d.Enabled,
		LastModified: now.UTC().Format(time.RFC3339),
	}
	return xmlRespWithHeader(http.StatusCreated, resp, map[string]string{
		"ETag":     etag,
		"Location": "/2020-05-31/distribution/" + id,
	})
}

func (p *Provider) getDistribution(req *http.Request) (*plugin.Response, error) {
	id := distIDFromPath(req.URL.Path)
	if id == "" {
		return cfError("InvalidInput", "missing distribution ID", http.StatusBadRequest), nil
	}
	d, err := p.store.GetDistribution(id)
	if err != nil {
		if err == ErrDistributionNotFound {
			return cfError("NoSuchDistribution", "distribution not found", http.StatusNotFound), nil
		}
		return nil, err
	}
	type response struct {
		XMLName      xml.Name `xml:"Distribution"`
		Id           string   `xml:"Id"`
		ARN          string   `xml:"ARN"`
		Status       string   `xml:"Status"`
		DomainName   string   `xml:"DomainName"`
		Comment      string   `xml:"Comment"`
		Enabled      bool     `xml:"DistributionConfig>Enabled"`
		LastModified string   `xml:"LastModifiedTime"`
	}
	resp := response{
		Id:           d.ID,
		ARN:          d.ARN,
		Status:       d.Status,
		DomainName:   d.DomainName,
		Comment:      d.Comment,
		Enabled:      d.Enabled,
		LastModified: d.UpdatedAt.UTC().Format(time.RFC3339),
	}
	return xmlRespWithHeader(http.StatusOK, resp, map[string]string{"ETag": d.ETag})
}

func (p *Provider) listDistributions(_ *http.Request) (*plugin.Response, error) {
	dists, err := p.store.ListDistributions()
	if err != nil {
		return nil, err
	}
	type item struct {
		Id         string `xml:"Id"`
		ARN        string `xml:"ARN"`
		Status     string `xml:"Status"`
		DomainName string `xml:"DomainName"`
		Comment    string `xml:"Comment"`
		Enabled    bool   `xml:"Enabled"`
	}
	type response struct {
		XMLName     xml.Name `xml:"DistributionList"`
		IsTruncated bool     `xml:"IsTruncated"`
		MaxItems    int      `xml:"MaxItems"`
		Quantity    int      `xml:"Quantity"`
		Items       []item   `xml:"Items>DistributionSummary"`
	}
	var items []item
	for _, d := range dists {
		items = append(items, item{
			Id:         d.ID,
			ARN:        d.ARN,
			Status:     d.Status,
			DomainName: d.DomainName,
			Comment:    d.Comment,
			Enabled:    d.Enabled,
		})
	}
	resp := response{
		IsTruncated: false,
		MaxItems:    100,
		Quantity:    len(items),
		Items:       items,
	}
	return xmlResp(http.StatusOK, resp)
}

func (p *Provider) updateDistribution(req *http.Request) (*plugin.Response, error) {
	id := distIDFromPath(req.URL.Path)
	if id == "" {
		return cfError("InvalidInput", "missing distribution ID", http.StatusBadRequest), nil
	}
	d, err := p.store.GetDistribution(id)
	if err != nil {
		if err == ErrDistributionNotFound {
			return cfError("NoSuchDistribution", "distribution not found", http.StatusNotFound), nil
		}
		return nil, err
	}
	type input struct {
		Comment string `xml:"Comment"`
		Enabled bool   `xml:"Enabled"`
	}
	var cfg input
	data, _ := readBody(req)
	_ = xml.Unmarshal(data, &cfg)

	d.Comment = cfg.Comment
	d.Enabled = cfg.Enabled
	d.ETag = generateETag()
	d.Config = string(data)
	d.UpdatedAt = time.Now()
	if err := p.store.UpdateDistribution(d); err != nil {
		return nil, err
	}
	type response struct {
		XMLName      xml.Name `xml:"Distribution"`
		Id           string   `xml:"Id"`
		ARN          string   `xml:"ARN"`
		Status       string   `xml:"Status"`
		DomainName   string   `xml:"DomainName"`
		Comment      string   `xml:"Comment"`
		Enabled      bool     `xml:"DistributionConfig>Enabled"`
		LastModified string   `xml:"LastModifiedTime"`
	}
	resp := response{
		Id:           d.ID,
		ARN:          d.ARN,
		Status:       d.Status,
		DomainName:   d.DomainName,
		Comment:      d.Comment,
		Enabled:      d.Enabled,
		LastModified: d.UpdatedAt.UTC().Format(time.RFC3339),
	}
	return xmlRespWithHeader(http.StatusOK, resp, map[string]string{"ETag": d.ETag})
}

func (p *Provider) deleteDistribution(req *http.Request) (*plugin.Response, error) {
	id := distIDFromPath(req.URL.Path)
	if id == "" {
		return cfError("InvalidInput", "missing distribution ID", http.StatusBadRequest), nil
	}
	if err := p.store.DeleteDistribution(id); err != nil {
		if err == ErrDistributionNotFound {
			return cfError("NoSuchDistribution", "distribution not found", http.StatusNotFound), nil
		}
		return nil, err
	}
	return &plugin.Response{StatusCode: http.StatusNoContent, ContentType: "application/xml"}, nil
}

func (p *Provider) getDistributionConfig(req *http.Request) (*plugin.Response, error) {
	id := distIDFromPath(req.URL.Path)
	if id == "" {
		return cfError("InvalidInput", "missing distribution ID", http.StatusBadRequest), nil
	}
	d, err := p.store.GetDistribution(id)
	if err != nil {
		if err == ErrDistributionNotFound {
			return cfError("NoSuchDistribution", "distribution not found", http.StatusNotFound), nil
		}
		return nil, err
	}
	type response struct {
		XMLName xml.Name `xml:"DistributionConfig"`
		Comment string   `xml:"Comment"`
		Enabled bool     `xml:"Enabled"`
	}
	resp := response{Comment: d.Comment, Enabled: d.Enabled}
	return xmlRespWithHeader(http.StatusOK, resp, map[string]string{"ETag": d.ETag})
}

// --- Cache Policy operations ---

type cachePolicyXML struct {
	XMLName           xml.Name `xml:"CachePolicy"`
	Id                string   `xml:"Id"`
	LastModified      string   `xml:"LastModifiedTime"`
	CachePolicyConfig struct {
		Name    string `xml:"Name"`
		Comment string `xml:"Comment"`
	} `xml:"CachePolicyConfig"`
}

func cpToXML(cp *CachePolicy) cachePolicyXML {
	var x cachePolicyXML
	x.Id = cp.ID
	x.LastModified = cp.CreatedAt.UTC().Format(time.RFC3339)
	x.CachePolicyConfig.Name = cp.Name
	x.CachePolicyConfig.Comment = cp.Comment
	return x
}

func (p *Provider) createCachePolicy(req *http.Request) (*plugin.Response, error) {
	type cpConfigInput struct {
		Name    string `xml:"Name"`
		Comment string `xml:"Comment"`
	}
	type input struct {
		XMLName           xml.Name      `xml:"CachePolicyConfig"`
		CachePolicyConfig cpConfigInput `xml:"CachePolicyConfig"`
		Name              string        `xml:"Name"`
		Comment           string        `xml:"Comment"`
	}
	var in input
	data, _ := readBody(req)
	_ = xml.Unmarshal(data, &in)
	name := in.Name
	comment := in.Comment
	if in.CachePolicyConfig.Name != "" {
		name = in.CachePolicyConfig.Name
		comment = in.CachePolicyConfig.Comment
	}
	if name == "" {
		return cfError("InvalidArgument", "Name is required", http.StatusBadRequest), nil
	}
	id := generateID()
	etag := generateETag()
	cp := &CachePolicy{
		ID:        id,
		ETag:      etag,
		Name:      name,
		Comment:   comment,
		Config:    string(data),
		CreatedAt: time.Now(),
	}
	if err := p.store.CreateCachePolicy(cp); err != nil {
		return cfError("CachePolicyAlreadyExists", err.Error(), http.StatusConflict), nil
	}
	type response struct {
		XMLName      xml.Name `xml:"CachePolicy"`
		Id           string   `xml:"Id"`
		LastModified string   `xml:"LastModifiedTime"`
		Config       struct {
			Name    string `xml:"Name"`
			Comment string `xml:"Comment"`
		} `xml:"CachePolicyConfig"`
	}
	var resp response
	resp.Id = cp.ID
	resp.LastModified = cp.CreatedAt.UTC().Format(time.RFC3339)
	resp.Config.Name = cp.Name
	resp.Config.Comment = cp.Comment
	return xmlRespWithHeader(http.StatusCreated, resp, map[string]string{
		"ETag":     etag,
		"Location": "/2020-05-31/cache-policy/" + id,
	})
}

func (p *Provider) getCachePolicy(req *http.Request) (*plugin.Response, error) {
	id := cachePolicyIDFromPath(req.URL.Path)
	if id == "" {
		return cfError("InvalidInput", "missing cache policy ID", http.StatusBadRequest), nil
	}
	cp, err := p.store.GetCachePolicy(id)
	if err != nil {
		if err == ErrCachePolicyNotFound {
			return cfError("NoSuchCachePolicy", "cache policy not found", http.StatusNotFound), nil
		}
		return nil, err
	}
	x := cpToXML(cp)
	return xmlRespWithHeader(http.StatusOK, x, map[string]string{"ETag": cp.ETag})
}

func (p *Provider) listCachePolicies(_ *http.Request) (*plugin.Response, error) {
	policies, err := p.store.ListCachePolicies()
	if err != nil {
		return nil, err
	}
	type item struct {
		CachePolicy struct {
			Id           string `xml:"Id"`
			LastModified string `xml:"LastModifiedTime"`
			Config       struct {
				Name    string `xml:"Name"`
				Comment string `xml:"Comment"`
			} `xml:"CachePolicyConfig"`
		} `xml:"CachePolicy"`
	}
	type response struct {
		XMLName     xml.Name `xml:"CachePolicyList"`
		IsTruncated bool     `xml:"IsTruncated"`
		MaxItems    int      `xml:"MaxItems"`
		Quantity    int      `xml:"Items>Quantity"`
		Items       []item   `xml:"Items>CachePolicySummary"`
	}
	var items []item
	for _, cp := range policies {
		var it item
		it.CachePolicy.Id = cp.ID
		it.CachePolicy.LastModified = cp.CreatedAt.UTC().Format(time.RFC3339)
		it.CachePolicy.Config.Name = cp.Name
		it.CachePolicy.Config.Comment = cp.Comment
		items = append(items, it)
	}
	resp := response{
		IsTruncated: false,
		MaxItems:    100,
		Quantity:    len(items),
		Items:       items,
	}
	return xmlResp(http.StatusOK, resp)
}

func (p *Provider) updateCachePolicy(req *http.Request) (*plugin.Response, error) {
	id := cachePolicyIDFromPath(req.URL.Path)
	if id == "" {
		return cfError("InvalidInput", "missing cache policy ID", http.StatusBadRequest), nil
	}
	cp, err := p.store.GetCachePolicy(id)
	if err != nil {
		if err == ErrCachePolicyNotFound {
			return cfError("NoSuchCachePolicy", "cache policy not found", http.StatusNotFound), nil
		}
		return nil, err
	}
	type input struct {
		Name    string `xml:"Name"`
		Comment string `xml:"Comment"`
	}
	var in input
	data, _ := readBody(req)
	_ = xml.Unmarshal(data, &in)
	if in.Name != "" {
		cp.Name = in.Name
	}
	cp.Comment = in.Comment
	cp.ETag = generateETag()
	cp.Config = string(data)
	if err := p.store.UpdateCachePolicy(cp); err != nil {
		return nil, err
	}
	x := cpToXML(cp)
	return xmlRespWithHeader(http.StatusOK, x, map[string]string{"ETag": cp.ETag})
}

func (p *Provider) deleteCachePolicy(req *http.Request) (*plugin.Response, error) {
	id := cachePolicyIDFromPath(req.URL.Path)
	if id == "" {
		return cfError("InvalidInput", "missing cache policy ID", http.StatusBadRequest), nil
	}
	if err := p.store.DeleteCachePolicy(id); err != nil {
		if err == ErrCachePolicyNotFound {
			return cfError("NoSuchCachePolicy", "cache policy not found", http.StatusNotFound), nil
		}
		return nil, err
	}
	return &plugin.Response{StatusCode: http.StatusNoContent, ContentType: "application/xml"}, nil
}

func (p *Provider) getCachePolicyConfig(req *http.Request) (*plugin.Response, error) {
	id := cachePolicyIDFromPath(req.URL.Path)
	if id == "" {
		return cfError("InvalidInput", "missing cache policy ID", http.StatusBadRequest), nil
	}
	cp, err := p.store.GetCachePolicy(id)
	if err != nil {
		if err == ErrCachePolicyNotFound {
			return cfError("NoSuchCachePolicy", "cache policy not found", http.StatusNotFound), nil
		}
		return nil, err
	}
	type response struct {
		XMLName xml.Name `xml:"CachePolicyConfig"`
		Name    string   `xml:"Name"`
		Comment string   `xml:"Comment"`
	}
	resp := response{Name: cp.Name, Comment: cp.Comment}
	return xmlRespWithHeader(http.StatusOK, resp, map[string]string{"ETag": cp.ETag})
}

// --- Origin Request Policy operations ---

func (p *Provider) createOriginRequestPolicy(req *http.Request) (*plugin.Response, error) {
	type input struct {
		Name    string `xml:"Name"`
		Comment string `xml:"Comment"`
	}
	var in input
	data, _ := readBody(req)
	_ = xml.Unmarshal(data, &in)
	if in.Name == "" {
		return cfError("InvalidArgument", "Name is required", http.StatusBadRequest), nil
	}
	id := generateID()
	etag := generateETag()
	orp := &OriginRequestPolicy{
		ID:        id,
		ETag:      etag,
		Name:      in.Name,
		Comment:   in.Comment,
		Config:    string(data),
		CreatedAt: time.Now(),
	}
	if err := p.store.CreateOriginRequestPolicy(orp); err != nil {
		return cfError("OriginRequestPolicyAlreadyExists", err.Error(), http.StatusConflict), nil
	}
	type response struct {
		XMLName      xml.Name `xml:"OriginRequestPolicy"`
		Id           string   `xml:"Id"`
		LastModified string   `xml:"LastModifiedTime"`
		Config       struct {
			Name    string `xml:"Name"`
			Comment string `xml:"Comment"`
		} `xml:"OriginRequestPolicyConfig"`
	}
	var resp response
	resp.Id = orp.ID
	resp.LastModified = orp.CreatedAt.UTC().Format(time.RFC3339)
	resp.Config.Name = orp.Name
	resp.Config.Comment = orp.Comment
	return xmlRespWithHeader(http.StatusCreated, resp, map[string]string{
		"ETag":     etag,
		"Location": "/2020-05-31/origin-request-policy/" + id,
	})
}

func (p *Provider) getOriginRequestPolicy(req *http.Request) (*plugin.Response, error) {
	id := originRequestPolicyIDFromPath(req.URL.Path)
	if id == "" {
		return cfError("InvalidInput", "missing origin request policy ID", http.StatusBadRequest), nil
	}
	orp, err := p.store.GetOriginRequestPolicy(id)
	if err != nil {
		if err == ErrOriginRequestPolicyNotFound {
			return cfError("NoSuchOriginRequestPolicy", "origin request policy not found", http.StatusNotFound), nil
		}
		return nil, err
	}
	type response struct {
		XMLName      xml.Name `xml:"OriginRequestPolicy"`
		Id           string   `xml:"Id"`
		LastModified string   `xml:"LastModifiedTime"`
		Config       struct {
			Name    string `xml:"Name"`
			Comment string `xml:"Comment"`
		} `xml:"OriginRequestPolicyConfig"`
	}
	var resp response
	resp.Id = orp.ID
	resp.LastModified = orp.CreatedAt.UTC().Format(time.RFC3339)
	resp.Config.Name = orp.Name
	resp.Config.Comment = orp.Comment
	return xmlRespWithHeader(http.StatusOK, resp, map[string]string{"ETag": orp.ETag})
}

func (p *Provider) listOriginRequestPolicies(_ *http.Request) (*plugin.Response, error) {
	policies, err := p.store.ListOriginRequestPolicies()
	if err != nil {
		return nil, err
	}
	type item struct {
		Id           string `xml:"Id"`
		LastModified string `xml:"LastModifiedTime"`
		Name         string `xml:"OriginRequestPolicyConfig>Name"`
		Comment      string `xml:"OriginRequestPolicyConfig>Comment"`
	}
	type response struct {
		XMLName     xml.Name `xml:"OriginRequestPolicyList"`
		IsTruncated bool     `xml:"IsTruncated"`
		MaxItems    int      `xml:"MaxItems"`
		Quantity    int      `xml:"Items>Quantity"`
		Items       []item   `xml:"Items>OriginRequestPolicySummary>OriginRequestPolicy"`
	}
	var items []item
	for _, orp := range policies {
		items = append(items, item{
			Id:           orp.ID,
			LastModified: orp.CreatedAt.UTC().Format(time.RFC3339),
			Name:         orp.Name,
			Comment:      orp.Comment,
		})
	}
	resp := response{
		IsTruncated: false,
		MaxItems:    100,
		Quantity:    len(items),
		Items:       items,
	}
	return xmlResp(http.StatusOK, resp)
}

func (p *Provider) updateOriginRequestPolicy(req *http.Request) (*plugin.Response, error) {
	id := originRequestPolicyIDFromPath(req.URL.Path)
	if id == "" {
		return cfError("InvalidInput", "missing origin request policy ID", http.StatusBadRequest), nil
	}
	orp, err := p.store.GetOriginRequestPolicy(id)
	if err != nil {
		if err == ErrOriginRequestPolicyNotFound {
			return cfError("NoSuchOriginRequestPolicy", "origin request policy not found", http.StatusNotFound), nil
		}
		return nil, err
	}
	type input struct {
		Name    string `xml:"Name"`
		Comment string `xml:"Comment"`
	}
	var in input
	data, _ := readBody(req)
	_ = xml.Unmarshal(data, &in)
	if in.Name != "" {
		orp.Name = in.Name
	}
	orp.Comment = in.Comment
	orp.ETag = generateETag()
	orp.Config = string(data)
	if err := p.store.UpdateOriginRequestPolicy(orp); err != nil {
		return nil, err
	}
	type response struct {
		XMLName      xml.Name `xml:"OriginRequestPolicy"`
		Id           string   `xml:"Id"`
		LastModified string   `xml:"LastModifiedTime"`
		Config       struct {
			Name    string `xml:"Name"`
			Comment string `xml:"Comment"`
		} `xml:"OriginRequestPolicyConfig"`
	}
	var resp response
	resp.Id = orp.ID
	resp.LastModified = orp.CreatedAt.UTC().Format(time.RFC3339)
	resp.Config.Name = orp.Name
	resp.Config.Comment = orp.Comment
	return xmlRespWithHeader(http.StatusOK, resp, map[string]string{"ETag": orp.ETag})
}

func (p *Provider) deleteOriginRequestPolicy(req *http.Request) (*plugin.Response, error) {
	id := originRequestPolicyIDFromPath(req.URL.Path)
	if id == "" {
		return cfError("InvalidInput", "missing origin request policy ID", http.StatusBadRequest), nil
	}
	if err := p.store.DeleteOriginRequestPolicy(id); err != nil {
		if err == ErrOriginRequestPolicyNotFound {
			return cfError("NoSuchOriginRequestPolicy", "origin request policy not found", http.StatusNotFound), nil
		}
		return nil, err
	}
	return &plugin.Response{StatusCode: http.StatusNoContent, ContentType: "application/xml"}, nil
}

func (p *Provider) getOriginRequestPolicyConfig(req *http.Request) (*plugin.Response, error) {
	id := originRequestPolicyIDFromPath(req.URL.Path)
	if id == "" {
		return cfError("InvalidInput", "missing origin request policy ID", http.StatusBadRequest), nil
	}
	orp, err := p.store.GetOriginRequestPolicy(id)
	if err != nil {
		if err == ErrOriginRequestPolicyNotFound {
			return cfError("NoSuchOriginRequestPolicy", "origin request policy not found", http.StatusNotFound), nil
		}
		return nil, err
	}
	type response struct {
		XMLName xml.Name `xml:"OriginRequestPolicyConfig"`
		Name    string   `xml:"Name"`
		Comment string   `xml:"Comment"`
	}
	resp := response{Name: orp.Name, Comment: orp.Comment}
	return xmlRespWithHeader(http.StatusOK, resp, map[string]string{"ETag": orp.ETag})
}

// --- Response Headers Policy operations ---

func (p *Provider) createResponseHeadersPolicy(req *http.Request) (*plugin.Response, error) {
	type input struct {
		Name    string `xml:"Name"`
		Comment string `xml:"Comment"`
	}
	var in input
	data, _ := readBody(req)
	_ = xml.Unmarshal(data, &in)
	if in.Name == "" {
		return cfError("InvalidArgument", "Name is required", http.StatusBadRequest), nil
	}
	id := generateID()
	etag := generateETag()
	rhp := &ResponseHeadersPolicy{
		ID:        id,
		ETag:      etag,
		Name:      in.Name,
		Comment:   in.Comment,
		Config:    string(data),
		CreatedAt: time.Now(),
	}
	if err := p.store.CreateResponseHeadersPolicy(rhp); err != nil {
		return cfError("ResponseHeadersPolicyAlreadyExists", err.Error(), http.StatusConflict), nil
	}
	type response struct {
		XMLName      xml.Name `xml:"ResponseHeadersPolicy"`
		Id           string   `xml:"Id"`
		LastModified string   `xml:"LastModifiedTime"`
		Config       struct {
			Name    string `xml:"Name"`
			Comment string `xml:"Comment"`
		} `xml:"ResponseHeadersPolicyConfig"`
	}
	var resp response
	resp.Id = rhp.ID
	resp.LastModified = rhp.CreatedAt.UTC().Format(time.RFC3339)
	resp.Config.Name = rhp.Name
	resp.Config.Comment = rhp.Comment
	return xmlRespWithHeader(http.StatusCreated, resp, map[string]string{
		"ETag":     etag,
		"Location": "/2020-05-31/response-headers-policy/" + id,
	})
}

func (p *Provider) getResponseHeadersPolicy(req *http.Request) (*plugin.Response, error) {
	id := responseHeadersPolicyIDFromPath(req.URL.Path)
	if id == "" {
		return cfError("InvalidInput", "missing response headers policy ID", http.StatusBadRequest), nil
	}
	rhp, err := p.store.GetResponseHeadersPolicy(id)
	if err != nil {
		if err == ErrResponseHeadersPolicyNotFound {
			return cfError("NoSuchResponseHeadersPolicy", "response headers policy not found", http.StatusNotFound), nil
		}
		return nil, err
	}
	type response struct {
		XMLName      xml.Name `xml:"ResponseHeadersPolicy"`
		Id           string   `xml:"Id"`
		LastModified string   `xml:"LastModifiedTime"`
		Config       struct {
			Name    string `xml:"Name"`
			Comment string `xml:"Comment"`
		} `xml:"ResponseHeadersPolicyConfig"`
	}
	var resp response
	resp.Id = rhp.ID
	resp.LastModified = rhp.CreatedAt.UTC().Format(time.RFC3339)
	resp.Config.Name = rhp.Name
	resp.Config.Comment = rhp.Comment
	return xmlRespWithHeader(http.StatusOK, resp, map[string]string{"ETag": rhp.ETag})
}

func (p *Provider) listResponseHeadersPolicies(_ *http.Request) (*plugin.Response, error) {
	policies, err := p.store.ListResponseHeadersPolicies()
	if err != nil {
		return nil, err
	}
	type item struct {
		Id           string `xml:"Id"`
		LastModified string `xml:"LastModifiedTime"`
		Name         string `xml:"ResponseHeadersPolicyConfig>Name"`
		Comment      string `xml:"ResponseHeadersPolicyConfig>Comment"`
	}
	type response struct {
		XMLName     xml.Name `xml:"ResponseHeadersPolicyList"`
		IsTruncated bool     `xml:"IsTruncated"`
		MaxItems    int      `xml:"MaxItems"`
		Quantity    int      `xml:"Items>Quantity"`
		Items       []item   `xml:"Items>ResponseHeadersPolicySummary>ResponseHeadersPolicy"`
	}
	var items []item
	for _, rhp := range policies {
		items = append(items, item{
			Id:           rhp.ID,
			LastModified: rhp.CreatedAt.UTC().Format(time.RFC3339),
			Name:         rhp.Name,
			Comment:      rhp.Comment,
		})
	}
	resp := response{
		IsTruncated: false,
		MaxItems:    100,
		Quantity:    len(items),
		Items:       items,
	}
	return xmlResp(http.StatusOK, resp)
}

func (p *Provider) updateResponseHeadersPolicy(req *http.Request) (*plugin.Response, error) {
	id := responseHeadersPolicyIDFromPath(req.URL.Path)
	if id == "" {
		return cfError("InvalidInput", "missing response headers policy ID", http.StatusBadRequest), nil
	}
	rhp, err := p.store.GetResponseHeadersPolicy(id)
	if err != nil {
		if err == ErrResponseHeadersPolicyNotFound {
			return cfError("NoSuchResponseHeadersPolicy", "response headers policy not found", http.StatusNotFound), nil
		}
		return nil, err
	}
	type input struct {
		Name    string `xml:"Name"`
		Comment string `xml:"Comment"`
	}
	var in input
	data, _ := readBody(req)
	_ = xml.Unmarshal(data, &in)
	if in.Name != "" {
		rhp.Name = in.Name
	}
	rhp.Comment = in.Comment
	rhp.ETag = generateETag()
	rhp.Config = string(data)
	if err := p.store.UpdateResponseHeadersPolicy(rhp); err != nil {
		return nil, err
	}
	type response struct {
		XMLName      xml.Name `xml:"ResponseHeadersPolicy"`
		Id           string   `xml:"Id"`
		LastModified string   `xml:"LastModifiedTime"`
		Config       struct {
			Name    string `xml:"Name"`
			Comment string `xml:"Comment"`
		} `xml:"ResponseHeadersPolicyConfig"`
	}
	var resp response
	resp.Id = rhp.ID
	resp.LastModified = rhp.CreatedAt.UTC().Format(time.RFC3339)
	resp.Config.Name = rhp.Name
	resp.Config.Comment = rhp.Comment
	return xmlRespWithHeader(http.StatusOK, resp, map[string]string{"ETag": rhp.ETag})
}

func (p *Provider) deleteResponseHeadersPolicy(req *http.Request) (*plugin.Response, error) {
	id := responseHeadersPolicyIDFromPath(req.URL.Path)
	if id == "" {
		return cfError("InvalidInput", "missing response headers policy ID", http.StatusBadRequest), nil
	}
	if err := p.store.DeleteResponseHeadersPolicy(id); err != nil {
		if err == ErrResponseHeadersPolicyNotFound {
			return cfError("NoSuchResponseHeadersPolicy", "response headers policy not found", http.StatusNotFound), nil
		}
		return nil, err
	}
	return &plugin.Response{StatusCode: http.StatusNoContent, ContentType: "application/xml"}, nil
}

func (p *Provider) getResponseHeadersPolicyConfig(req *http.Request) (*plugin.Response, error) {
	id := responseHeadersPolicyIDFromPath(req.URL.Path)
	if id == "" {
		return cfError("InvalidInput", "missing response headers policy ID", http.StatusBadRequest), nil
	}
	rhp, err := p.store.GetResponseHeadersPolicy(id)
	if err != nil {
		if err == ErrResponseHeadersPolicyNotFound {
			return cfError("NoSuchResponseHeadersPolicy", "response headers policy not found", http.StatusNotFound), nil
		}
		return nil, err
	}
	type response struct {
		XMLName xml.Name `xml:"ResponseHeadersPolicyConfig"`
		Name    string   `xml:"Name"`
		Comment string   `xml:"Comment"`
	}
	resp := response{Name: rhp.Name, Comment: rhp.Comment}
	return xmlRespWithHeader(http.StatusOK, resp, map[string]string{"ETag": rhp.ETag})
}

// --- Origin Access Control operations ---

func (p *Provider) createOriginAccessControl(req *http.Request) (*plugin.Response, error) {
	type configInput struct {
		Name            string `xml:"Name"`
		Description     string `xml:"Description"`
		SigningProtocol string `xml:"SigningProtocol"`
		SigningBehavior string `xml:"SigningBehavior"`
		OriginType      string `xml:"OriginAccessControlOriginType"`
	}
	type input struct {
		XMLName         xml.Name    `xml:"OriginAccessControlConfig"`
		Config          configInput `xml:"OriginAccessControlConfig"`
		Name            string      `xml:"Name"`
		Description     string      `xml:"Description"`
		SigningProtocol string      `xml:"SigningProtocol"`
		SigningBehavior string      `xml:"SigningBehavior"`
		OriginType      string      `xml:"OriginAccessControlOriginType"`
	}
	var in input
	data, _ := readBody(req)
	_ = xml.Unmarshal(data, &in)
	name := in.Name
	if in.Config.Name != "" {
		name = in.Config.Name
	}
	if name == "" {
		return cfError("InvalidArgument", "Name is required", http.StatusBadRequest), nil
	}
	sigProto := in.SigningProtocol
	if in.Config.SigningProtocol != "" {
		sigProto = in.Config.SigningProtocol
	}
	if sigProto == "" {
		sigProto = "sigv4"
	}
	sigBehavior := in.SigningBehavior
	if in.Config.SigningBehavior != "" {
		sigBehavior = in.Config.SigningBehavior
	}
	if sigBehavior == "" {
		sigBehavior = "always"
	}
	originType := in.OriginType
	if in.Config.OriginType != "" {
		originType = in.Config.OriginType
	}
	if originType == "" {
		originType = "s3"
	}
	desc := in.Description
	if in.Config.Description != "" {
		desc = in.Config.Description
	}
	id := generateID()
	etag := generateETag()
	oac := &OriginAccessControl{
		ID:              id,
		ETag:            etag,
		Name:            name,
		Description:     desc,
		SigningProtocol: sigProto,
		SigningBehavior: sigBehavior,
		OriginType:      originType,
		CreatedAt:       time.Now(),
	}
	if err := p.store.CreateOriginAccessControl(oac); err != nil {
		return nil, err
	}
	type response struct {
		XMLName xml.Name `xml:"OriginAccessControl"`
		Id      string   `xml:"Id"`
		Config  struct {
			Name            string `xml:"Name"`
			Description     string `xml:"Description"`
			SigningProtocol string `xml:"SigningProtocol"`
			SigningBehavior string `xml:"SigningBehavior"`
			OriginType      string `xml:"OriginAccessControlOriginType"`
		} `xml:"OriginAccessControlConfig"`
	}
	var resp response
	resp.Id = oac.ID
	resp.Config.Name = oac.Name
	resp.Config.Description = oac.Description
	resp.Config.SigningProtocol = oac.SigningProtocol
	resp.Config.SigningBehavior = oac.SigningBehavior
	resp.Config.OriginType = oac.OriginType
	return xmlRespWithHeader(http.StatusCreated, resp, map[string]string{
		"ETag":     etag,
		"Location": "/2020-05-31/origin-access-control/" + id,
	})
}

func (p *Provider) getOriginAccessControl(req *http.Request) (*plugin.Response, error) {
	id := oacIDFromPath(req.URL.Path)
	if id == "" {
		return cfError("InvalidInput", "missing origin access control ID", http.StatusBadRequest), nil
	}
	oac, err := p.store.GetOriginAccessControl(id)
	if err != nil {
		if err == ErrOriginAccessControlNotFound {
			return cfError("NoSuchOriginAccessControl", "origin access control not found", http.StatusNotFound), nil
		}
		return nil, err
	}
	type response struct {
		XMLName xml.Name `xml:"OriginAccessControl"`
		Id      string   `xml:"Id"`
		Config  struct {
			Name            string `xml:"Name"`
			Description     string `xml:"Description"`
			SigningProtocol string `xml:"SigningProtocol"`
			SigningBehavior string `xml:"SigningBehavior"`
			OriginType      string `xml:"OriginAccessControlOriginType"`
		} `xml:"OriginAccessControlConfig"`
	}
	var resp response
	resp.Id = oac.ID
	resp.Config.Name = oac.Name
	resp.Config.Description = oac.Description
	resp.Config.SigningProtocol = oac.SigningProtocol
	resp.Config.SigningBehavior = oac.SigningBehavior
	resp.Config.OriginType = oac.OriginType
	return xmlRespWithHeader(http.StatusOK, resp, map[string]string{"ETag": oac.ETag})
}

func (p *Provider) listOriginAccessControls(_ *http.Request) (*plugin.Response, error) {
	oacs, err := p.store.ListOriginAccessControls()
	if err != nil {
		return nil, err
	}
	type item struct {
		Id         string `xml:"Id"`
		Name       string `xml:"OriginAccessControlConfig>Name"`
		OriginType string `xml:"OriginAccessControlConfig>OriginAccessControlOriginType"`
	}
	type response struct {
		XMLName     xml.Name `xml:"OriginAccessControlList"`
		IsTruncated bool     `xml:"IsTruncated"`
		MaxItems    int      `xml:"MaxItems"`
		Quantity    int      `xml:"Items>Quantity"`
		Items       []item   `xml:"Items>OriginAccessControlSummary"`
	}
	var items []item
	for _, oac := range oacs {
		items = append(items, item{
			Id:         oac.ID,
			Name:       oac.Name,
			OriginType: oac.OriginType,
		})
	}
	resp := response{
		IsTruncated: false,
		MaxItems:    100,
		Quantity:    len(items),
		Items:       items,
	}
	return xmlResp(http.StatusOK, resp)
}

func (p *Provider) updateOriginAccessControl(req *http.Request) (*plugin.Response, error) {
	id := oacIDFromPath(req.URL.Path)
	if id == "" {
		return cfError("InvalidInput", "missing origin access control ID", http.StatusBadRequest), nil
	}
	oac, err := p.store.GetOriginAccessControl(id)
	if err != nil {
		if err == ErrOriginAccessControlNotFound {
			return cfError("NoSuchOriginAccessControl", "origin access control not found", http.StatusNotFound), nil
		}
		return nil, err
	}
	type input struct {
		Name            string `xml:"Name"`
		Description     string `xml:"Description"`
		SigningProtocol string `xml:"SigningProtocol"`
		SigningBehavior string `xml:"SigningBehavior"`
		OriginType      string `xml:"OriginAccessControlOriginType"`
	}
	var in input
	data, _ := readBody(req)
	_ = xml.Unmarshal(data, &in)
	if in.Name != "" {
		oac.Name = in.Name
	}
	oac.Description = in.Description
	if in.SigningProtocol != "" {
		oac.SigningProtocol = in.SigningProtocol
	}
	if in.SigningBehavior != "" {
		oac.SigningBehavior = in.SigningBehavior
	}
	if in.OriginType != "" {
		oac.OriginType = in.OriginType
	}
	oac.ETag = generateETag()
	if err := p.store.UpdateOriginAccessControl(oac); err != nil {
		return nil, err
	}
	type response struct {
		XMLName xml.Name `xml:"OriginAccessControl"`
		Id      string   `xml:"Id"`
		Config  struct {
			Name            string `xml:"Name"`
			Description     string `xml:"Description"`
			SigningProtocol string `xml:"SigningProtocol"`
			SigningBehavior string `xml:"SigningBehavior"`
			OriginType      string `xml:"OriginAccessControlOriginType"`
		} `xml:"OriginAccessControlConfig"`
	}
	var resp response
	resp.Id = oac.ID
	resp.Config.Name = oac.Name
	resp.Config.Description = oac.Description
	resp.Config.SigningProtocol = oac.SigningProtocol
	resp.Config.SigningBehavior = oac.SigningBehavior
	resp.Config.OriginType = oac.OriginType
	return xmlRespWithHeader(http.StatusOK, resp, map[string]string{"ETag": oac.ETag})
}

func (p *Provider) deleteOriginAccessControl(req *http.Request) (*plugin.Response, error) {
	id := oacIDFromPath(req.URL.Path)
	if id == "" {
		return cfError("InvalidInput", "missing origin access control ID", http.StatusBadRequest), nil
	}
	if err := p.store.DeleteOriginAccessControl(id); err != nil {
		if err == ErrOriginAccessControlNotFound {
			return cfError("NoSuchOriginAccessControl", "origin access control not found", http.StatusNotFound), nil
		}
		return nil, err
	}
	return &plugin.Response{StatusCode: http.StatusNoContent, ContentType: "application/xml"}, nil
}

func (p *Provider) getOriginAccessControlConfig(req *http.Request) (*plugin.Response, error) {
	id := oacIDFromPath(req.URL.Path)
	if id == "" {
		return cfError("InvalidInput", "missing origin access control ID", http.StatusBadRequest), nil
	}
	oac, err := p.store.GetOriginAccessControl(id)
	if err != nil {
		if err == ErrOriginAccessControlNotFound {
			return cfError("NoSuchOriginAccessControl", "origin access control not found", http.StatusNotFound), nil
		}
		return nil, err
	}
	type response struct {
		XMLName         xml.Name `xml:"OriginAccessControlConfig"`
		Name            string   `xml:"Name"`
		Description     string   `xml:"Description"`
		SigningProtocol string   `xml:"SigningProtocol"`
		SigningBehavior string   `xml:"SigningBehavior"`
		OriginType      string   `xml:"OriginAccessControlOriginType"`
	}
	resp := response{
		Name:            oac.Name,
		Description:     oac.Description,
		SigningProtocol: oac.SigningProtocol,
		SigningBehavior: oac.SigningBehavior,
		OriginType:      oac.OriginType,
	}
	return xmlRespWithHeader(http.StatusOK, resp, map[string]string{"ETag": oac.ETag})
}

// --- Function operations ---

func (p *Provider) createFunction(req *http.Request) (*plugin.Response, error) {
	type input struct {
		XMLName xml.Name `xml:"CreateFunctionRequest"`
		Name    string   `xml:"Name"`
		Comment string   `xml:"FunctionConfig>Comment"`
		Runtime string   `xml:"FunctionConfig>Runtime"`
		Code    string   `xml:"FunctionCode"`
	}
	var in input
	data, _ := readBody(req)
	_ = xml.Unmarshal(data, &in)
	if in.Name == "" {
		return cfError("InvalidArgument", "Name is required", http.StatusBadRequest), nil
	}
	runtime := in.Runtime
	if runtime == "" {
		runtime = "cloudfront-js-2.0"
	}
	etag := generateETag()
	now := time.Now()
	fn := &Function{
		Name:      in.Name,
		ETag:      etag,
		ARN:       fmt.Sprintf("arn:aws:cloudfront::%s:function/%s", defaultAccountID, in.Name),
		Status:    "UNASSOCIATED",
		Stage:     "DEVELOPMENT",
		Runtime:   runtime,
		Code:      in.Code,
		Comment:   in.Comment,
		CreatedAt: now,
		UpdatedAt: now,
	}
	if err := p.store.CreateFunction(fn); err != nil {
		return cfError("FunctionAlreadyExists", err.Error(), http.StatusConflict), nil
	}
	type response struct {
		XMLName      xml.Name `xml:"FunctionSummary"`
		Name         string   `xml:"Name"`
		ARN          string   `xml:"FunctionMetadata>FunctionARN"`
		Stage        string   `xml:"FunctionMetadata>Stage"`
		Status       string   `xml:"Status"`
		Comment      string   `xml:"FunctionConfig>Comment"`
		Runtime      string   `xml:"FunctionConfig>Runtime"`
		LastModified string   `xml:"FunctionMetadata>LastModifiedTime"`
	}
	resp := response{
		Name:         fn.Name,
		ARN:          fn.ARN,
		Stage:        fn.Stage,
		Status:       fn.Status,
		Comment:      fn.Comment,
		Runtime:      fn.Runtime,
		LastModified: fn.UpdatedAt.UTC().Format(time.RFC3339),
	}
	return xmlRespWithHeader(http.StatusCreated, resp, map[string]string{"ETag": etag})
}

func (p *Provider) describeFunction(req *http.Request) (*plugin.Response, error) {
	name := functionNameFromPath(req.URL.Path)
	if name == "" {
		return cfError("InvalidInput", "missing function name", http.StatusBadRequest), nil
	}
	fn, err := p.store.GetFunction(name)
	if err != nil {
		if err == ErrFunctionNotFound {
			return cfError("NoSuchFunctionExists", "function not found", http.StatusNotFound), nil
		}
		return nil, err
	}
	type response struct {
		XMLName      xml.Name `xml:"FunctionSummary"`
		Name         string   `xml:"Name"`
		ARN          string   `xml:"FunctionMetadata>FunctionARN"`
		Stage        string   `xml:"FunctionMetadata>Stage"`
		Status       string   `xml:"Status"`
		Comment      string   `xml:"FunctionConfig>Comment"`
		Runtime      string   `xml:"FunctionConfig>Runtime"`
		LastModified string   `xml:"FunctionMetadata>LastModifiedTime"`
	}
	resp := response{
		Name:         fn.Name,
		ARN:          fn.ARN,
		Stage:        fn.Stage,
		Status:       fn.Status,
		Comment:      fn.Comment,
		Runtime:      fn.Runtime,
		LastModified: fn.UpdatedAt.UTC().Format(time.RFC3339),
	}
	return xmlRespWithHeader(http.StatusOK, resp, map[string]string{"ETag": fn.ETag})
}

func (p *Provider) listFunctions(_ *http.Request) (*plugin.Response, error) {
	fns, err := p.store.ListFunctions()
	if err != nil {
		return nil, err
	}
	type item struct {
		Name         string `xml:"Name"`
		ARN          string `xml:"FunctionMetadata>FunctionARN"`
		Stage        string `xml:"FunctionMetadata>Stage"`
		Status       string `xml:"Status"`
		Comment      string `xml:"FunctionConfig>Comment"`
		Runtime      string `xml:"FunctionConfig>Runtime"`
		LastModified string `xml:"FunctionMetadata>LastModifiedTime"`
	}
	type response struct {
		XMLName     xml.Name `xml:"FunctionList"`
		IsTruncated bool     `xml:"IsTruncated"`
		MaxItems    int      `xml:"MaxItems"`
		Quantity    int      `xml:"Items>Quantity"`
		Items       []item   `xml:"Items>FunctionSummary"`
	}
	var items []item
	for _, fn := range fns {
		items = append(items, item{
			Name:         fn.Name,
			ARN:          fn.ARN,
			Stage:        fn.Stage,
			Status:       fn.Status,
			Comment:      fn.Comment,
			Runtime:      fn.Runtime,
			LastModified: fn.UpdatedAt.UTC().Format(time.RFC3339),
		})
	}
	resp := response{
		IsTruncated: false,
		MaxItems:    100,
		Quantity:    len(items),
		Items:       items,
	}
	return xmlResp(http.StatusOK, resp)
}

func (p *Provider) updateFunction(req *http.Request) (*plugin.Response, error) {
	name := functionNameFromPath(req.URL.Path)
	if name == "" {
		return cfError("InvalidInput", "missing function name", http.StatusBadRequest), nil
	}
	fn, err := p.store.GetFunction(name)
	if err != nil {
		if err == ErrFunctionNotFound {
			return cfError("NoSuchFunctionExists", "function not found", http.StatusNotFound), nil
		}
		return nil, err
	}
	type input struct {
		Comment string `xml:"FunctionConfig>Comment"`
		Runtime string `xml:"FunctionConfig>Runtime"`
		Code    string `xml:"FunctionCode"`
	}
	var in input
	data, _ := readBody(req)
	_ = xml.Unmarshal(data, &in)
	fn.Comment = in.Comment
	if in.Runtime != "" {
		fn.Runtime = in.Runtime
	}
	if in.Code != "" {
		fn.Code = in.Code
	}
	fn.ETag = generateETag()
	fn.UpdatedAt = time.Now()
	if err := p.store.UpdateFunction(fn); err != nil {
		return nil, err
	}
	type response struct {
		XMLName      xml.Name `xml:"FunctionSummary"`
		Name         string   `xml:"Name"`
		ARN          string   `xml:"FunctionMetadata>FunctionARN"`
		Stage        string   `xml:"FunctionMetadata>Stage"`
		Status       string   `xml:"Status"`
		Comment      string   `xml:"FunctionConfig>Comment"`
		Runtime      string   `xml:"FunctionConfig>Runtime"`
		LastModified string   `xml:"FunctionMetadata>LastModifiedTime"`
	}
	resp := response{
		Name:         fn.Name,
		ARN:          fn.ARN,
		Stage:        fn.Stage,
		Status:       fn.Status,
		Comment:      fn.Comment,
		Runtime:      fn.Runtime,
		LastModified: fn.UpdatedAt.UTC().Format(time.RFC3339),
	}
	return xmlRespWithHeader(http.StatusOK, resp, map[string]string{"ETag": fn.ETag})
}

func (p *Provider) deleteFunction(req *http.Request) (*plugin.Response, error) {
	name := functionNameFromPath(req.URL.Path)
	if name == "" {
		return cfError("InvalidInput", "missing function name", http.StatusBadRequest), nil
	}
	if err := p.store.DeleteFunction(name); err != nil {
		if err == ErrFunctionNotFound {
			return cfError("NoSuchFunctionExists", "function not found", http.StatusNotFound), nil
		}
		return nil, err
	}
	return &plugin.Response{StatusCode: http.StatusNoContent, ContentType: "application/xml"}, nil
}

func (p *Provider) publishFunction(req *http.Request) (*plugin.Response, error) {
	name := functionNameFromPath(req.URL.Path)
	if name == "" {
		return cfError("InvalidInput", "missing function name", http.StatusBadRequest), nil
	}
	fn, err := p.store.GetFunction(name)
	if err != nil {
		if err == ErrFunctionNotFound {
			return cfError("NoSuchFunctionExists", "function not found", http.StatusNotFound), nil
		}
		return nil, err
	}
	fn.Stage = "LIVE"
	fn.ETag = generateETag()
	fn.UpdatedAt = time.Now()
	if err := p.store.UpdateFunction(fn); err != nil {
		return nil, err
	}
	type response struct {
		XMLName      xml.Name `xml:"FunctionSummary"`
		Name         string   `xml:"Name"`
		ARN          string   `xml:"FunctionMetadata>FunctionARN"`
		Stage        string   `xml:"FunctionMetadata>Stage"`
		Status       string   `xml:"Status"`
		Comment      string   `xml:"FunctionConfig>Comment"`
		Runtime      string   `xml:"FunctionConfig>Runtime"`
		LastModified string   `xml:"FunctionMetadata>LastModifiedTime"`
	}
	resp := response{
		Name:         fn.Name,
		ARN:          fn.ARN,
		Stage:        fn.Stage,
		Status:       fn.Status,
		Comment:      fn.Comment,
		Runtime:      fn.Runtime,
		LastModified: fn.UpdatedAt.UTC().Format(time.RFC3339),
	}
	return xmlRespWithHeader(http.StatusCreated, resp, map[string]string{"ETag": fn.ETag})
}

func (p *Provider) testFunction(req *http.Request) (*plugin.Response, error) {
	name := functionNameFromPath(req.URL.Path)
	if name == "" {
		return cfError("InvalidInput", "missing function name", http.StatusBadRequest), nil
	}
	_, err := p.store.GetFunction(name)
	if err != nil {
		if err == ErrFunctionNotFound {
			return cfError("NoSuchFunctionExists", "function not found", http.StatusNotFound), nil
		}
		return nil, err
	}
	type response struct {
		XMLName    xml.Name `xml:"TestFunctionResult"`
		TestResult struct {
			ComputeUtilization string `xml:"ComputeUtilization"`
			FunctionOutput     string `xml:"FunctionOutput"`
			FunctionError      string `xml:"FunctionError"`
		} `xml:"TestResult"`
	}
	var resp response
	resp.TestResult.ComputeUtilization = "0"
	resp.TestResult.FunctionOutput = "{}"
	return xmlResp(http.StatusOK, resp)
}

// --- Invalidation operations ---

func (p *Provider) createInvalidation(req *http.Request) (*plugin.Response, error) {
	distID := distIDFromPath(req.URL.Path)
	if distID == "" {
		return cfError("InvalidInput", "missing distribution ID", http.StatusBadRequest), nil
	}
	// check distribution exists
	_, err := p.store.GetDistribution(distID)
	if err != nil {
		if err == ErrDistributionNotFound {
			return cfError("NoSuchDistribution", "distribution not found", http.StatusNotFound), nil
		}
		return nil, err
	}
	type pathsInput struct {
		Items    []string `xml:"Items>Path"`
		Quantity int      `xml:"Quantity"`
	}
	type input struct {
		XMLName         xml.Name   `xml:"InvalidationBatch"`
		Paths           pathsInput `xml:"Paths"`
		CallerReference string     `xml:"CallerReference"`
	}
	var in input
	data, _ := readBody(req)
	_ = xml.Unmarshal(data, &in)

	pathsJSON := "[]"
	if len(in.Paths.Items) > 0 {
		var sb strings.Builder
		sb.WriteString("[")
		for i, path := range in.Paths.Items {
			if i > 0 {
				sb.WriteString(",")
			}
			sb.WriteString(`"`)
			sb.WriteString(path)
			sb.WriteString(`"`)
		}
		sb.WriteString("]")
		pathsJSON = sb.String()
	}

	id := "I" + generateID()
	now := time.Now()
	inv := &Invalidation{
		ID:             id,
		DistributionID: distID,
		Status:         "Completed",
		Paths:          pathsJSON,
		CreatedAt:      now,
	}
	if err := p.store.CreateInvalidation(inv); err != nil {
		return nil, err
	}
	type response struct {
		XMLName    xml.Name `xml:"Invalidation"`
		Id         string   `xml:"Id"`
		Status     string   `xml:"Status"`
		CreateTime string   `xml:"CreateTime"`
		BatchPaths struct {
			Quantity int      `xml:"Paths>Quantity"`
			Items    []string `xml:"Paths>Items>Path"`
		} `xml:"InvalidationBatch"`
	}
	var resp response
	resp.Id = inv.ID
	resp.Status = inv.Status
	resp.CreateTime = now.UTC().Format(time.RFC3339)
	resp.BatchPaths.Quantity = len(in.Paths.Items)
	resp.BatchPaths.Items = in.Paths.Items
	return xmlRespWithHeader(http.StatusCreated, resp, map[string]string{
		"Location": fmt.Sprintf("/2020-05-31/distribution/%s/invalidation/%s", distID, id),
	})
}

func (p *Provider) getInvalidation(req *http.Request) (*plugin.Response, error) {
	distID := distIDFromPath(req.URL.Path)
	invID := invalidationIDFromPath(req.URL.Path)
	if distID == "" || invID == "" {
		return cfError("InvalidInput", "missing distribution ID or invalidation ID", http.StatusBadRequest), nil
	}
	inv, err := p.store.GetInvalidation(distID, invID)
	if err != nil {
		if err == ErrInvalidationNotFound {
			return cfError("NoSuchInvalidation", "invalidation not found", http.StatusNotFound), nil
		}
		return nil, err
	}
	type response struct {
		XMLName    xml.Name `xml:"Invalidation"`
		Id         string   `xml:"Id"`
		Status     string   `xml:"Status"`
		CreateTime string   `xml:"CreateTime"`
	}
	resp := response{
		Id:         inv.ID,
		Status:     inv.Status,
		CreateTime: inv.CreatedAt.UTC().Format(time.RFC3339),
	}
	return xmlResp(http.StatusOK, resp)
}

func (p *Provider) listInvalidations(req *http.Request) (*plugin.Response, error) {
	distID := distIDFromPath(req.URL.Path)
	if distID == "" {
		return cfError("InvalidInput", "missing distribution ID", http.StatusBadRequest), nil
	}
	invs, err := p.store.ListInvalidations(distID)
	if err != nil {
		return nil, err
	}
	type item struct {
		Id         string `xml:"Id"`
		Status     string `xml:"Status"`
		CreateTime string `xml:"CreateTime"`
	}
	type response struct {
		XMLName     xml.Name `xml:"InvalidationList"`
		IsTruncated bool     `xml:"IsTruncated"`
		MaxItems    int      `xml:"MaxItems"`
		Quantity    int      `xml:"Items>Quantity"`
		Items       []item   `xml:"Items>InvalidationSummary"`
	}
	var items []item
	for _, inv := range invs {
		items = append(items, item{
			Id:         inv.ID,
			Status:     inv.Status,
			CreateTime: inv.CreatedAt.UTC().Format(time.RFC3339),
		})
	}
	resp := response{
		IsTruncated: false,
		MaxItems:    100,
		Quantity:    len(items),
		Items:       items,
	}
	return xmlResp(http.StatusOK, resp)
}

// --- Tag operations ---

func (p *Provider) tagResource(req *http.Request) (*plugin.Response, error) {
	arn := req.URL.Query().Get("Resource")
	if arn == "" {
		return cfError("InvalidArgument", "Resource ARN is required", http.StatusBadRequest), nil
	}
	type tag struct {
		Key   string `xml:"Key"`
		Value string `xml:"Value"`
	}
	type input struct {
		XMLName xml.Name `xml:"Tags"`
		Items   []tag    `xml:"Items>Tag"`
	}
	var in input
	data, _ := readBody(req)
	_ = xml.Unmarshal(data, &in)
	tags := make(map[string]string, len(in.Items))
	for _, t := range in.Items {
		tags[t.Key] = t.Value
	}
	if err := p.store.PutTags(arn, tags); err != nil {
		return nil, err
	}
	return &plugin.Response{StatusCode: http.StatusNoContent, ContentType: "application/xml"}, nil
}

func (p *Provider) untagResource(req *http.Request) (*plugin.Response, error) {
	arn := req.URL.Query().Get("Resource")
	if arn == "" {
		return cfError("InvalidArgument", "Resource ARN is required", http.StatusBadRequest), nil
	}
	type input struct {
		XMLName xml.Name `xml:"TagKeys"`
		Keys    []string `xml:"Items>Key"`
	}
	var in input
	data, _ := readBody(req)
	_ = xml.Unmarshal(data, &in)
	if err := p.store.RemoveTags(arn, in.Keys); err != nil {
		return nil, err
	}
	return &plugin.Response{StatusCode: http.StatusNoContent, ContentType: "application/xml"}, nil
}

func (p *Provider) listTagsForResource(req *http.Request) (*plugin.Response, error) {
	arn := req.URL.Query().Get("Resource")
	if arn == "" {
		return cfError("InvalidArgument", "Resource ARN is required", http.StatusBadRequest), nil
	}
	tags, err := p.store.ListTags(arn)
	if err != nil {
		return nil, err
	}
	type tag struct {
		Key   string `xml:"Key"`
		Value string `xml:"Value"`
	}
	type response struct {
		XMLName xml.Name `xml:"Tags"`
		Items   []tag    `xml:"Items>Tag"`
	}
	var items []tag
	for k, v := range tags {
		items = append(items, tag{Key: k, Value: v})
	}
	resp := response{Items: items}
	return xmlResp(http.StatusOK, resp)
}

// --- body reader ---

func readBody(req *http.Request) ([]byte, error) {
	if req.Body == nil {
		return nil, nil
	}
	defer func() { _ = req.Body.Close() }()
	buf := make([]byte, 0, 512)
	tmp := make([]byte, 512)
	for {
		n, err := req.Body.Read(tmp)
		if n > 0 {
			buf = append(buf, tmp[:n]...)
		}
		if err != nil {
			break
		}
	}
	return buf, nil
}
