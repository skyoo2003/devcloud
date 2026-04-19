// SPDX-License-Identifier: Apache-2.0

// internal/services/wafv2/provider.go
package wafv2

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
)

const wafv2ContentType = "application/x-amz-json-1.1"

// Provider implements the WAF_20190729 service.
type Provider struct {
	store *Store
}

func (p *Provider) ServiceID() string             { return "wafv2" }
func (p *Provider) ServiceName() string           { return "WAF_20190729" }
func (p *Provider) Protocol() plugin.ProtocolType { return plugin.ProtocolJSON11 }

func (p *Provider) Init(cfg plugin.PluginConfig) error {
	dataDir := cfg.DataDir
	if dataDir == "" {
		dataDir = "."
	}
	var err error
	p.store, err = NewStore(filepath.Join(dataDir, "wafv2"))
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
		return wafError("SerializationException", "failed to read body", http.StatusBadRequest), nil
	}
	var params map[string]any
	if len(body) > 0 {
		if err := json.Unmarshal(body, &params); err != nil {
			return wafError("SerializationException", "invalid JSON", http.StatusBadRequest), nil
		}
	} else {
		params = map[string]any{}
	}

	action := op
	if action == "" {
		target := req.Header.Get("X-Amz-Target")
		if idx := strings.LastIndex(target, "."); idx >= 0 {
			action = target[idx+1:]
		}
	}

	switch action {
	// WebACL
	case "CreateWebACL":
		return p.createWebACL(params)
	case "GetWebACL":
		return p.getWebACL(params)
	case "ListWebACLs":
		return p.listWebACLs(params)
	case "UpdateWebACL":
		return p.updateWebACL(params)
	case "DeleteWebACL":
		return p.deleteWebACL(params)
	case "GetWebACLForResource":
		return p.getWebACLForResource(params)
	case "AssociateWebACL":
		return p.associateWebACL(params)
	case "DisassociateWebACL":
		return p.disassociateWebACL(params)
	case "ListResourcesForWebACL":
		return p.listResourcesForWebACL(params)
	// IPSet
	case "CreateIPSet":
		return p.createIPSet(params)
	case "GetIPSet":
		return p.getIPSet(params)
	case "ListIPSets":
		return p.listIPSets(params)
	case "UpdateIPSet":
		return p.updateIPSet(params)
	case "DeleteIPSet":
		return p.deleteIPSet(params)
	// RegexPatternSet
	case "CreateRegexPatternSet":
		return p.createRegexPatternSet(params)
	case "GetRegexPatternSet":
		return p.getRegexPatternSet(params)
	case "ListRegexPatternSets":
		return p.listRegexPatternSets(params)
	case "UpdateRegexPatternSet":
		return p.updateRegexPatternSet(params)
	case "DeleteRegexPatternSet":
		return p.deleteRegexPatternSet(params)
	// RuleGroup
	case "CreateRuleGroup":
		return p.createRuleGroup(params)
	case "GetRuleGroup":
		return p.getRuleGroup(params)
	case "ListRuleGroups":
		return p.listRuleGroups(params)
	case "UpdateRuleGroup":
		return p.updateRuleGroup(params)
	case "DeleteRuleGroup":
		return p.deleteRuleGroup(params)
	// APIKey
	case "CreateAPIKey":
		return p.createAPIKey(params)
	case "ListAPIKeys":
		return p.listAPIKeys(params)
	case "DeleteAPIKey":
		return p.deleteAPIKey(params)
	case "GetDecryptedAPIKey":
		return p.getDecryptedAPIKey(params)
	// Capacity
	case "CheckCapacity":
		return shared.JSONResponse(http.StatusOK, map[string]any{"Capacity": 10})
	// Logging
	case "PutLoggingConfiguration":
		return p.putLoggingConfiguration(params)
	case "GetLoggingConfiguration":
		return p.getLoggingConfiguration(params)
	case "ListLoggingConfigurations":
		return p.listLoggingConfigurations(params)
	case "DeleteLoggingConfiguration":
		return p.deleteLoggingConfiguration(params)
	// Permission policy
	case "PutPermissionPolicy":
		return p.putPermissionPolicy(params)
	case "GetPermissionPolicy":
		return p.getPermissionPolicy(params)
	case "DeletePermissionPolicy":
		return p.deletePermissionPolicy(params)
	// Tags
	case "TagResource":
		return p.tagResource(params)
	case "UntagResource":
		return p.untagResource(params)
	case "ListTagsForResource":
		return p.listTagsForResource(params)
	// Managed rule groups (read-only stubs)
	case "DescribeManagedRuleGroup":
		return shared.JSONResponse(http.StatusOK, map[string]any{
			"Capacity":        int64(100),
			"Rules":           []any{},
			"LabelNamespace":  "",
			"AvailableLabels": []any{},
			"ConsumedLabels":  []any{},
		})
	case "DescribeAllManagedProducts":
		return shared.JSONResponse(http.StatusOK, map[string]any{"ManagedProducts": []any{}})
	case "DescribeManagedProductsByVendor":
		return shared.JSONResponse(http.StatusOK, map[string]any{"ManagedProducts": []any{}})
	case "ListAvailableManagedRuleGroups":
		return shared.JSONResponse(http.StatusOK, map[string]any{"ManagedRuleGroups": []any{}})
	case "ListAvailableManagedRuleGroupVersions":
		return shared.JSONResponse(http.StatusOK, map[string]any{"Versions": []any{}, "CurrentDefaultVersion": ""})
	// Sampling
	case "GetSampledRequests":
		return shared.JSONResponse(http.StatusOK, map[string]any{
			"SampledRequests": []any{},
			"PopulationSize":  int64(0),
			"TimeWindow":      map[string]any{},
		})
	// Rate-based
	case "GetRateBasedStatementManagedKeys":
		return shared.JSONResponse(http.StatusOK, map[string]any{
			"ManagedKeysIPV4": map[string]any{"IPAddressVersion": "IPV4", "Addresses": []any{}},
			"ManagedKeysIPV6": map[string]any{"IPAddressVersion": "IPV6", "Addresses": []any{}},
		})
	// Managed rule sets
	case "GetManagedRuleSet":
		return shared.JSONResponse(http.StatusOK, map[string]any{"ManagedRuleSet": map[string]any{}, "LockToken": ""})
	case "ListManagedRuleSets":
		return shared.JSONResponse(http.StatusOK, map[string]any{"ManagedRuleSets": []any{}})
	case "PutManagedRuleSetVersions":
		return shared.JSONResponse(http.StatusOK, map[string]any{"NextLockToken": shared.GenerateID("", 32)})
	case "UpdateManagedRuleSetVersionExpiryDate":
		return shared.JSONResponse(http.StatusOK, map[string]any{"ExpiringVersion": "", "ExpiryTimestamp": int64(0), "NextLockToken": shared.GenerateID("", 32)})
	// Firewall manager
	case "DeleteFirewallManagerRuleGroups":
		return shared.JSONResponse(http.StatusOK, map[string]any{"NextWebACLLockToken": shared.GenerateID("", 32)})
	default:
		return wafError("InvalidAction", fmt.Sprintf("unknown action: %s", action), http.StatusBadRequest), nil
	}
}

func (p *Provider) ListResources(_ context.Context) ([]plugin.Resource, error) {
	acls, err := p.store.ListWebACLs("REGIONAL")
	if err != nil {
		return nil, err
	}
	res := make([]plugin.Resource, 0, len(acls))
	for _, a := range acls {
		res = append(res, plugin.Resource{Type: "wafv2-web-acl", ID: a.ID, Name: a.Name})
	}
	return res, nil
}

func (p *Provider) GetMetrics(_ context.Context) (*plugin.ServiceMetrics, error) {
	return &plugin.ServiceMetrics{}, nil
}

// --- WebACL ---

func (p *Provider) createWebACL(params map[string]any) (*plugin.Response, error) {
	name, _ := params["Name"].(string)
	scope, _ := params["Scope"].(string)
	if name == "" {
		return wafError("WAFInvalidParameterException", "Name is required", http.StatusBadRequest), nil
	}
	if scope == "" {
		scope = "REGIONAL"
	}
	description, _ := params["Description"].(string)

	defaultAction := "{\"Allow\":{}}"
	if da, ok := params["DefaultAction"]; ok {
		b, _ := json.Marshal(da)
		defaultAction = string(b)
	}
	rules := "[]"
	if r, ok := params["Rules"]; ok {
		b, _ := json.Marshal(r)
		rules = string(b)
	}
	visConfig := "{}"
	if vc, ok := params["VisibilityConfig"]; ok {
		b, _ := json.Marshal(vc)
		visConfig = string(b)
	}

	id := shared.GenerateUUID()
	arn := wafARN(scope, "webacl", name, id)
	token := shared.GenerateID("", 32)

	w := &WebACL{
		ID:               id,
		Name:             name,
		ARN:              arn,
		Scope:            scope,
		Description:      description,
		DefaultAction:    defaultAction,
		Rules:            rules,
		VisibilityConfig: visConfig,
		LockToken:        token,
	}
	if err := p.store.CreateWebACL(w); err != nil {
		if isUniqueErr(err) {
			return wafError("WAFDuplicateItemException", "web ACL already exists", http.StatusBadRequest), nil
		}
		return nil, err
	}

	if rawTags, ok := params["Tags"].([]any); ok {
		_ = p.store.tags.AddTags(arn, parseTagList(rawTags))
	}

	return shared.JSONResponse(http.StatusOK, map[string]any{
		"Summary": webACLSummary(w),
	})
}

func (p *Provider) getWebACL(params map[string]any) (*plugin.Response, error) {
	name, _ := params["Name"].(string)
	scope, _ := params["Scope"].(string)
	if name == "" {
		return wafError("WAFInvalidParameterException", "Name is required", http.StatusBadRequest), nil
	}
	if scope == "" {
		scope = "REGIONAL"
	}
	w, err := p.store.GetWebACL(name, scope)
	if err != nil {
		return wafError("WAFNonexistentItemException", "web ACL not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"WebACL":    webACLDetail(w),
		"LockToken": w.LockToken,
	})
}

func (p *Provider) listWebACLs(params map[string]any) (*plugin.Response, error) {
	scope, _ := params["Scope"].(string)
	if scope == "" {
		scope = "REGIONAL"
	}
	acls, err := p.store.ListWebACLs(scope)
	if err != nil {
		return nil, err
	}
	list := make([]map[string]any, 0, len(acls))
	for i := range acls {
		list = append(list, webACLSummary(&acls[i]))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"WebACLs":          list,
		"NextMarker":       "",
		"Summary":          map[string]any{},
		"ResponseMetadata": map[string]any{},
	})
}

func (p *Provider) updateWebACL(params map[string]any) (*plugin.Response, error) {
	name, _ := params["Name"].(string)
	scope, _ := params["Scope"].(string)
	if name == "" {
		return wafError("WAFInvalidParameterException", "Name is required", http.StatusBadRequest), nil
	}
	if scope == "" {
		scope = "REGIONAL"
	}
	fields := map[string]any{}
	if v, ok := params["Description"].(string); ok {
		fields["Description"] = v
	}
	if da, ok := params["DefaultAction"]; ok {
		b, _ := json.Marshal(da)
		fields["DefaultAction"] = string(b)
	}
	if r, ok := params["Rules"]; ok {
		b, _ := json.Marshal(r)
		fields["Rules"] = string(b)
	}
	if vc, ok := params["VisibilityConfig"]; ok {
		b, _ := json.Marshal(vc)
		fields["VisibilityConfig"] = string(b)
	}
	if err := p.store.UpdateWebACL(name, scope, fields); err != nil {
		return wafError("WAFNonexistentItemException", "web ACL not found", http.StatusBadRequest), nil
	}
	token, _ := fields["LockToken"].(string)
	return shared.JSONResponse(http.StatusOK, map[string]any{"NextLockToken": token})
}

func (p *Provider) deleteWebACL(params map[string]any) (*plugin.Response, error) {
	name, _ := params["Name"].(string)
	scope, _ := params["Scope"].(string)
	if name == "" {
		return wafError("WAFInvalidParameterException", "Name is required", http.StatusBadRequest), nil
	}
	if scope == "" {
		scope = "REGIONAL"
	}
	w, err := p.store.GetWebACL(name, scope)
	if err != nil {
		return wafError("WAFNonexistentItemException", "web ACL not found", http.StatusBadRequest), nil
	}
	_ = p.store.tags.DeleteAllTags(w.ARN)
	if err := p.store.DeleteWebACL(name, scope); err != nil {
		return wafError("WAFNonexistentItemException", "web ACL not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"ResponseMetadata": map[string]any{},
		"Summary":          webACLSummary(w),
	})
}

func (p *Provider) getWebACLForResource(params map[string]any) (*plugin.Response, error) {
	resourceARN, _ := params["ResourceArn"].(string)
	if resourceARN == "" {
		return wafError("WAFInvalidParameterException", "ResourceArn is required", http.StatusBadRequest), nil
	}
	webACLARN, err := p.store.GetWebACLForResource(resourceARN)
	if err != nil {
		return nil, err
	}
	if webACLARN == "" {
		return shared.JSONResponse(http.StatusOK, map[string]any{})
	}
	w, err := p.store.GetWebACLByARN(webACLARN)
	if err != nil {
		return shared.JSONResponse(http.StatusOK, map[string]any{})
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"WebACL": webACLDetail(w),
	})
}

func (p *Provider) associateWebACL(params map[string]any) (*plugin.Response, error) {
	resourceARN, _ := params["ResourceArn"].(string)
	webACLARN, _ := params["WebACLArn"].(string)
	if resourceARN == "" || webACLARN == "" {
		return wafError("WAFInvalidParameterException", "ResourceArn and WebACLArn are required", http.StatusBadRequest), nil
	}
	if err := p.store.AssociateWebACL(resourceARN, webACLARN); err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) disassociateWebACL(params map[string]any) (*plugin.Response, error) {
	resourceARN, _ := params["ResourceArn"].(string)
	if resourceARN == "" {
		return wafError("WAFInvalidParameterException", "ResourceArn is required", http.StatusBadRequest), nil
	}
	if err := p.store.DisassociateWebACL(resourceARN); err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) listResourcesForWebACL(params map[string]any) (*plugin.Response, error) {
	webACLARN, _ := params["WebACLArn"].(string)
	if webACLARN == "" {
		return wafError("WAFInvalidParameterException", "WebACLArn is required", http.StatusBadRequest), nil
	}
	arns, err := p.store.ListResourcesForWebACL(webACLARN)
	if err != nil {
		return nil, err
	}
	if arns == nil {
		arns = []string{}
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"ResourceArns": arns})
}

// --- IPSet ---

func (p *Provider) createIPSet(params map[string]any) (*plugin.Response, error) {
	name, _ := params["Name"].(string)
	scope, _ := params["Scope"].(string)
	if name == "" {
		return wafError("WAFInvalidParameterException", "Name is required", http.StatusBadRequest), nil
	}
	if scope == "" {
		scope = "REGIONAL"
	}
	description, _ := params["Description"].(string)
	ipVersion, _ := params["IPAddressVersion"].(string)
	if ipVersion == "" {
		ipVersion = "IPV4"
	}
	addresses := "[]"
	if a, ok := params["Addresses"]; ok {
		b, _ := json.Marshal(a)
		addresses = string(b)
	}

	id := shared.GenerateUUID()
	arn := wafARN(scope, "ipset", name, id)
	token := shared.GenerateID("", 32)

	ip := &IPSet{
		ID:          id,
		Name:        name,
		ARN:         arn,
		Scope:       scope,
		Description: description,
		IPVersion:   ipVersion,
		Addresses:   addresses,
		LockToken:   token,
	}
	if err := p.store.CreateIPSet(ip); err != nil {
		if isUniqueErr(err) {
			return wafError("WAFDuplicateItemException", "IP set already exists", http.StatusBadRequest), nil
		}
		return nil, err
	}

	if rawTags, ok := params["Tags"].([]any); ok {
		_ = p.store.tags.AddTags(arn, parseTagList(rawTags))
	}

	return shared.JSONResponse(http.StatusOK, map[string]any{
		"Summary": ipSetSummary(ip),
	})
}

func (p *Provider) getIPSet(params map[string]any) (*plugin.Response, error) {
	name, _ := params["Name"].(string)
	scope, _ := params["Scope"].(string)
	if name == "" {
		return wafError("WAFInvalidParameterException", "Name is required", http.StatusBadRequest), nil
	}
	if scope == "" {
		scope = "REGIONAL"
	}
	ip, err := p.store.GetIPSet(name, scope)
	if err != nil {
		return wafError("WAFNonexistentItemException", "IP set not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"IPSet":     ipSetDetail(ip),
		"LockToken": ip.LockToken,
	})
}

func (p *Provider) listIPSets(params map[string]any) (*plugin.Response, error) {
	scope, _ := params["Scope"].(string)
	if scope == "" {
		scope = "REGIONAL"
	}
	sets, err := p.store.ListIPSets(scope)
	if err != nil {
		return nil, err
	}
	list := make([]map[string]any, 0, len(sets))
	for i := range sets {
		list = append(list, ipSetSummary(&sets[i]))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"IPSets": list})
}

func (p *Provider) updateIPSet(params map[string]any) (*plugin.Response, error) {
	name, _ := params["Name"].(string)
	scope, _ := params["Scope"].(string)
	if name == "" {
		return wafError("WAFInvalidParameterException", "Name is required", http.StatusBadRequest), nil
	}
	if scope == "" {
		scope = "REGIONAL"
	}
	fields := map[string]any{}
	if v, ok := params["Description"].(string); ok {
		fields["Description"] = v
	}
	if a, ok := params["Addresses"]; ok {
		b, _ := json.Marshal(a)
		fields["Addresses"] = string(b)
	}
	if err := p.store.UpdateIPSet(name, scope, fields); err != nil {
		return wafError("WAFNonexistentItemException", "IP set not found", http.StatusBadRequest), nil
	}
	token, _ := fields["LockToken"].(string)
	return shared.JSONResponse(http.StatusOK, map[string]any{"NextLockToken": token})
}

func (p *Provider) deleteIPSet(params map[string]any) (*plugin.Response, error) {
	name, _ := params["Name"].(string)
	scope, _ := params["Scope"].(string)
	if name == "" {
		return wafError("WAFInvalidParameterException", "Name is required", http.StatusBadRequest), nil
	}
	if scope == "" {
		scope = "REGIONAL"
	}
	ip, err := p.store.GetIPSet(name, scope)
	if err != nil {
		return wafError("WAFNonexistentItemException", "IP set not found", http.StatusBadRequest), nil
	}
	_ = p.store.tags.DeleteAllTags(ip.ARN)
	if err := p.store.DeleteIPSet(name, scope); err != nil {
		return wafError("WAFNonexistentItemException", "IP set not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

// --- RegexPatternSet ---

func (p *Provider) createRegexPatternSet(params map[string]any) (*plugin.Response, error) {
	name, _ := params["Name"].(string)
	scope, _ := params["Scope"].(string)
	if name == "" {
		return wafError("WAFInvalidParameterException", "Name is required", http.StatusBadRequest), nil
	}
	if scope == "" {
		scope = "REGIONAL"
	}
	description, _ := params["Description"].(string)
	patterns := "[]"
	if ps, ok := params["RegularExpressionList"]; ok {
		b, _ := json.Marshal(ps)
		patterns = string(b)
	}

	id := shared.GenerateUUID()
	arn := wafARN(scope, "regexpatternset", name, id)
	token := shared.GenerateID("", 32)

	r := &RegexPatternSet{
		ID:          id,
		Name:        name,
		ARN:         arn,
		Scope:       scope,
		Description: description,
		Patterns:    patterns,
		LockToken:   token,
	}
	if err := p.store.CreateRegexPatternSet(r); err != nil {
		if isUniqueErr(err) {
			return wafError("WAFDuplicateItemException", "regex pattern set already exists", http.StatusBadRequest), nil
		}
		return nil, err
	}

	if rawTags, ok := params["Tags"].([]any); ok {
		_ = p.store.tags.AddTags(arn, parseTagList(rawTags))
	}

	return shared.JSONResponse(http.StatusOK, map[string]any{
		"Summary": regexPatternSetSummary(r),
	})
}

func (p *Provider) getRegexPatternSet(params map[string]any) (*plugin.Response, error) {
	name, _ := params["Name"].(string)
	scope, _ := params["Scope"].(string)
	if name == "" {
		return wafError("WAFInvalidParameterException", "Name is required", http.StatusBadRequest), nil
	}
	if scope == "" {
		scope = "REGIONAL"
	}
	r, err := p.store.GetRegexPatternSet(name, scope)
	if err != nil {
		return wafError("WAFNonexistentItemException", "regex pattern set not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"RegexPatternSet": regexPatternSetDetail(r),
		"LockToken":       r.LockToken,
	})
}

func (p *Provider) listRegexPatternSets(params map[string]any) (*plugin.Response, error) {
	scope, _ := params["Scope"].(string)
	if scope == "" {
		scope = "REGIONAL"
	}
	sets, err := p.store.ListRegexPatternSets(scope)
	if err != nil {
		return nil, err
	}
	list := make([]map[string]any, 0, len(sets))
	for i := range sets {
		list = append(list, regexPatternSetSummary(&sets[i]))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"RegexPatternSets": list})
}

func (p *Provider) updateRegexPatternSet(params map[string]any) (*plugin.Response, error) {
	name, _ := params["Name"].(string)
	scope, _ := params["Scope"].(string)
	if name == "" {
		return wafError("WAFInvalidParameterException", "Name is required", http.StatusBadRequest), nil
	}
	if scope == "" {
		scope = "REGIONAL"
	}
	fields := map[string]any{}
	if v, ok := params["Description"].(string); ok {
		fields["Description"] = v
	}
	if ps, ok := params["RegularExpressionList"]; ok {
		b, _ := json.Marshal(ps)
		fields["Patterns"] = string(b)
	}
	if err := p.store.UpdateRegexPatternSet(name, scope, fields); err != nil {
		return wafError("WAFNonexistentItemException", "regex pattern set not found", http.StatusBadRequest), nil
	}
	token, _ := fields["LockToken"].(string)
	return shared.JSONResponse(http.StatusOK, map[string]any{"NextLockToken": token})
}

func (p *Provider) deleteRegexPatternSet(params map[string]any) (*plugin.Response, error) {
	name, _ := params["Name"].(string)
	scope, _ := params["Scope"].(string)
	if name == "" {
		return wafError("WAFInvalidParameterException", "Name is required", http.StatusBadRequest), nil
	}
	if scope == "" {
		scope = "REGIONAL"
	}
	r, err := p.store.GetRegexPatternSet(name, scope)
	if err != nil {
		return wafError("WAFNonexistentItemException", "regex pattern set not found", http.StatusBadRequest), nil
	}
	_ = p.store.tags.DeleteAllTags(r.ARN)
	if err := p.store.DeleteRegexPatternSet(name, scope); err != nil {
		return wafError("WAFNonexistentItemException", "regex pattern set not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

// --- RuleGroup ---

func (p *Provider) createRuleGroup(params map[string]any) (*plugin.Response, error) {
	name, _ := params["Name"].(string)
	scope, _ := params["Scope"].(string)
	if name == "" {
		return wafError("WAFInvalidParameterException", "Name is required", http.StatusBadRequest), nil
	}
	if scope == "" {
		scope = "REGIONAL"
	}
	description, _ := params["Description"].(string)
	var capacity int64 = 100
	if c, ok := params["Capacity"].(float64); ok {
		capacity = int64(c)
	}
	rules := "[]"
	if r, ok := params["Rules"]; ok {
		b, _ := json.Marshal(r)
		rules = string(b)
	}
	visConfig := "{}"
	if vc, ok := params["VisibilityConfig"]; ok {
		b, _ := json.Marshal(vc)
		visConfig = string(b)
	}

	id := shared.GenerateUUID()
	arn := wafARN(scope, "rulegroup", name, id)
	token := shared.GenerateID("", 32)

	rg := &RuleGroup{
		ID:               id,
		Name:             name,
		ARN:              arn,
		Scope:            scope,
		Description:      description,
		Capacity:         capacity,
		Rules:            rules,
		VisibilityConfig: visConfig,
		LockToken:        token,
	}
	if err := p.store.CreateRuleGroup(rg); err != nil {
		if isUniqueErr(err) {
			return wafError("WAFDuplicateItemException", "rule group already exists", http.StatusBadRequest), nil
		}
		return nil, err
	}

	if rawTags, ok := params["Tags"].([]any); ok {
		_ = p.store.tags.AddTags(arn, parseTagList(rawTags))
	}

	return shared.JSONResponse(http.StatusOK, map[string]any{
		"Summary": ruleGroupSummary(rg),
	})
}

func (p *Provider) getRuleGroup(params map[string]any) (*plugin.Response, error) {
	name, _ := params["Name"].(string)
	scope, _ := params["Scope"].(string)
	if name == "" {
		return wafError("WAFInvalidParameterException", "Name is required", http.StatusBadRequest), nil
	}
	if scope == "" {
		scope = "REGIONAL"
	}
	rg, err := p.store.GetRuleGroup(name, scope)
	if err != nil {
		return wafError("WAFNonexistentItemException", "rule group not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"RuleGroup": ruleGroupDetail(rg),
		"LockToken": rg.LockToken,
	})
}

func (p *Provider) listRuleGroups(params map[string]any) (*plugin.Response, error) {
	scope, _ := params["Scope"].(string)
	if scope == "" {
		scope = "REGIONAL"
	}
	groups, err := p.store.ListRuleGroups(scope)
	if err != nil {
		return nil, err
	}
	list := make([]map[string]any, 0, len(groups))
	for i := range groups {
		list = append(list, ruleGroupSummary(&groups[i]))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"RuleGroups": list})
}

func (p *Provider) updateRuleGroup(params map[string]any) (*plugin.Response, error) {
	name, _ := params["Name"].(string)
	scope, _ := params["Scope"].(string)
	if name == "" {
		return wafError("WAFInvalidParameterException", "Name is required", http.StatusBadRequest), nil
	}
	if scope == "" {
		scope = "REGIONAL"
	}
	fields := map[string]any{}
	if v, ok := params["Description"].(string); ok {
		fields["Description"] = v
	}
	if r, ok := params["Rules"]; ok {
		b, _ := json.Marshal(r)
		fields["Rules"] = string(b)
	}
	if vc, ok := params["VisibilityConfig"]; ok {
		b, _ := json.Marshal(vc)
		fields["VisibilityConfig"] = string(b)
	}
	if err := p.store.UpdateRuleGroup(name, scope, fields); err != nil {
		return wafError("WAFNonexistentItemException", "rule group not found", http.StatusBadRequest), nil
	}
	token, _ := fields["LockToken"].(string)
	return shared.JSONResponse(http.StatusOK, map[string]any{"NextLockToken": token})
}

func (p *Provider) deleteRuleGroup(params map[string]any) (*plugin.Response, error) {
	name, _ := params["Name"].(string)
	scope, _ := params["Scope"].(string)
	if name == "" {
		return wafError("WAFInvalidParameterException", "Name is required", http.StatusBadRequest), nil
	}
	if scope == "" {
		scope = "REGIONAL"
	}
	rg, err := p.store.GetRuleGroup(name, scope)
	if err != nil {
		return wafError("WAFNonexistentItemException", "rule group not found", http.StatusBadRequest), nil
	}
	_ = p.store.tags.DeleteAllTags(rg.ARN)
	if err := p.store.DeleteRuleGroup(name, scope); err != nil {
		return wafError("WAFNonexistentItemException", "rule group not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

// --- APIKey ---

func (p *Provider) createAPIKey(params map[string]any) (*plugin.Response, error) {
	scope, _ := params["Scope"].(string)
	if scope == "" {
		scope = "REGIONAL"
	}
	tokenDomains := "[]"
	if td, ok := params["TokenDomains"]; ok {
		b, _ := json.Marshal(td)
		tokenDomains = string(b)
	}
	key := shared.GenerateID("", 64)
	k := &APIKey{
		Key:          key,
		Scope:        scope,
		TokenDomains: tokenDomains,
	}
	if err := p.store.CreateAPIKey(k); err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"APIKey": key})
}

func (p *Provider) listAPIKeys(params map[string]any) (*plugin.Response, error) {
	scope, _ := params["Scope"].(string)
	if scope == "" {
		scope = "REGIONAL"
	}
	keys, err := p.store.ListAPIKeys(scope)
	if err != nil {
		return nil, err
	}
	list := make([]map[string]any, 0, len(keys))
	for _, k := range keys {
		var domains []string
		_ = json.Unmarshal([]byte(k.TokenDomains), &domains)
		if domains == nil {
			domains = []string{}
		}
		list = append(list, map[string]any{
			"APIKey":            k.Key,
			"Scope":             k.Scope,
			"TokenDomains":      domains,
			"CreationTimestamp": k.CreatedAt.Unix(),
		})
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"APIKeySummaries": list})
}

func (p *Provider) deleteAPIKey(params map[string]any) (*plugin.Response, error) {
	key, _ := params["APIKey"].(string)
	scope, _ := params["Scope"].(string)
	if key == "" {
		return wafError("WAFInvalidParameterException", "APIKey is required", http.StatusBadRequest), nil
	}
	_ = scope
	if err := p.store.DeleteAPIKey(key); err != nil {
		return wafError("WAFNonexistentItemException", "API key not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) getDecryptedAPIKey(params map[string]any) (*plugin.Response, error) {
	key, _ := params["APIKey"].(string)
	scope, _ := params["Scope"].(string)
	if key == "" {
		return wafError("WAFInvalidParameterException", "APIKey is required", http.StatusBadRequest), nil
	}
	k, err := p.store.GetAPIKey(key)
	if err != nil {
		return wafError("WAFNonexistentItemException", "API key not found", http.StatusBadRequest), nil
	}
	var domains []string
	_ = json.Unmarshal([]byte(k.TokenDomains), &domains)
	if domains == nil {
		domains = []string{}
	}
	_ = scope
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"TokenDomains":      domains,
		"CreationTimestamp": k.CreatedAt.Unix(),
	})
}

// --- Logging ---

func (p *Provider) putLoggingConfiguration(params map[string]any) (*plugin.Response, error) {
	lc, _ := params["LoggingConfiguration"].(map[string]any)
	if lc == nil {
		return wafError("WAFInvalidParameterException", "LoggingConfiguration is required", http.StatusBadRequest), nil
	}
	resourceARN, _ := lc["ResourceArn"].(string)
	if resourceARN == "" {
		return wafError("WAFInvalidParameterException", "ResourceArn is required", http.StatusBadRequest), nil
	}
	destinations := "[]"
	if d, ok := lc["LogDestinationConfigs"]; ok {
		b, _ := json.Marshal(d)
		destinations = string(b)
	}
	redactedFields := "[]"
	if rf, ok := lc["RedactedFields"]; ok {
		b, _ := json.Marshal(rf)
		redactedFields = string(b)
	}
	if err := p.store.PutLoggingConfig(resourceARN, destinations, redactedFields); err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"LoggingConfiguration": buildLoggingConfig(resourceARN, destinations, redactedFields),
	})
}

func (p *Provider) getLoggingConfiguration(params map[string]any) (*plugin.Response, error) {
	resourceARN, _ := params["ResourceArn"].(string)
	if resourceARN == "" {
		return wafError("WAFInvalidParameterException", "ResourceArn is required", http.StatusBadRequest), nil
	}
	destinations, redactedFields, err := p.store.GetLoggingConfig(resourceARN)
	if err != nil {
		return wafError("WAFNonexistentItemException", "logging configuration not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"LoggingConfiguration": buildLoggingConfig(resourceARN, destinations, redactedFields),
	})
}

func (p *Provider) listLoggingConfigurations(params map[string]any) (*plugin.Response, error) {
	configs, err := p.store.ListLoggingConfigs()
	if err != nil {
		return nil, err
	}
	list := make([]map[string]any, 0, len(configs))
	for _, c := range configs {
		list = append(list, buildLoggingConfig(c[0], c[1], c[2]))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"LoggingConfigurations": list})
}

func (p *Provider) deleteLoggingConfiguration(params map[string]any) (*plugin.Response, error) {
	resourceARN, _ := params["ResourceArn"].(string)
	if resourceARN == "" {
		return wafError("WAFInvalidParameterException", "ResourceArn is required", http.StatusBadRequest), nil
	}
	if err := p.store.DeleteLoggingConfig(resourceARN); err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

// --- Permission policy ---

func (p *Provider) putPermissionPolicy(params map[string]any) (*plugin.Response, error) {
	resourceARN, _ := params["ResourceArn"].(string)
	policy, _ := params["Policy"].(string)
	if resourceARN == "" {
		return wafError("WAFInvalidParameterException", "ResourceArn is required", http.StatusBadRequest), nil
	}
	if err := p.store.PutPermissionPolicy(resourceARN, policy); err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) getPermissionPolicy(params map[string]any) (*plugin.Response, error) {
	resourceARN, _ := params["ResourceArn"].(string)
	if resourceARN == "" {
		return wafError("WAFInvalidParameterException", "ResourceArn is required", http.StatusBadRequest), nil
	}
	policy, err := p.store.GetPermissionPolicy(resourceARN)
	if err != nil {
		return wafError("WAFNonexistentItemException", "permission policy not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"Policy": policy})
}

func (p *Provider) deletePermissionPolicy(params map[string]any) (*plugin.Response, error) {
	resourceARN, _ := params["ResourceArn"].(string)
	if resourceARN == "" {
		return wafError("WAFInvalidParameterException", "ResourceArn is required", http.StatusBadRequest), nil
	}
	if err := p.store.DeletePermissionPolicy(resourceARN); err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

// --- Tags ---

func (p *Provider) tagResource(params map[string]any) (*plugin.Response, error) {
	resourceARN, _ := params["ResourceARN"].(string)
	if resourceARN == "" {
		return wafError("WAFInvalidParameterException", "ResourceARN is required", http.StatusBadRequest), nil
	}
	rawTags, _ := params["Tags"].([]any)
	if err := p.store.tags.AddTags(resourceARN, parseTagList(rawTags)); err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) untagResource(params map[string]any) (*plugin.Response, error) {
	resourceARN, _ := params["ResourceARN"].(string)
	if resourceARN == "" {
		return wafError("WAFInvalidParameterException", "ResourceARN is required", http.StatusBadRequest), nil
	}
	rawKeys, _ := params["TagKeys"].([]any)
	keys := make([]string, 0, len(rawKeys))
	for _, k := range rawKeys {
		if s, ok := k.(string); ok {
			keys = append(keys, s)
		}
	}
	if err := p.store.tags.RemoveTags(resourceARN, keys); err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) listTagsForResource(params map[string]any) (*plugin.Response, error) {
	resourceARN, _ := params["ResourceARN"].(string)
	if resourceARN == "" {
		return wafError("WAFInvalidParameterException", "ResourceARN is required", http.StatusBadRequest), nil
	}
	tags, err := p.store.tags.ListTags(resourceARN)
	if err != nil {
		return nil, err
	}
	list := make([]map[string]string, 0, len(tags))
	for k, v := range tags {
		list = append(list, map[string]string{"Key": k, "Value": v})
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"TagInfoForResource": map[string]any{
			"ResourceARN": resourceARN,
			"TagList":     list,
		},
	})
}

// --- helpers ---

func wafARN(scope, resourceType, name, id string) string {
	if scope == "CLOUDFRONT" {
		return fmt.Sprintf("arn:aws:wafv2:us-east-1:%s:global/%s/%s/%s",
			shared.DefaultAccountID, resourceType, name, id)
	}
	return fmt.Sprintf("arn:aws:wafv2:%s:%s:regional/%s/%s/%s",
		shared.DefaultRegion, shared.DefaultAccountID, resourceType, name, id)
}

func wafError(code, msg string, status int) *plugin.Response {
	b, _ := json.Marshal(map[string]string{"__type": code, "message": msg})
	return &plugin.Response{StatusCode: status, Body: b, ContentType: wafv2ContentType}
}

func webACLSummary(w *WebACL) map[string]any {
	return map[string]any{
		"Id":          w.ID,
		"Name":        w.Name,
		"ARN":         w.ARN,
		"Description": w.Description,
		"LockToken":   w.LockToken,
	}
}

func webACLDetail(w *WebACL) map[string]any {
	var defaultAction, rules, visConfig any
	_ = json.Unmarshal([]byte(w.DefaultAction), &defaultAction)
	_ = json.Unmarshal([]byte(w.Rules), &rules)
	_ = json.Unmarshal([]byte(w.VisibilityConfig), &visConfig)
	if rules == nil {
		rules = []any{}
	}
	return map[string]any{
		"Id":                       w.ID,
		"Name":                     w.Name,
		"ARN":                      w.ARN,
		"Description":              w.Description,
		"DefaultAction":            defaultAction,
		"Rules":                    rules,
		"VisibilityConfig":         visConfig,
		"Capacity":                 int64(0),
		"ManagedByFirewallManager": false,
	}
}

func ipSetSummary(ip *IPSet) map[string]any {
	return map[string]any{
		"Id":               ip.ID,
		"Name":             ip.Name,
		"ARN":              ip.ARN,
		"Description":      ip.Description,
		"LockToken":        ip.LockToken,
		"IPAddressVersion": ip.IPVersion,
	}
}

func ipSetDetail(ip *IPSet) map[string]any {
	var addresses []string
	_ = json.Unmarshal([]byte(ip.Addresses), &addresses)
	if addresses == nil {
		addresses = []string{}
	}
	return map[string]any{
		"Id":               ip.ID,
		"Name":             ip.Name,
		"ARN":              ip.ARN,
		"Description":      ip.Description,
		"IPAddressVersion": ip.IPVersion,
		"Addresses":        addresses,
	}
}

func regexPatternSetSummary(r *RegexPatternSet) map[string]any {
	return map[string]any{
		"Id":          r.ID,
		"Name":        r.Name,
		"ARN":         r.ARN,
		"Description": r.Description,
		"LockToken":   r.LockToken,
	}
}

func regexPatternSetDetail(r *RegexPatternSet) map[string]any {
	var patterns []any
	_ = json.Unmarshal([]byte(r.Patterns), &patterns)
	if patterns == nil {
		patterns = []any{}
	}
	return map[string]any{
		"Id":                    r.ID,
		"Name":                  r.Name,
		"ARN":                   r.ARN,
		"Description":           r.Description,
		"RegularExpressionList": patterns,
	}
}

func ruleGroupSummary(rg *RuleGroup) map[string]any {
	return map[string]any{
		"Id":          rg.ID,
		"Name":        rg.Name,
		"ARN":         rg.ARN,
		"Description": rg.Description,
		"LockToken":   rg.LockToken,
		"Capacity":    rg.Capacity,
	}
}

func ruleGroupDetail(rg *RuleGroup) map[string]any {
	var rules, visConfig any
	_ = json.Unmarshal([]byte(rg.Rules), &rules)
	_ = json.Unmarshal([]byte(rg.VisibilityConfig), &visConfig)
	if rules == nil {
		rules = []any{}
	}
	return map[string]any{
		"Id":               rg.ID,
		"Name":             rg.Name,
		"ARN":              rg.ARN,
		"Description":      rg.Description,
		"Capacity":         rg.Capacity,
		"Rules":            rules,
		"VisibilityConfig": visConfig,
	}
}

func buildLoggingConfig(resourceARN, destinations, redactedFields string) map[string]any {
	var dests, rf []any
	_ = json.Unmarshal([]byte(destinations), &dests)
	_ = json.Unmarshal([]byte(redactedFields), &rf)
	if dests == nil {
		dests = []any{}
	}
	if rf == nil {
		rf = []any{}
	}
	return map[string]any{
		"ResourceArn":              resourceARN,
		"LogDestinationConfigs":    dests,
		"RedactedFields":           rf,
		"ManagedByFirewallManager": false,
	}
}

func parseTagList(rawTags []any) map[string]string {
	tags := make(map[string]string)
	for _, t := range rawTags {
		tag, _ := t.(map[string]any)
		k, _ := tag["Key"].(string)
		v, _ := tag["Value"].(string)
		if k != "" {
			tags[k] = v
		}
	}
	return tags
}

func isUniqueErr(err error) bool {
	return err != nil && strings.Contains(err.Error(), "UNIQUE constraint failed")
}
