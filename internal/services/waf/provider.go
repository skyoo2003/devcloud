// SPDX-License-Identifier: Apache-2.0

// internal/services/waf/provider.go
package waf

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

const accountID = plugin.DefaultAccountID

// Provider implements the WAF_20150824 (WAF Classic) service.
type Provider struct {
	store *Store
}

func (p *Provider) ServiceID() string             { return "waf" }
func (p *Provider) ServiceName() string           { return "WAF_20150824" }
func (p *Provider) Protocol() plugin.ProtocolType { return plugin.ProtocolJSON11 }

func (p *Provider) Init(cfg plugin.PluginConfig) error {
	dataDir := cfg.DataDir
	if dataDir == "" {
		dataDir = "."
	}
	var err error
	p.store, err = NewStore(filepath.Join(dataDir, "waf"))
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
	// ChangeToken
	case "GetChangeToken":
		return p.getChangeToken()
	case "GetChangeTokenStatus":
		return p.getChangeTokenStatus(params)

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
	case "CreateWebACLMigrationStack":
		return jsonResp(http.StatusOK, map[string]any{"S3ObjectUrl": ""})

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

	// Rule
	case "CreateRule":
		return p.createRule(params)
	case "GetRule":
		return p.getRule(params)
	case "ListRules":
		return p.listRules(params)
	case "UpdateRule":
		return p.updateRule(params)
	case "DeleteRule":
		return p.deleteRule(params)

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
	case "ListActivatedRulesInRuleGroup":
		return jsonResp(http.StatusOK, map[string]any{"ActivatedRules": []any{}})
	case "ListSubscribedRuleGroups":
		return jsonResp(http.StatusOK, map[string]any{"RuleGroups": []any{}})

	// RateBasedRule
	case "CreateRateBasedRule":
		return p.createRateBasedRule(params)
	case "GetRateBasedRule":
		return p.getRateBasedRule(params)
	case "ListRateBasedRules":
		return p.listRateBasedRules(params)
	case "UpdateRateBasedRule":
		return p.updateRateBasedRule(params)
	case "DeleteRateBasedRule":
		return p.deleteRateBasedRule(params)
	case "GetRateBasedRuleManagedKeys":
		return jsonResp(http.StatusOK, map[string]any{"ManagedKeys": []any{}})

	// ByteMatchSet
	case "CreateByteMatchSet":
		return p.createByteMatchSet(params)
	case "GetByteMatchSet":
		return p.getByteMatchSet(params)
	case "ListByteMatchSets":
		return p.listByteMatchSets(params)
	case "UpdateByteMatchSet":
		return p.updateByteMatchSet(params)
	case "DeleteByteMatchSet":
		return p.deleteByteMatchSet(params)

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

	// SizeConstraintSet
	case "CreateSizeConstraintSet":
		return p.createSizeConstraintSet(params)
	case "GetSizeConstraintSet":
		return p.getSizeConstraintSet(params)
	case "ListSizeConstraintSets":
		return p.listSizeConstraintSets(params)
	case "UpdateSizeConstraintSet":
		return p.updateSizeConstraintSet(params)
	case "DeleteSizeConstraintSet":
		return p.deleteSizeConstraintSet(params)

	// SqlInjectionMatchSet
	case "CreateSqlInjectionMatchSet":
		return p.createSqlInjectionMatchSet(params)
	case "GetSqlInjectionMatchSet":
		return p.getSqlInjectionMatchSet(params)
	case "ListSqlInjectionMatchSets":
		return p.listSqlInjectionMatchSets(params)
	case "UpdateSqlInjectionMatchSet":
		return p.updateSqlInjectionMatchSet(params)
	case "DeleteSqlInjectionMatchSet":
		return p.deleteSqlInjectionMatchSet(params)

	// XssMatchSet
	case "CreateXssMatchSet":
		return p.createXssMatchSet(params)
	case "GetXssMatchSet":
		return p.getXssMatchSet(params)
	case "ListXssMatchSets":
		return p.listXssMatchSets(params)
	case "UpdateXssMatchSet":
		return p.updateXssMatchSet(params)
	case "DeleteXssMatchSet":
		return p.deleteXssMatchSet(params)

	// GeoMatchSet
	case "CreateGeoMatchSet":
		return p.createGeoMatchSet(params)
	case "GetGeoMatchSet":
		return p.getGeoMatchSet(params)
	case "ListGeoMatchSets":
		return p.listGeoMatchSets(params)
	case "UpdateGeoMatchSet":
		return p.updateGeoMatchSet(params)
	case "DeleteGeoMatchSet":
		return p.deleteGeoMatchSet(params)

	// RegexMatchSet
	case "CreateRegexMatchSet":
		return p.createRegexMatchSet(params)
	case "GetRegexMatchSet":
		return p.getRegexMatchSet(params)
	case "ListRegexMatchSets":
		return p.listRegexMatchSets(params)
	case "UpdateRegexMatchSet":
		return p.updateRegexMatchSet(params)
	case "DeleteRegexMatchSet":
		return p.deleteRegexMatchSet(params)

	// Logging
	case "PutLoggingConfiguration":
		return p.putLoggingConfiguration(params)
	case "GetLoggingConfiguration":
		return p.getLoggingConfiguration(params)
	case "DeleteLoggingConfiguration":
		return p.deleteLoggingConfiguration(params)
	case "ListLoggingConfigurations":
		return p.listLoggingConfigurations(params)

	// Permission Policy
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

	// Misc
	case "GetSampledRequests":
		return jsonResp(http.StatusOK, map[string]any{"SampledRequests": []any{}, "PopulationSize": 0, "TimeWindow": map[string]any{}})

	default:
		return wafError("WAFInvalidOperationException", fmt.Sprintf("operation not implemented: %s", action), http.StatusBadRequest), nil
	}
}

func (p *Provider) ListResources(_ context.Context) ([]plugin.Resource, error) {
	acls, err := p.store.ListWebACLs()
	if err != nil {
		return nil, err
	}
	out := make([]plugin.Resource, 0, len(acls))
	for _, a := range acls {
		out = append(out, plugin.Resource{Type: "web-acl", ID: a.ID, Name: a.Name})
	}
	return out, nil
}

func (p *Provider) GetMetrics(_ context.Context) (*plugin.ServiceMetrics, error) {
	return &plugin.ServiceMetrics{}, nil
}

// --- helpers ---

func wafError(code, msg string, status int) *plugin.Response {
	return shared.JSONError(code, msg, status)
}

func jsonResp(status int, v any) (*plugin.Response, error) {
	return shared.JSONResponse(status, v)
}

func wafARN(resourceType, id string) string {
	return fmt.Sprintf("arn:aws:waf::%s:%s/%s", accountID, resourceType, id)
}

func strParam(params map[string]any, key string) string {
	if v, ok := params[key]; ok {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}

func marshalJSON(v any) string {
	b, _ := json.Marshal(v)
	return string(b)
}

// --- ChangeToken ---

func (p *Provider) getChangeToken() (*plugin.Response, error) {
	token := shared.GenerateUUID()
	return jsonResp(http.StatusOK, map[string]string{"ChangeToken": token})
}

func (p *Provider) getChangeTokenStatus(_ map[string]any) (*plugin.Response, error) {
	return jsonResp(http.StatusOK, map[string]string{"ChangeTokenStatus": "INSYNC"})
}

// --- WebACL ---

func (p *Provider) createWebACL(params map[string]any) (*plugin.Response, error) {
	name := strParam(params, "Name")
	if name == "" {
		return wafError("WAFInvalidParameterException", "Name is required", http.StatusBadRequest), nil
	}
	changeToken := strParam(params, "ChangeToken")
	id := shared.GenerateUUID()
	arn := wafARN("webacl", id)

	defaultAction := "ALLOW"
	if da, ok := params["DefaultAction"].(map[string]any); ok {
		if t, ok := da["Type"].(string); ok {
			defaultAction = t
		}
	}

	w := &WebACL{
		ID:            id,
		Name:          name,
		ARN:           arn,
		DefaultAction: defaultAction,
		Rules:         "[]",
		ChangeToken:   changeToken,
	}
	if err := p.store.CreateWebACL(w); err != nil {
		if sqlite.IsUniqueConstraintError(err) {
			return wafError("WAFStaleDataException", "WebACL already exists", http.StatusBadRequest), nil
		}
		return nil, err
	}
	return jsonResp(http.StatusOK, map[string]any{
		"WebACL": map[string]any{
			"WebACLId":      id,
			"WebACLArn":     arn,
			"Name":          name,
			"DefaultAction": map[string]string{"Type": defaultAction},
			"Rules":         []any{},
			"MetricName":    name,
		},
		"ChangeToken": shared.GenerateUUID(),
	})
}

func (p *Provider) getWebACL(params map[string]any) (*plugin.Response, error) {
	id := strParam(params, "WebACLId")
	if id == "" {
		return wafError("WAFInvalidParameterException", "WebACLId is required", http.StatusBadRequest), nil
	}
	w, err := p.store.GetWebACL(id)
	if err != nil {
		if err == errNotFound {
			return wafError("WAFNonexistentItemException", "WebACL not found", http.StatusBadRequest), nil
		}
		return nil, err
	}
	var rules any
	_ = json.Unmarshal([]byte(w.Rules), &rules)
	if rules == nil {
		rules = []any{}
	}
	return jsonResp(http.StatusOK, map[string]any{
		"WebACL": map[string]any{
			"WebACLId":      w.ID,
			"WebACLArn":     w.ARN,
			"Name":          w.Name,
			"DefaultAction": map[string]string{"Type": w.DefaultAction},
			"Rules":         rules,
			"MetricName":    w.Name,
		},
	})
}

func (p *Provider) listWebACLs(_ map[string]any) (*plugin.Response, error) {
	acls, err := p.store.ListWebACLs()
	if err != nil {
		return nil, err
	}
	items := make([]map[string]any, 0, len(acls))
	for _, a := range acls {
		items = append(items, map[string]any{
			"WebACLId": a.ID,
			"Name":     a.Name,
		})
	}
	return jsonResp(http.StatusOK, map[string]any{"WebACLs": items})
}

func (p *Provider) updateWebACL(params map[string]any) (*plugin.Response, error) {
	id := strParam(params, "WebACLId")
	if id == "" {
		return wafError("WAFInvalidParameterException", "WebACLId is required", http.StatusBadRequest), nil
	}
	w, err := p.store.GetWebACL(id)
	if err != nil {
		if err == errNotFound {
			return wafError("WAFNonexistentItemException", "WebACL not found", http.StatusBadRequest), nil
		}
		return nil, err
	}
	if da, ok := params["DefaultAction"].(map[string]any); ok {
		if t, ok := da["Type"].(string); ok {
			w.DefaultAction = t
		}
	}
	if updates, ok := params["Updates"].([]any); ok {
		var rules []any
		_ = json.Unmarshal([]byte(w.Rules), &rules)
		if rules == nil {
			rules = []any{}
		}
		for _, u := range updates {
			if upd, ok := u.(map[string]any); ok {
				action := strFromMap(upd, "Action")
				if action == "INSERT" {
					if ar, ok := upd["ActivatedRule"]; ok {
						rules = append(rules, ar)
					}
				}
			}
		}
		w.Rules = marshalJSON(rules)
	}
	w.ChangeToken = strParam(params, "ChangeToken")
	if err := p.store.UpdateWebACL(w); err != nil {
		return nil, err
	}
	return jsonResp(http.StatusOK, map[string]any{"ChangeToken": shared.GenerateUUID()})
}

func (p *Provider) deleteWebACL(params map[string]any) (*plugin.Response, error) {
	id := strParam(params, "WebACLId")
	if id == "" {
		return wafError("WAFInvalidParameterException", "WebACLId is required", http.StatusBadRequest), nil
	}
	if err := p.store.DeleteWebACL(id); err != nil {
		if err == errNotFound {
			return wafError("WAFNonexistentItemException", "WebACL not found", http.StatusBadRequest), nil
		}
		return nil, err
	}
	return jsonResp(http.StatusOK, map[string]any{"ChangeToken": shared.GenerateUUID()})
}

// --- IPSet ---

func (p *Provider) createIPSet(params map[string]any) (*plugin.Response, error) {
	name := strParam(params, "Name")
	if name == "" {
		return wafError("WAFInvalidParameterException", "Name is required", http.StatusBadRequest), nil
	}
	id := shared.GenerateUUID()
	arn := wafARN("ipset", id)
	ip := &IPSet{
		ID:          id,
		Name:        name,
		ARN:         arn,
		Descriptors: "[]",
		ChangeToken: strParam(params, "ChangeToken"),
	}
	if err := p.store.CreateIPSet(ip); err != nil {
		return nil, err
	}
	return jsonResp(http.StatusOK, map[string]any{
		"IPSet": map[string]any{
			"IPSetId":          id,
			"Name":             name,
			"IPSetDescriptors": []any{},
		},
		"ChangeToken": shared.GenerateUUID(),
	})
}

func (p *Provider) getIPSet(params map[string]any) (*plugin.Response, error) {
	id := strParam(params, "IPSetId")
	if id == "" {
		return wafError("WAFInvalidParameterException", "IPSetId is required", http.StatusBadRequest), nil
	}
	ip, err := p.store.GetIPSet(id)
	if err != nil {
		if err == errNotFound {
			return wafError("WAFNonexistentItemException", "IPSet not found", http.StatusBadRequest), nil
		}
		return nil, err
	}
	var descs any
	_ = json.Unmarshal([]byte(ip.Descriptors), &descs)
	if descs == nil {
		descs = []any{}
	}
	return jsonResp(http.StatusOK, map[string]any{
		"IPSet": map[string]any{
			"IPSetId":          ip.ID,
			"Name":             ip.Name,
			"IPSetDescriptors": descs,
		},
	})
}

func (p *Provider) listIPSets(_ map[string]any) (*plugin.Response, error) {
	sets, err := p.store.ListIPSets()
	if err != nil {
		return nil, err
	}
	items := make([]map[string]any, 0, len(sets))
	for _, s := range sets {
		items = append(items, map[string]any{"IPSetId": s.ID, "Name": s.Name})
	}
	return jsonResp(http.StatusOK, map[string]any{"IPSets": items})
}

func (p *Provider) updateIPSet(params map[string]any) (*plugin.Response, error) {
	id := strParam(params, "IPSetId")
	if id == "" {
		return wafError("WAFInvalidParameterException", "IPSetId is required", http.StatusBadRequest), nil
	}
	ip, err := p.store.GetIPSet(id)
	if err != nil {
		if err == errNotFound {
			return wafError("WAFNonexistentItemException", "IPSet not found", http.StatusBadRequest), nil
		}
		return nil, err
	}
	if updates, ok := params["Updates"].([]any); ok {
		var descs []any
		_ = json.Unmarshal([]byte(ip.Descriptors), &descs)
		if descs == nil {
			descs = []any{}
		}
		for _, u := range updates {
			if upd, ok := u.(map[string]any); ok {
				action := strFromMap(upd, "Action")
				if action == "INSERT" {
					if d, ok := upd["IPSetDescriptor"]; ok {
						descs = append(descs, d)
					}
				}
			}
		}
		ip.Descriptors = marshalJSON(descs)
	}
	ip.ChangeToken = strParam(params, "ChangeToken")
	if err := p.store.UpdateIPSet(ip); err != nil {
		return nil, err
	}
	return jsonResp(http.StatusOK, map[string]any{"ChangeToken": shared.GenerateUUID()})
}

func (p *Provider) deleteIPSet(params map[string]any) (*plugin.Response, error) {
	id := strParam(params, "IPSetId")
	if id == "" {
		return wafError("WAFInvalidParameterException", "IPSetId is required", http.StatusBadRequest), nil
	}
	if err := p.store.DeleteIPSet(id); err != nil {
		if err == errNotFound {
			return wafError("WAFNonexistentItemException", "IPSet not found", http.StatusBadRequest), nil
		}
		return nil, err
	}
	return jsonResp(http.StatusOK, map[string]any{"ChangeToken": shared.GenerateUUID()})
}

// --- Rule ---

func (p *Provider) createRule(params map[string]any) (*plugin.Response, error) {
	name := strParam(params, "Name")
	if name == "" {
		return wafError("WAFInvalidParameterException", "Name is required", http.StatusBadRequest), nil
	}
	metricName := strParam(params, "MetricName")
	if metricName == "" {
		metricName = name
	}
	id := shared.GenerateUUID()
	arn := wafARN("rule", id)
	r := &Rule{
		ID:          id,
		Name:        name,
		ARN:         arn,
		MetricName:  metricName,
		Predicates:  "[]",
		ChangeToken: strParam(params, "ChangeToken"),
	}
	if err := p.store.CreateRule(r); err != nil {
		return nil, err
	}
	return jsonResp(http.StatusOK, map[string]any{
		"Rule": map[string]any{
			"RuleId":     id,
			"Name":       name,
			"MetricName": metricName,
			"Predicates": []any{},
		},
		"ChangeToken": shared.GenerateUUID(),
	})
}

func (p *Provider) getRule(params map[string]any) (*plugin.Response, error) {
	id := strParam(params, "RuleId")
	if id == "" {
		return wafError("WAFInvalidParameterException", "RuleId is required", http.StatusBadRequest), nil
	}
	r, err := p.store.GetRule(id)
	if err != nil {
		if err == errNotFound {
			return wafError("WAFNonexistentItemException", "Rule not found", http.StatusBadRequest), nil
		}
		return nil, err
	}
	var preds any
	_ = json.Unmarshal([]byte(r.Predicates), &preds)
	if preds == nil {
		preds = []any{}
	}
	return jsonResp(http.StatusOK, map[string]any{
		"Rule": map[string]any{
			"RuleId":     r.ID,
			"Name":       r.Name,
			"MetricName": r.MetricName,
			"Predicates": preds,
		},
	})
}

func (p *Provider) listRules(_ map[string]any) (*plugin.Response, error) {
	rules, err := p.store.ListRules()
	if err != nil {
		return nil, err
	}
	items := make([]map[string]any, 0, len(rules))
	for _, r := range rules {
		items = append(items, map[string]any{"RuleId": r.ID, "Name": r.Name})
	}
	return jsonResp(http.StatusOK, map[string]any{"Rules": items})
}

func (p *Provider) updateRule(params map[string]any) (*plugin.Response, error) {
	id := strParam(params, "RuleId")
	if id == "" {
		return wafError("WAFInvalidParameterException", "RuleId is required", http.StatusBadRequest), nil
	}
	r, err := p.store.GetRule(id)
	if err != nil {
		if err == errNotFound {
			return wafError("WAFNonexistentItemException", "Rule not found", http.StatusBadRequest), nil
		}
		return nil, err
	}
	if updates, ok := params["Updates"].([]any); ok {
		var preds []any
		_ = json.Unmarshal([]byte(r.Predicates), &preds)
		if preds == nil {
			preds = []any{}
		}
		for _, u := range updates {
			if upd, ok := u.(map[string]any); ok {
				action := strFromMap(upd, "Action")
				if action == "INSERT" {
					if pred, ok := upd["Predicate"]; ok {
						preds = append(preds, pred)
					}
				}
			}
		}
		r.Predicates = marshalJSON(preds)
	}
	r.ChangeToken = strParam(params, "ChangeToken")
	if err := p.store.UpdateRule(r); err != nil {
		return nil, err
	}
	return jsonResp(http.StatusOK, map[string]any{"ChangeToken": shared.GenerateUUID()})
}

func (p *Provider) deleteRule(params map[string]any) (*plugin.Response, error) {
	id := strParam(params, "RuleId")
	if id == "" {
		return wafError("WAFInvalidParameterException", "RuleId is required", http.StatusBadRequest), nil
	}
	if err := p.store.DeleteRule(id); err != nil {
		if err == errNotFound {
			return wafError("WAFNonexistentItemException", "Rule not found", http.StatusBadRequest), nil
		}
		return nil, err
	}
	return jsonResp(http.StatusOK, map[string]any{"ChangeToken": shared.GenerateUUID()})
}

// --- RuleGroup ---

func (p *Provider) createRuleGroup(params map[string]any) (*plugin.Response, error) {
	name := strParam(params, "Name")
	if name == "" {
		return wafError("WAFInvalidParameterException", "Name is required", http.StatusBadRequest), nil
	}
	metricName := strParam(params, "MetricName")
	if metricName == "" {
		metricName = name
	}
	id := shared.GenerateUUID()
	arn := wafARN("rulegroup", id)
	rg := &RuleGroup{
		ID:          id,
		Name:        name,
		ARN:         arn,
		MetricName:  metricName,
		ChangeToken: strParam(params, "ChangeToken"),
	}
	if err := p.store.CreateRuleGroup(rg); err != nil {
		return nil, err
	}
	return jsonResp(http.StatusOK, map[string]any{
		"RuleGroup": map[string]any{
			"RuleGroupId": id,
			"Name":        name,
			"MetricName":  metricName,
		},
		"ChangeToken": shared.GenerateUUID(),
	})
}

func (p *Provider) getRuleGroup(params map[string]any) (*plugin.Response, error) {
	id := strParam(params, "RuleGroupId")
	if id == "" {
		return wafError("WAFInvalidParameterException", "RuleGroupId is required", http.StatusBadRequest), nil
	}
	rg, err := p.store.GetRuleGroup(id)
	if err != nil {
		if err == errNotFound {
			return wafError("WAFNonexistentItemException", "RuleGroup not found", http.StatusBadRequest), nil
		}
		return nil, err
	}
	return jsonResp(http.StatusOK, map[string]any{
		"RuleGroup": map[string]any{
			"RuleGroupId": rg.ID,
			"Name":        rg.Name,
			"MetricName":  rg.MetricName,
		},
	})
}

func (p *Provider) listRuleGroups(_ map[string]any) (*plugin.Response, error) {
	groups, err := p.store.ListRuleGroups()
	if err != nil {
		return nil, err
	}
	items := make([]map[string]any, 0, len(groups))
	for _, rg := range groups {
		items = append(items, map[string]any{"RuleGroupId": rg.ID, "Name": rg.Name})
	}
	return jsonResp(http.StatusOK, map[string]any{"RuleGroups": items})
}

func (p *Provider) updateRuleGroup(params map[string]any) (*plugin.Response, error) {
	id := strParam(params, "RuleGroupId")
	if id == "" {
		return wafError("WAFInvalidParameterException", "RuleGroupId is required", http.StatusBadRequest), nil
	}
	if _, err := p.store.GetRuleGroup(id); err != nil {
		if err == errNotFound {
			return wafError("WAFNonexistentItemException", "RuleGroup not found", http.StatusBadRequest), nil
		}
		return nil, err
	}
	return jsonResp(http.StatusOK, map[string]any{"ChangeToken": shared.GenerateUUID()})
}

func (p *Provider) deleteRuleGroup(params map[string]any) (*plugin.Response, error) {
	id := strParam(params, "RuleGroupId")
	if id == "" {
		return wafError("WAFInvalidParameterException", "RuleGroupId is required", http.StatusBadRequest), nil
	}
	if err := p.store.DeleteRuleGroup(id); err != nil {
		if err == errNotFound {
			return wafError("WAFNonexistentItemException", "RuleGroup not found", http.StatusBadRequest), nil
		}
		return nil, err
	}
	return jsonResp(http.StatusOK, map[string]any{"ChangeToken": shared.GenerateUUID()})
}

// --- RateBasedRule ---

func (p *Provider) createRateBasedRule(params map[string]any) (*plugin.Response, error) {
	name := strParam(params, "Name")
	if name == "" {
		return wafError("WAFInvalidParameterException", "Name is required", http.StatusBadRequest), nil
	}
	metricName := strParam(params, "MetricName")
	if metricName == "" {
		metricName = name
	}
	rateLimit := int64(2000)
	if rl, ok := params["RateLimit"].(float64); ok {
		rateLimit = int64(rl)
	}
	id := shared.GenerateUUID()
	arn := wafARN("ratebasedrule", id)
	r := &RateBasedRule{
		ID:          id,
		Name:        name,
		ARN:         arn,
		MetricName:  metricName,
		RateLimit:   rateLimit,
		Predicates:  "[]",
		ChangeToken: strParam(params, "ChangeToken"),
	}
	if err := p.store.CreateRateBasedRule(r); err != nil {
		return nil, err
	}
	return jsonResp(http.StatusOK, map[string]any{
		"Rule": map[string]any{
			"RuleId":          id,
			"Name":            name,
			"MetricName":      metricName,
			"RateLimit":       rateLimit,
			"RateKey":         "IP",
			"MatchPredicates": []any{},
		},
		"ChangeToken": shared.GenerateUUID(),
	})
}

func (p *Provider) getRateBasedRule(params map[string]any) (*plugin.Response, error) {
	id := strParam(params, "RuleId")
	if id == "" {
		return wafError("WAFInvalidParameterException", "RuleId is required", http.StatusBadRequest), nil
	}
	r, err := p.store.GetRateBasedRule(id)
	if err != nil {
		if err == errNotFound {
			return wafError("WAFNonexistentItemException", "RateBasedRule not found", http.StatusBadRequest), nil
		}
		return nil, err
	}
	var preds any
	_ = json.Unmarshal([]byte(r.Predicates), &preds)
	if preds == nil {
		preds = []any{}
	}
	return jsonResp(http.StatusOK, map[string]any{
		"Rule": map[string]any{
			"RuleId":          r.ID,
			"Name":            r.Name,
			"MetricName":      r.MetricName,
			"RateLimit":       r.RateLimit,
			"RateKey":         "IP",
			"MatchPredicates": preds,
		},
	})
}

func (p *Provider) listRateBasedRules(_ map[string]any) (*plugin.Response, error) {
	rules, err := p.store.ListRateBasedRules()
	if err != nil {
		return nil, err
	}
	items := make([]map[string]any, 0, len(rules))
	for _, r := range rules {
		items = append(items, map[string]any{"RuleId": r.ID, "Name": r.Name})
	}
	return jsonResp(http.StatusOK, map[string]any{"Rules": items})
}

func (p *Provider) updateRateBasedRule(params map[string]any) (*plugin.Response, error) {
	id := strParam(params, "RuleId")
	if id == "" {
		return wafError("WAFInvalidParameterException", "RuleId is required", http.StatusBadRequest), nil
	}
	r, err := p.store.GetRateBasedRule(id)
	if err != nil {
		if err == errNotFound {
			return wafError("WAFNonexistentItemException", "RateBasedRule not found", http.StatusBadRequest), nil
		}
		return nil, err
	}
	if rl, ok := params["RateLimit"].(float64); ok {
		r.RateLimit = int64(rl)
	}
	r.ChangeToken = strParam(params, "ChangeToken")
	if err := p.store.UpdateRateBasedRule(r); err != nil {
		return nil, err
	}
	return jsonResp(http.StatusOK, map[string]any{"ChangeToken": shared.GenerateUUID()})
}

func (p *Provider) deleteRateBasedRule(params map[string]any) (*plugin.Response, error) {
	id := strParam(params, "RuleId")
	if id == "" {
		return wafError("WAFInvalidParameterException", "RuleId is required", http.StatusBadRequest), nil
	}
	if err := p.store.DeleteRateBasedRule(id); err != nil {
		if err == errNotFound {
			return wafError("WAFNonexistentItemException", "RateBasedRule not found", http.StatusBadRequest), nil
		}
		return nil, err
	}
	return jsonResp(http.StatusOK, map[string]any{"ChangeToken": shared.GenerateUUID()})
}

// --- ByteMatchSet ---

func (p *Provider) createByteMatchSet(params map[string]any) (*plugin.Response, error) {
	name := strParam(params, "Name")
	if name == "" {
		return wafError("WAFInvalidParameterException", "Name is required", http.StatusBadRequest), nil
	}
	id := shared.GenerateUUID()
	b := &ByteMatchSet{
		ID:          id,
		Name:        name,
		ChangeToken: strParam(params, "ChangeToken"),
		Tuples:      "[]",
	}
	if err := p.store.CreateByteMatchSet(b); err != nil {
		return nil, err
	}
	return jsonResp(http.StatusOK, map[string]any{
		"ByteMatchSet": map[string]any{
			"ByteMatchSetId":  id,
			"Name":            name,
			"ByteMatchTuples": []any{},
		},
		"ChangeToken": shared.GenerateUUID(),
	})
}

func (p *Provider) getByteMatchSet(params map[string]any) (*plugin.Response, error) {
	id := strParam(params, "ByteMatchSetId")
	if id == "" {
		return wafError("WAFInvalidParameterException", "ByteMatchSetId is required", http.StatusBadRequest), nil
	}
	b, err := p.store.GetByteMatchSet(id)
	if err != nil {
		if err == errNotFound {
			return wafError("WAFNonexistentItemException", "ByteMatchSet not found", http.StatusBadRequest), nil
		}
		return nil, err
	}
	var tuples any
	_ = json.Unmarshal([]byte(b.Tuples), &tuples)
	if tuples == nil {
		tuples = []any{}
	}
	return jsonResp(http.StatusOK, map[string]any{
		"ByteMatchSet": map[string]any{
			"ByteMatchSetId":  b.ID,
			"Name":            b.Name,
			"ByteMatchTuples": tuples,
		},
	})
}

func (p *Provider) listByteMatchSets(_ map[string]any) (*plugin.Response, error) {
	sets, err := p.store.ListByteMatchSets()
	if err != nil {
		return nil, err
	}
	items := make([]map[string]any, 0, len(sets))
	for _, b := range sets {
		items = append(items, map[string]any{"ByteMatchSetId": b.ID, "Name": b.Name})
	}
	return jsonResp(http.StatusOK, map[string]any{"ByteMatchSets": items})
}

func (p *Provider) updateByteMatchSet(params map[string]any) (*plugin.Response, error) {
	id := strParam(params, "ByteMatchSetId")
	if id == "" {
		return wafError("WAFInvalidParameterException", "ByteMatchSetId is required", http.StatusBadRequest), nil
	}
	b, err := p.store.GetByteMatchSet(id)
	if err != nil {
		if err == errNotFound {
			return wafError("WAFNonexistentItemException", "ByteMatchSet not found", http.StatusBadRequest), nil
		}
		return nil, err
	}
	if updates, ok := params["Updates"].([]any); ok {
		var tuples []any
		_ = json.Unmarshal([]byte(b.Tuples), &tuples)
		if tuples == nil {
			tuples = []any{}
		}
		for _, u := range updates {
			if upd, ok := u.(map[string]any); ok {
				action := strFromMap(upd, "Action")
				if action == "INSERT" {
					if tup, ok := upd["ByteMatchTuple"]; ok {
						tuples = append(tuples, tup)
					}
				}
			}
		}
		b.Tuples = marshalJSON(tuples)
	}
	b.ChangeToken = strParam(params, "ChangeToken")
	if err := p.store.UpdateByteMatchSet(b); err != nil {
		return nil, err
	}
	return jsonResp(http.StatusOK, map[string]any{"ChangeToken": shared.GenerateUUID()})
}

func (p *Provider) deleteByteMatchSet(params map[string]any) (*plugin.Response, error) {
	id := strParam(params, "ByteMatchSetId")
	if id == "" {
		return wafError("WAFInvalidParameterException", "ByteMatchSetId is required", http.StatusBadRequest), nil
	}
	if err := p.store.DeleteByteMatchSet(id); err != nil {
		if err == errNotFound {
			return wafError("WAFNonexistentItemException", "ByteMatchSet not found", http.StatusBadRequest), nil
		}
		return nil, err
	}
	return jsonResp(http.StatusOK, map[string]any{"ChangeToken": shared.GenerateUUID()})
}

// --- RegexPatternSet ---

func (p *Provider) createRegexPatternSet(params map[string]any) (*plugin.Response, error) {
	name := strParam(params, "Name")
	if name == "" {
		return wafError("WAFInvalidParameterException", "Name is required", http.StatusBadRequest), nil
	}
	id := shared.GenerateUUID()
	r := &RegexPatternSet{
		ID:          id,
		Name:        name,
		Patterns:    "[]",
		ChangeToken: strParam(params, "ChangeToken"),
	}
	if err := p.store.CreateRegexPatternSet(r); err != nil {
		return nil, err
	}
	return jsonResp(http.StatusOK, map[string]any{
		"RegexPatternSet": map[string]any{
			"RegexPatternSetId":   id,
			"Name":                name,
			"RegexPatternStrings": []any{},
		},
		"ChangeToken": shared.GenerateUUID(),
	})
}

func (p *Provider) getRegexPatternSet(params map[string]any) (*plugin.Response, error) {
	id := strParam(params, "RegexPatternSetId")
	if id == "" {
		return wafError("WAFInvalidParameterException", "RegexPatternSetId is required", http.StatusBadRequest), nil
	}
	r, err := p.store.GetRegexPatternSet(id)
	if err != nil {
		if err == errNotFound {
			return wafError("WAFNonexistentItemException", "RegexPatternSet not found", http.StatusBadRequest), nil
		}
		return nil, err
	}
	var patterns any
	_ = json.Unmarshal([]byte(r.Patterns), &patterns)
	if patterns == nil {
		patterns = []any{}
	}
	return jsonResp(http.StatusOK, map[string]any{
		"RegexPatternSet": map[string]any{
			"RegexPatternSetId":   r.ID,
			"Name":                r.Name,
			"RegexPatternStrings": patterns,
		},
	})
}

func (p *Provider) listRegexPatternSets(_ map[string]any) (*plugin.Response, error) {
	sets, err := p.store.ListRegexPatternSets()
	if err != nil {
		return nil, err
	}
	items := make([]map[string]any, 0, len(sets))
	for _, r := range sets {
		items = append(items, map[string]any{"RegexPatternSetId": r.ID, "Name": r.Name})
	}
	return jsonResp(http.StatusOK, map[string]any{"RegexPatternSets": items})
}

func (p *Provider) updateRegexPatternSet(params map[string]any) (*plugin.Response, error) {
	id := strParam(params, "RegexPatternSetId")
	if id == "" {
		return wafError("WAFInvalidParameterException", "RegexPatternSetId is required", http.StatusBadRequest), nil
	}
	r, err := p.store.GetRegexPatternSet(id)
	if err != nil {
		if err == errNotFound {
			return wafError("WAFNonexistentItemException", "RegexPatternSet not found", http.StatusBadRequest), nil
		}
		return nil, err
	}
	if updates, ok := params["Updates"].([]any); ok {
		var patterns []any
		_ = json.Unmarshal([]byte(r.Patterns), &patterns)
		if patterns == nil {
			patterns = []any{}
		}
		for _, u := range updates {
			if upd, ok := u.(map[string]any); ok {
				action := strFromMap(upd, "Action")
				if action == "INSERT" {
					if pat, ok := upd["RegexPatternString"]; ok {
						patterns = append(patterns, pat)
					}
				}
			}
		}
		r.Patterns = marshalJSON(patterns)
	}
	r.ChangeToken = strParam(params, "ChangeToken")
	if err := p.store.UpdateRegexPatternSet(r); err != nil {
		return nil, err
	}
	return jsonResp(http.StatusOK, map[string]any{"ChangeToken": shared.GenerateUUID()})
}

func (p *Provider) deleteRegexPatternSet(params map[string]any) (*plugin.Response, error) {
	id := strParam(params, "RegexPatternSetId")
	if id == "" {
		return wafError("WAFInvalidParameterException", "RegexPatternSetId is required", http.StatusBadRequest), nil
	}
	if err := p.store.DeleteRegexPatternSet(id); err != nil {
		if err == errNotFound {
			return wafError("WAFNonexistentItemException", "RegexPatternSet not found", http.StatusBadRequest), nil
		}
		return nil, err
	}
	return jsonResp(http.StatusOK, map[string]any{"ChangeToken": shared.GenerateUUID()})
}

// --- SizeConstraintSet ---

func (p *Provider) createSizeConstraintSet(params map[string]any) (*plugin.Response, error) {
	name := strParam(params, "Name")
	if name == "" {
		return wafError("WAFInvalidParameterException", "Name is required", http.StatusBadRequest), nil
	}
	id := shared.GenerateUUID()
	sc := &SizeConstraintSet{
		ID:          id,
		Name:        name,
		Constraints: "[]",
		ChangeToken: strParam(params, "ChangeToken"),
	}
	if err := p.store.CreateSizeConstraintSet(sc); err != nil {
		return nil, err
	}
	return jsonResp(http.StatusOK, map[string]any{
		"SizeConstraintSet": map[string]any{
			"SizeConstraintSetId": id,
			"Name":                name,
			"SizeConstraints":     []any{},
		},
		"ChangeToken": shared.GenerateUUID(),
	})
}

func (p *Provider) getSizeConstraintSet(params map[string]any) (*plugin.Response, error) {
	id := strParam(params, "SizeConstraintSetId")
	if id == "" {
		return wafError("WAFInvalidParameterException", "SizeConstraintSetId is required", http.StatusBadRequest), nil
	}
	sc, err := p.store.GetSizeConstraintSet(id)
	if err != nil {
		if err == errNotFound {
			return wafError("WAFNonexistentItemException", "SizeConstraintSet not found", http.StatusBadRequest), nil
		}
		return nil, err
	}
	var constraints any
	_ = json.Unmarshal([]byte(sc.Constraints), &constraints)
	if constraints == nil {
		constraints = []any{}
	}
	return jsonResp(http.StatusOK, map[string]any{
		"SizeConstraintSet": map[string]any{
			"SizeConstraintSetId": sc.ID,
			"Name":                sc.Name,
			"SizeConstraints":     constraints,
		},
	})
}

func (p *Provider) listSizeConstraintSets(_ map[string]any) (*plugin.Response, error) {
	sets, err := p.store.ListSizeConstraintSets()
	if err != nil {
		return nil, err
	}
	items := make([]map[string]any, 0, len(sets))
	for _, sc := range sets {
		items = append(items, map[string]any{"SizeConstraintSetId": sc.ID, "Name": sc.Name})
	}
	return jsonResp(http.StatusOK, map[string]any{"SizeConstraintSets": items})
}

func (p *Provider) updateSizeConstraintSet(params map[string]any) (*plugin.Response, error) {
	id := strParam(params, "SizeConstraintSetId")
	if id == "" {
		return wafError("WAFInvalidParameterException", "SizeConstraintSetId is required", http.StatusBadRequest), nil
	}
	sc, err := p.store.GetSizeConstraintSet(id)
	if err != nil {
		if err == errNotFound {
			return wafError("WAFNonexistentItemException", "SizeConstraintSet not found", http.StatusBadRequest), nil
		}
		return nil, err
	}
	if updates, ok := params["Updates"].([]any); ok {
		var constraints []any
		_ = json.Unmarshal([]byte(sc.Constraints), &constraints)
		if constraints == nil {
			constraints = []any{}
		}
		for _, u := range updates {
			if upd, ok := u.(map[string]any); ok {
				action := strFromMap(upd, "Action")
				if action == "INSERT" {
					if c, ok := upd["SizeConstraint"]; ok {
						constraints = append(constraints, c)
					}
				}
			}
		}
		sc.Constraints = marshalJSON(constraints)
	}
	sc.ChangeToken = strParam(params, "ChangeToken")
	if err := p.store.UpdateSizeConstraintSet(sc); err != nil {
		return nil, err
	}
	return jsonResp(http.StatusOK, map[string]any{"ChangeToken": shared.GenerateUUID()})
}

func (p *Provider) deleteSizeConstraintSet(params map[string]any) (*plugin.Response, error) {
	id := strParam(params, "SizeConstraintSetId")
	if id == "" {
		return wafError("WAFInvalidParameterException", "SizeConstraintSetId is required", http.StatusBadRequest), nil
	}
	if err := p.store.DeleteSizeConstraintSet(id); err != nil {
		if err == errNotFound {
			return wafError("WAFNonexistentItemException", "SizeConstraintSet not found", http.StatusBadRequest), nil
		}
		return nil, err
	}
	return jsonResp(http.StatusOK, map[string]any{"ChangeToken": shared.GenerateUUID()})
}

// --- SqlInjectionMatchSet ---

func (p *Provider) createSqlInjectionMatchSet(params map[string]any) (*plugin.Response, error) {
	name := strParam(params, "Name")
	if name == "" {
		return wafError("WAFInvalidParameterException", "Name is required", http.StatusBadRequest), nil
	}
	id := shared.GenerateUUID()
	si := &SqlInjectionMatchSet{
		ID:          id,
		Name:        name,
		Tuples:      "[]",
		ChangeToken: strParam(params, "ChangeToken"),
	}
	if err := p.store.CreateSqlInjectionMatchSet(si); err != nil {
		return nil, err
	}
	return jsonResp(http.StatusOK, map[string]any{
		"SqlInjectionMatchSet": map[string]any{
			"SqlInjectionMatchSetId":  id,
			"Name":                    name,
			"SqlInjectionMatchTuples": []any{},
		},
		"ChangeToken": shared.GenerateUUID(),
	})
}

func (p *Provider) getSqlInjectionMatchSet(params map[string]any) (*plugin.Response, error) {
	id := strParam(params, "SqlInjectionMatchSetId")
	if id == "" {
		return wafError("WAFInvalidParameterException", "SqlInjectionMatchSetId is required", http.StatusBadRequest), nil
	}
	si, err := p.store.GetSqlInjectionMatchSet(id)
	if err != nil {
		if err == errNotFound {
			return wafError("WAFNonexistentItemException", "SqlInjectionMatchSet not found", http.StatusBadRequest), nil
		}
		return nil, err
	}
	var tuples any
	_ = json.Unmarshal([]byte(si.Tuples), &tuples)
	if tuples == nil {
		tuples = []any{}
	}
	return jsonResp(http.StatusOK, map[string]any{
		"SqlInjectionMatchSet": map[string]any{
			"SqlInjectionMatchSetId":  si.ID,
			"Name":                    si.Name,
			"SqlInjectionMatchTuples": tuples,
		},
	})
}

func (p *Provider) listSqlInjectionMatchSets(_ map[string]any) (*plugin.Response, error) {
	sets, err := p.store.ListSqlInjectionMatchSets()
	if err != nil {
		return nil, err
	}
	items := make([]map[string]any, 0, len(sets))
	for _, si := range sets {
		items = append(items, map[string]any{"SqlInjectionMatchSetId": si.ID, "Name": si.Name})
	}
	return jsonResp(http.StatusOK, map[string]any{"SqlInjectionMatchSets": items})
}

func (p *Provider) updateSqlInjectionMatchSet(params map[string]any) (*plugin.Response, error) {
	id := strParam(params, "SqlInjectionMatchSetId")
	if id == "" {
		return wafError("WAFInvalidParameterException", "SqlInjectionMatchSetId is required", http.StatusBadRequest), nil
	}
	si, err := p.store.GetSqlInjectionMatchSet(id)
	if err != nil {
		if err == errNotFound {
			return wafError("WAFNonexistentItemException", "SqlInjectionMatchSet not found", http.StatusBadRequest), nil
		}
		return nil, err
	}
	if updates, ok := params["Updates"].([]any); ok {
		var tuples []any
		_ = json.Unmarshal([]byte(si.Tuples), &tuples)
		if tuples == nil {
			tuples = []any{}
		}
		for _, u := range updates {
			if upd, ok := u.(map[string]any); ok {
				action := strFromMap(upd, "Action")
				if action == "INSERT" {
					if tup, ok := upd["SqlInjectionMatchTuple"]; ok {
						tuples = append(tuples, tup)
					}
				}
			}
		}
		si.Tuples = marshalJSON(tuples)
	}
	si.ChangeToken = strParam(params, "ChangeToken")
	if err := p.store.UpdateSqlInjectionMatchSet(si); err != nil {
		return nil, err
	}
	return jsonResp(http.StatusOK, map[string]any{"ChangeToken": shared.GenerateUUID()})
}

func (p *Provider) deleteSqlInjectionMatchSet(params map[string]any) (*plugin.Response, error) {
	id := strParam(params, "SqlInjectionMatchSetId")
	if id == "" {
		return wafError("WAFInvalidParameterException", "SqlInjectionMatchSetId is required", http.StatusBadRequest), nil
	}
	if err := p.store.DeleteSqlInjectionMatchSet(id); err != nil {
		if err == errNotFound {
			return wafError("WAFNonexistentItemException", "SqlInjectionMatchSet not found", http.StatusBadRequest), nil
		}
		return nil, err
	}
	return jsonResp(http.StatusOK, map[string]any{"ChangeToken": shared.GenerateUUID()})
}

// --- XssMatchSet ---

func (p *Provider) createXssMatchSet(params map[string]any) (*plugin.Response, error) {
	name := strParam(params, "Name")
	if name == "" {
		return wafError("WAFInvalidParameterException", "Name is required", http.StatusBadRequest), nil
	}
	id := shared.GenerateUUID()
	x := &XssMatchSet{
		ID:          id,
		Name:        name,
		Tuples:      "[]",
		ChangeToken: strParam(params, "ChangeToken"),
	}
	if err := p.store.CreateXssMatchSet(x); err != nil {
		return nil, err
	}
	return jsonResp(http.StatusOK, map[string]any{
		"XssMatchSet": map[string]any{
			"XssMatchSetId":  id,
			"Name":           name,
			"XssMatchTuples": []any{},
		},
		"ChangeToken": shared.GenerateUUID(),
	})
}

func (p *Provider) getXssMatchSet(params map[string]any) (*plugin.Response, error) {
	id := strParam(params, "XssMatchSetId")
	if id == "" {
		return wafError("WAFInvalidParameterException", "XssMatchSetId is required", http.StatusBadRequest), nil
	}
	x, err := p.store.GetXssMatchSet(id)
	if err != nil {
		if err == errNotFound {
			return wafError("WAFNonexistentItemException", "XssMatchSet not found", http.StatusBadRequest), nil
		}
		return nil, err
	}
	var tuples any
	_ = json.Unmarshal([]byte(x.Tuples), &tuples)
	if tuples == nil {
		tuples = []any{}
	}
	return jsonResp(http.StatusOK, map[string]any{
		"XssMatchSet": map[string]any{
			"XssMatchSetId":  x.ID,
			"Name":           x.Name,
			"XssMatchTuples": tuples,
		},
	})
}

func (p *Provider) listXssMatchSets(_ map[string]any) (*plugin.Response, error) {
	sets, err := p.store.ListXssMatchSets()
	if err != nil {
		return nil, err
	}
	items := make([]map[string]any, 0, len(sets))
	for _, x := range sets {
		items = append(items, map[string]any{"XssMatchSetId": x.ID, "Name": x.Name})
	}
	return jsonResp(http.StatusOK, map[string]any{"XssMatchSets": items})
}

func (p *Provider) updateXssMatchSet(params map[string]any) (*plugin.Response, error) {
	id := strParam(params, "XssMatchSetId")
	if id == "" {
		return wafError("WAFInvalidParameterException", "XssMatchSetId is required", http.StatusBadRequest), nil
	}
	x, err := p.store.GetXssMatchSet(id)
	if err != nil {
		if err == errNotFound {
			return wafError("WAFNonexistentItemException", "XssMatchSet not found", http.StatusBadRequest), nil
		}
		return nil, err
	}
	if updates, ok := params["Updates"].([]any); ok {
		var tuples []any
		_ = json.Unmarshal([]byte(x.Tuples), &tuples)
		if tuples == nil {
			tuples = []any{}
		}
		for _, u := range updates {
			if upd, ok := u.(map[string]any); ok {
				action := strFromMap(upd, "Action")
				if action == "INSERT" {
					if tup, ok := upd["XssMatchTuple"]; ok {
						tuples = append(tuples, tup)
					}
				}
			}
		}
		x.Tuples = marshalJSON(tuples)
	}
	x.ChangeToken = strParam(params, "ChangeToken")
	if err := p.store.UpdateXssMatchSet(x); err != nil {
		return nil, err
	}
	return jsonResp(http.StatusOK, map[string]any{"ChangeToken": shared.GenerateUUID()})
}

func (p *Provider) deleteXssMatchSet(params map[string]any) (*plugin.Response, error) {
	id := strParam(params, "XssMatchSetId")
	if id == "" {
		return wafError("WAFInvalidParameterException", "XssMatchSetId is required", http.StatusBadRequest), nil
	}
	if err := p.store.DeleteXssMatchSet(id); err != nil {
		if err == errNotFound {
			return wafError("WAFNonexistentItemException", "XssMatchSet not found", http.StatusBadRequest), nil
		}
		return nil, err
	}
	return jsonResp(http.StatusOK, map[string]any{"ChangeToken": shared.GenerateUUID()})
}

// --- GeoMatchSet ---

func (p *Provider) createGeoMatchSet(params map[string]any) (*plugin.Response, error) {
	name := strParam(params, "Name")
	if name == "" {
		return wafError("WAFInvalidParameterException", "Name is required", http.StatusBadRequest), nil
	}
	id := shared.GenerateUUID()
	g := &GeoMatchSet{
		ID:          id,
		Name:        name,
		Constraints: "[]",
		ChangeToken: strParam(params, "ChangeToken"),
	}
	if err := p.store.CreateGeoMatchSet(g); err != nil {
		return nil, err
	}
	return jsonResp(http.StatusOK, map[string]any{
		"GeoMatchSet": map[string]any{
			"GeoMatchSetId":       id,
			"Name":                name,
			"GeoMatchConstraints": []any{},
		},
		"ChangeToken": shared.GenerateUUID(),
	})
}

func (p *Provider) getGeoMatchSet(params map[string]any) (*plugin.Response, error) {
	id := strParam(params, "GeoMatchSetId")
	if id == "" {
		return wafError("WAFInvalidParameterException", "GeoMatchSetId is required", http.StatusBadRequest), nil
	}
	g, err := p.store.GetGeoMatchSet(id)
	if err != nil {
		if err == errNotFound {
			return wafError("WAFNonexistentItemException", "GeoMatchSet not found", http.StatusBadRequest), nil
		}
		return nil, err
	}
	var constraints any
	_ = json.Unmarshal([]byte(g.Constraints), &constraints)
	if constraints == nil {
		constraints = []any{}
	}
	return jsonResp(http.StatusOK, map[string]any{
		"GeoMatchSet": map[string]any{
			"GeoMatchSetId":       g.ID,
			"Name":                g.Name,
			"GeoMatchConstraints": constraints,
		},
	})
}

func (p *Provider) listGeoMatchSets(_ map[string]any) (*plugin.Response, error) {
	sets, err := p.store.ListGeoMatchSets()
	if err != nil {
		return nil, err
	}
	items := make([]map[string]any, 0, len(sets))
	for _, g := range sets {
		items = append(items, map[string]any{"GeoMatchSetId": g.ID, "Name": g.Name})
	}
	return jsonResp(http.StatusOK, map[string]any{"GeoMatchSets": items})
}

func (p *Provider) updateGeoMatchSet(params map[string]any) (*plugin.Response, error) {
	id := strParam(params, "GeoMatchSetId")
	if id == "" {
		return wafError("WAFInvalidParameterException", "GeoMatchSetId is required", http.StatusBadRequest), nil
	}
	g, err := p.store.GetGeoMatchSet(id)
	if err != nil {
		if err == errNotFound {
			return wafError("WAFNonexistentItemException", "GeoMatchSet not found", http.StatusBadRequest), nil
		}
		return nil, err
	}
	if updates, ok := params["Updates"].([]any); ok {
		var constraints []any
		_ = json.Unmarshal([]byte(g.Constraints), &constraints)
		if constraints == nil {
			constraints = []any{}
		}
		for _, u := range updates {
			if upd, ok := u.(map[string]any); ok {
				action := strFromMap(upd, "Action")
				if action == "INSERT" {
					if c, ok := upd["GeoMatchConstraint"]; ok {
						constraints = append(constraints, c)
					}
				}
			}
		}
		g.Constraints = marshalJSON(constraints)
	}
	g.ChangeToken = strParam(params, "ChangeToken")
	if err := p.store.UpdateGeoMatchSet(g); err != nil {
		return nil, err
	}
	return jsonResp(http.StatusOK, map[string]any{"ChangeToken": shared.GenerateUUID()})
}

func (p *Provider) deleteGeoMatchSet(params map[string]any) (*plugin.Response, error) {
	id := strParam(params, "GeoMatchSetId")
	if id == "" {
		return wafError("WAFInvalidParameterException", "GeoMatchSetId is required", http.StatusBadRequest), nil
	}
	if err := p.store.DeleteGeoMatchSet(id); err != nil {
		if err == errNotFound {
			return wafError("WAFNonexistentItemException", "GeoMatchSet not found", http.StatusBadRequest), nil
		}
		return nil, err
	}
	return jsonResp(http.StatusOK, map[string]any{"ChangeToken": shared.GenerateUUID()})
}

// --- RegexMatchSet ---

func (p *Provider) createRegexMatchSet(params map[string]any) (*plugin.Response, error) {
	name := strParam(params, "Name")
	if name == "" {
		return wafError("WAFInvalidParameterException", "Name is required", http.StatusBadRequest), nil
	}
	id := shared.GenerateUUID()
	r := &RegexMatchSet{
		ID:          id,
		Name:        name,
		Tuples:      "[]",
		ChangeToken: strParam(params, "ChangeToken"),
	}
	if err := p.store.CreateRegexMatchSet(r); err != nil {
		return nil, err
	}
	return jsonResp(http.StatusOK, map[string]any{
		"RegexMatchSet": map[string]any{
			"RegexMatchSetId":  id,
			"Name":             name,
			"RegexMatchTuples": []any{},
		},
		"ChangeToken": shared.GenerateUUID(),
	})
}

func (p *Provider) getRegexMatchSet(params map[string]any) (*plugin.Response, error) {
	id := strParam(params, "RegexMatchSetId")
	if id == "" {
		return wafError("WAFInvalidParameterException", "RegexMatchSetId is required", http.StatusBadRequest), nil
	}
	r, err := p.store.GetRegexMatchSet(id)
	if err != nil {
		if err == errNotFound {
			return wafError("WAFNonexistentItemException", "RegexMatchSet not found", http.StatusBadRequest), nil
		}
		return nil, err
	}
	var tuples any
	_ = json.Unmarshal([]byte(r.Tuples), &tuples)
	if tuples == nil {
		tuples = []any{}
	}
	return jsonResp(http.StatusOK, map[string]any{
		"RegexMatchSet": map[string]any{
			"RegexMatchSetId":  r.ID,
			"Name":             r.Name,
			"RegexMatchTuples": tuples,
		},
	})
}

func (p *Provider) listRegexMatchSets(_ map[string]any) (*plugin.Response, error) {
	sets, err := p.store.ListRegexMatchSets()
	if err != nil {
		return nil, err
	}
	items := make([]map[string]any, 0, len(sets))
	for _, r := range sets {
		items = append(items, map[string]any{"RegexMatchSetId": r.ID, "Name": r.Name})
	}
	return jsonResp(http.StatusOK, map[string]any{"RegexMatchSets": items})
}

func (p *Provider) updateRegexMatchSet(params map[string]any) (*plugin.Response, error) {
	id := strParam(params, "RegexMatchSetId")
	if id == "" {
		return wafError("WAFInvalidParameterException", "RegexMatchSetId is required", http.StatusBadRequest), nil
	}
	r, err := p.store.GetRegexMatchSet(id)
	if err != nil {
		if err == errNotFound {
			return wafError("WAFNonexistentItemException", "RegexMatchSet not found", http.StatusBadRequest), nil
		}
		return nil, err
	}
	if updates, ok := params["Updates"].([]any); ok {
		var tuples []any
		_ = json.Unmarshal([]byte(r.Tuples), &tuples)
		if tuples == nil {
			tuples = []any{}
		}
		for _, u := range updates {
			if upd, ok := u.(map[string]any); ok {
				action := strFromMap(upd, "Action")
				if action == "INSERT" {
					if tup, ok := upd["RegexMatchTuple"]; ok {
						tuples = append(tuples, tup)
					}
				}
			}
		}
		r.Tuples = marshalJSON(tuples)
	}
	r.ChangeToken = strParam(params, "ChangeToken")
	if err := p.store.UpdateRegexMatchSet(r); err != nil {
		return nil, err
	}
	return jsonResp(http.StatusOK, map[string]any{"ChangeToken": shared.GenerateUUID()})
}

func (p *Provider) deleteRegexMatchSet(params map[string]any) (*plugin.Response, error) {
	id := strParam(params, "RegexMatchSetId")
	if id == "" {
		return wafError("WAFInvalidParameterException", "RegexMatchSetId is required", http.StatusBadRequest), nil
	}
	if err := p.store.DeleteRegexMatchSet(id); err != nil {
		if err == errNotFound {
			return wafError("WAFNonexistentItemException", "RegexMatchSet not found", http.StatusBadRequest), nil
		}
		return nil, err
	}
	return jsonResp(http.StatusOK, map[string]any{"ChangeToken": shared.GenerateUUID()})
}

// --- Logging ---

func (p *Provider) putLoggingConfiguration(params map[string]any) (*plugin.Response, error) {
	lc, ok := params["LoggingConfiguration"].(map[string]any)
	if !ok {
		return wafError("WAFInvalidParameterException", "LoggingConfiguration is required", http.StatusBadRequest), nil
	}
	arn := strFromMap(lc, "ResourceArn")
	if arn == "" {
		return wafError("WAFInvalidParameterException", "ResourceArn is required", http.StatusBadRequest), nil
	}
	config := marshalJSON(lc)
	if err := p.store.PutLoggingConfig(arn, config); err != nil {
		return nil, err
	}
	return jsonResp(http.StatusOK, map[string]any{"LoggingConfiguration": lc})
}

func (p *Provider) getLoggingConfiguration(params map[string]any) (*plugin.Response, error) {
	arn := strParam(params, "ResourceArn")
	if arn == "" {
		return wafError("WAFInvalidParameterException", "ResourceArn is required", http.StatusBadRequest), nil
	}
	config, err := p.store.GetLoggingConfig(arn)
	if err != nil {
		if err == errNotFound {
			return wafError("WAFNonexistentItemException", "logging configuration not found", http.StatusBadRequest), nil
		}
		return nil, err
	}
	var lc any
	_ = json.Unmarshal([]byte(config), &lc)
	return jsonResp(http.StatusOK, map[string]any{"LoggingConfiguration": lc})
}

func (p *Provider) deleteLoggingConfiguration(params map[string]any) (*plugin.Response, error) {
	arn := strParam(params, "ResourceArn")
	if arn == "" {
		return wafError("WAFInvalidParameterException", "ResourceArn is required", http.StatusBadRequest), nil
	}
	if err := p.store.DeleteLoggingConfig(arn); err != nil {
		if err == errNotFound {
			return wafError("WAFNonexistentItemException", "logging configuration not found", http.StatusBadRequest), nil
		}
		return nil, err
	}
	return jsonResp(http.StatusOK, map[string]any{})
}

func (p *Provider) listLoggingConfigurations(_ map[string]any) (*plugin.Response, error) {
	configs, err := p.store.ListLoggingConfigs()
	if err != nil {
		return nil, err
	}
	items := make([]any, 0, len(configs))
	for _, c := range configs {
		var lc any
		_ = json.Unmarshal([]byte(c["config"]), &lc)
		items = append(items, lc)
	}
	return jsonResp(http.StatusOK, map[string]any{"LoggingConfigurations": items})
}

// --- Permission Policy ---

func (p *Provider) putPermissionPolicy(params map[string]any) (*plugin.Response, error) {
	arn := strParam(params, "ResourceArn")
	policy := strParam(params, "Policy")
	if arn == "" {
		return wafError("WAFInvalidParameterException", "ResourceArn is required", http.StatusBadRequest), nil
	}
	if err := p.store.PutPermissionPolicy(arn, policy); err != nil {
		return nil, err
	}
	return jsonResp(http.StatusOK, map[string]any{})
}

func (p *Provider) getPermissionPolicy(params map[string]any) (*plugin.Response, error) {
	arn := strParam(params, "ResourceArn")
	if arn == "" {
		return wafError("WAFInvalidParameterException", "ResourceArn is required", http.StatusBadRequest), nil
	}
	policy, err := p.store.GetPermissionPolicy(arn)
	if err != nil {
		if err == errNotFound {
			return wafError("WAFNonexistentItemException", "permission policy not found", http.StatusBadRequest), nil
		}
		return nil, err
	}
	return jsonResp(http.StatusOK, map[string]string{"Policy": policy})
}

func (p *Provider) deletePermissionPolicy(params map[string]any) (*plugin.Response, error) {
	arn := strParam(params, "ResourceArn")
	if arn == "" {
		return wafError("WAFInvalidParameterException", "ResourceArn is required", http.StatusBadRequest), nil
	}
	if err := p.store.DeletePermissionPolicy(arn); err != nil {
		if err == errNotFound {
			return wafError("WAFNonexistentItemException", "permission policy not found", http.StatusBadRequest), nil
		}
		return nil, err
	}
	return jsonResp(http.StatusOK, map[string]any{})
}

// --- Tags ---

func (p *Provider) tagResource(params map[string]any) (*plugin.Response, error) {
	arn := strParam(params, "ResourceARN")
	if arn == "" {
		return wafError("WAFInvalidParameterException", "ResourceARN is required", http.StatusBadRequest), nil
	}
	tags := parseTagList(params["Tags"])
	if err := p.store.AddTags(arn, tags); err != nil {
		return nil, err
	}
	return jsonResp(http.StatusOK, map[string]any{})
}

func (p *Provider) untagResource(params map[string]any) (*plugin.Response, error) {
	arn := strParam(params, "ResourceARN")
	if arn == "" {
		return wafError("WAFInvalidParameterException", "ResourceARN is required", http.StatusBadRequest), nil
	}
	keys := parseTagKeyList(params["TagKeys"])
	if err := p.store.RemoveTags(arn, keys); err != nil {
		return nil, err
	}
	return jsonResp(http.StatusOK, map[string]any{})
}

func (p *Provider) listTagsForResource(params map[string]any) (*plugin.Response, error) {
	arn := strParam(params, "ResourceARN")
	if arn == "" {
		return wafError("WAFInvalidParameterException", "ResourceARN is required", http.StatusBadRequest), nil
	}
	tags, err := p.store.ListTags(arn)
	if err != nil {
		return nil, err
	}
	tagList := make([]map[string]string, 0, len(tags))
	for k, v := range tags {
		tagList = append(tagList, map[string]string{"Key": k, "Value": v})
	}
	return jsonResp(http.StatusOK, map[string]any{
		"TagInfoForResource": map[string]any{
			"ResourceARN": arn,
			"TagList":     tagList,
		},
	})
}

// --- util ---

func strFromMap(m map[string]any, key string) string {
	if v, ok := m[key]; ok {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}

func parseTagList(v any) map[string]string {
	tags := make(map[string]string)
	if list, ok := v.([]any); ok {
		for _, item := range list {
			if m, ok := item.(map[string]any); ok {
				k := strFromMap(m, "Key")
				val := strFromMap(m, "Value")
				if k != "" {
					tags[k] = val
				}
			}
		}
	}
	return tags
}

func parseTagKeyList(v any) []string {
	var keys []string
	if list, ok := v.([]any); ok {
		for _, item := range list {
			if s, ok := item.(string); ok {
				keys = append(keys, s)
			}
		}
	}
	return keys
}
