// SPDX-License-Identifier: Apache-2.0

// internal/services/route53resolver/provider.go
package route53resolver

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

type Provider struct {
	store *Store
}

func (p *Provider) ServiceID() string             { return "route53resolver" }
func (p *Provider) ServiceName() string           { return "Route53Resolver" }
func (p *Provider) Protocol() plugin.ProtocolType { return plugin.ProtocolJSON11 }

func (p *Provider) Init(cfg plugin.PluginConfig) error {
	dataDir := cfg.DataDir
	if dataDir == "" {
		dataDir = "."
	}
	var err error
	p.store, err = NewStore(filepath.Join(dataDir, "route53resolver"))
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

	action := op
	if action == "" {
		target := req.Header.Get("X-Amz-Target")
		if idx := strings.LastIndex(target, "."); idx >= 0 {
			action = target[idx+1:]
		}
	}

	switch action {
	// --- ResolverEndpoint ---
	case "CreateResolverEndpoint":
		return p.createResolverEndpoint(params)
	case "GetResolverEndpoint":
		return p.getResolverEndpoint(params)
	case "ListResolverEndpoints":
		return p.listResolverEndpoints(params)
	case "UpdateResolverEndpoint":
		return p.updateResolverEndpoint(params)
	case "DeleteResolverEndpoint":
		return p.deleteResolverEndpoint(params)
	case "AssociateResolverEndpointIpAddress":
		return p.associateResolverEndpointIpAddress(params)
	case "DisassociateResolverEndpointIpAddress":
		return p.disassociateResolverEndpointIpAddress(params)
	case "ListResolverEndpointIpAddresses":
		return p.listResolverEndpointIpAddresses(params)

	// --- ResolverRule ---
	case "CreateResolverRule":
		return p.createResolverRule(params)
	case "GetResolverRule":
		return p.getResolverRule(params)
	case "ListResolverRules":
		return p.listResolverRules(params)
	case "UpdateResolverRule":
		return p.updateResolverRule(params)
	case "DeleteResolverRule":
		return p.deleteResolverRule(params)
	case "AssociateResolverRule":
		return p.associateResolverRule(params)
	case "DisassociateResolverRule":
		return p.disassociateResolverRule(params)
	case "GetResolverRuleAssociation":
		return p.getResolverRuleAssociation(params)
	case "ListResolverRuleAssociations":
		return p.listResolverRuleAssociations(params)
	case "GetResolverRulePolicy":
		return p.getResolverRulePolicy(params)
	case "PutResolverRulePolicy":
		return p.putResolverRulePolicy(params)

	// --- QueryLogConfig ---
	case "CreateResolverQueryLogConfig":
		return p.createResolverQueryLogConfig(params)
	case "GetResolverQueryLogConfig":
		return p.getResolverQueryLogConfig(params)
	case "ListResolverQueryLogConfigs":
		return p.listResolverQueryLogConfigs(params)
	case "DeleteResolverQueryLogConfig":
		return p.deleteResolverQueryLogConfig(params)
	case "AssociateResolverQueryLogConfig":
		return p.associateResolverQueryLogConfig(params)
	case "DisassociateResolverQueryLogConfig":
		return p.disassociateResolverQueryLogConfig(params)
	case "GetResolverQueryLogConfigAssociation":
		return p.getResolverQueryLogConfigAssociation(params)
	case "ListResolverQueryLogConfigAssociations":
		return p.listResolverQueryLogConfigAssociations(params)
	case "GetResolverQueryLogConfigPolicy":
		return p.getResolverQueryLogConfigPolicy(params)
	case "PutResolverQueryLogConfigPolicy":
		return p.putResolverQueryLogConfigPolicy(params)

	// --- FirewallRuleGroup ---
	case "CreateFirewallRuleGroup":
		return p.createFirewallRuleGroup(params)
	case "GetFirewallRuleGroup":
		return p.getFirewallRuleGroup(params)
	case "ListFirewallRuleGroups":
		return p.listFirewallRuleGroups(params)
	case "DeleteFirewallRuleGroup":
		return p.deleteFirewallRuleGroup(params)

	// --- FirewallRule ---
	case "CreateFirewallRule":
		return p.createFirewallRule(params)
	case "ListFirewallRules":
		return p.listFirewallRules(params)
	case "UpdateFirewallRule":
		return p.updateFirewallRule(params)
	case "DeleteFirewallRule":
		return p.deleteFirewallRule(params)

	// --- FirewallDomainList ---
	case "CreateFirewallDomainList":
		return p.createFirewallDomainList(params)
	case "GetFirewallDomainList":
		return p.getFirewallDomainList(params)
	case "ListFirewallDomainLists":
		return p.listFirewallDomainLists(params)
	case "DeleteFirewallDomainList":
		return p.deleteFirewallDomainList(params)
	case "ImportFirewallDomains":
		return p.importFirewallDomains(params)
	case "ListFirewallDomains":
		return p.listFirewallDomains(params)
	case "UpdateFirewallDomains":
		return p.updateFirewallDomains(params)

	// --- FirewallRuleGroupAssociation ---
	case "AssociateFirewallRuleGroup":
		return p.associateFirewallRuleGroup(params)
	case "GetFirewallRuleGroupAssociation":
		return p.getFirewallRuleGroupAssociation(params)
	case "ListFirewallRuleGroupAssociations":
		return p.listFirewallRuleGroupAssociations(params)
	case "DisassociateFirewallRuleGroup":
		return p.disassociateFirewallRuleGroup(params)
	case "UpdateFirewallRuleGroupAssociation":
		return p.updateFirewallRuleGroupAssociation(params)
	case "GetFirewallRuleGroupPolicy":
		return p.getFirewallRuleGroupPolicy(params)
	case "PutFirewallRuleGroupPolicy":
		return p.putFirewallRuleGroupPolicy(params)

	// --- FirewallConfig (per-VPC) ---
	case "GetFirewallConfig":
		return p.getFirewallConfig(params)
	case "UpdateFirewallConfig":
		return p.updateFirewallConfig(params)
	case "ListFirewallConfigs":
		return p.listFirewallConfigs(params)

	// --- ResolverConfig ---
	case "GetResolverConfig":
		return p.getResolverConfig(params)
	case "UpdateResolverConfig":
		return p.updateResolverConfig(params)
	case "ListResolverConfigs":
		return p.listResolverConfigs(params)

	// --- ResolverDnssecConfig ---
	case "GetResolverDnssecConfig":
		return p.getResolverDnssecConfig(params)
	case "UpdateResolverDnssecConfig":
		return p.updateResolverDnssecConfig(params)
	case "ListResolverDnssecConfigs":
		return p.listResolverDnssecConfigs(params)

	// --- Tags ---
	case "TagResource":
		return p.tagResource(params)
	case "UntagResource":
		return p.untagResource(params)
	case "ListTagsForResource":
		return p.listTagsForResource(params)

	// --- Outpost (stub success/empty) ---
	case "CreateOutpostResolver", "GetOutpostResolver", "ListOutpostResolvers",
		"UpdateOutpostResolver", "DeleteOutpostResolver":
		return shared.JSONResponse(http.StatusOK, map[string]any{})

	default:
		return shared.JSONError("InvalidAction", fmt.Sprintf("unknown action: %s", action), http.StatusBadRequest), nil
	}
}

func (p *Provider) ListResources(_ context.Context) ([]plugin.Resource, error) {
	eps, err := p.store.ListEndpoints()
	if err != nil {
		return nil, err
	}
	res := make([]plugin.Resource, 0, len(eps))
	for _, ep := range eps {
		res = append(res, plugin.Resource{Type: "resolver-endpoint", ID: ep.ID, Name: ep.Name})
	}
	return res, nil
}

func (p *Provider) GetMetrics(_ context.Context) (*plugin.ServiceMetrics, error) {
	return &plugin.ServiceMetrics{}, nil
}

// ==================== helpers ====================

func str(params map[string]any, key string) string {
	v, _ := params[key].(string)
	return v
}

func intParam(params map[string]any, key string) int {
	switch v := params[key].(type) {
	case float64:
		return int(v)
	case int:
		return v
	}
	return 0
}

func parseTags(rawTags []any) map[string]string {
	tags := make(map[string]string)
	for _, t := range rawTags {
		tag, _ := t.(map[string]any)
		k, _ := tag["Key"].(string)
		if k == "" {
			k, _ = tag["key"].(string)
		}
		v, _ := tag["Value"].(string)
		if v == "" {
			v, _ = tag["value"].(string)
		}
		if k != "" {
			tags[k] = v
		}
	}
	return tags
}

func endpointToMap(r *endpointRow) map[string]any {
	var sgs []string
	_ = json.Unmarshal([]byte(r.SecurityGroups), &sgs)
	var ips []ipAddrEntry
	_ = json.Unmarshal([]byte(r.IPAddresses), &ips)
	ipResponses := make([]map[string]any, len(ips))
	for i, ip := range ips {
		ipResponses[i] = map[string]any{
			"IpId":     ip.IPID,
			"SubnetId": ip.SubnetID,
			"Ip":       ip.IP,
			"Status":   ip.Status,
		}
	}
	return map[string]any{
		"Id":               r.ID,
		"Arn":              r.ARN,
		"Name":             r.Name,
		"Direction":        r.Direction,
		"SecurityGroupIds": sgs,
		"IpAddressCount":   len(ips),
		"Status":           r.Status,
		"CreationTime":     time.Unix(r.CreatedAt, 0).UTC().Format(time.RFC3339),
		"ModificationTime": time.Unix(r.CreatedAt, 0).UTC().Format(time.RFC3339),
	}
}

func ruleToMap(r *ruleRow) map[string]any {
	var tIPs []map[string]any
	_ = json.Unmarshal([]byte(r.TargetIPs), &tIPs)
	if tIPs == nil {
		tIPs = []map[string]any{}
	}
	return map[string]any{
		"Id":                 r.ID,
		"Arn":                r.ARN,
		"Name":               r.Name,
		"DomainName":         r.DomainName,
		"RuleType":           r.RuleType,
		"ResolverEndpointId": r.ResolverEndpointID,
		"TargetIps":          tIPs,
		"Status":             r.Status,
		"CreationTime":       time.Unix(r.CreatedAt, 0).UTC().Format(time.RFC3339),
		"ModificationTime":   time.Unix(r.CreatedAt, 0).UTC().Format(time.RFC3339),
	}
}

func ruleAssocToMap(r *ruleAssocRow) map[string]any {
	return map[string]any{
		"Id":             r.ID,
		"Name":           r.Name,
		"ResolverRuleId": r.ResolverRuleID,
		"VPCId":          r.VPCID,
		"Status":         r.Status,
	}
}

func queryLogToMap(r *queryLogRow) map[string]any {
	return map[string]any{
		"Id":             r.ID,
		"Arn":            r.ARN,
		"Name":           r.Name,
		"DestinationArn": r.Destination,
		"Status":         r.Status,
		"CreationTime":   time.Unix(r.CreatedAt, 0).UTC().Format(time.RFC3339),
	}
}

func queryLogAssocToMap(r *queryLogAssocRow) map[string]any {
	return map[string]any{
		"Id":                       r.ID,
		"ResolverQueryLogConfigId": r.ConfigID,
		"ResourceId":               r.ResourceID,
		"Status":                   r.Status,
	}
}

func fwRuleGroupToMap(r *fwRuleGroupRow) map[string]any {
	return map[string]any{
		"Id":           r.ID,
		"Arn":          r.ARN,
		"Name":         r.Name,
		"Status":       r.Status,
		"RuleCount":    r.RuleCount,
		"CreationTime": time.Unix(r.CreatedAt, 0).UTC().Format(time.RFC3339),
	}
}

func fwRuleToMap(r *fwRuleRow) map[string]any {
	return map[string]any{
		"FirewallRuleGroupId":  r.GroupID,
		"FirewallDomainListId": r.DomainListID,
		"Name":                 r.Name,
		"Priority":             r.Priority,
		"Action":               r.Action,
		"BlockResponse":        r.BlockResponse,
	}
}

func fwDomainListToMap(r *fwDomainListRow) map[string]any {
	return map[string]any{
		"Id":           r.ID,
		"Arn":          r.ARN,
		"Name":         r.Name,
		"Status":       r.Status,
		"DomainCount":  r.DomainCount,
		"CreationTime": time.Unix(r.CreatedAt, 0).UTC().Format(time.RFC3339),
	}
}

func fwRuleGroupAssocToMap(r *fwRuleGroupAssocRow) map[string]any {
	return map[string]any{
		"Id":                  r.ID,
		"FirewallRuleGroupId": r.GroupID,
		"VpcId":               r.VPCID,
		"Name":                r.Name,
		"Priority":            r.Priority,
		"Status":              r.Status,
	}
}

// ==================== ResolverEndpoint ====================

func (p *Provider) createResolverEndpoint(params map[string]any) (*plugin.Response, error) {
	name := str(params, "Name")
	direction := str(params, "Direction")
	if direction == "" {
		direction = "INBOUND"
	}

	// SecurityGroupIds
	var sgIDs []string
	if raw, ok := params["SecurityGroupIds"].([]any); ok {
		for _, s := range raw {
			if sv, ok := s.(string); ok {
				sgIDs = append(sgIDs, sv)
			}
		}
	}

	// IpAddresses
	var ipAddrs []ipAddrEntry
	if raw, ok := params["IpAddresses"].([]any); ok {
		for _, item := range raw {
			m, _ := item.(map[string]any)
			subnet, _ := m["SubnetId"].(string)
			ip, _ := m["Ip"].(string)
			ipAddrs = append(ipAddrs, ipAddrEntry{
				SubnetID: subnet,
				IP:       ip,
				IPID:     shared.GenerateID("rni-", 21),
				Status:   "ATTACHED",
			})
		}
	}

	id := shared.GenerateID("rslvr-in-", 24)
	arn := shared.BuildARN("route53resolver", "resolver-endpoint", id)

	ep, err := p.store.CreateEndpoint(id, arn, name, direction, sgIDs, ipAddrs)
	if err != nil {
		return nil, err
	}

	if rawTags, ok := params["Tags"].([]any); ok {
		_ = p.store.tags.AddTags(arn, parseTags(rawTags))
	}

	return shared.JSONResponse(http.StatusOK, map[string]any{
		"ResolverEndpoint": endpointToMap(ep),
	})
}

func (p *Provider) getResolverEndpoint(params map[string]any) (*plugin.Response, error) {
	id := str(params, "ResolverEndpointId")
	if id == "" {
		return shared.JSONError("ValidationException", "ResolverEndpointId is required", http.StatusBadRequest), nil
	}
	ep, err := p.store.GetEndpoint(id)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "resolver endpoint not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"ResolverEndpoint": endpointToMap(ep)})
}

func (p *Provider) listResolverEndpoints(_ map[string]any) (*plugin.Response, error) {
	eps, err := p.store.ListEndpoints()
	if err != nil {
		return nil, err
	}
	list := make([]map[string]any, 0, len(eps))
	for i := range eps {
		list = append(list, endpointToMap(&eps[i]))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"ResolverEndpoints": list,
		"MaxResults":        len(list),
	})
}

func (p *Provider) updateResolverEndpoint(params map[string]any) (*plugin.Response, error) {
	id := str(params, "ResolverEndpointId")
	name := str(params, "Name")
	if err := p.store.UpdateEndpoint(id, name); err != nil {
		return shared.JSONError("ResourceNotFoundException", "resolver endpoint not found", http.StatusBadRequest), nil
	}
	ep, _ := p.store.GetEndpoint(id)
	return shared.JSONResponse(http.StatusOK, map[string]any{"ResolverEndpoint": endpointToMap(ep)})
}

func (p *Provider) deleteResolverEndpoint(params map[string]any) (*plugin.Response, error) {
	id := str(params, "ResolverEndpointId")
	ep, err := p.store.GetEndpoint(id)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "resolver endpoint not found", http.StatusBadRequest), nil
	}
	_ = p.store.tags.DeleteAllTags(ep.ARN)
	if err := p.store.DeleteEndpoint(id); err != nil {
		return shared.JSONError("ResourceNotFoundException", "resolver endpoint not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"ResolverEndpoint": endpointToMap(ep)})
}

func (p *Provider) associateResolverEndpointIpAddress(params map[string]any) (*plugin.Response, error) {
	id := str(params, "ResolverEndpointId")
	ipMap, _ := params["IpAddress"].(map[string]any)
	subnet, _ := ipMap["SubnetId"].(string)
	ip, _ := ipMap["Ip"].(string)
	entry := ipAddrEntry{
		SubnetID: subnet,
		IP:       ip,
		IPID:     shared.GenerateID("rni-", 21),
		Status:   "ATTACHED",
	}
	ep, err := p.store.AssociateEndpointIP(id, entry)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "resolver endpoint not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"ResolverEndpoint": endpointToMap(ep)})
}

func (p *Provider) disassociateResolverEndpointIpAddress(params map[string]any) (*plugin.Response, error) {
	id := str(params, "ResolverEndpointId")
	ipMap, _ := params["IpAddress"].(map[string]any)
	ipID, _ := ipMap["IpId"].(string)
	ep, err := p.store.DisassociateEndpointIP(id, ipID)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "resolver endpoint not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"ResolverEndpoint": endpointToMap(ep)})
}

func (p *Provider) listResolverEndpointIpAddresses(params map[string]any) (*plugin.Response, error) {
	id := str(params, "ResolverEndpointId")
	ips, err := p.store.ListEndpointIPs(id)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "resolver endpoint not found", http.StatusBadRequest), nil
	}
	list := make([]map[string]any, 0, len(ips))
	for _, ip := range ips {
		list = append(list, map[string]any{
			"IpId":     ip.IPID,
			"SubnetId": ip.SubnetID,
			"Ip":       ip.IP,
			"Status":   ip.Status,
		})
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"IpAddresses": list,
		"MaxResults":  len(list),
	})
}

// ==================== ResolverRule ====================

func (p *Provider) createResolverRule(params map[string]any) (*plugin.Response, error) {
	name := str(params, "Name")
	domainName := str(params, "DomainName")
	if domainName != "" && !strings.HasSuffix(domainName, ".") {
		domainName += "."
	}
	ruleType := str(params, "RuleType")
	if ruleType == "" {
		ruleType = "FORWARD"
	}
	endpointID := str(params, "ResolverEndpointId")

	var targetIPs []map[string]any
	if raw, ok := params["TargetIps"].([]any); ok {
		for _, item := range raw {
			m, _ := item.(map[string]any)
			targetIPs = append(targetIPs, m)
		}
	}

	id := shared.GenerateID("rslvr-rr-", 24)
	arn := shared.BuildARN("route53resolver", "resolver-rule", id)
	rule, err := p.store.CreateRule(id, arn, name, domainName, ruleType, endpointID, targetIPs)
	if err != nil {
		return nil, err
	}

	if rawTags, ok := params["Tags"].([]any); ok {
		_ = p.store.tags.AddTags(arn, parseTags(rawTags))
	}

	return shared.JSONResponse(http.StatusOK, map[string]any{"ResolverRule": ruleToMap(rule)})
}

func (p *Provider) getResolverRule(params map[string]any) (*plugin.Response, error) {
	id := str(params, "ResolverRuleId")
	rule, err := p.store.GetRule(id)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "resolver rule not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"ResolverRule": ruleToMap(rule)})
}

func (p *Provider) listResolverRules(_ map[string]any) (*plugin.Response, error) {
	rules, err := p.store.ListRules()
	if err != nil {
		return nil, err
	}
	list := make([]map[string]any, 0, len(rules))
	for i := range rules {
		list = append(list, ruleToMap(&rules[i]))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"ResolverRules": list,
		"MaxResults":    len(list),
	})
}

func (p *Provider) updateResolverRule(params map[string]any) (*plugin.Response, error) {
	id := str(params, "ResolverRuleId")
	cfg, _ := params["Config"].(map[string]any)
	name, _ := cfg["Name"].(string)
	endpointID, _ := cfg["ResolverEndpointId"].(string)
	var targetIPs []map[string]any
	if raw, ok := cfg["TargetIps"].([]any); ok {
		for _, item := range raw {
			m, _ := item.(map[string]any)
			targetIPs = append(targetIPs, m)
		}
	}
	// get existing to fill defaults
	existing, err := p.store.GetRule(id)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "resolver rule not found", http.StatusBadRequest), nil
	}
	if name == "" {
		name = existing.Name
	}
	if endpointID == "" {
		endpointID = existing.ResolverEndpointID
	}
	if targetIPs == nil {
		_ = json.Unmarshal([]byte(existing.TargetIPs), &targetIPs)
	}
	if err := p.store.UpdateRule(id, name, endpointID, targetIPs); err != nil {
		return shared.JSONError("ResourceNotFoundException", "resolver rule not found", http.StatusBadRequest), nil
	}
	rule, _ := p.store.GetRule(id)
	return shared.JSONResponse(http.StatusOK, map[string]any{"ResolverRule": ruleToMap(rule)})
}

func (p *Provider) deleteResolverRule(params map[string]any) (*plugin.Response, error) {
	id := str(params, "ResolverRuleId")
	rule, err := p.store.GetRule(id)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "resolver rule not found", http.StatusBadRequest), nil
	}
	_ = p.store.tags.DeleteAllTags(rule.ARN)
	if err := p.store.DeleteRule(id); err != nil {
		return shared.JSONError("ResourceNotFoundException", "resolver rule not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"ResolverRule": ruleToMap(rule)})
}

func (p *Provider) associateResolverRule(params map[string]any) (*plugin.Response, error) {
	ruleID := str(params, "ResolverRuleId")
	vpcID := str(params, "VPCId")
	name := str(params, "Name")
	id := shared.GenerateID("rslvr-ra-", 24)
	assoc, err := p.store.AssociateRule(id, ruleID, vpcID, name)
	if err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"ResolverRuleAssociation": ruleAssocToMap(assoc)})
}

func (p *Provider) disassociateResolverRule(params map[string]any) (*plugin.Response, error) {
	ruleID := str(params, "ResolverRuleId")
	vpcID := str(params, "VPCId")
	assoc, err := p.store.DisassociateRule(ruleID, vpcID)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "rule association not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"ResolverRuleAssociation": ruleAssocToMap(assoc)})
}

func (p *Provider) getResolverRuleAssociation(params map[string]any) (*plugin.Response, error) {
	id := str(params, "ResolverRuleAssociationId")
	assoc, err := p.store.GetRuleAssociation(id)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "rule association not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"ResolverRuleAssociation": ruleAssocToMap(assoc)})
}

func (p *Provider) listResolverRuleAssociations(_ map[string]any) (*plugin.Response, error) {
	assocs, err := p.store.ListRuleAssociations()
	if err != nil {
		return nil, err
	}
	list := make([]map[string]any, 0, len(assocs))
	for i := range assocs {
		list = append(list, ruleAssocToMap(&assocs[i]))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"ResolverRuleAssociations": list,
		"MaxResults":               len(list),
	})
}

func (p *Provider) getResolverRulePolicy(params map[string]any) (*plugin.Response, error) {
	return shared.JSONResponse(http.StatusOK, map[string]any{"ResolverRulePolicy": ""})
}

func (p *Provider) putResolverRulePolicy(_ map[string]any) (*plugin.Response, error) {
	return shared.JSONResponse(http.StatusOK, map[string]any{"ReturnValue": true})
}

// ==================== QueryLogConfig ====================

func (p *Provider) createResolverQueryLogConfig(params map[string]any) (*plugin.Response, error) {
	name := str(params, "Name")
	dest := str(params, "DestinationArn")
	if name == "" {
		return shared.JSONError("ValidationException", "Name is required", http.StatusBadRequest), nil
	}
	id := shared.GenerateID("rqlc-", 28)
	arn := shared.BuildARN("route53resolver", "resolver-query-log-config", id)
	cfg, err := p.store.CreateQueryLogConfig(id, arn, name, dest)
	if err != nil {
		return nil, err
	}
	if rawTags, ok := params["Tags"].([]any); ok {
		_ = p.store.tags.AddTags(arn, parseTags(rawTags))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"ResolverQueryLogConfig": queryLogToMap(cfg)})
}

func (p *Provider) getResolverQueryLogConfig(params map[string]any) (*plugin.Response, error) {
	id := str(params, "ResolverQueryLogConfigId")
	cfg, err := p.store.GetQueryLogConfig(id)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "query log config not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"ResolverQueryLogConfig": queryLogToMap(cfg)})
}

func (p *Provider) listResolverQueryLogConfigs(_ map[string]any) (*plugin.Response, error) {
	cfgs, err := p.store.ListQueryLogConfigs()
	if err != nil {
		return nil, err
	}
	list := make([]map[string]any, 0, len(cfgs))
	for i := range cfgs {
		list = append(list, queryLogToMap(&cfgs[i]))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"ResolverQueryLogConfigs": list,
		"TotalCount":              len(list),
		"TotalFilteredCount":      len(list),
	})
}

func (p *Provider) deleteResolverQueryLogConfig(params map[string]any) (*plugin.Response, error) {
	id := str(params, "ResolverQueryLogConfigId")
	cfg, err := p.store.GetQueryLogConfig(id)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "query log config not found", http.StatusBadRequest), nil
	}
	_ = p.store.tags.DeleteAllTags(cfg.ARN)
	if err := p.store.DeleteQueryLogConfig(id); err != nil {
		return shared.JSONError("ResourceNotFoundException", "query log config not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"ResolverQueryLogConfig": queryLogToMap(cfg)})
}

func (p *Provider) associateResolverQueryLogConfig(params map[string]any) (*plugin.Response, error) {
	configID := str(params, "ResolverQueryLogConfigId")
	resourceID := str(params, "ResourceId")
	id := shared.GenerateID("rqlca-", 28)
	assoc, err := p.store.AssociateQueryLogConfig(id, configID, resourceID)
	if err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"ResolverQueryLogConfigAssociation": queryLogAssocToMap(assoc)})
}

func (p *Provider) disassociateResolverQueryLogConfig(params map[string]any) (*plugin.Response, error) {
	configID := str(params, "ResolverQueryLogConfigId")
	resourceID := str(params, "ResourceId")
	assoc, err := p.store.DisassociateQueryLogConfig(configID, resourceID)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "query log association not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"ResolverQueryLogConfigAssociation": queryLogAssocToMap(assoc)})
}

func (p *Provider) getResolverQueryLogConfigAssociation(params map[string]any) (*plugin.Response, error) {
	id := str(params, "ResolverQueryLogConfigAssociationId")
	assoc, err := p.store.GetQueryLogConfigAssociation(id)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "query log association not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"ResolverQueryLogConfigAssociation": queryLogAssocToMap(assoc)})
}

func (p *Provider) listResolverQueryLogConfigAssociations(_ map[string]any) (*plugin.Response, error) {
	assocs, err := p.store.ListQueryLogConfigAssociations()
	if err != nil {
		return nil, err
	}
	list := make([]map[string]any, 0, len(assocs))
	for i := range assocs {
		list = append(list, queryLogAssocToMap(&assocs[i]))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"ResolverQueryLogConfigAssociations": list,
		"TotalCount":                         len(list),
		"TotalFilteredCount":                 len(list),
	})
}

func (p *Provider) getResolverQueryLogConfigPolicy(_ map[string]any) (*plugin.Response, error) {
	return shared.JSONResponse(http.StatusOK, map[string]any{"ResolverQueryLogConfigPolicy": ""})
}

func (p *Provider) putResolverQueryLogConfigPolicy(_ map[string]any) (*plugin.Response, error) {
	return shared.JSONResponse(http.StatusOK, map[string]any{"ReturnValue": true})
}

// ==================== FirewallRuleGroup ====================

func (p *Provider) createFirewallRuleGroup(params map[string]any) (*plugin.Response, error) {
	name := str(params, "Name")
	if name == "" {
		return shared.JSONError("ValidationException", "Name is required", http.StatusBadRequest), nil
	}
	id := shared.GenerateID("rslvr-frg-", 28)
	arn := shared.BuildARN("route53resolver", "firewall-rule-group", id)
	grp, err := p.store.CreateFirewallRuleGroup(id, arn, name)
	if err != nil {
		return nil, err
	}
	if rawTags, ok := params["Tags"].([]any); ok {
		_ = p.store.tags.AddTags(arn, parseTags(rawTags))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"FirewallRuleGroup": fwRuleGroupToMap(grp)})
}

func (p *Provider) getFirewallRuleGroup(params map[string]any) (*plugin.Response, error) {
	id := str(params, "FirewallRuleGroupId")
	grp, err := p.store.GetFirewallRuleGroup(id)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "firewall rule group not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"FirewallRuleGroup": fwRuleGroupToMap(grp)})
}

func (p *Provider) listFirewallRuleGroups(_ map[string]any) (*plugin.Response, error) {
	grps, err := p.store.ListFirewallRuleGroups()
	if err != nil {
		return nil, err
	}
	list := make([]map[string]any, 0, len(grps))
	for i := range grps {
		list = append(list, map[string]any{
			"Id":   grps[i].ID,
			"Arn":  grps[i].ARN,
			"Name": grps[i].Name,
		})
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"FirewallRuleGroups": list})
}

func (p *Provider) deleteFirewallRuleGroup(params map[string]any) (*plugin.Response, error) {
	id := str(params, "FirewallRuleGroupId")
	grp, err := p.store.GetFirewallRuleGroup(id)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "firewall rule group not found", http.StatusBadRequest), nil
	}
	_ = p.store.tags.DeleteAllTags(grp.ARN)
	if err := p.store.DeleteFirewallRuleGroup(id); err != nil {
		return shared.JSONError("ResourceNotFoundException", "firewall rule group not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"FirewallRuleGroup": fwRuleGroupToMap(grp)})
}

// ==================== FirewallRule ====================

func (p *Provider) createFirewallRule(params map[string]any) (*plugin.Response, error) {
	groupID := str(params, "FirewallRuleGroupId")
	domainListID := str(params, "FirewallDomainListId")
	name := str(params, "Name")
	priority := intParam(params, "Priority")
	action := str(params, "Action")
	if action == "" {
		action = "BLOCK"
	}
	blockResponse := str(params, "BlockResponse")
	if blockResponse == "" {
		blockResponse = "NODATA"
	}
	rule, err := p.store.CreateFirewallRule(groupID, domainListID, name, priority, action, blockResponse)
	if err != nil {
		if sqlite_isUnique(err) {
			return shared.JSONError("ValidationException", "firewall rule already exists", http.StatusBadRequest), nil
		}
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"FirewallRule": fwRuleToMap(rule)})
}

func (p *Provider) listFirewallRules(params map[string]any) (*plugin.Response, error) {
	groupID := str(params, "FirewallRuleGroupId")
	rules, err := p.store.ListFirewallRules(groupID)
	if err != nil {
		return nil, err
	}
	list := make([]map[string]any, 0, len(rules))
	for i := range rules {
		list = append(list, fwRuleToMap(&rules[i]))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"FirewallRules": list})
}

func (p *Provider) updateFirewallRule(params map[string]any) (*plugin.Response, error) {
	groupID := str(params, "FirewallRuleGroupId")
	domainListID := str(params, "FirewallDomainListId")
	name := str(params, "Name")
	priority := intParam(params, "Priority")
	action := str(params, "Action")
	blockResponse := str(params, "BlockResponse")
	rule, err := p.store.UpdateFirewallRule(groupID, domainListID, name, priority, action, blockResponse)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "firewall rule not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"FirewallRule": fwRuleToMap(rule)})
}

func (p *Provider) deleteFirewallRule(params map[string]any) (*plugin.Response, error) {
	groupID := str(params, "FirewallRuleGroupId")
	domainListID := str(params, "FirewallDomainListId")
	// get before delete for response
	rules, _ := p.store.ListFirewallRules(groupID)
	var found *fwRuleRow
	for i := range rules {
		if rules[i].DomainListID == domainListID {
			found = &rules[i]
			break
		}
	}
	if err := p.store.DeleteFirewallRule(groupID, domainListID); err != nil {
		return shared.JSONError("ResourceNotFoundException", "firewall rule not found", http.StatusBadRequest), nil
	}
	if found == nil {
		return shared.JSONResponse(http.StatusOK, map[string]any{"FirewallRule": map[string]any{}})
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"FirewallRule": fwRuleToMap(found)})
}

// ==================== FirewallDomainList ====================

func (p *Provider) createFirewallDomainList(params map[string]any) (*plugin.Response, error) {
	name := str(params, "Name")
	if name == "" {
		return shared.JSONError("ValidationException", "Name is required", http.StatusBadRequest), nil
	}
	id := shared.GenerateID("rslvr-fdl-", 28)
	arn := shared.BuildARN("route53resolver", "firewall-domain-list", id)
	dl, err := p.store.CreateFirewallDomainList(id, arn, name)
	if err != nil {
		return nil, err
	}
	if rawTags, ok := params["Tags"].([]any); ok {
		_ = p.store.tags.AddTags(arn, parseTags(rawTags))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"FirewallDomainList": fwDomainListToMap(dl)})
}

func (p *Provider) getFirewallDomainList(params map[string]any) (*plugin.Response, error) {
	id := str(params, "FirewallDomainListId")
	dl, err := p.store.GetFirewallDomainList(id)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "firewall domain list not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"FirewallDomainList": fwDomainListToMap(dl)})
}

func (p *Provider) listFirewallDomainLists(_ map[string]any) (*plugin.Response, error) {
	dls, err := p.store.ListFirewallDomainLists()
	if err != nil {
		return nil, err
	}
	list := make([]map[string]any, 0, len(dls))
	for i := range dls {
		list = append(list, map[string]any{
			"Id":   dls[i].ID,
			"Arn":  dls[i].ARN,
			"Name": dls[i].Name,
		})
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"FirewallDomainLists": list})
}

func (p *Provider) deleteFirewallDomainList(params map[string]any) (*plugin.Response, error) {
	id := str(params, "FirewallDomainListId")
	dl, err := p.store.GetFirewallDomainList(id)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "firewall domain list not found", http.StatusBadRequest), nil
	}
	_ = p.store.tags.DeleteAllTags(dl.ARN)
	if err := p.store.DeleteFirewallDomainList(id); err != nil {
		return shared.JSONError("ResourceNotFoundException", "firewall domain list not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"FirewallDomainList": fwDomainListToMap(dl)})
}

func (p *Provider) importFirewallDomains(params map[string]any) (*plugin.Response, error) {
	id := str(params, "FirewallDomainListId")
	dl, err := p.store.GetFirewallDomainList(id)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "firewall domain list not found", http.StatusBadRequest), nil
	}
	// ImportFirewallDomains is a no-op in emulator (URL-based import not supported)
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"Id":     dl.ID,
		"Name":   dl.Name,
		"Status": dl.Status,
	})
}

func (p *Provider) listFirewallDomains(params map[string]any) (*plugin.Response, error) {
	id := str(params, "FirewallDomainListId")
	domains, err := p.store.ListFirewallDomains(id)
	if err != nil {
		return nil, err
	}
	if domains == nil {
		domains = []string{}
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"Domains": domains})
}

func (p *Provider) updateFirewallDomains(params map[string]any) (*plugin.Response, error) {
	id := str(params, "FirewallDomainListId")
	op := str(params, "Operation")
	if op == "" {
		op = "ADD"
	}
	var domains []string
	if raw, ok := params["Domains"].([]any); ok {
		for _, d := range raw {
			if s, ok := d.(string); ok {
				domains = append(domains, s)
			}
		}
	}
	if _, err := p.store.GetFirewallDomainList(id); err != nil {
		return shared.JSONError("ResourceNotFoundException", "firewall domain list not found", http.StatusBadRequest), nil
	}
	if err := p.store.UpdateFirewallDomains(id, op, domains); err != nil {
		return nil, err
	}
	// re-fetch for updated count
	dl, _ := p.store.GetFirewallDomainList(id)
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"Id":     dl.ID,
		"Name":   dl.Name,
		"Status": dl.Status,
	})
}

// ==================== FirewallRuleGroupAssociation ====================

func (p *Provider) associateFirewallRuleGroup(params map[string]any) (*plugin.Response, error) {
	groupID := str(params, "FirewallRuleGroupId")
	vpcID := str(params, "VpcId")
	name := str(params, "Name")
	priority := intParam(params, "Priority")
	id := shared.GenerateID("rslvr-frgassoc-", 28)
	assoc, err := p.store.AssociateFirewallRuleGroup(id, groupID, vpcID, name, priority)
	if err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"FirewallRuleGroupAssociation": fwRuleGroupAssocToMap(assoc)})
}

func (p *Provider) getFirewallRuleGroupAssociation(params map[string]any) (*plugin.Response, error) {
	id := str(params, "FirewallRuleGroupAssociationId")
	assoc, err := p.store.GetFirewallRuleGroupAssociation(id)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "firewall rule group association not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"FirewallRuleGroupAssociation": fwRuleGroupAssocToMap(assoc)})
}

func (p *Provider) listFirewallRuleGroupAssociations(params map[string]any) (*plugin.Response, error) {
	vpcID := str(params, "VpcId")
	assocs, err := p.store.ListFirewallRuleGroupAssociations(vpcID)
	if err != nil {
		return nil, err
	}
	list := make([]map[string]any, 0, len(assocs))
	for i := range assocs {
		list = append(list, fwRuleGroupAssocToMap(&assocs[i]))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"FirewallRuleGroupAssociations": list})
}

func (p *Provider) disassociateFirewallRuleGroup(params map[string]any) (*plugin.Response, error) {
	id := str(params, "FirewallRuleGroupAssociationId")
	assoc, err := p.store.DisassociateFirewallRuleGroup(id)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "firewall rule group association not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"FirewallRuleGroupAssociation": fwRuleGroupAssocToMap(assoc)})
}

func (p *Provider) updateFirewallRuleGroupAssociation(params map[string]any) (*plugin.Response, error) {
	id := str(params, "FirewallRuleGroupAssociationId")
	name := str(params, "Name")
	priority := intParam(params, "Priority")
	assoc, err := p.store.UpdateFirewallRuleGroupAssociation(id, name, priority)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "firewall rule group association not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"FirewallRuleGroupAssociation": fwRuleGroupAssocToMap(assoc)})
}

func (p *Provider) getFirewallRuleGroupPolicy(_ map[string]any) (*plugin.Response, error) {
	return shared.JSONResponse(http.StatusOK, map[string]any{"FirewallRuleGroupPolicy": ""})
}

func (p *Provider) putFirewallRuleGroupPolicy(_ map[string]any) (*plugin.Response, error) {
	return shared.JSONResponse(http.StatusOK, map[string]any{"ReturnValue": true})
}

// ==================== FirewallConfig (per-VPC, stub) ====================

func (p *Provider) getFirewallConfig(params map[string]any) (*plugin.Response, error) {
	resourceID := str(params, "ResourceId")
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"FirewallConfig": map[string]any{
			"Id":               shared.GenerateID("rslvr-fc-", 24),
			"ResourceId":       resourceID,
			"OwnerId":          shared.DefaultAccountID,
			"FirewallFailOpen": "DISABLED",
		},
	})
}

func (p *Provider) updateFirewallConfig(params map[string]any) (*plugin.Response, error) {
	resourceID := str(params, "ResourceId")
	failOpen := str(params, "FirewallFailOpen")
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"FirewallConfig": map[string]any{
			"Id":               shared.GenerateID("rslvr-fc-", 24),
			"ResourceId":       resourceID,
			"OwnerId":          shared.DefaultAccountID,
			"FirewallFailOpen": failOpen,
		},
	})
}

func (p *Provider) listFirewallConfigs(_ map[string]any) (*plugin.Response, error) {
	return shared.JSONResponse(http.StatusOK, map[string]any{"FirewallConfigs": []any{}})
}

// ==================== ResolverConfig (stub) ====================

func (p *Provider) getResolverConfig(params map[string]any) (*plugin.Response, error) {
	resourceID := str(params, "ResourceId")
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"ResolverConfig": map[string]any{
			"Id":                 shared.GenerateID("rslvr-rc-", 24),
			"ResourceId":         resourceID,
			"OwnerId":            shared.DefaultAccountID,
			"AutodefinedReverse": "DISABLED",
		},
	})
}

func (p *Provider) updateResolverConfig(params map[string]any) (*plugin.Response, error) {
	resourceID := str(params, "ResourceId")
	flag := str(params, "AutodefinedReverseFlag")
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"ResolverConfig": map[string]any{
			"Id":                 shared.GenerateID("rslvr-rc-", 24),
			"ResourceId":         resourceID,
			"OwnerId":            shared.DefaultAccountID,
			"AutodefinedReverse": flag,
		},
	})
}

func (p *Provider) listResolverConfigs(_ map[string]any) (*plugin.Response, error) {
	return shared.JSONResponse(http.StatusOK, map[string]any{"ResolverConfigs": []any{}})
}

// ==================== ResolverDnssecConfig (stub) ====================

func (p *Provider) getResolverDnssecConfig(params map[string]any) (*plugin.Response, error) {
	resourceID := str(params, "ResourceId")
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"ResolverDNSSECConfig": map[string]any{
			"Id":               shared.GenerateID("rslvr-dnssec-", 24),
			"ResourceId":       resourceID,
			"OwnerId":          shared.DefaultAccountID,
			"ValidationStatus": "DISABLED",
		},
	})
}

func (p *Provider) updateResolverDnssecConfig(params map[string]any) (*plugin.Response, error) {
	resourceID := str(params, "ResourceId")
	validation := str(params, "Validation")
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"ResolverDNSSECConfig": map[string]any{
			"Id":               shared.GenerateID("rslvr-dnssec-", 24),
			"ResourceId":       resourceID,
			"OwnerId":          shared.DefaultAccountID,
			"ValidationStatus": validation,
		},
	})
}

func (p *Provider) listResolverDnssecConfigs(_ map[string]any) (*plugin.Response, error) {
	return shared.JSONResponse(http.StatusOK, map[string]any{"ResolverDnssecConfigs": []any{}})
}

// ==================== Tags ====================

func (p *Provider) tagResource(params map[string]any) (*plugin.Response, error) {
	arn := str(params, "ResourceArn")
	rawTags, _ := params["Tags"].([]any)
	if err := p.store.tags.AddTags(arn, parseTags(rawTags)); err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) untagResource(params map[string]any) (*plugin.Response, error) {
	arn := str(params, "ResourceArn")
	rawKeys, _ := params["TagKeys"].([]any)
	keys := make([]string, 0, len(rawKeys))
	for _, k := range rawKeys {
		if s, ok := k.(string); ok {
			keys = append(keys, s)
		}
	}
	if err := p.store.tags.RemoveTags(arn, keys); err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) listTagsForResource(params map[string]any) (*plugin.Response, error) {
	arn := str(params, "ResourceArn")
	tags, err := p.store.tags.ListTags(arn)
	if err != nil {
		return nil, err
	}
	tagList := make([]map[string]string, 0, len(tags))
	for k, v := range tags {
		tagList = append(tagList, map[string]string{"Key": k, "Value": v})
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"Tags": tagList})
}

// ==================== misc ====================

func sqlite_isUnique(err error) bool {
	return err != nil && strings.Contains(err.Error(), "UNIQUE constraint failed")
}
