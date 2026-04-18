// SPDX-License-Identifier: Apache-2.0

// internal/services/route53resolver/provider_test.go
package route53resolver

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/skyoo2003/devcloud/internal/plugin"
)

func newTestProvider(t *testing.T) *Provider {
	t.Helper()
	dir, err := os.MkdirTemp("", "r53resolver-test-*")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	p := &Provider{}
	if err := p.Init(plugin.PluginConfig{DataDir: dir}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
	return p
}

func call(t *testing.T, p *Provider, action string, body map[string]any) map[string]any {
	t.Helper()
	b, _ := json.Marshal(body)
	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(b))
	resp, err := p.HandleRequest(context.Background(), action, req)
	if err != nil {
		t.Fatalf("action %s error: %v", action, err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("action %s: status %d body %s", action, resp.StatusCode, resp.Body)
	}
	var out map[string]any
	_ = json.Unmarshal(resp.Body, &out)
	return out
}

func TestResolverEndpointCRUD(t *testing.T) {
	p := newTestProvider(t)

	// Create
	out := call(t, p, "CreateResolverEndpoint", map[string]any{
		"Name":             "test-endpoint",
		"Direction":        "INBOUND",
		"SecurityGroupIds": []any{"sg-abc123"},
		"IpAddresses": []any{
			map[string]any{"SubnetId": "subnet-111", "Ip": "10.0.0.10"},
		},
		"Tags": []any{
			map[string]any{"Key": "Env", "Value": "test"},
		},
	})
	ep := out["ResolverEndpoint"].(map[string]any)
	epID := ep["Id"].(string)
	if epID == "" {
		t.Fatal("expected endpoint ID")
	}
	if ep["Direction"] != "INBOUND" {
		t.Errorf("expected INBOUND direction, got %v", ep["Direction"])
	}

	// Get
	out = call(t, p, "GetResolverEndpoint", map[string]any{"ResolverEndpointId": epID})
	if out["ResolverEndpoint"].(map[string]any)["Id"] != epID {
		t.Error("get returned wrong endpoint")
	}

	// List
	out = call(t, p, "ListResolverEndpoints", map[string]any{})
	eps := out["ResolverEndpoints"].([]any)
	if len(eps) != 1 {
		t.Errorf("expected 1 endpoint, got %d", len(eps))
	}

	// Update
	out = call(t, p, "UpdateResolverEndpoint", map[string]any{
		"ResolverEndpointId": epID,
		"Name":               "updated-endpoint",
	})
	if out["ResolverEndpoint"].(map[string]any)["Name"] != "updated-endpoint" {
		t.Error("name not updated")
	}

	// AssociateIp
	out = call(t, p, "AssociateResolverEndpointIpAddress", map[string]any{
		"ResolverEndpointId": epID,
		"IpAddress":          map[string]any{"SubnetId": "subnet-222", "Ip": "10.0.0.20"},
	})
	ipCount := int(out["ResolverEndpoint"].(map[string]any)["IpAddressCount"].(float64))
	if ipCount != 2 {
		t.Errorf("expected 2 IPs, got %d", ipCount)
	}

	// ListIpAddresses
	out = call(t, p, "ListResolverEndpointIpAddresses", map[string]any{"ResolverEndpointId": epID})
	ipList := out["IpAddresses"].([]any)
	if len(ipList) != 2 {
		t.Errorf("expected 2 IPs in list, got %d", len(ipList))
	}

	// DisassociateIp
	firstIP := ipList[0].(map[string]any)
	ipID := firstIP["IpId"].(string)
	call(t, p, "DisassociateResolverEndpointIpAddress", map[string]any{
		"ResolverEndpointId": epID,
		"IpAddress":          map[string]any{"IpId": ipID},
	})

	// Tags
	out = call(t, p, "ListTagsForResource", map[string]any{
		"ResourceArn": ep["Arn"].(string),
	})
	tags := out["Tags"].([]any)
	if len(tags) != 1 {
		t.Errorf("expected 1 tag, got %d", len(tags))
	}

	// Delete
	call(t, p, "DeleteResolverEndpoint", map[string]any{"ResolverEndpointId": epID})

	// Verify gone
	b, _ := json.Marshal(map[string]any{"ResolverEndpointId": epID})
	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(b))
	resp, _ := p.HandleRequest(context.Background(), "GetResolverEndpoint", req)
	if resp.StatusCode == http.StatusOK {
		t.Error("expected not found after delete")
	}
}

func TestResolverRuleCRUD(t *testing.T) {
	p := newTestProvider(t)

	// Create
	out := call(t, p, "CreateResolverRule", map[string]any{
		"Name":       "my-rule",
		"DomainName": "example.com",
		"RuleType":   "FORWARD",
		"TargetIps": []any{
			map[string]any{"Ip": "8.8.8.8", "Port": float64(53)},
		},
	})
	rule := out["ResolverRule"].(map[string]any)
	ruleID := rule["Id"].(string)
	if ruleID == "" {
		t.Fatal("expected rule ID")
	}

	// Get
	out = call(t, p, "GetResolverRule", map[string]any{"ResolverRuleId": ruleID})
	if out["ResolverRule"].(map[string]any)["DomainName"] != "example.com." {
		t.Error("domain name mismatch")
	}

	// List
	out = call(t, p, "ListResolverRules", map[string]any{})
	rules := out["ResolverRules"].([]any)
	if len(rules) != 1 {
		t.Errorf("expected 1 rule, got %d", len(rules))
	}

	// Update
	out = call(t, p, "UpdateResolverRule", map[string]any{
		"ResolverRuleId": ruleID,
		"Config": map[string]any{
			"Name": "updated-rule",
		},
	})
	if out["ResolverRule"].(map[string]any)["Name"] != "updated-rule" {
		t.Error("name not updated")
	}

	// Associate
	out = call(t, p, "AssociateResolverRule", map[string]any{
		"ResolverRuleId": ruleID,
		"VPCId":          "vpc-12345",
		"Name":           "assoc1",
	})
	assoc := out["ResolverRuleAssociation"].(map[string]any)
	assocID := assoc["Id"].(string)

	// GetAssociation
	out = call(t, p, "GetResolverRuleAssociation", map[string]any{"ResolverRuleAssociationId": assocID})
	if out["ResolverRuleAssociation"].(map[string]any)["VPCId"] != "vpc-12345" {
		t.Error("VPCId mismatch")
	}

	// ListAssociations
	out = call(t, p, "ListResolverRuleAssociations", map[string]any{})
	assocs := out["ResolverRuleAssociations"].([]any)
	if len(assocs) != 1 {
		t.Errorf("expected 1 association, got %d", len(assocs))
	}

	// Disassociate
	call(t, p, "DisassociateResolverRule", map[string]any{
		"ResolverRuleId": ruleID,
		"VPCId":          "vpc-12345",
	})

	// Policy
	out = call(t, p, "GetResolverRulePolicy", map[string]any{"Arn": rule["Arn"].(string)})
	if _, ok := out["ResolverRulePolicy"]; !ok {
		t.Error("expected ResolverRulePolicy key")
	}
	call(t, p, "PutResolverRulePolicy", map[string]any{"Arn": rule["Arn"].(string), "ResolverRulePolicy": "{}"})

	// Delete
	call(t, p, "DeleteResolverRule", map[string]any{"ResolverRuleId": ruleID})
}

func TestQueryLogConfigCRUD(t *testing.T) {
	p := newTestProvider(t)

	// Create
	out := call(t, p, "CreateResolverQueryLogConfig", map[string]any{
		"Name":           "my-log-config",
		"DestinationArn": "arn:aws:s3:::my-bucket",
	})
	cfg := out["ResolverQueryLogConfig"].(map[string]any)
	cfgID := cfg["Id"].(string)
	if cfgID == "" {
		t.Fatal("expected config ID")
	}

	// Get
	out = call(t, p, "GetResolverQueryLogConfig", map[string]any{"ResolverQueryLogConfigId": cfgID})
	if out["ResolverQueryLogConfig"].(map[string]any)["Name"] != "my-log-config" {
		t.Error("name mismatch")
	}

	// List
	out = call(t, p, "ListResolverQueryLogConfigs", map[string]any{})
	cfgs := out["ResolverQueryLogConfigs"].([]any)
	if len(cfgs) != 1 {
		t.Errorf("expected 1 config, got %d", len(cfgs))
	}

	// Associate
	out = call(t, p, "AssociateResolverQueryLogConfig", map[string]any{
		"ResolverQueryLogConfigId": cfgID,
		"ResourceId":               "vpc-aabbcc",
	})
	assoc := out["ResolverQueryLogConfigAssociation"].(map[string]any)
	assocID := assoc["Id"].(string)

	// GetAssociation
	out = call(t, p, "GetResolverQueryLogConfigAssociation", map[string]any{
		"ResolverQueryLogConfigAssociationId": assocID,
	})
	if out["ResolverQueryLogConfigAssociation"].(map[string]any)["ResourceId"] != "vpc-aabbcc" {
		t.Error("resource ID mismatch")
	}

	// ListAssociations
	out = call(t, p, "ListResolverQueryLogConfigAssociations", map[string]any{})
	assocs := out["ResolverQueryLogConfigAssociations"].([]any)
	if len(assocs) != 1 {
		t.Errorf("expected 1 association, got %d", len(assocs))
	}

	// Disassociate
	call(t, p, "DisassociateResolverQueryLogConfig", map[string]any{
		"ResolverQueryLogConfigId": cfgID,
		"ResourceId":               "vpc-aabbcc",
	})

	// Policy
	call(t, p, "PutResolverQueryLogConfigPolicy", map[string]any{"Arn": cfg["Arn"].(string)})
	out = call(t, p, "GetResolverQueryLogConfigPolicy", map[string]any{"Arn": cfg["Arn"].(string)})
	if _, ok := out["ResolverQueryLogConfigPolicy"]; !ok {
		t.Error("expected ResolverQueryLogConfigPolicy key")
	}

	// Delete
	call(t, p, "DeleteResolverQueryLogConfig", map[string]any{"ResolverQueryLogConfigId": cfgID})

	b, _ := json.Marshal(map[string]any{"ResolverQueryLogConfigId": cfgID})
	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(b))
	resp, _ := p.HandleRequest(context.Background(), "GetResolverQueryLogConfig", req)
	if resp.StatusCode == http.StatusOK {
		t.Error("expected not found after delete")
	}
}

func TestFirewallRuleGroupCRUD(t *testing.T) {
	p := newTestProvider(t)

	// Create group
	out := call(t, p, "CreateFirewallRuleGroup", map[string]any{
		"Name": "my-fw-group",
	})
	grp := out["FirewallRuleGroup"].(map[string]any)
	grpID := grp["Id"].(string)

	// Get group
	out = call(t, p, "GetFirewallRuleGroup", map[string]any{"FirewallRuleGroupId": grpID})
	if out["FirewallRuleGroup"].(map[string]any)["Name"] != "my-fw-group" {
		t.Error("name mismatch")
	}

	// List groups
	out = call(t, p, "ListFirewallRuleGroups", map[string]any{})
	grps := out["FirewallRuleGroups"].([]any)
	if len(grps) != 1 {
		t.Errorf("expected 1 group, got %d", len(grps))
	}

	// Create domain list for rules
	dlOut := call(t, p, "CreateFirewallDomainList", map[string]any{"Name": "block-list"})
	dlID := dlOut["FirewallDomainList"].(map[string]any)["Id"].(string)

	// Create rule
	out = call(t, p, "CreateFirewallRule", map[string]any{
		"FirewallRuleGroupId":  grpID,
		"FirewallDomainListId": dlID,
		"Name":                 "block-bad-domains",
		"Priority":             float64(100),
		"Action":               "BLOCK",
		"BlockResponse":        "NXDOMAIN",
	})
	fwRule := out["FirewallRule"].(map[string]any)
	if fwRule["Action"] != "BLOCK" {
		t.Error("action mismatch")
	}

	// List rules
	out = call(t, p, "ListFirewallRules", map[string]any{"FirewallRuleGroupId": grpID})
	fwRules := out["FirewallRules"].([]any)
	if len(fwRules) != 1 {
		t.Errorf("expected 1 firewall rule, got %d", len(fwRules))
	}

	// Update rule
	out = call(t, p, "UpdateFirewallRule", map[string]any{
		"FirewallRuleGroupId":  grpID,
		"FirewallDomainListId": dlID,
		"Name":                 "updated-rule",
		"Priority":             float64(200),
		"Action":               "ALLOW",
		"BlockResponse":        "NODATA",
	})
	if out["FirewallRule"].(map[string]any)["Action"] != "ALLOW" {
		t.Error("action not updated")
	}

	// Delete rule
	call(t, p, "DeleteFirewallRule", map[string]any{
		"FirewallRuleGroupId":  grpID,
		"FirewallDomainListId": dlID,
	})

	// Associate group to VPC
	out = call(t, p, "AssociateFirewallRuleGroup", map[string]any{
		"FirewallRuleGroupId": grpID,
		"VpcId":               "vpc-fw1",
		"Name":                "fw-assoc",
		"Priority":            float64(100),
	})
	assoc := out["FirewallRuleGroupAssociation"].(map[string]any)
	assocID := assoc["Id"].(string)

	// Get association
	out = call(t, p, "GetFirewallRuleGroupAssociation", map[string]any{"FirewallRuleGroupAssociationId": assocID})
	if out["FirewallRuleGroupAssociation"].(map[string]any)["VpcId"] != "vpc-fw1" {
		t.Error("VpcId mismatch")
	}

	// List associations
	out = call(t, p, "ListFirewallRuleGroupAssociations", map[string]any{"VpcId": "vpc-fw1"})
	assocs := out["FirewallRuleGroupAssociations"].([]any)
	if len(assocs) != 1 {
		t.Errorf("expected 1 association, got %d", len(assocs))
	}

	// Update association
	call(t, p, "UpdateFirewallRuleGroupAssociation", map[string]any{
		"FirewallRuleGroupAssociationId": assocID,
		"Name":                           "fw-assoc-updated",
		"Priority":                       float64(200),
	})

	// Disassociate
	call(t, p, "DisassociateFirewallRuleGroup", map[string]any{"FirewallRuleGroupAssociationId": assocID})

	// Policy
	call(t, p, "PutFirewallRuleGroupPolicy", map[string]any{"Arn": grp["Arn"].(string)})
	out = call(t, p, "GetFirewallRuleGroupPolicy", map[string]any{"Arn": grp["Arn"].(string)})
	if _, ok := out["FirewallRuleGroupPolicy"]; !ok {
		t.Error("expected FirewallRuleGroupPolicy key")
	}

	// Delete group
	call(t, p, "DeleteFirewallRuleGroup", map[string]any{"FirewallRuleGroupId": grpID})
}

func TestFirewallDomainListCRUD(t *testing.T) {
	p := newTestProvider(t)

	// Create
	out := call(t, p, "CreateFirewallDomainList", map[string]any{"Name": "my-domain-list"})
	dl := out["FirewallDomainList"].(map[string]any)
	dlID := dl["Id"].(string)

	// Get
	out = call(t, p, "GetFirewallDomainList", map[string]any{"FirewallDomainListId": dlID})
	if out["FirewallDomainList"].(map[string]any)["Name"] != "my-domain-list" {
		t.Error("name mismatch")
	}

	// List
	out = call(t, p, "ListFirewallDomainLists", map[string]any{})
	dls := out["FirewallDomainLists"].([]any)
	if len(dls) != 1 {
		t.Errorf("expected 1 domain list, got %d", len(dls))
	}

	// UpdateFirewallDomains ADD
	call(t, p, "UpdateFirewallDomains", map[string]any{
		"FirewallDomainListId": dlID,
		"Operation":            "ADD",
		"Domains":              []any{"bad.example.com", "evil.example.com"},
	})

	// ListFirewallDomains
	out = call(t, p, "ListFirewallDomains", map[string]any{"FirewallDomainListId": dlID})
	domains := out["Domains"].([]any)
	if len(domains) != 2 {
		t.Errorf("expected 2 domains, got %d", len(domains))
	}

	// UpdateFirewallDomains REMOVE
	call(t, p, "UpdateFirewallDomains", map[string]any{
		"FirewallDomainListId": dlID,
		"Operation":            "REMOVE",
		"Domains":              []any{"evil.example.com"},
	})
	out = call(t, p, "ListFirewallDomains", map[string]any{"FirewallDomainListId": dlID})
	domains = out["Domains"].([]any)
	if len(domains) != 1 {
		t.Errorf("expected 1 domain after remove, got %d", len(domains))
	}

	// UpdateFirewallDomains REPLACE
	call(t, p, "UpdateFirewallDomains", map[string]any{
		"FirewallDomainListId": dlID,
		"Operation":            "REPLACE",
		"Domains":              []any{"new1.example.com", "new2.example.com", "new3.example.com"},
	})
	out = call(t, p, "ListFirewallDomains", map[string]any{"FirewallDomainListId": dlID})
	domains = out["Domains"].([]any)
	if len(domains) != 3 {
		t.Errorf("expected 3 domains after replace, got %d", len(domains))
	}

	// ImportFirewallDomains (stub)
	call(t, p, "ImportFirewallDomains", map[string]any{
		"FirewallDomainListId": dlID,
		"Operation":            "REPLACE",
		"DomainFileUrl":        "https://example.com/domains.txt",
	})

	// Delete
	call(t, p, "DeleteFirewallDomainList", map[string]any{"FirewallDomainListId": dlID})

	b, _ := json.Marshal(map[string]any{"FirewallDomainListId": dlID})
	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(b))
	resp, _ := p.HandleRequest(context.Background(), "GetFirewallDomainList", req)
	if resp.StatusCode == http.StatusOK {
		t.Error("expected not found after delete")
	}
}

func TestTags(t *testing.T) {
	p := newTestProvider(t)

	// Create endpoint for tagging
	out := call(t, p, "CreateResolverEndpoint", map[string]any{
		"Name":             "tag-test-ep",
		"Direction":        "OUTBOUND",
		"SecurityGroupIds": []any{"sg-xyz"},
		"IpAddresses":      []any{map[string]any{"SubnetId": "subnet-t1", "Ip": "10.0.1.1"}},
	})
	arn := out["ResolverEndpoint"].(map[string]any)["Arn"].(string)

	// TagResource
	call(t, p, "TagResource", map[string]any{
		"ResourceArn": arn,
		"Tags": []any{
			map[string]any{"Key": "Project", "Value": "DevCloud"},
			map[string]any{"Key": "Stage", "Value": "dev"},
		},
	})

	// ListTagsForResource
	out = call(t, p, "ListTagsForResource", map[string]any{"ResourceArn": arn})
	tags := out["Tags"].([]any)
	if len(tags) != 2 {
		t.Errorf("expected 2 tags, got %d", len(tags))
	}

	// UntagResource
	call(t, p, "UntagResource", map[string]any{
		"ResourceArn": arn,
		"TagKeys":     []any{"Stage"},
	})

	out = call(t, p, "ListTagsForResource", map[string]any{"ResourceArn": arn})
	tags = out["Tags"].([]any)
	if len(tags) != 1 {
		t.Errorf("expected 1 tag after untag, got %d", len(tags))
	}
}
