// SPDX-License-Identifier: Apache-2.0

package elasticloadbalancingv2

import (
	"context"
	"encoding/xml"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/skyoo2003/devcloud/internal/plugin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestProvider(t *testing.T) *Provider {
	t.Helper()
	p := &Provider{}
	require.NoError(t, p.Init(plugin.PluginConfig{DataDir: t.TempDir()}))
	t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
	return p
}

func callQuery(t *testing.T, p *Provider, action string, params map[string]string) *plugin.Response {
	t.Helper()
	form := url.Values{}
	form.Set("Action", action)
	for k, v := range params {
		form.Set(k, v)
	}
	req := httptest.NewRequest("POST", "/", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := p.HandleRequest(context.Background(), "", req)
	require.NoError(t, err)
	return resp
}

// TestLoadBalancerCRUD tests create, describe, attributes, set security groups/subnets, and delete.
func TestLoadBalancerCRUD(t *testing.T) {
	p := newTestProvider(t)

	// Create
	resp := callQuery(t, p, "CreateLoadBalancer", map[string]string{
		"Name":                    "my-alb",
		"Type":                    "application",
		"Scheme":                  "internet-facing",
		"Subnets.member.1":        "subnet-aaa",
		"Subnets.member.2":        "subnet-bbb",
		"SecurityGroups.member.1": "sg-111",
	})
	assert.Equal(t, 200, resp.StatusCode, string(resp.Body))

	type createLBResp struct {
		LBs []struct {
			ARN    string `xml:"LoadBalancerArn"`
			Name   string `xml:"LoadBalancerName"`
			Scheme string `xml:"Scheme"`
			Type   string `xml:"Type"`
		} `xml:"CreateLoadBalancerResult>LoadBalancers>member"`
	}
	var cr createLBResp
	require.NoError(t, xml.Unmarshal(resp.Body, &cr))
	require.Len(t, cr.LBs, 1)
	assert.Equal(t, "my-alb", cr.LBs[0].Name)
	assert.Equal(t, "application", cr.LBs[0].Type)
	assert.Equal(t, "internet-facing", cr.LBs[0].Scheme)
	assert.Contains(t, cr.LBs[0].ARN, "arn:aws:elasticloadbalancing")
	lbARN := cr.LBs[0].ARN

	// Describe
	descResp := callQuery(t, p, "DescribeLoadBalancers", map[string]string{
		"LoadBalancerArns.member.1": lbARN,
	})
	assert.Equal(t, 200, descResp.StatusCode)

	type describeLBsResp struct {
		LBs []struct {
			Name string `xml:"LoadBalancerName"`
		} `xml:"DescribeLoadBalancersResult>LoadBalancers>member"`
	}
	var dr describeLBsResp
	require.NoError(t, xml.Unmarshal(descResp.Body, &dr))
	require.Len(t, dr.LBs, 1)
	assert.Equal(t, "my-alb", dr.LBs[0].Name)

	// Describe by Name
	descByNameResp := callQuery(t, p, "DescribeLoadBalancers", map[string]string{
		"Names.member.1": "my-alb",
	})
	assert.Equal(t, 200, descByNameResp.StatusCode)
	var dr2 describeLBsResp
	require.NoError(t, xml.Unmarshal(descByNameResp.Body, &dr2))
	require.Len(t, dr2.LBs, 1)

	// Describe all
	descAllResp := callQuery(t, p, "DescribeLoadBalancers", map[string]string{})
	assert.Equal(t, 200, descAllResp.StatusCode)
	var dr3 describeLBsResp
	require.NoError(t, xml.Unmarshal(descAllResp.Body, &dr3))
	require.Len(t, dr3.LBs, 1)

	// ModifyLoadBalancerAttributes
	modAttrResp := callQuery(t, p, "ModifyLoadBalancerAttributes", map[string]string{
		"LoadBalancerArn":           lbARN,
		"Attributes.member.1.Key":   "idle_timeout.timeout_seconds",
		"Attributes.member.1.Value": "120",
	})
	assert.Equal(t, 200, modAttrResp.StatusCode)

	// DescribeLoadBalancerAttributes
	descAttrResp := callQuery(t, p, "DescribeLoadBalancerAttributes", map[string]string{
		"LoadBalancerArn": lbARN,
	})
	assert.Equal(t, 200, descAttrResp.StatusCode)

	// SetSecurityGroups
	setSGResp := callQuery(t, p, "SetSecurityGroups", map[string]string{
		"LoadBalancerArn":         lbARN,
		"SecurityGroups.member.1": "sg-222",
		"SecurityGroups.member.2": "sg-333",
	})
	assert.Equal(t, 200, setSGResp.StatusCode)

	// SetSubnets
	setSubResp := callQuery(t, p, "SetSubnets", map[string]string{
		"LoadBalancerArn":  lbARN,
		"Subnets.member.1": "subnet-ccc",
	})
	assert.Equal(t, 200, setSubResp.StatusCode)

	// SetIpAddressType
	setIPResp := callQuery(t, p, "SetIpAddressType", map[string]string{
		"LoadBalancerArn": lbARN,
		"IpAddressType":   "dualstack",
	})
	assert.Equal(t, 200, setIPResp.StatusCode)

	// Delete
	delResp := callQuery(t, p, "DeleteLoadBalancer", map[string]string{
		"LoadBalancerArn": lbARN,
	})
	assert.Equal(t, 200, delResp.StatusCode)

	// Verify gone
	descAfterDel := callQuery(t, p, "DescribeLoadBalancers", map[string]string{})
	var drAfter describeLBsResp
	require.NoError(t, xml.Unmarshal(descAfterDel.Body, &drAfter))
	assert.Len(t, drAfter.LBs, 0)

	// Duplicate name
	callQuery(t, p, "CreateLoadBalancer", map[string]string{"Name": "dup-lb"})
	dupResp := callQuery(t, p, "CreateLoadBalancer", map[string]string{"Name": "dup-lb"})
	assert.Equal(t, 409, dupResp.StatusCode)
}

// TestTargetGroupCRUD tests create, describe, modify, attributes, and delete target groups.
func TestTargetGroupCRUD(t *testing.T) {
	p := newTestProvider(t)

	// Create
	resp := callQuery(t, p, "CreateTargetGroup", map[string]string{
		"Name":       "my-tg",
		"Protocol":   "HTTP",
		"Port":       "8080",
		"VpcId":      "vpc-123",
		"TargetType": "instance",
	})
	assert.Equal(t, 200, resp.StatusCode, string(resp.Body))

	type createTGResp struct {
		TGs []struct {
			ARN  string `xml:"TargetGroupArn"`
			Name string `xml:"TargetGroupName"`
			Port int    `xml:"Port"`
		} `xml:"CreateTargetGroupResult>TargetGroups>member"`
	}
	var cr createTGResp
	require.NoError(t, xml.Unmarshal(resp.Body, &cr))
	require.Len(t, cr.TGs, 1)
	assert.Equal(t, "my-tg", cr.TGs[0].Name)
	assert.Equal(t, 8080, cr.TGs[0].Port)
	tgARN := cr.TGs[0].ARN

	// Describe
	descResp := callQuery(t, p, "DescribeTargetGroups", map[string]string{
		"TargetGroupArns.member.1": tgARN,
	})
	assert.Equal(t, 200, descResp.StatusCode)

	type describeTGsResp struct {
		TGs []struct {
			Name string `xml:"TargetGroupName"`
		} `xml:"DescribeTargetGroupsResult>TargetGroups>member"`
	}
	var dr describeTGsResp
	require.NoError(t, xml.Unmarshal(descResp.Body, &dr))
	require.Len(t, dr.TGs, 1)
	assert.Equal(t, "my-tg", dr.TGs[0].Name)

	// Describe all
	descAllResp := callQuery(t, p, "DescribeTargetGroups", map[string]string{})
	var drAll describeTGsResp
	require.NoError(t, xml.Unmarshal(descAllResp.Body, &drAll))
	require.Len(t, drAll.TGs, 1)

	// Modify
	modResp := callQuery(t, p, "ModifyTargetGroup", map[string]string{
		"TargetGroupArn": tgARN,
	})
	assert.Equal(t, 200, modResp.StatusCode)

	// ModifyTargetGroupAttributes
	modAttrResp := callQuery(t, p, "ModifyTargetGroupAttributes", map[string]string{
		"TargetGroupArn":            tgARN,
		"Attributes.member.1.Key":   "deregistration_delay.timeout_seconds",
		"Attributes.member.1.Value": "60",
	})
	assert.Equal(t, 200, modAttrResp.StatusCode)

	// DescribeTargetGroupAttributes
	descAttrResp := callQuery(t, p, "DescribeTargetGroupAttributes", map[string]string{
		"TargetGroupArn": tgARN,
	})
	assert.Equal(t, 200, descAttrResp.StatusCode)

	// Delete
	delResp := callQuery(t, p, "DeleteTargetGroup", map[string]string{
		"TargetGroupArn": tgARN,
	})
	assert.Equal(t, 200, delResp.StatusCode)

	// Verify gone
	descAfterDel := callQuery(t, p, "DescribeTargetGroups", map[string]string{})
	var drAfter describeTGsResp
	require.NoError(t, xml.Unmarshal(descAfterDel.Body, &drAfter))
	assert.Len(t, drAfter.TGs, 0)
}

// TestListenerCRUD tests create, describe, modify, and delete listeners.
func TestListenerCRUD(t *testing.T) {
	p := newTestProvider(t)

	// Create LB first
	lbResp := callQuery(t, p, "CreateLoadBalancer", map[string]string{"Name": "lb-for-listener"})
	type lbARNResp struct {
		ARN string `xml:"CreateLoadBalancerResult>LoadBalancers>member>LoadBalancerArn"`
	}
	var lbR lbARNResp
	require.NoError(t, xml.Unmarshal(lbResp.Body, &lbR))
	lbARN := lbR.ARN

	// Create TG
	tgResp := callQuery(t, p, "CreateTargetGroup", map[string]string{"Name": "tg-for-listener", "Protocol": "HTTP", "Port": "80"})
	type tgARNResp struct {
		ARN string `xml:"CreateTargetGroupResult>TargetGroups>member>TargetGroupArn"`
	}
	var tgR tgARNResp
	require.NoError(t, xml.Unmarshal(tgResp.Body, &tgR))
	tgARN := tgR.ARN

	// Create Listener
	resp := callQuery(t, p, "CreateListener", map[string]string{
		"LoadBalancerArn":                        lbARN,
		"Protocol":                               "HTTP",
		"Port":                                   "80",
		"DefaultActions.member.1.Type":           "forward",
		"DefaultActions.member.1.TargetGroupArn": tgARN,
	})
	assert.Equal(t, 200, resp.StatusCode, string(resp.Body))

	type createListenerResp struct {
		Listeners []struct {
			ARN      string `xml:"ListenerArn"`
			Protocol string `xml:"Protocol"`
			Port     int    `xml:"Port"`
		} `xml:"CreateListenerResult>Listeners>member"`
	}
	var cr createListenerResp
	require.NoError(t, xml.Unmarshal(resp.Body, &cr))
	require.Len(t, cr.Listeners, 1)
	assert.Equal(t, "HTTP", cr.Listeners[0].Protocol)
	assert.Equal(t, 80, cr.Listeners[0].Port)
	listenerARN := cr.Listeners[0].ARN

	// Describe by LB ARN
	descResp := callQuery(t, p, "DescribeListeners", map[string]string{
		"LoadBalancerArn": lbARN,
	})
	assert.Equal(t, 200, descResp.StatusCode)

	type descListenersResp struct {
		Listeners []struct {
			Protocol string `xml:"Protocol"`
		} `xml:"DescribeListenersResult>Listeners>member"`
	}
	var dr descListenersResp
	require.NoError(t, xml.Unmarshal(descResp.Body, &dr))
	require.Len(t, dr.Listeners, 1)

	// Describe by ARN
	descByARNResp := callQuery(t, p, "DescribeListeners", map[string]string{
		"ListenerArns.member.1": listenerARN,
	})
	assert.Equal(t, 200, descByARNResp.StatusCode)

	// Modify
	modResp := callQuery(t, p, "ModifyListener", map[string]string{
		"ListenerArn": listenerARN,
		"Port":        "8080",
	})
	assert.Equal(t, 200, modResp.StatusCode)

	type modListenerResp struct {
		Listeners []struct {
			Port int `xml:"Port"`
		} `xml:"ModifyListenerResult>Listeners>member"`
	}
	var mr modListenerResp
	require.NoError(t, xml.Unmarshal(modResp.Body, &mr))
	require.Len(t, mr.Listeners, 1)
	assert.Equal(t, 8080, mr.Listeners[0].Port)

	// Delete
	delResp := callQuery(t, p, "DeleteListener", map[string]string{
		"ListenerArn": listenerARN,
	})
	assert.Equal(t, 200, delResp.StatusCode)

	// Verify gone
	descAfterDel := callQuery(t, p, "DescribeListeners", map[string]string{"LoadBalancerArn": lbARN})
	var drAfter descListenersResp
	require.NoError(t, xml.Unmarshal(descAfterDel.Body, &drAfter))
	assert.Len(t, drAfter.Listeners, 0)
}

// TestRuleCRUD tests create, describe, modify, set priorities, and delete rules.
func TestRuleCRUD(t *testing.T) {
	p := newTestProvider(t)

	// Create LB + TG + Listener
	lbResp := callQuery(t, p, "CreateLoadBalancer", map[string]string{"Name": "lb-for-rules"})
	type lbARNResp struct {
		ARN string `xml:"CreateLoadBalancerResult>LoadBalancers>member>LoadBalancerArn"`
	}
	var lbR lbARNResp
	require.NoError(t, xml.Unmarshal(lbResp.Body, &lbR))

	tgResp := callQuery(t, p, "CreateTargetGroup", map[string]string{"Name": "tg-for-rules", "Protocol": "HTTP", "Port": "80"})
	type tgARNResp struct {
		ARN string `xml:"CreateTargetGroupResult>TargetGroups>member>TargetGroupArn"`
	}
	var tgR tgARNResp
	require.NoError(t, xml.Unmarshal(tgResp.Body, &tgR))

	listenerResp := callQuery(t, p, "CreateListener", map[string]string{
		"LoadBalancerArn":                        lbR.ARN,
		"Protocol":                               "HTTP",
		"Port":                                   "80",
		"DefaultActions.member.1.Type":           "forward",
		"DefaultActions.member.1.TargetGroupArn": tgR.ARN,
	})
	type listenerARNResp struct {
		ARN string `xml:"CreateListenerResult>Listeners>member>ListenerArn"`
	}
	var lsnR listenerARNResp
	require.NoError(t, xml.Unmarshal(listenerResp.Body, &lsnR))
	listenerARN := lsnR.ARN

	// Create Rule
	resp := callQuery(t, p, "CreateRule", map[string]string{
		"ListenerArn":                         listenerARN,
		"Priority":                            "10",
		"Conditions.member.1.Field":           "path-pattern",
		"Conditions.member.1.Values.member.1": "/api/*",
		"Actions.member.1.Type":               "forward",
		"Actions.member.1.TargetGroupArn":     tgR.ARN,
	})
	assert.Equal(t, 200, resp.StatusCode, string(resp.Body))

	type createRuleResp struct {
		Rules []struct {
			ARN      string `xml:"RuleArn"`
			Priority string `xml:"Priority"`
		} `xml:"CreateRuleResult>Rules>member"`
	}
	var cr createRuleResp
	require.NoError(t, xml.Unmarshal(resp.Body, &cr))
	require.Len(t, cr.Rules, 1)
	assert.Equal(t, "10", cr.Rules[0].Priority)
	ruleARN := cr.Rules[0].ARN

	// Describe by listener ARN
	descResp := callQuery(t, p, "DescribeRules", map[string]string{
		"ListenerArn": listenerARN,
	})
	assert.Equal(t, 200, descResp.StatusCode)
	type descRulesResp struct {
		Rules []struct {
			Priority string `xml:"Priority"`
		} `xml:"DescribeRulesResult>Rules>member"`
	}
	var dr descRulesResp
	require.NoError(t, xml.Unmarshal(descResp.Body, &dr))
	require.Len(t, dr.Rules, 1)

	// Describe by rule ARN
	descByARNResp := callQuery(t, p, "DescribeRules", map[string]string{
		"RuleArns.member.1": ruleARN,
	})
	assert.Equal(t, 200, descByARNResp.StatusCode)

	// Modify
	modResp := callQuery(t, p, "ModifyRule", map[string]string{
		"RuleArn":                             ruleARN,
		"Conditions.member.1.Field":           "path-pattern",
		"Conditions.member.1.Values.member.1": "/new/*",
	})
	assert.Equal(t, 200, modResp.StatusCode)

	// SetRulePriorities
	setPriResp := callQuery(t, p, "SetRulePriorities", map[string]string{
		"RulePriorities.member.1.RuleArn":  ruleARN,
		"RulePriorities.member.1.Priority": "20",
	})
	assert.Equal(t, 200, setPriResp.StatusCode)

	type setPriResp2 struct {
		Rules []struct {
			Priority string `xml:"Priority"`
		} `xml:"SetRulePrioritiesResult>Rules>member"`
	}
	var spr setPriResp2
	require.NoError(t, xml.Unmarshal(setPriResp.Body, &spr))
	require.Len(t, spr.Rules, 1)
	assert.Equal(t, "20", spr.Rules[0].Priority)

	// Delete
	delResp := callQuery(t, p, "DeleteRule", map[string]string{
		"RuleArn": ruleARN,
	})
	assert.Equal(t, 200, delResp.StatusCode)

	// Verify gone
	descAfterDel := callQuery(t, p, "DescribeRules", map[string]string{"ListenerArn": listenerARN})
	var drAfter descRulesResp
	require.NoError(t, xml.Unmarshal(descAfterDel.Body, &drAfter))
	assert.Len(t, drAfter.Rules, 0)
}

// TestRegisterDeregisterTargets tests register, describe health, and deregister.
func TestRegisterDeregisterTargets(t *testing.T) {
	p := newTestProvider(t)

	tgResp := callQuery(t, p, "CreateTargetGroup", map[string]string{"Name": "tg-targets", "Protocol": "HTTP", "Port": "80"})
	type tgARNResp struct {
		ARN string `xml:"CreateTargetGroupResult>TargetGroups>member>TargetGroupArn"`
	}
	var tgR tgARNResp
	require.NoError(t, xml.Unmarshal(tgResp.Body, &tgR))
	tgARN := tgR.ARN

	// Register
	regResp := callQuery(t, p, "RegisterTargets", map[string]string{
		"TargetGroupArn":        tgARN,
		"Targets.member.1.Id":   "i-aaa111",
		"Targets.member.1.Port": "8080",
		"Targets.member.2.Id":   "i-bbb222",
	})
	assert.Equal(t, 200, regResp.StatusCode, string(regResp.Body))

	// DescribeTargetHealth
	healthResp := callQuery(t, p, "DescribeTargetHealth", map[string]string{
		"TargetGroupArn": tgARN,
	})
	assert.Equal(t, 200, healthResp.StatusCode)

	type descHealthResp struct {
		Descs []struct {
			ID    string `xml:"Target>Id"`
			State string `xml:"TargetHealth>State"`
		} `xml:"DescribeTargetHealthResult>TargetHealthDescriptions>member"`
	}
	var hr descHealthResp
	require.NoError(t, xml.Unmarshal(healthResp.Body, &hr))
	assert.Len(t, hr.Descs, 2)
	for _, d := range hr.Descs {
		assert.Equal(t, "healthy", d.State)
	}

	// Deregister one
	deregResp := callQuery(t, p, "DeregisterTargets", map[string]string{
		"TargetGroupArn":      tgARN,
		"Targets.member.1.Id": "i-aaa111",
	})
	assert.Equal(t, 200, deregResp.StatusCode)

	// Verify one left
	healthResp2 := callQuery(t, p, "DescribeTargetHealth", map[string]string{"TargetGroupArn": tgARN})
	var hr2 descHealthResp
	require.NoError(t, xml.Unmarshal(healthResp2.Body, &hr2))
	assert.Len(t, hr2.Descs, 1)
	assert.Equal(t, "i-bbb222", hr2.Descs[0].ID)
}

// TestTags tests AddTags, DescribeTags, RemoveTags.
func TestTags(t *testing.T) {
	p := newTestProvider(t)

	lbResp := callQuery(t, p, "CreateLoadBalancer", map[string]string{"Name": "lb-for-tags"})
	type lbARNResp struct {
		ARN string `xml:"CreateLoadBalancerResult>LoadBalancers>member>LoadBalancerArn"`
	}
	var lbR lbARNResp
	require.NoError(t, xml.Unmarshal(lbResp.Body, &lbR))
	lbARN := lbR.ARN

	// AddTags
	addResp := callQuery(t, p, "AddTags", map[string]string{
		"ResourceArns.member.1": lbARN,
		"Tags.member.1.Key":     "Env",
		"Tags.member.1.Value":   "prod",
		"Tags.member.2.Key":     "Team",
		"Tags.member.2.Value":   "platform",
	})
	assert.Equal(t, 200, addResp.StatusCode, string(addResp.Body))

	// DescribeTags
	descResp := callQuery(t, p, "DescribeTags", map[string]string{
		"ResourceArns.member.1": lbARN,
	})
	assert.Equal(t, 200, descResp.StatusCode)

	type descTagsResp struct {
		Descs []struct {
			ARN  string `xml:"ResourceArn"`
			Tags []struct {
				Key   string `xml:"Key"`
				Value string `xml:"Value"`
			} `xml:"Tags>member"`
		} `xml:"DescribeTagsResult>TagDescriptions>member"`
	}
	var dr descTagsResp
	require.NoError(t, xml.Unmarshal(descResp.Body, &dr))
	require.Len(t, dr.Descs, 1)
	assert.Equal(t, lbARN, dr.Descs[0].ARN)
	assert.Len(t, dr.Descs[0].Tags, 2)

	// RemoveTags
	removeResp := callQuery(t, p, "RemoveTags", map[string]string{
		"ResourceArns.member.1": lbARN,
		"TagKeys.member.1":      "Env",
	})
	assert.Equal(t, 200, removeResp.StatusCode)

	// Verify one tag left
	descAfterRemove := callQuery(t, p, "DescribeTags", map[string]string{"ResourceArns.member.1": lbARN})
	var drAfter descTagsResp
	require.NoError(t, xml.Unmarshal(descAfterRemove.Body, &drAfter))
	require.Len(t, drAfter.Descs, 1)
	assert.Len(t, drAfter.Descs[0].Tags, 1)
	assert.Equal(t, "Team", drAfter.Descs[0].Tags[0].Key)
	assert.Equal(t, "platform", drAfter.Descs[0].Tags[0].Value)
}
