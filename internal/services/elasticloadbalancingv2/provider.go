// SPDX-License-Identifier: Apache-2.0

package elasticloadbalancingv2

import (
	"context"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/skyoo2003/devcloud/internal/plugin"
	"github.com/skyoo2003/devcloud/internal/shared"
)

// Provider implements the ElasticLoadBalancing_v10 service (Query/XML protocol).
type Provider struct {
	store *Store
}

func (p *Provider) ServiceID() string             { return "elasticloadbalancingv2" }
func (p *Provider) ServiceName() string           { return "ElasticLoadBalancing_v10" }
func (p *Provider) Protocol() plugin.ProtocolType { return plugin.ProtocolQuery }

func (p *Provider) Init(cfg plugin.PluginConfig) error {
	if err := os.MkdirAll(cfg.DataDir, 0o755); err != nil {
		return fmt.Errorf("init elasticloadbalancingv2: %w", err)
	}
	var err error
	p.store, err = NewStore(cfg.DataDir)
	return err
}

func (p *Provider) Shutdown(_ context.Context) error {
	if p.store != nil {
		return p.store.Close()
	}
	return nil
}

func (p *Provider) HandleRequest(ctx context.Context, op string, req *http.Request) (*plugin.Response, error) {
	body, err := io.ReadAll(req.Body)
	if err != nil {
		return elbError("InvalidRequest", "failed to read request body", http.StatusBadRequest), nil
	}
	form, err := url.ParseQuery(string(body))
	if err != nil {
		return elbError("InvalidRequest", "failed to parse form body", http.StatusBadRequest), nil
	}
	action := op
	if action == "" {
		action = form.Get("Action")
	}

	switch action {
	// LoadBalancer
	case "CreateLoadBalancer":
		return p.handleCreateLoadBalancer(form)
	case "DescribeLoadBalancers":
		return p.handleDescribeLoadBalancers(form)
	case "DeleteLoadBalancer":
		return p.handleDeleteLoadBalancer(form)
	case "ModifyLoadBalancerAttributes":
		return p.handleModifyLoadBalancerAttributes(form)
	case "DescribeLoadBalancerAttributes":
		return p.handleDescribeLoadBalancerAttributes(form)
	case "SetSecurityGroups":
		return p.handleSetSecurityGroups(form)
	case "SetSubnets":
		return p.handleSetSubnets(form)
	case "SetIpAddressType":
		return p.handleSetIpAddressType(form)

	// TargetGroup
	case "CreateTargetGroup":
		return p.handleCreateTargetGroup(form)
	case "DescribeTargetGroups":
		return p.handleDescribeTargetGroups(form)
	case "ModifyTargetGroup":
		return p.handleModifyTargetGroup(form)
	case "ModifyTargetGroupAttributes":
		return p.handleModifyTargetGroupAttributes(form)
	case "DeleteTargetGroup":
		return p.handleDeleteTargetGroup(form)
	case "DescribeTargetGroupAttributes":
		return p.handleDescribeTargetGroupAttributes(form)

	// Targets
	case "RegisterTargets":
		return p.handleRegisterTargets(form)
	case "DeregisterTargets":
		return p.handleDeregisterTargets(form)
	case "DescribeTargetHealth":
		return p.handleDescribeTargetHealth(form)

	// Listener
	case "CreateListener":
		return p.handleCreateListener(form)
	case "DescribeListeners":
		return p.handleDescribeListeners(form)
	case "ModifyListener":
		return p.handleModifyListener(form)
	case "DeleteListener":
		return p.handleDeleteListener(form)

	// Rule
	case "CreateRule":
		return p.handleCreateRule(form)
	case "DescribeRules":
		return p.handleDescribeRules(form)
	case "ModifyRule":
		return p.handleModifyRule(form)
	case "DeleteRule":
		return p.handleDeleteRule(form)
	case "SetRulePriorities":
		return p.handleSetRulePriorities(form)

	// Tags
	case "AddTags":
		return p.handleAddTags(form)
	case "RemoveTags":
		return p.handleRemoveTags(form)
	case "DescribeTags":
		return p.handleDescribeTags(form)

	// SSL Policies
	case "DescribeSSLPolicies":
		return p.handleDescribeSSLPolicies(form)

	// Account Limits
	case "DescribeAccountLimits":
		return p.handleDescribeAccountLimits(form)

	// Listener Certificates
	case "AddListenerCertificates":
		return p.handleAddListenerCertificates(form)
	case "RemoveListenerCertificates":
		return p.handleRemoveListenerCertificates(form)
	case "DescribeListenerCertificates":
		return p.handleDescribeListenerCertificates(form)

	default:
		type genericResult struct {
			XMLName xml.Name `xml:"GenericResponse"`
		}
		return shared.XMLResponse(http.StatusOK, genericResult{XMLName: xml.Name{Local: action + "Response"}})
	}
}

func (p *Provider) ListResources(ctx context.Context) ([]plugin.Resource, error) {
	lbs, err := p.store.ListLoadBalancers(nil)
	if err != nil {
		return nil, err
	}
	out := make([]plugin.Resource, 0, len(lbs))
	for _, lb := range lbs {
		out = append(out, plugin.Resource{Type: "load-balancer", ID: lb.ARN, Name: lb.Name})
	}
	return out, nil
}

func (p *Provider) GetMetrics(_ context.Context) (*plugin.ServiceMetrics, error) {
	return &plugin.ServiceMetrics{}, nil
}

// --- helpers ---

func elbError(code, msg string, status int) *plugin.Response {
	return shared.QueryXMLError(code, msg, status)
}

func elbXMLResponse(status int, v any) (*plugin.Response, error) {
	return shared.XMLResponse(status, v)
}

// --- XML types ---

type lbStateXML struct {
	Code string `xml:"Code"`
}

type availabilityZoneXML struct {
	SubnetId string `xml:"SubnetId"`
	ZoneName string `xml:"ZoneName"`
}

type loadBalancerXML struct {
	LoadBalancerArn   string                `xml:"LoadBalancerArn"`
	DNSName           string                `xml:"DNSName"`
	LoadBalancerName  string                `xml:"LoadBalancerName"`
	State             lbStateXML            `xml:"State"`
	Type              string                `xml:"Type"`
	Scheme            string                `xml:"Scheme"`
	VpcId             string                `xml:"VpcId"`
	IpAddressType     string                `xml:"IpAddressType"`
	CreatedTime       string                `xml:"CreatedTime"`
	AvailabilityZones []availabilityZoneXML `xml:"AvailabilityZones>member"`
	SecurityGroups    []string              `xml:"SecurityGroups>member"`
}

func lbToXML(lb *LoadBalancer) loadBalancerXML {
	azs := make([]availabilityZoneXML, 0, len(lb.Subnets))
	for _, s := range lb.Subnets {
		azs = append(azs, availabilityZoneXML{SubnetId: s, ZoneName: "us-east-1a"})
	}
	return loadBalancerXML{
		LoadBalancerArn:   lb.ARN,
		DNSName:           lb.DNSName,
		LoadBalancerName:  lb.Name,
		State:             lbStateXML{Code: lb.State},
		Type:              lb.Type,
		Scheme:            lb.Scheme,
		VpcId:             lb.VpcID,
		IpAddressType:     lb.IPType,
		CreatedTime:       lb.CreatedAt.UTC().Format(time.RFC3339),
		AvailabilityZones: azs,
		SecurityGroups:    lb.SecurityGroups,
	}
}

type healthCheckXML struct {
	Path               string `xml:"HealthCheckPath,omitempty"`
	Protocol           string `xml:"HealthCheckProtocol,omitempty"`
	IntervalSeconds    string `xml:"HealthCheckIntervalSeconds,omitempty"`
	TimeoutSeconds     string `xml:"HealthCheckTimeoutSeconds,omitempty"`
	HealthyThreshold   string `xml:"HealthyThresholdCount,omitempty"`
	UnhealthyThreshold string `xml:"UnhealthyThresholdCount,omitempty"`
}

type targetGroupXML struct {
	TargetGroupArn  string         `xml:"TargetGroupArn"`
	TargetGroupName string         `xml:"TargetGroupName"`
	Protocol        string         `xml:"Protocol"`
	Port            int            `xml:"Port"`
	VpcId           string         `xml:"VpcId"`
	TargetType      string         `xml:"TargetType"`
	HealthCheck     healthCheckXML `xml:"HealthCheck"`
}

func tgToXML(tg *TargetGroup) targetGroupXML {
	hc := healthCheckXML{}
	if tg.HealthCheck != nil {
		hc.Path = tg.HealthCheck["path"]
		hc.Protocol = tg.HealthCheck["protocol"]
		hc.IntervalSeconds = tg.HealthCheck["interval"]
		hc.TimeoutSeconds = tg.HealthCheck["timeout"]
		hc.HealthyThreshold = tg.HealthCheck["healthyThreshold"]
		hc.UnhealthyThreshold = tg.HealthCheck["unhealthyThreshold"]
	}
	return targetGroupXML{
		TargetGroupArn:  tg.ARN,
		TargetGroupName: tg.Name,
		Protocol:        tg.Protocol,
		Port:            tg.Port,
		VpcId:           tg.VpcID,
		TargetType:      tg.TargetType,
		HealthCheck:     hc,
	}
}

type actionXML struct {
	Type           string `xml:"Type"`
	TargetGroupArn string `xml:"TargetGroupArn,omitempty"`
}

type certificateXML struct {
	CertificateArn string `xml:"CertificateArn"`
	IsDefault      bool   `xml:"IsDefault,omitempty"`
}

type listenerXML struct {
	ListenerArn     string           `xml:"ListenerArn"`
	LoadBalancerArn string           `xml:"LoadBalancerArn"`
	Protocol        string           `xml:"Protocol"`
	Port            int              `xml:"Port"`
	SslPolicy       string           `xml:"SslPolicy,omitempty"`
	DefaultActions  []actionXML      `xml:"DefaultActions>member"`
	Certificates    []certificateXML `xml:"Certificates>member,omitempty"`
}

func listenerToXML(l *Listener) listenerXML {
	acts := make([]actionXML, 0, len(l.DefaultActions))
	for _, a := range l.DefaultActions {
		acts = append(acts, actionXML{Type: a["Type"], TargetGroupArn: a["TargetGroupArn"]})
	}
	certs := make([]certificateXML, 0, len(l.Certificates))
	for _, c := range l.Certificates {
		certs = append(certs, certificateXML{CertificateArn: c["CertificateArn"]})
	}
	return listenerXML{
		ListenerArn:     l.ARN,
		LoadBalancerArn: l.LBARN,
		Protocol:        l.Protocol,
		Port:            l.Port,
		SslPolicy:       l.SSLPolicy,
		DefaultActions:  acts,
		Certificates:    certs,
	}
}

type ruleConditionXML struct {
	Field  string   `xml:"Field"`
	Values []string `xml:"Values>member"`
}

type ruleXML struct {
	RuleArn    string             `xml:"RuleArn"`
	Priority   string             `xml:"Priority"`
	Conditions []ruleConditionXML `xml:"Conditions>member"`
	Actions    []actionXML        `xml:"Actions>member"`
	IsDefault  bool               `xml:"IsDefault"`
}

func ruleToXML(r *Rule) ruleXML {
	conds := make([]ruleConditionXML, 0, len(r.Conditions))
	for _, c := range r.Conditions {
		vals := strings.Split(c["Values"], ",")
		if c["Values"] == "" {
			vals = nil
		}
		conds = append(conds, ruleConditionXML{Field: c["Field"], Values: vals})
	}
	acts := make([]actionXML, 0, len(r.Actions))
	for _, a := range r.Actions {
		acts = append(acts, actionXML{Type: a["Type"], TargetGroupArn: a["TargetGroupArn"]})
	}
	return ruleXML{
		RuleArn:    r.ARN,
		Priority:   r.Priority,
		Conditions: conds,
		Actions:    acts,
		IsDefault:  r.IsDefault,
	}
}

type tagXML struct {
	Key   string `xml:"Key"`
	Value string `xml:"Value"`
}

// --- LoadBalancer handlers ---

func (p *Provider) handleCreateLoadBalancer(form url.Values) (*plugin.Response, error) {
	name := form.Get("Name")
	if name == "" {
		return elbError("ValidationError", "Name is required", http.StatusBadRequest), nil
	}
	lbType := form.Get("Type")
	if lbType == "" {
		lbType = "application"
	}
	scheme := form.Get("Scheme")
	if scheme == "" {
		scheme = "internet-facing"
	}

	var subnets []string
	for i := 1; ; i++ {
		v := form.Get(fmt.Sprintf("Subnets.member.%d", i))
		if v == "" {
			break
		}
		subnets = append(subnets, v)
	}

	var sgs []string
	for i := 1; ; i++ {
		v := form.Get(fmt.Sprintf("SecurityGroups.member.%d", i))
		if v == "" {
			break
		}
		sgs = append(sgs, v)
	}

	arn := shared.BuildARN("elasticloadbalancing", "loadbalancer/app", name)
	lb, err := p.store.CreateLoadBalancer(arn, name, lbType, scheme, "", subnets, sgs)
	if err != nil {
		if strings.Contains(err.Error(), "UNIQUE constraint failed") {
			return elbError("DuplicateLoadBalancerName", "load balancer already exists: "+name, http.StatusConflict), nil
		}
		return nil, err
	}

	type createLBResult struct {
		XMLName xml.Name `xml:"CreateLoadBalancerResponse"`
		Result  struct {
			LoadBalancers []loadBalancerXML `xml:"LoadBalancers>member"`
		} `xml:"CreateLoadBalancerResult"`
	}
	var resp createLBResult
	resp.Result.LoadBalancers = []loadBalancerXML{lbToXML(lb)}
	return elbXMLResponse(http.StatusOK, resp)
}

func (p *Provider) handleDescribeLoadBalancers(form url.Values) (*plugin.Response, error) {
	var arns []string
	for i := 1; ; i++ {
		v := form.Get(fmt.Sprintf("LoadBalancerArns.member.%d", i))
		if v == "" {
			break
		}
		arns = append(arns, v)
	}

	// Also support Names filter
	var names []string
	for i := 1; ; i++ {
		v := form.Get(fmt.Sprintf("Names.member.%d", i))
		if v == "" {
			break
		}
		names = append(names, v)
	}

	lbs, err := p.store.ListLoadBalancers(arns)
	if err != nil {
		return nil, err
	}

	// Filter by names if provided
	if len(names) > 0 {
		nameSet := map[string]bool{}
		for _, n := range names {
			nameSet[n] = true
		}
		filtered := lbs[:0]
		for _, lb := range lbs {
			if nameSet[lb.Name] {
				filtered = append(filtered, lb)
			}
		}
		lbs = filtered
	}

	if (len(arns) > 0 || len(names) > 0) && len(lbs) == 0 {
		return elbError("LoadBalancerNotFound", "load balancer not found", http.StatusBadRequest), nil
	}

	xmlLBs := make([]loadBalancerXML, 0, len(lbs))
	for i := range lbs {
		xmlLBs = append(xmlLBs, lbToXML(&lbs[i]))
	}

	type describeLBsResult struct {
		XMLName xml.Name `xml:"DescribeLoadBalancersResponse"`
		Result  struct {
			LoadBalancers []loadBalancerXML `xml:"LoadBalancers>member"`
		} `xml:"DescribeLoadBalancersResult"`
	}
	var resp describeLBsResult
	resp.Result.LoadBalancers = xmlLBs
	return elbXMLResponse(http.StatusOK, resp)
}

func (p *Provider) handleDeleteLoadBalancer(form url.Values) (*plugin.Response, error) {
	arn := form.Get("LoadBalancerArn")
	if arn == "" {
		return elbError("ValidationError", "LoadBalancerArn is required", http.StatusBadRequest), nil
	}
	if err := p.store.DeleteLoadBalancer(arn); err != nil {
		if strings.Contains(err.Error(), "not found") {
			return elbError("LoadBalancerNotFound", "load balancer not found: "+arn, http.StatusBadRequest), nil
		}
		return nil, err
	}
	type deleteResult struct {
		XMLName xml.Name `xml:"DeleteLoadBalancerResponse"`
		Result  struct{} `xml:"DeleteLoadBalancerResult"`
	}
	return elbXMLResponse(http.StatusOK, deleteResult{})
}

func (p *Provider) handleModifyLoadBalancerAttributes(form url.Values) (*plugin.Response, error) {
	arn := form.Get("LoadBalancerArn")
	if arn == "" {
		return elbError("ValidationError", "LoadBalancerArn is required", http.StatusBadRequest), nil
	}
	// Collect attributes
	type attrXML struct {
		Key   string `xml:"Key"`
		Value string `xml:"Value"`
	}
	var attrs []attrXML
	for i := 1; ; i++ {
		k := form.Get(fmt.Sprintf("Attributes.member.%d.Key", i))
		v := form.Get(fmt.Sprintf("Attributes.member.%d.Value", i))
		if k == "" {
			break
		}
		attrs = append(attrs, attrXML{Key: k, Value: v})
	}
	type modifyResult struct {
		XMLName xml.Name `xml:"ModifyLoadBalancerAttributesResponse"`
		Result  struct {
			Attributes []attrXML `xml:"Attributes>member"`
		} `xml:"ModifyLoadBalancerAttributesResult"`
	}
	var resp modifyResult
	resp.Result.Attributes = attrs
	return elbXMLResponse(http.StatusOK, resp)
}

func (p *Provider) handleDescribeLoadBalancerAttributes(form url.Values) (*plugin.Response, error) {
	arn := form.Get("LoadBalancerArn")
	if arn == "" {
		return elbError("ValidationError", "LoadBalancerArn is required", http.StatusBadRequest), nil
	}
	type attrXML struct {
		Key   string `xml:"Key"`
		Value string `xml:"Value"`
	}
	attrs := []attrXML{
		{Key: "access_logs.s3.enabled", Value: "false"},
		{Key: "deletion_protection.enabled", Value: "false"},
		{Key: "idle_timeout.timeout_seconds", Value: "60"},
	}
	type descResult struct {
		XMLName xml.Name `xml:"DescribeLoadBalancerAttributesResponse"`
		Result  struct {
			Attributes []attrXML `xml:"Attributes>member"`
		} `xml:"DescribeLoadBalancerAttributesResult"`
	}
	var resp descResult
	resp.Result.Attributes = attrs
	return elbXMLResponse(http.StatusOK, resp)
}

func (p *Provider) handleSetSecurityGroups(form url.Values) (*plugin.Response, error) {
	arn := form.Get("LoadBalancerArn")
	if arn == "" {
		return elbError("ValidationError", "LoadBalancerArn is required", http.StatusBadRequest), nil
	}
	var sgs []string
	for i := 1; ; i++ {
		v := form.Get(fmt.Sprintf("SecurityGroups.member.%d", i))
		if v == "" {
			break
		}
		sgs = append(sgs, v)
	}
	if err := p.store.SetLoadBalancerSecurityGroups(arn, sgs); err != nil {
		return elbError("LoadBalancerNotFound", err.Error(), http.StatusBadRequest), nil
	}
	type result struct {
		XMLName xml.Name `xml:"SetSecurityGroupsResponse"`
		Result  struct {
			SecurityGroupIds []string `xml:"SecurityGroupIds>member"`
		} `xml:"SetSecurityGroupsResult"`
	}
	var resp result
	resp.Result.SecurityGroupIds = sgs
	return elbXMLResponse(http.StatusOK, resp)
}

func (p *Provider) handleSetSubnets(form url.Values) (*plugin.Response, error) {
	arn := form.Get("LoadBalancerArn")
	if arn == "" {
		return elbError("ValidationError", "LoadBalancerArn is required", http.StatusBadRequest), nil
	}
	var subnets []string
	for i := 1; ; i++ {
		v := form.Get(fmt.Sprintf("Subnets.member.%d", i))
		if v == "" {
			break
		}
		subnets = append(subnets, v)
	}
	if err := p.store.SetLoadBalancerSubnets(arn, subnets); err != nil {
		return elbError("LoadBalancerNotFound", err.Error(), http.StatusBadRequest), nil
	}
	type result struct {
		XMLName xml.Name `xml:"SetSubnetsResponse"`
		Result  struct {
			AvailabilityZones []availabilityZoneXML `xml:"AvailabilityZones>member"`
		} `xml:"SetSubnetsResult"`
	}
	var resp result
	azs := make([]availabilityZoneXML, 0, len(subnets))
	for _, s := range subnets {
		azs = append(azs, availabilityZoneXML{SubnetId: s, ZoneName: "us-east-1a"})
	}
	resp.Result.AvailabilityZones = azs
	return elbXMLResponse(http.StatusOK, resp)
}

func (p *Provider) handleSetIpAddressType(form url.Values) (*plugin.Response, error) {
	arn := form.Get("LoadBalancerArn")
	ipType := form.Get("IpAddressType")
	if arn == "" || ipType == "" {
		return elbError("ValidationError", "LoadBalancerArn and IpAddressType are required", http.StatusBadRequest), nil
	}
	if err := p.store.SetLoadBalancerIPAddressType(arn, ipType); err != nil {
		return elbError("LoadBalancerNotFound", err.Error(), http.StatusBadRequest), nil
	}
	type result struct {
		XMLName xml.Name `xml:"SetIpAddressTypeResponse"`
		Result  struct {
			IpAddressType string `xml:"IpAddressType"`
		} `xml:"SetIpAddressTypeResult"`
	}
	var resp result
	resp.Result.IpAddressType = ipType
	return elbXMLResponse(http.StatusOK, resp)
}

// --- TargetGroup handlers ---

func (p *Provider) handleCreateTargetGroup(form url.Values) (*plugin.Response, error) {
	name := form.Get("Name")
	if name == "" {
		return elbError("ValidationError", "Name is required", http.StatusBadRequest), nil
	}
	protocol := form.Get("Protocol")
	if protocol == "" {
		protocol = "HTTP"
	}
	port := 80
	if v := form.Get("Port"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			port = n
		}
	}
	vpcID := form.Get("VpcId")
	targetType := form.Get("TargetType")
	if targetType == "" {
		targetType = "instance"
	}
	arn := shared.BuildARN("elasticloadbalancing", "targetgroup", name)
	tg, err := p.store.CreateTargetGroup(arn, name, protocol, port, vpcID, targetType)
	if err != nil {
		if strings.Contains(err.Error(), "UNIQUE constraint failed") {
			return elbError("DuplicateTargetGroupName", "target group already exists: "+name, http.StatusConflict), nil
		}
		return nil, err
	}

	type createTGResult struct {
		XMLName xml.Name `xml:"CreateTargetGroupResponse"`
		Result  struct {
			TargetGroups []targetGroupXML `xml:"TargetGroups>member"`
		} `xml:"CreateTargetGroupResult"`
	}
	var resp createTGResult
	resp.Result.TargetGroups = []targetGroupXML{tgToXML(tg)}
	return elbXMLResponse(http.StatusOK, resp)
}

func (p *Provider) handleDescribeTargetGroups(form url.Values) (*plugin.Response, error) {
	var arns []string
	for i := 1; ; i++ {
		v := form.Get(fmt.Sprintf("TargetGroupArns.member.%d", i))
		if v == "" {
			break
		}
		arns = append(arns, v)
	}

	tgs, err := p.store.ListTargetGroups(arns)
	if err != nil {
		return nil, err
	}

	xmlTGs := make([]targetGroupXML, 0, len(tgs))
	for i := range tgs {
		xmlTGs = append(xmlTGs, tgToXML(&tgs[i]))
	}

	type describeTGsResult struct {
		XMLName xml.Name `xml:"DescribeTargetGroupsResponse"`
		Result  struct {
			TargetGroups []targetGroupXML `xml:"TargetGroups>member"`
		} `xml:"DescribeTargetGroupsResult"`
	}
	var resp describeTGsResult
	resp.Result.TargetGroups = xmlTGs
	return elbXMLResponse(http.StatusOK, resp)
}

func (p *Provider) handleModifyTargetGroup(form url.Values) (*plugin.Response, error) {
	arn := form.Get("TargetGroupArn")
	if arn == "" {
		return elbError("ValidationError", "TargetGroupArn is required", http.StatusBadRequest), nil
	}
	tg, err := p.store.GetTargetGroup(arn)
	if err != nil {
		return elbError("TargetGroupNotFound", "target group not found: "+arn, http.StatusBadRequest), nil
	}
	type modifyTGResult struct {
		XMLName xml.Name `xml:"ModifyTargetGroupResponse"`
		Result  struct {
			TargetGroups []targetGroupXML `xml:"TargetGroups>member"`
		} `xml:"ModifyTargetGroupResult"`
	}
	var resp modifyTGResult
	resp.Result.TargetGroups = []targetGroupXML{tgToXML(tg)}
	return elbXMLResponse(http.StatusOK, resp)
}

func (p *Provider) handleModifyTargetGroupAttributes(form url.Values) (*plugin.Response, error) {
	arn := form.Get("TargetGroupArn")
	if arn == "" {
		return elbError("ValidationError", "TargetGroupArn is required", http.StatusBadRequest), nil
	}
	type attrXML struct {
		Key   string `xml:"Key"`
		Value string `xml:"Value"`
	}
	var attrs []attrXML
	for i := 1; ; i++ {
		k := form.Get(fmt.Sprintf("Attributes.member.%d.Key", i))
		v := form.Get(fmt.Sprintf("Attributes.member.%d.Value", i))
		if k == "" {
			break
		}
		attrs = append(attrs, attrXML{Key: k, Value: v})
	}
	type result struct {
		XMLName xml.Name `xml:"ModifyTargetGroupAttributesResponse"`
		Result  struct {
			Attributes []attrXML `xml:"Attributes>member"`
		} `xml:"ModifyTargetGroupAttributesResult"`
	}
	var resp result
	resp.Result.Attributes = attrs
	return elbXMLResponse(http.StatusOK, resp)
}

func (p *Provider) handleDescribeTargetGroupAttributes(form url.Values) (*plugin.Response, error) {
	arn := form.Get("TargetGroupArn")
	if arn == "" {
		return elbError("ValidationError", "TargetGroupArn is required", http.StatusBadRequest), nil
	}
	type attrXML struct {
		Key   string `xml:"Key"`
		Value string `xml:"Value"`
	}
	attrs := []attrXML{
		{Key: "deregistration_delay.timeout_seconds", Value: "300"},
		{Key: "stickiness.enabled", Value: "false"},
	}
	type result struct {
		XMLName xml.Name `xml:"DescribeTargetGroupAttributesResponse"`
		Result  struct {
			Attributes []attrXML `xml:"Attributes>member"`
		} `xml:"DescribeTargetGroupAttributesResult"`
	}
	var resp result
	resp.Result.Attributes = attrs
	return elbXMLResponse(http.StatusOK, resp)
}

func (p *Provider) handleDeleteTargetGroup(form url.Values) (*plugin.Response, error) {
	arn := form.Get("TargetGroupArn")
	if arn == "" {
		return elbError("ValidationError", "TargetGroupArn is required", http.StatusBadRequest), nil
	}
	if err := p.store.DeleteTargetGroup(arn); err != nil {
		return elbError("TargetGroupNotFound", "target group not found: "+arn, http.StatusBadRequest), nil
	}
	type result struct {
		XMLName xml.Name `xml:"DeleteTargetGroupResponse"`
		Result  struct{} `xml:"DeleteTargetGroupResult"`
	}
	return elbXMLResponse(http.StatusOK, result{})
}

// --- Target handlers ---

func (p *Provider) handleRegisterTargets(form url.Values) (*plugin.Response, error) {
	tgARN := form.Get("TargetGroupArn")
	if tgARN == "" {
		return elbError("ValidationError", "TargetGroupArn is required", http.StatusBadRequest), nil
	}
	for i := 1; ; i++ {
		id := form.Get(fmt.Sprintf("Targets.member.%d.Id", i))
		if id == "" {
			break
		}
		port := 0
		if v := form.Get(fmt.Sprintf("Targets.member.%d.Port", i)); v != "" {
			if n, err := strconv.Atoi(v); err == nil {
				port = n
			}
		}
		az := form.Get(fmt.Sprintf("Targets.member.%d.AvailabilityZone", i))
		if err := p.store.RegisterTarget(tgARN, id, port, az); err != nil {
			return nil, err
		}
	}
	type result struct {
		XMLName xml.Name `xml:"RegisterTargetsResponse"`
		Result  struct{} `xml:"RegisterTargetsResult"`
	}
	return elbXMLResponse(http.StatusOK, result{})
}

func (p *Provider) handleDeregisterTargets(form url.Values) (*plugin.Response, error) {
	tgARN := form.Get("TargetGroupArn")
	if tgARN == "" {
		return elbError("ValidationError", "TargetGroupArn is required", http.StatusBadRequest), nil
	}
	for i := 1; ; i++ {
		id := form.Get(fmt.Sprintf("Targets.member.%d.Id", i))
		if id == "" {
			break
		}
		if err := p.store.DeregisterTarget(tgARN, id); err != nil {
			return nil, err
		}
	}
	type result struct {
		XMLName xml.Name `xml:"DeregisterTargetsResponse"`
		Result  struct{} `xml:"DeregisterTargetsResult"`
	}
	return elbXMLResponse(http.StatusOK, result{})
}

func (p *Provider) handleDescribeTargetHealth(form url.Values) (*plugin.Response, error) {
	tgARN := form.Get("TargetGroupArn")
	if tgARN == "" {
		return elbError("ValidationError", "TargetGroupArn is required", http.StatusBadRequest), nil
	}
	targets, err := p.store.ListTargets(tgARN)
	if err != nil {
		return nil, err
	}
	type targetDescXML struct {
		Id   string `xml:"Id"`
		Port int    `xml:"Port,omitempty"`
	}
	type healthStateXML struct {
		State string `xml:"State"`
	}
	type targetHealthDescXML struct {
		Target      targetDescXML  `xml:"Target"`
		HealthState healthStateXML `xml:"TargetHealth"`
	}
	descs := make([]targetHealthDescXML, 0, len(targets))
	for _, t := range targets {
		descs = append(descs, targetHealthDescXML{
			Target:      targetDescXML{Id: t.TargetID, Port: t.Port},
			HealthState: healthStateXML{State: t.Health},
		})
	}
	type result struct {
		XMLName xml.Name `xml:"DescribeTargetHealthResponse"`
		Result  struct {
			TargetHealthDescriptions []targetHealthDescXML `xml:"TargetHealthDescriptions>member"`
		} `xml:"DescribeTargetHealthResult"`
	}
	var resp result
	resp.Result.TargetHealthDescriptions = descs
	return elbXMLResponse(http.StatusOK, resp)
}

// --- Listener handlers ---

func (p *Provider) handleCreateListener(form url.Values) (*plugin.Response, error) {
	lbARN := form.Get("LoadBalancerArn")
	if lbARN == "" {
		return elbError("ValidationError", "LoadBalancerArn is required", http.StatusBadRequest), nil
	}
	protocol := form.Get("Protocol")
	if protocol == "" {
		protocol = "HTTP"
	}
	port := 80
	if v := form.Get("Port"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			port = n
		}
	}
	sslPolicy := form.Get("SslPolicy")

	var defaultActions []map[string]string
	for i := 1; ; i++ {
		t := form.Get(fmt.Sprintf("DefaultActions.member.%d.Type", i))
		if t == "" {
			break
		}
		a := map[string]string{"Type": t}
		if tgARN := form.Get(fmt.Sprintf("DefaultActions.member.%d.TargetGroupArn", i)); tgARN != "" {
			a["TargetGroupArn"] = tgARN
		}
		defaultActions = append(defaultActions, a)
	}

	var certs []map[string]string
	for i := 1; ; i++ {
		certARN := form.Get(fmt.Sprintf("Certificates.member.%d.CertificateArn", i))
		if certARN == "" {
			break
		}
		certs = append(certs, map[string]string{"CertificateArn": certARN})
	}

	arn := shared.BuildARN("elasticloadbalancing", "listener/app", fmt.Sprintf("%s/%d", lbARN, port))
	listener, err := p.store.CreateListener(arn, lbARN, protocol, port, defaultActions, sslPolicy, certs)
	if err != nil {
		return nil, err
	}

	type result struct {
		XMLName xml.Name `xml:"CreateListenerResponse"`
		Result  struct {
			Listeners []listenerXML `xml:"Listeners>member"`
		} `xml:"CreateListenerResult"`
	}
	var resp result
	resp.Result.Listeners = []listenerXML{listenerToXML(listener)}
	return elbXMLResponse(http.StatusOK, resp)
}

func (p *Provider) handleDescribeListeners(form url.Values) (*plugin.Response, error) {
	lbARN := form.Get("LoadBalancerArn")

	var listenerARNs []string
	for i := 1; ; i++ {
		v := form.Get(fmt.Sprintf("ListenerArns.member.%d", i))
		if v == "" {
			break
		}
		listenerARNs = append(listenerARNs, v)
	}

	var listeners []Listener
	var err error
	if len(listenerARNs) > 0 {
		listeners, err = p.store.ListListenersByARNs(listenerARNs)
	} else {
		listeners, err = p.store.ListListeners(lbARN)
	}
	if err != nil {
		return nil, err
	}

	xmlListeners := make([]listenerXML, 0, len(listeners))
	for i := range listeners {
		xmlListeners = append(xmlListeners, listenerToXML(&listeners[i]))
	}

	type result struct {
		XMLName xml.Name `xml:"DescribeListenersResponse"`
		Result  struct {
			Listeners []listenerXML `xml:"Listeners>member"`
		} `xml:"DescribeListenersResult"`
	}
	var resp result
	resp.Result.Listeners = xmlListeners
	return elbXMLResponse(http.StatusOK, resp)
}

func (p *Provider) handleModifyListener(form url.Values) (*plugin.Response, error) {
	arn := form.Get("ListenerArn")
	if arn == "" {
		return elbError("ValidationError", "ListenerArn is required", http.StatusBadRequest), nil
	}
	l, err := p.store.GetListener(arn)
	if err != nil {
		return elbError("ListenerNotFound", "listener not found: "+arn, http.StatusBadRequest), nil
	}

	if v := form.Get("Protocol"); v != "" {
		l.Protocol = v
	}
	if v := form.Get("Port"); v != "" {
		if n, err2 := strconv.Atoi(v); err2 == nil {
			l.Port = n
		}
	}
	if v := form.Get("SslPolicy"); v != "" {
		l.SSLPolicy = v
	}

	var defaultActions []map[string]string
	for i := 1; ; i++ {
		t := form.Get(fmt.Sprintf("DefaultActions.member.%d.Type", i))
		if t == "" {
			break
		}
		a := map[string]string{"Type": t}
		if tgARN := form.Get(fmt.Sprintf("DefaultActions.member.%d.TargetGroupArn", i)); tgARN != "" {
			a["TargetGroupArn"] = tgARN
		}
		defaultActions = append(defaultActions, a)
	}
	if len(defaultActions) > 0 {
		l.DefaultActions = defaultActions
	}

	if err := p.store.UpdateListener(arn, l.Protocol, l.Port, l.DefaultActions, l.SSLPolicy, l.Certificates); err != nil {
		return nil, err
	}

	type result struct {
		XMLName xml.Name `xml:"ModifyListenerResponse"`
		Result  struct {
			Listeners []listenerXML `xml:"Listeners>member"`
		} `xml:"ModifyListenerResult"`
	}
	var resp result
	resp.Result.Listeners = []listenerXML{listenerToXML(l)}
	return elbXMLResponse(http.StatusOK, resp)
}

func (p *Provider) handleDeleteListener(form url.Values) (*plugin.Response, error) {
	arn := form.Get("ListenerArn")
	if arn == "" {
		return elbError("ValidationError", "ListenerArn is required", http.StatusBadRequest), nil
	}
	if err := p.store.DeleteListener(arn); err != nil {
		return elbError("ListenerNotFound", "listener not found: "+arn, http.StatusBadRequest), nil
	}
	type result struct {
		XMLName xml.Name `xml:"DeleteListenerResponse"`
		Result  struct{} `xml:"DeleteListenerResult"`
	}
	return elbXMLResponse(http.StatusOK, result{})
}

// --- Rule handlers ---

func (p *Provider) handleCreateRule(form url.Values) (*plugin.Response, error) {
	listenerARN := form.Get("ListenerArn")
	if listenerARN == "" {
		return elbError("ValidationError", "ListenerArn is required", http.StatusBadRequest), nil
	}
	priority := form.Get("Priority")
	if priority == "" {
		priority = "1"
	}

	var conditions []map[string]string
	for i := 1; ; i++ {
		field := form.Get(fmt.Sprintf("Conditions.member.%d.Field", i))
		if field == "" {
			break
		}
		var vals []string
		for j := 1; ; j++ {
			v := form.Get(fmt.Sprintf("Conditions.member.%d.Values.member.%d", i, j))
			if v == "" {
				break
			}
			vals = append(vals, v)
		}
		conditions = append(conditions, map[string]string{"Field": field, "Values": strings.Join(vals, ",")})
	}

	var actions []map[string]string
	for i := 1; ; i++ {
		t := form.Get(fmt.Sprintf("Actions.member.%d.Type", i))
		if t == "" {
			break
		}
		a := map[string]string{"Type": t}
		if tgARN := form.Get(fmt.Sprintf("Actions.member.%d.TargetGroupArn", i)); tgARN != "" {
			a["TargetGroupArn"] = tgARN
		}
		actions = append(actions, a)
	}

	arn := shared.BuildARN("elasticloadbalancing", "listener-rule/app", fmt.Sprintf("%s/%s", listenerARN, priority))
	rule, err := p.store.CreateRule(arn, listenerARN, priority, conditions, actions, false)
	if err != nil {
		return nil, err
	}

	type result struct {
		XMLName xml.Name `xml:"CreateRuleResponse"`
		Result  struct {
			Rules []ruleXML `xml:"Rules>member"`
		} `xml:"CreateRuleResult"`
	}
	var resp result
	resp.Result.Rules = []ruleXML{ruleToXML(rule)}
	return elbXMLResponse(http.StatusOK, resp)
}

func (p *Provider) handleDescribeRules(form url.Values) (*plugin.Response, error) {
	listenerARN := form.Get("ListenerArn")

	var ruleARNs []string
	for i := 1; ; i++ {
		v := form.Get(fmt.Sprintf("RuleArns.member.%d", i))
		if v == "" {
			break
		}
		ruleARNs = append(ruleARNs, v)
	}

	var rules []Rule
	var err error
	if len(ruleARNs) > 0 {
		rules, err = p.store.ListRulesByARNs(ruleARNs)
	} else {
		rules, err = p.store.ListRules(listenerARN)
	}
	if err != nil {
		return nil, err
	}

	xmlRules := make([]ruleXML, 0, len(rules))
	for i := range rules {
		xmlRules = append(xmlRules, ruleToXML(&rules[i]))
	}

	type result struct {
		XMLName xml.Name `xml:"DescribeRulesResponse"`
		Result  struct {
			Rules []ruleXML `xml:"Rules>member"`
		} `xml:"DescribeRulesResult"`
	}
	var resp result
	resp.Result.Rules = xmlRules
	return elbXMLResponse(http.StatusOK, resp)
}

func (p *Provider) handleModifyRule(form url.Values) (*plugin.Response, error) {
	arn := form.Get("RuleArn")
	if arn == "" {
		return elbError("ValidationError", "RuleArn is required", http.StatusBadRequest), nil
	}
	r, err := p.store.GetRule(arn)
	if err != nil {
		return elbError("RuleNotFound", "rule not found: "+arn, http.StatusBadRequest), nil
	}

	var conditions []map[string]string
	for i := 1; ; i++ {
		field := form.Get(fmt.Sprintf("Conditions.member.%d.Field", i))
		if field == "" {
			break
		}
		var vals []string
		for j := 1; ; j++ {
			v := form.Get(fmt.Sprintf("Conditions.member.%d.Values.member.%d", i, j))
			if v == "" {
				break
			}
			vals = append(vals, v)
		}
		conditions = append(conditions, map[string]string{"Field": field, "Values": strings.Join(vals, ",")})
	}
	if len(conditions) > 0 {
		r.Conditions = conditions
	}

	var actions []map[string]string
	for i := 1; ; i++ {
		t := form.Get(fmt.Sprintf("Actions.member.%d.Type", i))
		if t == "" {
			break
		}
		a := map[string]string{"Type": t}
		if tgARN := form.Get(fmt.Sprintf("Actions.member.%d.TargetGroupArn", i)); tgARN != "" {
			a["TargetGroupArn"] = tgARN
		}
		actions = append(actions, a)
	}
	if len(actions) > 0 {
		r.Actions = actions
	}

	if err := p.store.UpdateRule(arn, r.Conditions, r.Actions); err != nil {
		return nil, err
	}

	type result struct {
		XMLName xml.Name `xml:"ModifyRuleResponse"`
		Result  struct {
			Rules []ruleXML `xml:"Rules>member"`
		} `xml:"ModifyRuleResult"`
	}
	var resp result
	resp.Result.Rules = []ruleXML{ruleToXML(r)}
	return elbXMLResponse(http.StatusOK, resp)
}

func (p *Provider) handleDeleteRule(form url.Values) (*plugin.Response, error) {
	arn := form.Get("RuleArn")
	if arn == "" {
		return elbError("ValidationError", "RuleArn is required", http.StatusBadRequest), nil
	}
	if err := p.store.DeleteRule(arn); err != nil {
		return elbError("RuleNotFound", "rule not found: "+arn, http.StatusBadRequest), nil
	}
	type result struct {
		XMLName xml.Name `xml:"DeleteRuleResponse"`
		Result  struct{} `xml:"DeleteRuleResult"`
	}
	return elbXMLResponse(http.StatusOK, result{})
}

func (p *Provider) handleSetRulePriorities(form url.Values) (*plugin.Response, error) {
	var updatedRules []Rule
	for i := 1; ; i++ {
		arn := form.Get(fmt.Sprintf("RulePriorities.member.%d.RuleArn", i))
		priority := form.Get(fmt.Sprintf("RulePriorities.member.%d.Priority", i))
		if arn == "" {
			break
		}
		if err := p.store.UpdateRulePriority(arn, priority); err != nil {
			return elbError("RuleNotFound", "rule not found: "+arn, http.StatusBadRequest), nil
		}
		r, err := p.store.GetRule(arn)
		if err != nil {
			return nil, err
		}
		updatedRules = append(updatedRules, *r)
	}

	xmlRules := make([]ruleXML, 0, len(updatedRules))
	for i := range updatedRules {
		xmlRules = append(xmlRules, ruleToXML(&updatedRules[i]))
	}

	type result struct {
		XMLName xml.Name `xml:"SetRulePrioritiesResponse"`
		Result  struct {
			Rules []ruleXML `xml:"Rules>member"`
		} `xml:"SetRulePrioritiesResult"`
	}
	var resp result
	resp.Result.Rules = xmlRules
	return elbXMLResponse(http.StatusOK, resp)
}

// --- Tag handlers ---

func (p *Provider) handleAddTags(form url.Values) (*plugin.Response, error) {
	var arns []string
	for i := 1; ; i++ {
		v := form.Get(fmt.Sprintf("ResourceArns.member.%d", i))
		if v == "" {
			break
		}
		arns = append(arns, v)
	}
	tags := map[string]string{}
	for i := 1; ; i++ {
		k := form.Get(fmt.Sprintf("Tags.member.%d.Key", i))
		v := form.Get(fmt.Sprintf("Tags.member.%d.Value", i))
		if k == "" {
			break
		}
		tags[k] = v
	}
	for _, arn := range arns {
		if err := p.store.AddTags(arn, tags); err != nil {
			return nil, err
		}
	}
	type result struct {
		XMLName xml.Name `xml:"AddTagsResponse"`
		Result  struct{} `xml:"AddTagsResult"`
	}
	return elbXMLResponse(http.StatusOK, result{})
}

func (p *Provider) handleRemoveTags(form url.Values) (*plugin.Response, error) {
	var arns []string
	for i := 1; ; i++ {
		v := form.Get(fmt.Sprintf("ResourceArns.member.%d", i))
		if v == "" {
			break
		}
		arns = append(arns, v)
	}
	var keys []string
	for i := 1; ; i++ {
		k := form.Get(fmt.Sprintf("TagKeys.member.%d", i))
		if k == "" {
			break
		}
		keys = append(keys, k)
	}
	for _, arn := range arns {
		if err := p.store.RemoveTags(arn, keys); err != nil {
			return nil, err
		}
	}
	type result struct {
		XMLName xml.Name `xml:"RemoveTagsResponse"`
		Result  struct{} `xml:"RemoveTagsResult"`
	}
	return elbXMLResponse(http.StatusOK, result{})
}

func (p *Provider) handleDescribeTags(form url.Values) (*plugin.Response, error) {
	var arns []string
	for i := 1; ; i++ {
		v := form.Get(fmt.Sprintf("ResourceArns.member.%d", i))
		if v == "" {
			break
		}
		arns = append(arns, v)
	}

	type tagDescXML struct {
		ResourceArn string   `xml:"ResourceArn"`
		Tags        []tagXML `xml:"Tags>member"`
	}
	var descs []tagDescXML
	for _, arn := range arns {
		tagMap, err := p.store.ListTags(arn)
		if err != nil {
			return nil, err
		}
		xmlTags := make([]tagXML, 0, len(tagMap))
		for k, v := range tagMap {
			xmlTags = append(xmlTags, tagXML{Key: k, Value: v})
		}
		descs = append(descs, tagDescXML{ResourceArn: arn, Tags: xmlTags})
	}

	type result struct {
		XMLName xml.Name `xml:"DescribeTagsResponse"`
		Result  struct {
			TagDescriptions []tagDescXML `xml:"TagDescriptions>member"`
		} `xml:"DescribeTagsResult"`
	}
	var resp result
	resp.Result.TagDescriptions = descs
	return elbXMLResponse(http.StatusOK, resp)
}

// --- SSL Policies ---

func (p *Provider) handleDescribeSSLPolicies(form url.Values) (*plugin.Response, error) {
	type sslPolicyXML struct {
		Name string `xml:"Name"`
	}
	policies := []sslPolicyXML{
		{Name: "ELBSecurityPolicy-2016-08"},
		{Name: "ELBSecurityPolicy-TLS13-1-2-2021-06"},
		{Name: "ELBSecurityPolicy-FS-1-2-Res-2020-10"},
	}
	type result struct {
		XMLName xml.Name `xml:"DescribeSSLPoliciesResponse"`
		Result  struct {
			SslPolicies []sslPolicyXML `xml:"SslPolicies>member"`
		} `xml:"DescribeSSLPoliciesResult"`
	}
	var resp result
	resp.Result.SslPolicies = policies
	return elbXMLResponse(http.StatusOK, resp)
}

// --- Account Limits ---

func (p *Provider) handleDescribeAccountLimits(form url.Values) (*plugin.Response, error) {
	type limitXML struct {
		Name string `xml:"Name"`
		Max  string `xml:"Max"`
	}
	limits := []limitXML{
		{Name: "application-load-balancers", Max: "20"},
		{Name: "target-groups", Max: "3000"},
		{Name: "listeners-per-application-load-balancer", Max: "50"},
		{Name: "rules-per-application-load-balancer", Max: "100"},
	}
	type result struct {
		XMLName xml.Name `xml:"DescribeAccountLimitsResponse"`
		Result  struct {
			Limits []limitXML `xml:"Limits>member"`
		} `xml:"DescribeAccountLimitsResult"`
	}
	var resp result
	resp.Result.Limits = limits
	return elbXMLResponse(http.StatusOK, resp)
}

// --- Listener Certificates ---

func (p *Provider) handleAddListenerCertificates(form url.Values) (*plugin.Response, error) {
	arn := form.Get("ListenerArn")
	if arn == "" {
		return elbError("ValidationError", "ListenerArn is required", http.StatusBadRequest), nil
	}
	var certs []map[string]string
	for i := 1; ; i++ {
		certARN := form.Get(fmt.Sprintf("Certificates.member.%d.CertificateArn", i))
		if certARN == "" {
			break
		}
		certs = append(certs, map[string]string{"CertificateArn": certARN})
	}
	if err := p.store.AddListenerCertificates(arn, certs); err != nil {
		return elbError("ListenerNotFound", err.Error(), http.StatusBadRequest), nil
	}
	xmlCerts := make([]certificateXML, 0, len(certs))
	for _, c := range certs {
		xmlCerts = append(xmlCerts, certificateXML{CertificateArn: c["CertificateArn"]})
	}
	type result struct {
		XMLName xml.Name `xml:"AddListenerCertificatesResponse"`
		Result  struct {
			Certificates []certificateXML `xml:"Certificates>member"`
		} `xml:"AddListenerCertificatesResult"`
	}
	var resp result
	resp.Result.Certificates = xmlCerts
	return elbXMLResponse(http.StatusOK, resp)
}

func (p *Provider) handleRemoveListenerCertificates(form url.Values) (*plugin.Response, error) {
	arn := form.Get("ListenerArn")
	if arn == "" {
		return elbError("ValidationError", "ListenerArn is required", http.StatusBadRequest), nil
	}
	var certs []map[string]string
	for i := 1; ; i++ {
		certARN := form.Get(fmt.Sprintf("Certificates.member.%d.CertificateArn", i))
		if certARN == "" {
			break
		}
		certs = append(certs, map[string]string{"CertificateArn": certARN})
	}
	if err := p.store.RemoveListenerCertificates(arn, certs); err != nil {
		return elbError("ListenerNotFound", err.Error(), http.StatusBadRequest), nil
	}
	type result struct {
		XMLName xml.Name `xml:"RemoveListenerCertificatesResponse"`
		Result  struct{} `xml:"RemoveListenerCertificatesResult"`
	}
	return elbXMLResponse(http.StatusOK, result{})
}

func (p *Provider) handleDescribeListenerCertificates(form url.Values) (*plugin.Response, error) {
	arn := form.Get("ListenerArn")
	if arn == "" {
		return elbError("ValidationError", "ListenerArn is required", http.StatusBadRequest), nil
	}
	l, err := p.store.GetListener(arn)
	if err != nil {
		return elbError("ListenerNotFound", "listener not found: "+arn, http.StatusBadRequest), nil
	}
	xmlCerts := make([]certificateXML, 0, len(l.Certificates))
	for _, c := range l.Certificates {
		xmlCerts = append(xmlCerts, certificateXML{CertificateArn: c["CertificateArn"]})
	}
	type result struct {
		XMLName xml.Name `xml:"DescribeListenerCertificatesResponse"`
		Result  struct {
			Certificates []certificateXML `xml:"Certificates>member"`
		} `xml:"DescribeListenerCertificatesResult"`
	}
	var resp result
	resp.Result.Certificates = xmlCerts
	return elbXMLResponse(http.StatusOK, resp)
}
