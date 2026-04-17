// SPDX-License-Identifier: Apache-2.0

package autoscaling

import (
	"context"
	"encoding/xml"
	"errors"
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

// Provider implements the AutoScaling_2011_01_01 service (Query/XML protocol).
type Provider struct {
	store *Store
}

func (p *Provider) ServiceID() string             { return "autoscaling" }
func (p *Provider) ServiceName() string           { return "AutoScaling_2011_01_01" }
func (p *Provider) Protocol() plugin.ProtocolType { return plugin.ProtocolQuery }

func (p *Provider) Init(cfg plugin.PluginConfig) error {
	if err := os.MkdirAll(cfg.DataDir, 0o755); err != nil {
		return fmt.Errorf("init autoscaling: %w", err)
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
		return asError("InvalidRequest", "failed to read request body", http.StatusBadRequest), nil
	}
	form, err := url.ParseQuery(string(body))
	if err != nil {
		return asError("InvalidRequest", "failed to parse form body", http.StatusBadRequest), nil
	}
	action := op
	if action == "" {
		action = form.Get("Action")
	}

	switch action {
	// AutoScalingGroup
	case "CreateAutoScalingGroup":
		return p.handleCreateAutoScalingGroup(form)
	case "DescribeAutoScalingGroups":
		return p.handleDescribeAutoScalingGroups(form)
	case "UpdateAutoScalingGroup":
		return p.handleUpdateAutoScalingGroup(form)
	case "DeleteAutoScalingGroup":
		return p.handleDeleteAutoScalingGroup(form)
	case "SetDesiredCapacity":
		return p.handleSetDesiredCapacity(form)

	// LaunchConfiguration
	case "CreateLaunchConfiguration":
		return p.handleCreateLaunchConfiguration(form)
	case "DescribeLaunchConfigurations":
		return p.handleDescribeLaunchConfigurations(form)
	case "DeleteLaunchConfiguration":
		return p.handleDeleteLaunchConfiguration(form)

	// ScalingPolicy
	case "PutScalingPolicy":
		return p.handlePutScalingPolicy(form)
	case "DescribePolicies":
		return p.handleDescribePolicies(form)
	case "DeletePolicy":
		return p.handleDeletePolicy(form)
	case "ExecutePolicy":
		return p.handleExecutePolicy(form)

	// ScheduledAction
	case "PutScheduledUpdateGroupAction":
		return p.handlePutScheduledUpdateGroupAction(form)
	case "DescribeScheduledActions":
		return p.handleDescribeScheduledActions(form)
	case "DeleteScheduledAction":
		return p.handleDeleteScheduledAction(form)
	case "BatchPutScheduledUpdateGroupAction":
		return p.handleBatchPutScheduledUpdateGroupAction(form)
	case "BatchDeleteScheduledAction":
		return p.handleBatchDeleteScheduledAction(form)

	// LifecycleHook
	case "PutLifecycleHook":
		return p.handlePutLifecycleHook(form)
	case "DescribeLifecycleHooks":
		return p.handleDescribeLifecycleHooks(form)
	case "DeleteLifecycleHook":
		return p.handleDeleteLifecycleHook(form)
	case "CompleteLifecycleAction":
		return p.handleCompleteLifecycleAction(form)
	case "RecordLifecycleActionHeartbeat":
		return p.handleRecordLifecycleActionHeartbeat(form)

	// Instance operations (no-op / stub)
	case "AttachInstances":
		return p.handleAttachInstances(form)
	case "DetachInstances":
		return p.handleDetachInstances(form)
	case "SetInstanceHealth":
		return p.handleSetInstanceHealth(form)
	case "SetInstanceProtection":
		return p.handleSetInstanceProtection(form)
	case "EnterStandby":
		return p.handleEnterStandby(form)
	case "ExitStandby":
		return p.handleExitStandby(form)

	// Load balancer operations (stub)
	case "AttachLoadBalancers":
		return p.handleAttachLoadBalancers(form)
	case "DetachLoadBalancers":
		return p.handleDetachLoadBalancers(form)
	case "AttachLoadBalancerTargetGroups":
		return p.handleAttachLoadBalancerTargetGroups(form)
	case "DetachLoadBalancerTargetGroups":
		return p.handleDetachLoadBalancerTargetGroups(form)
	case "AttachTrafficSources":
		return p.handleAttachTrafficSources(form)
	case "DetachTrafficSources":
		return p.handleDetachTrafficSources(form)

	// Tags
	case "CreateOrUpdateTags":
		return p.handleCreateOrUpdateTags(form)
	case "DeleteTags":
		return p.handleDeleteTags(form)
	case "DescribeTags":
		return p.handleDescribeTags(form)

	// Misc
	case "DescribeAccountLimits":
		return p.handleDescribeAccountLimits(form)
	case "DescribeAutoScalingInstances":
		return p.handleDescribeAutoScalingInstances(form)
	case "StartInstanceRefresh":
		return p.handleStartInstanceRefresh(form)
	case "DescribeInstanceRefreshes":
		return p.handleDescribeInstanceRefreshes(form)
	case "CancelInstanceRefresh":
		return p.handleCancelInstanceRefresh(form)
	case "RollbackInstanceRefresh":
		return p.handleRollbackInstanceRefresh(form)
	case "SuspendProcesses":
		return p.handleSuspendProcesses(form)
	case "ResumeProcesses":
		return p.handleResumeProcesses(form)
	case "EnableMetricsCollection":
		return p.handleEnableMetricsCollection(form)
	case "DisableMetricsCollection":
		return p.handleDisableMetricsCollection(form)
	case "TerminateInstanceInAutoScalingGroup":
		return p.handleTerminateInstanceInAutoScalingGroup(form)

	default:
		type genericResult struct {
			XMLName xml.Name `xml:"GenericResponse"`
		}
		return asXMLResponse(http.StatusOK, genericResult{XMLName: xml.Name{Local: action + "Response"}})
	}
}

func (p *Provider) ListResources(ctx context.Context) ([]plugin.Resource, error) {
	asgs, err := p.store.ListASGs(nil)
	if err != nil {
		return nil, err
	}
	out := make([]plugin.Resource, 0, len(asgs))
	for _, a := range asgs {
		out = append(out, plugin.Resource{Type: "auto-scaling-group", ID: a.Name, Name: a.Name})
	}
	return out, nil
}

func (p *Provider) GetMetrics(_ context.Context) (*plugin.ServiceMetrics, error) {
	return &plugin.ServiceMetrics{}, nil
}

// --- helpers ---

func asError(code, msg string, status int) *plugin.Response {
	return shared.QueryXMLError(code, msg, status)
}

func asXMLResponse(status int, v any) (*plugin.Response, error) {
	return shared.XMLResponse(status, v)
}

// --- XML types ---

type asgXML struct {
	AutoScalingGroupName    string `xml:"AutoScalingGroupName"`
	AutoScalingGroupARN     string `xml:"AutoScalingGroupARN"`
	LaunchConfigurationName string `xml:"LaunchConfigurationName,omitempty"`
	MinSize                 int    `xml:"MinSize"`
	MaxSize                 int    `xml:"MaxSize"`
	DesiredCapacity         int    `xml:"DesiredCapacity"`
	HealthCheckType         string `xml:"HealthCheckType"`
	VPCZoneIdentifier       string `xml:"VPCZoneIdentifier,omitempty"`
	CreatedTime             string `xml:"CreatedTime"`
	Status                  string `xml:"Status,omitempty"`
}

func asgToXML(a *AutoScalingGroup) asgXML {
	return asgXML{
		AutoScalingGroupName:    a.Name,
		AutoScalingGroupARN:     a.ARN,
		LaunchConfigurationName: a.LaunchConfig,
		MinSize:                 a.MinSize,
		MaxSize:                 a.MaxSize,
		DesiredCapacity:         a.Desired,
		HealthCheckType:         a.HealthCheck,
		VPCZoneIdentifier:       a.VPCZones,
		CreatedTime:             a.CreatedAt.UTC().Format(time.RFC3339),
		Status:                  a.Status,
	}
}

type lcXML struct {
	LaunchConfigurationName string `xml:"LaunchConfigurationName"`
	LaunchConfigurationARN  string `xml:"LaunchConfigurationARN"`
	ImageId                 string `xml:"ImageId"`
	InstanceType            string `xml:"InstanceType"`
	KeyName                 string `xml:"KeyName,omitempty"`
	CreatedTime             string `xml:"CreatedTime"`
}

func lcToXML(lc *LaunchConfiguration) lcXML {
	return lcXML{
		LaunchConfigurationName: lc.Name,
		LaunchConfigurationARN:  lc.ARN,
		ImageId:                 lc.ImageID,
		InstanceType:            lc.InstanceType,
		KeyName:                 lc.KeyName,
		CreatedTime:             lc.CreatedAt.UTC().Format(time.RFC3339),
	}
}

type policyXML struct {
	PolicyARN            string `xml:"PolicyARN"`
	PolicyName           string `xml:"PolicyName"`
	AutoScalingGroupName string `xml:"AutoScalingGroupName"`
	PolicyType           string `xml:"PolicyType"`
	AdjustmentType       string `xml:"AdjustmentType,omitempty"`
	ScalingAdjustment    int    `xml:"ScalingAdjustment,omitempty"`
}

func policyToXML(pol *ScalingPolicy) policyXML {
	return policyXML{
		PolicyARN:            pol.ARN,
		PolicyName:           pol.Name,
		AutoScalingGroupName: pol.ASGName,
		PolicyType:           pol.PolicyType,
		AdjustmentType:       pol.AdjustmentType,
		ScalingAdjustment:    pol.ScalingAdjustment,
	}
}

type scheduledActionXML struct {
	ScheduledActionName  string `xml:"ScheduledActionName"`
	ScheduledActionARN   string `xml:"ScheduledActionARN"`
	AutoScalingGroupName string `xml:"AutoScalingGroupName"`
	Recurrence           string `xml:"Recurrence,omitempty"`
	MinSize              int    `xml:"MinSize,omitempty"`
	MaxSize              int    `xml:"MaxSize,omitempty"`
	DesiredCapacity      int    `xml:"DesiredCapacity,omitempty"`
}

func scheduledActionToXML(a *ScheduledAction) scheduledActionXML {
	x := scheduledActionXML{
		ScheduledActionName:  a.Name,
		ScheduledActionARN:   a.ARN,
		AutoScalingGroupName: a.ASGName,
		Recurrence:           a.Schedule,
	}
	if a.MinSize >= 0 {
		x.MinSize = a.MinSize
	}
	if a.MaxSize >= 0 {
		x.MaxSize = a.MaxSize
	}
	if a.Desired >= 0 {
		x.DesiredCapacity = a.Desired
	}
	return x
}

type lifecycleHookXML struct {
	LifecycleHookName    string `xml:"LifecycleHookName"`
	AutoScalingGroupName string `xml:"AutoScalingGroupName"`
	LifecycleTransition  string `xml:"LifecycleTransition"`
	HeartbeatTimeout     int    `xml:"HeartbeatTimeout"`
	DefaultResult        string `xml:"DefaultResult"`
	RoleARN              string `xml:"RoleARN,omitempty"`
}

func hookToXML(h *LifecycleHook) lifecycleHookXML {
	return lifecycleHookXML{
		LifecycleHookName:    h.Name,
		AutoScalingGroupName: h.ASGName,
		LifecycleTransition:  h.Transition,
		HeartbeatTimeout:     h.HeartbeatTimeout,
		DefaultResult:        h.DefaultResult,
		RoleARN:              h.RoleARN,
	}
}

// --- AutoScalingGroup handlers ---

func (p *Provider) handleCreateAutoScalingGroup(form url.Values) (*plugin.Response, error) {
	name := form.Get("AutoScalingGroupName")
	if name == "" {
		return asError("MissingParameter", "AutoScalingGroupName is required", http.StatusBadRequest), nil
	}
	launchConfig := form.Get("LaunchConfigurationName")
	minSize := parseInt(form.Get("MinSize"), 0)
	maxSize := parseInt(form.Get("MaxSize"), 10)
	desired := parseInt(form.Get("DesiredCapacity"), minSize)
	vpcZones := form.Get("VPCZoneIdentifier")
	healthCheck := form.Get("HealthCheckType")
	if healthCheck == "" {
		healthCheck = "EC2"
	}
	arn := shared.BuildARN("autoscaling", "autoScalingGroup", name)
	_, err := p.store.CreateASG(name, arn, launchConfig, minSize, maxSize, desired, "[]", vpcZones, healthCheck, "[]")
	if err != nil {
		if strings.Contains(err.Error(), "UNIQUE constraint failed") {
			return asError("AlreadyExists", "auto scaling group already exists: "+name, http.StatusConflict), nil
		}
		return nil, err
	}

	type createASGResponse struct {
		XMLName xml.Name `xml:"CreateAutoScalingGroupResponse"`
		Meta    struct {
			RequestId string `xml:"RequestId"`
		} `xml:"ResponseMetadata"`
	}
	var resp createASGResponse
	resp.Meta.RequestId = shared.GenerateUUID()
	return asXMLResponse(http.StatusOK, resp)
}

func (p *Provider) handleDescribeAutoScalingGroups(form url.Values) (*plugin.Response, error) {
	var names []string
	for i := 1; ; i++ {
		key := fmt.Sprintf("AutoScalingGroupNames.member.%d", i)
		v := form.Get(key)
		if v == "" {
			break
		}
		names = append(names, v)
	}
	asgs, err := p.store.ListASGs(names)
	if err != nil {
		return nil, err
	}
	items := make([]asgXML, 0, len(asgs))
	for i := range asgs {
		items = append(items, asgToXML(&asgs[i]))
	}

	type describeASGsResponse struct {
		XMLName xml.Name `xml:"DescribeAutoScalingGroupsResponse"`
		Groups  []asgXML `xml:"DescribeAutoScalingGroupsResult>AutoScalingGroups>member"`
	}
	return asXMLResponse(http.StatusOK, describeASGsResponse{Groups: items})
}

func (p *Provider) handleUpdateAutoScalingGroup(form url.Values) (*plugin.Response, error) {
	name := form.Get("AutoScalingGroupName")
	if name == "" {
		return asError("MissingParameter", "AutoScalingGroupName is required", http.StatusBadRequest), nil
	}
	existing, err := p.store.GetASG(name)
	if err != nil {
		if errors.Is(err, errASGNotFound) {
			return asError("ValidationError", "auto scaling group not found: "+name, http.StatusBadRequest), nil
		}
		return nil, err
	}
	launchConfig := form.Get("LaunchConfigurationName")
	if launchConfig == "" {
		launchConfig = existing.LaunchConfig
	}
	minSize := existing.MinSize
	if v := form.Get("MinSize"); v != "" {
		minSize = parseInt(v, minSize)
	}
	maxSize := existing.MaxSize
	if v := form.Get("MaxSize"); v != "" {
		maxSize = parseInt(v, maxSize)
	}
	desired := existing.Desired
	if v := form.Get("DesiredCapacity"); v != "" {
		desired = parseInt(v, desired)
	}
	vpcZones := form.Get("VPCZoneIdentifier")
	healthCheck := form.Get("HealthCheckType")

	if err := p.store.UpdateASG(name, launchConfig, minSize, maxSize, desired, vpcZones, healthCheck); err != nil {
		if errors.Is(err, errASGNotFound) {
			return asError("ValidationError", "auto scaling group not found: "+name, http.StatusBadRequest), nil
		}
		return nil, err
	}

	type updateASGResponse struct {
		XMLName xml.Name `xml:"UpdateAutoScalingGroupResponse"`
		Meta    struct {
			RequestId string `xml:"RequestId"`
		} `xml:"ResponseMetadata"`
	}
	var resp updateASGResponse
	resp.Meta.RequestId = shared.GenerateUUID()
	return asXMLResponse(http.StatusOK, resp)
}

func (p *Provider) handleDeleteAutoScalingGroup(form url.Values) (*plugin.Response, error) {
	name := form.Get("AutoScalingGroupName")
	if name == "" {
		return asError("MissingParameter", "AutoScalingGroupName is required", http.StatusBadRequest), nil
	}
	_, err := p.store.DeleteASG(name)
	if err != nil {
		if errors.Is(err, errASGNotFound) {
			return asError("ValidationError", "auto scaling group not found: "+name, http.StatusBadRequest), nil
		}
		return nil, err
	}

	type deleteASGResponse struct {
		XMLName xml.Name `xml:"DeleteAutoScalingGroupResponse"`
		Meta    struct {
			RequestId string `xml:"RequestId"`
		} `xml:"ResponseMetadata"`
	}
	var resp deleteASGResponse
	resp.Meta.RequestId = shared.GenerateUUID()
	return asXMLResponse(http.StatusOK, resp)
}

func (p *Provider) handleSetDesiredCapacity(form url.Values) (*plugin.Response, error) {
	name := form.Get("AutoScalingGroupName")
	if name == "" {
		return asError("MissingParameter", "AutoScalingGroupName is required", http.StatusBadRequest), nil
	}
	desired := parseInt(form.Get("DesiredCapacity"), 0)
	if err := p.store.SetDesiredCapacity(name, desired); err != nil {
		if errors.Is(err, errASGNotFound) {
			return asError("ValidationError", "auto scaling group not found: "+name, http.StatusBadRequest), nil
		}
		return nil, err
	}

	type setDesiredResponse struct {
		XMLName xml.Name `xml:"SetDesiredCapacityResponse"`
		Meta    struct {
			RequestId string `xml:"RequestId"`
		} `xml:"ResponseMetadata"`
	}
	var resp setDesiredResponse
	resp.Meta.RequestId = shared.GenerateUUID()
	return asXMLResponse(http.StatusOK, resp)
}

// --- LaunchConfiguration handlers ---

func (p *Provider) handleCreateLaunchConfiguration(form url.Values) (*plugin.Response, error) {
	name := form.Get("LaunchConfigurationName")
	if name == "" {
		return asError("MissingParameter", "LaunchConfigurationName is required", http.StatusBadRequest), nil
	}
	imageID := form.Get("ImageId")
	instanceType := form.Get("InstanceType")
	if instanceType == "" {
		instanceType = "t3.micro"
	}
	keyName := form.Get("KeyName")
	secGroups := collectMembers(form, "SecurityGroups.member")
	secGroupsJSON := toJSONArray(secGroups)
	arn := shared.BuildARN("autoscaling", "launchConfiguration", name)
	_, err := p.store.CreateLC(name, arn, imageID, instanceType, keyName, secGroupsJSON)
	if err != nil {
		if strings.Contains(err.Error(), "UNIQUE constraint failed") {
			return asError("AlreadyExists", "launch configuration already exists: "+name, http.StatusConflict), nil
		}
		return nil, err
	}

	type createLCResponse struct {
		XMLName xml.Name `xml:"CreateLaunchConfigurationResponse"`
		Meta    struct {
			RequestId string `xml:"RequestId"`
		} `xml:"ResponseMetadata"`
	}
	var resp createLCResponse
	resp.Meta.RequestId = shared.GenerateUUID()
	return asXMLResponse(http.StatusOK, resp)
}

func (p *Provider) handleDescribeLaunchConfigurations(form url.Values) (*plugin.Response, error) {
	var names []string
	for i := 1; ; i++ {
		key := fmt.Sprintf("LaunchConfigurationNames.member.%d", i)
		v := form.Get(key)
		if v == "" {
			break
		}
		names = append(names, v)
	}
	lcs, err := p.store.ListLCs(names)
	if err != nil {
		return nil, err
	}
	items := make([]lcXML, 0, len(lcs))
	for i := range lcs {
		items = append(items, lcToXML(&lcs[i]))
	}

	type describeLCsResponse struct {
		XMLName xml.Name `xml:"DescribeLaunchConfigurationsResponse"`
		LCs     []lcXML  `xml:"DescribeLaunchConfigurationsResult>LaunchConfigurations>member"`
	}
	return asXMLResponse(http.StatusOK, describeLCsResponse{LCs: items})
}

func (p *Provider) handleDeleteLaunchConfiguration(form url.Values) (*plugin.Response, error) {
	name := form.Get("LaunchConfigurationName")
	if name == "" {
		return asError("MissingParameter", "LaunchConfigurationName is required", http.StatusBadRequest), nil
	}
	_, err := p.store.DeleteLC(name)
	if err != nil {
		if errors.Is(err, errLCNotFound) {
			return asError("ValidationError", "launch configuration not found: "+name, http.StatusBadRequest), nil
		}
		return nil, err
	}

	type deleteLCResponse struct {
		XMLName xml.Name `xml:"DeleteLaunchConfigurationResponse"`
		Meta    struct {
			RequestId string `xml:"RequestId"`
		} `xml:"ResponseMetadata"`
	}
	var resp deleteLCResponse
	resp.Meta.RequestId = shared.GenerateUUID()
	return asXMLResponse(http.StatusOK, resp)
}

// --- ScalingPolicy handlers ---

func (p *Provider) handlePutScalingPolicy(form url.Values) (*plugin.Response, error) {
	name := form.Get("PolicyName")
	asgName := form.Get("AutoScalingGroupName")
	if name == "" || asgName == "" {
		return asError("MissingParameter", "PolicyName and AutoScalingGroupName are required", http.StatusBadRequest), nil
	}
	policyType := form.Get("PolicyType")
	if policyType == "" {
		policyType = "SimpleScaling"
	}
	adjustmentType := form.Get("AdjustmentType")
	scalingAdj := parseInt(form.Get("ScalingAdjustment"), 0)
	arn := shared.BuildARN("autoscaling", "scalingPolicy", asgName+"/"+name)
	pol, err := p.store.PutPolicy(arn, name, asgName, policyType, adjustmentType, scalingAdj, "{}")
	if err != nil {
		return nil, err
	}

	type putPolicyResponse struct {
		XMLName   xml.Name `xml:"PutScalingPolicyResponse"`
		PolicyARN string   `xml:"PutScalingPolicyResult>PolicyARN"`
	}
	return asXMLResponse(http.StatusOK, putPolicyResponse{PolicyARN: pol.ARN})
}

func (p *Provider) handleDescribePolicies(form url.Values) (*plugin.Response, error) {
	asgName := form.Get("AutoScalingGroupName")
	policyName := form.Get("PolicyName")
	policies, err := p.store.ListPolicies(asgName, policyName)
	if err != nil {
		return nil, err
	}
	items := make([]policyXML, 0, len(policies))
	for i := range policies {
		items = append(items, policyToXML(&policies[i]))
	}

	type describePoliciesResponse struct {
		XMLName  xml.Name    `xml:"DescribePoliciesResponse"`
		Policies []policyXML `xml:"DescribePoliciesResult>ScalingPolicies>member"`
	}
	return asXMLResponse(http.StatusOK, describePoliciesResponse{Policies: items})
}

func (p *Provider) handleDeletePolicy(form url.Values) (*plugin.Response, error) {
	asgName := form.Get("AutoScalingGroupName")
	policyName := form.Get("PolicyName")
	if policyName == "" {
		return asError("MissingParameter", "PolicyName is required", http.StatusBadRequest), nil
	}
	if err := p.store.DeletePolicy(asgName, policyName); err != nil {
		if errors.Is(err, errPolicyNotFound) {
			return asError("ValidationError", "scaling policy not found: "+policyName, http.StatusBadRequest), nil
		}
		return nil, err
	}

	type deletePolicyResponse struct {
		XMLName xml.Name `xml:"DeletePolicyResponse"`
		Meta    struct {
			RequestId string `xml:"RequestId"`
		} `xml:"ResponseMetadata"`
	}
	var resp deletePolicyResponse
	resp.Meta.RequestId = shared.GenerateUUID()
	return asXMLResponse(http.StatusOK, resp)
}

func (p *Provider) handleExecutePolicy(form url.Values) (*plugin.Response, error) {
	type executePolicyResponse struct {
		XMLName xml.Name `xml:"ExecutePolicyResponse"`
		Meta    struct {
			RequestId string `xml:"RequestId"`
		} `xml:"ResponseMetadata"`
	}
	var resp executePolicyResponse
	resp.Meta.RequestId = shared.GenerateUUID()
	return asXMLResponse(http.StatusOK, resp)
}

// --- ScheduledAction handlers ---

func (p *Provider) handlePutScheduledUpdateGroupAction(form url.Values) (*plugin.Response, error) {
	name := form.Get("ScheduledActionName")
	asgName := form.Get("AutoScalingGroupName")
	if name == "" || asgName == "" {
		return asError("MissingParameter", "ScheduledActionName and AutoScalingGroupName are required", http.StatusBadRequest), nil
	}
	minSize := parseInt(form.Get("MinSize"), -1)
	maxSize := parseInt(form.Get("MaxSize"), -1)
	desired := parseInt(form.Get("DesiredCapacity"), -1)
	schedule := form.Get("Recurrence")
	arn := shared.BuildARN("autoscaling", "scheduledUpdateGroupAction", asgName+"/"+name)
	_, err := p.store.PutScheduledAction(arn, name, asgName, minSize, maxSize, desired, schedule, 0, 0)
	if err != nil {
		return nil, err
	}

	type putScheduledResponse struct {
		XMLName xml.Name `xml:"PutScheduledUpdateGroupActionResponse"`
		Meta    struct {
			RequestId string `xml:"RequestId"`
		} `xml:"ResponseMetadata"`
	}
	var resp putScheduledResponse
	resp.Meta.RequestId = shared.GenerateUUID()
	return asXMLResponse(http.StatusOK, resp)
}

func (p *Provider) handleDescribeScheduledActions(form url.Values) (*plugin.Response, error) {
	asgName := form.Get("AutoScalingGroupName")
	actionName := form.Get("ScheduledActionName")
	actions, err := p.store.ListScheduledActions(asgName, actionName)
	if err != nil {
		return nil, err
	}
	items := make([]scheduledActionXML, 0, len(actions))
	for i := range actions {
		items = append(items, scheduledActionToXML(&actions[i]))
	}

	type describeScheduledResponse struct {
		XMLName xml.Name             `xml:"DescribeScheduledActionsResponse"`
		Actions []scheduledActionXML `xml:"DescribeScheduledActionsResult>ScheduledUpdateGroupActions>member"`
	}
	return asXMLResponse(http.StatusOK, describeScheduledResponse{Actions: items})
}

func (p *Provider) handleDeleteScheduledAction(form url.Values) (*plugin.Response, error) {
	asgName := form.Get("AutoScalingGroupName")
	name := form.Get("ScheduledActionName")
	if name == "" {
		return asError("MissingParameter", "ScheduledActionName is required", http.StatusBadRequest), nil
	}
	if err := p.store.DeleteScheduledAction(asgName, name); err != nil {
		if errors.Is(err, errScheduledNotFound) {
			return asError("ValidationError", "scheduled action not found: "+name, http.StatusBadRequest), nil
		}
		return nil, err
	}

	type deleteScheduledResponse struct {
		XMLName xml.Name `xml:"DeleteScheduledActionResponse"`
		Meta    struct {
			RequestId string `xml:"RequestId"`
		} `xml:"ResponseMetadata"`
	}
	var resp deleteScheduledResponse
	resp.Meta.RequestId = shared.GenerateUUID()
	return asXMLResponse(http.StatusOK, resp)
}

func (p *Provider) handleBatchPutScheduledUpdateGroupAction(form url.Values) (*plugin.Response, error) {
	asgName := form.Get("AutoScalingGroupName")
	for i := 1; ; i++ {
		prefix := fmt.Sprintf("ScheduledUpdateGroupActions.member.%d.", i)
		name := form.Get(prefix + "ScheduledActionName")
		if name == "" {
			break
		}
		minSize := parseInt(form.Get(prefix+"MinSize"), -1)
		maxSize := parseInt(form.Get(prefix+"MaxSize"), -1)
		desired := parseInt(form.Get(prefix+"DesiredCapacity"), -1)
		schedule := form.Get(prefix + "Recurrence")
		arn := shared.BuildARN("autoscaling", "scheduledUpdateGroupAction", asgName+"/"+name)
		if _, err := p.store.PutScheduledAction(arn, name, asgName, minSize, maxSize, desired, schedule, 0, 0); err != nil {
			return nil, err
		}
	}

	type batchPutResponse struct {
		XMLName xml.Name `xml:"BatchPutScheduledUpdateGroupActionResponse"`
		Result  struct {
			FailedItems []string `xml:"FailedScheduledUpdateGroupActions>member"`
		} `xml:"BatchPutScheduledUpdateGroupActionResult"`
	}
	return asXMLResponse(http.StatusOK, batchPutResponse{})
}

func (p *Provider) handleBatchDeleteScheduledAction(form url.Values) (*plugin.Response, error) {
	asgName := form.Get("AutoScalingGroupName")
	for i := 1; ; i++ {
		key := fmt.Sprintf("ScheduledActionNames.member.%d", i)
		name := form.Get(key)
		if name == "" {
			break
		}
		// ignore not found errors for batch
		_ = p.store.DeleteScheduledAction(asgName, name)
	}

	type batchDeleteResponse struct {
		XMLName xml.Name `xml:"BatchDeleteScheduledActionResponse"`
		Result  struct {
			FailedItems []string `xml:"FailedScheduledActions>member"`
		} `xml:"BatchDeleteScheduledActionResult"`
	}
	return asXMLResponse(http.StatusOK, batchDeleteResponse{})
}

// --- LifecycleHook handlers ---

func (p *Provider) handlePutLifecycleHook(form url.Values) (*plugin.Response, error) {
	name := form.Get("LifecycleHookName")
	asgName := form.Get("AutoScalingGroupName")
	if name == "" || asgName == "" {
		return asError("MissingParameter", "LifecycleHookName and AutoScalingGroupName are required", http.StatusBadRequest), nil
	}
	transition := form.Get("LifecycleTransition")
	if transition == "" {
		transition = "autoscaling:EC2_INSTANCE_LAUNCHING"
	}
	heartbeatTimeout := parseInt(form.Get("HeartbeatTimeout"), 3600)
	defaultResult := form.Get("DefaultResult")
	if defaultResult == "" {
		defaultResult = "ABANDON"
	}
	roleARN := form.Get("RoleARN")
	_, err := p.store.PutLifecycleHook(name, asgName, transition, heartbeatTimeout, defaultResult, roleARN)
	if err != nil {
		return nil, err
	}

	type putHookResponse struct {
		XMLName xml.Name `xml:"PutLifecycleHookResponse"`
		Meta    struct {
			RequestId string `xml:"RequestId"`
		} `xml:"ResponseMetadata"`
	}
	var resp putHookResponse
	resp.Meta.RequestId = shared.GenerateUUID()
	return asXMLResponse(http.StatusOK, resp)
}

func (p *Provider) handleDescribeLifecycleHooks(form url.Values) (*plugin.Response, error) {
	asgName := form.Get("AutoScalingGroupName")
	hookName := form.Get("LifecycleHookName")
	hooks, err := p.store.ListLifecycleHooks(asgName, hookName)
	if err != nil {
		return nil, err
	}
	items := make([]lifecycleHookXML, 0, len(hooks))
	for i := range hooks {
		items = append(items, hookToXML(&hooks[i]))
	}

	type describeHooksResponse struct {
		XMLName xml.Name           `xml:"DescribeLifecycleHooksResponse"`
		Hooks   []lifecycleHookXML `xml:"DescribeLifecycleHooksResult>LifecycleHooks>member"`
	}
	return asXMLResponse(http.StatusOK, describeHooksResponse{Hooks: items})
}

func (p *Provider) handleDeleteLifecycleHook(form url.Values) (*plugin.Response, error) {
	name := form.Get("LifecycleHookName")
	asgName := form.Get("AutoScalingGroupName")
	if name == "" || asgName == "" {
		return asError("MissingParameter", "LifecycleHookName and AutoScalingGroupName are required", http.StatusBadRequest), nil
	}
	if err := p.store.DeleteLifecycleHook(asgName, name); err != nil {
		if errors.Is(err, errHookNotFound) {
			return asError("ValidationError", "lifecycle hook not found: "+name, http.StatusBadRequest), nil
		}
		return nil, err
	}

	type deleteHookResponse struct {
		XMLName xml.Name `xml:"DeleteLifecycleHookResponse"`
		Meta    struct {
			RequestId string `xml:"RequestId"`
		} `xml:"ResponseMetadata"`
	}
	var resp deleteHookResponse
	resp.Meta.RequestId = shared.GenerateUUID()
	return asXMLResponse(http.StatusOK, resp)
}

func (p *Provider) handleCompleteLifecycleAction(form url.Values) (*plugin.Response, error) {
	type completeHookResponse struct {
		XMLName xml.Name `xml:"CompleteLifecycleActionResponse"`
		Meta    struct {
			RequestId string `xml:"RequestId"`
		} `xml:"ResponseMetadata"`
	}
	var resp completeHookResponse
	resp.Meta.RequestId = shared.GenerateUUID()
	return asXMLResponse(http.StatusOK, resp)
}

func (p *Provider) handleRecordLifecycleActionHeartbeat(form url.Values) (*plugin.Response, error) {
	type recordHeartbeatResponse struct {
		XMLName xml.Name `xml:"RecordLifecycleActionHeartbeatResponse"`
		Meta    struct {
			RequestId string `xml:"RequestId"`
		} `xml:"ResponseMetadata"`
	}
	var resp recordHeartbeatResponse
	resp.Meta.RequestId = shared.GenerateUUID()
	return asXMLResponse(http.StatusOK, resp)
}

// --- Instance operation stubs ---

func (p *Provider) handleAttachInstances(form url.Values) (*plugin.Response, error) {
	type r struct {
		XMLName xml.Name `xml:"AttachInstancesResponse"`
		Meta    struct {
			RequestId string `xml:"RequestId"`
		} `xml:"ResponseMetadata"`
	}
	var resp r
	resp.Meta.RequestId = shared.GenerateUUID()
	return asXMLResponse(http.StatusOK, resp)
}

func (p *Provider) handleDetachInstances(form url.Values) (*plugin.Response, error) {
	type r struct {
		XMLName xml.Name `xml:"DetachInstancesResponse"`
		Result  struct {
			Activities []string `xml:"Activities>member"`
		} `xml:"DetachInstancesResult"`
	}
	return asXMLResponse(http.StatusOK, r{})
}

func (p *Provider) handleSetInstanceHealth(form url.Values) (*plugin.Response, error) {
	type r struct {
		XMLName xml.Name `xml:"SetInstanceHealthResponse"`
		Meta    struct {
			RequestId string `xml:"RequestId"`
		} `xml:"ResponseMetadata"`
	}
	var resp r
	resp.Meta.RequestId = shared.GenerateUUID()
	return asXMLResponse(http.StatusOK, resp)
}

func (p *Provider) handleSetInstanceProtection(form url.Values) (*plugin.Response, error) {
	type r struct {
		XMLName xml.Name `xml:"SetInstanceProtectionResponse"`
		Meta    struct {
			RequestId string `xml:"RequestId"`
		} `xml:"ResponseMetadata"`
	}
	var resp r
	resp.Meta.RequestId = shared.GenerateUUID()
	return asXMLResponse(http.StatusOK, resp)
}

func (p *Provider) handleEnterStandby(form url.Values) (*plugin.Response, error) {
	type r struct {
		XMLName xml.Name `xml:"EnterStandbyResponse"`
		Result  struct {
			Activities []string `xml:"Activities>member"`
		} `xml:"EnterStandbyResult"`
	}
	return asXMLResponse(http.StatusOK, r{})
}

func (p *Provider) handleExitStandby(form url.Values) (*plugin.Response, error) {
	type r struct {
		XMLName xml.Name `xml:"ExitStandbyResponse"`
		Result  struct {
			Activities []string `xml:"Activities>member"`
		} `xml:"ExitStandbyResult"`
	}
	return asXMLResponse(http.StatusOK, r{})
}

// --- Load balancer stubs ---

func (p *Provider) handleAttachLoadBalancers(form url.Values) (*plugin.Response, error) {
	type r struct {
		XMLName xml.Name `xml:"AttachLoadBalancersResponse"`
		Meta    struct {
			RequestId string `xml:"RequestId"`
		} `xml:"ResponseMetadata"`
	}
	var resp r
	resp.Meta.RequestId = shared.GenerateUUID()
	return asXMLResponse(http.StatusOK, resp)
}

func (p *Provider) handleDetachLoadBalancers(form url.Values) (*plugin.Response, error) {
	type r struct {
		XMLName xml.Name `xml:"DetachLoadBalancersResponse"`
		Meta    struct {
			RequestId string `xml:"RequestId"`
		} `xml:"ResponseMetadata"`
	}
	var resp r
	resp.Meta.RequestId = shared.GenerateUUID()
	return asXMLResponse(http.StatusOK, resp)
}

func (p *Provider) handleAttachLoadBalancerTargetGroups(form url.Values) (*plugin.Response, error) {
	type r struct {
		XMLName xml.Name `xml:"AttachLoadBalancerTargetGroupsResponse"`
		Meta    struct {
			RequestId string `xml:"RequestId"`
		} `xml:"ResponseMetadata"`
	}
	var resp r
	resp.Meta.RequestId = shared.GenerateUUID()
	return asXMLResponse(http.StatusOK, resp)
}

func (p *Provider) handleDetachLoadBalancerTargetGroups(form url.Values) (*plugin.Response, error) {
	type r struct {
		XMLName xml.Name `xml:"DetachLoadBalancerTargetGroupsResponse"`
		Meta    struct {
			RequestId string `xml:"RequestId"`
		} `xml:"ResponseMetadata"`
	}
	var resp r
	resp.Meta.RequestId = shared.GenerateUUID()
	return asXMLResponse(http.StatusOK, resp)
}

func (p *Provider) handleAttachTrafficSources(form url.Values) (*plugin.Response, error) {
	type r struct {
		XMLName xml.Name `xml:"AttachTrafficSourcesResponse"`
		Meta    struct {
			RequestId string `xml:"RequestId"`
		} `xml:"ResponseMetadata"`
	}
	var resp r
	resp.Meta.RequestId = shared.GenerateUUID()
	return asXMLResponse(http.StatusOK, resp)
}

func (p *Provider) handleDetachTrafficSources(form url.Values) (*plugin.Response, error) {
	type r struct {
		XMLName xml.Name `xml:"DetachTrafficSourcesResponse"`
		Meta    struct {
			RequestId string `xml:"RequestId"`
		} `xml:"ResponseMetadata"`
	}
	var resp r
	resp.Meta.RequestId = shared.GenerateUUID()
	return asXMLResponse(http.StatusOK, resp)
}

// --- Tag handlers ---

func (p *Provider) handleCreateOrUpdateTags(form url.Values) (*plugin.Response, error) {
	for i := 1; ; i++ {
		prefix := fmt.Sprintf("Tags.member.%d.", i)
		resID := form.Get(prefix + "ResourceId")
		key := form.Get(prefix + "Key")
		if key == "" {
			break
		}
		value := form.Get(prefix + "Value")
		if resID == "" {
			resID = form.Get("AutoScalingGroupName")
		}
		arn := shared.BuildARN("autoscaling", "autoScalingGroup", resID)
		if err := p.store.AddTags(arn, map[string]string{key: value}); err != nil {
			return nil, err
		}
	}

	type r struct {
		XMLName xml.Name `xml:"CreateOrUpdateTagsResponse"`
		Meta    struct {
			RequestId string `xml:"RequestId"`
		} `xml:"ResponseMetadata"`
	}
	var resp r
	resp.Meta.RequestId = shared.GenerateUUID()
	return asXMLResponse(http.StatusOK, resp)
}

func (p *Provider) handleDeleteTags(form url.Values) (*plugin.Response, error) {
	for i := 1; ; i++ {
		prefix := fmt.Sprintf("Tags.member.%d.", i)
		resID := form.Get(prefix + "ResourceId")
		key := form.Get(prefix + "Key")
		if key == "" {
			break
		}
		if resID == "" {
			resID = form.Get("AutoScalingGroupName")
		}
		arn := shared.BuildARN("autoscaling", "autoScalingGroup", resID)
		if err := p.store.RemoveTags(arn, []string{key}); err != nil {
			return nil, err
		}
	}

	type r struct {
		XMLName xml.Name `xml:"DeleteTagsResponse"`
		Meta    struct {
			RequestId string `xml:"RequestId"`
		} `xml:"ResponseMetadata"`
	}
	var resp r
	resp.Meta.RequestId = shared.GenerateUUID()
	return asXMLResponse(http.StatusOK, resp)
}

type tagXML struct {
	ResourceId   string `xml:"ResourceId"`
	ResourceType string `xml:"ResourceType"`
	Key          string `xml:"Key"`
	Value        string `xml:"Value"`
}

func (p *Provider) handleDescribeTags(form url.Values) (*plugin.Response, error) {
	allTags, err := p.store.ListAllTags()
	if err != nil {
		return nil, err
	}
	var items []tagXML
	for resARN, tags := range allTags {
		for k, v := range tags {
			items = append(items, tagXML{
				ResourceId:   resARN,
				ResourceType: "auto-scaling-group",
				Key:          k,
				Value:        v,
			})
		}
	}

	type r struct {
		XMLName xml.Name `xml:"DescribeTagsResponse"`
		Tags    []tagXML `xml:"DescribeTagsResult>Tags>member"`
	}
	return asXMLResponse(http.StatusOK, r{Tags: items})
}

// --- Misc handlers ---

func (p *Provider) handleDescribeAccountLimits(_ url.Values) (*plugin.Response, error) {
	type r struct {
		XMLName        xml.Name `xml:"DescribeAccountLimitsResponse"`
		MaxGroups      int      `xml:"DescribeAccountLimitsResult>MaxNumberOfAutoScalingGroups"`
		MaxLCs         int      `xml:"DescribeAccountLimitsResult>MaxNumberOfLaunchConfigurations"`
		NumberOfGroups int      `xml:"DescribeAccountLimitsResult>NumberOfAutoScalingGroups"`
		NumberOfLCs    int      `xml:"DescribeAccountLimitsResult>NumberOfLaunchConfigurations"`
	}
	asgs, _ := p.store.ListASGs(nil)
	lcs, _ := p.store.ListLCs(nil)
	return asXMLResponse(http.StatusOK, r{
		MaxGroups:      200,
		MaxLCs:         200,
		NumberOfGroups: len(asgs),
		NumberOfLCs:    len(lcs),
	})
}

func (p *Provider) handleDescribeAutoScalingInstances(_ url.Values) (*plugin.Response, error) {
	type r struct {
		XMLName   xml.Name `xml:"DescribeAutoScalingInstancesResponse"`
		Instances []string `xml:"DescribeAutoScalingInstancesResult>AutoScalingInstances>member"`
	}
	return asXMLResponse(http.StatusOK, r{})
}

func (p *Provider) handleStartInstanceRefresh(form url.Values) (*plugin.Response, error) {
	type r struct {
		XMLName           xml.Name `xml:"StartInstanceRefreshResponse"`
		InstanceRefreshId string   `xml:"StartInstanceRefreshResult>InstanceRefreshId"`
	}
	return asXMLResponse(http.StatusOK, r{InstanceRefreshId: shared.GenerateUUID()})
}

func (p *Provider) handleDescribeInstanceRefreshes(_ url.Values) (*plugin.Response, error) {
	type r struct {
		XMLName   xml.Name `xml:"DescribeInstanceRefreshesResponse"`
		Refreshes []string `xml:"DescribeInstanceRefreshesResult>InstanceRefreshes>member"`
	}
	return asXMLResponse(http.StatusOK, r{})
}

func (p *Provider) handleCancelInstanceRefresh(form url.Values) (*plugin.Response, error) {
	type r struct {
		XMLName           xml.Name `xml:"CancelInstanceRefreshResponse"`
		InstanceRefreshId string   `xml:"CancelInstanceRefreshResult>InstanceRefreshId"`
	}
	return asXMLResponse(http.StatusOK, r{InstanceRefreshId: form.Get("InstanceRefreshId")})
}

func (p *Provider) handleRollbackInstanceRefresh(form url.Values) (*plugin.Response, error) {
	type r struct {
		XMLName           xml.Name `xml:"RollbackInstanceRefreshResponse"`
		InstanceRefreshId string   `xml:"RollbackInstanceRefreshResult>InstanceRefreshId"`
	}
	return asXMLResponse(http.StatusOK, r{InstanceRefreshId: form.Get("InstanceRefreshId")})
}

func (p *Provider) handleSuspendProcesses(_ url.Values) (*plugin.Response, error) {
	type r struct {
		XMLName xml.Name `xml:"SuspendProcessesResponse"`
		Meta    struct {
			RequestId string `xml:"RequestId"`
		} `xml:"ResponseMetadata"`
	}
	var resp r
	resp.Meta.RequestId = shared.GenerateUUID()
	return asXMLResponse(http.StatusOK, resp)
}

func (p *Provider) handleResumeProcesses(_ url.Values) (*plugin.Response, error) {
	type r struct {
		XMLName xml.Name `xml:"ResumeProcessesResponse"`
		Meta    struct {
			RequestId string `xml:"RequestId"`
		} `xml:"ResponseMetadata"`
	}
	var resp r
	resp.Meta.RequestId = shared.GenerateUUID()
	return asXMLResponse(http.StatusOK, resp)
}

func (p *Provider) handleEnableMetricsCollection(_ url.Values) (*plugin.Response, error) {
	type r struct {
		XMLName xml.Name `xml:"EnableMetricsCollectionResponse"`
		Meta    struct {
			RequestId string `xml:"RequestId"`
		} `xml:"ResponseMetadata"`
	}
	var resp r
	resp.Meta.RequestId = shared.GenerateUUID()
	return asXMLResponse(http.StatusOK, resp)
}

func (p *Provider) handleDisableMetricsCollection(_ url.Values) (*plugin.Response, error) {
	type r struct {
		XMLName xml.Name `xml:"DisableMetricsCollectionResponse"`
		Meta    struct {
			RequestId string `xml:"RequestId"`
		} `xml:"ResponseMetadata"`
	}
	var resp r
	resp.Meta.RequestId = shared.GenerateUUID()
	return asXMLResponse(http.StatusOK, resp)
}

func (p *Provider) handleTerminateInstanceInAutoScalingGroup(form url.Values) (*plugin.Response, error) {
	type activityXML struct {
		ActivityId           string `xml:"ActivityId"`
		AutoScalingGroupName string `xml:"AutoScalingGroupName"`
		StatusCode           string `xml:"StatusCode"`
		Description          string `xml:"Description"`
	}
	type r struct {
		XMLName  xml.Name    `xml:"TerminateInstanceInAutoScalingGroupResponse"`
		Activity activityXML `xml:"TerminateInstanceInAutoScalingGroupResult>Activity"`
	}
	return asXMLResponse(http.StatusOK, r{
		Activity: activityXML{
			ActivityId:           shared.GenerateUUID(),
			AutoScalingGroupName: form.Get("AutoScalingGroupName"),
			StatusCode:           "InProgress",
			Description:          "Terminating instance",
		},
	})
}

// --- utility ---

func parseInt(s string, def int) int {
	if s == "" {
		return def
	}
	v, err := strconv.Atoi(s)
	if err != nil {
		return def
	}
	return v
}

func collectMembers(form url.Values, prefix string) []string {
	var out []string
	for i := 1; ; i++ {
		v := form.Get(fmt.Sprintf("%s.%d", prefix, i))
		if v == "" {
			break
		}
		out = append(out, v)
	}
	return out
}

func toJSONArray(items []string) string {
	if len(items) == 0 {
		return "[]"
	}
	var sb strings.Builder
	sb.WriteString("[")
	for i, item := range items {
		if i > 0 {
			sb.WriteString(",")
		}
		sb.WriteString(`"`)
		sb.WriteString(strings.ReplaceAll(item, `"`, `\"`))
		sb.WriteString(`"`)
	}
	sb.WriteString("]")
	return sb.String()
}
