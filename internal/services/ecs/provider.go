// SPDX-License-Identifier: Apache-2.0

// internal/services/ecs/provider.go
package ecs

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"

	"github.com/skyoo2003/devcloud/internal/plugin"
)

const defaultAccountID = plugin.DefaultAccountID

// Provider implements the ECS service (JSON 1.1 protocol).
type Provider struct {
	store *ECSStore
}

func (p *Provider) ServiceID() string             { return "ecs" }
func (p *Provider) ServiceName() string           { return "Amazon ECS" }
func (p *Provider) Protocol() plugin.ProtocolType { return plugin.ProtocolJSON11 }

func (p *Provider) Init(cfg plugin.PluginConfig) error {
	if err := os.MkdirAll(cfg.DataDir, 0o755); err != nil {
		return fmt.Errorf("init ecs: %w", err)
	}
	var err error
	p.store, err = NewECSStore(cfg.DataDir)
	return err
}

func (p *Provider) Shutdown(_ context.Context) error {
	if p.store != nil {
		return p.store.Close()
	}
	return nil
}

func (p *Provider) HandleRequest(ctx context.Context, op string, req *http.Request) (*plugin.Response, error) {
	rawBody, err := io.ReadAll(req.Body)
	if err != nil {
		return ecsError("InvalidRequest", "failed to read body", http.StatusBadRequest), nil
	}
	var params map[string]any
	if len(rawBody) > 0 {
		if err := json.Unmarshal(rawBody, &params); err != nil {
			return ecsError("InvalidRequest", "failed to parse JSON", http.StatusBadRequest), nil
		}
	} else {
		params = make(map[string]any)
	}

	action := op
	if action == "" {
		target := req.Header.Get("X-Amz-Target")
		if idx := strings.LastIndex(target, "."); idx >= 0 {
			action = target[idx+1:]
		} else {
			action = target
		}
	}

	switch action {
	case "CreateCluster":
		return p.handleCreateCluster(params)
	case "ListClusters":
		return p.handleListClusters(params)
	case "DescribeClusters":
		return p.handleDescribeClusters(params)
	case "DeleteCluster":
		return p.handleDeleteCluster(params)
	case "UpdateCluster":
		return p.handleUpdateCluster(params)
	case "UpdateClusterSettings":
		return p.handleUpdateClusterSettings(params)
	case "RegisterTaskDefinition":
		return p.handleRegisterTaskDefinition(params)
	case "DescribeTaskDefinition":
		return p.handleDescribeTaskDefinition(params)
	case "ListTaskDefinitions":
		return p.handleListTaskDefinitions(params)
	case "ListTaskDefinitionFamilies":
		return p.handleListTaskDefinitionFamilies(params)
	case "DeregisterTaskDefinition":
		return p.handleDeregisterTaskDefinition(params)
	case "RunTask":
		return p.handleRunTask(params)
	case "StartTask":
		return p.handleStartTask(params)
	case "StopTask":
		return p.handleStopTask(params)
	case "DescribeTasks":
		return p.handleDescribeTasks(params)
	case "ListTasks":
		return p.handleListTasks(params)
	case "CreateService":
		return p.handleCreateService(params)
	case "UpdateService":
		return p.handleUpdateService(params)
	case "DeleteService":
		return p.handleDeleteService(params)
	case "ListServices":
		return p.handleListServices(params)
	case "DescribeServices":
		return p.handleDescribeServices(params)
	// Capacity Providers
	case "CreateCapacityProvider":
		return p.handleCreateCapacityProvider(params)
	case "DescribeCapacityProviders":
		return p.handleDescribeCapacityProviders(params)
	case "DeleteCapacityProvider":
		return p.handleDeleteCapacityProvider(params)
	case "PutClusterCapacityProviders":
		return p.handlePutClusterCapacityProviders(params)
	case "UpdateCapacityProvider":
		return p.handleUpdateCapacityProvider(params)
	// Container Instances
	case "ListContainerInstances":
		return p.handleListContainerInstances(params)
	case "DescribeContainerInstances":
		return p.handleDescribeContainerInstances(params)
	case "DeregisterContainerInstance":
		return p.handleDeregisterContainerInstance(params)
	case "RegisterContainerInstance":
		return p.handleRegisterContainerInstance(params)
	// Attributes
	case "PutAttributes":
		return p.handlePutAttributes(params)
	case "DeleteAttributes":
		return p.handleDeleteAttributes(params)
	case "ListAttributes":
		return p.handleListAttributes(params)
	// Task Sets
	case "CreateTaskSet":
		return p.handleCreateTaskSet(params)
	case "DeleteTaskSet":
		return p.handleDeleteTaskSet(params)
	case "UpdateTaskSet":
		return p.handleUpdateTaskSet(params)
	case "DescribeTaskSets":
		return p.handleDescribeTaskSets(params)
	case "UpdateServicePrimaryTaskSet":
		return p.handleUpdateServicePrimaryTaskSet(params)
	// Service Auto-Scaling
	case "RegisterScalableTarget":
		return p.handleRegisterScalableTarget(params)
	case "DeregisterScalableTarget":
		return p.handleDeregisterScalableTarget(params)
	case "DescribeScalableTargets":
		return p.handleDescribeScalableTargets(params)
	case "PutScalingPolicy":
		return p.handlePutScalingPolicy(params)
	case "DeleteScalingPolicy":
		return p.handleDeleteScalingPolicy(params)
	case "DescribeScalingPolicies":
		return p.handleDescribeScalingPolicies(params)
	// Tags
	case "TagResource":
		return p.handleTagResource(params)
	case "UntagResource":
		return p.handleUntagResource(params)
	case "ListTagsForResource":
		return p.handleListTagsForResource(params)
	// Account Settings
	case "ListAccountSettings":
		return p.handleListAccountSettings(params)
	case "PutAccountSetting":
		return p.handlePutAccountSetting(params)
	case "PutAccountSettingDefault":
		return p.handlePutAccountSettingDefault(params)
	case "DeleteAccountSetting":
		return p.handleDeleteAccountSetting(params)
	// Other
	case "UpdateContainerInstancesState":
		return p.handleUpdateContainerInstancesState(params)
	case "UpdateTaskProtection":
		return p.handleUpdateTaskProtection(params)
	case "GetTaskProtection":
		return p.handleGetTaskProtection(params)
	case "SubmitTaskStateChange":
		return p.handleSubmitTaskStateChange(params)
	case "SubmitContainerStateChange":
		return p.handleSubmitContainerStateChange(params)
	case "DiscoverPollEndpoint":
		return p.handleDiscoverPollEndpoint(params)
	default:
		return ecsError("InvalidAction", fmt.Sprintf("unknown action: %s", action), http.StatusBadRequest), nil
	}
}

func (p *Provider) ListResources(ctx context.Context) ([]plugin.Resource, error) {
	arns, err := p.store.ListClusters(defaultAccountID)
	if err != nil {
		return nil, err
	}
	out := make([]plugin.Resource, 0, len(arns))
	for _, arn := range arns {
		parts := strings.Split(arn, "/")
		name := parts[len(parts)-1]
		out = append(out, plugin.Resource{Type: "cluster", ID: arn, Name: name})
	}
	return out, nil
}

func (p *Provider) GetMetrics(_ context.Context) (*plugin.ServiceMetrics, error) {
	return &plugin.ServiceMetrics{}, nil
}

// --- operation handlers ---

func (p *Provider) handleCreateCluster(params map[string]any) (*plugin.Response, error) {
	name := strParam(params, "clusterName")
	if name == "" {
		name = "default"
	}
	cluster, err := p.store.CreateCluster(defaultAccountID, name)
	if err != nil {
		return nil, err
	}
	return ecsJSON(http.StatusOK, map[string]any{
		"cluster": clusterToMap(cluster),
	})
}

func (p *Provider) handleListClusters(_ map[string]any) (*plugin.Response, error) {
	arns, err := p.store.ListClusters(defaultAccountID)
	if err != nil {
		return nil, err
	}
	if arns == nil {
		arns = []string{}
	}
	return ecsJSON(http.StatusOK, map[string]any{"clusterArns": arns})
}

func (p *Provider) handleDescribeClusters(params map[string]any) (*plugin.Response, error) {
	requested := strSliceParam(params, "clusters")
	// Resolve cluster names to ARNs for lookup.
	lookupArns := make([]string, 0, len(requested))
	for _, r := range requested {
		if strings.Contains(r, ":") {
			lookupArns = append(lookupArns, r)
		} else {
			lookupArns = append(lookupArns, fmt.Sprintf("arn:aws:ecs:%s:%s:cluster/%s", region, defaultAccountID, r))
		}
	}
	clusters, err := p.store.DescribeClusters(defaultAccountID, lookupArns)
	if err != nil {
		return nil, err
	}
	items := make([]map[string]any, 0, len(clusters))
	foundArns := make(map[string]bool)
	for i := range clusters {
		items = append(items, clusterToMap(&clusters[i]))
		foundArns[clusters[i].ARN] = true
	}
	// Build failures for requested clusters not found.
	failures := make([]any, 0)
	for _, arn := range lookupArns {
		if !foundArns[arn] {
			failures = append(failures, map[string]any{
				"arn":    arn,
				"reason": "MISSING",
			})
		}
	}
	return ecsJSON(http.StatusOK, map[string]any{"clusters": items, "failures": failures})
}

func (p *Provider) handleDeleteCluster(params map[string]any) (*plugin.Response, error) {
	arn := strParam(params, "cluster")
	if arn == "" {
		return ecsError("MissingParameter", "cluster is required", http.StatusBadRequest), nil
	}
	if err := p.store.DeleteCluster(defaultAccountID, arn); err != nil {
		return ecsError("ClusterNotFoundException", "cluster not found", http.StatusBadRequest), nil
	}
	return ecsJSON(http.StatusOK, map[string]any{"cluster": map[string]any{"clusterArn": arn, "status": "INACTIVE"}})
}

func (p *Provider) handleRegisterTaskDefinition(params map[string]any) (*plugin.Response, error) {
	family := strParam(params, "family")
	if family == "" {
		return ecsError("MissingParameter", "family is required", http.StatusBadRequest), nil
	}
	containerDefsRaw := params["containerDefinitions"]
	containerDefs := "[]"
	if containerDefsRaw != nil {
		b, _ := json.Marshal(containerDefsRaw)
		containerDefs = string(b)
	}
	td, err := p.store.RegisterTaskDefinition(defaultAccountID, family, containerDefs)
	if err != nil {
		return nil, err
	}
	return ecsJSON(http.StatusOK, map[string]any{"taskDefinition": taskDefToMap(td)})
}

func (p *Provider) handleDescribeTaskDefinition(params map[string]any) (*plugin.Response, error) {
	arn := strParam(params, "taskDefinition")
	if arn == "" {
		return ecsError("MissingParameter", "taskDefinition is required", http.StatusBadRequest), nil
	}
	td, err := p.store.DescribeTaskDefinition(defaultAccountID, arn)
	if err != nil {
		return ecsError("ClientException", "task definition not found", http.StatusBadRequest), nil
	}
	return ecsJSON(http.StatusOK, map[string]any{"taskDefinition": taskDefToMap(td)})
}

func (p *Provider) handleListTaskDefinitions(_ map[string]any) (*plugin.Response, error) {
	arns, err := p.store.ListTaskDefinitions(defaultAccountID)
	if err != nil {
		return nil, err
	}
	if arns == nil {
		arns = []string{}
	}
	return ecsJSON(http.StatusOK, map[string]any{"taskDefinitionArns": arns})
}

func (p *Provider) handleDeregisterTaskDefinition(params map[string]any) (*plugin.Response, error) {
	arn := strParam(params, "taskDefinition")
	if arn == "" {
		return ecsError("MissingParameter", "taskDefinition is required", http.StatusBadRequest), nil
	}
	td, err := p.store.DeregisterTaskDefinition(defaultAccountID, arn)
	if err != nil {
		return ecsError("ClientException", "task definition not found", http.StatusBadRequest), nil
	}
	return ecsJSON(http.StatusOK, map[string]any{"taskDefinition": taskDefToMap(td)})
}

func (p *Provider) handleRunTask(params map[string]any) (*plugin.Response, error) {
	clusterArn := strParam(params, "cluster")
	taskDefArn := strParam(params, "taskDefinition")
	if clusterArn == "" || taskDefArn == "" {
		return ecsError("MissingParameter", "cluster and taskDefinition are required", http.StatusBadRequest), nil
	}
	task, err := p.store.RunTask(defaultAccountID, clusterArn, taskDefArn)
	if err != nil {
		return nil, err
	}
	return ecsJSON(http.StatusOK, map[string]any{"tasks": []any{taskToMap(task)}, "failures": []any{}})
}

func (p *Provider) handleStopTask(params map[string]any) (*plugin.Response, error) {
	taskArn := strParam(params, "task")
	if taskArn == "" {
		return ecsError("MissingParameter", "task is required", http.StatusBadRequest), nil
	}
	if err := p.store.StopTask(defaultAccountID, taskArn); err != nil {
		return ecsError("InvalidParameterException", "task not found", http.StatusBadRequest), nil
	}
	return ecsJSON(http.StatusOK, map[string]any{"task": map[string]any{"taskArn": taskArn, "lastStatus": "STOPPED"}})
}

func (p *Provider) handleDescribeTasks(params map[string]any) (*plugin.Response, error) {
	arns := strSliceParam(params, "tasks")
	tasks, err := p.store.DescribeTasks(defaultAccountID, arns)
	if err != nil {
		return nil, err
	}
	items := make([]any, 0, len(tasks))
	for i := range tasks {
		items = append(items, taskToMap(&tasks[i]))
	}
	return ecsJSON(http.StatusOK, map[string]any{"tasks": items, "failures": []any{}})
}

func (p *Provider) handleCreateService(params map[string]any) (*plugin.Response, error) {
	name := strParam(params, "serviceName")
	clusterArn := strParam(params, "cluster")
	taskDefArn := strParam(params, "taskDefinition")
	if name == "" || clusterArn == "" || taskDefArn == "" {
		return ecsError("MissingParameter", "serviceName, cluster, and taskDefinition are required", http.StatusBadRequest), nil
	}
	desiredCount := intParam(params, "desiredCount", 1)
	// Derive cluster name from ARN for service ARN construction.
	parts := strings.Split(clusterArn, "/")
	clusterName := parts[len(parts)-1]
	svc, err := p.store.CreateService(defaultAccountID, clusterArn, clusterName, name, taskDefArn, desiredCount)
	if err != nil {
		return ecsError("InvalidParameterException", err.Error(), http.StatusBadRequest), nil
	}
	return ecsJSON(http.StatusOK, map[string]any{"service": serviceToMap(svc)})
}

func (p *Provider) handleUpdateService(params map[string]any) (*plugin.Response, error) {
	serviceRef := strParam(params, "service")
	clusterArn := strParam(params, "cluster")
	taskDefArn := strParam(params, "taskDefinition")
	desiredCount := intParam(params, "desiredCount", -1)
	if serviceRef == "" {
		return ecsError("MissingParameter", "service is required", http.StatusBadRequest), nil
	}
	// If serviceRef is not an ARN, resolve it by name within the cluster.
	serviceArn := serviceRef
	if !strings.Contains(serviceRef, ":") {
		resolved, err := p.store.FindServiceARNByName(defaultAccountID, clusterArn, serviceRef)
		if err != nil {
			return ecsError("ServiceNotFoundException", "service not found", http.StatusBadRequest), nil
		}
		serviceArn = resolved
	}
	svc, err := p.store.UpdateService(defaultAccountID, serviceArn, desiredCount, taskDefArn)
	if err != nil {
		return ecsError("ServiceNotFoundException", "service not found", http.StatusBadRequest), nil
	}
	return ecsJSON(http.StatusOK, map[string]any{"service": serviceToMap(svc)})
}

func (p *Provider) handleDeleteService(params map[string]any) (*plugin.Response, error) {
	serviceArn := strParam(params, "service")
	if serviceArn == "" {
		return ecsError("MissingParameter", "service is required", http.StatusBadRequest), nil
	}
	if err := p.store.DeleteService(defaultAccountID, serviceArn); err != nil {
		return ecsError("ServiceNotFoundException", "service not found", http.StatusBadRequest), nil
	}
	return ecsJSON(http.StatusOK, map[string]any{"service": map[string]any{"serviceArn": serviceArn, "status": "INACTIVE"}})
}

func (p *Provider) handleListServices(params map[string]any) (*plugin.Response, error) {
	clusterArn := strParam(params, "cluster")
	if clusterArn == "" {
		return ecsError("MissingParameter", "cluster is required", http.StatusBadRequest), nil
	}
	arns, err := p.store.ListServices(defaultAccountID, clusterArn)
	if err != nil {
		return nil, err
	}
	if arns == nil {
		arns = []string{}
	}
	return ecsJSON(http.StatusOK, map[string]any{"serviceArns": arns})
}

// --- Capacity Provider handlers ---

func (p *Provider) handleCreateCapacityProvider(params map[string]any) (*plugin.Response, error) {
	name := strParam(params, "name")
	if name == "" {
		return ecsError("MissingParameter", "name is required", http.StatusBadRequest), nil
	}
	asgARN := ""
	if asg, ok := params["autoScalingGroupProvider"].(map[string]any); ok {
		asgARN, _ = asg["autoScalingGroupArn"].(string)
	}
	cp, err := p.store.CreateCapacityProvider(defaultAccountID, name, asgARN)
	if err != nil {
		return nil, err
	}
	return ecsJSON(http.StatusOK, map[string]any{"capacityProvider": capacityProviderToMap(cp)})
}

func (p *Provider) handleDescribeCapacityProviders(params map[string]any) (*plugin.Response, error) {
	names := strSliceParam(params, "capacityProviders")
	cps, err := p.store.DescribeCapacityProviders(defaultAccountID, names)
	if err != nil {
		return nil, err
	}
	items := make([]map[string]any, 0, len(cps))
	for i := range cps {
		items = append(items, capacityProviderToMap(&cps[i]))
	}
	return ecsJSON(http.StatusOK, map[string]any{"capacityProviders": items, "failures": []any{}})
}

func (p *Provider) handleDeleteCapacityProvider(params map[string]any) (*plugin.Response, error) {
	nameOrARN := strParam(params, "capacityProvider")
	if nameOrARN == "" {
		return ecsError("MissingParameter", "capacityProvider is required", http.StatusBadRequest), nil
	}
	if err := p.store.DeleteCapacityProvider(defaultAccountID, nameOrARN); err != nil {
		return ecsError("ResourceNotFoundException", "capacity provider not found", http.StatusBadRequest), nil
	}
	return ecsJSON(http.StatusOK, map[string]any{"capacityProvider": map[string]any{"name": nameOrARN, "status": "INACTIVE"}})
}

func (p *Provider) handlePutClusterCapacityProviders(params map[string]any) (*plugin.Response, error) {
	clusterRef := strParam(params, "cluster")
	if clusterRef == "" {
		return ecsError("MissingParameter", "cluster is required", http.StatusBadRequest), nil
	}
	// Resolve cluster ARN
	clusterARN := clusterRef
	if !strings.Contains(clusterRef, ":") {
		clusterARN = fmt.Sprintf("arn:aws:ecs:%s:%s:cluster/%s", region, defaultAccountID, clusterRef)
	}
	clusters, err := p.store.DescribeClusters(defaultAccountID, []string{clusterARN})
	if err != nil || len(clusters) == 0 {
		return ecsError("ClusterNotFoundException", "cluster not found", http.StatusBadRequest), nil
	}
	return ecsJSON(http.StatusOK, map[string]any{"cluster": clusterToMap(&clusters[0])})
}

// --- Container Instance handlers ---

func (p *Provider) handleListContainerInstances(params map[string]any) (*plugin.Response, error) {
	clusterRef := strParam(params, "cluster")
	if clusterRef == "" {
		return ecsError("MissingParameter", "cluster is required", http.StatusBadRequest), nil
	}
	clusterARN := clusterRef
	if !strings.Contains(clusterRef, ":") {
		clusterARN = fmt.Sprintf("arn:aws:ecs:%s:%s:cluster/%s", region, defaultAccountID, clusterRef)
	}
	status := strParam(params, "status")
	arns, err := p.store.ListContainerInstances(defaultAccountID, clusterARN, status)
	if err != nil {
		return nil, err
	}
	if arns == nil {
		arns = []string{}
	}
	return ecsJSON(http.StatusOK, map[string]any{"containerInstanceArns": arns})
}

func (p *Provider) handleDescribeContainerInstances(params map[string]any) (*plugin.Response, error) {
	arns := strSliceParam(params, "containerInstances")
	cis, err := p.store.DescribeContainerInstances(defaultAccountID, arns)
	if err != nil {
		return nil, err
	}
	items := make([]any, 0, len(cis))
	for i := range cis {
		items = append(items, containerInstanceToMap(&cis[i]))
	}
	return ecsJSON(http.StatusOK, map[string]any{"containerInstances": items, "failures": []any{}})
}

func (p *Provider) handleDeregisterContainerInstance(params map[string]any) (*plugin.Response, error) {
	arn := strParam(params, "containerInstance")
	if arn == "" {
		return ecsError("MissingParameter", "containerInstance is required", http.StatusBadRequest), nil
	}
	force := false
	if v, ok := params["force"].(bool); ok {
		force = v
	}
	if err := p.store.DeregisterContainerInstance(defaultAccountID, arn, force); err != nil {
		return ecsError("InvalidParameterException", "container instance not found", http.StatusBadRequest), nil
	}
	return ecsJSON(http.StatusOK, map[string]any{"containerInstance": map[string]any{"containerInstanceArn": arn, "status": "INACTIVE"}})
}

// --- Attribute handlers ---

func (p *Provider) handlePutAttributes(params map[string]any) (*plugin.Response, error) {
	clusterRef := strParam(params, "cluster")
	clusterARN := clusterRef
	if clusterRef != "" && !strings.Contains(clusterRef, ":") {
		clusterARN = fmt.Sprintf("arn:aws:ecs:%s:%s:cluster/%s", region, defaultAccountID, clusterRef)
	}
	rawAttrs, _ := params["attributes"].([]any)
	attrs := parseAttributes(rawAttrs)
	if err := p.store.PutAttributes(defaultAccountID, clusterARN, attrs); err != nil {
		return nil, err
	}
	items := make([]any, len(attrs))
	for i, a := range attrs {
		items[i] = attributeToMap(a)
	}
	return ecsJSON(http.StatusOK, map[string]any{"attributes": items})
}

func (p *Provider) handleDeleteAttributes(params map[string]any) (*plugin.Response, error) {
	clusterRef := strParam(params, "cluster")
	clusterARN := clusterRef
	if clusterRef != "" && !strings.Contains(clusterRef, ":") {
		clusterARN = fmt.Sprintf("arn:aws:ecs:%s:%s:cluster/%s", region, defaultAccountID, clusterRef)
	}
	rawAttrs, _ := params["attributes"].([]any)
	attrs := parseAttributes(rawAttrs)
	if err := p.store.DeleteAttributes(defaultAccountID, clusterARN, attrs); err != nil {
		return nil, err
	}
	return ecsJSON(http.StatusOK, map[string]any{"attributes": []any{}})
}

func (p *Provider) handleListAttributes(params map[string]any) (*plugin.Response, error) {
	clusterRef := strParam(params, "cluster")
	clusterARN := clusterRef
	if clusterRef != "" && !strings.Contains(clusterRef, ":") {
		clusterARN = fmt.Sprintf("arn:aws:ecs:%s:%s:cluster/%s", region, defaultAccountID, clusterRef)
	}
	targetType := strParam(params, "targetType")
	attrName := strParam(params, "attributeName")
	attrValue := strParam(params, "attributeValue")
	attrs, err := p.store.ListAttributes(defaultAccountID, clusterARN, targetType, attrName, attrValue)
	if err != nil {
		return nil, err
	}
	items := make([]any, 0, len(attrs))
	for _, a := range attrs {
		items = append(items, attributeToMap(a))
	}
	return ecsJSON(http.StatusOK, map[string]any{"attributes": items})
}

// --- Other handlers ---

func (p *Provider) handleUpdateContainerInstancesState(params map[string]any) (*plugin.Response, error) {
	arns := strSliceParam(params, "containerInstances")
	status := strParam(params, "status")
	if status == "" {
		return ecsError("MissingParameter", "status is required", http.StatusBadRequest), nil
	}
	if err := p.store.UpdateContainerInstancesState(defaultAccountID, arns, status); err != nil {
		return nil, err
	}
	items := make([]any, 0, len(arns))
	for _, arn := range arns {
		items = append(items, map[string]any{"containerInstanceArn": arn, "status": status})
	}
	return ecsJSON(http.StatusOK, map[string]any{"containerInstances": items, "failures": []any{}})
}

func (p *Provider) handleUpdateTaskProtection(params map[string]any) (*plugin.Response, error) {
	taskArns := strSliceParam(params, "tasks")
	protectionEnabled := false
	if v, ok := params["protectionEnabled"].(bool); ok {
		protectionEnabled = v
	}
	items := make([]any, 0, len(taskArns))
	for _, arn := range taskArns {
		items = append(items, map[string]any{
			"taskArn":           arn,
			"protectionEnabled": protectionEnabled,
		})
	}
	return ecsJSON(http.StatusOK, map[string]any{"protectedTasks": items, "failures": []any{}})
}

func (p *Provider) handleGetTaskProtection(params map[string]any) (*plugin.Response, error) {
	taskArns := strSliceParam(params, "tasks")
	items := make([]any, 0, len(taskArns))
	for _, arn := range taskArns {
		items = append(items, map[string]any{
			"taskArn":           arn,
			"protectionEnabled": false,
		})
	}
	return ecsJSON(http.StatusOK, map[string]any{"protectedTasks": items, "failures": []any{}})
}

func (p *Provider) handleSubmitTaskStateChange(_ map[string]any) (*plugin.Response, error) {
	return ecsJSON(http.StatusOK, map[string]any{"acknowledgment": "ACK"})
}

// --- helpers ---

func ecsError(code, message string, status int) *plugin.Response {
	body, _ := json.Marshal(map[string]any{"__type": code, "message": message})
	return &plugin.Response{StatusCode: status, ContentType: "application/x-amz-json-1.1", Body: body}
}

func ecsJSON(status int, v any) (*plugin.Response, error) {
	body, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	return &plugin.Response{StatusCode: status, ContentType: "application/x-amz-json-1.1", Body: body}, nil
}

func strParam(params map[string]any, key string) string {
	if v, ok := params[key]; ok {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}

func intParam(params map[string]any, key string, defaultVal int) int {
	if v, ok := params[key]; ok {
		switch n := v.(type) {
		case float64:
			return int(n)
		case int:
			return n
		}
	}
	return defaultVal
}

func strSliceParam(params map[string]any, key string) []string {
	v, ok := params[key]
	if !ok {
		return nil
	}
	arr, ok := v.([]any)
	if !ok {
		return nil
	}
	out := make([]string, 0, len(arr))
	for _, item := range arr {
		if s, ok := item.(string); ok {
			out = append(out, s)
		}
	}
	return out
}

func clusterToMap(c *Cluster) map[string]any {
	return map[string]any{
		"clusterArn":  c.ARN,
		"clusterName": c.Name,
		"status":      c.Status,
	}
}

func taskDefToMap(td *TaskDefinition) map[string]any {
	var containerDefs any
	_ = json.Unmarshal([]byte(td.ContainerDefs), &containerDefs)
	return map[string]any{
		"taskDefinitionArn":    td.ARN,
		"family":               td.Family,
		"revision":             td.Revision,
		"status":               td.Status,
		"containerDefinitions": containerDefs,
	}
}

func taskToMap(t *Task) map[string]any {
	m := map[string]any{
		"taskArn":           t.ARN,
		"clusterArn":        t.ClusterARN,
		"taskDefinitionArn": t.TaskDefARN,
		"lastStatus":        t.Status,
		"desiredStatus":     t.DesiredStatus,
	}
	if t.StartedAt != nil {
		m["startedAt"] = t.StartedAt.Format("2006-01-02T15:04:05Z")
	}
	if t.StoppedAt != nil {
		m["stoppedAt"] = t.StoppedAt.Format("2006-01-02T15:04:05Z")
	}
	return m
}

func serviceToMap(s *Service) map[string]any {
	return map[string]any{
		"serviceArn":     s.ARN,
		"serviceName":    s.Name,
		"clusterArn":     s.ClusterARN,
		"taskDefinition": s.TaskDefARN,
		"desiredCount":   s.DesiredCount,
		"runningCount":   s.RunningCount,
		"status":         s.Status,
	}
}

func capacityProviderToMap(cp *CapacityProvider) map[string]any {
	return map[string]any{
		"name":                cp.Name,
		"capacityProviderArn": cp.ARN,
		"status":              cp.Status,
		"autoScalingGroupProvider": map[string]any{
			"autoScalingGroupArn": cp.AsgARN,
		},
	}
}

func containerInstanceToMap(ci *ContainerInstance) map[string]any {
	return map[string]any{
		"containerInstanceArn": ci.ARN,
		"clusterArn":           ci.ClusterARN,
		"ec2InstanceId":        ci.Ec2InstanceID,
		"status":               ci.Status,
		"agentConnected":       ci.AgentConnected,
		"registeredAt":         ci.RegisteredAt.Format("2006-01-02T15:04:05Z"),
	}
}

func attributeToMap(a ECSAttribute) map[string]any {
	return map[string]any{
		"name":       a.Name,
		"value":      a.Value,
		"targetType": a.TargetType,
		"targetId":   a.TargetID,
	}
}

func parseAttributes(raw []any) []ECSAttribute {
	out := make([]ECSAttribute, 0, len(raw))
	for _, item := range raw {
		m, ok := item.(map[string]any)
		if !ok {
			continue
		}
		a := ECSAttribute{
			TargetType: "container-instance",
		}
		if v, ok := m["name"].(string); ok {
			a.Name = v
		}
		if v, ok := m["value"].(string); ok {
			a.Value = v
		}
		if v, ok := m["targetType"].(string); ok {
			a.TargetType = v
		}
		if v, ok := m["targetId"].(string); ok {
			a.TargetID = v
		}
		out = append(out, a)
	}
	return out
}

// --- Cluster extras ---

func (p *Provider) handleUpdateCluster(params map[string]any) (*plugin.Response, error) {
	clusterRef := strParam(params, "cluster")
	if clusterRef == "" {
		return ecsError("MissingParameter", "cluster is required", http.StatusBadRequest), nil
	}
	clusterARN := clusterRef
	if !strings.Contains(clusterRef, ":") {
		clusterARN = fmt.Sprintf("arn:aws:ecs:%s:%s:cluster/%s", region, defaultAccountID, clusterRef)
	}
	clusters, err := p.store.DescribeClusters(defaultAccountID, []string{clusterARN})
	if err != nil || len(clusters) == 0 {
		return ecsError("ClusterNotFoundException", "cluster not found", http.StatusBadRequest), nil
	}
	return ecsJSON(http.StatusOK, map[string]any{"cluster": clusterToMap(&clusters[0])})
}

func (p *Provider) handleUpdateClusterSettings(params map[string]any) (*plugin.Response, error) {
	return p.handleUpdateCluster(params)
}

// --- TaskDefinition extras ---

func (p *Provider) handleListTaskDefinitionFamilies(_ map[string]any) (*plugin.Response, error) {
	arns, err := p.store.ListTaskDefinitions(defaultAccountID)
	if err != nil {
		return nil, err
	}
	familiesSet := make(map[string]bool)
	for _, a := range arns {
		// arn:aws:ecs:region:acct:task-definition/family:rev
		parts := strings.Split(a, "/")
		if len(parts) < 2 {
			continue
		}
		fam := parts[len(parts)-1]
		if idx := strings.LastIndex(fam, ":"); idx >= 0 {
			fam = fam[:idx]
		}
		familiesSet[fam] = true
	}
	families := make([]string, 0, len(familiesSet))
	for f := range familiesSet {
		families = append(families, f)
	}
	return ecsJSON(http.StatusOK, map[string]any{"families": families})
}

// --- Task extras ---

func (p *Provider) handleStartTask(params map[string]any) (*plugin.Response, error) {
	clusterArn := strParam(params, "cluster")
	taskDefArn := strParam(params, "taskDefinition")
	if clusterArn == "" || taskDefArn == "" {
		return ecsError("MissingParameter", "cluster and taskDefinition are required", http.StatusBadRequest), nil
	}
	task, err := p.store.RunTask(defaultAccountID, clusterArn, taskDefArn)
	if err != nil {
		return nil, err
	}
	return ecsJSON(http.StatusOK, map[string]any{"tasks": []any{taskToMap(task)}, "failures": []any{}})
}

func (p *Provider) handleListTasks(params map[string]any) (*plugin.Response, error) {
	clusterArn := strParam(params, "cluster")
	// ListTasks for a given cluster just returns all RUNNING tasks in that cluster.
	tasks, err := p.store.DescribeTasks(defaultAccountID, nil)
	if err != nil {
		return nil, err
	}
	arns := make([]string, 0, len(tasks))
	for _, t := range tasks {
		if clusterArn == "" || t.ClusterARN == clusterArn {
			arns = append(arns, t.ARN)
		}
	}
	return ecsJSON(http.StatusOK, map[string]any{"taskArns": arns})
}

// --- Service extras ---

func (p *Provider) handleDescribeServices(params map[string]any) (*plugin.Response, error) {
	arns := strSliceParam(params, "services")
	clusterRef := strParam(params, "cluster")
	// Resolve service names to ARNs within cluster.
	resolved := make([]string, 0, len(arns))
	for _, a := range arns {
		if strings.Contains(a, ":") {
			resolved = append(resolved, a)
			continue
		}
		clusterARN := clusterRef
		if clusterRef != "" && !strings.Contains(clusterRef, ":") {
			clusterARN = fmt.Sprintf("arn:aws:ecs:%s:%s:cluster/%s", region, defaultAccountID, clusterRef)
		}
		if found, err := p.store.FindServiceARNByName(defaultAccountID, clusterARN, a); err == nil {
			resolved = append(resolved, found)
		}
	}
	services, err := p.store.DescribeServices(defaultAccountID, resolved)
	if err != nil {
		return nil, err
	}
	items := make([]any, 0, len(services))
	for i := range services {
		items = append(items, serviceToMap(&services[i]))
	}
	return ecsJSON(http.StatusOK, map[string]any{"services": items, "failures": []any{}})
}

// --- Capacity Provider extras ---

func (p *Provider) handleUpdateCapacityProvider(params map[string]any) (*plugin.Response, error) {
	name := strParam(params, "name")
	if name == "" {
		return ecsError("MissingParameter", "name is required", http.StatusBadRequest), nil
	}
	cps, err := p.store.DescribeCapacityProviders(defaultAccountID, []string{name})
	if err != nil || len(cps) == 0 {
		return ecsError("ResourceNotFoundException", "capacity provider not found", http.StatusBadRequest), nil
	}
	return ecsJSON(http.StatusOK, map[string]any{"capacityProvider": capacityProviderToMap(&cps[0])})
}

// --- Container Instance extras ---

func (p *Provider) handleRegisterContainerInstance(params map[string]any) (*plugin.Response, error) {
	clusterRef := strParam(params, "cluster")
	if clusterRef == "" {
		clusterRef = "default"
	}
	clusterARN := clusterRef
	clusterName := clusterRef
	if strings.Contains(clusterRef, ":") {
		parts := strings.Split(clusterRef, "/")
		clusterName = parts[len(parts)-1]
	} else {
		clusterARN = fmt.Sprintf("arn:aws:ecs:%s:%s:cluster/%s", region, defaultAccountID, clusterRef)
	}
	ec2ID := strParam(params, "instanceIdentityDocument")
	ci, err := p.store.RegisterContainerInstance(defaultAccountID, clusterARN, clusterName, ec2ID)
	if err != nil {
		return nil, err
	}
	return ecsJSON(http.StatusOK, map[string]any{"containerInstance": containerInstanceToMap(ci)})
}

// --- Task Sets ---

func (p *Provider) handleCreateTaskSet(params map[string]any) (*plugin.Response, error) {
	serviceARN := strParam(params, "service")
	clusterARN := strParam(params, "cluster")
	taskDefARN := strParam(params, "taskDefinition")
	externalID := strParam(params, "externalId")
	launchType := strParam(params, "launchType")
	if serviceARN == "" || clusterARN == "" || taskDefARN == "" {
		return ecsError("MissingParameter", "service, cluster, and taskDefinition are required", http.StatusBadRequest), nil
	}
	ts, err := p.store.CreateTaskSet(defaultAccountID, clusterARN, serviceARN, taskDefARN, externalID, launchType)
	if err != nil {
		return nil, err
	}
	return ecsJSON(http.StatusOK, map[string]any{"taskSet": taskSetToMap(ts)})
}

func (p *Provider) handleDeleteTaskSet(params map[string]any) (*plugin.Response, error) {
	id := strParam(params, "taskSet")
	if id == "" {
		return ecsError("MissingParameter", "taskSet is required", http.StatusBadRequest), nil
	}
	if err := p.store.DeleteTaskSet(defaultAccountID, id); err != nil {
		return ecsError("TaskSetNotFoundException", "task set not found", http.StatusBadRequest), nil
	}
	return ecsJSON(http.StatusOK, map[string]any{"taskSet": map[string]any{"id": id, "status": "INACTIVE"}})
}

func (p *Provider) handleUpdateTaskSet(params map[string]any) (*plugin.Response, error) {
	id := strParam(params, "taskSet")
	if id == "" {
		return ecsError("MissingParameter", "taskSet is required", http.StatusBadRequest), nil
	}
	scale := 100.0
	unit := "PERCENT"
	if s, ok := params["scale"].(map[string]any); ok {
		if v, ok := s["value"].(float64); ok {
			scale = v
		}
		if v, ok := s["unit"].(string); ok {
			unit = v
		}
	}
	ts, err := p.store.UpdateTaskSet(defaultAccountID, id, scale, unit)
	if err != nil {
		return ecsError("TaskSetNotFoundException", "task set not found", http.StatusBadRequest), nil
	}
	return ecsJSON(http.StatusOK, map[string]any{"taskSet": taskSetToMap(ts)})
}

func (p *Provider) handleDescribeTaskSets(params map[string]any) (*plugin.Response, error) {
	serviceARN := strParam(params, "service")
	sets, err := p.store.ListTaskSets(defaultAccountID, serviceARN)
	if err != nil {
		return nil, err
	}
	items := make([]any, 0, len(sets))
	for i := range sets {
		items = append(items, taskSetToMap(&sets[i]))
	}
	return ecsJSON(http.StatusOK, map[string]any{"taskSets": items, "failures": []any{}})
}

func (p *Provider) handleUpdateServicePrimaryTaskSet(params map[string]any) (*plugin.Response, error) {
	id := strParam(params, "primaryTaskSet")
	if id == "" {
		return ecsError("MissingParameter", "primaryTaskSet is required", http.StatusBadRequest), nil
	}
	ts, err := p.store.GetTaskSet(defaultAccountID, id)
	if err != nil {
		return ecsError("TaskSetNotFoundException", "task set not found", http.StatusBadRequest), nil
	}
	return ecsJSON(http.StatusOK, map[string]any{"taskSet": taskSetToMap(ts)})
}

// --- Service Auto-Scaling ---

func (p *Provider) handleRegisterScalableTarget(params map[string]any) (*plugin.Response, error) {
	serviceARN := strParam(params, "serviceARN")
	if serviceARN == "" {
		serviceARN = strParam(params, "resourceId")
	}
	dimension := strParam(params, "scalableDimension")
	if serviceARN == "" || dimension == "" {
		return ecsError("InvalidParameterException", "serviceARN and scalableDimension are required", http.StatusBadRequest), nil
	}
	minCap := intParam(params, "minCapacity", 0)
	maxCap := intParam(params, "maxCapacity", 10)
	roleARN := strParam(params, "roleARN")
	if _, err := p.store.RegisterScalableTarget(defaultAccountID, serviceARN, dimension, roleARN, minCap, maxCap); err != nil {
		return nil, err
	}
	return ecsJSON(http.StatusOK, map[string]any{})
}

func (p *Provider) handleDeregisterScalableTarget(params map[string]any) (*plugin.Response, error) {
	serviceARN := strParam(params, "serviceARN")
	if serviceARN == "" {
		serviceARN = strParam(params, "resourceId")
	}
	dimension := strParam(params, "scalableDimension")
	if err := p.store.DeregisterScalableTarget(defaultAccountID, serviceARN, dimension); err != nil {
		return ecsError("ObjectNotFoundException", "scalable target not found", http.StatusBadRequest), nil
	}
	return ecsJSON(http.StatusOK, map[string]any{})
}

func (p *Provider) handleDescribeScalableTargets(params map[string]any) (*plugin.Response, error) {
	serviceARN := strParam(params, "serviceARN")
	if serviceARN == "" {
		serviceARN = strParam(params, "resourceId")
	}
	dimension := strParam(params, "scalableDimension")
	targets, err := p.store.ListScalableTargets(defaultAccountID, serviceARN, dimension)
	if err != nil {
		return nil, err
	}
	items := make([]any, 0, len(targets))
	for _, t := range targets {
		items = append(items, scalableTargetToMap(&t))
	}
	return ecsJSON(http.StatusOK, map[string]any{"scalableTargets": items})
}

func (p *Provider) handlePutScalingPolicy(params map[string]any) (*plugin.Response, error) {
	serviceARN := strParam(params, "serviceARN")
	if serviceARN == "" {
		serviceARN = strParam(params, "resourceId")
	}
	policyName := strParam(params, "policyName")
	dimension := strParam(params, "scalableDimension")
	if serviceARN == "" || policyName == "" || dimension == "" {
		return ecsError("InvalidParameterException", "serviceARN, policyName, and scalableDimension are required", http.StatusBadRequest), nil
	}
	policyType := strParam(params, "policyType")
	if policyType == "" {
		policyType = "TargetTrackingScaling"
	}
	config := "{}"
	if cfg, ok := params["targetTrackingScalingPolicyConfiguration"]; ok {
		b, _ := json.Marshal(cfg)
		config = string(b)
	} else if cfg, ok := params["stepScalingPolicyConfiguration"]; ok {
		b, _ := json.Marshal(cfg)
		config = string(b)
	}
	policy, err := p.store.PutServiceScalingPolicy(defaultAccountID, serviceARN, policyName, dimension, policyType, config)
	if err != nil {
		return nil, err
	}
	return ecsJSON(http.StatusOK, map[string]any{"policyARN": policy.PolicyARN, "alarms": []any{}})
}

func (p *Provider) handleDeleteScalingPolicy(params map[string]any) (*plugin.Response, error) {
	serviceARN := strParam(params, "serviceARN")
	if serviceARN == "" {
		serviceARN = strParam(params, "resourceId")
	}
	policyName := strParam(params, "policyName")
	if err := p.store.DeleteServiceScalingPolicy(defaultAccountID, serviceARN, policyName); err != nil {
		return ecsError("ObjectNotFoundException", "scaling policy not found", http.StatusBadRequest), nil
	}
	return ecsJSON(http.StatusOK, map[string]any{})
}

func (p *Provider) handleDescribeScalingPolicies(params map[string]any) (*plugin.Response, error) {
	serviceARN := strParam(params, "serviceARN")
	if serviceARN == "" {
		serviceARN = strParam(params, "resourceId")
	}
	policies, err := p.store.ListServiceScalingPolicies(defaultAccountID, serviceARN)
	if err != nil {
		return nil, err
	}
	items := make([]any, 0, len(policies))
	for i := range policies {
		items = append(items, scalingPolicyToMap(&policies[i]))
	}
	return ecsJSON(http.StatusOK, map[string]any{"scalingPolicies": items})
}

// --- Tags ---

func (p *Provider) handleTagResource(params map[string]any) (*plugin.Response, error) {
	arn := strParam(params, "resourceArn")
	if arn == "" {
		return ecsError("MissingParameter", "resourceArn is required", http.StatusBadRequest), nil
	}
	rawTags, _ := params["tags"].([]any)
	tags := make(map[string]string)
	for _, t := range rawTags {
		m, ok := t.(map[string]any)
		if !ok {
			continue
		}
		k, _ := m["key"].(string)
		v, _ := m["value"].(string)
		if k != "" {
			tags[k] = v
		}
	}
	if err := p.store.tags.AddTags(arn, tags); err != nil {
		return nil, err
	}
	return ecsJSON(http.StatusOK, map[string]any{})
}

func (p *Provider) handleUntagResource(params map[string]any) (*plugin.Response, error) {
	arn := strParam(params, "resourceArn")
	if arn == "" {
		return ecsError("MissingParameter", "resourceArn is required", http.StatusBadRequest), nil
	}
	keys := strSliceParam(params, "tagKeys")
	if err := p.store.tags.RemoveTags(arn, keys); err != nil {
		return nil, err
	}
	return ecsJSON(http.StatusOK, map[string]any{})
}

func (p *Provider) handleListTagsForResource(params map[string]any) (*plugin.Response, error) {
	arn := strParam(params, "resourceArn")
	if arn == "" {
		return ecsError("MissingParameter", "resourceArn is required", http.StatusBadRequest), nil
	}
	tags, err := p.store.tags.ListTags(arn)
	if err != nil {
		return nil, err
	}
	items := make([]map[string]string, 0, len(tags))
	for k, v := range tags {
		items = append(items, map[string]string{"key": k, "value": v})
	}
	return ecsJSON(http.StatusOK, map[string]any{"tags": items})
}

// --- Account Settings ---

func (p *Provider) handleListAccountSettings(params map[string]any) (*plugin.Response, error) {
	name := strParam(params, "name")
	principal := strParam(params, "principalArn")
	settings, err := p.store.ListAccountSettings(defaultAccountID, name, principal)
	if err != nil {
		return nil, err
	}
	items := make([]any, 0, len(settings))
	for _, st := range settings {
		items = append(items, map[string]any{
			"name":         st.Name,
			"value":        st.Value,
			"principalArn": st.Principal,
		})
	}
	return ecsJSON(http.StatusOK, map[string]any{"settings": items})
}

func (p *Provider) handlePutAccountSetting(params map[string]any) (*plugin.Response, error) {
	name := strParam(params, "name")
	value := strParam(params, "value")
	principal := strParam(params, "principalArn")
	if name == "" || value == "" {
		return ecsError("MissingParameter", "name and value are required", http.StatusBadRequest), nil
	}
	setting, err := p.store.PutAccountSetting(defaultAccountID, name, value, principal)
	if err != nil {
		return nil, err
	}
	return ecsJSON(http.StatusOK, map[string]any{"setting": map[string]any{
		"name":         setting.Name,
		"value":        setting.Value,
		"principalArn": setting.Principal,
	}})
}

func (p *Provider) handlePutAccountSettingDefault(params map[string]any) (*plugin.Response, error) {
	return p.handlePutAccountSetting(params)
}

func (p *Provider) handleDeleteAccountSetting(params map[string]any) (*plugin.Response, error) {
	name := strParam(params, "name")
	principal := strParam(params, "principalArn")
	if err := p.store.DeleteAccountSetting(defaultAccountID, name, principal); err != nil {
		return nil, err
	}
	return ecsJSON(http.StatusOK, map[string]any{"setting": map[string]any{"name": name, "principalArn": principal}})
}

// --- Other extras ---

func (p *Provider) handleSubmitContainerStateChange(_ map[string]any) (*plugin.Response, error) {
	return ecsJSON(http.StatusOK, map[string]any{"acknowledgment": "ACK"})
}

func (p *Provider) handleDiscoverPollEndpoint(_ map[string]any) (*plugin.Response, error) {
	return ecsJSON(http.StatusOK, map[string]any{
		"endpoint":               fmt.Sprintf("https://ecs-a-1.%s.amazonaws.com/", region),
		"telemetryEndpoint":      fmt.Sprintf("https://ecs-t-1.%s.amazonaws.com/", region),
		"serviceConnectEndpoint": fmt.Sprintf("https://ecs-sc-1.%s.amazonaws.com/", region),
	})
}

// --- helpers for new types ---

func taskSetToMap(ts *TaskSet) map[string]any {
	return map[string]any{
		"id":             ts.ID,
		"taskSetArn":     ts.ARN,
		"serviceArn":     ts.ServiceARN,
		"clusterArn":     ts.ClusterARN,
		"taskDefinition": ts.TaskDefARN,
		"externalId":     ts.ExternalID,
		"launchType":     ts.LaunchType,
		"status":         ts.Status,
		"scale": map[string]any{
			"value": ts.ScaleValue,
			"unit":  ts.ScaleUnit,
		},
		"createdAt": ts.CreatedAt.Format("2006-01-02T15:04:05Z"),
	}
}

func scalableTargetToMap(t *ServiceScalableTarget) map[string]any {
	return map[string]any{
		"serviceARN":        t.ServiceARN,
		"resourceId":        t.ServiceARN,
		"scalableDimension": t.ScalableDimension,
		"minCapacity":       t.MinCapacity,
		"maxCapacity":       t.MaxCapacity,
		"roleARN":           t.RoleARN,
		"creationTime":      t.CreatedAt.Format("2006-01-02T15:04:05Z"),
	}
}

func scalingPolicyToMap(p *ServiceScalingPolicy) map[string]any {
	return map[string]any{
		"policyARN":         p.PolicyARN,
		"policyName":        p.PolicyName,
		"serviceARN":        p.ServiceARN,
		"resourceId":        p.ServiceARN,
		"scalableDimension": p.ScalableDimension,
		"policyType":        p.PolicyType,
		"creationTime":      p.CreatedAt.Format("2006-01-02T15:04:05Z"),
	}
}
