// SPDX-License-Identifier: Apache-2.0

// internal/services/ecs/provider_test.go
package ecs

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/skyoo2003/devcloud/internal/plugin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestECSProvider(t *testing.T) *Provider {
	t.Helper()
	dir := t.TempDir()
	p := &Provider{}
	err := p.Init(plugin.PluginConfig{DataDir: dir, Options: map[string]any{
		"db_path": filepath.Join(dir, "ecs.db"),
	}})
	require.NoError(t, err)
	t.Cleanup(func() { p.Shutdown(context.Background()) })
	return p
}

func ecsRequest(t *testing.T, p *Provider, action string, body map[string]any) map[string]any {
	t.Helper()
	raw, _ := json.Marshal(body)
	req := httptest.NewRequest("POST", "/", bytes.NewReader(raw))
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req.Header.Set("X-Amz-Target", "AmazonEC2ContainerServiceV20141113."+action)
	resp, err := p.HandleRequest(context.Background(), "", req)
	require.NoError(t, err)
	var result map[string]any
	json.Unmarshal(resp.Body, &result)
	return result
}

func TestClusterLifecycle(t *testing.T) {
	p := newTestECSProvider(t)

	result := ecsRequest(t, p, "CreateCluster", map[string]any{"clusterName": "my-cluster"})
	cluster, ok := result["cluster"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "my-cluster", cluster["clusterName"])
	assert.Equal(t, "ACTIVE", cluster["status"])

	listResult := ecsRequest(t, p, "ListClusters", map[string]any{})
	arns, ok := listResult["clusterArns"].([]any)
	require.True(t, ok)
	assert.Len(t, arns, 1)

	clusterArn := cluster["clusterArn"].(string)
	deleteResult := ecsRequest(t, p, "DeleteCluster", map[string]any{"cluster": clusterArn})
	deleted, ok := deleteResult["cluster"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "INACTIVE", deleted["status"])
}

func TestTaskDefinitionAndRunTask(t *testing.T) {
	p := newTestECSProvider(t)

	// Create cluster
	clusterResult := ecsRequest(t, p, "CreateCluster", map[string]any{"clusterName": "test-cluster"})
	clusterArn := clusterResult["cluster"].(map[string]any)["clusterArn"].(string)

	// Register task definition
	tdResult := ecsRequest(t, p, "RegisterTaskDefinition", map[string]any{
		"family": "my-task",
		"containerDefinitions": []map[string]any{
			{"name": "web", "image": "nginx:latest"},
		},
	})
	td, ok := tdResult["taskDefinition"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "my-task", td["family"])
	assert.Equal(t, float64(1), td["revision"])
	taskDefArn := td["taskDefinitionArn"].(string)

	// Run task
	runResult := ecsRequest(t, p, "RunTask", map[string]any{
		"cluster":        clusterArn,
		"taskDefinition": taskDefArn,
	})
	tasks, ok := runResult["tasks"].([]any)
	require.True(t, ok)
	require.Len(t, tasks, 1)
	task := tasks[0].(map[string]any)
	assert.Equal(t, "RUNNING", task["lastStatus"])
	taskArn := task["taskArn"].(string)

	// Stop task
	stopResult := ecsRequest(t, p, "StopTask", map[string]any{"task": taskArn})
	stoppedTask, ok := stopResult["task"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "STOPPED", stoppedTask["lastStatus"])
}

func TestServiceCRUD(t *testing.T) {
	p := newTestECSProvider(t)

	clusterResult := ecsRequest(t, p, "CreateCluster", map[string]any{"clusterName": "svc-cluster"})
	clusterArn := clusterResult["cluster"].(map[string]any)["clusterArn"].(string)

	tdResult := ecsRequest(t, p, "RegisterTaskDefinition", map[string]any{"family": "svc-task"})
	taskDefArn := tdResult["taskDefinition"].(map[string]any)["taskDefinitionArn"].(string)

	svcResult := ecsRequest(t, p, "CreateService", map[string]any{
		"serviceName":    "my-service",
		"cluster":        clusterArn,
		"taskDefinition": taskDefArn,
		"desiredCount":   2,
	})
	svc, ok := svcResult["service"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "my-service", svc["serviceName"])
	serviceArn := svc["serviceArn"].(string)

	listResult := ecsRequest(t, p, "ListServices", map[string]any{"cluster": clusterArn})
	arns := listResult["serviceArns"].([]any)
	assert.Len(t, arns, 1)
	assert.Equal(t, serviceArn, arns[0])

	deleteResult := ecsRequest(t, p, "DeleteService", map[string]any{"service": serviceArn})
	deleted := deleteResult["service"].(map[string]any)
	assert.Equal(t, "INACTIVE", deleted["status"])
}

func TestCapacityProviders(t *testing.T) {
	p := newTestECSProvider(t)

	// Create
	res := ecsRequest(t, p, "CreateCapacityProvider", map[string]any{
		"name": "my-cp",
		"autoScalingGroupProvider": map[string]any{
			"autoScalingGroupArn": "arn:aws:autoscaling:us-east-1:000000000000:autoScalingGroup:123:autoScalingGroupName/my-asg",
		},
	})
	cp, ok := res["capacityProvider"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "my-cp", cp["name"])
	assert.Equal(t, "ACTIVE", cp["status"])

	// Describe
	descRes := ecsRequest(t, p, "DescribeCapacityProviders", map[string]any{
		"capacityProviders": []string{"my-cp"},
	})
	cps, ok := descRes["capacityProviders"].([]any)
	require.True(t, ok)
	assert.Len(t, cps, 1)

	// Delete
	delRes := ecsRequest(t, p, "DeleteCapacityProvider", map[string]any{"capacityProvider": "my-cp"})
	delCp, ok := delRes["capacityProvider"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "INACTIVE", delCp["status"])
}

func TestContainerInstances(t *testing.T) {
	p := newTestECSProvider(t)

	// Create cluster first
	clusterRes := ecsRequest(t, p, "CreateCluster", map[string]any{"clusterName": "ci-cluster"})
	clusterArn := clusterRes["cluster"].(map[string]any)["clusterArn"].(string)

	// Register a container instance via store directly
	ci, err := p.store.RegisterContainerInstance(defaultAccountID, clusterArn, "ci-cluster", "i-123456")
	require.NoError(t, err)
	assert.Equal(t, "ACTIVE", ci.Status)
	assert.True(t, ci.AgentConnected)

	// List
	listRes := ecsRequest(t, p, "ListContainerInstances", map[string]any{"cluster": clusterArn})
	arns, ok := listRes["containerInstanceArns"].([]any)
	require.True(t, ok)
	assert.Len(t, arns, 1)

	// Describe
	descRes := ecsRequest(t, p, "DescribeContainerInstances", map[string]any{
		"cluster":            clusterArn,
		"containerInstances": []string{ci.ARN},
	})
	cis, ok := descRes["containerInstances"].([]any)
	require.True(t, ok)
	assert.Len(t, cis, 1)

	// Deregister
	deregRes := ecsRequest(t, p, "DeregisterContainerInstance", map[string]any{
		"cluster":           clusterArn,
		"containerInstance": ci.ARN,
	})
	ciMap, ok := deregRes["containerInstance"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "INACTIVE", ciMap["status"])
}

func TestAttributes(t *testing.T) {
	p := newTestECSProvider(t)

	clusterRes := ecsRequest(t, p, "CreateCluster", map[string]any{"clusterName": "attr-cluster"})
	clusterArn := clusterRes["cluster"].(map[string]any)["clusterArn"].(string)

	// Put attributes
	putRes := ecsRequest(t, p, "PutAttributes", map[string]any{
		"cluster": clusterArn,
		"attributes": []any{
			map[string]any{"name": "color", "value": "blue", "targetType": "container-instance", "targetId": "ci-1"},
		},
	})
	attrs, ok := putRes["attributes"].([]any)
	require.True(t, ok)
	assert.Len(t, attrs, 1)

	// List attributes
	listRes := ecsRequest(t, p, "ListAttributes", map[string]any{
		"cluster":       clusterArn,
		"targetType":    "container-instance",
		"attributeName": "color",
	})
	listedAttrs, ok := listRes["attributes"].([]any)
	require.True(t, ok)
	assert.Len(t, listedAttrs, 1)

	// Delete attributes
	ecsRequest(t, p, "DeleteAttributes", map[string]any{
		"cluster": clusterArn,
		"attributes": []any{
			map[string]any{"name": "color", "targetType": "container-instance", "targetId": "ci-1"},
		},
	})
	listRes2 := ecsRequest(t, p, "ListAttributes", map[string]any{
		"cluster":    clusterArn,
		"targetType": "container-instance",
	})
	listedAttrs2, _ := listRes2["attributes"].([]any)
	assert.Len(t, listedAttrs2, 0)
}

func TestTaskProtection(t *testing.T) {
	p := newTestECSProvider(t)

	// UpdateTaskProtection
	res := ecsRequest(t, p, "UpdateTaskProtection", map[string]any{
		"tasks":             []string{"arn:aws:ecs:us-east-1:000000000000:task/abc"},
		"protectionEnabled": true,
	})
	protectedTasks, ok := res["protectedTasks"].([]any)
	require.True(t, ok)
	assert.Len(t, protectedTasks, 1)

	// GetTaskProtection
	res2 := ecsRequest(t, p, "GetTaskProtection", map[string]any{
		"tasks": []string{"arn:aws:ecs:us-east-1:000000000000:task/abc"},
	})
	protectedTasks2, ok := res2["protectedTasks"].([]any)
	require.True(t, ok)
	assert.Len(t, protectedTasks2, 1)
}

func TestSubmitTaskStateChange(t *testing.T) {
	p := newTestECSProvider(t)
	res := ecsRequest(t, p, "SubmitTaskStateChange", map[string]any{})
	_, ok := res["acknowledgment"]
	assert.True(t, ok)
}

func TestServiceAutoScaling(t *testing.T) {
	p := newTestECSProvider(t)

	// Register a scalable target
	res := ecsRequest(t, p, "RegisterScalableTarget", map[string]any{
		"serviceARN":        "arn:aws:ecs:us-east-1:000000000000:service/c1/svc-1",
		"scalableDimension": "ecs:service:DesiredCount",
		"minCapacity":       1,
		"maxCapacity":       10,
	})
	_ = res

	// Put a scaling policy
	polRes := ecsRequest(t, p, "PutScalingPolicy", map[string]any{
		"serviceARN":        "arn:aws:ecs:us-east-1:000000000000:service/c1/svc-1",
		"policyName":        "cpu-scale",
		"scalableDimension": "ecs:service:DesiredCount",
		"policyType":        "TargetTrackingScaling",
	})
	assert.Contains(t, polRes["policyARN"], "arn:aws:")

	// Describe
	descRes := ecsRequest(t, p, "DescribeScalableTargets", map[string]any{
		"serviceARN":        "arn:aws:ecs:us-east-1:000000000000:service/c1/svc-1",
		"scalableDimension": "ecs:service:DesiredCount",
	})
	targets, _ := descRes["scalableTargets"].([]any)
	assert.Len(t, targets, 1)

	descPolRes := ecsRequest(t, p, "DescribeScalingPolicies", map[string]any{
		"serviceARN": "arn:aws:ecs:us-east-1:000000000000:service/c1/svc-1",
	})
	policies, _ := descPolRes["scalingPolicies"].([]any)
	assert.Len(t, policies, 1)

	// Delete policy
	ecsRequest(t, p, "DeleteScalingPolicy", map[string]any{
		"serviceARN": "arn:aws:ecs:us-east-1:000000000000:service/c1/svc-1",
		"policyName": "cpu-scale",
	})

	// Deregister
	ecsRequest(t, p, "DeregisterScalableTarget", map[string]any{
		"serviceARN":        "arn:aws:ecs:us-east-1:000000000000:service/c1/svc-1",
		"scalableDimension": "ecs:service:DesiredCount",
	})
}

func TestECSTags(t *testing.T) {
	p := newTestECSProvider(t)
	arn := "arn:aws:ecs:us-east-1:000000000000:cluster/tagged"

	// Tag
	tagRes := ecsRequest(t, p, "TagResource", map[string]any{
		"resourceArn": arn,
		"tags": []any{
			map[string]any{"key": "env", "value": "prod"},
			map[string]any{"key": "team", "value": "data"},
		},
	})
	_ = tagRes

	// List tags
	listRes := ecsRequest(t, p, "ListTagsForResource", map[string]any{"resourceArn": arn})
	tags, _ := listRes["tags"].([]any)
	assert.Len(t, tags, 2)

	// Untag
	ecsRequest(t, p, "UntagResource", map[string]any{"resourceArn": arn, "tagKeys": []string{"env"}})
	listRes2 := ecsRequest(t, p, "ListTagsForResource", map[string]any{"resourceArn": arn})
	tags2, _ := listRes2["tags"].([]any)
	assert.Len(t, tags2, 1)
}

func TestAccountSettings(t *testing.T) {
	p := newTestECSProvider(t)

	putRes := ecsRequest(t, p, "PutAccountSetting", map[string]any{
		"name":  "serviceLongArnFormat",
		"value": "enabled",
	})
	setting, ok := putRes["setting"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "enabled", setting["value"])

	listRes := ecsRequest(t, p, "ListAccountSettings", map[string]any{})
	settings, _ := listRes["settings"].([]any)
	assert.GreaterOrEqual(t, len(settings), 1)

	ecsRequest(t, p, "DeleteAccountSetting", map[string]any{
		"name": "serviceLongArnFormat",
	})
}

func TestTaskSets(t *testing.T) {
	p := newTestECSProvider(t)

	clusterRes := ecsRequest(t, p, "CreateCluster", map[string]any{"clusterName": "ts-cluster"})
	clusterArn := clusterRes["cluster"].(map[string]any)["clusterArn"].(string)

	tdRes := ecsRequest(t, p, "RegisterTaskDefinition", map[string]any{"family": "ts-task"})
	tdArn := tdRes["taskDefinition"].(map[string]any)["taskDefinitionArn"].(string)

	svcRes := ecsRequest(t, p, "CreateService", map[string]any{
		"cluster":        clusterArn,
		"serviceName":    "ts-svc",
		"taskDefinition": tdArn,
	})
	svcArn := svcRes["service"].(map[string]any)["serviceArn"].(string)

	// Create task set
	createRes := ecsRequest(t, p, "CreateTaskSet", map[string]any{
		"cluster":        clusterArn,
		"service":        svcArn,
		"taskDefinition": tdArn,
		"externalId":     "ext-1",
	})
	ts, ok := createRes["taskSet"].(map[string]any)
	require.True(t, ok)
	id := ts["id"].(string)

	// Describe
	descRes := ecsRequest(t, p, "DescribeTaskSets", map[string]any{"service": svcArn})
	sets, _ := descRes["taskSets"].([]any)
	assert.Len(t, sets, 1)

	// Update
	updRes := ecsRequest(t, p, "UpdateTaskSet", map[string]any{
		"taskSet": id,
		"scale":   map[string]any{"value": 50.0, "unit": "PERCENT"},
	})
	updated, _ := updRes["taskSet"].(map[string]any)
	scale, _ := updated["scale"].(map[string]any)
	assert.Equal(t, 50.0, scale["value"])

	// Delete
	delRes := ecsRequest(t, p, "DeleteTaskSet", map[string]any{"taskSet": id})
	assert.Equal(t, "INACTIVE", delRes["taskSet"].(map[string]any)["status"])
}

func TestStartTaskAndListTasks(t *testing.T) {
	p := newTestECSProvider(t)
	clusterRes := ecsRequest(t, p, "CreateCluster", map[string]any{"clusterName": "st-cluster"})
	clusterArn := clusterRes["cluster"].(map[string]any)["clusterArn"].(string)

	tdRes := ecsRequest(t, p, "RegisterTaskDefinition", map[string]any{"family": "st-task"})
	tdArn := tdRes["taskDefinition"].(map[string]any)["taskDefinitionArn"].(string)

	startRes := ecsRequest(t, p, "StartTask", map[string]any{
		"cluster":        clusterArn,
		"taskDefinition": tdArn,
	})
	tasks, _ := startRes["tasks"].([]any)
	assert.Len(t, tasks, 1)

	listRes := ecsRequest(t, p, "ListTasks", map[string]any{"cluster": clusterArn})
	arns, _ := listRes["taskArns"].([]any)
	assert.GreaterOrEqual(t, len(arns), 1)
}

func TestDescribeServices(t *testing.T) {
	p := newTestECSProvider(t)
	clusterRes := ecsRequest(t, p, "CreateCluster", map[string]any{"clusterName": "ds-cluster"})
	clusterArn := clusterRes["cluster"].(map[string]any)["clusterArn"].(string)

	tdRes := ecsRequest(t, p, "RegisterTaskDefinition", map[string]any{"family": "ds-task"})
	tdArn := tdRes["taskDefinition"].(map[string]any)["taskDefinitionArn"].(string)

	ecsRequest(t, p, "CreateService", map[string]any{
		"cluster":        clusterArn,
		"serviceName":    "ds-svc",
		"taskDefinition": tdArn,
	})

	descRes := ecsRequest(t, p, "DescribeServices", map[string]any{
		"cluster":  clusterArn,
		"services": []string{"ds-svc"},
	})
	services, _ := descRes["services"].([]any)
	assert.Len(t, services, 1)
}
