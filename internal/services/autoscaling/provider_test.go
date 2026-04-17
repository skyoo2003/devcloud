// SPDX-License-Identifier: Apache-2.0

package autoscaling

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

// TestAutoScalingGroupCRUD tests create/describe/update/delete/set-desired for ASGs.
func TestAutoScalingGroupCRUD(t *testing.T) {
	p := newTestProvider(t)

	// Create
	resp := callQuery(t, p, "CreateAutoScalingGroup", map[string]string{
		"AutoScalingGroupName":    "my-asg",
		"LaunchConfigurationName": "my-lc",
		"MinSize":                 "1",
		"MaxSize":                 "5",
		"DesiredCapacity":         "2",
		"VPCZoneIdentifier":       "subnet-123",
		"HealthCheckType":         "EC2",
	})
	assert.Equal(t, 200, resp.StatusCode, string(resp.Body))

	// Describe
	descResp := callQuery(t, p, "DescribeAutoScalingGroups", map[string]string{
		"AutoScalingGroupNames.member.1": "my-asg",
	})
	assert.Equal(t, 200, descResp.StatusCode, string(descResp.Body))

	type describeResult struct {
		Groups []struct {
			Name            string `xml:"AutoScalingGroupName"`
			ARN             string `xml:"AutoScalingGroupARN"`
			LaunchConfig    string `xml:"LaunchConfigurationName"`
			MinSize         int    `xml:"MinSize"`
			MaxSize         int    `xml:"MaxSize"`
			DesiredCapacity int    `xml:"DesiredCapacity"`
			HealthCheckType string `xml:"HealthCheckType"`
		} `xml:"DescribeAutoScalingGroupsResult>AutoScalingGroups>member"`
	}
	var dr describeResult
	require.NoError(t, xml.Unmarshal(descResp.Body, &dr))
	require.Len(t, dr.Groups, 1)
	assert.Equal(t, "my-asg", dr.Groups[0].Name)
	assert.Contains(t, dr.Groups[0].ARN, "arn:aws:autoscaling")
	assert.Equal(t, "my-lc", dr.Groups[0].LaunchConfig)
	assert.Equal(t, 1, dr.Groups[0].MinSize)
	assert.Equal(t, 5, dr.Groups[0].MaxSize)
	assert.Equal(t, 2, dr.Groups[0].DesiredCapacity)
	assert.Equal(t, "EC2", dr.Groups[0].HealthCheckType)

	// Update
	updateResp := callQuery(t, p, "UpdateAutoScalingGroup", map[string]string{
		"AutoScalingGroupName": "my-asg",
		"MinSize":              "2",
		"MaxSize":              "10",
		"DesiredCapacity":      "4",
	})
	assert.Equal(t, 200, updateResp.StatusCode, string(updateResp.Body))

	// Describe after update
	descAfterResp := callQuery(t, p, "DescribeAutoScalingGroups", map[string]string{
		"AutoScalingGroupNames.member.1": "my-asg",
	})
	var dr2 describeResult
	require.NoError(t, xml.Unmarshal(descAfterResp.Body, &dr2))
	require.Len(t, dr2.Groups, 1)
	assert.Equal(t, 2, dr2.Groups[0].MinSize)
	assert.Equal(t, 10, dr2.Groups[0].MaxSize)
	assert.Equal(t, 4, dr2.Groups[0].DesiredCapacity)

	// SetDesiredCapacity
	setResp := callQuery(t, p, "SetDesiredCapacity", map[string]string{
		"AutoScalingGroupName": "my-asg",
		"DesiredCapacity":      "6",
	})
	assert.Equal(t, 200, setResp.StatusCode, string(setResp.Body))

	descAfterSet := callQuery(t, p, "DescribeAutoScalingGroups", map[string]string{
		"AutoScalingGroupNames.member.1": "my-asg",
	})
	var dr3 describeResult
	require.NoError(t, xml.Unmarshal(descAfterSet.Body, &dr3))
	require.Len(t, dr3.Groups, 1)
	assert.Equal(t, 6, dr3.Groups[0].DesiredCapacity)

	// Delete
	delResp := callQuery(t, p, "DeleteAutoScalingGroup", map[string]string{
		"AutoScalingGroupName": "my-asg",
	})
	assert.Equal(t, 200, delResp.StatusCode, string(delResp.Body))

	// Describe after delete - should be empty
	descGone := callQuery(t, p, "DescribeAutoScalingGroups", map[string]string{
		"AutoScalingGroupNames.member.1": "my-asg",
	})
	var drGone describeResult
	require.NoError(t, xml.Unmarshal(descGone.Body, &drGone))
	assert.Len(t, drGone.Groups, 0)
}

// TestLaunchConfigCRUD tests create/describe/delete for launch configurations.
func TestLaunchConfigCRUD(t *testing.T) {
	p := newTestProvider(t)

	// Create
	resp := callQuery(t, p, "CreateLaunchConfiguration", map[string]string{
		"LaunchConfigurationName": "my-lc",
		"ImageId":                 "ami-12345678",
		"InstanceType":            "t3.small",
		"KeyName":                 "my-key",
	})
	assert.Equal(t, 200, resp.StatusCode, string(resp.Body))

	// Duplicate should fail
	dupResp := callQuery(t, p, "CreateLaunchConfiguration", map[string]string{
		"LaunchConfigurationName": "my-lc",
		"ImageId":                 "ami-12345678",
		"InstanceType":            "t3.small",
	})
	assert.Equal(t, 409, dupResp.StatusCode)

	// Describe
	descResp := callQuery(t, p, "DescribeLaunchConfigurations", map[string]string{
		"LaunchConfigurationNames.member.1": "my-lc",
	})
	assert.Equal(t, 200, descResp.StatusCode, string(descResp.Body))

	type describeLCsResult struct {
		LCs []struct {
			Name         string `xml:"LaunchConfigurationName"`
			ARN          string `xml:"LaunchConfigurationARN"`
			ImageId      string `xml:"ImageId"`
			InstanceType string `xml:"InstanceType"`
			KeyName      string `xml:"KeyName"`
		} `xml:"DescribeLaunchConfigurationsResult>LaunchConfigurations>member"`
	}
	var dr describeLCsResult
	require.NoError(t, xml.Unmarshal(descResp.Body, &dr))
	require.Len(t, dr.LCs, 1)
	assert.Equal(t, "my-lc", dr.LCs[0].Name)
	assert.Contains(t, dr.LCs[0].ARN, "arn:aws:autoscaling")
	assert.Equal(t, "ami-12345678", dr.LCs[0].ImageId)
	assert.Equal(t, "t3.small", dr.LCs[0].InstanceType)
	assert.Equal(t, "my-key", dr.LCs[0].KeyName)

	// Delete
	delResp := callQuery(t, p, "DeleteLaunchConfiguration", map[string]string{
		"LaunchConfigurationName": "my-lc",
	})
	assert.Equal(t, 200, delResp.StatusCode, string(delResp.Body))

	// Describe after delete - should be empty
	descGone := callQuery(t, p, "DescribeLaunchConfigurations", map[string]string{
		"LaunchConfigurationNames.member.1": "my-lc",
	})
	var drGone describeLCsResult
	require.NoError(t, xml.Unmarshal(descGone.Body, &drGone))
	assert.Len(t, drGone.LCs, 0)
}

// TestScalingPolicyCRUD tests put/describe/delete/execute for scaling policies.
func TestScalingPolicyCRUD(t *testing.T) {
	p := newTestProvider(t)

	// Create ASG first
	callQuery(t, p, "CreateAutoScalingGroup", map[string]string{
		"AutoScalingGroupName": "asg-for-policy",
		"MinSize":              "1",
		"MaxSize":              "5",
	})

	// PutScalingPolicy
	putResp := callQuery(t, p, "PutScalingPolicy", map[string]string{
		"AutoScalingGroupName": "asg-for-policy",
		"PolicyName":           "my-policy",
		"PolicyType":           "SimpleScaling",
		"AdjustmentType":       "ChangeInCapacity",
		"ScalingAdjustment":    "2",
	})
	assert.Equal(t, 200, putResp.StatusCode, string(putResp.Body))

	type putPolicyResult struct {
		PolicyARN string `xml:"PutScalingPolicyResult>PolicyARN"`
	}
	var pr putPolicyResult
	require.NoError(t, xml.Unmarshal(putResp.Body, &pr))
	assert.Contains(t, pr.PolicyARN, "arn:aws:autoscaling")

	// DescribePolicies
	descResp := callQuery(t, p, "DescribePolicies", map[string]string{
		"AutoScalingGroupName": "asg-for-policy",
	})
	assert.Equal(t, 200, descResp.StatusCode, string(descResp.Body))

	type describePoliciesResult struct {
		Policies []struct {
			PolicyName           string `xml:"PolicyName"`
			AutoScalingGroupName string `xml:"AutoScalingGroupName"`
			PolicyType           string `xml:"PolicyType"`
			AdjustmentType       string `xml:"AdjustmentType"`
			ScalingAdjustment    int    `xml:"ScalingAdjustment"`
		} `xml:"DescribePoliciesResult>ScalingPolicies>member"`
	}
	var dr describePoliciesResult
	require.NoError(t, xml.Unmarshal(descResp.Body, &dr))
	require.Len(t, dr.Policies, 1)
	assert.Equal(t, "my-policy", dr.Policies[0].PolicyName)
	assert.Equal(t, "asg-for-policy", dr.Policies[0].AutoScalingGroupName)
	assert.Equal(t, "SimpleScaling", dr.Policies[0].PolicyType)
	assert.Equal(t, "ChangeInCapacity", dr.Policies[0].AdjustmentType)
	assert.Equal(t, 2, dr.Policies[0].ScalingAdjustment)

	// ExecutePolicy (stub)
	execResp := callQuery(t, p, "ExecutePolicy", map[string]string{
		"AutoScalingGroupName": "asg-for-policy",
		"PolicyName":           "my-policy",
	})
	assert.Equal(t, 200, execResp.StatusCode)

	// DeletePolicy
	delResp := callQuery(t, p, "DeletePolicy", map[string]string{
		"AutoScalingGroupName": "asg-for-policy",
		"PolicyName":           "my-policy",
	})
	assert.Equal(t, 200, delResp.StatusCode, string(delResp.Body))

	// Describe after delete - empty
	descGone := callQuery(t, p, "DescribePolicies", map[string]string{
		"AutoScalingGroupName": "asg-for-policy",
	})
	var drGone describePoliciesResult
	require.NoError(t, xml.Unmarshal(descGone.Body, &drGone))
	assert.Len(t, drGone.Policies, 0)
}

// TestScheduledActionCRUD tests put/describe/delete/batch for scheduled actions.
func TestScheduledActionCRUD(t *testing.T) {
	p := newTestProvider(t)

	// Create ASG
	callQuery(t, p, "CreateAutoScalingGroup", map[string]string{
		"AutoScalingGroupName": "asg-scheduled",
		"MinSize":              "1",
		"MaxSize":              "10",
	})

	// PutScheduledUpdateGroupAction
	putResp := callQuery(t, p, "PutScheduledUpdateGroupAction", map[string]string{
		"AutoScalingGroupName": "asg-scheduled",
		"ScheduledActionName":  "scale-up",
		"Recurrence":           "0 9 * * *",
		"MinSize":              "2",
		"MaxSize":              "10",
		"DesiredCapacity":      "5",
	})
	assert.Equal(t, 200, putResp.StatusCode, string(putResp.Body))

	// DescribeScheduledActions
	descResp := callQuery(t, p, "DescribeScheduledActions", map[string]string{
		"AutoScalingGroupName": "asg-scheduled",
	})
	assert.Equal(t, 200, descResp.StatusCode, string(descResp.Body))

	type describeScheduledResult struct {
		Actions []struct {
			Name       string `xml:"ScheduledActionName"`
			ARN        string `xml:"ScheduledActionARN"`
			ASGName    string `xml:"AutoScalingGroupName"`
			Recurrence string `xml:"Recurrence"`
			MinSize    int    `xml:"MinSize"`
			MaxSize    int    `xml:"MaxSize"`
			DesiredCap int    `xml:"DesiredCapacity"`
		} `xml:"DescribeScheduledActionsResult>ScheduledUpdateGroupActions>member"`
	}
	var dr describeScheduledResult
	require.NoError(t, xml.Unmarshal(descResp.Body, &dr))
	require.Len(t, dr.Actions, 1)
	assert.Equal(t, "scale-up", dr.Actions[0].Name)
	assert.Contains(t, dr.Actions[0].ARN, "arn:aws:autoscaling")
	assert.Equal(t, "asg-scheduled", dr.Actions[0].ASGName)
	assert.Equal(t, "0 9 * * *", dr.Actions[0].Recurrence)
	assert.Equal(t, 5, dr.Actions[0].DesiredCap)

	// BatchPutScheduledUpdateGroupAction
	batchPutResp := callQuery(t, p, "BatchPutScheduledUpdateGroupAction", map[string]string{
		"AutoScalingGroupName": "asg-scheduled",
		"ScheduledUpdateGroupActions.member.1.ScheduledActionName": "scale-down",
		"ScheduledUpdateGroupActions.member.1.Recurrence":          "0 21 * * *",
		"ScheduledUpdateGroupActions.member.1.DesiredCapacity":     "1",
		"ScheduledUpdateGroupActions.member.2.ScheduledActionName": "scale-mid",
		"ScheduledUpdateGroupActions.member.2.Recurrence":          "0 12 * * *",
		"ScheduledUpdateGroupActions.member.2.DesiredCapacity":     "3",
	})
	assert.Equal(t, 200, batchPutResp.StatusCode, string(batchPutResp.Body))

	descAfterBatch := callQuery(t, p, "DescribeScheduledActions", map[string]string{
		"AutoScalingGroupName": "asg-scheduled",
	})
	var drBatch describeScheduledResult
	require.NoError(t, xml.Unmarshal(descAfterBatch.Body, &drBatch))
	assert.Len(t, drBatch.Actions, 3)

	// BatchDeleteScheduledAction
	batchDelResp := callQuery(t, p, "BatchDeleteScheduledAction", map[string]string{
		"AutoScalingGroupName":          "asg-scheduled",
		"ScheduledActionNames.member.1": "scale-down",
		"ScheduledActionNames.member.2": "scale-mid",
	})
	assert.Equal(t, 200, batchDelResp.StatusCode, string(batchDelResp.Body))

	descAfterBatchDel := callQuery(t, p, "DescribeScheduledActions", map[string]string{
		"AutoScalingGroupName": "asg-scheduled",
	})
	var drAfterDel describeScheduledResult
	require.NoError(t, xml.Unmarshal(descAfterBatchDel.Body, &drAfterDel))
	assert.Len(t, drAfterDel.Actions, 1)
	assert.Equal(t, "scale-up", drAfterDel.Actions[0].Name)

	// DeleteScheduledAction
	delResp := callQuery(t, p, "DeleteScheduledAction", map[string]string{
		"AutoScalingGroupName": "asg-scheduled",
		"ScheduledActionName":  "scale-up",
	})
	assert.Equal(t, 200, delResp.StatusCode, string(delResp.Body))

	descGone := callQuery(t, p, "DescribeScheduledActions", map[string]string{
		"AutoScalingGroupName": "asg-scheduled",
	})
	var drGone describeScheduledResult
	require.NoError(t, xml.Unmarshal(descGone.Body, &drGone))
	assert.Len(t, drGone.Actions, 0)
}

// TestLifecycleHookCRUD tests put/describe/delete/complete/heartbeat for lifecycle hooks.
func TestLifecycleHookCRUD(t *testing.T) {
	p := newTestProvider(t)

	// Create ASG
	callQuery(t, p, "CreateAutoScalingGroup", map[string]string{
		"AutoScalingGroupName": "asg-hooks",
		"MinSize":              "1",
		"MaxSize":              "5",
	})

	// PutLifecycleHook
	putResp := callQuery(t, p, "PutLifecycleHook", map[string]string{
		"AutoScalingGroupName": "asg-hooks",
		"LifecycleHookName":    "my-hook",
		"LifecycleTransition":  "autoscaling:EC2_INSTANCE_LAUNCHING",
		"HeartbeatTimeout":     "300",
		"DefaultResult":        "CONTINUE",
		"RoleARN":              "arn:aws:iam::000000000000:role/my-role",
	})
	assert.Equal(t, 200, putResp.StatusCode, string(putResp.Body))

	// DescribeLifecycleHooks
	descResp := callQuery(t, p, "DescribeLifecycleHooks", map[string]string{
		"AutoScalingGroupName": "asg-hooks",
	})
	assert.Equal(t, 200, descResp.StatusCode, string(descResp.Body))

	type describeHooksResult struct {
		Hooks []struct {
			Name       string `xml:"LifecycleHookName"`
			ASGName    string `xml:"AutoScalingGroupName"`
			Transition string `xml:"LifecycleTransition"`
			Timeout    int    `xml:"HeartbeatTimeout"`
			DefaultRes string `xml:"DefaultResult"`
			RoleARN    string `xml:"RoleARN"`
		} `xml:"DescribeLifecycleHooksResult>LifecycleHooks>member"`
	}
	var dr describeHooksResult
	require.NoError(t, xml.Unmarshal(descResp.Body, &dr))
	require.Len(t, dr.Hooks, 1)
	assert.Equal(t, "my-hook", dr.Hooks[0].Name)
	assert.Equal(t, "asg-hooks", dr.Hooks[0].ASGName)
	assert.Equal(t, "autoscaling:EC2_INSTANCE_LAUNCHING", dr.Hooks[0].Transition)
	assert.Equal(t, 300, dr.Hooks[0].Timeout)
	assert.Equal(t, "CONTINUE", dr.Hooks[0].DefaultRes)
	assert.Equal(t, "arn:aws:iam::000000000000:role/my-role", dr.Hooks[0].RoleARN)

	// CompleteLifecycleAction (stub)
	completeResp := callQuery(t, p, "CompleteLifecycleAction", map[string]string{
		"AutoScalingGroupName":  "asg-hooks",
		"LifecycleHookName":     "my-hook",
		"LifecycleActionResult": "CONTINUE",
	})
	assert.Equal(t, 200, completeResp.StatusCode)

	// RecordLifecycleActionHeartbeat (stub)
	heartbeatResp := callQuery(t, p, "RecordLifecycleActionHeartbeat", map[string]string{
		"AutoScalingGroupName": "asg-hooks",
		"LifecycleHookName":    "my-hook",
	})
	assert.Equal(t, 200, heartbeatResp.StatusCode)

	// Upsert hook (put again should update)
	putResp2 := callQuery(t, p, "PutLifecycleHook", map[string]string{
		"AutoScalingGroupName": "asg-hooks",
		"LifecycleHookName":    "my-hook",
		"LifecycleTransition":  "autoscaling:EC2_INSTANCE_TERMINATING",
		"HeartbeatTimeout":     "600",
		"DefaultResult":        "ABANDON",
	})
	assert.Equal(t, 200, putResp2.StatusCode)

	descAfterUpdate := callQuery(t, p, "DescribeLifecycleHooks", map[string]string{
		"AutoScalingGroupName": "asg-hooks",
	})
	var drUpdated describeHooksResult
	require.NoError(t, xml.Unmarshal(descAfterUpdate.Body, &drUpdated))
	require.Len(t, drUpdated.Hooks, 1)
	assert.Equal(t, "autoscaling:EC2_INSTANCE_TERMINATING", drUpdated.Hooks[0].Transition)
	assert.Equal(t, 600, drUpdated.Hooks[0].Timeout)
	assert.Equal(t, "ABANDON", drUpdated.Hooks[0].DefaultRes)

	// DeleteLifecycleHook
	delResp := callQuery(t, p, "DeleteLifecycleHook", map[string]string{
		"AutoScalingGroupName": "asg-hooks",
		"LifecycleHookName":    "my-hook",
	})
	assert.Equal(t, 200, delResp.StatusCode, string(delResp.Body))

	descGone := callQuery(t, p, "DescribeLifecycleHooks", map[string]string{
		"AutoScalingGroupName": "asg-hooks",
	})
	var drGone describeHooksResult
	require.NoError(t, xml.Unmarshal(descGone.Body, &drGone))
	assert.Len(t, drGone.Hooks, 0)
}

// TestTags tests CreateOrUpdateTags, DescribeTags, DeleteTags.
func TestTags(t *testing.T) {
	p := newTestProvider(t)

	// Create ASG
	callQuery(t, p, "CreateAutoScalingGroup", map[string]string{
		"AutoScalingGroupName": "tagged-asg",
		"MinSize":              "1",
		"MaxSize":              "5",
	})

	// CreateOrUpdateTags
	tagResp := callQuery(t, p, "CreateOrUpdateTags", map[string]string{
		"Tags.member.1.ResourceId": "tagged-asg",
		"Tags.member.1.Key":        "Environment",
		"Tags.member.1.Value":      "production",
		"Tags.member.2.ResourceId": "tagged-asg",
		"Tags.member.2.Key":        "Team",
		"Tags.member.2.Value":      "platform",
	})
	assert.Equal(t, 200, tagResp.StatusCode, string(tagResp.Body))

	// DescribeTags
	descResp := callQuery(t, p, "DescribeTags", map[string]string{})
	assert.Equal(t, 200, descResp.StatusCode, string(descResp.Body))

	type describeTagsResult struct {
		Tags []struct {
			Key   string `xml:"Key"`
			Value string `xml:"Value"`
		} `xml:"DescribeTagsResult>Tags>member"`
	}
	var dr describeTagsResult
	require.NoError(t, xml.Unmarshal(descResp.Body, &dr))
	assert.Len(t, dr.Tags, 2)

	// DeleteTags
	delResp := callQuery(t, p, "DeleteTags", map[string]string{
		"Tags.member.1.ResourceId": "tagged-asg",
		"Tags.member.1.Key":        "Environment",
	})
	assert.Equal(t, 200, delResp.StatusCode, string(delResp.Body))

	// DescribeTags after delete
	descAfterDel := callQuery(t, p, "DescribeTags", map[string]string{})
	var drAfterDel describeTagsResult
	require.NoError(t, xml.Unmarshal(descAfterDel.Body, &drAfterDel))
	assert.Len(t, drAfterDel.Tags, 1)
	assert.Equal(t, "Team", drAfterDel.Tags[0].Key)
	assert.Equal(t, "platform", drAfterDel.Tags[0].Value)
}
