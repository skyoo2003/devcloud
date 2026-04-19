// SPDX-License-Identifier: Apache-2.0

// internal/services/shield/provider.go
package shield

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

// Provider implements the Shield Advanced (Shield_20160616) service.
type Provider struct {
	store *Store
}

func (p *Provider) ServiceID() string             { return "shield" }
func (p *Provider) ServiceName() string           { return "Shield_20160616" }
func (p *Provider) Protocol() plugin.ProtocolType { return plugin.ProtocolJSON11 }

func (p *Provider) Init(cfg plugin.PluginConfig) error {
	dataDir := cfg.DataDir
	if dataDir == "" {
		dataDir = "."
	}
	var err error
	p.store, err = NewStore(filepath.Join(dataDir, "shield"))
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
	case "CreateProtection":
		return p.createProtection(params)
	case "DescribeProtection":
		return p.describeProtection(params)
	case "ListProtections":
		return p.listProtections(params)
	case "DeleteProtection":
		return p.deleteProtection(params)
	case "AssociateHealthCheck":
		return p.associateHealthCheck(params)
	case "DisassociateHealthCheck":
		return p.disassociateHealthCheck(params)
	case "CreateProtectionGroup":
		return p.createProtectionGroup(params)
	case "DescribeProtectionGroup":
		return p.describeProtectionGroup(params)
	case "ListProtectionGroups":
		return p.listProtectionGroups(params)
	case "UpdateProtectionGroup":
		return p.updateProtectionGroup(params)
	case "DeleteProtectionGroup":
		return p.deleteProtectionGroup(params)
	case "ListResourcesInProtectionGroup":
		return p.listResourcesInProtectionGroup(params)
	case "CreateSubscription":
		return p.createSubscription()
	case "DescribeSubscription":
		return p.describeSubscription()
	case "UpdateSubscription":
		return p.updateSubscription(params)
	case "DeleteSubscription":
		return p.deleteSubscription()
	case "GetSubscriptionState":
		return p.getSubscriptionState()
	case "EnableProactiveEngagement":
		return p.enableProactiveEngagement()
	case "DisableProactiveEngagement":
		return p.disableProactiveEngagement()
	case "AssociateProactiveEngagementDetails":
		return shared.JSONResponse(http.StatusOK, map[string]any{})
	case "UpdateEmergencyContactSettings":
		return shared.JSONResponse(http.StatusOK, map[string]any{})
	case "DescribeEmergencyContactSettings":
		return shared.JSONResponse(http.StatusOK, map[string]any{"EmergencyContactList": []any{}})
	case "AssociateDRTRole":
		return shared.JSONResponse(http.StatusOK, map[string]any{})
	case "AssociateDRTLogBucket":
		return shared.JSONResponse(http.StatusOK, map[string]any{})
	case "DescribeDRTAccess":
		return shared.JSONResponse(http.StatusOK, map[string]any{"RoleArn": "", "LogBucketList": []any{}})
	case "DisassociateDRTRole":
		return shared.JSONResponse(http.StatusOK, map[string]any{})
	case "DisassociateDRTLogBucket":
		return shared.JSONResponse(http.StatusOK, map[string]any{})
	case "EnableApplicationLayerAutomaticResponse":
		return shared.JSONResponse(http.StatusOK, map[string]any{})
	case "DisableApplicationLayerAutomaticResponse":
		return shared.JSONResponse(http.StatusOK, map[string]any{})
	case "UpdateApplicationLayerAutomaticResponse":
		return shared.JSONResponse(http.StatusOK, map[string]any{})
	case "DescribeAttack":
		return p.describeAttack(params)
	case "DescribeAttackStatistics":
		return p.describeAttackStatistics()
	case "ListAttacks":
		return shared.JSONResponse(http.StatusOK, map[string]any{"AttackSummaries": []any{}})
	case "TagResource":
		return p.tagResource(params)
	case "UntagResource":
		return p.untagResource(params)
	case "ListTagsForResource":
		return p.listTagsForResource(params)
	default:
		return shared.JSONError("InvalidAction", fmt.Sprintf("unknown action: %s", action), http.StatusBadRequest), nil
	}
}

func (p *Provider) ListResources(_ context.Context) ([]plugin.Resource, error) {
	protections, err := p.store.ListProtections()
	if err != nil {
		return nil, err
	}
	res := make([]plugin.Resource, 0, len(protections))
	for _, prot := range protections {
		res = append(res, plugin.Resource{Type: "shield-protection", ID: prot.ID, Name: prot.Name})
	}
	return res, nil
}

func (p *Provider) GetMetrics(_ context.Context) (*plugin.ServiceMetrics, error) {
	return &plugin.ServiceMetrics{}, nil
}

// --- Protection ---

func (p *Provider) createProtection(params map[string]any) (*plugin.Response, error) {
	name, _ := params["Name"].(string)
	resourceARN, _ := params["ResourceArn"].(string)
	if name == "" || resourceARN == "" {
		return shared.JSONError("InvalidParameterException", "Name and ResourceArn are required", http.StatusBadRequest), nil
	}

	id := shared.GenerateUUID()
	arn := shared.BuildARN("shield", "protection", id)
	prot := &Protection{
		ID:             id,
		ARN:            arn,
		Name:           name,
		ResourceARN:    resourceARN,
		HealthCheckIDs: "[]",
	}

	if err := p.store.CreateProtection(prot); err != nil {
		if isUniqueErr(err) {
			return shared.JSONError("ResourceAlreadyExistsException", "protection already exists for this resource", http.StatusBadRequest), nil
		}
		return nil, err
	}

	if rawTags, ok := params["Tags"].([]any); ok {
		tags := parseTagList(rawTags)
		_ = p.store.tags.AddTags(arn, tags)
	}

	return shared.JSONResponse(http.StatusOK, map[string]any{"ProtectionId": id})
}

func (p *Provider) describeProtection(params map[string]any) (*plugin.Response, error) {
	protID, _ := params["ProtectionId"].(string)
	resourceARN, _ := params["ResourceArn"].(string)

	var prot *Protection
	var err error
	if protID != "" {
		prot, err = p.store.GetProtection(protID)
	} else if resourceARN != "" {
		prot, err = p.store.GetProtectionByResourceARN(resourceARN)
	} else {
		return shared.JSONError("InvalidParameterException", "ProtectionId or ResourceArn is required", http.StatusBadRequest), nil
	}
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "protection not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"Protection": protectionToMap(prot),
	})
}

func (p *Provider) listProtections(params map[string]any) (*plugin.Response, error) {
	protections, err := p.store.ListProtections()
	if err != nil {
		return nil, err
	}
	list := make([]map[string]any, 0, len(protections))
	for i := range protections {
		list = append(list, protectionToMap(&protections[i]))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"Protections": list,
	})
}

func (p *Provider) deleteProtection(params map[string]any) (*plugin.Response, error) {
	protID, _ := params["ProtectionId"].(string)
	if protID == "" {
		return shared.JSONError("InvalidParameterException", "ProtectionId is required", http.StatusBadRequest), nil
	}
	prot, err := p.store.GetProtection(protID)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "protection not found", http.StatusBadRequest), nil
	}
	_ = p.store.tags.DeleteAllTags(prot.ARN)
	if err := p.store.DeleteProtection(protID); err != nil {
		return shared.JSONError("ResourceNotFoundException", "protection not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) associateHealthCheck(params map[string]any) (*plugin.Response, error) {
	protID, _ := params["ProtectionId"].(string)
	healthCheckARN, _ := params["HealthCheckArn"].(string)
	if protID == "" || healthCheckARN == "" {
		return shared.JSONError("InvalidParameterException", "ProtectionId and HealthCheckArn are required", http.StatusBadRequest), nil
	}
	prot, err := p.store.GetProtection(protID)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "protection not found", http.StatusBadRequest), nil
	}
	var ids []string
	_ = json.Unmarshal([]byte(prot.HealthCheckIDs), &ids)
	ids = append(ids, healthCheckARN)
	b, _ := json.Marshal(ids)
	p.store.UpdateProtectionHealthChecks(protID, string(b)) //nolint:errcheck
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) disassociateHealthCheck(params map[string]any) (*plugin.Response, error) {
	protID, _ := params["ProtectionId"].(string)
	healthCheckARN, _ := params["HealthCheckArn"].(string)
	if protID == "" || healthCheckARN == "" {
		return shared.JSONError("InvalidParameterException", "ProtectionId and HealthCheckArn are required", http.StatusBadRequest), nil
	}
	prot, err := p.store.GetProtection(protID)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "protection not found", http.StatusBadRequest), nil
	}
	var ids []string
	_ = json.Unmarshal([]byte(prot.HealthCheckIDs), &ids)
	filtered := ids[:0]
	for _, id := range ids {
		if id != healthCheckARN {
			filtered = append(filtered, id)
		}
	}
	b, _ := json.Marshal(filtered)
	p.store.UpdateProtectionHealthChecks(protID, string(b)) //nolint:errcheck
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

// --- ProtectionGroup ---

func (p *Provider) createProtectionGroup(params map[string]any) (*plugin.Response, error) {
	groupID, _ := params["ProtectionGroupId"].(string)
	aggregation, _ := params["Aggregation"].(string)
	pattern, _ := params["Pattern"].(string)
	if groupID == "" {
		return shared.JSONError("InvalidParameterException", "ProtectionGroupId is required", http.StatusBadRequest), nil
	}
	if aggregation == "" {
		aggregation = "SUM"
	}
	if pattern == "" {
		pattern = "ALL"
	}
	resourceType, _ := params["ResourceType"].(string)
	membersJSON := "[]"
	if members, ok := params["Members"].([]any); ok {
		b, _ := json.Marshal(members)
		membersJSON = string(b)
	}

	arn := shared.BuildARN("shield", "protection-group", groupID)
	g := &ProtectionGroup{
		ID:           groupID,
		ARN:          arn,
		Aggregation:  aggregation,
		Pattern:      pattern,
		ResourceType: resourceType,
		Members:      membersJSON,
	}

	if err := p.store.CreateProtectionGroup(g); err != nil {
		if isUniqueErr(err) {
			return shared.JSONError("ResourceAlreadyExistsException", "protection group already exists", http.StatusBadRequest), nil
		}
		return nil, err
	}

	if rawTags, ok := params["Tags"].([]any); ok {
		tags := parseTagList(rawTags)
		_ = p.store.tags.AddTags(arn, tags)
	}

	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) describeProtectionGroup(params map[string]any) (*plugin.Response, error) {
	groupID, _ := params["ProtectionGroupId"].(string)
	if groupID == "" {
		return shared.JSONError("InvalidParameterException", "ProtectionGroupId is required", http.StatusBadRequest), nil
	}
	g, err := p.store.GetProtectionGroup(groupID)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "protection group not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"ProtectionGroup": protectionGroupToMap(g),
	})
}

func (p *Provider) listProtectionGroups(params map[string]any) (*plugin.Response, error) {
	groups, err := p.store.ListProtectionGroups()
	if err != nil {
		return nil, err
	}
	list := make([]map[string]any, 0, len(groups))
	for i := range groups {
		list = append(list, protectionGroupToMap(&groups[i]))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"ProtectionGroups": list,
	})
}

func (p *Provider) updateProtectionGroup(params map[string]any) (*plugin.Response, error) {
	groupID, _ := params["ProtectionGroupId"].(string)
	if groupID == "" {
		return shared.JSONError("InvalidParameterException", "ProtectionGroupId is required", http.StatusBadRequest), nil
	}
	fields := map[string]any{}
	if v, ok := params["Aggregation"].(string); ok {
		fields["Aggregation"] = v
	}
	if v, ok := params["Pattern"].(string); ok {
		fields["Pattern"] = v
	}
	if v, ok := params["ResourceType"].(string); ok {
		fields["ResourceType"] = v
	}
	if members, ok := params["Members"].([]any); ok {
		b, _ := json.Marshal(members)
		fields["Members"] = string(b)
	}
	if err := p.store.UpdateProtectionGroup(groupID, fields); err != nil {
		return shared.JSONError("ResourceNotFoundException", "protection group not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) deleteProtectionGroup(params map[string]any) (*plugin.Response, error) {
	groupID, _ := params["ProtectionGroupId"].(string)
	if groupID == "" {
		return shared.JSONError("InvalidParameterException", "ProtectionGroupId is required", http.StatusBadRequest), nil
	}
	g, err := p.store.GetProtectionGroup(groupID)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "protection group not found", http.StatusBadRequest), nil
	}
	_ = p.store.tags.DeleteAllTags(g.ARN)
	if err := p.store.DeleteProtectionGroup(groupID); err != nil {
		return shared.JSONError("ResourceNotFoundException", "protection group not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) listResourcesInProtectionGroup(params map[string]any) (*plugin.Response, error) {
	groupID, _ := params["ProtectionGroupId"].(string)
	if groupID == "" {
		return shared.JSONError("InvalidParameterException", "ProtectionGroupId is required", http.StatusBadRequest), nil
	}
	g, err := p.store.GetProtectionGroup(groupID)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "protection group not found", http.StatusBadRequest), nil
	}
	var members []string
	_ = json.Unmarshal([]byte(g.Members), &members)
	arns := make([]string, 0, len(members))
	arns = append(arns, members...)
	return shared.JSONResponse(http.StatusOK, map[string]any{"ResourceArns": arns})
}

// --- Subscription ---

func (p *Provider) createSubscription() (*plugin.Response, error) {
	now := time.Now().Unix()
	_, _ = p.store.store.DB().Exec(
		`UPDATE subscription SET state='ACTIVE', start_time=?, end_time=? WHERE id='default'`,
		now, now+365*24*3600,
	)
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) describeSubscription() (*plugin.Response, error) {
	sub, err := p.store.GetSubscription()
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "subscription not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"Subscription": subscriptionToMap(sub),
	})
}

func (p *Provider) updateSubscription(params map[string]any) (*plugin.Response, error) {
	if err := p.store.UpdateSubscription(params); err != nil {
		return shared.JSONError("ResourceNotFoundException", "subscription not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) deleteSubscription() (*plugin.Response, error) {
	p.store.SetSubscriptionState("INACTIVE") //nolint:errcheck
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) getSubscriptionState() (*plugin.Response, error) {
	sub, err := p.store.GetSubscription()
	if err != nil {
		return shared.JSONResponse(http.StatusOK, map[string]any{"SubscriptionState": "INACTIVE"})
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"SubscriptionState": sub.State})
}

func (p *Provider) enableProactiveEngagement() (*plugin.Response, error) {
	p.store.SetProactiveEngagement("ENABLED") //nolint:errcheck
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) disableProactiveEngagement() (*plugin.Response, error) {
	p.store.SetProactiveEngagement("DISABLED") //nolint:errcheck
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

// --- Attacks ---

func (p *Provider) describeAttack(params map[string]any) (*plugin.Response, error) {
	attackID, _ := params["AttackId"].(string)
	if attackID == "" {
		return shared.JSONError("InvalidParameterException", "AttackId is required", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"Attack": map[string]any{
			"AttackId":       attackID,
			"StartTime":      time.Now().Add(-1 * time.Hour).Unix(),
			"EndTime":        time.Now().Unix(),
			"AttackCounters": []any{},
		},
	})
}

func (p *Provider) describeAttackStatistics() (*plugin.Response, error) {
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"TimeRange": map[string]any{
			"FromInclusive": time.Now().Add(-30 * 24 * time.Hour).Unix(),
			"ToExclusive":   time.Now().Unix(),
		},
		"DataItems": []any{},
	})
}

// --- Tags ---

func (p *Provider) tagResource(params map[string]any) (*plugin.Response, error) {
	resourceARN, _ := params["ResourceARN"].(string)
	if resourceARN == "" {
		return shared.JSONError("InvalidParameterException", "ResourceARN is required", http.StatusBadRequest), nil
	}
	rawTags, _ := params["Tags"].([]any)
	tags := parseTagList(rawTags)
	if err := p.store.tags.AddTags(resourceARN, tags); err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) untagResource(params map[string]any) (*plugin.Response, error) {
	resourceARN, _ := params["ResourceARN"].(string)
	if resourceARN == "" {
		return shared.JSONError("InvalidParameterException", "ResourceARN is required", http.StatusBadRequest), nil
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
		return shared.JSONError("InvalidParameterException", "ResourceARN is required", http.StatusBadRequest), nil
	}
	tags, err := p.store.tags.ListTags(resourceARN)
	if err != nil {
		return nil, err
	}
	tagList := make([]map[string]string, 0, len(tags))
	for k, v := range tags {
		tagList = append(tagList, map[string]string{"Key": k, "Value": v})
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"Tags": tagList})
}

// --- Helpers ---

func protectionToMap(prot *Protection) map[string]any {
	var healthCheckIDs []string
	_ = json.Unmarshal([]byte(prot.HealthCheckIDs), &healthCheckIDs)
	if healthCheckIDs == nil {
		healthCheckIDs = []string{}
	}
	return map[string]any{
		"Id":             prot.ID,
		"ProtectionArn":  prot.ARN,
		"Name":           prot.Name,
		"ResourceArn":    prot.ResourceARN,
		"HealthCheckIds": healthCheckIDs,
		"CreationTime":   prot.CreatedAt.Unix(),
	}
}

func protectionGroupToMap(g *ProtectionGroup) map[string]any {
	var members []string
	_ = json.Unmarshal([]byte(g.Members), &members)
	if members == nil {
		members = []string{}
	}
	return map[string]any{
		"ProtectionGroupId":  g.ID,
		"ProtectionGroupArn": g.ARN,
		"Aggregation":        g.Aggregation,
		"Pattern":            g.Pattern,
		"ResourceType":       g.ResourceType,
		"Members":            members,
	}
}

func subscriptionToMap(sub *Subscription) map[string]any {
	return map[string]any{
		"SubscriptionArn":           shared.BuildARN("shield", "subscription", sub.ID),
		"StartTime":                 sub.StartTime,
		"EndTime":                   sub.EndTime,
		"AutoRenew":                 sub.AutoRenew,
		"ProactiveEngagementStatus": sub.ProactiveEngagement,
		"SubscriptionLimits":        map[string]any{},
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
