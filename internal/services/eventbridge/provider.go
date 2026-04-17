// SPDX-License-Identifier: Apache-2.0

// internal/services/eventbridge/provider.go
package eventbridge

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"strings"
	"time"

	"github.com/skyoo2003/devcloud/internal/plugin"
)

const defaultAccountID = plugin.DefaultAccountID

type Provider struct {
	store      *EBStore
	serverPort int
}

func (p *Provider) ServiceID() string             { return "eventbridge" }
func (p *Provider) ServiceName() string           { return "AmazonEventBridge" }
func (p *Provider) Protocol() plugin.ProtocolType { return plugin.ProtocolJSON11 }

func (p *Provider) Init(cfg plugin.PluginConfig) error {
	dataDir := cfg.DataDir
	if dataDir == "" {
		dataDir = "."
	}
	if port, ok := cfg.Options["server_port"].(int); ok {
		p.serverPort = port
	}
	var err error
	p.store, err = NewEBStore(filepath.Join(dataDir, "eventbridge"))
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
		return ebError("InvalidParameterException", "failed to read body", http.StatusBadRequest), nil
	}
	var params map[string]any
	if len(body) > 0 {
		if err := json.Unmarshal(body, &params); err != nil {
			return ebError("InvalidParameterException", "invalid JSON", http.StatusBadRequest), nil
		}
	} else {
		params = map[string]any{}
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
	case "CreateEventBus":
		return p.createEventBus(params)
	case "DeleteEventBus":
		return p.deleteEventBus(params)
	case "ListEventBuses":
		return p.listEventBuses(params)
	case "PutRule":
		return p.putRule(params)
	case "DeleteRule":
		return p.deleteRule(params)
	case "ListRules":
		return p.listRules(params)
	case "EnableRule":
		return p.enableRule(params)
	case "DisableRule":
		return p.disableRule(params)
	case "PutTargets":
		return p.putTargets(params)
	case "RemoveTargets":
		return p.removeTargets(params)
	case "ListTargetsByRule":
		return p.listTargetsByRule(params)
	case "PutEvents":
		return p.putEvents(params)
	case "DescribeEventBus":
		return p.describeEventBus(params)
	case "DescribeRule":
		return p.describeRule(params)
	// Archive operations
	case "CreateArchive":
		return p.createArchive(params)
	case "DescribeArchive":
		return p.describeArchive(params)
	case "ListArchives":
		return p.listArchives(params)
	case "UpdateArchive":
		return p.updateArchive(params)
	case "DeleteArchive":
		return p.deleteArchive(params)
	// Replay operations
	case "StartReplay":
		return p.startReplay(params)
	case "DescribeReplay":
		return p.describeReplay(params)
	case "ListReplays":
		return p.listReplays(params)
	case "CancelReplay":
		return p.cancelReplay(params)
	// Test
	case "TestEventPattern":
		return p.testEventPattern(params)
	default:
		return ebError("InvalidAction", fmt.Sprintf("unknown action: %s", action), http.StatusBadRequest), nil
	}
}

func (p *Provider) ListResources(_ context.Context) ([]plugin.Resource, error) {
	buses, err := p.store.ListEventBuses(defaultAccountID)
	if err != nil {
		return nil, err
	}
	resources := make([]plugin.Resource, 0, len(buses))
	for _, b := range buses {
		resources = append(resources, plugin.Resource{Type: "event-bus", ID: b.ARN, Name: b.Name})
	}
	return resources, nil
}

func (p *Provider) GetMetrics(_ context.Context) (*plugin.ServiceMetrics, error) {
	return &plugin.ServiceMetrics{}, nil
}

func (p *Provider) createEventBus(params map[string]any) (*plugin.Response, error) {
	name, _ := params["Name"].(string)
	if name == "" {
		return ebError("InvalidParameterException", "Name is required", http.StatusBadRequest), nil
	}
	if err := p.store.CreateEventBus(name, defaultAccountID); err != nil {
		return nil, err
	}
	arn := "arn:aws:events:us-east-1:" + defaultAccountID + ":event-bus/" + name
	return jsonResp(http.StatusOK, map[string]any{"EventBusArn": arn})
}

func (p *Provider) deleteEventBus(params map[string]any) (*plugin.Response, error) {
	name, _ := params["Name"].(string)
	if name == "" {
		return ebError("InvalidParameterException", "Name is required", http.StatusBadRequest), nil
	}
	if err := p.store.DeleteEventBus(name, defaultAccountID); err != nil {
		return ebError("ResourceNotFoundException", "event bus not found", http.StatusBadRequest), nil
	}
	return jsonResp(http.StatusOK, map[string]any{})
}

func (p *Provider) listEventBuses(_ map[string]any) (*plugin.Response, error) {
	buses, err := p.store.ListEventBuses(defaultAccountID)
	if err != nil {
		return nil, err
	}
	list := make([]map[string]string, 0, len(buses))
	for _, b := range buses {
		list = append(list, map[string]string{"Name": b.Name, "Arn": b.ARN})
	}
	return jsonResp(http.StatusOK, map[string]any{"EventBuses": list})
}

func (p *Provider) putRule(params map[string]any) (*plugin.Response, error) {
	name, _ := params["Name"].(string)
	if name == "" {
		return ebError("InvalidParameterException", "Name is required", http.StatusBadRequest), nil
	}
	busName, _ := params["EventBusName"].(string)
	if busName == "" {
		busName = "default"
	}
	state, _ := params["State"].(string)
	if state == "" {
		state = "ENABLED"
	}
	scheduleExpression, _ := params["ScheduleExpression"].(string)
	var eventPattern map[string]any
	if ep, ok := params["EventPattern"].(string); ok && ep != "" {
		_ = json.Unmarshal([]byte(ep), &eventPattern)
	}
	if err := p.store.PutRule(name, busName, defaultAccountID, eventPattern, state, scheduleExpression); err != nil {
		return nil, err
	}
	arn := "arn:aws:events:us-east-1:" + defaultAccountID + ":rule/" + name
	return jsonResp(http.StatusOK, map[string]any{"RuleArn": arn})
}

func (p *Provider) deleteRule(params map[string]any) (*plugin.Response, error) {
	name, _ := params["Name"].(string)
	busName, _ := params["EventBusName"].(string)
	if busName == "" {
		busName = "default"
	}
	if err := p.store.DeleteRule(name, busName, defaultAccountID); err != nil {
		return ebError("ResourceNotFoundException", "rule not found", http.StatusBadRequest), nil
	}
	return jsonResp(http.StatusOK, map[string]any{})
}

func (p *Provider) listRules(params map[string]any) (*plugin.Response, error) {
	busName, _ := params["EventBusName"].(string)
	if busName == "" {
		busName = "default"
	}
	rules, err := p.store.ListRules(busName, defaultAccountID)
	if err != nil {
		return nil, err
	}
	list := make([]map[string]any, 0, len(rules))
	for _, r := range rules {
		arn := "arn:aws:events:us-east-1:" + defaultAccountID + ":rule/" + r.Name
		list = append(list, map[string]any{
			"Name":  r.Name,
			"Arn":   arn,
			"State": r.State,
		})
	}
	return jsonResp(http.StatusOK, map[string]any{"Rules": list})
}

func (p *Provider) enableRule(params map[string]any) (*plugin.Response, error) {
	name, _ := params["Name"].(string)
	busName, _ := params["EventBusName"].(string)
	if busName == "" {
		busName = "default"
	}
	if err := p.store.EnableRule(name, busName, defaultAccountID); err != nil {
		return nil, err
	}
	return jsonResp(http.StatusOK, map[string]any{})
}

func (p *Provider) disableRule(params map[string]any) (*plugin.Response, error) {
	name, _ := params["Name"].(string)
	busName, _ := params["EventBusName"].(string)
	if busName == "" {
		busName = "default"
	}
	if err := p.store.DisableRule(name, busName, defaultAccountID); err != nil {
		return nil, err
	}
	return jsonResp(http.StatusOK, map[string]any{})
}

func (p *Provider) putTargets(params map[string]any) (*plugin.Response, error) {
	ruleName, _ := params["Rule"].(string)
	if ruleName == "" {
		return ebError("InvalidParameterException", "Rule is required", http.StatusBadRequest), nil
	}
	busName, _ := params["EventBusName"].(string)
	if busName == "" {
		busName = "default"
	}
	targetsRaw, _ := params["Targets"].([]any)
	targets := make([]Target, 0, len(targetsRaw))
	for _, t := range targetsRaw {
		if m, ok := t.(map[string]any); ok {
			id, _ := m["Id"].(string)
			arn, _ := m["Arn"].(string)
			inputPath, _ := m["InputPath"].(string)
			targets = append(targets, Target{
				RuleName:  ruleName,
				BusName:   busName,
				TargetID:  id,
				ARN:       arn,
				AccountID: defaultAccountID,
				InputPath: inputPath,
			})
		}
	}
	if err := p.store.PutTargets(ruleName, busName, defaultAccountID, targets); err != nil {
		return nil, err
	}
	return jsonResp(http.StatusOK, map[string]any{"FailedEntryCount": 0, "FailedEntries": []any{}})
}

func (p *Provider) removeTargets(params map[string]any) (*plugin.Response, error) {
	ruleName, _ := params["Rule"].(string)
	busName, _ := params["EventBusName"].(string)
	if busName == "" {
		busName = "default"
	}
	idsRaw, _ := params["Ids"].([]any)
	ids := make([]string, 0, len(idsRaw))
	for _, id := range idsRaw {
		if s, ok := id.(string); ok {
			ids = append(ids, s)
		}
	}
	if err := p.store.RemoveTargets(ruleName, busName, defaultAccountID, ids); err != nil {
		return nil, err
	}
	return jsonResp(http.StatusOK, map[string]any{"FailedEntryCount": 0, "FailedEntries": []any{}})
}

func (p *Provider) listTargetsByRule(params map[string]any) (*plugin.Response, error) {
	ruleName, _ := params["Rule"].(string)
	if ruleName == "" {
		return ebError("InvalidParameterException", "Rule is required", http.StatusBadRequest), nil
	}
	busName, _ := params["EventBusName"].(string)
	if busName == "" {
		busName = "default"
	}
	targets, err := p.store.ListTargetsByRule(ruleName, busName, defaultAccountID)
	if err != nil {
		return nil, err
	}
	list := make([]map[string]string, 0, len(targets))
	for _, t := range targets {
		list = append(list, map[string]string{"Id": t.TargetID, "Arn": t.ARN})
	}
	return jsonResp(http.StatusOK, map[string]any{"Targets": list})
}

func (p *Provider) describeEventBus(params map[string]any) (*plugin.Response, error) {
	name, _ := params["Name"].(string)
	if name == "" {
		name = "default"
	}
	bus, err := p.store.GetEventBus(name, defaultAccountID)
	if err != nil {
		return ebError("ResourceNotFoundException", "Event bus "+name+" does not exist.", http.StatusBadRequest), nil
	}
	return jsonResp(http.StatusOK, map[string]any{
		"Name": bus.Name,
		"Arn":  bus.ARN,
	})
}

func (p *Provider) describeRule(params map[string]any) (*plugin.Response, error) {
	name, _ := params["Name"].(string)
	if name == "" {
		return ebError("InvalidParameterException", "Name is required", http.StatusBadRequest), nil
	}
	busName, _ := params["EventBusName"].(string)
	if busName == "" {
		busName = "default"
	}
	rule, err := p.store.GetRule(name, busName, defaultAccountID)
	if err != nil {
		return ebError("ResourceNotFoundException", "Rule "+name+" does not exist.", http.StatusBadRequest), nil
	}
	arn := "arn:aws:events:us-east-1:" + defaultAccountID + ":rule/" + rule.Name
	result := map[string]any{
		"Name":         rule.Name,
		"Arn":          arn,
		"State":        rule.State,
		"EventBusName": rule.BusName,
	}
	if rule.ScheduleExpression != "" {
		result["ScheduleExpression"] = rule.ScheduleExpression
	}
	return jsonResp(http.StatusOK, result)
}

func (p *Provider) putEvents(params map[string]any) (*plugin.Response, error) {
	entriesRaw, _ := params["Entries"].([]any)
	failedCount := 0
	resultEntries := make([]map[string]any, 0, len(entriesRaw))

	for _, e := range entriesRaw {
		entry, ok := e.(map[string]any)
		if !ok {
			failedCount++
			continue
		}
		busName, _ := entry["EventBusName"].(string)
		if busName == "" {
			busName = "default"
		}
		// Match against stored rules and dispatch to their targets.
		matchedRules, err := p.store.MatchingRules(busName, defaultAccountID, entry)
		if err != nil {
			failedCount++
			continue
		}
		eventID := randomID(16)
		// For each matched rule, dispatch event to targets asynchronously.
		if p.serverPort > 0 {
			eventJSON, _ := json.Marshal(entry)
			for _, rule := range matchedRules {
				targets, _ := p.store.ListTargetsByRule(rule.Name, busName, defaultAccountID)
				for _, target := range targets {
					go p.dispatchToTarget(target.ARN, eventJSON)
				}
			}
		}
		_ = nowUnix() // available for timestamping events
		resultEntries = append(resultEntries, map[string]any{"EventId": eventID})
	}

	return jsonResp(http.StatusOK, map[string]any{
		"FailedEntryCount": failedCount,
		"Entries":          resultEntries,
	})
}

// dispatchToTarget sends an event to the given target ARN via internal HTTP.
func (p *Provider) dispatchToTarget(targetARN string, eventJSON []byte) {
	base := fmt.Sprintf("http://localhost:%d/", p.serverPort)

	if strings.HasPrefix(targetARN, "arn:aws:sqs:") {
		// Extract queue name from ARN: arn:aws:sqs:region:account:QUEUE_NAME
		parts := strings.Split(targetARN, ":")
		if len(parts) < 6 {
			return
		}
		queueName := parts[len(parts)-1]
		accountID := parts[4]
		region := parts[3]
		queueURL := fmt.Sprintf("http://localhost:%d/%s/%s", p.serverPort, accountID, queueName)
		_ = region
		body, _ := json.Marshal(map[string]any{
			"QueueUrl":    queueURL,
			"MessageBody": string(eventJSON),
		})
		req, err := http.NewRequest("POST", base, bytes.NewReader(body))
		if err != nil {
			return
		}
		req.Header.Set("Content-Type", "application/x-amz-json-1.0")
		req.Header.Set("X-Amz-Target", "AmazonSQS.SendMessage")
		http.DefaultClient.Do(req) //nolint:errcheck
		return
	}

	if strings.HasPrefix(targetARN, "arn:aws:sns:") {
		body, _ := json.Marshal(map[string]any{
			"TopicArn": targetARN,
			"Message":  string(eventJSON),
		})
		req, err := http.NewRequest("POST", base, bytes.NewReader(body))
		if err != nil {
			return
		}
		req.Header.Set("Content-Type", "application/x-amz-json-1.0")
		req.Header.Set("X-Amz-Target", "AmazonSNS.Publish")
		http.DefaultClient.Do(req) //nolint:errcheck
		return
	}

	if strings.HasPrefix(targetARN, "arn:aws:lambda:") {
		// Extract function name: arn:aws:lambda:region:account:function:FUNCTION_NAME
		parts := strings.Split(targetARN, ":")
		if len(parts) < 7 {
			return
		}
		funcName := parts[len(parts)-1]
		url := fmt.Sprintf("http://localhost:%d/2015-03-31/functions/%s/invocations", p.serverPort, funcName)
		req, err := http.NewRequest("POST", url, bytes.NewReader(eventJSON))
		if err != nil {
			return
		}
		req.Header.Set("Content-Type", "application/json")
		http.DefaultClient.Do(req) //nolint:errcheck
		return
	}
}

// ──────────────────────────────────────────────
// Archive operations
// ──────────────────────────────────────────────

func (p *Provider) createArchive(params map[string]any) (*plugin.Response, error) {
	archiveName, _ := params["ArchiveName"].(string)
	if archiveName == "" {
		return ebError("InvalidParameterException", "ArchiveName is required", http.StatusBadRequest), nil
	}
	eventSourceARN, _ := params["EventSourceArn"].(string)
	if eventSourceARN == "" {
		return ebError("InvalidParameterException", "EventSourceArn is required", http.StatusBadRequest), nil
	}
	description, _ := params["Description"].(string)
	eventPattern, _ := params["EventPattern"].(string)
	var retentionDays int
	if rd, ok := params["RetentionDays"].(float64); ok {
		retentionDays = int(rd)
	}
	a := Archive{
		ArchiveName:    archiveName,
		EventSourceARN: eventSourceARN,
		Description:    description,
		EventPattern:   eventPattern,
		RetentionDays:  retentionDays,
		State:          "ENABLED",
		AccountID:      defaultAccountID,
		CreatedAt:      time.Now().UTC(),
	}
	if err := p.store.CreateArchive(a); err != nil {
		return nil, err
	}
	arn := "arn:aws:events:us-east-1:" + defaultAccountID + ":archive/" + archiveName
	return jsonResp(http.StatusOK, map[string]any{
		"ArchiveArn":   arn,
		"State":        a.State,
		"CreationTime": a.CreatedAt.Format(time.RFC3339),
	})
}

func (p *Provider) describeArchive(params map[string]any) (*plugin.Response, error) {
	archiveName, _ := params["ArchiveName"].(string)
	if archiveName == "" {
		return ebError("InvalidParameterException", "ArchiveName is required", http.StatusBadRequest), nil
	}
	a, err := p.store.GetArchive(archiveName, defaultAccountID)
	if err != nil {
		return ebError("ResourceNotFoundException", "archive not found", http.StatusBadRequest), nil
	}
	arn := "arn:aws:events:us-east-1:" + defaultAccountID + ":archive/" + a.ArchiveName
	return jsonResp(http.StatusOK, map[string]any{
		"ArchiveName":    a.ArchiveName,
		"ArchiveArn":     arn,
		"EventSourceArn": a.EventSourceARN,
		"Description":    a.Description,
		"EventPattern":   a.EventPattern,
		"RetentionDays":  a.RetentionDays,
		"State":          a.State,
		"CreationTime":   a.CreatedAt.Format(time.RFC3339),
	})
}

func (p *Provider) listArchives(params map[string]any) (*plugin.Response, error) {
	namePrefix, _ := params["NamePrefix"].(string)
	eventSourceARN, _ := params["EventSourceArn"].(string)
	archives, err := p.store.ListArchives(namePrefix, eventSourceARN, defaultAccountID)
	if err != nil {
		return nil, err
	}
	list := make([]map[string]any, 0, len(archives))
	for _, a := range archives {
		arn := "arn:aws:events:us-east-1:" + defaultAccountID + ":archive/" + a.ArchiveName
		list = append(list, map[string]any{
			"ArchiveName":    a.ArchiveName,
			"ArchiveArn":     arn,
			"EventSourceArn": a.EventSourceARN,
			"State":          a.State,
			"RetentionDays":  a.RetentionDays,
			"CreationTime":   a.CreatedAt.Format(time.RFC3339),
		})
	}
	return jsonResp(http.StatusOK, map[string]any{"Archives": list})
}

func (p *Provider) updateArchive(params map[string]any) (*plugin.Response, error) {
	archiveName, _ := params["ArchiveName"].(string)
	if archiveName == "" {
		return ebError("InvalidParameterException", "ArchiveName is required", http.StatusBadRequest), nil
	}
	// Get current values for fields not provided.
	existing, err := p.store.GetArchive(archiveName, defaultAccountID)
	if err != nil {
		return ebError("ResourceNotFoundException", "archive not found", http.StatusBadRequest), nil
	}
	description := existing.Description
	if d, ok := params["Description"].(string); ok {
		description = d
	}
	eventPattern := existing.EventPattern
	if ep, ok := params["EventPattern"].(string); ok {
		eventPattern = ep
	}
	retentionDays := existing.RetentionDays
	if rd, ok := params["RetentionDays"].(float64); ok {
		retentionDays = int(rd)
	}
	if err := p.store.UpdateArchive(archiveName, defaultAccountID, description, eventPattern, retentionDays); err != nil {
		return nil, err
	}
	arn := "arn:aws:events:us-east-1:" + defaultAccountID + ":archive/" + archiveName
	return jsonResp(http.StatusOK, map[string]any{
		"ArchiveArn": arn,
		"State":      existing.State,
	})
}

func (p *Provider) deleteArchive(params map[string]any) (*plugin.Response, error) {
	archiveName, _ := params["ArchiveName"].(string)
	if archiveName == "" {
		return ebError("InvalidParameterException", "ArchiveName is required", http.StatusBadRequest), nil
	}
	if err := p.store.DeleteArchive(archiveName, defaultAccountID); err != nil {
		return ebError("ResourceNotFoundException", "archive not found", http.StatusBadRequest), nil
	}
	return jsonResp(http.StatusOK, map[string]any{})
}

// ──────────────────────────────────────────────
// Replay operations
// ──────────────────────────────────────────────

func (p *Provider) startReplay(params map[string]any) (*plugin.Response, error) {
	replayName, _ := params["ReplayName"].(string)
	if replayName == "" {
		return ebError("InvalidParameterException", "ReplayName is required", http.StatusBadRequest), nil
	}
	eventSourceARN, _ := params["EventSourceArn"].(string)
	if eventSourceARN == "" {
		return ebError("InvalidParameterException", "EventSourceArn is required", http.StatusBadRequest), nil
	}

	// Extract archive name from EventSourceArn (arn:aws:events:...:archive/NAME).
	archiveName := ""
	if idx := strings.LastIndex(eventSourceARN, "/"); idx >= 0 {
		archiveName = eventSourceARN[idx+1:]
	}

	// Parse destination.
	destinationStr := ""
	if dest, ok := params["Destination"].(map[string]any); ok {
		destBytes, _ := json.Marshal(dest)
		destinationStr = string(destBytes)
	}

	// Parse timestamps (can be float64 unix seconds from boto3).
	var startTime, endTime *time.Time
	if s, ok := params["EventStartTime"].(float64); ok {
		t := time.Unix(int64(s), 0).UTC()
		startTime = &t
	}
	if e, ok := params["EventEndTime"].(float64); ok {
		t := time.Unix(int64(e), 0).UTC()
		endTime = &t
	}

	r := Replay{
		ReplayName:     replayName,
		ArchiveName:    archiveName,
		EventSourceARN: eventSourceARN,
		Destination:    destinationStr,
		EventStartTime: startTime,
		EventEndTime:   endTime,
		State:          "COMPLETED",
		AccountID:      defaultAccountID,
		CreatedAt:      time.Now().UTC(),
	}
	if err := p.store.CreateReplay(r); err != nil {
		return nil, err
	}
	arn := "arn:aws:events:us-east-1:" + defaultAccountID + ":replay/" + replayName
	return jsonResp(http.StatusOK, map[string]any{
		"ReplayArn":       arn,
		"State":           r.State,
		"ReplayStartTime": r.CreatedAt.Format(time.RFC3339),
	})
}

func (p *Provider) describeReplay(params map[string]any) (*plugin.Response, error) {
	replayName, _ := params["ReplayName"].(string)
	if replayName == "" {
		return ebError("InvalidParameterException", "ReplayName is required", http.StatusBadRequest), nil
	}
	r, err := p.store.GetReplay(replayName, defaultAccountID)
	if err != nil {
		return ebError("ResourceNotFoundException", "replay not found", http.StatusBadRequest), nil
	}
	arn := "arn:aws:events:us-east-1:" + defaultAccountID + ":replay/" + r.ReplayName
	result := map[string]any{
		"ReplayName":      r.ReplayName,
		"ReplayArn":       arn,
		"EventSourceArn":  r.EventSourceARN,
		"State":           r.State,
		"ReplayStartTime": r.CreatedAt.Format(time.RFC3339),
	}
	var dest map[string]any
	if r.Destination != "" && json.Unmarshal([]byte(r.Destination), &dest) == nil {
		result["Destination"] = dest
	}
	return jsonResp(http.StatusOK, result)
}

func (p *Provider) listReplays(params map[string]any) (*plugin.Response, error) {
	namePrefix, _ := params["NamePrefix"].(string)
	eventSourceARN, _ := params["EventSourceArn"].(string)
	replays, err := p.store.ListReplays(namePrefix, eventSourceARN, defaultAccountID)
	if err != nil {
		return nil, err
	}
	list := make([]map[string]any, 0, len(replays))
	for _, r := range replays {
		arn := "arn:aws:events:us-east-1:" + defaultAccountID + ":replay/" + r.ReplayName
		list = append(list, map[string]any{
			"ReplayName":      r.ReplayName,
			"ReplayArn":       arn,
			"EventSourceArn":  r.EventSourceARN,
			"State":           r.State,
			"ReplayStartTime": r.CreatedAt.Format(time.RFC3339),
		})
	}
	return jsonResp(http.StatusOK, map[string]any{"Replays": list})
}

func (p *Provider) cancelReplay(params map[string]any) (*plugin.Response, error) {
	replayName, _ := params["ReplayName"].(string)
	if replayName == "" {
		return ebError("InvalidParameterException", "ReplayName is required", http.StatusBadRequest), nil
	}
	if err := p.store.CancelReplay(replayName, defaultAccountID); err != nil {
		return ebError("ResourceNotFoundException", "replay not found", http.StatusBadRequest), nil
	}
	arn := "arn:aws:events:us-east-1:" + defaultAccountID + ":replay/" + replayName
	return jsonResp(http.StatusOK, map[string]any{
		"ReplayArn": arn,
		"State":     "CANCELLED",
	})
}

// ──────────────────────────────────────────────
// TestEventPattern
// ──────────────────────────────────────────────

func (p *Provider) testEventPattern(params map[string]any) (*plugin.Response, error) {
	patternStr, _ := params["EventPattern"].(string)
	eventStr, _ := params["Event"].(string)
	if patternStr == "" || eventStr == "" {
		return ebError("InvalidParameterException", "EventPattern and Event are required", http.StatusBadRequest), nil
	}
	var pattern map[string]any
	if err := json.Unmarshal([]byte(patternStr), &pattern); err != nil {
		return ebError("InvalidParameterException", "invalid EventPattern JSON", http.StatusBadRequest), nil
	}
	var event map[string]any
	if err := json.Unmarshal([]byte(eventStr), &event); err != nil {
		return ebError("InvalidParameterException", "invalid Event JSON", http.StatusBadRequest), nil
	}
	result := matchesPattern(pattern, event)
	return jsonResp(http.StatusOK, map[string]any{"Result": result})
}

// matchesPattern evaluates whether an event matches a pattern.
// It checks each key in the pattern against the event. For array values
// (as used in EventBridge patterns), the event field must equal one of the values.
func matchesPattern(pattern map[string]any, event map[string]any) bool {
	for key, patternVal := range pattern {
		eventVal, exists := event[key]
		if !exists {
			return false
		}
		switch pv := patternVal.(type) {
		case []any:
			// Pattern value is a list of allowed values — event must match one.
			matched := false
			for _, allowed := range pv {
				if fmt.Sprintf("%v", allowed) == fmt.Sprintf("%v", eventVal) {
					matched = true
					break
				}
			}
			if !matched {
				return false
			}
		case map[string]any:
			// Nested pattern — recurse if event value is also a map.
			if evMap, ok := eventVal.(map[string]any); ok {
				if !matchesPattern(pv, evMap) {
					return false
				}
			} else {
				return false
			}
		default:
			if fmt.Sprintf("%v", pv) != fmt.Sprintf("%v", eventVal) {
				return false
			}
		}
	}
	return true
}

func ebError(code, message string, status int) *plugin.Response {
	body, _ := json.Marshal(map[string]any{"__type": code, "message": message})
	return &plugin.Response{StatusCode: status, ContentType: "application/x-amz-json-1.1", Body: body}
}

func jsonResp(status int, v any) (*plugin.Response, error) {
	body, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	return &plugin.Response{StatusCode: status, ContentType: "application/x-amz-json-1.1", Body: body}, nil
}
