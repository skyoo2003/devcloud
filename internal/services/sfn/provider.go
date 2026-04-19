// SPDX-License-Identifier: Apache-2.0

// internal/services/sfn/provider.go
package sfn

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
	"github.com/skyoo2003/devcloud/internal/storage/sqlite"
)

// Provider implements the AWS Step Functions (SFN) service.
type Provider struct {
	store *Store
}

func (p *Provider) ServiceID() string             { return "sfn" }
func (p *Provider) ServiceName() string           { return "StepFunctions" }
func (p *Provider) Protocol() plugin.ProtocolType { return plugin.ProtocolJSON10 }

func (p *Provider) Init(cfg plugin.PluginConfig) error {
	dataDir := cfg.DataDir
	if dataDir == "" {
		dataDir = "."
	}
	var err error
	p.store, err = NewStore(filepath.Join(dataDir, "sfn"))
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
		return json10Err("SerializationException", "failed to read body", http.StatusBadRequest), nil
	}
	var params map[string]any
	if len(body) > 0 {
		if err := json.Unmarshal(body, &params); err != nil {
			return json10Err("SerializationException", "invalid JSON", http.StatusBadRequest), nil
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
	// StateMachine
	case "CreateStateMachine":
		return p.createStateMachine(params)
	case "DescribeStateMachine":
		return p.describeStateMachine(params)
	case "ListStateMachines":
		return p.listStateMachines(params)
	case "DeleteStateMachine":
		return p.deleteStateMachine(params)
	case "UpdateStateMachine":
		return p.updateStateMachine(params)
	// StateMachine versions
	case "PublishStateMachineVersion":
		return p.publishStateMachineVersion(params)
	case "ListStateMachineVersions":
		return p.listStateMachineVersions(params)
	case "DeleteStateMachineVersion":
		return p.deleteStateMachineVersion(params)
	// StateMachine aliases
	case "CreateStateMachineAlias":
		return p.createStateMachineAlias(params)
	case "DescribeStateMachineAlias":
		return p.describeStateMachineAlias(params)
	case "ListStateMachineAliases":
		return p.listStateMachineAliases(params)
	case "UpdateStateMachineAlias":
		return p.updateStateMachineAlias(params)
	case "DeleteStateMachineAlias":
		return p.deleteStateMachineAlias(params)
	// Executions
	case "StartExecution":
		return p.startExecution(params)
	case "StartSyncExecution":
		return p.startSyncExecution(params)
	case "DescribeExecution":
		return p.describeExecution(params)
	case "ListExecutions":
		return p.listExecutions(params)
	case "StopExecution":
		return p.stopExecution(params)
	case "GetExecutionHistory":
		return p.getExecutionHistory(params)
	case "RedriveExecution":
		return p.redriveExecution(params)
	case "DescribeStateMachineForExecution":
		return p.describeStateMachineForExecution(params)
	// Activities
	case "CreateActivity":
		return p.createActivity(params)
	case "DescribeActivity":
		return p.describeActivity(params)
	case "ListActivities":
		return p.listActivities(params)
	case "DeleteActivity":
		return p.deleteActivity(params)
	case "GetActivityTask":
		return p.getActivityTask(params)
	case "SendTaskSuccess":
		return p.sendTaskSuccess(params)
	case "SendTaskFailure":
		return p.sendTaskFailure(params)
	case "SendTaskHeartbeat":
		return p.sendTaskHeartbeat(params)
	// Testing / Validation
	case "TestState":
		return p.testState(params)
	case "ValidateStateMachineDefinition":
		return p.validateStateMachineDefinition(params)
	// Tags
	case "TagResource":
		return p.tagResource(params)
	case "UntagResource":
		return p.untagResource(params)
	case "ListTagsForResource":
		return p.listTagsForResource(params)
	// MapRun (stub)
	case "DescribeMapRun":
		return p.describeMapRun(params)
	case "ListMapRuns":
		return p.listMapRuns(params)
	case "UpdateMapRun":
		return p.updateMapRun(params)
	default:
		return json10Err("InvalidAction", fmt.Sprintf("unknown action: %s", action), http.StatusBadRequest), nil
	}
}

func (p *Provider) ListResources(_ context.Context) ([]plugin.Resource, error) {
	sms, err := p.store.ListStateMachines()
	if err != nil {
		return nil, err
	}
	res := make([]plugin.Resource, 0, len(sms))
	for _, sm := range sms {
		res = append(res, plugin.Resource{Type: "state-machine", ID: sm.ARN, Name: sm.Name})
	}
	return res, nil
}

func (p *Provider) GetMetrics(_ context.Context) (*plugin.ServiceMetrics, error) {
	return &plugin.ServiceMetrics{}, nil
}

// ---- Helpers ----

func json10Resp(status int, v any) (*plugin.Response, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	return &plugin.Response{StatusCode: status, Body: b, ContentType: "application/x-amz-json-1.0"}, nil
}

func json10Err(code, message string, status int) *plugin.Response {
	b, _ := json.Marshal(map[string]string{"__type": code, "message": message})
	return &plugin.Response{StatusCode: status, Body: b, ContentType: "application/x-amz-json-1.0"}
}

func buildStateMachineARN(name string) string {
	return fmt.Sprintf("arn:aws:states:%s:%s:stateMachine:%s",
		shared.DefaultRegion, shared.DefaultAccountID, name)
}

func buildExecutionARN(stateMachineName, executionName string) string {
	return fmt.Sprintf("arn:aws:states:%s:%s:execution:%s:%s",
		shared.DefaultRegion, shared.DefaultAccountID, stateMachineName, executionName)
}

func buildActivityARN(name string) string {
	return fmt.Sprintf("arn:aws:states:%s:%s:activity:%s",
		shared.DefaultRegion, shared.DefaultAccountID, name)
}

func buildAliasARN(stateMachineName, aliasName string) string {
	return fmt.Sprintf("arn:aws:states:%s:%s:stateMachine:%s:%s",
		shared.DefaultRegion, shared.DefaultAccountID, stateMachineName, aliasName)
}

func parseTags(rawTags []any) map[string]string {
	tags := make(map[string]string)
	for _, t := range rawTags {
		tag, _ := t.(map[string]any)
		k, _ := tag["key"].(string)
		v, _ := tag["value"].(string)
		// Also support capitalised keys for compatibility
		if k == "" {
			k, _ = tag["Key"].(string)
		}
		if v == "" {
			v, _ = tag["Value"].(string)
		}
		if k != "" {
			tags[k] = v
		}
	}
	return tags
}

func smToMap(sm *StateMachine) map[string]any {
	return map[string]any{
		"stateMachineArn": sm.ARN,
		"name":            sm.Name,
		"definition":      sm.Definition,
		"roleArn":         sm.RoleARN,
		"type":            sm.Type,
		"status":          sm.Status,
		"revisionId":      sm.RevisionID,
		"creationDate":    sm.CreatedAt.Unix(),
	}
}

func execToMap(ex *Execution) map[string]any {
	m := map[string]any{
		"executionArn":    ex.ARN,
		"name":            ex.Name,
		"stateMachineArn": ex.StateMachine,
		"status":          ex.Status,
		"input":           ex.Input,
		"output":          ex.Output,
		"startDate":       ex.StartDate.Unix(),
	}
	if !ex.StopDate.IsZero() {
		m["stopDate"] = ex.StopDate.Unix()
	}
	return m
}

func actToMap(act *Activity) map[string]any {
	return map[string]any{
		"activityArn":  act.ARN,
		"name":         act.Name,
		"creationDate": act.CreatedAt.Unix(),
	}
}

func aliasToMap(a *StateMachineAlias) map[string]any {
	var rc any
	_ = json.Unmarshal([]byte(a.RoutingConfig), &rc)
	return map[string]any{
		"stateMachineAliasArn": a.ARN,
		"name":                 a.Name,
		"description":          a.Description,
		"routingConfiguration": rc,
		"creationDate":         a.CreatedAt.Unix(),
	}
}

// smNameFromARN extracts the state machine name from an ARN.
func smNameFromARN(arn string) string {
	// arn:aws:states:region:account:stateMachine:name
	parts := strings.Split(arn, ":")
	if len(parts) >= 7 {
		return parts[6]
	}
	return ""
}

// ---- StateMachine handlers ----

func (p *Provider) createStateMachine(params map[string]any) (*plugin.Response, error) {
	name, _ := params["name"].(string)
	if name == "" {
		return json10Err("ValidationException", "name is required", http.StatusBadRequest), nil
	}
	definition, _ := params["definition"].(string)
	if definition == "" {
		definition = "{}"
	}
	roleARN, _ := params["roleArn"].(string)
	smType, _ := params["type"].(string)
	if smType == "" {
		smType = "STANDARD"
	}
	arn := buildStateMachineARN(name)
	now := time.Now()
	sm := &StateMachine{
		ARN:        arn,
		Name:       name,
		Definition: definition,
		RoleARN:    roleARN,
		Type:       smType,
		Status:     "ACTIVE",
		RevisionID: shared.GenerateUUID(),
		Version:    1,
		CreatedAt:  now,
		UpdatedAt:  now,
	}
	if err := p.store.CreateStateMachine(sm); err != nil {
		if sqlite.IsUniqueConstraintError(err) {
			return json10Err("StateMachineAlreadyExists", "state machine already exists: "+name, http.StatusBadRequest), nil
		}
		return nil, err
	}
	// Handle tags
	if rawTags, ok := params["tags"].([]any); ok {
		_ = p.store.tags.AddTags(arn, parseTags(rawTags))
	}
	return json10Resp(http.StatusOK, map[string]any{
		"stateMachineArn": arn,
		"creationDate":    now.Unix(),
	})
}

func (p *Provider) describeStateMachine(params map[string]any) (*plugin.Response, error) {
	arn, _ := params["stateMachineArn"].(string)
	if arn == "" {
		return json10Err("ValidationException", "stateMachineArn is required", http.StatusBadRequest), nil
	}
	sm, err := p.store.GetStateMachineByARN(arn)
	if err != nil {
		return json10Err("StateMachineDoesNotExist", "state machine does not exist: "+arn, http.StatusBadRequest), nil
	}
	return json10Resp(http.StatusOK, smToMap(sm))
}

func (p *Provider) listStateMachines(_ map[string]any) (*plugin.Response, error) {
	sms, err := p.store.ListStateMachines()
	if err != nil {
		return nil, err
	}
	items := make([]map[string]any, 0, len(sms))
	for _, sm := range sms {
		items = append(items, map[string]any{
			"stateMachineArn": sm.ARN,
			"name":            sm.Name,
			"type":            sm.Type,
			"creationDate":    sm.CreatedAt.Unix(),
		})
	}
	return json10Resp(http.StatusOK, map[string]any{
		"stateMachines": items,
	})
}

func (p *Provider) deleteStateMachine(params map[string]any) (*plugin.Response, error) {
	arn, _ := params["stateMachineArn"].(string)
	if arn == "" {
		return json10Err("ValidationException", "stateMachineArn is required", http.StatusBadRequest), nil
	}
	if err := p.store.DeleteStateMachine(arn); err != nil {
		return json10Err("StateMachineDoesNotExist", "state machine does not exist: "+arn, http.StatusBadRequest), nil
	}
	_ = p.store.tags.DeleteAllTags(arn)
	return json10Resp(http.StatusOK, map[string]any{})
}

func (p *Provider) updateStateMachine(params map[string]any) (*plugin.Response, error) {
	arn, _ := params["stateMachineArn"].(string)
	if arn == "" {
		return json10Err("ValidationException", "stateMachineArn is required", http.StatusBadRequest), nil
	}
	sm, err := p.store.GetStateMachineByARN(arn)
	if err != nil {
		return json10Err("StateMachineDoesNotExist", "state machine does not exist: "+arn, http.StatusBadRequest), nil
	}
	if def, ok := params["definition"].(string); ok && def != "" {
		sm.Definition = def
	}
	if role, ok := params["roleArn"].(string); ok && role != "" {
		sm.RoleARN = role
	}
	sm.RevisionID = shared.GenerateUUID()
	sm.Version++
	now := time.Now()
	if err := p.store.UpdateStateMachine(arn, sm.Definition, sm.RoleARN, sm.RevisionID, sm.Version, now); err != nil {
		return nil, err
	}
	return json10Resp(http.StatusOK, map[string]any{
		"updateDate":             now.Unix(),
		"revisionId":             sm.RevisionID,
		"stateMachineVersionArn": arn,
	})
}

// ---- StateMachine version handlers (simplified) ----

func (p *Provider) publishStateMachineVersion(params map[string]any) (*plugin.Response, error) {
	arn, _ := params["stateMachineArn"].(string)
	if arn == "" {
		return json10Err("ValidationException", "stateMachineArn is required", http.StatusBadRequest), nil
	}
	sm, err := p.store.GetStateMachineByARN(arn)
	if err != nil {
		return json10Err("StateMachineDoesNotExist", "state machine does not exist: "+arn, http.StatusBadRequest), nil
	}
	versionARN := fmt.Sprintf("%s:%d", arn, sm.Version)
	return json10Resp(http.StatusOK, map[string]any{
		"creationDate":           sm.CreatedAt.Unix(),
		"stateMachineVersionArn": versionARN,
	})
}

func (p *Provider) listStateMachineVersions(params map[string]any) (*plugin.Response, error) {
	arn, _ := params["stateMachineArn"].(string)
	if arn == "" {
		return json10Err("ValidationException", "stateMachineArn is required", http.StatusBadRequest), nil
	}
	sm, err := p.store.GetStateMachineByARN(arn)
	if err != nil {
		return json10Err("StateMachineDoesNotExist", "state machine does not exist: "+arn, http.StatusBadRequest), nil
	}
	versions := []map[string]any{
		{
			"stateMachineVersionArn": fmt.Sprintf("%s:%d", arn, sm.Version),
			"creationDate":           sm.CreatedAt.Unix(),
		},
	}
	return json10Resp(http.StatusOK, map[string]any{
		"stateMachineVersions": versions,
	})
}

func (p *Provider) deleteStateMachineVersion(params map[string]any) (*plugin.Response, error) {
	// Simplified: versions are not actually stored separately
	versionARN, _ := params["stateMachineVersionArn"].(string)
	if versionARN == "" {
		return json10Err("ValidationException", "stateMachineVersionArn is required", http.StatusBadRequest), nil
	}
	return json10Resp(http.StatusOK, map[string]any{})
}

// ---- StateMachineAlias handlers ----

func (p *Provider) createStateMachineAlias(params map[string]any) (*plugin.Response, error) {
	name, _ := params["name"].(string)
	if name == "" {
		return json10Err("ValidationException", "name is required", http.StatusBadRequest), nil
	}
	// routing config required
	var routingJSON string
	if rc, ok := params["routingConfiguration"]; ok {
		b, _ := json.Marshal(rc)
		routingJSON = string(b)
	} else {
		routingJSON = "[]"
	}
	description, _ := params["description"].(string)

	// Find which state machine this alias belongs to - derive from routing config
	// routingConfiguration: [{"stateMachineVersionArn": "...", "weight": N}]
	var stateMachineARN string
	rcArr, _ := params["routingConfiguration"].([]any)
	if len(rcArr) > 0 {
		m, _ := rcArr[0].(map[string]any)
		versionARN, _ := m["stateMachineVersionArn"].(string)
		// Strip version suffix: arn:...:stateMachine:name:1 -> arn:...:stateMachine:name
		if idx := strings.LastIndex(versionARN, ":"); idx >= 0 {
			// Check if last segment is numeric
			last := versionARN[idx+1:]
			isNum := true
			for _, c := range last {
				if c < '0' || c > '9' {
					isNum = false
					break
				}
			}
			if isNum {
				stateMachineARN = versionARN[:idx]
			} else {
				stateMachineARN = versionARN
			}
		}
	}
	if stateMachineARN == "" {
		return json10Err("ValidationException", "routingConfiguration must reference a state machine version", http.StatusBadRequest), nil
	}

	smName := smNameFromARN(stateMachineARN)
	aliasARN := buildAliasARN(smName, name)
	now := time.Now()
	a := &StateMachineAlias{
		ARN:           aliasARN,
		Name:          name,
		StateMachine:  stateMachineARN,
		Description:   description,
		RoutingConfig: routingJSON,
		CreatedAt:     now,
		UpdatedAt:     now,
	}
	if err := p.store.CreateAlias(a); err != nil {
		if sqlite.IsUniqueConstraintError(err) {
			return json10Err("ConflictException", "alias already exists: "+name, http.StatusConflict), nil
		}
		return nil, err
	}
	return json10Resp(http.StatusOK, map[string]any{
		"creationDate":         now.Unix(),
		"stateMachineAliasArn": aliasARN,
	})
}

func (p *Provider) describeStateMachineAlias(params map[string]any) (*plugin.Response, error) {
	arn, _ := params["stateMachineAliasArn"].(string)
	if arn == "" {
		return json10Err("ValidationException", "stateMachineAliasArn is required", http.StatusBadRequest), nil
	}
	a, err := p.store.GetAliasByARN(arn)
	if err != nil {
		return json10Err("ResourceNotFound", "alias not found: "+arn, http.StatusNotFound), nil
	}
	return json10Resp(http.StatusOK, aliasToMap(a))
}

func (p *Provider) listStateMachineAliases(params map[string]any) (*plugin.Response, error) {
	smARN, _ := params["stateMachineArn"].(string)
	if smARN == "" {
		return json10Err("ValidationException", "stateMachineArn is required", http.StatusBadRequest), nil
	}
	aliases, err := p.store.ListAliases(smARN)
	if err != nil {
		return nil, err
	}
	items := make([]map[string]any, 0, len(aliases))
	for _, a := range aliases {
		items = append(items, aliasToMap(&a))
	}
	return json10Resp(http.StatusOK, map[string]any{
		"stateMachineAliases": items,
	})
}

func (p *Provider) updateStateMachineAlias(params map[string]any) (*plugin.Response, error) {
	arn, _ := params["stateMachineAliasArn"].(string)
	if arn == "" {
		return json10Err("ValidationException", "stateMachineAliasArn is required", http.StatusBadRequest), nil
	}
	a, err := p.store.GetAliasByARN(arn)
	if err != nil {
		return json10Err("ResourceNotFound", "alias not found: "+arn, http.StatusNotFound), nil
	}
	if desc, ok := params["description"].(string); ok {
		a.Description = desc
	}
	if rc, ok := params["routingConfiguration"]; ok {
		b, _ := json.Marshal(rc)
		a.RoutingConfig = string(b)
	}
	now := time.Now()
	if err := p.store.UpdateAlias(arn, a.Description, a.RoutingConfig, now); err != nil {
		return nil, err
	}
	return json10Resp(http.StatusOK, map[string]any{
		"updateDate": now.Unix(),
	})
}

func (p *Provider) deleteStateMachineAlias(params map[string]any) (*plugin.Response, error) {
	arn, _ := params["stateMachineAliasArn"].(string)
	if arn == "" {
		return json10Err("ValidationException", "stateMachineAliasArn is required", http.StatusBadRequest), nil
	}
	if err := p.store.DeleteAlias(arn); err != nil {
		return json10Err("ResourceNotFound", "alias not found: "+arn, http.StatusNotFound), nil
	}
	return json10Resp(http.StatusOK, map[string]any{})
}

// ---- Execution handlers ----

func (p *Provider) startExecution(params map[string]any) (*plugin.Response, error) {
	smARN, _ := params["stateMachineArn"].(string)
	if smARN == "" {
		return json10Err("ValidationException", "stateMachineArn is required", http.StatusBadRequest), nil
	}
	if _, err := p.store.GetStateMachineByARN(smARN); err != nil {
		return json10Err("StateMachineDoesNotExist", "state machine does not exist: "+smARN, http.StatusBadRequest), nil
	}
	input, _ := params["input"].(string)
	if input == "" {
		input = "{}"
	}
	name, _ := params["name"].(string)
	if name == "" {
		name = shared.GenerateUUID()
	}
	smName := smNameFromARN(smARN)
	execARN := buildExecutionARN(smName, name)
	now := time.Now()
	ex := &Execution{
		ARN:          execARN,
		Name:         name,
		StateMachine: smARN,
		Status:       "SUCCEEDED",
		Input:        input,
		Output:       "{}",
		StartDate:    now,
		StopDate:     now,
	}
	if err := p.store.CreateExecution(ex); err != nil {
		if sqlite.IsUniqueConstraintError(err) {
			return json10Err("ExecutionAlreadyExists", "execution already exists: "+name, http.StatusBadRequest), nil
		}
		return nil, err
	}
	return json10Resp(http.StatusOK, map[string]any{
		"executionArn": execARN,
		"startDate":    now.Unix(),
	})
}

func (p *Provider) startSyncExecution(params map[string]any) (*plugin.Response, error) {
	smARN, _ := params["stateMachineArn"].(string)
	if smARN == "" {
		return json10Err("ValidationException", "stateMachineArn is required", http.StatusBadRequest), nil
	}
	if _, err := p.store.GetStateMachineByARN(smARN); err != nil {
		return json10Err("StateMachineDoesNotExist", "state machine does not exist: "+smARN, http.StatusBadRequest), nil
	}
	input, _ := params["input"].(string)
	if input == "" {
		input = "{}"
	}
	name, _ := params["name"].(string)
	if name == "" {
		name = shared.GenerateUUID()
	}
	smName := smNameFromARN(smARN)
	execARN := buildExecutionARN(smName, name)
	now := time.Now()
	ex := &Execution{
		ARN:          execARN,
		Name:         name,
		StateMachine: smARN,
		Status:       "SUCCEEDED",
		Input:        input,
		Output:       "{}",
		StartDate:    now,
		StopDate:     now,
	}
	if err := p.store.CreateExecution(ex); err != nil {
		if sqlite.IsUniqueConstraintError(err) {
			return json10Err("ExecutionAlreadyExists", "execution already exists: "+name, http.StatusBadRequest), nil
		}
		return nil, err
	}
	return json10Resp(http.StatusOK, map[string]any{
		"executionArn":    execARN,
		"name":            name,
		"status":          "SUCCEEDED",
		"startDate":       now.Unix(),
		"stopDate":        now.Unix(),
		"input":           input,
		"output":          "{}",
		"stateMachineArn": smARN,
	})
}

func (p *Provider) describeExecution(params map[string]any) (*plugin.Response, error) {
	arn, _ := params["executionArn"].(string)
	if arn == "" {
		return json10Err("ValidationException", "executionArn is required", http.StatusBadRequest), nil
	}
	ex, err := p.store.GetExecution(arn)
	if err != nil {
		return json10Err("ExecutionDoesNotExist", "execution does not exist: "+arn, http.StatusBadRequest), nil
	}
	return json10Resp(http.StatusOK, execToMap(ex))
}

func (p *Provider) listExecutions(params map[string]any) (*plugin.Response, error) {
	smARN, _ := params["stateMachineArn"].(string)
	if smARN == "" {
		return json10Err("ValidationException", "stateMachineArn is required", http.StatusBadRequest), nil
	}
	statusFilter, _ := params["statusFilter"].(string)
	execs, err := p.store.ListExecutions(smARN, statusFilter)
	if err != nil {
		return nil, err
	}
	items := make([]map[string]any, 0, len(execs))
	for _, ex := range execs {
		items = append(items, map[string]any{
			"executionArn":    ex.ARN,
			"name":            ex.Name,
			"stateMachineArn": ex.StateMachine,
			"status":          ex.Status,
			"startDate":       ex.StartDate.Unix(),
		})
	}
	return json10Resp(http.StatusOK, map[string]any{
		"executions": items,
	})
}

func (p *Provider) stopExecution(params map[string]any) (*plugin.Response, error) {
	arn, _ := params["executionArn"].(string)
	if arn == "" {
		return json10Err("ValidationException", "executionArn is required", http.StatusBadRequest), nil
	}
	now := time.Now()
	if err := p.store.UpdateExecutionStatus(arn, "ABORTED", now); err != nil {
		return json10Err("ExecutionDoesNotExist", "execution does not exist: "+arn, http.StatusBadRequest), nil
	}
	return json10Resp(http.StatusOK, map[string]any{
		"stopDate": now.Unix(),
	})
}

func (p *Provider) getExecutionHistory(params map[string]any) (*plugin.Response, error) {
	arn, _ := params["executionArn"].(string)
	if arn == "" {
		return json10Err("ValidationException", "executionArn is required", http.StatusBadRequest), nil
	}
	ex, err := p.store.GetExecution(arn)
	if err != nil {
		return json10Err("ExecutionDoesNotExist", "execution does not exist: "+arn, http.StatusBadRequest), nil
	}
	events := []map[string]any{
		{
			"id":        1,
			"type":      "ExecutionStarted",
			"timestamp": ex.StartDate.Unix(),
			"executionStartedEventDetails": map[string]any{
				"input":   ex.Input,
				"roleArn": "",
			},
		},
		{
			"id":              2,
			"previousEventId": 1,
			"type":            "ExecutionSucceeded",
			"timestamp":       ex.StopDate.Unix(),
			"executionSucceededEventDetails": map[string]any{
				"output": ex.Output,
			},
		},
	}
	return json10Resp(http.StatusOK, map[string]any{
		"events": events,
	})
}

func (p *Provider) redriveExecution(params map[string]any) (*plugin.Response, error) {
	arn, _ := params["executionArn"].(string)
	if arn == "" {
		return json10Err("ValidationException", "executionArn is required", http.StatusBadRequest), nil
	}
	ex, err := p.store.GetExecution(arn)
	if err != nil {
		return json10Err("ExecutionDoesNotExist", "execution does not exist: "+arn, http.StatusBadRequest), nil
	}
	// Create a new execution entry with a new ARN
	smName := smNameFromARN(ex.StateMachine)
	newName := shared.GenerateUUID()
	newARN := buildExecutionARN(smName, newName)
	now := time.Now()
	newEx := &Execution{
		ARN:          newARN,
		Name:         newName,
		StateMachine: ex.StateMachine,
		Status:       "SUCCEEDED",
		Input:        ex.Input,
		Output:       "{}",
		StartDate:    now,
		StopDate:     now,
	}
	if err := p.store.CreateExecution(newEx); err != nil {
		return nil, err
	}
	return json10Resp(http.StatusOK, map[string]any{
		"redriveDate": now.Unix(),
	})
}

func (p *Provider) describeStateMachineForExecution(params map[string]any) (*plugin.Response, error) {
	execARN, _ := params["executionArn"].(string)
	if execARN == "" {
		return json10Err("ValidationException", "executionArn is required", http.StatusBadRequest), nil
	}
	ex, err := p.store.GetExecution(execARN)
	if err != nil {
		return json10Err("ExecutionDoesNotExist", "execution does not exist: "+execARN, http.StatusBadRequest), nil
	}
	sm, err := p.store.GetStateMachineByARN(ex.StateMachine)
	if err != nil {
		return json10Err("StateMachineDoesNotExist", "state machine does not exist", http.StatusBadRequest), nil
	}
	return json10Resp(http.StatusOK, smToMap(sm))
}

// ---- Activity handlers ----

func (p *Provider) createActivity(params map[string]any) (*plugin.Response, error) {
	name, _ := params["name"].(string)
	if name == "" {
		return json10Err("ValidationException", "name is required", http.StatusBadRequest), nil
	}
	arn := buildActivityARN(name)
	now := time.Now()
	act := &Activity{
		ARN:       arn,
		Name:      name,
		CreatedAt: now,
	}
	if err := p.store.CreateActivity(act); err != nil {
		if sqlite.IsUniqueConstraintError(err) {
			return json10Err("ActivityAlreadyExists", "activity already exists: "+name, http.StatusBadRequest), nil
		}
		return nil, err
	}
	if rawTags, ok := params["tags"].([]any); ok {
		_ = p.store.tags.AddTags(arn, parseTags(rawTags))
	}
	return json10Resp(http.StatusOK, map[string]any{
		"activityArn":  arn,
		"creationDate": now.Unix(),
	})
}

func (p *Provider) describeActivity(params map[string]any) (*plugin.Response, error) {
	arn, _ := params["activityArn"].(string)
	if arn == "" {
		return json10Err("ValidationException", "activityArn is required", http.StatusBadRequest), nil
	}
	act, err := p.store.GetActivityByARN(arn)
	if err != nil {
		return json10Err("ActivityDoesNotExist", "activity does not exist: "+arn, http.StatusBadRequest), nil
	}
	return json10Resp(http.StatusOK, actToMap(act))
}

func (p *Provider) listActivities(_ map[string]any) (*plugin.Response, error) {
	acts, err := p.store.ListActivities()
	if err != nil {
		return nil, err
	}
	items := make([]map[string]any, 0, len(acts))
	for _, act := range acts {
		items = append(items, actToMap(&act))
	}
	return json10Resp(http.StatusOK, map[string]any{
		"activities": items,
	})
}

func (p *Provider) deleteActivity(params map[string]any) (*plugin.Response, error) {
	arn, _ := params["activityArn"].(string)
	if arn == "" {
		return json10Err("ValidationException", "activityArn is required", http.StatusBadRequest), nil
	}
	if err := p.store.DeleteActivity(arn); err != nil {
		return json10Err("ActivityDoesNotExist", "activity does not exist: "+arn, http.StatusBadRequest), nil
	}
	_ = p.store.tags.DeleteAllTags(arn)
	return json10Resp(http.StatusOK, map[string]any{})
}

func (p *Provider) getActivityTask(params map[string]any) (*plugin.Response, error) {
	arn, _ := params["activityArn"].(string)
	if arn == "" {
		return json10Err("ValidationException", "activityArn is required", http.StatusBadRequest), nil
	}
	if _, err := p.store.GetActivityByARN(arn); err != nil {
		return json10Err("ActivityDoesNotExist", "activity does not exist: "+arn, http.StatusBadRequest), nil
	}
	return json10Resp(http.StatusOK, map[string]any{
		"taskToken": shared.GenerateUUID(),
		"input":     "{}",
	})
}

func (p *Provider) sendTaskSuccess(_ map[string]any) (*plugin.Response, error) {
	return json10Resp(http.StatusOK, map[string]any{})
}

func (p *Provider) sendTaskFailure(_ map[string]any) (*plugin.Response, error) {
	return json10Resp(http.StatusOK, map[string]any{})
}

func (p *Provider) sendTaskHeartbeat(_ map[string]any) (*plugin.Response, error) {
	return json10Resp(http.StatusOK, map[string]any{})
}

// ---- Testing / Validation ----

func (p *Provider) testState(_ map[string]any) (*plugin.Response, error) {
	return json10Resp(http.StatusOK, map[string]any{
		"status": "SUCCEEDED",
		"output": "{}",
	})
}

func (p *Provider) validateStateMachineDefinition(_ map[string]any) (*plugin.Response, error) {
	return json10Resp(http.StatusOK, map[string]any{
		"result":      "OK",
		"truncated":   false,
		"diagnostics": []any{},
	})
}

// ---- Tag handlers ----

func (p *Provider) tagResource(params map[string]any) (*plugin.Response, error) {
	arn, _ := params["resourceArn"].(string)
	if arn == "" {
		return json10Err("ValidationException", "resourceArn is required", http.StatusBadRequest), nil
	}
	rawTags, _ := params["tags"].([]any)
	if err := p.store.tags.AddTags(arn, parseTags(rawTags)); err != nil {
		return nil, err
	}
	return json10Resp(http.StatusOK, map[string]any{})
}

func (p *Provider) untagResource(params map[string]any) (*plugin.Response, error) {
	arn, _ := params["resourceArn"].(string)
	if arn == "" {
		return json10Err("ValidationException", "resourceArn is required", http.StatusBadRequest), nil
	}
	rawKeys, _ := params["tagKeys"].([]any)
	keys := make([]string, 0, len(rawKeys))
	for _, k := range rawKeys {
		if s, ok := k.(string); ok {
			keys = append(keys, s)
		}
	}
	if err := p.store.tags.RemoveTags(arn, keys); err != nil {
		return nil, err
	}
	return json10Resp(http.StatusOK, map[string]any{})
}

func (p *Provider) listTagsForResource(params map[string]any) (*plugin.Response, error) {
	arn, _ := params["resourceArn"].(string)
	if arn == "" {
		return json10Err("ValidationException", "resourceArn is required", http.StatusBadRequest), nil
	}
	tags, err := p.store.tags.ListTags(arn)
	if err != nil {
		return nil, err
	}
	tagList := make([]map[string]string, 0, len(tags))
	for k, v := range tags {
		tagList = append(tagList, map[string]string{"key": k, "value": v})
	}
	return json10Resp(http.StatusOK, map[string]any{
		"tags": tagList,
	})
}

// ---- MapRun stubs ----

func (p *Provider) describeMapRun(_ map[string]any) (*plugin.Response, error) {
	return json10Resp(http.StatusOK, map[string]any{})
}

func (p *Provider) listMapRuns(_ map[string]any) (*plugin.Response, error) {
	return json10Resp(http.StatusOK, map[string]any{
		"mapRuns": []any{},
	})
}

func (p *Provider) updateMapRun(_ map[string]any) (*plugin.Response, error) {
	return json10Resp(http.StatusOK, map[string]any{})
}
