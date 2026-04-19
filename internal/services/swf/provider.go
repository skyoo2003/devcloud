// SPDX-License-Identifier: Apache-2.0

// internal/services/swf/provider.go
package swf

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

// Provider implements the Amazon SWF (SimpleWorkflowService) service.
type Provider struct {
	store *Store
}

func (p *Provider) ServiceID() string             { return "swf" }
func (p *Provider) ServiceName() string           { return "SimpleWorkflowService" }
func (p *Provider) Protocol() plugin.ProtocolType { return plugin.ProtocolJSON10 }

func (p *Provider) Init(cfg plugin.PluginConfig) error {
	dataDir := cfg.DataDir
	if dataDir == "" {
		dataDir = "."
	}
	var err error
	p.store, err = NewStore(filepath.Join(dataDir, "swf"))
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
	// Domain
	case "RegisterDomain":
		return p.registerDomain(params)
	case "DescribeDomain":
		return p.describeDomain(params)
	case "ListDomains":
		return p.listDomains(params)
	case "DeprecateDomain":
		return p.deprecateDomain(params)
	case "UndeprecateDomain":
		return p.undeprecateDomain(params)
	// WorkflowType
	case "RegisterWorkflowType":
		return p.registerWorkflowType(params)
	case "DescribeWorkflowType":
		return p.describeWorkflowType(params)
	case "ListWorkflowTypes":
		return p.listWorkflowTypes(params)
	case "DeprecateWorkflowType":
		return p.deprecateWorkflowType(params)
	case "UndeprecateWorkflowType":
		return p.undeprecateWorkflowType(params)
	case "DeleteWorkflowType":
		return p.deleteWorkflowType(params)
	// ActivityType
	case "RegisterActivityType":
		return p.registerActivityType(params)
	case "DescribeActivityType":
		return p.describeActivityType(params)
	case "ListActivityTypes":
		return p.listActivityTypes(params)
	case "DeprecateActivityType":
		return p.deprecateActivityType(params)
	case "UndeprecateActivityType":
		return p.undeprecateActivityType(params)
	case "DeleteActivityType":
		return p.deleteActivityType(params)
	// WorkflowExecution
	case "StartWorkflowExecution":
		return p.startWorkflowExecution(params)
	case "DescribeWorkflowExecution":
		return p.describeWorkflowExecution(params)
	case "ListOpenWorkflowExecutions":
		return p.listOpenWorkflowExecutions(params)
	case "ListClosedWorkflowExecutions":
		return p.listClosedWorkflowExecutions(params)
	case "TerminateWorkflowExecution":
		return p.terminateWorkflowExecution(params)
	case "RequestCancelWorkflowExecution":
		return p.requestCancelWorkflowExecution(params)
	case "SignalWorkflowExecution":
		return p.signalWorkflowExecution(params)
	case "GetWorkflowExecutionHistory":
		return p.getWorkflowExecutionHistory(params)
	// Counts
	case "CountOpenWorkflowExecutions":
		return p.countOpenWorkflowExecutions(params)
	case "CountClosedWorkflowExecutions":
		return p.countClosedWorkflowExecutions(params)
	case "CountPendingActivityTasks":
		return p.countPendingActivityTasks(params)
	case "CountPendingDecisionTasks":
		return p.countPendingDecisionTasks(params)
	// Polling
	case "PollForActivityTask":
		return p.pollForActivityTask(params)
	case "PollForDecisionTask":
		return p.pollForDecisionTask(params)
	// Task completion
	case "RespondActivityTaskCompleted":
		return p.respondActivityTaskCompleted(params)
	case "RespondActivityTaskFailed":
		return p.respondActivityTaskFailed(params)
	case "RespondActivityTaskCanceled":
		return p.respondActivityTaskCanceled(params)
	case "RespondDecisionTaskCompleted":
		return p.respondDecisionTaskCompleted(params)
	case "RecordActivityTaskHeartbeat":
		return p.recordActivityTaskHeartbeat(params)
	// Tags
	case "TagResource":
		return p.tagResource(params)
	case "UntagResource":
		return p.untagResource(params)
	case "ListTagsForResource":
		return p.listTagsForResource(params)
	default:
		return json10Err("InvalidAction", fmt.Sprintf("unknown action: %s", action), http.StatusBadRequest), nil
	}
}

func (p *Provider) ListResources(_ context.Context) ([]plugin.Resource, error) {
	domains, err := p.store.ListDomains("")
	if err != nil {
		return nil, err
	}
	res := make([]plugin.Resource, 0, len(domains))
	for _, d := range domains {
		res = append(res, plugin.Resource{Type: "domain", ID: d.ARN, Name: d.Name})
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

func buildDomainARN(name string) string {
	return fmt.Sprintf("arn:aws:swf:%s:%s:/domain/%s",
		shared.DefaultRegion, shared.DefaultAccountID, name)
}

func parseTags(rawTags []any) map[string]string {
	tags := make(map[string]string)
	for _, t := range rawTags {
		tag, _ := t.(map[string]any)
		k, _ := tag["key"].(string)
		v, _ := tag["value"].(string)
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

func strParam(params map[string]any, key string) string {
	v, _ := params[key].(string)
	return v
}

func typeInfoParam(params map[string]any, key string) (name, version string) {
	m, _ := params[key].(map[string]any)
	if m == nil {
		return "", ""
	}
	name, _ = m["name"].(string)
	version, _ = m["version"].(string)
	return name, version
}

// ---- Domain handlers ----

func (p *Provider) registerDomain(params map[string]any) (*plugin.Response, error) {
	name := strParam(params, "name")
	if name == "" {
		return json10Err("ValidationException", "name is required", http.StatusBadRequest), nil
	}
	description := strParam(params, "description")
	retention := strParam(params, "workflowExecutionRetentionPeriodInDays")
	if retention == "" {
		retention = "30"
	}
	now := time.Now()
	d := &Domain{
		Name:        name,
		ARN:         buildDomainARN(name),
		Status:      "REGISTERED",
		Description: description,
		Retention:   retention,
		CreatedAt:   now,
	}
	if err := p.store.CreateDomain(d); err != nil {
		if strings.Contains(err.Error(), "UNIQUE constraint failed") {
			return json10Err("DomainAlreadyExistsFault", "domain already exists: "+name, http.StatusBadRequest), nil
		}
		return nil, err
	}
	if rawTags, ok := params["tags"].([]any); ok {
		_ = p.store.tags.AddTags(d.ARN, parseTags(rawTags))
	}
	return json10Resp(http.StatusOK, map[string]any{})
}

func (p *Provider) describeDomain(params map[string]any) (*plugin.Response, error) {
	name := strParam(params, "name")
	if name == "" {
		return json10Err("ValidationException", "name is required", http.StatusBadRequest), nil
	}
	d, err := p.store.GetDomain(name)
	if err != nil {
		return json10Err("UnknownResourceFault", "domain not found: "+name, http.StatusBadRequest), nil
	}
	return json10Resp(http.StatusOK, domainToMap(d))
}

func (p *Provider) listDomains(params map[string]any) (*plugin.Response, error) {
	statusFilter := strParam(params, "registrationStatus")
	domains, err := p.store.ListDomains(statusFilter)
	if err != nil {
		return nil, err
	}
	items := make([]map[string]any, 0, len(domains))
	for _, d := range domains {
		dm := domainToMap(&d)
		info, _ := dm["domainInfo"].(map[string]any)
		items = append(items, info)
	}
	return json10Resp(http.StatusOK, map[string]any{
		"domainInfos": items,
	})
}

func (p *Provider) deprecateDomain(params map[string]any) (*plugin.Response, error) {
	name := strParam(params, "name")
	if name == "" {
		return json10Err("ValidationException", "name is required", http.StatusBadRequest), nil
	}
	d, err := p.store.GetDomain(name)
	if err != nil {
		return json10Err("UnknownResourceFault", "domain not found: "+name, http.StatusBadRequest), nil
	}
	if d.Status == "DEPRECATED" {
		return json10Err("DomainDeprecatedFault", "domain already deprecated: "+name, http.StatusBadRequest), nil
	}
	if err := p.store.UpdateDomainStatus(name, "DEPRECATED"); err != nil {
		return nil, err
	}
	return json10Resp(http.StatusOK, map[string]any{})
}

func (p *Provider) undeprecateDomain(params map[string]any) (*plugin.Response, error) {
	name := strParam(params, "name")
	if name == "" {
		return json10Err("ValidationException", "name is required", http.StatusBadRequest), nil
	}
	if _, err := p.store.GetDomain(name); err != nil {
		return json10Err("UnknownResourceFault", "domain not found: "+name, http.StatusBadRequest), nil
	}
	if err := p.store.UpdateDomainStatus(name, "REGISTERED"); err != nil {
		return nil, err
	}
	return json10Resp(http.StatusOK, map[string]any{})
}

// ---- WorkflowType handlers ----

func (p *Provider) registerWorkflowType(params map[string]any) (*plugin.Response, error) {
	domain := strParam(params, "domain")
	name := strParam(params, "name")
	version := strParam(params, "version")
	if domain == "" || name == "" || version == "" {
		return json10Err("ValidationException", "domain, name, version are required", http.StatusBadRequest), nil
	}
	now := time.Now()
	wt := &WorkflowType{
		Domain:         domain,
		Name:           name,
		Version:        version,
		Status:         "REGISTERED",
		Description:    strParam(params, "description"),
		DefaultTimeout: "NONE",
		CreatedAt:      now,
	}
	if err := p.store.CreateWorkflowType(wt); err != nil {
		if strings.Contains(err.Error(), "UNIQUE constraint failed") {
			return json10Err("TypeAlreadyExistsFault", "workflow type already exists", http.StatusBadRequest), nil
		}
		return nil, err
	}
	return json10Resp(http.StatusOK, map[string]any{})
}

func (p *Provider) describeWorkflowType(params map[string]any) (*plugin.Response, error) {
	domain := strParam(params, "domain")
	name, version := typeInfoParam(params, "workflowType")
	if domain == "" || name == "" || version == "" {
		return json10Err("ValidationException", "domain and workflowType are required", http.StatusBadRequest), nil
	}
	wt, err := p.store.GetWorkflowType(domain, name, version)
	if err != nil {
		return json10Err("UnknownResourceFault", "workflow type not found", http.StatusBadRequest), nil
	}
	return json10Resp(http.StatusOK, workflowTypeToMap(wt))
}

func (p *Provider) listWorkflowTypes(params map[string]any) (*plugin.Response, error) {
	domain := strParam(params, "domain")
	if domain == "" {
		return json10Err("ValidationException", "domain is required", http.StatusBadRequest), nil
	}
	statusFilter := strParam(params, "registrationStatus")
	wts, err := p.store.ListWorkflowTypes(domain, statusFilter)
	if err != nil {
		return nil, err
	}
	items := make([]map[string]any, 0, len(wts))
	for _, wt := range wts {
		items = append(items, workflowTypeToMap(&wt))
	}
	return json10Resp(http.StatusOK, map[string]any{
		"typeInfos": items,
	})
}

func (p *Provider) deprecateWorkflowType(params map[string]any) (*plugin.Response, error) {
	domain := strParam(params, "domain")
	name, version := typeInfoParam(params, "workflowType")
	if domain == "" || name == "" || version == "" {
		return json10Err("ValidationException", "domain and workflowType are required", http.StatusBadRequest), nil
	}
	wt, err := p.store.GetWorkflowType(domain, name, version)
	if err != nil {
		return json10Err("UnknownResourceFault", "workflow type not found", http.StatusBadRequest), nil
	}
	if wt.Status == "DEPRECATED" {
		return json10Err("TypeDeprecatedFault", "workflow type already deprecated", http.StatusBadRequest), nil
	}
	if err := p.store.UpdateWorkflowTypeStatus(domain, name, version, "DEPRECATED"); err != nil {
		return nil, err
	}
	return json10Resp(http.StatusOK, map[string]any{})
}

func (p *Provider) undeprecateWorkflowType(params map[string]any) (*plugin.Response, error) {
	domain := strParam(params, "domain")
	name, version := typeInfoParam(params, "workflowType")
	if domain == "" || name == "" || version == "" {
		return json10Err("ValidationException", "domain and workflowType are required", http.StatusBadRequest), nil
	}
	if _, err := p.store.GetWorkflowType(domain, name, version); err != nil {
		return json10Err("UnknownResourceFault", "workflow type not found", http.StatusBadRequest), nil
	}
	if err := p.store.UpdateWorkflowTypeStatus(domain, name, version, "REGISTERED"); err != nil {
		return nil, err
	}
	return json10Resp(http.StatusOK, map[string]any{})
}

func (p *Provider) deleteWorkflowType(params map[string]any) (*plugin.Response, error) {
	domain := strParam(params, "domain")
	name, version := typeInfoParam(params, "workflowType")
	if domain == "" || name == "" || version == "" {
		return json10Err("ValidationException", "domain and workflowType are required", http.StatusBadRequest), nil
	}
	wt, err := p.store.GetWorkflowType(domain, name, version)
	if err != nil {
		return json10Err("UnknownResourceFault", "workflow type not found", http.StatusBadRequest), nil
	}
	if wt.Status != "DEPRECATED" {
		return json10Err("TypeNotDeprecatedFault", "workflow type must be deprecated before deletion", http.StatusBadRequest), nil
	}
	if err := p.store.DeleteWorkflowType(domain, name, version); err != nil {
		return nil, err
	}
	return json10Resp(http.StatusOK, map[string]any{})
}

// ---- ActivityType handlers ----

func (p *Provider) registerActivityType(params map[string]any) (*plugin.Response, error) {
	domain := strParam(params, "domain")
	name := strParam(params, "name")
	version := strParam(params, "version")
	if domain == "" || name == "" || version == "" {
		return json10Err("ValidationException", "domain, name, version are required", http.StatusBadRequest), nil
	}
	now := time.Now()
	at := &ActivityType{
		Domain:         domain,
		Name:           name,
		Version:        version,
		Status:         "REGISTERED",
		Description:    strParam(params, "description"),
		DefaultTimeout: "NONE",
		CreatedAt:      now,
	}
	if err := p.store.CreateActivityType(at); err != nil {
		if strings.Contains(err.Error(), "UNIQUE constraint failed") {
			return json10Err("TypeAlreadyExistsFault", "activity type already exists", http.StatusBadRequest), nil
		}
		return nil, err
	}
	return json10Resp(http.StatusOK, map[string]any{})
}

func (p *Provider) describeActivityType(params map[string]any) (*plugin.Response, error) {
	domain := strParam(params, "domain")
	name, version := typeInfoParam(params, "activityType")
	if domain == "" || name == "" || version == "" {
		return json10Err("ValidationException", "domain and activityType are required", http.StatusBadRequest), nil
	}
	at, err := p.store.GetActivityType(domain, name, version)
	if err != nil {
		return json10Err("UnknownResourceFault", "activity type not found", http.StatusBadRequest), nil
	}
	return json10Resp(http.StatusOK, activityTypeToMap(at))
}

func (p *Provider) listActivityTypes(params map[string]any) (*plugin.Response, error) {
	domain := strParam(params, "domain")
	if domain == "" {
		return json10Err("ValidationException", "domain is required", http.StatusBadRequest), nil
	}
	statusFilter := strParam(params, "registrationStatus")
	ats, err := p.store.ListActivityTypes(domain, statusFilter)
	if err != nil {
		return nil, err
	}
	items := make([]map[string]any, 0, len(ats))
	for _, at := range ats {
		items = append(items, activityTypeToMap(&at))
	}
	return json10Resp(http.StatusOK, map[string]any{
		"typeInfos": items,
	})
}

func (p *Provider) deprecateActivityType(params map[string]any) (*plugin.Response, error) {
	domain := strParam(params, "domain")
	name, version := typeInfoParam(params, "activityType")
	if domain == "" || name == "" || version == "" {
		return json10Err("ValidationException", "domain and activityType are required", http.StatusBadRequest), nil
	}
	at, err := p.store.GetActivityType(domain, name, version)
	if err != nil {
		return json10Err("UnknownResourceFault", "activity type not found", http.StatusBadRequest), nil
	}
	if at.Status == "DEPRECATED" {
		return json10Err("TypeDeprecatedFault", "activity type already deprecated", http.StatusBadRequest), nil
	}
	if err := p.store.UpdateActivityTypeStatus(domain, name, version, "DEPRECATED"); err != nil {
		return nil, err
	}
	return json10Resp(http.StatusOK, map[string]any{})
}

func (p *Provider) undeprecateActivityType(params map[string]any) (*plugin.Response, error) {
	domain := strParam(params, "domain")
	name, version := typeInfoParam(params, "activityType")
	if domain == "" || name == "" || version == "" {
		return json10Err("ValidationException", "domain and activityType are required", http.StatusBadRequest), nil
	}
	if _, err := p.store.GetActivityType(domain, name, version); err != nil {
		return json10Err("UnknownResourceFault", "activity type not found", http.StatusBadRequest), nil
	}
	if err := p.store.UpdateActivityTypeStatus(domain, name, version, "REGISTERED"); err != nil {
		return nil, err
	}
	return json10Resp(http.StatusOK, map[string]any{})
}

func (p *Provider) deleteActivityType(params map[string]any) (*plugin.Response, error) {
	domain := strParam(params, "domain")
	name, version := typeInfoParam(params, "activityType")
	if domain == "" || name == "" || version == "" {
		return json10Err("ValidationException", "domain and activityType are required", http.StatusBadRequest), nil
	}
	at, err := p.store.GetActivityType(domain, name, version)
	if err != nil {
		return json10Err("UnknownResourceFault", "activity type not found", http.StatusBadRequest), nil
	}
	if at.Status != "DEPRECATED" {
		return json10Err("TypeNotDeprecatedFault", "activity type must be deprecated before deletion", http.StatusBadRequest), nil
	}
	if err := p.store.DeleteActivityType(domain, name, version); err != nil {
		return nil, err
	}
	return json10Resp(http.StatusOK, map[string]any{})
}

// ---- WorkflowExecution handlers ----

func (p *Provider) startWorkflowExecution(params map[string]any) (*plugin.Response, error) {
	domain := strParam(params, "domain")
	workflowID := strParam(params, "workflowId")
	if domain == "" || workflowID == "" {
		return json10Err("ValidationException", "domain and workflowId are required", http.StatusBadRequest), nil
	}
	wfName, wfVersion := typeInfoParam(params, "workflowType")
	input := strParam(params, "input")
	runID := shared.GenerateUUID()
	now := time.Now()

	tagList := "[]"
	if rawTags, ok := params["tagList"].([]any); ok {
		b, _ := json.Marshal(rawTags)
		tagList = string(b)
	}

	we := &WorkflowExecution{
		Domain:          domain,
		WorkflowID:      workflowID,
		RunID:           runID,
		WorkflowName:    wfName,
		WorkflowVersion: wfVersion,
		Status:          "OPEN",
		Input:           input,
		TagList:         tagList,
		StartTime:       now,
	}
	if err := p.store.CreateWorkflowExecution(we); err != nil {
		if strings.Contains(err.Error(), "UNIQUE constraint failed") {
			return json10Err("WorkflowExecutionAlreadyStartedFault", "workflow execution already started", http.StatusBadRequest), nil
		}
		return nil, err
	}
	return json10Resp(http.StatusOK, map[string]any{
		"runId": runID,
	})
}

func (p *Provider) describeWorkflowExecution(params map[string]any) (*plugin.Response, error) {
	domain := strParam(params, "domain")
	execMap, _ := params["execution"].(map[string]any)
	if domain == "" || execMap == nil {
		return json10Err("ValidationException", "domain and execution are required", http.StatusBadRequest), nil
	}
	workflowID, _ := execMap["workflowId"].(string)
	runID, _ := execMap["runId"].(string)
	if workflowID == "" || runID == "" {
		return json10Err("ValidationException", "execution.workflowId and execution.runId are required", http.StatusBadRequest), nil
	}
	we, err := p.store.GetWorkflowExecution(domain, workflowID, runID)
	if err != nil {
		return json10Err("UnknownResourceFault", "workflow execution not found", http.StatusBadRequest), nil
	}
	return json10Resp(http.StatusOK, executionToMap(we))
}

func (p *Provider) listOpenWorkflowExecutions(params map[string]any) (*plugin.Response, error) {
	domain := strParam(params, "domain")
	if domain == "" {
		return json10Err("ValidationException", "domain is required", http.StatusBadRequest), nil
	}
	execs, err := p.store.ListWorkflowExecutions(domain, "OPEN")
	if err != nil {
		return nil, err
	}
	items := make([]map[string]any, 0, len(execs))
	for _, we := range execs {
		items = append(items, executionToMap(&we))
	}
	return json10Resp(http.StatusOK, map[string]any{
		"executionInfos": items,
	})
}

func (p *Provider) listClosedWorkflowExecutions(params map[string]any) (*plugin.Response, error) {
	domain := strParam(params, "domain")
	if domain == "" {
		return json10Err("ValidationException", "domain is required", http.StatusBadRequest), nil
	}
	execs, err := p.store.ListWorkflowExecutions(domain, "CLOSED")
	if err != nil {
		return nil, err
	}
	items := make([]map[string]any, 0, len(execs))
	for _, we := range execs {
		items = append(items, executionToMap(&we))
	}
	return json10Resp(http.StatusOK, map[string]any{
		"executionInfos": items,
	})
}

func (p *Provider) terminateWorkflowExecution(params map[string]any) (*plugin.Response, error) {
	domain := strParam(params, "domain")
	workflowID := strParam(params, "workflowId")
	if domain == "" || workflowID == "" {
		return json10Err("ValidationException", "domain and workflowId are required", http.StatusBadRequest), nil
	}
	runID := strParam(params, "runId")
	var we *WorkflowExecution
	var err error
	if runID != "" {
		we, err = p.store.GetWorkflowExecution(domain, workflowID, runID)
	} else {
		we, err = p.store.GetLatestWorkflowExecution(domain, workflowID)
	}
	if err != nil {
		return json10Err("UnknownResourceFault", "workflow execution not found", http.StatusBadRequest), nil
	}
	if err := p.store.CloseWorkflowExecution(domain, workflowID, we.RunID, "TERMINATED", time.Now()); err != nil {
		return nil, err
	}
	return json10Resp(http.StatusOK, map[string]any{})
}

func (p *Provider) requestCancelWorkflowExecution(params map[string]any) (*plugin.Response, error) {
	domain := strParam(params, "domain")
	workflowID := strParam(params, "workflowId")
	if domain == "" || workflowID == "" {
		return json10Err("ValidationException", "domain and workflowId are required", http.StatusBadRequest), nil
	}
	runID := strParam(params, "runId")
	var we *WorkflowExecution
	var err error
	if runID != "" {
		we, err = p.store.GetWorkflowExecution(domain, workflowID, runID)
	} else {
		we, err = p.store.GetLatestWorkflowExecution(domain, workflowID)
	}
	if err != nil {
		return json10Err("UnknownResourceFault", "workflow execution not found", http.StatusBadRequest), nil
	}
	if err := p.store.CloseWorkflowExecution(domain, workflowID, we.RunID, "CANCELED", time.Now()); err != nil {
		return nil, err
	}
	return json10Resp(http.StatusOK, map[string]any{})
}

func (p *Provider) signalWorkflowExecution(_ map[string]any) (*plugin.Response, error) {
	return json10Resp(http.StatusOK, map[string]any{})
}

func (p *Provider) getWorkflowExecutionHistory(params map[string]any) (*plugin.Response, error) {
	domain := strParam(params, "domain")
	execMap, _ := params["execution"].(map[string]any)
	if domain == "" || execMap == nil {
		return json10Err("ValidationException", "domain and execution are required", http.StatusBadRequest), nil
	}
	workflowID, _ := execMap["workflowId"].(string)
	runID, _ := execMap["runId"].(string)
	we, err := p.store.GetWorkflowExecution(domain, workflowID, runID)
	if err != nil {
		return json10Err("UnknownResourceFault", "workflow execution not found", http.StatusBadRequest), nil
	}
	events := []map[string]any{
		{
			"eventId":        1,
			"eventType":      "WorkflowExecutionStarted",
			"eventTimestamp": we.StartTime.Unix(),
		},
		{
			"eventId":        2,
			"eventType":      "DecisionTaskScheduled",
			"eventTimestamp": we.StartTime.Unix(),
		},
	}
	return json10Resp(http.StatusOK, map[string]any{
		"events": events,
	})
}

// ---- Count handlers ----

func (p *Provider) countOpenWorkflowExecutions(params map[string]any) (*plugin.Response, error) {
	domain := strParam(params, "domain")
	if domain == "" {
		return json10Err("ValidationException", "domain is required", http.StatusBadRequest), nil
	}
	count, err := p.store.CountWorkflowExecutions(domain, "OPEN")
	if err != nil {
		return nil, err
	}
	return json10Resp(http.StatusOK, map[string]any{"count": count, "truncated": false})
}

func (p *Provider) countClosedWorkflowExecutions(params map[string]any) (*plugin.Response, error) {
	domain := strParam(params, "domain")
	if domain == "" {
		return json10Err("ValidationException", "domain is required", http.StatusBadRequest), nil
	}
	count, err := p.store.CountWorkflowExecutions(domain, "CLOSED")
	if err != nil {
		return nil, err
	}
	return json10Resp(http.StatusOK, map[string]any{"count": count, "truncated": false})
}

func (p *Provider) countPendingActivityTasks(_ map[string]any) (*plugin.Response, error) {
	return json10Resp(http.StatusOK, map[string]any{"count": 0, "truncated": false})
}

func (p *Provider) countPendingDecisionTasks(_ map[string]any) (*plugin.Response, error) {
	return json10Resp(http.StatusOK, map[string]any{"count": 0, "truncated": false})
}

// ---- Polling handlers ----

func (p *Provider) pollForActivityTask(_ map[string]any) (*plugin.Response, error) {
	return json10Resp(http.StatusOK, map[string]any{
		"taskToken": "",
	})
}

func (p *Provider) pollForDecisionTask(_ map[string]any) (*plugin.Response, error) {
	return json10Resp(http.StatusOK, map[string]any{
		"taskToken": "",
		"events":    []any{},
	})
}

// ---- Task completion handlers ----

func (p *Provider) respondActivityTaskCompleted(_ map[string]any) (*plugin.Response, error) {
	return json10Resp(http.StatusOK, map[string]any{})
}

func (p *Provider) respondActivityTaskFailed(_ map[string]any) (*plugin.Response, error) {
	return json10Resp(http.StatusOK, map[string]any{})
}

func (p *Provider) respondActivityTaskCanceled(_ map[string]any) (*plugin.Response, error) {
	return json10Resp(http.StatusOK, map[string]any{})
}

func (p *Provider) respondDecisionTaskCompleted(_ map[string]any) (*plugin.Response, error) {
	return json10Resp(http.StatusOK, map[string]any{})
}

func (p *Provider) recordActivityTaskHeartbeat(_ map[string]any) (*plugin.Response, error) {
	return json10Resp(http.StatusOK, map[string]any{"cancelRequested": false})
}

// ---- Tag handlers ----

func (p *Provider) tagResource(params map[string]any) (*plugin.Response, error) {
	arn := strParam(params, "resourceArn")
	if arn == "" {
		return json10Err("ValidationException", "resourceArn is required", http.StatusBadRequest), nil
	}
	if rawTags, ok := params["tags"].([]any); ok {
		_ = p.store.tags.AddTags(arn, parseTags(rawTags))
	}
	return json10Resp(http.StatusOK, map[string]any{})
}

func (p *Provider) untagResource(params map[string]any) (*plugin.Response, error) {
	arn := strParam(params, "resourceArn")
	if arn == "" {
		return json10Err("ValidationException", "resourceArn is required", http.StatusBadRequest), nil
	}
	if rawKeys, ok := params["tagKeys"].([]any); ok {
		keys := make([]string, 0, len(rawKeys))
		for _, k := range rawKeys {
			if s, ok := k.(string); ok {
				keys = append(keys, s)
			}
		}
		p.store.tags.RemoveTags(arn, keys) //nolint:errcheck
	}
	return json10Resp(http.StatusOK, map[string]any{})
}

func (p *Provider) listTagsForResource(params map[string]any) (*plugin.Response, error) {
	arn := strParam(params, "resourceArn")
	if arn == "" {
		return json10Err("ValidationException", "resourceArn is required", http.StatusBadRequest), nil
	}
	tagsMap, err := p.store.tags.ListTags(arn)
	if err != nil {
		return nil, err
	}
	items := make([]map[string]any, 0, len(tagsMap))
	for k, v := range tagsMap {
		items = append(items, map[string]any{"key": k, "value": v})
	}
	return json10Resp(http.StatusOK, map[string]any{"tags": items})
}

// ---- Serializers ----

func domainToMap(d *Domain) map[string]any {
	return map[string]any{
		"domainInfo": map[string]any{
			"name":        d.Name,
			"arn":         d.ARN,
			"status":      d.Status,
			"description": d.Description,
		},
		"configuration": map[string]any{
			"workflowExecutionRetentionPeriodInDays": d.Retention,
		},
	}
}

func workflowTypeToMap(wt *WorkflowType) map[string]any {
	return map[string]any{
		"typeInfo": map[string]any{
			"workflowType": map[string]any{
				"name":    wt.Name,
				"version": wt.Version,
			},
			"status":       wt.Status,
			"description":  wt.Description,
			"creationDate": wt.CreatedAt.Unix(),
			"domain":       wt.Domain,
		},
		"configuration": map[string]any{
			"defaultExecutionStartToCloseTimeout": wt.DefaultTimeout,
			"defaultTaskStartToCloseTimeout":      wt.DefaultTimeout,
		},
	}
}

func activityTypeToMap(at *ActivityType) map[string]any {
	return map[string]any{
		"typeInfo": map[string]any{
			"activityType": map[string]any{
				"name":    at.Name,
				"version": at.Version,
			},
			"status":       at.Status,
			"description":  at.Description,
			"creationDate": at.CreatedAt.Unix(),
			"domain":       at.Domain,
		},
		"configuration": map[string]any{
			"defaultTaskStartToCloseTimeout": at.DefaultTimeout,
			"defaultScheduleToCloseTimeout":  at.DefaultTimeout,
			"defaultScheduleToStartTimeout":  at.DefaultTimeout,
			"defaultHeartbeatTimeout":        at.DefaultTimeout,
		},
	}
}

func executionToMap(we *WorkflowExecution) map[string]any {
	m := map[string]any{
		"executionInfo": map[string]any{
			"execution": map[string]any{
				"workflowId": we.WorkflowID,
				"runId":      we.RunID,
			},
			"workflowType": map[string]any{
				"name":    we.WorkflowName,
				"version": we.WorkflowVersion,
			},
			"startTimestamp":  we.StartTime.Unix(),
			"executionStatus": we.Status,
			"closeStatus":     we.CloseStatus,
			"domain":          we.Domain,
		},
		"executionConfiguration": map[string]any{},
		"openCounts":             map[string]any{},
	}
	if !we.CloseTime.IsZero() {
		info := m["executionInfo"].(map[string]any)
		info["closeTimestamp"] = we.CloseTime.Unix()
	}
	return m
}
