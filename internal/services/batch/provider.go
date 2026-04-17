// SPDX-License-Identifier: Apache-2.0

// internal/services/batch/provider.go
package batch

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

// Provider implements the AWS Batch service.
type Provider struct {
	store *Store
}

func (p *Provider) ServiceID() string             { return "batch" }
func (p *Provider) ServiceName() string           { return "BatchV20160810" }
func (p *Provider) Protocol() plugin.ProtocolType { return plugin.ProtocolRESTJSON }

func (p *Provider) Init(cfg plugin.PluginConfig) error {
	dataDir := cfg.DataDir
	if dataDir == "" {
		dataDir = "."
	}
	var err error
	p.store, err = NewStore(filepath.Join(dataDir, "batch"))
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
		return shared.JSONError("ClientException", "failed to read body", http.StatusBadRequest), nil
	}
	var params map[string]any
	if len(body) > 0 {
		if err := json.Unmarshal(body, &params); err != nil {
			return shared.JSONError("ClientException", "invalid JSON", http.StatusBadRequest), nil
		}
	} else {
		params = map[string]any{}
	}

	if op == "" {
		op = resolveOp(req.Method, req.URL.Path)
	}

	switch op {
	// ComputeEnvironment
	case "CreateComputeEnvironment":
		return p.createComputeEnvironment(params)
	case "DescribeComputeEnvironments":
		return p.describeComputeEnvironments(params)
	case "UpdateComputeEnvironment":
		return p.updateComputeEnvironment(params)
	case "DeleteComputeEnvironment":
		return p.deleteComputeEnvironment(params)

	// JobQueue
	case "CreateJobQueue":
		return p.createJobQueue(params)
	case "DescribeJobQueues":
		return p.describeJobQueues(params)
	case "UpdateJobQueue":
		return p.updateJobQueue(params)
	case "DeleteJobQueue":
		return p.deleteJobQueue(params)

	// JobDefinition
	case "RegisterJobDefinition":
		return p.registerJobDefinition(params)
	case "DescribeJobDefinitions":
		return p.describeJobDefinitions(params)
	case "DeregisterJobDefinition":
		return p.deregisterJobDefinition(params)

	// Jobs
	case "SubmitJob":
		return p.submitJob(params)
	case "DescribeJobs":
		return p.describeJobs(params)
	case "ListJobs":
		return p.listJobs(params)
	case "CancelJob":
		return p.cancelJob(params)
	case "TerminateJob":
		return p.terminateJob(params)

	// SchedulingPolicy
	case "CreateSchedulingPolicy":
		return p.createSchedulingPolicy(params)
	case "DescribeSchedulingPolicies":
		return p.describeSchedulingPolicies(params)
	case "ListSchedulingPolicies":
		return p.listSchedulingPolicies()
	case "UpdateSchedulingPolicy":
		return p.updateSchedulingPolicy(params)
	case "DeleteSchedulingPolicy":
		return p.deleteSchedulingPolicy(params)

	// Tags
	case "TagResource":
		return p.tagResource(req, params)
	case "UntagResource":
		return p.untagResource(req)
	case "ListTagsForResource":
		return p.listTagsForResource(req)

	// GetJobQueueSnapshot — return empty snapshot
	case "GetJobQueueSnapshot":
		return shared.JSONResponse(http.StatusOK, map[string]any{
			"frontOfQueue": map[string]any{
				"jobs":          []any{},
				"lastUpdatedAt": 0,
			},
		})

	// Stub operations — ConsumableResource, QuotaShare, ServiceEnvironment, ServiceJob
	case "CreateConsumableResource":
		return shared.JSONResponse(http.StatusOK, map[string]any{
			"consumableResourceArn":  shared.BuildARN("batch", "consumable-resource", strParam(params, "consumableResourceName")),
			"consumableResourceName": strParam(params, "consumableResourceName"),
		})
	case "DeleteConsumableResource":
		return shared.JSONResponse(http.StatusOK, map[string]any{})
	case "UpdateConsumableResource":
		return shared.JSONResponse(http.StatusOK, map[string]any{
			"consumableResourceArn":  "",
			"consumableResourceName": "",
			"totalQuantity":          0,
		})
	case "DescribeConsumableResource":
		return shared.JSONResponse(http.StatusOK, map[string]any{
			"consumableResourceArn":  "",
			"consumableResourceName": "",
			"availableQuantity":      0,
			"inUseQuantity":          0,
			"totalQuantity":          0,
			"resourceType":           "",
			"createdAt":              0,
			"tags":                   map[string]string{},
		})
	case "ListConsumableResources":
		return shared.JSONResponse(http.StatusOK, map[string]any{"consumableResources": []any{}, "nextToken": ""})
	case "ListJobsByConsumableResource":
		return shared.JSONResponse(http.StatusOK, map[string]any{"jobs": []any{}, "nextToken": ""})
	case "CreateQuotaShare":
		return shared.JSONResponse(http.StatusOK, map[string]any{
			"quotaShareArn":  shared.BuildARN("batch", "quota-share", strParam(params, "quotaShareName")),
			"quotaShareName": strParam(params, "quotaShareName"),
		})
	case "DeleteQuotaShare":
		return shared.JSONResponse(http.StatusOK, map[string]any{})
	case "UpdateQuotaShare":
		return shared.JSONResponse(http.StatusOK, map[string]any{"quotaShareArn": "", "quotaShareName": ""})
	case "DescribeQuotaShare":
		return shared.JSONResponse(http.StatusOK, map[string]any{})
	case "ListQuotaShares":
		return shared.JSONResponse(http.StatusOK, map[string]any{"quotaShares": []any{}, "nextToken": ""})
	case "CreateServiceEnvironment":
		return shared.JSONResponse(http.StatusOK, map[string]any{
			"serviceEnvironmentArn":  shared.BuildARN("batch", "service-environment", strParam(params, "serviceEnvironmentName")),
			"serviceEnvironmentName": strParam(params, "serviceEnvironmentName"),
		})
	case "DeleteServiceEnvironment":
		return shared.JSONResponse(http.StatusOK, map[string]any{})
	case "UpdateServiceEnvironment":
		return shared.JSONResponse(http.StatusOK, map[string]any{"serviceEnvironmentArn": "", "serviceEnvironmentName": ""})
	case "DescribeServiceEnvironments":
		return shared.JSONResponse(http.StatusOK, map[string]any{"serviceEnvironments": []any{}, "nextToken": ""})
	case "SubmitServiceJob":
		jobID := shared.GenerateUUID()
		return shared.JSONResponse(http.StatusOK, map[string]any{
			"jobArn":  shared.BuildARN("batch", "job", jobID),
			"jobId":   jobID,
			"jobName": strParam(params, "jobName"),
		})
	case "TerminateServiceJob":
		return shared.JSONResponse(http.StatusOK, map[string]any{})
	case "DescribeServiceJob":
		return shared.JSONResponse(http.StatusOK, map[string]any{})
	case "ListServiceJobs":
		return shared.JSONResponse(http.StatusOK, map[string]any{"jobSummaryList": []any{}, "nextToken": ""})
	case "UpdateServiceJob":
		return shared.JSONResponse(http.StatusOK, map[string]any{})

	default:
		return shared.JSONError("InvalidAction", fmt.Sprintf("unknown action: %s", op), http.StatusBadRequest), nil
	}
}

func resolveOp(method, path string) string {
	// Batch uses POST for all operations with paths like /v1/createcomputeenvironment
	p := strings.TrimPrefix(path, "/v1/")
	p = strings.TrimPrefix(p, "/v1")
	p = strings.Trim(p, "/")
	// Also handle /tags/{arn} which uses GET/POST/DELETE
	if strings.HasPrefix(p, "tags/") || p == "tags" {
		switch method {
		case http.MethodPost:
			return "TagResource"
		case http.MethodGet:
			return "ListTagsForResource"
		case http.MethodDelete:
			return "UntagResource"
		}
	}
	// Map lowercase path to operation name
	ops := map[string]string{
		"createcomputeenvironment":     "CreateComputeEnvironment",
		"describecomputeenvironments":  "DescribeComputeEnvironments",
		"updatecomputeenvironment":     "UpdateComputeEnvironment",
		"deletecomputeenvironment":     "DeleteComputeEnvironment",
		"createjobqueue":               "CreateJobQueue",
		"describejobqueues":            "DescribeJobQueues",
		"updatejobqueue":               "UpdateJobQueue",
		"deletejobqueue":               "DeleteJobQueue",
		"registerjobdefinition":        "RegisterJobDefinition",
		"describejobdefinitions":       "DescribeJobDefinitions",
		"deregisterjobdefinition":      "DeregisterJobDefinition",
		"submitjob":                    "SubmitJob",
		"describejobs":                 "DescribeJobs",
		"listjobs":                     "ListJobs",
		"canceljob":                    "CancelJob",
		"terminatejob":                 "TerminateJob",
		"createschedulingpolicy":       "CreateSchedulingPolicy",
		"describeschedulingpolicies":   "DescribeSchedulingPolicies",
		"listschedulingpolicies":       "ListSchedulingPolicies",
		"updateschedulingpolicy":       "UpdateSchedulingPolicy",
		"deleteschedulingpolicy":       "DeleteSchedulingPolicy",
		"getjobqueuesnapshot":          "GetJobQueueSnapshot",
		"createconsumableresource":     "CreateConsumableResource",
		"deleteconsumableresource":     "DeleteConsumableResource",
		"updateconsumableresource":     "UpdateConsumableResource",
		"describeconsumableresource":   "DescribeConsumableResource",
		"listconsumableresources":      "ListConsumableResources",
		"listjobsbyconsumableresource": "ListJobsByConsumableResource",
		"createquotashare":             "CreateQuotaShare",
		"deletequotashare":             "DeleteQuotaShare",
		"updatequotashare":             "UpdateQuotaShare",
		"describequotashare":           "DescribeQuotaShare",
		"listquotashares":              "ListQuotaShares",
		"createserviceenvironment":     "CreateServiceEnvironment",
		"deleteserviceenvironment":     "DeleteServiceEnvironment",
		"updateserviceenvironment":     "UpdateServiceEnvironment",
		"describeserviceenvironments":  "DescribeServiceEnvironments",
		"submitservicejob":             "SubmitServiceJob",
		"terminateservicejob":          "TerminateServiceJob",
		"describeservicejob":           "DescribeServiceJob",
		"listservicejobs":              "ListServiceJobs",
		"updateservicejob":             "UpdateServiceJob",
	}
	if op, ok := ops[strings.ToLower(p)]; ok {
		return op
	}
	return ""
}

func (p *Provider) ListResources(_ context.Context) ([]plugin.Resource, error) {
	ces, err := p.store.ListComputeEnvironments()
	if err != nil {
		return nil, err
	}
	var res []plugin.Resource
	for _, ce := range ces {
		res = append(res, plugin.Resource{Type: "batch-compute-environment", ID: ce.ARN, Name: ce.Name})
	}
	jqs, err := p.store.ListJobQueues()
	if err != nil {
		return nil, err
	}
	for _, jq := range jqs {
		res = append(res, plugin.Resource{Type: "batch-job-queue", ID: jq.ARN, Name: jq.Name})
	}
	return res, nil
}

func (p *Provider) GetMetrics(_ context.Context) (*plugin.ServiceMetrics, error) {
	return &plugin.ServiceMetrics{}, nil
}

// --- ComputeEnvironment CRUD ---

func (p *Provider) createComputeEnvironment(params map[string]any) (*plugin.Response, error) {
	name := strParam(params, "computeEnvironmentName")
	if name == "" {
		return shared.JSONError("ClientException", "computeEnvironmentName is required", http.StatusBadRequest), nil
	}
	ceType := strParamDefault(params, "type", "MANAGED")
	state := strParamDefault(params, "state", "ENABLED")
	serviceRole := strParam(params, "serviceRole")

	computeResources := "{}"
	if cr, ok := params["computeResources"]; ok {
		computeResources = toJSON(cr)
	}

	arn := shared.BuildARN("batch", "compute-environment", name)
	ce := &ComputeEnvironment{
		ARN:              arn,
		Name:             name,
		Type:             ceType,
		State:            state,
		Status:           "VALID",
		ServiceRole:      serviceRole,
		ComputeResources: computeResources,
	}
	if err := p.store.CreateComputeEnvironment(ce); err != nil {
		if isUniqueErr(err) {
			return shared.JSONError("ClientException", "compute environment already exists", http.StatusBadRequest), nil
		}
		return nil, err
	}
	if rawTags, ok := params["tags"].(map[string]any); ok {
		p.store.tags.AddTags(arn, toStringMap(rawTags))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"computeEnvironmentArn":  arn,
		"computeEnvironmentName": name,
	})
}

func (p *Provider) describeComputeEnvironments(params map[string]any) (*plugin.Response, error) {
	filters := toStringSlice(params["computeEnvironments"])

	var ces []*ComputeEnvironment
	if len(filters) > 0 {
		for _, f := range filters {
			ce, err := p.store.GetComputeEnvironment(f)
			if err != nil {
				continue
			}
			ces = append(ces, ce)
		}
	} else {
		var err error
		ces, err = p.store.ListComputeEnvironments()
		if err != nil {
			return nil, err
		}
	}

	list := make([]map[string]any, 0, len(ces))
	for _, ce := range ces {
		tags, _ := p.store.tags.ListTags(ce.ARN)
		list = append(list, ceToMap(ce, tags))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"computeEnvironments": list,
		"nextToken":           "",
	})
}

func (p *Provider) updateComputeEnvironment(params map[string]any) (*plugin.Response, error) {
	nameOrARN := strParam(params, "computeEnvironment")
	if nameOrARN == "" {
		return shared.JSONError("ClientException", "computeEnvironment is required", http.StatusBadRequest), nil
	}
	ce, err := p.store.GetComputeEnvironment(nameOrARN)
	if err != nil {
		return shared.JSONError("ClientException", "compute environment not found", http.StatusBadRequest), nil
	}
	state := strParamDefault(params, "state", ce.State)
	serviceRole := strParamDefault(params, "serviceRole", ce.ServiceRole)

	if err := p.store.UpdateComputeEnvironment(nameOrARN, state, serviceRole); err != nil {
		return shared.JSONError("ClientException", "compute environment not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"computeEnvironmentArn":  ce.ARN,
		"computeEnvironmentName": ce.Name,
	})
}

func (p *Provider) deleteComputeEnvironment(params map[string]any) (*plugin.Response, error) {
	nameOrARN := strParam(params, "computeEnvironment")
	if nameOrARN == "" {
		return shared.JSONError("ClientException", "computeEnvironment is required", http.StatusBadRequest), nil
	}
	ce, err := p.store.GetComputeEnvironment(nameOrARN)
	if err != nil {
		return shared.JSONError("ClientException", "compute environment not found", http.StatusBadRequest), nil
	}
	p.store.tags.DeleteAllTags(ce.ARN)
	if err := p.store.DeleteComputeEnvironment(nameOrARN); err != nil {
		return shared.JSONError("ClientException", "compute environment not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

// --- JobQueue CRUD ---

func (p *Provider) createJobQueue(params map[string]any) (*plugin.Response, error) {
	name := strParam(params, "jobQueueName")
	if name == "" {
		return shared.JSONError("ClientException", "jobQueueName is required", http.StatusBadRequest), nil
	}
	state := strParamDefault(params, "state", "ENABLED")
	priority := int32(0)
	if v, ok := params["priority"].(float64); ok {
		priority = int32(v)
	}
	schedulingPolicy := strParam(params, "schedulingPolicyArn")

	computeEnvs := "[]"
	if ce, ok := params["computeEnvironmentOrder"]; ok {
		computeEnvs = toJSON(ce)
	}

	arn := shared.BuildARN("batch", "job-queue", name)
	jq := &JobQueue{
		ARN:              arn,
		Name:             name,
		State:            state,
		Status:           "VALID",
		Priority:         priority,
		ComputeEnvs:      computeEnvs,
		SchedulingPolicy: schedulingPolicy,
	}
	if err := p.store.CreateJobQueue(jq); err != nil {
		if isUniqueErr(err) {
			return shared.JSONError("ClientException", "job queue already exists", http.StatusBadRequest), nil
		}
		return nil, err
	}
	if rawTags, ok := params["tags"].(map[string]any); ok {
		p.store.tags.AddTags(arn, toStringMap(rawTags))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"jobQueueArn":  arn,
		"jobQueueName": name,
	})
}

func (p *Provider) describeJobQueues(params map[string]any) (*plugin.Response, error) {
	filters := toStringSlice(params["jobQueues"])

	var jqs []*JobQueue
	if len(filters) > 0 {
		for _, f := range filters {
			jq, err := p.store.GetJobQueue(f)
			if err != nil {
				continue
			}
			jqs = append(jqs, jq)
		}
	} else {
		var err error
		jqs, err = p.store.ListJobQueues()
		if err != nil {
			return nil, err
		}
	}

	list := make([]map[string]any, 0, len(jqs))
	for _, jq := range jqs {
		tags, _ := p.store.tags.ListTags(jq.ARN)
		list = append(list, jqToMap(jq, tags))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"jobQueues": list,
		"nextToken": "",
	})
}

func (p *Provider) updateJobQueue(params map[string]any) (*plugin.Response, error) {
	nameOrARN := strParam(params, "jobQueue")
	if nameOrARN == "" {
		return shared.JSONError("ClientException", "jobQueue is required", http.StatusBadRequest), nil
	}
	jq, err := p.store.GetJobQueue(nameOrARN)
	if err != nil {
		return shared.JSONError("ClientException", "job queue not found", http.StatusBadRequest), nil
	}
	state := strParamDefault(params, "state", jq.State)
	priority := jq.Priority
	if v, ok := params["priority"].(float64); ok {
		priority = int32(v)
	}
	schedulingPolicy := strParamDefault(params, "schedulingPolicyArn", jq.SchedulingPolicy)

	if err := p.store.UpdateJobQueue(nameOrARN, state, priority, schedulingPolicy); err != nil {
		return shared.JSONError("ClientException", "job queue not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"jobQueueArn":  jq.ARN,
		"jobQueueName": jq.Name,
	})
}

func (p *Provider) deleteJobQueue(params map[string]any) (*plugin.Response, error) {
	nameOrARN := strParam(params, "jobQueue")
	if nameOrARN == "" {
		return shared.JSONError("ClientException", "jobQueue is required", http.StatusBadRequest), nil
	}
	jq, err := p.store.GetJobQueue(nameOrARN)
	if err != nil {
		return shared.JSONError("ClientException", "job queue not found", http.StatusBadRequest), nil
	}
	p.store.tags.DeleteAllTags(jq.ARN)
	if err := p.store.DeleteJobQueue(nameOrARN); err != nil {
		return shared.JSONError("ClientException", "job queue not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

// --- JobDefinition CRUD ---

func (p *Provider) registerJobDefinition(params map[string]any) (*plugin.Response, error) {
	name := strParam(params, "jobDefinitionName")
	if name == "" {
		return shared.JSONError("ClientException", "jobDefinitionName is required", http.StatusBadRequest), nil
	}
	jdType := strParamDefault(params, "type", "container")

	containerProps := "{}"
	if cp, ok := params["containerProperties"]; ok {
		containerProps = toJSON(cp)
	}
	parameters := "{}"
	if p2, ok := params["parameters"]; ok {
		parameters = toJSON(p2)
	}
	timeout := "{}"
	if t, ok := params["timeout"]; ok {
		timeout = toJSON(t)
	}

	jd := &JobDefinition{
		Name:           name,
		Type:           jdType,
		Status:         "ACTIVE",
		ContainerProps: containerProps,
		Parameters:     parameters,
		Timeout:        timeout,
	}

	// ARN will be set after we know the revision
	if err := p.store.RegisterJobDefinition(jd); err != nil {
		return nil, err
	}

	arn := fmt.Sprintf("arn:aws:batch:%s:%s:job-definition/%s:%d",
		shared.DefaultRegion, shared.DefaultAccountID, name, jd.Revision)
	// Update the ARN with the revision
	p.store.UpdateJobDefinitionARN(name, jd.Revision, arn)
	jd.ARN = arn

	if rawTags, ok := params["tags"].(map[string]any); ok {
		p.store.tags.AddTags(arn, toStringMap(rawTags))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"jobDefinitionArn":  arn,
		"jobDefinitionName": name,
		"revision":          jd.Revision,
	})
}

func (p *Provider) describeJobDefinitions(params map[string]any) (*plugin.Response, error) {
	name := strParam(params, "jobDefinitionName")
	status := strParam(params, "status")
	arnFilters := toStringSlice(params["jobDefinitions"])

	var jds []*JobDefinition
	var err error

	if len(arnFilters) > 0 {
		for _, f := range arnFilters {
			jd, ferr := p.store.GetJobDefinition(f)
			if ferr != nil {
				continue
			}
			jds = append(jds, jd)
		}
	} else {
		jds, err = p.store.ListJobDefinitions(name, status)
		if err != nil {
			return nil, err
		}
	}

	list := make([]map[string]any, 0, len(jds))
	for _, jd := range jds {
		tags, _ := p.store.tags.ListTags(jd.ARN)
		list = append(list, jdToMap(jd, tags))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"jobDefinitions": list,
		"nextToken":      "",
	})
}

func (p *Provider) deregisterJobDefinition(params map[string]any) (*plugin.Response, error) {
	nameOrARN := strParam(params, "jobDefinition")
	if nameOrARN == "" {
		return shared.JSONError("ClientException", "jobDefinition is required", http.StatusBadRequest), nil
	}
	if err := p.store.DeregisterJobDefinition(nameOrARN); err != nil {
		return shared.JSONError("ClientException", "job definition not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

// --- Jobs ---

func (p *Provider) submitJob(params map[string]any) (*plugin.Response, error) {
	jobName := strParam(params, "jobName")
	if jobName == "" {
		return shared.JSONError("ClientException", "jobName is required", http.StatusBadRequest), nil
	}
	jobQueue := strParam(params, "jobQueue")
	jobDefinition := strParam(params, "jobDefinition")

	parameters := "{}"
	if p2, ok := params["parameters"]; ok {
		parameters = toJSON(p2)
	}
	container := "{}"
	if c, ok := params["containerOverrides"]; ok {
		container = toJSON(c)
	}

	jobID := shared.GenerateUUID()
	arn := shared.BuildARN("batch", "job", jobID)
	now := nowUnix()

	j := &Job{
		ID:           jobID,
		ARN:          arn,
		Name:         jobName,
		Queue:        jobQueue,
		Definition:   jobDefinition,
		Status:       "SUCCEEDED",
		StatusReason: "",
		Parameters:   parameters,
		Container:    container,
		StartedAt:    now,
		StoppedAt:    now,
	}
	if err := p.store.CreateJob(j); err != nil {
		return nil, err
	}
	if rawTags, ok := params["tags"].(map[string]any); ok {
		p.store.tags.AddTags(arn, toStringMap(rawTags))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"jobArn":  arn,
		"jobId":   jobID,
		"jobName": jobName,
	})
}

func (p *Provider) describeJobs(params map[string]any) (*plugin.Response, error) {
	ids := toStringSlice(params["jobs"])
	var jobs []*Job
	for _, id := range ids {
		j, err := p.store.GetJob(id)
		if err != nil {
			continue
		}
		jobs = append(jobs, j)
	}
	list := make([]map[string]any, 0, len(jobs))
	for _, j := range jobs {
		tags, _ := p.store.tags.ListTags(j.ARN)
		list = append(list, jobToMap(j, tags))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"jobs": list})
}

func (p *Provider) listJobs(params map[string]any) (*plugin.Response, error) {
	queue := strParam(params, "jobQueue")
	status := strParam(params, "jobStatus")

	jobs, err := p.store.ListJobs(queue, status)
	if err != nil {
		return nil, err
	}
	list := make([]map[string]any, 0, len(jobs))
	for _, j := range jobs {
		list = append(list, jobSummaryToMap(j))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"jobSummaryList": list,
		"nextToken":      "",
	})
}

func (p *Provider) cancelJob(params map[string]any) (*plugin.Response, error) {
	jobID := strParam(params, "jobId")
	reason := strParamDefault(params, "reason", "cancelled")
	if jobID == "" {
		return shared.JSONError("ClientException", "jobId is required", http.StatusBadRequest), nil
	}
	if err := p.store.UpdateJobStatus(jobID, "CANCELLED", reason); err != nil {
		return shared.JSONError("ClientException", "job not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) terminateJob(params map[string]any) (*plugin.Response, error) {
	jobID := strParam(params, "jobId")
	reason := strParamDefault(params, "reason", "terminated")
	if jobID == "" {
		return shared.JSONError("ClientException", "jobId is required", http.StatusBadRequest), nil
	}
	if err := p.store.UpdateJobStatus(jobID, "FAILED", reason); err != nil {
		return shared.JSONError("ClientException", "job not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

// --- SchedulingPolicy CRUD ---

func (p *Provider) createSchedulingPolicy(params map[string]any) (*plugin.Response, error) {
	name := strParam(params, "name")
	if name == "" {
		return shared.JSONError("ClientException", "name is required", http.StatusBadRequest), nil
	}
	fairshare := "{}"
	if fp, ok := params["fairsharePolicy"]; ok {
		fairshare = toJSON(fp)
	}

	arn := shared.BuildARN("batch", "scheduling-policy", name)
	sp := &SchedulingPolicy{
		ARN:       arn,
		Name:      name,
		Fairshare: fairshare,
	}
	if err := p.store.CreateSchedulingPolicy(sp); err != nil {
		if isUniqueErr(err) {
			return shared.JSONError("ClientException", "scheduling policy already exists", http.StatusBadRequest), nil
		}
		return nil, err
	}
	if rawTags, ok := params["tags"].(map[string]any); ok {
		p.store.tags.AddTags(arn, toStringMap(rawTags))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"arn":  arn,
		"name": name,
	})
}

func (p *Provider) describeSchedulingPolicies(params map[string]any) (*plugin.Response, error) {
	arns := toStringSlice(params["arns"])
	var sps []*SchedulingPolicy
	if len(arns) > 0 {
		for _, a := range arns {
			sp, err := p.store.GetSchedulingPolicy(a)
			if err != nil {
				continue
			}
			sps = append(sps, sp)
		}
	} else {
		var err error
		sps, err = p.store.ListSchedulingPolicies()
		if err != nil {
			return nil, err
		}
	}
	list := make([]map[string]any, 0, len(sps))
	for _, sp := range sps {
		tags, _ := p.store.tags.ListTags(sp.ARN)
		list = append(list, spToDetailMap(sp, tags))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"schedulingPolicies": list})
}

func (p *Provider) listSchedulingPolicies() (*plugin.Response, error) {
	sps, err := p.store.ListSchedulingPolicies()
	if err != nil {
		return nil, err
	}
	list := make([]map[string]any, 0, len(sps))
	for _, sp := range sps {
		list = append(list, map[string]any{"arn": sp.ARN})
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"schedulingPolicies": list,
		"nextToken":          "",
	})
}

func (p *Provider) updateSchedulingPolicy(params map[string]any) (*plugin.Response, error) {
	arn := strParam(params, "arn")
	if arn == "" {
		return shared.JSONError("ClientException", "arn is required", http.StatusBadRequest), nil
	}
	fairshare := "{}"
	if fp, ok := params["fairsharePolicy"]; ok {
		fairshare = toJSON(fp)
	}
	if err := p.store.UpdateSchedulingPolicy(arn, fairshare); err != nil {
		return shared.JSONError("ClientException", "scheduling policy not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) deleteSchedulingPolicy(params map[string]any) (*plugin.Response, error) {
	arn := strParam(params, "arn")
	if arn == "" {
		return shared.JSONError("ClientException", "arn is required", http.StatusBadRequest), nil
	}
	sp, err := p.store.GetSchedulingPolicy(arn)
	if err != nil {
		return shared.JSONError("ClientException", "scheduling policy not found", http.StatusBadRequest), nil
	}
	p.store.tags.DeleteAllTags(sp.ARN)
	if err := p.store.DeleteSchedulingPolicy(arn); err != nil {
		return shared.JSONError("ClientException", "scheduling policy not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

// --- Tags ---

func (p *Provider) tagResource(req *http.Request, params map[string]any) (*plugin.Response, error) {
	arn := extractPathParam(req.URL.Path, "tags")
	if arn == "" {
		arn = strParam(params, "resourceArn")
	}
	if arn == "" {
		return shared.JSONError("ClientException", "resourceArn is required", http.StatusBadRequest), nil
	}
	rawTags, _ := params["tags"].(map[string]any)
	if err := p.store.tags.AddTags(arn, toStringMap(rawTags)); err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) untagResource(req *http.Request) (*plugin.Response, error) {
	arn := extractPathParam(req.URL.Path, "tags")
	if arn == "" {
		return shared.JSONError("ClientException", "resourceArn is required", http.StatusBadRequest), nil
	}
	keys := req.URL.Query()["tagKeys"]
	if err := p.store.tags.RemoveTags(arn, keys); err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) listTagsForResource(req *http.Request) (*plugin.Response, error) {
	arn := extractPathParam(req.URL.Path, "tags")
	if arn == "" {
		return shared.JSONError("ClientException", "resourceArn is required", http.StatusBadRequest), nil
	}
	tags, err := p.store.tags.ListTags(arn)
	if err != nil {
		return nil, err
	}
	if tags == nil {
		tags = map[string]string{}
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"tags": tags})
}

// --- Serialization helpers ---

func ceToMap(ce *ComputeEnvironment, tags map[string]string) map[string]any {
	if tags == nil {
		tags = map[string]string{}
	}
	return map[string]any{
		"computeEnvironmentArn":  ce.ARN,
		"computeEnvironmentName": ce.Name,
		"type":                   ce.Type,
		"state":                  ce.State,
		"status":                 ce.Status,
		"statusReason":           "",
		"serviceRole":            ce.ServiceRole,
		"tags":                   tags,
	}
}

func jqToMap(jq *JobQueue, tags map[string]string) map[string]any {
	if tags == nil {
		tags = map[string]string{}
	}
	var ceOrder []any
	json.Unmarshal([]byte(jq.ComputeEnvs), &ceOrder)
	if ceOrder == nil {
		ceOrder = []any{}
	}
	return map[string]any{
		"jobQueueArn":             jq.ARN,
		"jobQueueName":            jq.Name,
		"state":                   jq.State,
		"status":                  jq.Status,
		"statusReason":            "",
		"priority":                jq.Priority,
		"computeEnvironmentOrder": ceOrder,
		"schedulingPolicyArn":     jq.SchedulingPolicy,
		"tags":                    tags,
	}
}

func jdToMap(jd *JobDefinition, tags map[string]string) map[string]any {
	if tags == nil {
		tags = map[string]string{}
	}
	var containerProps any
	json.Unmarshal([]byte(jd.ContainerProps), &containerProps)
	var parameters any
	json.Unmarshal([]byte(jd.Parameters), &parameters)
	var timeout any
	json.Unmarshal([]byte(jd.Timeout), &timeout)
	return map[string]any{
		"jobDefinitionArn":    jd.ARN,
		"jobDefinitionName":   jd.Name,
		"revision":            jd.Revision,
		"type":                jd.Type,
		"status":              jd.Status,
		"containerProperties": containerProps,
		"parameters":          parameters,
		"timeout":             timeout,
		"tags":                tags,
	}
}

func jobToMap(j *Job, tags map[string]string) map[string]any {
	if tags == nil {
		tags = map[string]string{}
	}
	var parameters any
	json.Unmarshal([]byte(j.Parameters), &parameters)
	var container any
	json.Unmarshal([]byte(j.Container), &container)
	return map[string]any{
		"jobId":         j.ID,
		"jobArn":        j.ARN,
		"jobName":       j.Name,
		"jobQueue":      j.Queue,
		"jobDefinition": j.Definition,
		"status":        j.Status,
		"statusReason":  j.StatusReason,
		"parameters":    parameters,
		"container":     container,
		"createdAt":     j.CreatedAt,
		"startedAt":     j.StartedAt,
		"stoppedAt":     j.StoppedAt,
		"tags":          tags,
	}
}

func jobSummaryToMap(j *Job) map[string]any {
	return map[string]any{
		"jobId":         j.ID,
		"jobArn":        j.ARN,
		"jobName":       j.Name,
		"jobDefinition": j.Definition,
		"status":        j.Status,
		"statusReason":  j.StatusReason,
		"createdAt":     j.CreatedAt,
		"startedAt":     j.StartedAt,
		"stoppedAt":     j.StoppedAt,
	}
}

func spToDetailMap(sp *SchedulingPolicy, tags map[string]string) map[string]any {
	if tags == nil {
		tags = map[string]string{}
	}
	var fairshare any
	json.Unmarshal([]byte(sp.Fairshare), &fairshare)
	return map[string]any{
		"arn":             sp.ARN,
		"name":            sp.Name,
		"fairsharePolicy": fairshare,
		"tags":            tags,
	}
}

// --- Util ---

func strParam(params map[string]any, key string) string {
	if v, ok := params[key].(string); ok {
		return v
	}
	return ""
}

func strParamDefault(params map[string]any, key, def string) string {
	if v, ok := params[key].(string); ok && v != "" {
		return v
	}
	return def
}

func toStringMap(m map[string]any) map[string]string {
	result := make(map[string]string)
	for k, v := range m {
		if s, ok := v.(string); ok {
			result[k] = s
		}
	}
	return result
}

func toStringSlice(v any) []string {
	if v == nil {
		return nil
	}
	arr, ok := v.([]any)
	if !ok {
		return nil
	}
	result := make([]string, 0, len(arr))
	for _, item := range arr {
		if s, ok := item.(string); ok {
			result = append(result, s)
		}
	}
	return result
}

func extractPathParam(path, key string) string {
	parts := strings.Split(path, "/")
	for i, p := range parts {
		if p == key && i+1 < len(parts) {
			return parts[i+1]
		}
	}
	return ""
}

func nowUnix() int64 {
	return time.Now().Unix()
}
