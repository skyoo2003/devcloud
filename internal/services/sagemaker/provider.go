// SPDX-License-Identifier: Apache-2.0

// internal/services/sagemaker/provider.go
package sagemaker

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"path/filepath"
	"strings"

	"github.com/skyoo2003/devcloud/internal/plugin"
	"github.com/skyoo2003/devcloud/internal/shared"
)

type Provider struct {
	store *Store
}

func (p *Provider) ServiceID() string             { return "sagemaker" }
func (p *Provider) ServiceName() string           { return "SageMaker" }
func (p *Provider) Protocol() plugin.ProtocolType { return plugin.ProtocolJSON11 }

func (p *Provider) Init(cfg plugin.PluginConfig) error {
	dataDir := cfg.DataDir
	if dataDir == "" {
		dataDir = "."
	}
	var err error
	p.store, err = NewStore(filepath.Join(dataDir, "sagemaker"))
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
	// ---- NotebookInstance ----
	case "CreateNotebookInstance":
		return p.createNotebookInstance(params)
	case "DescribeNotebookInstance":
		return p.describeNotebookInstance(params)
	case "ListNotebookInstances":
		return p.listNotebookInstances(params)
	case "DeleteNotebookInstance":
		return p.deleteNotebookInstance(params)
	case "StartNotebookInstance":
		return p.startNotebookInstance(params)
	case "StopNotebookInstance":
		return p.stopNotebookInstance(params)
	case "UpdateNotebookInstance":
		return p.updateNotebookInstance(params)

	// ---- Model ----
	case "CreateModel":
		return p.createModel(params)
	case "DescribeModel":
		return p.describeModel(params)
	case "ListModels":
		return p.listModels(params)
	case "DeleteModel":
		return p.deleteModel(params)

	// ---- EndpointConfig ----
	case "CreateEndpointConfig":
		return p.createEndpointConfig(params)
	case "DescribeEndpointConfig":
		return p.describeEndpointConfig(params)
	case "ListEndpointConfigs":
		return p.listEndpointConfigs(params)
	case "DeleteEndpointConfig":
		return p.deleteEndpointConfig(params)

	// ---- Endpoint ----
	case "CreateEndpoint":
		return p.createEndpoint(params)
	case "DescribeEndpoint":
		return p.describeEndpoint(params)
	case "ListEndpoints":
		return p.listEndpoints(params)
	case "UpdateEndpoint":
		return p.updateEndpoint(params)
	case "DeleteEndpoint":
		return p.deleteEndpoint(params)

	// ---- TrainingJob ----
	case "CreateTrainingJob":
		return p.createTrainingJob(params)
	case "DescribeTrainingJob":
		return p.describeTrainingJob(params)
	case "ListTrainingJobs":
		return p.listTrainingJobs(params)
	case "StopTrainingJob":
		return p.stopTrainingJob(params)

	// ---- ProcessingJob ----
	case "CreateProcessingJob":
		return p.createProcessingJob(params)
	case "DescribeProcessingJob":
		return p.describeProcessingJob(params)
	case "ListProcessingJobs":
		return p.listProcessingJobs(params)
	case "StopProcessingJob":
		return p.stopProcessingJob(params)

	// ---- TransformJob ----
	case "CreateTransformJob":
		return p.createTransformJob(params)
	case "DescribeTransformJob":
		return p.describeTransformJob(params)
	case "ListTransformJobs":
		return p.listTransformJobs(params)
	case "StopTransformJob":
		return p.stopTransformJob(params)

	// ---- Pipeline ----
	case "CreatePipeline":
		return p.createPipeline(params)
	case "DescribePipeline":
		return p.describePipeline(params)
	case "ListPipelines":
		return p.listPipelines(params)
	case "UpdatePipeline":
		return p.updatePipeline(params)
	case "DeletePipeline":
		return p.deletePipeline(params)
	case "StartPipelineExecution":
		return p.startPipelineExecution(params)
	case "DescribePipelineExecution":
		return p.describePipelineExecution(params)
	case "ListPipelineExecutions":
		return p.listPipelineExecutions(params)
	case "StopPipelineExecution":
		return p.stopPipelineExecution(params)

	// ---- Experiment ----
	case "CreateExperiment":
		return p.createExperiment(params)
	case "DescribeExperiment":
		return p.describeExperiment(params)
	case "ListExperiments":
		return p.listExperiments(params)
	case "UpdateExperiment":
		return p.updateExperiment(params)
	case "DeleteExperiment":
		return p.deleteExperiment(params)

	// ---- Trial ----
	case "CreateTrial":
		return p.createTrial(params)
	case "DescribeTrial":
		return p.describeTrial(params)
	case "ListTrials":
		return p.listTrials(params)
	case "UpdateTrial":
		return p.updateTrial(params)
	case "DeleteTrial":
		return p.deleteTrial(params)

	// ---- Domain ----
	case "CreateDomain":
		return p.createDomain(params)
	case "DescribeDomain":
		return p.describeDomain(params)
	case "ListDomains":
		return p.listDomains(params)
	case "DeleteDomain":
		return p.deleteDomain(params)
	case "UpdateDomain":
		return p.updateDomain(params)

	// ---- UserProfile ----
	case "CreateUserProfile":
		return p.createUserProfile(params)
	case "DescribeUserProfile":
		return p.describeUserProfile(params)
	case "ListUserProfiles":
		return p.listUserProfiles(params)
	case "DeleteUserProfile":
		return p.deleteUserProfile(params)
	case "UpdateUserProfile":
		return p.updateUserProfile(params)

	// ---- Tags ----
	case "AddTags":
		return p.addTags(params)
	case "DeleteTags":
		return p.deleteTags(params)
	case "ListTags":
		return p.listTags(params)

	// ---- Search (stub) ----
	case "Search":
		return shared.JSONResponse(http.StatusOK, map[string]any{"Results": []any{}, "NextToken": nil})

	// ---- ~320 stub operations ----
	default:
		return shared.JSONResponse(http.StatusOK, map[string]any{})
	}
}

func (p *Provider) ListResources(_ context.Context) ([]plugin.Resource, error) {
	nbs, err := p.store.ListNotebookInstances()
	if err != nil {
		return nil, err
	}
	res := make([]plugin.Resource, 0, len(nbs))
	for _, nb := range nbs {
		res = append(res, plugin.Resource{Type: "notebook-instance", ID: nb.Name, Name: nb.Name})
	}
	return res, nil
}

func (p *Provider) GetMetrics(_ context.Context) (*plugin.ServiceMetrics, error) {
	return &plugin.ServiceMetrics{}, nil
}

// ---- NotebookInstance handlers ----

func (p *Provider) createNotebookInstance(params map[string]any) (*plugin.Response, error) {
	name := strParam(params, "NotebookInstanceName")
	if name == "" {
		return shared.JSONError("ValidationException", "NotebookInstanceName is required", http.StatusBadRequest), nil
	}
	instanceType := strParam(params, "InstanceType")
	if instanceType == "" {
		instanceType = "ml.t3.medium"
	}
	roleARN := strParam(params, "RoleArn")
	arn := shared.BuildARN("sagemaker", "notebook-instance", name)
	nb, err := p.store.CreateNotebookInstance(name, arn, "Pending", instanceType, roleARN, "")
	if err != nil {
		if sqliteIsUnique(err) {
			return shared.JSONError("ResourceInUse", "notebook instance already exists: "+name, http.StatusBadRequest), nil
		}
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"NotebookInstanceArn": nb.ARN})
}

func (p *Provider) describeNotebookInstance(params map[string]any) (*plugin.Response, error) {
	name := strParam(params, "NotebookInstanceName")
	nb, err := p.store.GetNotebookInstance(name)
	if err != nil {
		return shared.JSONError("ResourceNotFound", "notebook instance not found: "+name, http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"NotebookInstanceName":   nb.Name,
		"NotebookInstanceArn":    nb.ARN,
		"NotebookInstanceStatus": nb.Status,
		"InstanceType":           nb.InstanceType,
		"RoleArn":                nb.RoleARN,
		"Url":                    nb.URL,
		"CreationTime":           nb.CreatedAt.Unix(),
	})
}

func (p *Provider) listNotebookInstances(_ map[string]any) (*plugin.Response, error) {
	nbs, err := p.store.ListNotebookInstances()
	if err != nil {
		return nil, err
	}
	items := make([]map[string]any, 0, len(nbs))
	for _, nb := range nbs {
		items = append(items, map[string]any{
			"NotebookInstanceName":   nb.Name,
			"NotebookInstanceArn":    nb.ARN,
			"NotebookInstanceStatus": nb.Status,
			"InstanceType":           nb.InstanceType,
			"CreationTime":           nb.CreatedAt.Unix(),
		})
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"NotebookInstances": items})
}

func (p *Provider) deleteNotebookInstance(params map[string]any) (*plugin.Response, error) {
	name := strParam(params, "NotebookInstanceName")
	if err := p.store.DeleteNotebookInstance(name); err != nil {
		return shared.JSONError("ResourceNotFound", "notebook instance not found: "+name, http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) startNotebookInstance(params map[string]any) (*plugin.Response, error) {
	name := strParam(params, "NotebookInstanceName")
	if err := p.store.UpdateNotebookInstanceStatus(name, "InService"); err != nil {
		return shared.JSONError("ResourceNotFound", "notebook instance not found: "+name, http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) stopNotebookInstance(params map[string]any) (*plugin.Response, error) {
	name := strParam(params, "NotebookInstanceName")
	if err := p.store.UpdateNotebookInstanceStatus(name, "Stopped"); err != nil {
		return shared.JSONError("ResourceNotFound", "notebook instance not found: "+name, http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) updateNotebookInstance(params map[string]any) (*plugin.Response, error) {
	name := strParam(params, "NotebookInstanceName")
	instanceType := strParam(params, "InstanceType")
	if instanceType == "" {
		nb, err := p.store.GetNotebookInstance(name)
		if err != nil {
			return shared.JSONError("ResourceNotFound", "notebook instance not found: "+name, http.StatusBadRequest), nil
		}
		instanceType = nb.InstanceType
	}
	roleARN := strParam(params, "RoleArn")
	if roleARN == "" {
		nb, err := p.store.GetNotebookInstance(name)
		if err != nil {
			return shared.JSONError("ResourceNotFound", "notebook instance not found: "+name, http.StatusBadRequest), nil
		}
		roleARN = nb.RoleARN
	}
	if err := p.store.UpdateNotebookInstance(name, instanceType, roleARN); err != nil {
		return shared.JSONError("ResourceNotFound", "notebook instance not found: "+name, http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

// ---- Model handlers ----

func (p *Provider) createModel(params map[string]any) (*plugin.Response, error) {
	name := strParam(params, "ModelName")
	if name == "" {
		return shared.JSONError("ValidationException", "ModelName is required", http.StatusBadRequest), nil
	}
	executionRole := strParam(params, "ExecutionRoleArn")
	primaryContainer := marshalParam(params, "PrimaryContainer")
	arn := shared.BuildARN("sagemaker", "model", name)
	m, err := p.store.CreateModel(name, arn, executionRole, primaryContainer)
	if err != nil {
		if sqliteIsUnique(err) {
			return shared.JSONError("ResourceInUse", "model already exists: "+name, http.StatusBadRequest), nil
		}
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"ModelArn": m.ARN})
}

func (p *Provider) describeModel(params map[string]any) (*plugin.Response, error) {
	name := strParam(params, "ModelName")
	m, err := p.store.GetModel(name)
	if err != nil {
		return shared.JSONError("ResourceNotFound", "model not found: "+name, http.StatusBadRequest), nil
	}
	var container any
	json.Unmarshal([]byte(m.PrimaryContainer), &container)
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"ModelName":        m.Name,
		"ModelArn":         m.ARN,
		"ExecutionRoleArn": m.ExecutionRole,
		"PrimaryContainer": container,
		"CreationTime":     m.CreatedAt.Unix(),
	})
}

func (p *Provider) listModels(_ map[string]any) (*plugin.Response, error) {
	models, err := p.store.ListModels()
	if err != nil {
		return nil, err
	}
	items := make([]map[string]any, 0, len(models))
	for _, m := range models {
		items = append(items, map[string]any{
			"ModelName":    m.Name,
			"ModelArn":     m.ARN,
			"CreationTime": m.CreatedAt.Unix(),
		})
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"Models": items})
}

func (p *Provider) deleteModel(params map[string]any) (*plugin.Response, error) {
	name := strParam(params, "ModelName")
	if err := p.store.DeleteModel(name); err != nil {
		return shared.JSONError("ResourceNotFound", "model not found: "+name, http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

// ---- EndpointConfig handlers ----

func (p *Provider) createEndpointConfig(params map[string]any) (*plugin.Response, error) {
	name := strParam(params, "EndpointConfigName")
	if name == "" {
		return shared.JSONError("ValidationException", "EndpointConfigName is required", http.StatusBadRequest), nil
	}
	productionVariants := marshalParamArray(params, "ProductionVariants")
	arn := shared.BuildARN("sagemaker", "endpoint-config", name)
	ec, err := p.store.CreateEndpointConfig(name, arn, productionVariants)
	if err != nil {
		if sqliteIsUnique(err) {
			return shared.JSONError("ResourceInUse", "endpoint config already exists: "+name, http.StatusBadRequest), nil
		}
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"EndpointConfigArn": ec.ARN})
}

func (p *Provider) describeEndpointConfig(params map[string]any) (*plugin.Response, error) {
	name := strParam(params, "EndpointConfigName")
	ec, err := p.store.GetEndpointConfig(name)
	if err != nil {
		return shared.JSONError("ResourceNotFound", "endpoint config not found: "+name, http.StatusBadRequest), nil
	}
	var variants any
	json.Unmarshal([]byte(ec.ProductionVariants), &variants)
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"EndpointConfigName": ec.Name,
		"EndpointConfigArn":  ec.ARN,
		"ProductionVariants": variants,
		"CreationTime":       ec.CreatedAt.Unix(),
	})
}

func (p *Provider) listEndpointConfigs(_ map[string]any) (*plugin.Response, error) {
	configs, err := p.store.ListEndpointConfigs()
	if err != nil {
		return nil, err
	}
	items := make([]map[string]any, 0, len(configs))
	for _, ec := range configs {
		items = append(items, map[string]any{
			"EndpointConfigName": ec.Name,
			"EndpointConfigArn":  ec.ARN,
			"CreationTime":       ec.CreatedAt.Unix(),
		})
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"EndpointConfigs": items})
}

func (p *Provider) deleteEndpointConfig(params map[string]any) (*plugin.Response, error) {
	name := strParam(params, "EndpointConfigName")
	if err := p.store.DeleteEndpointConfig(name); err != nil {
		return shared.JSONError("ResourceNotFound", "endpoint config not found: "+name, http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

// ---- Endpoint handlers ----

func (p *Provider) createEndpoint(params map[string]any) (*plugin.Response, error) {
	name := strParam(params, "EndpointName")
	if name == "" {
		return shared.JSONError("ValidationException", "EndpointName is required", http.StatusBadRequest), nil
	}
	configName := strParam(params, "EndpointConfigName")
	arn := shared.BuildARN("sagemaker", "endpoint", name)
	e, err := p.store.CreateEndpoint(name, arn, configName, "Creating")
	if err != nil {
		if sqliteIsUnique(err) {
			return shared.JSONError("ResourceInUse", "endpoint already exists: "+name, http.StatusBadRequest), nil
		}
		return nil, err
	}
	// transition to InService immediately
	_ = p.store.UpdateEndpoint(name, configName)
	return shared.JSONResponse(http.StatusOK, map[string]any{"EndpointArn": e.ARN})
}

func (p *Provider) describeEndpoint(params map[string]any) (*plugin.Response, error) {
	name := strParam(params, "EndpointName")
	e, err := p.store.GetEndpoint(name)
	if err != nil {
		return shared.JSONError("ResourceNotFound", "endpoint not found: "+name, http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"EndpointName":       e.Name,
		"EndpointArn":        e.ARN,
		"EndpointConfigName": e.ConfigName,
		"EndpointStatus":     e.Status,
		"CreationTime":       e.CreatedAt.Unix(),
		"LastModifiedTime":   e.UpdatedAt.Unix(),
	})
}

func (p *Provider) listEndpoints(_ map[string]any) (*plugin.Response, error) {
	endpoints, err := p.store.ListEndpoints()
	if err != nil {
		return nil, err
	}
	items := make([]map[string]any, 0, len(endpoints))
	for _, e := range endpoints {
		items = append(items, map[string]any{
			"EndpointName":   e.Name,
			"EndpointArn":    e.ARN,
			"EndpointStatus": e.Status,
			"CreationTime":   e.CreatedAt.Unix(),
		})
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"Endpoints": items})
}

func (p *Provider) updateEndpoint(params map[string]any) (*plugin.Response, error) {
	name := strParam(params, "EndpointName")
	configName := strParam(params, "EndpointConfigName")
	e, err := p.store.GetEndpoint(name)
	if err != nil {
		return shared.JSONError("ResourceNotFound", "endpoint not found: "+name, http.StatusBadRequest), nil
	}
	if err := p.store.UpdateEndpoint(name, configName); err != nil {
		return shared.JSONError("ResourceNotFound", "endpoint not found: "+name, http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"EndpointArn": e.ARN})
}

func (p *Provider) deleteEndpoint(params map[string]any) (*plugin.Response, error) {
	name := strParam(params, "EndpointName")
	if err := p.store.DeleteEndpoint(name); err != nil {
		return shared.JSONError("ResourceNotFound", "endpoint not found: "+name, http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

// ---- TrainingJob handlers ----

func (p *Provider) createTrainingJob(params map[string]any) (*plugin.Response, error) {
	name := strParam(params, "TrainingJobName")
	if name == "" {
		return shared.JSONError("ValidationException", "TrainingJobName is required", http.StatusBadRequest), nil
	}
	roleARN := strParam(params, "RoleArn")
	algorithm := marshalParam(params, "AlgorithmSpecification")
	inputConfig := marshalParamArray(params, "InputDataConfig")
	outputConfig := marshalParam(params, "OutputDataConfig")
	resourceConfig := marshalParam(params, "ResourceConfig")
	arn := shared.BuildARN("sagemaker", "training-job", name)
	tj, err := p.store.CreateTrainingJob(name, arn, "Completed", roleARN, algorithm, inputConfig, outputConfig, resourceConfig)
	if err != nil {
		if sqliteIsUnique(err) {
			return shared.JSONError("ResourceInUse", "training job already exists: "+name, http.StatusBadRequest), nil
		}
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"TrainingJobArn": tj.ARN})
}

func (p *Provider) describeTrainingJob(params map[string]any) (*plugin.Response, error) {
	name := strParam(params, "TrainingJobName")
	tj, err := p.store.GetTrainingJob(name)
	if err != nil {
		return shared.JSONError("ResourceNotFound", "training job not found: "+name, http.StatusBadRequest), nil
	}
	var algorithm, inputConfig, outputConfig, resourceConfig any
	json.Unmarshal([]byte(tj.Algorithm), &algorithm)
	json.Unmarshal([]byte(tj.InputConfig), &inputConfig)
	json.Unmarshal([]byte(tj.OutputConfig), &outputConfig)
	json.Unmarshal([]byte(tj.ResourceConfig), &resourceConfig)
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"TrainingJobName":        tj.Name,
		"TrainingJobArn":         tj.ARN,
		"TrainingJobStatus":      tj.Status,
		"RoleArn":                tj.RoleARN,
		"AlgorithmSpecification": algorithm,
		"InputDataConfig":        inputConfig,
		"OutputDataConfig":       outputConfig,
		"ResourceConfig":         resourceConfig,
		"CreationTime":           tj.CreatedAt.Unix(),
	})
}

func (p *Provider) listTrainingJobs(_ map[string]any) (*plugin.Response, error) {
	jobs, err := p.store.ListTrainingJobs()
	if err != nil {
		return nil, err
	}
	items := make([]map[string]any, 0, len(jobs))
	for _, tj := range jobs {
		items = append(items, map[string]any{
			"TrainingJobName":   tj.Name,
			"TrainingJobArn":    tj.ARN,
			"TrainingJobStatus": tj.Status,
			"CreationTime":      tj.CreatedAt.Unix(),
		})
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"TrainingJobSummaries": items})
}

func (p *Provider) stopTrainingJob(params map[string]any) (*plugin.Response, error) {
	name := strParam(params, "TrainingJobName")
	if err := p.store.UpdateTrainingJobStatus(name, "Stopped"); err != nil {
		return shared.JSONError("ResourceNotFound", "training job not found: "+name, http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

// ---- ProcessingJob handlers ----

func (p *Provider) createProcessingJob(params map[string]any) (*plugin.Response, error) {
	name := strParam(params, "ProcessingJobName")
	if name == "" {
		return shared.JSONError("ValidationException", "ProcessingJobName is required", http.StatusBadRequest), nil
	}
	roleARN := strParam(params, "RoleArn")
	appSpec := marshalParam(params, "AppSpecification")
	inputs := marshalParamArray(params, "ProcessingInputs")
	outputs := marshalParamArray(params, "ProcessingOutputConfig")
	resources := marshalParam(params, "ProcessingResources")
	arn := shared.BuildARN("sagemaker", "processing-job", name)
	pj, err := p.store.CreateProcessingJob(name, arn, "Completed", roleARN, appSpec, inputs, outputs, resources)
	if err != nil {
		if sqliteIsUnique(err) {
			return shared.JSONError("ResourceInUse", "processing job already exists: "+name, http.StatusBadRequest), nil
		}
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"ProcessingJobArn": pj.ARN})
}

func (p *Provider) describeProcessingJob(params map[string]any) (*plugin.Response, error) {
	name := strParam(params, "ProcessingJobName")
	pj, err := p.store.GetProcessingJob(name)
	if err != nil {
		return shared.JSONError("ResourceNotFound", "processing job not found: "+name, http.StatusBadRequest), nil
	}
	var appSpec, inputs, outputs, resources any
	json.Unmarshal([]byte(pj.AppSpec), &appSpec)
	json.Unmarshal([]byte(pj.Inputs), &inputs)
	json.Unmarshal([]byte(pj.Outputs), &outputs)
	json.Unmarshal([]byte(pj.Resources), &resources)
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"ProcessingJobName":      pj.Name,
		"ProcessingJobArn":       pj.ARN,
		"ProcessingJobStatus":    pj.Status,
		"RoleArn":                pj.RoleARN,
		"AppSpecification":       appSpec,
		"ProcessingInputs":       inputs,
		"ProcessingOutputConfig": outputs,
		"ProcessingResources":    resources,
		"CreationTime":           pj.CreatedAt.Unix(),
	})
}

func (p *Provider) listProcessingJobs(_ map[string]any) (*plugin.Response, error) {
	jobs, err := p.store.ListProcessingJobs()
	if err != nil {
		return nil, err
	}
	items := make([]map[string]any, 0, len(jobs))
	for _, pj := range jobs {
		items = append(items, map[string]any{
			"ProcessingJobName":   pj.Name,
			"ProcessingJobArn":    pj.ARN,
			"ProcessingJobStatus": pj.Status,
			"CreationTime":        pj.CreatedAt.Unix(),
		})
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"ProcessingJobSummaries": items})
}

func (p *Provider) stopProcessingJob(params map[string]any) (*plugin.Response, error) {
	name := strParam(params, "ProcessingJobName")
	if err := p.store.UpdateProcessingJobStatus(name, "Stopped"); err != nil {
		return shared.JSONError("ResourceNotFound", "processing job not found: "+name, http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

// ---- TransformJob handlers ----

func (p *Provider) createTransformJob(params map[string]any) (*plugin.Response, error) {
	name := strParam(params, "TransformJobName")
	if name == "" {
		return shared.JSONError("ValidationException", "TransformJobName is required", http.StatusBadRequest), nil
	}
	modelName := strParam(params, "ModelName")
	input := marshalParam(params, "TransformInput")
	output := marshalParam(params, "TransformOutput")
	resources := marshalParam(params, "TransformResources")
	arn := shared.BuildARN("sagemaker", "transform-job", name)
	tj, err := p.store.CreateTransformJob(name, arn, "Completed", modelName, input, output, resources)
	if err != nil {
		if sqliteIsUnique(err) {
			return shared.JSONError("ResourceInUse", "transform job already exists: "+name, http.StatusBadRequest), nil
		}
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"TransformJobArn": tj.ARN})
}

func (p *Provider) describeTransformJob(params map[string]any) (*plugin.Response, error) {
	name := strParam(params, "TransformJobName")
	tj, err := p.store.GetTransformJob(name)
	if err != nil {
		return shared.JSONError("ResourceNotFound", "transform job not found: "+name, http.StatusBadRequest), nil
	}
	var input, output, resources any
	json.Unmarshal([]byte(tj.Input), &input)
	json.Unmarshal([]byte(tj.Output), &output)
	json.Unmarshal([]byte(tj.Resources), &resources)
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"TransformJobName":   tj.Name,
		"TransformJobArn":    tj.ARN,
		"TransformJobStatus": tj.Status,
		"ModelName":          tj.ModelName,
		"TransformInput":     input,
		"TransformOutput":    output,
		"TransformResources": resources,
		"CreationTime":       tj.CreatedAt.Unix(),
	})
}

func (p *Provider) listTransformJobs(_ map[string]any) (*plugin.Response, error) {
	jobs, err := p.store.ListTransformJobs()
	if err != nil {
		return nil, err
	}
	items := make([]map[string]any, 0, len(jobs))
	for _, tj := range jobs {
		items = append(items, map[string]any{
			"TransformJobName":   tj.Name,
			"TransformJobArn":    tj.ARN,
			"TransformJobStatus": tj.Status,
			"CreationTime":       tj.CreatedAt.Unix(),
		})
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"TransformJobSummaries": items})
}

func (p *Provider) stopTransformJob(params map[string]any) (*plugin.Response, error) {
	name := strParam(params, "TransformJobName")
	if err := p.store.UpdateTransformJobStatus(name, "Stopped"); err != nil {
		return shared.JSONError("ResourceNotFound", "transform job not found: "+name, http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

// ---- Pipeline handlers ----

func (p *Provider) createPipeline(params map[string]any) (*plugin.Response, error) {
	name := strParam(params, "PipelineName")
	if name == "" {
		return shared.JSONError("ValidationException", "PipelineName is required", http.StatusBadRequest), nil
	}
	roleARN := strParam(params, "RoleArn")
	definition := strParam(params, "PipelineDefinition")
	description := strParam(params, "PipelineDescription")
	arn := shared.BuildARN("sagemaker", "pipeline", name)
	pl, err := p.store.CreatePipeline(name, arn, roleARN, definition, description)
	if err != nil {
		if sqliteIsUnique(err) {
			return shared.JSONError("ResourceInUse", "pipeline already exists: "+name, http.StatusBadRequest), nil
		}
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"PipelineArn": pl.ARN})
}

func (p *Provider) describePipeline(params map[string]any) (*plugin.Response, error) {
	name := strParam(params, "PipelineName")
	pl, err := p.store.GetPipeline(name)
	if err != nil {
		return shared.JSONError("ResourceNotFound", "pipeline not found: "+name, http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"PipelineName":        pl.Name,
		"PipelineArn":         pl.ARN,
		"RoleArn":             pl.RoleARN,
		"PipelineDefinition":  pl.Definition,
		"PipelineDescription": pl.Description,
		"PipelineStatus":      "Active",
		"CreationTime":        pl.CreatedAt.Unix(),
		"LastModifiedTime":    pl.UpdatedAt.Unix(),
	})
}

func (p *Provider) listPipelines(_ map[string]any) (*plugin.Response, error) {
	pipelines, err := p.store.ListPipelines()
	if err != nil {
		return nil, err
	}
	items := make([]map[string]any, 0, len(pipelines))
	for _, pl := range pipelines {
		items = append(items, map[string]any{
			"PipelineName":     pl.Name,
			"PipelineArn":      pl.ARN,
			"PipelineStatus":   "Active",
			"CreationTime":     pl.CreatedAt.Unix(),
			"LastModifiedTime": pl.UpdatedAt.Unix(),
		})
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"PipelineSummaries": items})
}

func (p *Provider) updatePipeline(params map[string]any) (*plugin.Response, error) {
	name := strParam(params, "PipelineName")
	pl, err := p.store.GetPipeline(name)
	if err != nil {
		return shared.JSONError("ResourceNotFound", "pipeline not found: "+name, http.StatusBadRequest), nil
	}
	roleARN := strParam(params, "RoleArn")
	if roleARN == "" {
		roleARN = pl.RoleARN
	}
	definition := strParam(params, "PipelineDefinition")
	if definition == "" {
		definition = pl.Definition
	}
	description := strParam(params, "PipelineDescription")
	if description == "" {
		description = pl.Description
	}
	if err := p.store.UpdatePipeline(name, roleARN, definition, description); err != nil {
		return shared.JSONError("ResourceNotFound", "pipeline not found: "+name, http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"PipelineArn": pl.ARN})
}

func (p *Provider) deletePipeline(params map[string]any) (*plugin.Response, error) {
	name := strParam(params, "PipelineName")
	pl, err := p.store.GetPipeline(name)
	if err != nil {
		return shared.JSONError("ResourceNotFound", "pipeline not found: "+name, http.StatusBadRequest), nil
	}
	if err := p.store.DeletePipeline(name); err != nil {
		return shared.JSONError("ResourceNotFound", "pipeline not found: "+name, http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"PipelineArn": pl.ARN})
}

func (p *Provider) startPipelineExecution(params map[string]any) (*plugin.Response, error) {
	name := strParam(params, "PipelineName")
	if _, err := p.store.GetPipeline(name); err != nil {
		return shared.JSONError("ResourceNotFound", "pipeline not found: "+name, http.StatusBadRequest), nil
	}
	execARN := shared.BuildARN("sagemaker", "pipeline", name+"/execution/"+shared.GenerateID("", 12))
	pe, err := p.store.CreatePipelineExecution(execARN, name, "Succeeded")
	if err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"PipelineExecutionArn": pe.ARN})
}

func (p *Provider) describePipelineExecution(params map[string]any) (*plugin.Response, error) {
	arn := strParam(params, "PipelineExecutionArn")
	pe, err := p.store.GetPipelineExecution(arn)
	if err != nil {
		return shared.JSONError("ResourceNotFound", "pipeline execution not found: "+arn, http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"PipelineExecutionArn":    pe.ARN,
		"PipelineName":            pe.PipelineName,
		"PipelineExecutionStatus": pe.Status,
		"CreationTime":            pe.CreatedAt.Unix(),
	})
}

func (p *Provider) listPipelineExecutions(params map[string]any) (*plugin.Response, error) {
	name := strParam(params, "PipelineName")
	executions, err := p.store.ListPipelineExecutions(name)
	if err != nil {
		return nil, err
	}
	items := make([]map[string]any, 0, len(executions))
	for _, pe := range executions {
		items = append(items, map[string]any{
			"PipelineExecutionArn":    pe.ARN,
			"PipelineExecutionStatus": pe.Status,
			"CreationTime":            pe.CreatedAt.Unix(),
		})
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"PipelineExecutionSummaries": items})
}

func (p *Provider) stopPipelineExecution(params map[string]any) (*plugin.Response, error) {
	arn := strParam(params, "PipelineExecutionArn")
	if err := p.store.UpdatePipelineExecutionStatus(arn, "Stopped"); err != nil {
		return shared.JSONError("ResourceNotFound", "pipeline execution not found: "+arn, http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"PipelineExecutionArn": arn})
}

// ---- Experiment handlers ----

func (p *Provider) createExperiment(params map[string]any) (*plugin.Response, error) {
	name := strParam(params, "ExperimentName")
	if name == "" {
		return shared.JSONError("ValidationException", "ExperimentName is required", http.StatusBadRequest), nil
	}
	description := strParam(params, "Description")
	arn := shared.BuildARN("sagemaker", "experiment", name)
	e, err := p.store.CreateExperiment(name, arn, description)
	if err != nil {
		if sqliteIsUnique(err) {
			return shared.JSONError("ResourceInUse", "experiment already exists: "+name, http.StatusBadRequest), nil
		}
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"ExperimentArn": e.ARN})
}

func (p *Provider) describeExperiment(params map[string]any) (*plugin.Response, error) {
	name := strParam(params, "ExperimentName")
	e, err := p.store.GetExperiment(name)
	if err != nil {
		return shared.JSONError("ResourceNotFound", "experiment not found: "+name, http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"ExperimentName": e.Name,
		"ExperimentArn":  e.ARN,
		"Description":    e.Description,
		"CreationTime":   e.CreatedAt.Unix(),
	})
}

func (p *Provider) listExperiments(_ map[string]any) (*plugin.Response, error) {
	experiments, err := p.store.ListExperiments()
	if err != nil {
		return nil, err
	}
	items := make([]map[string]any, 0, len(experiments))
	for _, e := range experiments {
		items = append(items, map[string]any{
			"ExperimentName": e.Name,
			"ExperimentArn":  e.ARN,
			"CreationTime":   e.CreatedAt.Unix(),
		})
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"ExperimentSummaries": items})
}

func (p *Provider) updateExperiment(params map[string]any) (*plugin.Response, error) {
	name := strParam(params, "ExperimentName")
	description := strParam(params, "Description")
	e, err := p.store.GetExperiment(name)
	if err != nil {
		return shared.JSONError("ResourceNotFound", "experiment not found: "+name, http.StatusBadRequest), nil
	}
	if err := p.store.UpdateExperiment(name, description); err != nil {
		return shared.JSONError("ResourceNotFound", "experiment not found: "+name, http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"ExperimentArn": e.ARN})
}

func (p *Provider) deleteExperiment(params map[string]any) (*plugin.Response, error) {
	name := strParam(params, "ExperimentName")
	e, err := p.store.GetExperiment(name)
	if err != nil {
		return shared.JSONError("ResourceNotFound", "experiment not found: "+name, http.StatusBadRequest), nil
	}
	if err := p.store.DeleteExperiment(name); err != nil {
		return shared.JSONError("ResourceNotFound", "experiment not found: "+name, http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"ExperimentArn": e.ARN})
}

// ---- Trial handlers ----

func (p *Provider) createTrial(params map[string]any) (*plugin.Response, error) {
	name := strParam(params, "TrialName")
	if name == "" {
		return shared.JSONError("ValidationException", "TrialName is required", http.StatusBadRequest), nil
	}
	experimentName := strParam(params, "ExperimentName")
	arn := shared.BuildARN("sagemaker", "experiment-trial", name)
	tr, err := p.store.CreateTrial(name, arn, experimentName)
	if err != nil {
		if sqliteIsUnique(err) {
			return shared.JSONError("ResourceInUse", "trial already exists: "+name, http.StatusBadRequest), nil
		}
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"TrialArn": tr.ARN})
}

func (p *Provider) describeTrial(params map[string]any) (*plugin.Response, error) {
	name := strParam(params, "TrialName")
	tr, err := p.store.GetTrial(name)
	if err != nil {
		return shared.JSONError("ResourceNotFound", "trial not found: "+name, http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"TrialName":      tr.Name,
		"TrialArn":       tr.ARN,
		"ExperimentName": tr.ExperimentName,
		"CreationTime":   tr.CreatedAt.Unix(),
	})
}

func (p *Provider) listTrials(params map[string]any) (*plugin.Response, error) {
	experimentName := strParam(params, "ExperimentName")
	trials, err := p.store.ListTrials(experimentName)
	if err != nil {
		return nil, err
	}
	items := make([]map[string]any, 0, len(trials))
	for _, tr := range trials {
		items = append(items, map[string]any{
			"TrialName":      tr.Name,
			"TrialArn":       tr.ARN,
			"ExperimentName": tr.ExperimentName,
			"CreationTime":   tr.CreatedAt.Unix(),
		})
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"TrialSummaries": items})
}

func (p *Provider) updateTrial(params map[string]any) (*plugin.Response, error) {
	name := strParam(params, "TrialName")
	experimentName := strParam(params, "ExperimentName")
	tr, err := p.store.GetTrial(name)
	if err != nil {
		return shared.JSONError("ResourceNotFound", "trial not found: "+name, http.StatusBadRequest), nil
	}
	if experimentName == "" {
		experimentName = tr.ExperimentName
	}
	if err := p.store.UpdateTrial(name, experimentName); err != nil {
		return shared.JSONError("ResourceNotFound", "trial not found: "+name, http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"TrialArn": tr.ARN})
}

func (p *Provider) deleteTrial(params map[string]any) (*plugin.Response, error) {
	name := strParam(params, "TrialName")
	tr, err := p.store.GetTrial(name)
	if err != nil {
		return shared.JSONError("ResourceNotFound", "trial not found: "+name, http.StatusBadRequest), nil
	}
	if err := p.store.DeleteTrial(name); err != nil {
		return shared.JSONError("ResourceNotFound", "trial not found: "+name, http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"TrialArn": tr.ARN})
}

// ---- Domain handlers ----

func (p *Provider) createDomain(params map[string]any) (*plugin.Response, error) {
	name := strParam(params, "DomainName")
	if name == "" {
		return shared.JSONError("ValidationException", "DomainName is required", http.StatusBadRequest), nil
	}
	authMode := strParam(params, "AuthMode")
	if authMode == "" {
		authMode = "IAM"
	}
	vpcID := strParam(params, "VpcId")
	id := shared.GenerateID("d-", 16)
	arn := shared.BuildARN("sagemaker", "domain", id)
	d, err := p.store.CreateDomain(id, arn, name, "InService", authMode, vpcID)
	if err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"DomainId":  d.ID,
		"DomainArn": d.ARN,
	})
}

func (p *Provider) describeDomain(params map[string]any) (*plugin.Response, error) {
	id := strParam(params, "DomainId")
	d, err := p.store.GetDomain(id)
	if err != nil {
		return shared.JSONError("ResourceNotFound", "domain not found: "+id, http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"DomainId":     d.ID,
		"DomainArn":    d.ARN,
		"DomainName":   d.Name,
		"Status":       d.Status,
		"AuthMode":     d.AuthMode,
		"VpcId":        d.VpcID,
		"CreationTime": d.CreatedAt.Unix(),
	})
}

func (p *Provider) listDomains(_ map[string]any) (*plugin.Response, error) {
	domains, err := p.store.ListDomains()
	if err != nil {
		return nil, err
	}
	items := make([]map[string]any, 0, len(domains))
	for _, d := range domains {
		items = append(items, map[string]any{
			"DomainId":     d.ID,
			"DomainArn":    d.ARN,
			"DomainName":   d.Name,
			"Status":       d.Status,
			"CreationTime": d.CreatedAt.Unix(),
		})
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"Domains": items})
}

func (p *Provider) updateDomain(params map[string]any) (*plugin.Response, error) {
	id := strParam(params, "DomainId")
	d, err := p.store.GetDomain(id)
	if err != nil {
		return shared.JSONError("ResourceNotFound", "domain not found: "+id, http.StatusBadRequest), nil
	}
	vpcID := strParam(params, "VpcId")
	if vpcID == "" {
		vpcID = d.VpcID
	}
	if err := p.store.UpdateDomain(id, vpcID); err != nil {
		return shared.JSONError("ResourceNotFound", "domain not found: "+id, http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"DomainArn": d.ARN})
}

func (p *Provider) deleteDomain(params map[string]any) (*plugin.Response, error) {
	id := strParam(params, "DomainId")
	if err := p.store.DeleteDomain(id); err != nil {
		return shared.JSONError("ResourceNotFound", "domain not found: "+id, http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

// ---- UserProfile handlers ----

func (p *Provider) createUserProfile(params map[string]any) (*plugin.Response, error) {
	domainID := strParam(params, "DomainId")
	name := strParam(params, "UserProfileName")
	if domainID == "" || name == "" {
		return shared.JSONError("ValidationException", "DomainId and UserProfileName are required", http.StatusBadRequest), nil
	}
	arn := shared.BuildARN("sagemaker", "user-profile", domainID+"/"+name)
	up, err := p.store.CreateUserProfile(domainID, name, arn, "InService")
	if err != nil {
		if sqliteIsUnique(err) {
			return shared.JSONError("ResourceInUse", "user profile already exists: "+name, http.StatusBadRequest), nil
		}
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"UserProfileArn": up.ARN})
}

func (p *Provider) describeUserProfile(params map[string]any) (*plugin.Response, error) {
	domainID := strParam(params, "DomainId")
	name := strParam(params, "UserProfileName")
	up, err := p.store.GetUserProfile(domainID, name)
	if err != nil {
		return shared.JSONError("ResourceNotFound", "user profile not found: "+name, http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"DomainId":        up.DomainID,
		"UserProfileName": up.Name,
		"UserProfileArn":  up.ARN,
		"Status":          up.Status,
		"CreationTime":    up.CreatedAt.Unix(),
	})
}

func (p *Provider) listUserProfiles(params map[string]any) (*plugin.Response, error) {
	domainID := strParam(params, "DomainIdEquals")
	profiles, err := p.store.ListUserProfiles(domainID)
	if err != nil {
		return nil, err
	}
	items := make([]map[string]any, 0, len(profiles))
	for _, up := range profiles {
		items = append(items, map[string]any{
			"DomainId":        up.DomainID,
			"UserProfileName": up.Name,
			"UserProfileArn":  up.ARN,
			"Status":          up.Status,
			"CreationTime":    up.CreatedAt.Unix(),
		})
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"UserProfiles": items})
}

func (p *Provider) updateUserProfile(params map[string]any) (*plugin.Response, error) {
	domainID := strParam(params, "DomainId")
	name := strParam(params, "UserProfileName")
	up, err := p.store.GetUserProfile(domainID, name)
	if err != nil {
		return shared.JSONError("ResourceNotFound", "user profile not found: "+name, http.StatusBadRequest), nil
	}
	if err := p.store.UpdateUserProfile(domainID, name, up.Status); err != nil {
		return shared.JSONError("ResourceNotFound", "user profile not found: "+name, http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"UserProfileArn": up.ARN})
}

func (p *Provider) deleteUserProfile(params map[string]any) (*plugin.Response, error) {
	domainID := strParam(params, "DomainId")
	name := strParam(params, "UserProfileName")
	if err := p.store.DeleteUserProfile(domainID, name); err != nil {
		return shared.JSONError("ResourceNotFound", "user profile not found: "+name, http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

// ---- Tags handlers ----

func (p *Provider) addTags(params map[string]any) (*plugin.Response, error) {
	resourceARN := strParam(params, "ResourceArn")
	tagsRaw, _ := params["Tags"].([]any)
	tags := make(map[string]string, len(tagsRaw))
	for _, t := range tagsRaw {
		if tm, ok := t.(map[string]any); ok {
			k, _ := tm["Key"].(string)
			v, _ := tm["Value"].(string)
			if k != "" {
				tags[k] = v
			}
		}
	}
	if err := p.store.AddTags(resourceARN, tags); err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"Tags": tagsRaw})
}

func (p *Provider) deleteTags(params map[string]any) (*plugin.Response, error) {
	resourceARN := strParam(params, "ResourceArn")
	keys := stringsParam(params, "TagKeys")
	if err := p.store.RemoveTags(resourceARN, keys); err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) listTags(params map[string]any) (*plugin.Response, error) {
	resourceARN := strParam(params, "ResourceArn")
	tags, err := p.store.ListTagsForResource(resourceARN)
	if err != nil {
		return nil, err
	}
	items := make([]map[string]any, 0, len(tags))
	for k, v := range tags {
		items = append(items, map[string]any{"Key": k, "Value": v})
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"Tags": items})
}

// ---- helpers ----

func strParam(params map[string]any, key string) string {
	if v, ok := params[key]; ok {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}

func marshalParam(params map[string]any, key string) string {
	if v, ok := params[key]; ok {
		b, _ := json.Marshal(v)
		return string(b)
	}
	return "{}"
}

func marshalParamArray(params map[string]any, key string) string {
	if v, ok := params[key]; ok {
		b, _ := json.Marshal(v)
		return string(b)
	}
	return "[]"
}

func sqliteIsUnique(err error) bool {
	return err != nil && strings.Contains(err.Error(), "UNIQUE constraint failed")
}

func stringsParam(params map[string]any, key string) []string {
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
