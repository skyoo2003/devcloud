// SPDX-License-Identifier: Apache-2.0

// internal/services/sagemaker/provider_test.go
package sagemaker

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/skyoo2003/devcloud/internal/plugin"
)

func newTestProvider(t *testing.T) *Provider {
	t.Helper()
	p := &Provider{}
	if err := p.Init(plugin.PluginConfig{DataDir: t.TempDir()}); err != nil {
		t.Fatalf("Init: %v", err)
	}
	t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
	return p
}

func callOp(t *testing.T, p *Provider, op, body string) *plugin.Response {
	t.Helper()
	req := httptest.NewRequest("POST", "/"+op, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err := p.HandleRequest(context.Background(), op, req)
	if err != nil {
		t.Fatalf("%s: unexpected error: %v", op, err)
	}
	if resp == nil {
		t.Fatalf("%s: nil response", op)
	}
	return resp
}

func parseBody(t *testing.T, resp *plugin.Response) map[string]any {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal(resp.Body, &m); err != nil {
		t.Fatalf("unmarshal: %v (body=%s)", err, string(resp.Body))
	}
	return m
}

func assertOK(t *testing.T, resp *plugin.Response) {
	t.Helper()
	if resp.StatusCode != 200 {
		t.Fatalf("expected 200, got %d: %s", resp.StatusCode, string(resp.Body))
	}
}

func TestNotebookInstanceCRUD(t *testing.T) {
	p := newTestProvider(t)

	// Create
	resp := callOp(t, p, "CreateNotebookInstance", `{"NotebookInstanceName":"nb1","InstanceType":"ml.t3.medium","RoleArn":"arn:aws:iam::000000000000:role/test"}`)
	assertOK(t, resp)
	body := parseBody(t, resp)
	if body["NotebookInstanceArn"] == nil {
		t.Fatal("expected NotebookInstanceArn")
	}

	// Duplicate
	resp = callOp(t, p, "CreateNotebookInstance", `{"NotebookInstanceName":"nb1","InstanceType":"ml.t3.medium"}`)
	if resp.StatusCode == 200 {
		t.Fatal("expected error on duplicate")
	}

	// Describe
	resp = callOp(t, p, "DescribeNotebookInstance", `{"NotebookInstanceName":"nb1"}`)
	assertOK(t, resp)
	body = parseBody(t, resp)
	if body["NotebookInstanceName"] != "nb1" {
		t.Errorf("expected Name=nb1, got %v", body["NotebookInstanceName"])
	}

	// List
	callOp(t, p, "CreateNotebookInstance", `{"NotebookInstanceName":"nb2","InstanceType":"ml.t3.large"}`)
	resp = callOp(t, p, "ListNotebookInstances", `{}`)
	assertOK(t, resp)
	body = parseBody(t, resp)
	list, _ := body["NotebookInstances"].([]any)
	if len(list) < 2 {
		t.Errorf("expected at least 2 notebook instances, got %d", len(list))
	}

	// Start / Stop
	resp = callOp(t, p, "StopNotebookInstance", `{"NotebookInstanceName":"nb1"}`)
	assertOK(t, resp)
	resp = callOp(t, p, "DescribeNotebookInstance", `{"NotebookInstanceName":"nb1"}`)
	assertOK(t, resp)
	body = parseBody(t, resp)
	if body["NotebookInstanceStatus"] != "Stopped" {
		t.Errorf("expected Stopped, got %v", body["NotebookInstanceStatus"])
	}

	resp = callOp(t, p, "StartNotebookInstance", `{"NotebookInstanceName":"nb1"}`)
	assertOK(t, resp)
	resp = callOp(t, p, "DescribeNotebookInstance", `{"NotebookInstanceName":"nb1"}`)
	assertOK(t, resp)
	body = parseBody(t, resp)
	if body["NotebookInstanceStatus"] != "InService" {
		t.Errorf("expected InService, got %v", body["NotebookInstanceStatus"])
	}

	// Update
	resp = callOp(t, p, "UpdateNotebookInstance", `{"NotebookInstanceName":"nb1","InstanceType":"ml.t3.xlarge"}`)
	assertOK(t, resp)

	// Delete
	resp = callOp(t, p, "DeleteNotebookInstance", `{"NotebookInstanceName":"nb1"}`)
	assertOK(t, resp)

	// Not found after delete
	resp = callOp(t, p, "DescribeNotebookInstance", `{"NotebookInstanceName":"nb1"}`)
	if resp.StatusCode == 200 {
		t.Fatal("expected error after delete")
	}
}

func TestModelCRUD(t *testing.T) {
	p := newTestProvider(t)

	// Create
	resp := callOp(t, p, "CreateModel", `{"ModelName":"model1","ExecutionRoleArn":"arn:aws:iam::000000000000:role/test","PrimaryContainer":{"Image":"123456789.dkr.ecr.us-east-1.amazonaws.com/myimage:latest"}}`)
	assertOK(t, resp)
	body := parseBody(t, resp)
	if body["ModelArn"] == nil {
		t.Fatal("expected ModelArn")
	}

	// Duplicate
	resp = callOp(t, p, "CreateModel", `{"ModelName":"model1"}`)
	if resp.StatusCode == 200 {
		t.Fatal("expected error on duplicate")
	}

	// Describe
	resp = callOp(t, p, "DescribeModel", `{"ModelName":"model1"}`)
	assertOK(t, resp)
	body = parseBody(t, resp)
	if body["ModelName"] != "model1" {
		t.Errorf("expected ModelName=model1, got %v", body["ModelName"])
	}

	// List
	callOp(t, p, "CreateModel", `{"ModelName":"model2"}`)
	resp = callOp(t, p, "ListModels", `{}`)
	assertOK(t, resp)
	body = parseBody(t, resp)
	models, _ := body["Models"].([]any)
	if len(models) < 2 {
		t.Errorf("expected at least 2 models, got %d", len(models))
	}

	// Delete
	resp = callOp(t, p, "DeleteModel", `{"ModelName":"model1"}`)
	assertOK(t, resp)

	resp = callOp(t, p, "DescribeModel", `{"ModelName":"model1"}`)
	if resp.StatusCode == 200 {
		t.Fatal("expected error after delete")
	}
}

func TestEndpointCRUD(t *testing.T) {
	p := newTestProvider(t)

	// Create EndpointConfig
	resp := callOp(t, p, "CreateEndpointConfig", `{"EndpointConfigName":"cfg1","ProductionVariants":[{"VariantName":"v1","ModelName":"model1","InitialInstanceCount":1,"InstanceType":"ml.m5.large"}]}`)
	assertOK(t, resp)

	// Describe EndpointConfig
	resp = callOp(t, p, "DescribeEndpointConfig", `{"EndpointConfigName":"cfg1"}`)
	assertOK(t, resp)
	body := parseBody(t, resp)
	if body["EndpointConfigName"] != "cfg1" {
		t.Errorf("expected EndpointConfigName=cfg1, got %v", body["EndpointConfigName"])
	}

	// List EndpointConfigs
	callOp(t, p, "CreateEndpointConfig", `{"EndpointConfigName":"cfg2"}`)
	resp = callOp(t, p, "ListEndpointConfigs", `{}`)
	assertOK(t, resp)
	body = parseBody(t, resp)
	cfgs, _ := body["EndpointConfigs"].([]any)
	if len(cfgs) < 2 {
		t.Errorf("expected at least 2 endpoint configs, got %d", len(cfgs))
	}

	// Create Endpoint
	resp = callOp(t, p, "CreateEndpoint", `{"EndpointName":"ep1","EndpointConfigName":"cfg1"}`)
	assertOK(t, resp)
	body = parseBody(t, resp)
	if body["EndpointArn"] == nil {
		t.Fatal("expected EndpointArn")
	}

	// Duplicate Endpoint
	resp = callOp(t, p, "CreateEndpoint", `{"EndpointName":"ep1","EndpointConfigName":"cfg1"}`)
	if resp.StatusCode == 200 {
		t.Fatal("expected error on duplicate endpoint")
	}

	// Describe Endpoint
	resp = callOp(t, p, "DescribeEndpoint", `{"EndpointName":"ep1"}`)
	assertOK(t, resp)
	body = parseBody(t, resp)
	if body["EndpointName"] != "ep1" {
		t.Errorf("expected EndpointName=ep1, got %v", body["EndpointName"])
	}

	// List Endpoints
	callOp(t, p, "CreateEndpoint", `{"EndpointName":"ep2","EndpointConfigName":"cfg2"}`)
	resp = callOp(t, p, "ListEndpoints", `{}`)
	assertOK(t, resp)
	body = parseBody(t, resp)
	eps, _ := body["Endpoints"].([]any)
	if len(eps) < 2 {
		t.Errorf("expected at least 2 endpoints, got %d", len(eps))
	}

	// Update Endpoint
	resp = callOp(t, p, "UpdateEndpoint", `{"EndpointName":"ep1","EndpointConfigName":"cfg2"}`)
	assertOK(t, resp)

	// Delete Endpoint
	resp = callOp(t, p, "DeleteEndpoint", `{"EndpointName":"ep1"}`)
	assertOK(t, resp)

	// Delete EndpointConfig
	resp = callOp(t, p, "DeleteEndpointConfig", `{"EndpointConfigName":"cfg1"}`)
	assertOK(t, resp)

	resp = callOp(t, p, "DescribeEndpoint", `{"EndpointName":"ep1"}`)
	if resp.StatusCode == 200 {
		t.Fatal("expected error after delete")
	}
}

func TestTrainingJobCRUD(t *testing.T) {
	p := newTestProvider(t)

	// Create
	resp := callOp(t, p, "CreateTrainingJob", `{"TrainingJobName":"tj1","RoleArn":"arn:aws:iam::000000000000:role/test","AlgorithmSpecification":{"TrainingInputMode":"File"},"ResourceConfig":{"InstanceType":"ml.m5.xlarge","InstanceCount":1,"VolumeSizeInGB":50},"StoppingCondition":{"MaxRuntimeInSeconds":3600}}`)
	assertOK(t, resp)
	body := parseBody(t, resp)
	if body["TrainingJobArn"] == nil {
		t.Fatal("expected TrainingJobArn")
	}

	// Describe
	resp = callOp(t, p, "DescribeTrainingJob", `{"TrainingJobName":"tj1"}`)
	assertOK(t, resp)
	body = parseBody(t, resp)
	if body["TrainingJobName"] != "tj1" {
		t.Errorf("expected TrainingJobName=tj1, got %v", body["TrainingJobName"])
	}
	if body["TrainingJobStatus"] != "Completed" {
		t.Errorf("expected status=Completed, got %v", body["TrainingJobStatus"])
	}

	// List
	callOp(t, p, "CreateTrainingJob", `{"TrainingJobName":"tj2","RoleArn":"role2"}`)
	resp = callOp(t, p, "ListTrainingJobs", `{}`)
	assertOK(t, resp)
	body = parseBody(t, resp)
	jobs, _ := body["TrainingJobSummaries"].([]any)
	if len(jobs) < 2 {
		t.Errorf("expected at least 2 training jobs, got %d", len(jobs))
	}

	// Stop
	resp = callOp(t, p, "StopTrainingJob", `{"TrainingJobName":"tj1"}`)
	assertOK(t, resp)
	resp = callOp(t, p, "DescribeTrainingJob", `{"TrainingJobName":"tj1"}`)
	assertOK(t, resp)
	body = parseBody(t, resp)
	if body["TrainingJobStatus"] != "Stopped" {
		t.Errorf("expected Stopped, got %v", body["TrainingJobStatus"])
	}

	// Processing job
	resp = callOp(t, p, "CreateProcessingJob", `{"ProcessingJobName":"pj1","RoleArn":"role","AppSpecification":{"ImageUri":"img"},"ProcessingResources":{"ClusterConfig":{"InstanceCount":1,"InstanceType":"ml.m5.xlarge","VolumeSizeInGB":30}}}`)
	assertOK(t, resp)
	resp = callOp(t, p, "DescribeProcessingJob", `{"ProcessingJobName":"pj1"}`)
	assertOK(t, resp)
	body = parseBody(t, resp)
	if body["ProcessingJobName"] != "pj1" {
		t.Errorf("expected ProcessingJobName=pj1, got %v", body["ProcessingJobName"])
	}

	resp = callOp(t, p, "ListProcessingJobs", `{}`)
	assertOK(t, resp)
	body = parseBody(t, resp)
	pjobs, _ := body["ProcessingJobSummaries"].([]any)
	if len(pjobs) < 1 {
		t.Errorf("expected at least 1 processing job, got %d", len(pjobs))
	}

	resp = callOp(t, p, "StopProcessingJob", `{"ProcessingJobName":"pj1"}`)
	assertOK(t, resp)

	// Transform job
	resp = callOp(t, p, "CreateTransformJob", `{"TransformJobName":"xj1","ModelName":"model1","TransformInput":{"DataSource":{"S3DataSource":{"S3Uri":"s3://bucket/input"}}},"TransformOutput":{"S3OutputPath":"s3://bucket/output"},"TransformResources":{"InstanceType":"ml.m5.xlarge","InstanceCount":1}}`)
	assertOK(t, resp)
	resp = callOp(t, p, "DescribeTransformJob", `{"TransformJobName":"xj1"}`)
	assertOK(t, resp)
	body = parseBody(t, resp)
	if body["TransformJobName"] != "xj1" {
		t.Errorf("expected TransformJobName=xj1, got %v", body["TransformJobName"])
	}

	resp = callOp(t, p, "ListTransformJobs", `{}`)
	assertOK(t, resp)
	body = parseBody(t, resp)
	xjobs, _ := body["TransformJobSummaries"].([]any)
	if len(xjobs) < 1 {
		t.Errorf("expected at least 1 transform job, got %d", len(xjobs))
	}

	resp = callOp(t, p, "StopTransformJob", `{"TransformJobName":"xj1"}`)
	assertOK(t, resp)
}

func TestPipelineCRUD(t *testing.T) {
	p := newTestProvider(t)

	// Create
	resp := callOp(t, p, "CreatePipeline", `{"PipelineName":"pl1","RoleArn":"arn:aws:iam::000000000000:role/test","PipelineDefinition":"{\"Version\":\"2020-12-01\"}","PipelineDescription":"test pipeline"}`)
	assertOK(t, resp)
	body := parseBody(t, resp)
	if body["PipelineArn"] == nil {
		t.Fatal("expected PipelineArn")
	}

	// Duplicate
	resp = callOp(t, p, "CreatePipeline", `{"PipelineName":"pl1"}`)
	if resp.StatusCode == 200 {
		t.Fatal("expected error on duplicate")
	}

	// Describe
	resp = callOp(t, p, "DescribePipeline", `{"PipelineName":"pl1"}`)
	assertOK(t, resp)
	body = parseBody(t, resp)
	if body["PipelineName"] != "pl1" {
		t.Errorf("expected PipelineName=pl1, got %v", body["PipelineName"])
	}
	if body["PipelineDescription"] != "test pipeline" {
		t.Errorf("expected description='test pipeline', got %v", body["PipelineDescription"])
	}

	// List
	callOp(t, p, "CreatePipeline", `{"PipelineName":"pl2","RoleArn":"role2"}`)
	resp = callOp(t, p, "ListPipelines", `{}`)
	assertOK(t, resp)
	body = parseBody(t, resp)
	pls, _ := body["PipelineSummaries"].([]any)
	if len(pls) < 2 {
		t.Errorf("expected at least 2 pipelines, got %d", len(pls))
	}

	// Update
	resp = callOp(t, p, "UpdatePipeline", `{"PipelineName":"pl1","PipelineDescription":"updated pipeline"}`)
	assertOK(t, resp)
	resp = callOp(t, p, "DescribePipeline", `{"PipelineName":"pl1"}`)
	assertOK(t, resp)
	body = parseBody(t, resp)
	if body["PipelineDescription"] != "updated pipeline" {
		t.Errorf("expected 'updated pipeline', got %v", body["PipelineDescription"])
	}

	// Start Execution
	resp = callOp(t, p, "StartPipelineExecution", `{"PipelineName":"pl1"}`)
	assertOK(t, resp)
	body = parseBody(t, resp)
	execARN, _ := body["PipelineExecutionArn"].(string)
	if execARN == "" {
		t.Fatal("expected PipelineExecutionArn")
	}

	// Describe Execution
	resp = callOp(t, p, "DescribePipelineExecution", `{"PipelineExecutionArn":"`+execARN+`"}`)
	assertOK(t, resp)
	body = parseBody(t, resp)
	if body["PipelineName"] != "pl1" {
		t.Errorf("expected PipelineName=pl1, got %v", body["PipelineName"])
	}

	// List Executions
	callOp(t, p, "StartPipelineExecution", `{"PipelineName":"pl1"}`)
	resp = callOp(t, p, "ListPipelineExecutions", `{"PipelineName":"pl1"}`)
	assertOK(t, resp)
	body = parseBody(t, resp)
	execs, _ := body["PipelineExecutionSummaries"].([]any)
	if len(execs) < 2 {
		t.Errorf("expected at least 2 executions, got %d", len(execs))
	}

	// Stop Execution
	resp = callOp(t, p, "StopPipelineExecution", `{"PipelineExecutionArn":"`+execARN+`"}`)
	assertOK(t, resp)

	// Delete Pipeline
	resp = callOp(t, p, "DeletePipeline", `{"PipelineName":"pl1"}`)
	assertOK(t, resp)

	resp = callOp(t, p, "DescribePipeline", `{"PipelineName":"pl1"}`)
	if resp.StatusCode == 200 {
		t.Fatal("expected error after delete")
	}
}

func TestExperimentAndTrialCRUD(t *testing.T) {
	p := newTestProvider(t)

	// Create Experiment
	resp := callOp(t, p, "CreateExperiment", `{"ExperimentName":"exp1","Description":"test experiment"}`)
	assertOK(t, resp)
	body := parseBody(t, resp)
	if body["ExperimentArn"] == nil {
		t.Fatal("expected ExperimentArn")
	}

	// Duplicate
	resp = callOp(t, p, "CreateExperiment", `{"ExperimentName":"exp1"}`)
	if resp.StatusCode == 200 {
		t.Fatal("expected error on duplicate")
	}

	// Describe Experiment
	resp = callOp(t, p, "DescribeExperiment", `{"ExperimentName":"exp1"}`)
	assertOK(t, resp)
	body = parseBody(t, resp)
	if body["ExperimentName"] != "exp1" {
		t.Errorf("expected ExperimentName=exp1, got %v", body["ExperimentName"])
	}

	// List Experiments
	callOp(t, p, "CreateExperiment", `{"ExperimentName":"exp2"}`)
	resp = callOp(t, p, "ListExperiments", `{}`)
	assertOK(t, resp)
	body = parseBody(t, resp)
	exps, _ := body["ExperimentSummaries"].([]any)
	if len(exps) < 2 {
		t.Errorf("expected at least 2 experiments, got %d", len(exps))
	}

	// Update Experiment
	resp = callOp(t, p, "UpdateExperiment", `{"ExperimentName":"exp1","Description":"updated"}`)
	assertOK(t, resp)
	resp = callOp(t, p, "DescribeExperiment", `{"ExperimentName":"exp1"}`)
	assertOK(t, resp)
	body = parseBody(t, resp)
	if body["Description"] != "updated" {
		t.Errorf("expected Description=updated, got %v", body["Description"])
	}

	// Create Trial
	resp = callOp(t, p, "CreateTrial", `{"TrialName":"trial1","ExperimentName":"exp1"}`)
	assertOK(t, resp)
	body = parseBody(t, resp)
	if body["TrialArn"] == nil {
		t.Fatal("expected TrialArn")
	}

	// Duplicate Trial
	resp = callOp(t, p, "CreateTrial", `{"TrialName":"trial1","ExperimentName":"exp1"}`)
	if resp.StatusCode == 200 {
		t.Fatal("expected error on duplicate trial")
	}

	// Describe Trial
	resp = callOp(t, p, "DescribeTrial", `{"TrialName":"trial1"}`)
	assertOK(t, resp)
	body = parseBody(t, resp)
	if body["TrialName"] != "trial1" {
		t.Errorf("expected TrialName=trial1, got %v", body["TrialName"])
	}
	if body["ExperimentName"] != "exp1" {
		t.Errorf("expected ExperimentName=exp1, got %v", body["ExperimentName"])
	}

	// List Trials
	callOp(t, p, "CreateTrial", `{"TrialName":"trial2","ExperimentName":"exp1"}`)
	resp = callOp(t, p, "ListTrials", `{"ExperimentName":"exp1"}`)
	assertOK(t, resp)
	body = parseBody(t, resp)
	trials, _ := body["TrialSummaries"].([]any)
	if len(trials) < 2 {
		t.Errorf("expected at least 2 trials, got %d", len(trials))
	}

	// Update Trial
	resp = callOp(t, p, "UpdateTrial", `{"TrialName":"trial1","ExperimentName":"exp2"}`)
	assertOK(t, resp)

	// Delete Trial
	resp = callOp(t, p, "DeleteTrial", `{"TrialName":"trial1"}`)
	assertOK(t, resp)

	// Delete Experiment
	resp = callOp(t, p, "DeleteExperiment", `{"ExperimentName":"exp1"}`)
	assertOK(t, resp)

	resp = callOp(t, p, "DescribeExperiment", `{"ExperimentName":"exp1"}`)
	if resp.StatusCode == 200 {
		t.Fatal("expected error after delete")
	}
}

func TestDomainAndUserProfileCRUD(t *testing.T) {
	p := newTestProvider(t)

	// Create Domain
	resp := callOp(t, p, "CreateDomain", `{"DomainName":"dom1","AuthMode":"IAM","VpcId":"vpc-12345","DefaultUserSettings":{}}`)
	assertOK(t, resp)
	body := parseBody(t, resp)
	domainID, _ := body["DomainId"].(string)
	if domainID == "" {
		t.Fatal("expected DomainId")
	}

	// Describe Domain
	resp = callOp(t, p, "DescribeDomain", `{"DomainId":"`+domainID+`"}`)
	assertOK(t, resp)
	body = parseBody(t, resp)
	if body["DomainName"] != "dom1" {
		t.Errorf("expected DomainName=dom1, got %v", body["DomainName"])
	}

	// List Domains
	callOp(t, p, "CreateDomain", `{"DomainName":"dom2","AuthMode":"SSO"}`)
	resp = callOp(t, p, "ListDomains", `{}`)
	assertOK(t, resp)
	body = parseBody(t, resp)
	domains, _ := body["Domains"].([]any)
	if len(domains) < 2 {
		t.Errorf("expected at least 2 domains, got %d", len(domains))
	}

	// Update Domain
	resp = callOp(t, p, "UpdateDomain", `{"DomainId":"`+domainID+`","VpcId":"vpc-99999"}`)
	assertOK(t, resp)

	// Create UserProfile
	resp = callOp(t, p, "CreateUserProfile", `{"DomainId":"`+domainID+`","UserProfileName":"user1"}`)
	assertOK(t, resp)
	body = parseBody(t, resp)
	if body["UserProfileArn"] == nil {
		t.Fatal("expected UserProfileArn")
	}

	// Duplicate UserProfile
	resp = callOp(t, p, "CreateUserProfile", `{"DomainId":"`+domainID+`","UserProfileName":"user1"}`)
	if resp.StatusCode == 200 {
		t.Fatal("expected error on duplicate user profile")
	}

	// Describe UserProfile
	resp = callOp(t, p, "DescribeUserProfile", `{"DomainId":"`+domainID+`","UserProfileName":"user1"}`)
	assertOK(t, resp)
	body = parseBody(t, resp)
	if body["UserProfileName"] != "user1" {
		t.Errorf("expected UserProfileName=user1, got %v", body["UserProfileName"])
	}

	// List UserProfiles
	callOp(t, p, "CreateUserProfile", `{"DomainId":"`+domainID+`","UserProfileName":"user2"}`)
	resp = callOp(t, p, "ListUserProfiles", `{"DomainIdEquals":"`+domainID+`"}`)
	assertOK(t, resp)
	body = parseBody(t, resp)
	profiles, _ := body["UserProfiles"].([]any)
	if len(profiles) < 2 {
		t.Errorf("expected at least 2 user profiles, got %d", len(profiles))
	}

	// Update UserProfile
	resp = callOp(t, p, "UpdateUserProfile", `{"DomainId":"`+domainID+`","UserProfileName":"user1"}`)
	assertOK(t, resp)

	// Delete UserProfile
	resp = callOp(t, p, "DeleteUserProfile", `{"DomainId":"`+domainID+`","UserProfileName":"user1"}`)
	assertOK(t, resp)

	// Delete Domain
	resp = callOp(t, p, "DeleteDomain", `{"DomainId":"`+domainID+`"}`)
	assertOK(t, resp)

	resp = callOp(t, p, "DescribeDomain", `{"DomainId":"`+domainID+`"}`)
	if resp.StatusCode == 200 {
		t.Fatal("expected error after delete")
	}
}

func TestTags(t *testing.T) {
	p := newTestProvider(t)

	// Create a notebook instance to use as resource
	resp := callOp(t, p, "CreateNotebookInstance", `{"NotebookInstanceName":"nb-tags","InstanceType":"ml.t3.medium"}`)
	assertOK(t, resp)
	body := parseBody(t, resp)
	arn, _ := body["NotebookInstanceArn"].(string)

	// AddTags
	addBody := `{"ResourceArn":"` + arn + `","Tags":[{"Key":"Env","Value":"test"},{"Key":"Team","Value":"ml"}]}`
	resp = callOp(t, p, "AddTags", addBody)
	assertOK(t, resp)

	// ListTags
	resp = callOp(t, p, "ListTags", `{"ResourceArn":"`+arn+`"}`)
	assertOK(t, resp)
	body = parseBody(t, resp)
	tags, _ := body["Tags"].([]any)
	if len(tags) != 2 {
		t.Errorf("expected 2 tags, got %d", len(tags))
	}

	// DeleteTags
	resp = callOp(t, p, "DeleteTags", `{"ResourceArn":"`+arn+`","TagKeys":["Env"]}`)
	assertOK(t, resp)

	resp = callOp(t, p, "ListTags", `{"ResourceArn":"`+arn+`"}`)
	assertOK(t, resp)
	body = parseBody(t, resp)
	tags, _ = body["Tags"].([]any)
	if len(tags) != 1 {
		t.Errorf("expected 1 tag after delete, got %d", len(tags))
	}
}

func TestDefaultStub(t *testing.T) {
	p := newTestProvider(t)

	// Verify unknown operations return {}
	resp := callOp(t, p, "CreateAutoMLJob", `{}`)
	assertOK(t, resp)

	resp = callOp(t, p, "DescribeCluster", `{}`)
	assertOK(t, resp)

	resp = callOp(t, p, "Search", `{"Resource":"TrainingJob"}`)
	assertOK(t, resp)
	body := parseBody(t, resp)
	results, _ := body["Results"].([]any)
	if len(results) != 0 {
		t.Errorf("expected empty Search results, got %d", len(results))
	}
}
