// SPDX-License-Identifier: Apache-2.0

// internal/services/emr/provider.go
package emr

import (
	"context"
	"encoding/json"
	"fmt"
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

func (p *Provider) ServiceID() string             { return "emr" }
func (p *Provider) ServiceName() string           { return "ElasticMapReduce" }
func (p *Provider) Protocol() plugin.ProtocolType { return plugin.ProtocolJSON11 }

func (p *Provider) Init(cfg plugin.PluginConfig) error {
	dataDir := cfg.DataDir
	if dataDir == "" {
		dataDir = "."
	}
	var err error
	p.store, err = NewStore(filepath.Join(dataDir, "emr"))
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
	// Cluster / Job Flow
	case "RunJobFlow":
		return p.runJobFlow(params)
	case "DescribeCluster":
		return p.describeCluster(params)
	case "ListClusters":
		return p.listClusters(params)
	case "TerminateJobFlows":
		return p.terminateJobFlows(params)
	case "DescribeJobFlows":
		return p.describeJobFlows(params)
	case "SetTerminationProtection":
		return p.setTerminationProtection(params)
	case "SetVisibleToAllUsers":
		return p.setVisibleToAllUsers(params)
	case "SetKeepJobFlowAliveWhenNoSteps":
		return p.setKeepJobFlowAliveWhenNoSteps(params)
	case "SetUnhealthyNodeReplacement":
		return shared.JSONResponse(http.StatusOK, map[string]any{})
	case "ModifyCluster":
		return p.modifyCluster(params)

	// Steps
	case "AddJobFlowSteps":
		return p.addJobFlowSteps(params)
	case "ListSteps":
		return p.listSteps(params)
	case "DescribeStep":
		return p.describeStep(params)
	case "CancelSteps":
		return p.cancelSteps(params)

	// Security Configuration
	case "CreateSecurityConfiguration":
		return p.createSecurityConfiguration(params)
	case "DeleteSecurityConfiguration":
		return p.deleteSecurityConfiguration(params)
	case "DescribeSecurityConfiguration":
		return p.describeSecurityConfiguration(params)
	case "ListSecurityConfigurations":
		return p.listSecurityConfigurations(params)

	// Studio
	case "CreateStudio":
		return p.createStudio(params)
	case "DescribeStudio":
		return p.describeStudio(params)
	case "ListStudios":
		return p.listStudios(params)
	case "DeleteStudio":
		return p.deleteStudio(params)
	case "UpdateStudio":
		return p.updateStudio(params)

	// Tags
	case "AddTags":
		return p.addTags(params)
	case "RemoveTags":
		return p.removeTags(params)

	// Stub operations — return success/empty
	case "AddInstanceFleet":
		return shared.JSONResponse(http.StatusOK, map[string]any{
			"ClusterId": strParam(params, "ClusterId"), "ClusterArn": "", "InstanceFleetId": "",
		})
	case "AddInstanceGroups":
		return shared.JSONResponse(http.StatusOK, map[string]any{
			"JobFlowId": strParam(params, "JobFlowId"), "ClusterArn": "", "InstanceGroupIds": []string{},
		})
	case "CreateStudioSessionMapping":
		return shared.JSONResponse(http.StatusOK, map[string]any{})
	case "DeleteStudioSessionMapping":
		return shared.JSONResponse(http.StatusOK, map[string]any{})
	case "UpdateStudioSessionMapping":
		return shared.JSONResponse(http.StatusOK, map[string]any{})
	case "GetStudioSessionMapping":
		return shared.JSONResponse(http.StatusOK, map[string]any{"SessionMapping": map[string]any{}})
	case "ListStudioSessionMappings":
		return shared.JSONResponse(http.StatusOK, map[string]any{"SessionMappings": []any{}})
	case "ListBootstrapActions":
		return shared.JSONResponse(http.StatusOK, map[string]any{"BootstrapActions": []any{}})
	case "ListInstanceFleets":
		return shared.JSONResponse(http.StatusOK, map[string]any{"InstanceFleets": []any{}})
	case "ListInstanceGroups":
		return shared.JSONResponse(http.StatusOK, map[string]any{"InstanceGroups": []any{}})
	case "ListInstances":
		return shared.JSONResponse(http.StatusOK, map[string]any{"Instances": []any{}})
	case "ListNotebookExecutions":
		return shared.JSONResponse(http.StatusOK, map[string]any{"NotebookExecutions": []any{}})
	case "ListReleaseLabels":
		return shared.JSONResponse(http.StatusOK, map[string]any{"ReleaseLabels": []string{"emr-7.0.0", "emr-6.15.0"}})
	case "ListSupportedInstanceTypes":
		return shared.JSONResponse(http.StatusOK, map[string]any{"SupportedInstanceTypes": []any{}})
	case "ModifyInstanceFleet":
		return shared.JSONResponse(http.StatusOK, map[string]any{})
	case "ModifyInstanceGroups":
		return shared.JSONResponse(http.StatusOK, map[string]any{})
	case "PutAutoScalingPolicy":
		return shared.JSONResponse(http.StatusOK, map[string]any{})
	case "PutAutoTerminationPolicy":
		return shared.JSONResponse(http.StatusOK, map[string]any{})
	case "PutBlockPublicAccessConfiguration":
		return shared.JSONResponse(http.StatusOK, map[string]any{})
	case "PutManagedScalingPolicy":
		return shared.JSONResponse(http.StatusOK, map[string]any{})
	case "RemoveAutoScalingPolicy":
		return shared.JSONResponse(http.StatusOK, map[string]any{})
	case "RemoveAutoTerminationPolicy":
		return shared.JSONResponse(http.StatusOK, map[string]any{})
	case "RemoveManagedScalingPolicy":
		return shared.JSONResponse(http.StatusOK, map[string]any{})
	case "GetAutoTerminationPolicy":
		return shared.JSONResponse(http.StatusOK, map[string]any{"AutoTerminationPolicy": map[string]any{}})
	case "GetBlockPublicAccessConfiguration":
		return shared.JSONResponse(http.StatusOK, map[string]any{
			"BlockPublicAccessConfiguration":         map[string]any{"BlockPublicSecurityGroupRules": false},
			"BlockPublicAccessConfigurationMetadata": map[string]any{},
		})
	case "GetClusterSessionCredentials":
		return shared.JSONResponse(http.StatusOK, map[string]any{"Credentials": map[string]any{}})
	case "GetManagedScalingPolicy":
		return shared.JSONResponse(http.StatusOK, map[string]any{"ManagedScalingPolicy": map[string]any{}})
	case "GetOnClusterAppUIPresignedURL":
		return shared.JSONResponse(http.StatusOK, map[string]any{"PresignedURL": ""})
	case "GetPersistentAppUIPresignedURL":
		return shared.JSONResponse(http.StatusOK, map[string]any{"PresignedURL": ""})
	case "CreatePersistentAppUI":
		return shared.JSONResponse(http.StatusOK, map[string]any{"PersistentAppUIId": shared.GenerateID("", 16)})
	case "DescribePersistentAppUI":
		return shared.JSONResponse(http.StatusOK, map[string]any{"PersistentAppUI": map[string]any{}})
	case "DescribeNotebookExecution":
		return shared.JSONResponse(http.StatusOK, map[string]any{"NotebookExecution": map[string]any{}})
	case "DescribeReleaseLabel":
		return shared.JSONResponse(http.StatusOK, map[string]any{"ReleaseLabel": "emr-7.0.0", "Applications": []any{}})
	case "StartNotebookExecution":
		return shared.JSONResponse(http.StatusOK, map[string]any{"NotebookExecutionId": shared.GenerateID("", 16)})
	case "StopNotebookExecution":
		return shared.JSONResponse(http.StatusOK, map[string]any{})

	default:
		return shared.JSONError("InvalidAction", fmt.Sprintf("unknown action: %s", action), http.StatusBadRequest), nil
	}
}

func (p *Provider) ListResources(_ context.Context) ([]plugin.Resource, error) {
	clusters, err := p.store.ListClusters("")
	if err != nil {
		return nil, err
	}
	res := make([]plugin.Resource, 0, len(clusters))
	for _, c := range clusters {
		res = append(res, plugin.Resource{Type: "cluster", ID: c.ID, Name: c.Name})
	}
	return res, nil
}

func (p *Provider) GetMetrics(_ context.Context) (*plugin.ServiceMetrics, error) {
	return &plugin.ServiceMetrics{}, nil
}

// ---- Cluster operations ----

func (p *Provider) runJobFlow(params map[string]any) (*plugin.Response, error) {
	name, _ := params["Name"].(string)
	if name == "" {
		return shared.JSONError("ValidationException", "Name is required", http.StatusBadRequest), nil
	}

	id := "j-" + shared.GenerateID("", 13)
	arn := shared.BuildARN("elasticmapreduce", "cluster", id)

	releaseLabel, _ := params["ReleaseLabel"].(string)
	if releaseLabel == "" {
		releaseLabel = "emr-7.0.0"
	}
	logURI, _ := params["LogUri"].(string)
	serviceRole, _ := params["ServiceRole"].(string)
	autoTerminate := boolParam(params, "AutoTerminate")

	instanceType := "m5.xlarge"
	instanceCount := 1
	if rawIG, ok := params["Instances"].(map[string]any); ok {
		if it, ok := rawIG["MasterInstanceType"].(string); ok && it != "" {
			instanceType = it
		}
		if ic, ok := rawIG["InstanceCount"].(float64); ok {
			instanceCount = int(ic)
		}
	}

	c := &Cluster{
		ID:            id,
		Name:          name,
		ARN:           arn,
		Status:        "WAITING",
		ReleaseLabel:  releaseLabel,
		InstanceType:  instanceType,
		InstanceCount: instanceCount,
		LogURI:        logURI,
		ServiceRole:   serviceRole,
		AutoTerminate: autoTerminate,
	}
	if err := p.store.CreateCluster(c); err != nil {
		return nil, err
	}

	// Handle optional steps
	stepIDs := []string{}
	if rawSteps, ok := params["Steps"].([]any); ok {
		for _, rs := range rawSteps {
			sm, _ := rs.(map[string]any)
			stepID := "s-" + shared.GenerateID("", 13)
			stepName, _ := sm["Name"].(string)
			aof, _ := sm["ActionOnFailure"].(string)
			if aof == "" {
				aof = "CONTINUE"
			}
			configJSON := "{}"
			if cfg, ok := sm["HadoopJarStep"]; ok {
				b, _ := json.Marshal(cfg)
				configJSON = string(b)
			}
			step := &Step{
				ID:              stepID,
				ClusterID:       id,
				Name:            stepName,
				Status:          "COMPLETED",
				ActionOnFailure: aof,
				Config:          configJSON,
			}
			if err := p.store.CreateStep(step); err != nil {
				return nil, err
			}
			stepIDs = append(stepIDs, stepID)
		}
	}

	// Handle tags
	if rawTags, ok := params["Tags"].([]any); ok {
		//nolint:errcheck
		_ = p.store.tags.AddTags(arn, parseTags(rawTags))
	}

	return shared.JSONResponse(http.StatusOK, map[string]any{
		"JobFlowId":  id,
		"ClusterArn": arn,
		"StepIds":    stepIDs,
	})
}

func (p *Provider) describeCluster(params map[string]any) (*plugin.Response, error) {
	id, _ := params["ClusterId"].(string)
	if id == "" {
		return shared.JSONError("ValidationException", "ClusterId is required", http.StatusBadRequest), nil
	}
	c, err := p.store.GetCluster(id)
	if err != nil {
		return shared.JSONError("InvalidRequestException", "cluster not found", http.StatusBadRequest), nil
	}
	tags, _ := p.store.tags.ListTags(c.ARN)
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"Cluster": clusterToMap(c, tags),
	})
}

func (p *Provider) listClusters(params map[string]any) (*plugin.Response, error) {
	statusFilter := ""
	if rawFilters, ok := params["ClusterStates"].([]any); ok && len(rawFilters) > 0 {
		if s, ok := rawFilters[0].(string); ok {
			statusFilter = s
		}
	}
	clusters, err := p.store.ListClusters(statusFilter)
	if err != nil {
		return nil, err
	}
	list := make([]map[string]any, 0, len(clusters))
	for _, c := range clusters {
		list = append(list, map[string]any{
			"Id":                      c.ID,
			"Name":                    c.Name,
			"ClusterArn":              c.ARN,
			"Status":                  map[string]any{"State": c.Status},
			"NormalizedInstanceHours": 0,
		})
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"Clusters": list,
		"Marker":   nil,
	})
}

func (p *Provider) terminateJobFlows(params map[string]any) (*plugin.Response, error) {
	rawIDs, _ := params["JobFlowIds"].([]any)
	for _, raw := range rawIDs {
		id, _ := raw.(string)
		if id == "" {
			continue
		}
		_ = p.store.UpdateClusterStatus(id, "TERMINATED")
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) describeJobFlows(params map[string]any) (*plugin.Response, error) {
	rawIDs, _ := params["JobFlowIds"].([]any)
	var jobFlows []map[string]any
	for _, raw := range rawIDs {
		id, _ := raw.(string)
		c, err := p.store.GetCluster(id)
		if err != nil {
			continue
		}
		tags, _ := p.store.tags.ListTags(c.ARN)
		jobFlows = append(jobFlows, clusterToJobFlowMap(c, tags))
	}
	if jobFlows == nil {
		jobFlows = []map[string]any{}
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"JobFlows": jobFlows})
}

func (p *Provider) setTerminationProtection(params map[string]any) (*plugin.Response, error) {
	rawIDs, _ := params["JobFlowIds"].([]any)
	for _, raw := range rawIDs {
		id, _ := raw.(string)
		if id == "" {
			continue
		}
		// No-op: we don't persist termination protection; just validate the cluster exists
		_, _ = p.store.GetCluster(id)
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) setVisibleToAllUsers(params map[string]any) (*plugin.Response, error) {
	rawIDs, _ := params["JobFlowIds"].([]any)
	for _, raw := range rawIDs {
		id, _ := raw.(string)
		if id == "" {
			continue
		}
		_, _ = p.store.GetCluster(id)
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) setKeepJobFlowAliveWhenNoSteps(params map[string]any) (*plugin.Response, error) {
	rawIDs, _ := params["JobFlowIds"].([]any)
	for _, raw := range rawIDs {
		id, _ := raw.(string)
		if id == "" {
			continue
		}
		_, _ = p.store.GetCluster(id)
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) modifyCluster(params map[string]any) (*plugin.Response, error) {
	id, _ := params["ClusterId"].(string)
	if id == "" {
		return shared.JSONError("ValidationException", "ClusterId is required", http.StatusBadRequest), nil
	}
	if _, err := p.store.GetCluster(id); err != nil {
		return shared.JSONError("InvalidRequestException", "cluster not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"StepConcurrencyLevel": 1})
}

// ---- Step operations ----

func (p *Provider) addJobFlowSteps(params map[string]any) (*plugin.Response, error) {
	clusterID, _ := params["JobFlowId"].(string)
	if clusterID == "" {
		return shared.JSONError("ValidationException", "JobFlowId is required", http.StatusBadRequest), nil
	}
	if _, err := p.store.GetCluster(clusterID); err != nil {
		return shared.JSONError("InvalidRequestException", "cluster not found", http.StatusBadRequest), nil
	}

	rawSteps, _ := params["Steps"].([]any)
	stepIDs := make([]string, 0, len(rawSteps))
	for _, rs := range rawSteps {
		sm, _ := rs.(map[string]any)
		stepID := "s-" + shared.GenerateID("", 13)
		stepName, _ := sm["Name"].(string)
		aof, _ := sm["ActionOnFailure"].(string)
		if aof == "" {
			aof = "CONTINUE"
		}
		configJSON := "{}"
		if cfg, ok := sm["HadoopJarStep"]; ok {
			b, _ := json.Marshal(cfg)
			configJSON = string(b)
		}
		step := &Step{
			ID:              stepID,
			ClusterID:       clusterID,
			Name:            stepName,
			Status:          "COMPLETED",
			ActionOnFailure: aof,
			Config:          configJSON,
		}
		if err := p.store.CreateStep(step); err != nil {
			return nil, err
		}
		stepIDs = append(stepIDs, stepID)
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"StepIds": stepIDs})
}

func (p *Provider) listSteps(params map[string]any) (*plugin.Response, error) {
	clusterID, _ := params["ClusterId"].(string)
	if clusterID == "" {
		return shared.JSONError("ValidationException", "ClusterId is required", http.StatusBadRequest), nil
	}
	statusFilter := ""
	if rawFilters, ok := params["StepStates"].([]any); ok && len(rawFilters) > 0 {
		if s, ok := rawFilters[0].(string); ok {
			statusFilter = s
		}
	}
	steps, err := p.store.ListSteps(clusterID, statusFilter)
	if err != nil {
		return nil, err
	}
	list := make([]map[string]any, 0, len(steps))
	for _, st := range steps {
		list = append(list, stepToMap(&st))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"Steps":  list,
		"Marker": nil,
	})
}

func (p *Provider) describeStep(params map[string]any) (*plugin.Response, error) {
	clusterID, _ := params["ClusterId"].(string)
	stepID, _ := params["StepId"].(string)
	if clusterID == "" || stepID == "" {
		return shared.JSONError("ValidationException", "ClusterId and StepId are required", http.StatusBadRequest), nil
	}
	st, err := p.store.GetStep(clusterID, stepID)
	if err != nil {
		return shared.JSONError("InvalidRequestException", "step not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"Step": stepToMap(st)})
}

func (p *Provider) cancelSteps(params map[string]any) (*plugin.Response, error) {
	clusterID, _ := params["ClusterId"].(string)
	if clusterID == "" {
		return shared.JSONError("ValidationException", "ClusterId is required", http.StatusBadRequest), nil
	}
	rawIDs, _ := params["StepIds"].([]any)
	results := make([]map[string]any, 0, len(rawIDs))
	for _, raw := range rawIDs {
		stepID, _ := raw.(string)
		if stepID == "" {
			continue
		}
		reason := "SUCCEEDED"
		if err := p.store.UpdateStepStatus(clusterID, stepID, "CANCELLED"); err != nil {
			reason = "FAILED"
		}
		results = append(results, map[string]any{
			"StepId": stepID,
			"Reason": reason,
		})
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"CancelStepsInfoList": results})
}

// ---- SecurityConfiguration operations ----

func (p *Provider) createSecurityConfiguration(params map[string]any) (*plugin.Response, error) {
	name, _ := params["Name"].(string)
	if name == "" {
		return shared.JSONError("ValidationException", "Name is required", http.StatusBadRequest), nil
	}
	configJSON := "{}"
	if cfg, ok := params["SecurityConfiguration"]; ok {
		if s, ok := cfg.(string); ok {
			configJSON = s
		} else {
			b, _ := json.Marshal(cfg)
			configJSON = string(b)
		}
	}
	sc, err := p.store.CreateSecurityConfig(name, configJSON)
	if err != nil {
		if err == errSecurityConfigExists {
			return shared.JSONError("InvalidRequestException", "security configuration already exists", http.StatusBadRequest), nil
		}
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"Name":             sc.Name,
		"CreationDateTime": sc.CreatedAt.Unix(),
	})
}

func (p *Provider) deleteSecurityConfiguration(params map[string]any) (*plugin.Response, error) {
	name, _ := params["Name"].(string)
	if name == "" {
		return shared.JSONError("ValidationException", "Name is required", http.StatusBadRequest), nil
	}
	if err := p.store.DeleteSecurityConfig(name); err != nil {
		return shared.JSONError("InvalidRequestException", "security configuration not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) describeSecurityConfiguration(params map[string]any) (*plugin.Response, error) {
	name, _ := params["Name"].(string)
	if name == "" {
		return shared.JSONError("ValidationException", "Name is required", http.StatusBadRequest), nil
	}
	sc, err := p.store.GetSecurityConfig(name)
	if err != nil {
		return shared.JSONError("InvalidRequestException", "security configuration not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"Name":                  sc.Name,
		"SecurityConfiguration": sc.Config,
		"CreationDateTime":      sc.CreatedAt.Unix(),
	})
}

func (p *Provider) listSecurityConfigurations(_ map[string]any) (*plugin.Response, error) {
	configs, err := p.store.ListSecurityConfigs()
	if err != nil {
		return nil, err
	}
	list := make([]map[string]any, 0, len(configs))
	for _, sc := range configs {
		list = append(list, map[string]any{
			"Name":             sc.Name,
			"CreationDateTime": sc.CreatedAt.Unix(),
		})
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"SecurityConfigurations": list,
		"Marker":                 nil,
	})
}

// ---- Studio operations ----

func (p *Provider) createStudio(params map[string]any) (*plugin.Response, error) {
	name, _ := params["Name"].(string)
	if name == "" {
		return shared.JSONError("ValidationException", "Name is required", http.StatusBadRequest), nil
	}
	id := "es-" + shared.GenerateID("", 13)
	arn := shared.BuildARN("elasticmapreduce", "studio", id)
	description, _ := params["Description"].(string)
	authMode, _ := params["AuthMode"].(string)
	if authMode == "" {
		authMode = "IAM"
	}
	vpcID, _ := params["VpcId"].(string)
	url := fmt.Sprintf("https://%s.emrstudio-prod.us-east-1.amazonaws.com", id)

	st := &Studio{
		ID:          id,
		ARN:         arn,
		Name:        name,
		Description: description,
		AuthMode:    authMode,
		VpcID:       vpcID,
		URL:         url,
	}
	if err := p.store.CreateStudio(st); err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"StudioId":  id,
		"Url":       url,
		"StudioArn": arn,
	})
}

func (p *Provider) describeStudio(params map[string]any) (*plugin.Response, error) {
	id, _ := params["StudioId"].(string)
	if id == "" {
		return shared.JSONError("ValidationException", "StudioId is required", http.StatusBadRequest), nil
	}
	st, err := p.store.GetStudio(id)
	if err != nil {
		return shared.JSONError("InvalidRequestException", "studio not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"Studio": studioToMap(st)})
}

func (p *Provider) listStudios(_ map[string]any) (*plugin.Response, error) {
	studios, err := p.store.ListStudios()
	if err != nil {
		return nil, err
	}
	list := make([]map[string]any, 0, len(studios))
	for _, st := range studios {
		list = append(list, map[string]any{
			"StudioId":    st.ID,
			"StudioArn":   st.ARN,
			"Name":        st.Name,
			"Description": st.Description,
			"AuthMode":    st.AuthMode,
			"Url":         st.URL,
		})
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"Studios": list,
		"Marker":  nil,
	})
}

func (p *Provider) deleteStudio(params map[string]any) (*plugin.Response, error) {
	id, _ := params["StudioId"].(string)
	if id == "" {
		return shared.JSONError("ValidationException", "StudioId is required", http.StatusBadRequest), nil
	}
	if err := p.store.DeleteStudio(id); err != nil {
		return shared.JSONError("InvalidRequestException", "studio not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) updateStudio(params map[string]any) (*plugin.Response, error) {
	id, _ := params["StudioId"].(string)
	if id == "" {
		return shared.JSONError("ValidationException", "StudioId is required", http.StatusBadRequest), nil
	}
	st, err := p.store.GetStudio(id)
	if err != nil {
		return shared.JSONError("InvalidRequestException", "studio not found", http.StatusBadRequest), nil
	}
	name, _ := params["Name"].(string)
	if name == "" {
		name = st.Name
	}
	description, _ := params["Description"].(string)
	if description == "" {
		description = st.Description
	}
	if err := p.store.UpdateStudio(id, name, description); err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

// ---- Tag operations ----

func (p *Provider) addTags(params map[string]any) (*plugin.Response, error) {
	resourceID, _ := params["ResourceId"].(string)
	if resourceID == "" {
		return shared.JSONError("ValidationException", "ResourceId is required", http.StatusBadRequest), nil
	}
	// ResourceId can be a cluster ID or studio ARN; build ARN if it looks like cluster id
	arn := resourceID
	if strings.HasPrefix(resourceID, "j-") {
		c, err := p.store.GetCluster(resourceID)
		if err != nil {
			return shared.JSONError("InvalidRequestException", "resource not found", http.StatusBadRequest), nil
		}
		arn = c.ARN
	}
	rawTags, _ := params["Tags"].([]any)
	if err := p.store.tags.AddTags(arn, parseTags(rawTags)); err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) removeTags(params map[string]any) (*plugin.Response, error) {
	resourceID, _ := params["ResourceId"].(string)
	if resourceID == "" {
		return shared.JSONError("ValidationException", "ResourceId is required", http.StatusBadRequest), nil
	}
	arn := resourceID
	if strings.HasPrefix(resourceID, "j-") {
		c, err := p.store.GetCluster(resourceID)
		if err != nil {
			return shared.JSONError("InvalidRequestException", "resource not found", http.StatusBadRequest), nil
		}
		arn = c.ARN
	}
	rawKeys, _ := params["TagKeys"].([]any)
	keys := make([]string, 0, len(rawKeys))
	for _, k := range rawKeys {
		if s, ok := k.(string); ok {
			keys = append(keys, s)
		}
	}
	if err := p.store.tags.RemoveTags(arn, keys); err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

// ---- helpers ----

func clusterToMap(c *Cluster, tags map[string]string) map[string]any {
	tagList := make([]map[string]string, 0, len(tags))
	for k, v := range tags {
		tagList = append(tagList, map[string]string{"Key": k, "Value": v})
	}
	return map[string]any{
		"Id":            c.ID,
		"Name":          c.Name,
		"ClusterArn":    c.ARN,
		"ReleaseLabel":  c.ReleaseLabel,
		"LogUri":        c.LogURI,
		"ServiceRole":   c.ServiceRole,
		"AutoTerminate": c.AutoTerminate,
		"Status": map[string]any{
			"State":             c.Status,
			"StateChangeReason": map[string]any{},
			"Timeline":          map[string]any{"CreationDateTime": c.CreatedAt.Unix()},
		},
		"Ec2InstanceAttributes":   map[string]any{},
		"Tags":                    tagList,
		"NormalizedInstanceHours": 0,
	}
}

func clusterToJobFlowMap(c *Cluster, tags map[string]string) map[string]any {
	tagList := make([]map[string]string, 0, len(tags))
	for k, v := range tags {
		tagList = append(tagList, map[string]string{"Key": k, "Value": v})
	}
	return map[string]any{
		"JobFlowId":     c.ID,
		"Name":          c.Name,
		"AmiVersion":    c.ReleaseLabel,
		"LogUri":        c.LogURI,
		"ServiceRole":   c.ServiceRole,
		"AutoTerminate": c.AutoTerminate,
		"ExecutionStatusDetail": map[string]any{
			"State":            c.Status,
			"CreationDateTime": c.CreatedAt.Unix(),
		},
		"Tags": tagList,
	}
}

func stepToMap(st *Step) map[string]any {
	var config any
	_ = json.Unmarshal([]byte(st.Config), &config)
	return map[string]any{
		"Id":   st.ID,
		"Name": st.Name,
		"Status": map[string]any{
			"State":             st.Status,
			"StateChangeReason": map[string]any{},
			"Timeline":          map[string]any{"CreationDateTime": st.CreatedAt.Unix()},
		},
		"ActionOnFailure": st.ActionOnFailure,
		"Config":          config,
	}
}

func studioToMap(st *Studio) map[string]any {
	return map[string]any{
		"StudioId":    st.ID,
		"StudioArn":   st.ARN,
		"Name":        st.Name,
		"Description": st.Description,
		"AuthMode":    st.AuthMode,
		"VpcId":       st.VpcID,
		"Url":         st.URL,
	}
}

func parseTags(rawTags []any) map[string]string {
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

func strParam(params map[string]any, key string) string {
	v, _ := params[key].(string)
	return v
}

func boolParam(params map[string]any, key string) bool {
	v, _ := params[key].(bool)
	return v
}
