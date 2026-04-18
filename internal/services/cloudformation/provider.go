// SPDX-License-Identifier: Apache-2.0

package cloudformation

import (
	"context"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/skyoo2003/devcloud/internal/plugin"
	"github.com/skyoo2003/devcloud/internal/shared"
)

// Provider implements the CloudFormation service (Query/XML protocol).
type Provider struct {
	store  *Store
	engine *Engine
}

func (p *Provider) ServiceID() string             { return "cloudformation" }
func (p *Provider) ServiceName() string           { return "CloudFormation" }
func (p *Provider) Protocol() plugin.ProtocolType { return plugin.ProtocolQuery }

func (p *Provider) Init(cfg plugin.PluginConfig) error {
	if err := os.MkdirAll(cfg.DataDir, 0o755); err != nil {
		return fmt.Errorf("init cloudformation: %w", err)
	}
	var err error
	p.store, err = NewStore(cfg.DataDir)
	if err != nil {
		return err
	}
	port := 0
	if v, ok := cfg.Options["server_port"].(int); ok {
		port = v
	}
	p.engine = NewEngine(p.store, plugin.DefaultRegistry, port)
	return nil
}

func (p *Provider) Shutdown(_ context.Context) error {
	if p.store != nil {
		return p.store.Close()
	}
	return nil
}

func (p *Provider) HandleRequest(ctx context.Context, op string, req *http.Request) (*plugin.Response, error) {
	body, err := io.ReadAll(req.Body)
	if err != nil {
		return cfnError("InvalidRequest", "failed to read request body", http.StatusBadRequest), nil
	}
	form, err := url.ParseQuery(string(body))
	if err != nil {
		return cfnError("InvalidRequest", "failed to parse form body", http.StatusBadRequest), nil
	}
	action := op
	if action == "" {
		action = form.Get("Action")
	}

	switch action {
	// Stack operations
	case "CreateStack":
		return p.handleCreateStack(form)
	case "UpdateStack":
		return p.handleUpdateStack(form)
	case "DeleteStack":
		return p.handleDeleteStack(form)
	case "DescribeStacks":
		return p.handleDescribeStacks(form)
	case "ListStacks":
		return p.handleListStacks(form)
	case "GetTemplate":
		return p.handleGetTemplate(form)
	case "GetTemplateSummary":
		return p.handleGetTemplateSummary(form)
	case "ValidateTemplate":
		return p.handleValidateTemplate(form)
	case "SetStackPolicy":
		return p.handleSetStackPolicy(form)
	case "GetStackPolicy":
		return p.handleGetStackPolicy(form)
	case "ContinueUpdateRollback":
		return p.handleContinueUpdateRollback(form)
	case "RollbackStack":
		return p.handleRollbackStack(form)
	case "UpdateTerminationProtection":
		return p.handleUpdateTerminationProtection(form)
	case "SignalResource":
		return p.handleSignalResource(form)

	// ChangeSet operations
	case "CreateChangeSet":
		return p.handleCreateChangeSet(form)
	case "DescribeChangeSet":
		return p.handleDescribeChangeSet(form)
	case "ListChangeSets":
		return p.handleListChangeSets(form)
	case "ExecuteChangeSet":
		return p.handleExecuteChangeSet(form)
	case "DeleteChangeSet":
		return p.handleDeleteChangeSet(form)

	// Stack events & resources
	case "DescribeStackEvents":
		return p.handleDescribeStackEvents(form)
	case "DescribeStackResource":
		return p.handleDescribeStackResource(form)
	case "DescribeStackResources":
		return p.handleDescribeStackResources(form)
	case "ListStackResources":
		return p.handleListStackResources(form)
	case "DescribeStackResourceDrifts":
		return p.handleDescribeStackResourceDrifts(form)
	case "DetectStackDrift":
		return p.handleDetectStackDrift(form)
	case "DetectStackResourceDrift":
		return p.handleDetectStackResourceDrift(form)
	case "DescribeStackDriftDetectionStatus":
		return p.handleDescribeStackDriftDetectionStatus(form)

	// StackSet operations
	case "CreateStackSet":
		return p.handleCreateStackSet(form)
	case "DescribeStackSet":
		return p.handleDescribeStackSet(form)
	case "ListStackSets":
		return p.handleListStackSets(form)
	case "UpdateStackSet":
		return p.handleUpdateStackSet(form)
	case "DeleteStackSet":
		return p.handleDeleteStackSet(form)
	case "CreateStackInstances":
		return p.handleCreateStackInstances(form)
	case "DeleteStackInstances":
		return p.handleDeleteStackInstances(form)
	case "UpdateStackInstances":
		return p.handleUpdateStackInstances(form)
	case "ListStackInstances":
		return p.handleListStackInstances(form)
	case "DescribeStackInstance":
		return p.handleDescribeStackInstance(form)
	case "ListStackSetOperations":
		return p.handleListStackSetOperations(form)
	case "DescribeStackSetOperation":
		return p.handleDescribeStackSetOperation(form)
	case "ListStackSetOperationResults":
		return p.handleListStackSetOperationResults(form)
	case "StopStackSetOperation":
		return p.handleStopStackSetOperation(form)
	case "ImportStacksToStackSet":
		return p.handleImportStacksToStackSet(form)

	// Exports/Imports
	case "ListExports":
		return p.handleListExports(form)
	case "ListImports":
		return p.handleListImports(form)

	// Account/Limits
	case "DescribeAccountLimits":
		return p.handleDescribeAccountLimits(form)

	// Type registry
	case "ListTypes":
		return p.handleListTypes(form)
	case "DescribeType":
		return p.handleDescribeType(form)
	case "RegisterType":
		return p.handleRegisterType(form)
	case "DeregisterType":
		return p.handleDeregisterType(form)
	case "DescribeTypeRegistration":
		return p.handleDescribeTypeRegistration(form)
	case "ListTypeRegistrations":
		return p.handleListTypeRegistrations(form)
	case "ListTypeVersions":
		return p.handleListTypeVersions(form)
	case "SetTypeDefaultVersion":
		return p.handleSetTypeDefaultVersion(form)
	case "SetTypeConfiguration":
		return p.handleSetTypeConfiguration(form)
	case "BatchDescribeTypeConfigurations":
		return p.handleBatchDescribeTypeConfigurations(form)
	case "ActivateType":
		return p.handleActivateType(form)
	case "DeactivateType":
		return p.handleDeactivateType(form)
	case "PublishType":
		return p.handlePublishType(form)
	case "TestType":
		return p.handleTestType(form)
	case "RegisterPublisher":
		return p.handleRegisterPublisher(form)
	case "DescribePublisher":
		return p.handleDescribePublisher(form)

	default:
		type genericResult struct {
			XMLName xml.Name `xml:"GenericResponse"`
		}
		return shared.XMLResponse(http.StatusOK, genericResult{XMLName: xml.Name{Local: action + "Response"}})
	}
}

func (p *Provider) ListResources(ctx context.Context) ([]plugin.Resource, error) {
	stacks, err := p.store.ListStacks("")
	if err != nil {
		return nil, err
	}
	out := make([]plugin.Resource, 0, len(stacks))
	for _, st := range stacks {
		out = append(out, plugin.Resource{Type: "stack", ID: st.ARN, Name: st.Name})
	}
	return out, nil
}

func (p *Provider) GetMetrics(_ context.Context) (*plugin.ServiceMetrics, error) {
	return &plugin.ServiceMetrics{}, nil
}

// ============================================================
// Stack handlers
// ============================================================

// stackXML represents a CloudFormation Stack in XML.
type stackXML struct {
	StackName           string `xml:"StackName"`
	StackID             string `xml:"StackId"`
	StackStatus         string `xml:"StackStatus"`
	TemplateDescription string `xml:"Description,omitempty"`
	RoleARN             string `xml:"RoleARN,omitempty"`
	CreationTime        string `xml:"CreationTime"`
	LastUpdatedTime     string `xml:"LastUpdatedTime"`
	DisableRollback     bool   `xml:"DisableRollback"`
}

func stackToXML(st *Stack) stackXML {
	return stackXML{
		StackName:       st.Name,
		StackID:         st.ARN,
		StackStatus:     st.Status,
		RoleARN:         st.RoleARN,
		CreationTime:    st.CreatedAt.UTC().Format(time.RFC3339),
		LastUpdatedTime: st.UpdatedAt.UTC().Format(time.RFC3339),
		DisableRollback: st.DisableRollback,
	}
}

func (p *Provider) handleCreateStack(form url.Values) (*plugin.Response, error) {
	name := form.Get("StackName")
	if name == "" {
		return cfnError("ValidationError", "StackName is required", http.StatusBadRequest), nil
	}
	templateBody := form.Get("TemplateBody")
	if templateBody == "" {
		templateBody = "{}"
	}
	parameters := parseParametersForm(form)
	capabilities := parseCapabilitiesForm(form)
	roleARN := form.Get("RoleARN")
	description := form.Get("Description")
	disableRollback := form.Get("DisableRollback") == "true"

	id := "stk-" + shared.GenerateID("", 16)
	arn := shared.BuildARN("cloudformation", "stack", name+"/"+id)

	st, err := p.store.CreateStack(name, id, arn, templateBody, parameters, "[]", capabilities, roleARN, description, disableRollback)
	if err != nil {
		if sqlite_IsUnique(err) {
			return cfnError("AlreadyExistsException", "Stack ["+name+"] already exists", http.StatusBadRequest), nil
		}
		return nil, err
	}

	// Parse tags
	tags := parseTagsForm(form)
	if len(tags) > 0 {
		p.store.AddTags(arn, tags) //nolint:errcheck
	}

	// Provision the template via the engine.  The engine updates the
	// stack_resources table and, on failure, rolls the stack back.
	_ = p.store.SetStackStatus(name, "CREATE_IN_PROGRESS")
	tmpl, parseErr := ParseTemplate(templateBody)
	if parseErr != nil {
		_ = p.store.SetStackStatus(name, "CREATE_FAILED")
		return cfnError("ValidationError", parseErr.Error(), http.StatusBadRequest), nil
	}
	params := parseParamsMap(form)
	if err := p.engine.ProvisionStack(context.Background(), name, tmpl, params); err != nil {
		// Engine already rolled back; nothing else to do.  Surface the
		// failure as a ROLLBACK_COMPLETE stack status but still return the
		// StackId so boto3 waiters can observe it.
		type result struct {
			XMLName xml.Name `xml:"CreateStackResponse"`
			Result  struct {
				StackID string `xml:"StackId"`
			} `xml:"CreateStackResult"`
			Meta respMeta
		}
		var r result
		r.Result.StackID = st.ARN
		r.Meta = newMeta()
		return cfnXMLResponse(http.StatusOK, r)
	}
	_ = p.store.SetStackStatus(name, "CREATE_COMPLETE")

	type result struct {
		XMLName xml.Name `xml:"CreateStackResponse"`
		Result  struct {
			StackID string `xml:"StackId"`
		} `xml:"CreateStackResult"`
		Meta respMeta
	}
	var r result
	r.Result.StackID = st.ARN
	r.Meta = newMeta()
	return cfnXMLResponse(http.StatusOK, r)
}

func (p *Provider) handleUpdateStack(form url.Values) (*plugin.Response, error) {
	name := form.Get("StackName")
	if name == "" {
		return cfnError("ValidationError", "StackName is required", http.StatusBadRequest), nil
	}

	// Verify stack exists
	existing, err := p.store.GetStack(name)
	if err != nil {
		return cfnError("ValidationError", "Stack "+name+" does not exist", http.StatusBadRequest), nil
	}

	templateBody := form.Get("TemplateBody")
	if templateBody == "" {
		templateBody = existing.TemplateBody
	}
	parameters := parseParametersForm(form)
	if parameters == "[]" {
		parameters = existing.Parameters
	}
	capabilities := parseCapabilitiesForm(form)
	if capabilities == "[]" {
		capabilities = existing.Capabilities
	}
	roleARN := form.Get("RoleARN")
	if roleARN == "" {
		roleARN = existing.RoleARN
	}
	description := form.Get("Description")
	if description == "" {
		description = existing.Description
	}

	if err := p.store.UpdateStack(name, templateBody, parameters, existing.Outputs, capabilities, roleARN, description); err != nil {
		return cfnError("ValidationError", err.Error(), http.StatusBadRequest), nil
	}

	// Re-provision the template.  Resources that already exist are
	// idempotently re-upserted; brand new ones are created.  A best-effort
	// approach — we do not attempt a diff-based update here.
	_ = p.store.SetStackStatus(name, "UPDATE_IN_PROGRESS")
	tmpl, parseErr := ParseTemplate(templateBody)
	if parseErr == nil {
		params := parseParamsMap(form)
		if err := p.engine.ProvisionStack(context.Background(), name, tmpl, params); err != nil {
			_ = p.store.SetStackStatus(name, "UPDATE_FAILED")
		} else {
			_ = p.store.SetStackStatus(name, "UPDATE_COMPLETE")
		}
	} else {
		_ = p.store.SetStackStatus(name, "UPDATE_FAILED")
	}

	type result struct {
		XMLName xml.Name `xml:"UpdateStackResponse"`
		Result  struct {
			StackID string `xml:"StackId"`
		} `xml:"UpdateStackResult"`
		Meta respMeta
	}
	var r result
	r.Result.StackID = existing.ARN
	r.Meta = newMeta()
	return cfnXMLResponse(http.StatusOK, r)
}

func (p *Provider) handleDeleteStack(form url.Values) (*plugin.Response, error) {
	name := form.Get("StackName")
	if name == "" {
		return cfnError("ValidationError", "StackName is required", http.StatusBadRequest), nil
	}
	// Get ARN for tag cleanup
	st, _ := p.store.GetStack(name)
	// Tear down real resources via other service plugins before dropping
	// the stack row.  Errors are best-effort.
	if st != nil && p.engine != nil {
		_ = p.store.SetStackStatus(name, "DELETE_IN_PROGRESS")
		p.engine.TearDownStack(context.Background(), name)
	}
	if err := p.store.DeleteStack(name); err != nil {
		// CloudFormation silently succeeds on non-existent stacks
		if err == errStackNotFound {
			type result struct {
				XMLName xml.Name `xml:"DeleteStackResponse"`
				Meta    respMeta
			}
			return cfnXMLResponse(http.StatusOK, result{Meta: newMeta()})
		}
		return nil, err
	}
	if st != nil {
		p.store.DeleteAllTags(st.ARN) //nolint:errcheck
	}

	type result struct {
		XMLName xml.Name `xml:"DeleteStackResponse"`
		Meta    respMeta
	}
	return cfnXMLResponse(http.StatusOK, result{Meta: newMeta()})
}

func (p *Provider) handleDescribeStacks(form url.Values) (*plugin.Response, error) {
	name := form.Get("StackName")
	var stacks []Stack
	var err error
	if name != "" {
		st, e := p.store.GetStack(name)
		if e != nil {
			return cfnError("ValidationError", "Stack "+name+" does not exist", http.StatusBadRequest), nil
		}
		stacks = []Stack{*st}
	} else {
		stacks, err = p.store.ListStacks("")
		if err != nil {
			return nil, err
		}
	}

	type result struct {
		XMLName xml.Name `xml:"DescribeStacksResponse"`
		Result  struct {
			Stacks []stackXML `xml:"Stacks>member"`
		} `xml:"DescribeStacksResult"`
		Meta respMeta
	}
	var r result
	for _, st := range stacks {
		r.Result.Stacks = append(r.Result.Stacks, stackToXML(&st))
	}
	r.Meta = newMeta()
	return cfnXMLResponse(http.StatusOK, r)
}

func (p *Provider) handleListStacks(form url.Values) (*plugin.Response, error) {
	statusFilter := form.Get("StackStatusFilter.member.1")
	stacks, err := p.store.ListStacks(statusFilter)
	if err != nil {
		return nil, err
	}

	type stackSummary struct {
		StackName    string `xml:"StackName"`
		StackID      string `xml:"StackId"`
		StackStatus  string `xml:"StackStatus"`
		CreationTime string `xml:"CreationTime"`
	}
	type result struct {
		XMLName xml.Name `xml:"ListStacksResponse"`
		Result  struct {
			StackSummaries []stackSummary `xml:"StackSummaries>member"`
		} `xml:"ListStacksResult"`
		Meta respMeta
	}
	var r result
	for _, st := range stacks {
		r.Result.StackSummaries = append(r.Result.StackSummaries, stackSummary{
			StackName:    st.Name,
			StackID:      st.ARN,
			StackStatus:  st.Status,
			CreationTime: st.CreatedAt.UTC().Format(time.RFC3339),
		})
	}
	r.Meta = newMeta()
	return cfnXMLResponse(http.StatusOK, r)
}

func (p *Provider) handleGetTemplate(form url.Values) (*plugin.Response, error) {
	name := form.Get("StackName")
	if name == "" {
		return cfnError("ValidationError", "StackName is required", http.StatusBadRequest), nil
	}
	st, err := p.store.GetStack(name)
	if err != nil {
		return cfnError("ValidationError", "Stack "+name+" does not exist", http.StatusBadRequest), nil
	}

	type result struct {
		XMLName xml.Name `xml:"GetTemplateResponse"`
		Result  struct {
			TemplateBody string `xml:"TemplateBody"`
		} `xml:"GetTemplateResult"`
		Meta respMeta
	}
	var r result
	r.Result.TemplateBody = st.TemplateBody
	r.Meta = newMeta()
	return cfnXMLResponse(http.StatusOK, r)
}

func (p *Provider) handleGetTemplateSummary(form url.Values) (*plugin.Response, error) {
	templateBody := form.Get("TemplateBody")
	stackName := form.Get("StackName")
	if templateBody == "" && stackName != "" {
		st, err := p.store.GetStack(stackName)
		if err == nil {
			templateBody = st.TemplateBody
		}
	}
	params := extractParametersFromTemplate(templateBody)

	type paramDecl struct {
		ParameterKey  string `xml:"ParameterKey"`
		ParameterType string `xml:"ParameterType"`
	}
	type result struct {
		XMLName xml.Name `xml:"GetTemplateSummaryResponse"`
		Result  struct {
			Parameters []paramDecl `xml:"Parameters>member"`
		} `xml:"GetTemplateSummaryResult"`
		Meta respMeta
	}
	var r result
	for _, pk := range params {
		r.Result.Parameters = append(r.Result.Parameters, paramDecl{ParameterKey: pk, ParameterType: "String"})
	}
	r.Meta = newMeta()
	return cfnXMLResponse(http.StatusOK, r)
}

func (p *Provider) handleValidateTemplate(form url.Values) (*plugin.Response, error) {
	templateBody := form.Get("TemplateBody")
	params := extractParametersFromTemplate(templateBody)
	description := extractDescriptionFromTemplate(templateBody)

	type paramDecl struct {
		ParameterKey  string `xml:"ParameterKey"`
		ParameterType string `xml:"ParameterType"`
	}
	type validateResult struct {
		Parameters  []paramDecl `xml:"Parameters>member"`
		Description string      `xml:"Description"`
	}
	type result struct {
		XMLName xml.Name       `xml:"ValidateTemplateResponse"`
		Result  validateResult `xml:"ValidateTemplateResult"`
		Meta    respMeta
	}
	var r result
	for _, pk := range params {
		r.Result.Parameters = append(r.Result.Parameters, paramDecl{ParameterKey: pk, ParameterType: "String"})
	}
	r.Result.Description = description
	r.Meta = newMeta()
	return cfnXMLResponse(http.StatusOK, r)
}

func (p *Provider) handleSetStackPolicy(form url.Values) (*plugin.Response, error) {
	type result struct {
		XMLName xml.Name `xml:"SetStackPolicyResponse"`
		Meta    respMeta
	}
	return cfnXMLResponse(http.StatusOK, result{Meta: newMeta()})
}

func (p *Provider) handleGetStackPolicy(form url.Values) (*plugin.Response, error) {
	type result struct {
		XMLName xml.Name `xml:"GetStackPolicyResponse"`
		Result  struct {
			StackPolicyBody string `xml:"StackPolicyBody"`
		} `xml:"GetStackPolicyResult"`
		Meta respMeta
	}
	var r result
	r.Meta = newMeta()
	return cfnXMLResponse(http.StatusOK, r)
}

func (p *Provider) handleContinueUpdateRollback(form url.Values) (*plugin.Response, error) {
	type result struct {
		XMLName xml.Name `xml:"ContinueUpdateRollbackResponse"`
		Result  struct{} `xml:"ContinueUpdateRollbackResult"`
		Meta    respMeta
	}
	return cfnXMLResponse(http.StatusOK, result{Meta: newMeta()})
}

func (p *Provider) handleRollbackStack(form url.Values) (*plugin.Response, error) {
	type result struct {
		XMLName xml.Name `xml:"RollbackStackResponse"`
		Result  struct {
			StackID string `xml:"StackId"`
		} `xml:"RollbackStackResult"`
		Meta respMeta
	}
	name := form.Get("StackName")
	var r result
	if st, err := p.store.GetStack(name); err == nil {
		r.Result.StackID = st.ARN
	}
	r.Meta = newMeta()
	return cfnXMLResponse(http.StatusOK, r)
}

func (p *Provider) handleUpdateTerminationProtection(form url.Values) (*plugin.Response, error) {
	type result struct {
		XMLName xml.Name `xml:"UpdateTerminationProtectionResponse"`
		Result  struct {
			StackID string `xml:"StackId"`
		} `xml:"UpdateTerminationProtectionResult"`
		Meta respMeta
	}
	name := form.Get("StackName")
	var r result
	if st, err := p.store.GetStack(name); err == nil {
		r.Result.StackID = st.ARN
	}
	r.Meta = newMeta()
	return cfnXMLResponse(http.StatusOK, r)
}

func (p *Provider) handleSignalResource(form url.Values) (*plugin.Response, error) {
	type result struct {
		XMLName xml.Name `xml:"SignalResourceResponse"`
		Meta    respMeta
	}
	return cfnXMLResponse(http.StatusOK, result{Meta: newMeta()})
}

// ============================================================
// ChangeSet handlers
// ============================================================

func (p *Provider) handleCreateChangeSet(form url.Values) (*plugin.Response, error) {
	stackName := form.Get("StackName")
	csName := form.Get("ChangeSetName")
	if stackName == "" || csName == "" {
		return cfnError("ValidationError", "StackName and ChangeSetName are required", http.StatusBadRequest), nil
	}
	templateBody := form.Get("TemplateBody")
	if templateBody == "" {
		templateBody = "{}"
	}
	parameters := parseParametersForm(form)
	description := form.Get("Description")

	id := shared.GenerateUUID()
	arn := shared.BuildARN("cloudformation", "changeSet", csName+"/"+id)

	cs, err := p.store.CreateChangeSet(csName, id, arn, stackName, templateBody, parameters, description)
	if err != nil {
		return nil, err
	}

	type result struct {
		XMLName xml.Name `xml:"CreateChangeSetResponse"`
		Result  struct {
			ID      string `xml:"Id"`
			StackID string `xml:"StackId"`
		} `xml:"CreateChangeSetResult"`
		Meta respMeta
	}
	var r result
	r.Result.ID = cs.ARN
	if st, e := p.store.GetStack(stackName); e == nil {
		r.Result.StackID = st.ARN
	}
	r.Meta = newMeta()
	return cfnXMLResponse(http.StatusOK, r)
}

func (p *Provider) handleDescribeChangeSet(form url.Values) (*plugin.Response, error) {
	stackName := form.Get("StackName")
	csName := form.Get("ChangeSetName")
	if csName == "" {
		return cfnError("ValidationError", "ChangeSetName is required", http.StatusBadRequest), nil
	}
	cs, err := p.store.GetChangeSet(stackName, csName)
	if err != nil {
		return cfnError("ChangeSetNotFoundException", "ChangeSet "+csName+" not found", http.StatusBadRequest), nil
	}

	type changeSetXML struct {
		ChangeSetName   string `xml:"ChangeSetName"`
		ChangeSetID     string `xml:"ChangeSetId"`
		StackName       string `xml:"StackName"`
		Status          string `xml:"Status"`
		ExecutionStatus string `xml:"ExecutionStatus"`
		Description     string `xml:"Description"`
		CreationTime    string `xml:"CreationTime"`
	}
	type result struct {
		XMLName xml.Name     `xml:"DescribeChangeSetResponse"`
		Result  changeSetXML `xml:"DescribeChangeSetResult"`
		Meta    respMeta
	}
	r := result{
		Result: changeSetXML{
			ChangeSetName:   cs.Name,
			ChangeSetID:     cs.ARN,
			StackName:       cs.StackName,
			Status:          cs.Status,
			ExecutionStatus: cs.ExecutionStatus,
			Description:     cs.Description,
			CreationTime:    cs.CreatedAt.UTC().Format(time.RFC3339),
		},
		Meta: newMeta(),
	}
	return cfnXMLResponse(http.StatusOK, r)
}

func (p *Provider) handleListChangeSets(form url.Values) (*plugin.Response, error) {
	stackName := form.Get("StackName")
	css, err := p.store.ListChangeSets(stackName)
	if err != nil {
		return nil, err
	}

	type csSummary struct {
		ChangeSetName   string `xml:"ChangeSetName"`
		ChangeSetID     string `xml:"ChangeSetId"`
		StackName       string `xml:"StackName"`
		Status          string `xml:"Status"`
		ExecutionStatus string `xml:"ExecutionStatus"`
		CreationTime    string `xml:"CreationTime"`
	}
	type result struct {
		XMLName xml.Name `xml:"ListChangeSetsResponse"`
		Result  struct {
			Summaries []csSummary `xml:"Summaries>member"`
		} `xml:"ListChangeSetsResult"`
		Meta respMeta
	}
	var r result
	for _, cs := range css {
		r.Result.Summaries = append(r.Result.Summaries, csSummary{
			ChangeSetName:   cs.Name,
			ChangeSetID:     cs.ARN,
			StackName:       cs.StackName,
			Status:          cs.Status,
			ExecutionStatus: cs.ExecutionStatus,
			CreationTime:    cs.CreatedAt.UTC().Format(time.RFC3339),
		})
	}
	r.Meta = newMeta()
	return cfnXMLResponse(http.StatusOK, r)
}

func (p *Provider) handleExecuteChangeSet(form url.Values) (*plugin.Response, error) {
	stackName := form.Get("StackName")
	csName := form.Get("ChangeSetName")
	if err := p.store.ExecuteChangeSet(stackName, csName); err != nil {
		return cfnError("ChangeSetNotFoundException", "ChangeSet "+csName+" not found", http.StatusBadRequest), nil
	}
	type result struct {
		XMLName xml.Name `xml:"ExecuteChangeSetResponse"`
		Result  struct{} `xml:"ExecuteChangeSetResult"`
		Meta    respMeta
	}
	return cfnXMLResponse(http.StatusOK, result{Meta: newMeta()})
}

func (p *Provider) handleDeleteChangeSet(form url.Values) (*plugin.Response, error) {
	stackName := form.Get("StackName")
	csName := form.Get("ChangeSetName")
	p.store.DeleteChangeSet(stackName, csName) //nolint:errcheck
	type result struct {
		XMLName xml.Name `xml:"DeleteChangeSetResponse"`
		Result  struct{} `xml:"DeleteChangeSetResult"`
		Meta    respMeta
	}
	return cfnXMLResponse(http.StatusOK, result{Meta: newMeta()})
}

// ============================================================
// Stack Events & Resources
// ============================================================

func (p *Provider) handleDescribeStackEvents(form url.Values) (*plugin.Response, error) {
	stackName := form.Get("StackName")
	st, err := p.store.GetStack(stackName)
	if err != nil {
		return cfnError("ValidationError", "Stack "+stackName+" does not exist", http.StatusBadRequest), nil
	}

	now := time.Now().UTC().Format(time.RFC3339)
	type eventXML struct {
		StackID            string `xml:"StackId"`
		EventID            string `xml:"EventId"`
		StackName          string `xml:"StackName"`
		LogicalResourceID  string `xml:"LogicalResourceId"`
		PhysicalResourceID string `xml:"PhysicalResourceId"`
		ResourceType       string `xml:"ResourceType"`
		ResourceStatus     string `xml:"ResourceStatus"`
		Timestamp          string `xml:"Timestamp"`
	}
	type result struct {
		XMLName xml.Name `xml:"DescribeStackEventsResponse"`
		Result  struct {
			StackEvents []eventXML `xml:"StackEvents>member"`
		} `xml:"DescribeStackEventsResult"`
		Meta respMeta
	}
	var r result
	r.Result.StackEvents = []eventXML{
		{
			StackID: st.ARN, EventID: shared.GenerateUUID(),
			StackName: stackName, LogicalResourceID: stackName,
			PhysicalResourceID: st.ARN, ResourceType: "AWS::CloudFormation::Stack",
			ResourceStatus: "CREATE_IN_PROGRESS", Timestamp: st.CreatedAt.UTC().Format(time.RFC3339),
		},
		{
			StackID: st.ARN, EventID: shared.GenerateUUID(),
			StackName: stackName, LogicalResourceID: stackName,
			PhysicalResourceID: st.ARN, ResourceType: "AWS::CloudFormation::Stack",
			ResourceStatus: "CREATE_COMPLETE", Timestamp: now,
		},
	}
	r.Meta = newMeta()
	return cfnXMLResponse(http.StatusOK, r)
}

func (p *Provider) handleDescribeStackResource(form url.Values) (*plugin.Response, error) {
	stackName := form.Get("StackName")
	logicalID := form.Get("LogicalResourceId")
	res, err := p.store.GetStackResource(stackName, logicalID)
	if err != nil {
		return cfnError("ValidationError", "Resource not found", http.StatusBadRequest), nil
	}

	type resDetail struct {
		StackName          string `xml:"StackName"`
		StackID            string `xml:"StackId"`
		LogicalResourceID  string `xml:"LogicalResourceId"`
		PhysicalResourceID string `xml:"PhysicalResourceId"`
		ResourceType       string `xml:"ResourceType"`
		ResourceStatus     string `xml:"ResourceStatus"`
	}
	type result struct {
		XMLName xml.Name  `xml:"DescribeStackResourceResponse"`
		Result  resDetail `xml:"DescribeStackResourceResult>StackResourceDetail"`
		Meta    respMeta
	}
	st, _ := p.store.GetStack(stackName)
	stackID := ""
	if st != nil {
		stackID = st.ARN
	}
	r := result{
		Result: resDetail{
			StackName: stackName, StackID: stackID,
			LogicalResourceID: res.LogicalID, PhysicalResourceID: res.PhysicalID,
			ResourceType: res.Type, ResourceStatus: res.Status,
		},
		Meta: newMeta(),
	}
	return cfnXMLResponse(http.StatusOK, r)
}

func (p *Provider) handleDescribeStackResources(form url.Values) (*plugin.Response, error) {
	stackName := form.Get("StackName")
	resources, err := p.store.ListStackResources(stackName)
	if err != nil {
		return nil, err
	}

	type resXML struct {
		StackName          string `xml:"StackName"`
		LogicalResourceID  string `xml:"LogicalResourceId"`
		PhysicalResourceID string `xml:"PhysicalResourceId"`
		ResourceType       string `xml:"ResourceType"`
		ResourceStatus     string `xml:"ResourceStatus"`
	}
	type result struct {
		XMLName xml.Name `xml:"DescribeStackResourcesResponse"`
		Result  struct {
			StackResources []resXML `xml:"StackResources>member"`
		} `xml:"DescribeStackResourcesResult"`
		Meta respMeta
	}
	var r result
	for _, res := range resources {
		r.Result.StackResources = append(r.Result.StackResources, resXML{
			StackName: res.StackName, LogicalResourceID: res.LogicalID,
			PhysicalResourceID: res.PhysicalID, ResourceType: res.Type,
			ResourceStatus: res.Status,
		})
	}
	r.Meta = newMeta()
	return cfnXMLResponse(http.StatusOK, r)
}

func (p *Provider) handleListStackResources(form url.Values) (*plugin.Response, error) {
	stackName := form.Get("StackName")
	resources, err := p.store.ListStackResources(stackName)
	if err != nil {
		return nil, err
	}

	type resXML struct {
		LogicalResourceID  string `xml:"LogicalResourceId"`
		PhysicalResourceID string `xml:"PhysicalResourceId"`
		ResourceType       string `xml:"ResourceType"`
		ResourceStatus     string `xml:"ResourceStatus"`
	}
	type result struct {
		XMLName xml.Name `xml:"ListStackResourcesResponse"`
		Result  struct {
			StackResourceSummaries []resXML `xml:"StackResourceSummaries>member"`
		} `xml:"ListStackResourcesResult"`
		Meta respMeta
	}
	var r result
	for _, res := range resources {
		r.Result.StackResourceSummaries = append(r.Result.StackResourceSummaries, resXML{
			LogicalResourceID: res.LogicalID, PhysicalResourceID: res.PhysicalID,
			ResourceType: res.Type, ResourceStatus: res.Status,
		})
	}
	r.Meta = newMeta()
	return cfnXMLResponse(http.StatusOK, r)
}

func (p *Provider) handleDescribeStackResourceDrifts(form url.Values) (*plugin.Response, error) {
	type result struct {
		XMLName xml.Name `xml:"DescribeStackResourceDriftsResponse"`
		Result  struct {
			StackResourceDrifts []struct{} `xml:"StackResourceDrifts>member"`
		} `xml:"DescribeStackResourceDriftsResult"`
		Meta respMeta
	}
	return cfnXMLResponse(http.StatusOK, result{Meta: newMeta()})
}

func (p *Provider) handleDetectStackDrift(form url.Values) (*plugin.Response, error) {
	type result struct {
		XMLName xml.Name `xml:"DetectStackDriftResponse"`
		Result  struct {
			StackDriftDetectionID string `xml:"StackDriftDetectionId"`
		} `xml:"DetectStackDriftResult"`
		Meta respMeta
	}
	var r result
	r.Result.StackDriftDetectionID = shared.GenerateUUID()
	r.Meta = newMeta()
	return cfnXMLResponse(http.StatusOK, r)
}

func (p *Provider) handleDetectStackResourceDrift(form url.Values) (*plugin.Response, error) {
	type result struct {
		XMLName xml.Name `xml:"DetectStackResourceDriftResponse"`
		Result  struct{} `xml:"DetectStackResourceDriftResult"`
		Meta    respMeta
	}
	return cfnXMLResponse(http.StatusOK, result{Meta: newMeta()})
}

func (p *Provider) handleDescribeStackDriftDetectionStatus(form url.Values) (*plugin.Response, error) {
	type result struct {
		XMLName xml.Name `xml:"DescribeStackDriftDetectionStatusResponse"`
		Result  struct {
			DetectionStatus string `xml:"DetectionStatus"`
		} `xml:"DescribeStackDriftDetectionStatusResult"`
		Meta respMeta
	}
	var r result
	r.Result.DetectionStatus = "DETECTION_COMPLETE"
	r.Meta = newMeta()
	return cfnXMLResponse(http.StatusOK, r)
}

// ============================================================
// StackSet handlers
// ============================================================

type stackSetXML struct {
	StackSetName string `xml:"StackSetName"`
	StackSetID   string `xml:"StackSetId"`
	StackSetARN  string `xml:"StackSetARN"`
	Status       string `xml:"Status"`
	Description  string `xml:"Description,omitempty"`
}

func stackSetToXML(ss *StackSet) stackSetXML {
	return stackSetXML{
		StackSetName: ss.Name,
		StackSetID:   ss.ID,
		StackSetARN:  ss.ARN,
		Status:       ss.Status,
		Description:  ss.Description,
	}
}

func (p *Provider) handleCreateStackSet(form url.Values) (*plugin.Response, error) {
	name := form.Get("StackSetName")
	if name == "" {
		return cfnError("ValidationError", "StackSetName is required", http.StatusBadRequest), nil
	}
	templateBody := form.Get("TemplateBody")
	if templateBody == "" {
		templateBody = "{}"
	}
	description := form.Get("Description")
	adminRole := form.Get("AdministrationRoleARN")
	executionRole := form.Get("ExecutionRoleName")

	id := shared.GenerateUUID()
	arn := shared.BuildARN("cloudformation", "stackset", name+":"+id)

	_, err := p.store.CreateStackSet(name, id, arn, templateBody, description, adminRole, executionRole)
	if err != nil {
		if sqlite_IsUnique(err) {
			return cfnError("NameAlreadyExistsException", "StackSet "+name+" already exists", http.StatusBadRequest), nil
		}
		return nil, err
	}

	type result struct {
		XMLName xml.Name `xml:"CreateStackSetResponse"`
		Result  struct {
			StackSetID string `xml:"StackSetId"`
		} `xml:"CreateStackSetResult"`
		Meta respMeta
	}
	var r result
	r.Result.StackSetID = id
	r.Meta = newMeta()
	return cfnXMLResponse(http.StatusOK, r)
}

func (p *Provider) handleDescribeStackSet(form url.Values) (*plugin.Response, error) {
	name := form.Get("StackSetName")
	ss, err := p.store.GetStackSet(name)
	if err != nil {
		return cfnError("StackSetNotFoundException", "StackSet "+name+" not found", http.StatusBadRequest), nil
	}

	type result struct {
		XMLName xml.Name `xml:"DescribeStackSetResponse"`
		Result  struct {
			StackSet stackSetXML `xml:"StackSet"`
		} `xml:"DescribeStackSetResult"`
		Meta respMeta
	}
	r := result{Meta: newMeta()}
	r.Result.StackSet = stackSetToXML(ss)
	return cfnXMLResponse(http.StatusOK, r)
}

func (p *Provider) handleListStackSets(form url.Values) (*plugin.Response, error) {
	sets, err := p.store.ListStackSets()
	if err != nil {
		return nil, err
	}

	type result struct {
		XMLName xml.Name `xml:"ListStackSetsResponse"`
		Result  struct {
			Summaries []stackSetXML `xml:"Summaries>member"`
		} `xml:"ListStackSetsResult"`
		Meta respMeta
	}
	var r result
	for _, ss := range sets {
		r.Result.Summaries = append(r.Result.Summaries, stackSetToXML(&ss))
	}
	r.Meta = newMeta()
	return cfnXMLResponse(http.StatusOK, r)
}

func (p *Provider) handleUpdateStackSet(form url.Values) (*plugin.Response, error) {
	name := form.Get("StackSetName")
	existing, err := p.store.GetStackSet(name)
	if err != nil {
		return cfnError("StackSetNotFoundException", "StackSet "+name+" not found", http.StatusBadRequest), nil
	}
	templateBody := form.Get("TemplateBody")
	if templateBody == "" {
		templateBody = existing.TemplateBody
	}
	description := form.Get("Description")
	if description == "" {
		description = existing.Description
	}
	adminRole := form.Get("AdministrationRoleARN")
	if adminRole == "" {
		adminRole = existing.AdminRole
	}
	executionRole := form.Get("ExecutionRoleName")
	if executionRole == "" {
		executionRole = existing.ExecutionRole
	}
	if err := p.store.UpdateStackSet(name, templateBody, description, adminRole, executionRole); err != nil {
		return nil, err
	}

	type result struct {
		XMLName xml.Name `xml:"UpdateStackSetResponse"`
		Result  struct {
			OperationID string `xml:"OperationId"`
		} `xml:"UpdateStackSetResult"`
		Meta respMeta
	}
	var r result
	r.Result.OperationID = shared.GenerateUUID()
	r.Meta = newMeta()
	return cfnXMLResponse(http.StatusOK, r)
}

func (p *Provider) handleDeleteStackSet(form url.Values) (*plugin.Response, error) {
	name := form.Get("StackSetName")
	if err := p.store.DeleteStackSet(name); err != nil {
		return cfnError("StackSetNotFoundException", "StackSet "+name+" not found", http.StatusBadRequest), nil
	}
	type result struct {
		XMLName xml.Name `xml:"DeleteStackSetResponse"`
		Result  struct{} `xml:"DeleteStackSetResult"`
		Meta    respMeta
	}
	return cfnXMLResponse(http.StatusOK, result{Meta: newMeta()})
}

func (p *Provider) handleCreateStackInstances(form url.Values) (*plugin.Response, error) {
	type result struct {
		XMLName xml.Name `xml:"CreateStackInstancesResponse"`
		Result  struct {
			OperationID string `xml:"OperationId"`
		} `xml:"CreateStackInstancesResult"`
		Meta respMeta
	}
	var r result
	r.Result.OperationID = shared.GenerateUUID()
	r.Meta = newMeta()
	return cfnXMLResponse(http.StatusOK, r)
}

func (p *Provider) handleDeleteStackInstances(form url.Values) (*plugin.Response, error) {
	type result struct {
		XMLName xml.Name `xml:"DeleteStackInstancesResponse"`
		Result  struct {
			OperationID string `xml:"OperationId"`
		} `xml:"DeleteStackInstancesResult"`
		Meta respMeta
	}
	var r result
	r.Result.OperationID = shared.GenerateUUID()
	r.Meta = newMeta()
	return cfnXMLResponse(http.StatusOK, r)
}

func (p *Provider) handleUpdateStackInstances(form url.Values) (*plugin.Response, error) {
	type result struct {
		XMLName xml.Name `xml:"UpdateStackInstancesResponse"`
		Result  struct {
			OperationID string `xml:"OperationId"`
		} `xml:"UpdateStackInstancesResult"`
		Meta respMeta
	}
	var r result
	r.Result.OperationID = shared.GenerateUUID()
	r.Meta = newMeta()
	return cfnXMLResponse(http.StatusOK, r)
}

func (p *Provider) handleListStackInstances(form url.Values) (*plugin.Response, error) {
	type result struct {
		XMLName xml.Name `xml:"ListStackInstancesResponse"`
		Result  struct {
			Summaries []struct{} `xml:"Summaries>member"`
		} `xml:"ListStackInstancesResult"`
		Meta respMeta
	}
	return cfnXMLResponse(http.StatusOK, result{Meta: newMeta()})
}

func (p *Provider) handleDescribeStackInstance(form url.Values) (*plugin.Response, error) {
	return cfnError("StackInstanceNotFoundException", "Stack instance not found", http.StatusBadRequest), nil
}

func (p *Provider) handleListStackSetOperations(form url.Values) (*plugin.Response, error) {
	type result struct {
		XMLName xml.Name `xml:"ListStackSetOperationsResponse"`
		Result  struct {
			Summaries []struct{} `xml:"Summaries>member"`
		} `xml:"ListStackSetOperationsResult"`
		Meta respMeta
	}
	return cfnXMLResponse(http.StatusOK, result{Meta: newMeta()})
}

func (p *Provider) handleDescribeStackSetOperation(form url.Values) (*plugin.Response, error) {
	type result struct {
		XMLName xml.Name `xml:"DescribeStackSetOperationResponse"`
		Result  struct {
			StackSetOperation struct {
				OperationID string `xml:"OperationId"`
				Status      string `xml:"Status"`
			} `xml:"StackSetOperation"`
		} `xml:"DescribeStackSetOperationResult"`
		Meta respMeta
	}
	var r result
	r.Result.StackSetOperation.OperationID = form.Get("OperationId")
	r.Result.StackSetOperation.Status = "SUCCEEDED"
	r.Meta = newMeta()
	return cfnXMLResponse(http.StatusOK, r)
}

func (p *Provider) handleListStackSetOperationResults(form url.Values) (*plugin.Response, error) {
	type result struct {
		XMLName xml.Name `xml:"ListStackSetOperationResultsResponse"`
		Result  struct {
			Summaries []struct{} `xml:"Summaries>member"`
		} `xml:"ListStackSetOperationResultsResult"`
		Meta respMeta
	}
	return cfnXMLResponse(http.StatusOK, result{Meta: newMeta()})
}

func (p *Provider) handleStopStackSetOperation(form url.Values) (*plugin.Response, error) {
	type result struct {
		XMLName xml.Name `xml:"StopStackSetOperationResponse"`
		Result  struct{} `xml:"StopStackSetOperationResult"`
		Meta    respMeta
	}
	return cfnXMLResponse(http.StatusOK, result{Meta: newMeta()})
}

func (p *Provider) handleImportStacksToStackSet(form url.Values) (*plugin.Response, error) {
	type result struct {
		XMLName xml.Name `xml:"ImportStacksToStackSetResponse"`
		Result  struct {
			OperationID string `xml:"OperationId"`
		} `xml:"ImportStacksToStackSetResult"`
		Meta respMeta
	}
	var r result
	r.Result.OperationID = shared.GenerateUUID()
	r.Meta = newMeta()
	return cfnXMLResponse(http.StatusOK, r)
}

// ============================================================
// Exports / Imports / Limits
// ============================================================

func (p *Provider) handleListExports(form url.Values) (*plugin.Response, error) {
	exports, err := p.store.ListExports()
	if err != nil {
		return nil, err
	}

	type exportXML struct {
		Name             string `xml:"Name"`
		Value            string `xml:"Value"`
		ExportingStackID string `xml:"ExportingStackId"`
	}
	type result struct {
		XMLName xml.Name `xml:"ListExportsResponse"`
		Result  struct {
			Exports []exportXML `xml:"Exports>member"`
		} `xml:"ListExportsResult"`
		Meta respMeta
	}
	var r result
	for _, e := range exports {
		r.Result.Exports = append(r.Result.Exports, exportXML{Name: e.Name, Value: e.Value, ExportingStackID: e.StackName})
	}
	r.Meta = newMeta()
	return cfnXMLResponse(http.StatusOK, r)
}

func (p *Provider) handleListImports(form url.Values) (*plugin.Response, error) {
	type result struct {
		XMLName xml.Name `xml:"ListImportsResponse"`
		Result  struct {
			Imports []string `xml:"Imports>member"`
		} `xml:"ListImportsResult"`
		Meta respMeta
	}
	return cfnXMLResponse(http.StatusOK, result{Meta: newMeta()})
}

func (p *Provider) handleDescribeAccountLimits(form url.Values) (*plugin.Response, error) {
	type limitXML struct {
		Name  string `xml:"Name"`
		Value int    `xml:"Value"`
	}
	type result struct {
		XMLName xml.Name `xml:"DescribeAccountLimitsResponse"`
		Result  struct {
			AccountLimits []limitXML `xml:"AccountLimits>member"`
		} `xml:"DescribeAccountLimitsResult"`
		Meta respMeta
	}
	var r result
	r.Result.AccountLimits = []limitXML{
		{Name: "StackLimit", Value: 200},
		{Name: "StackOutputsLimit", Value: 200},
	}
	r.Meta = newMeta()
	return cfnXMLResponse(http.StatusOK, r)
}

// ============================================================
// Type registry handlers (stub)
// ============================================================

func (p *Provider) handleListTypes(form url.Values) (*plugin.Response, error) {
	type result struct {
		XMLName xml.Name `xml:"ListTypesResponse"`
		Result  struct {
			TypeSummaries []struct{} `xml:"TypeSummaries>member"`
		} `xml:"ListTypesResult"`
		Meta respMeta
	}
	return cfnXMLResponse(http.StatusOK, result{Meta: newMeta()})
}

func (p *Provider) handleDescribeType(form url.Values) (*plugin.Response, error) {
	typeName := form.Get("TypeName")
	if typeName == "" {
		typeName = form.Get("Arn")
	}
	type result struct {
		XMLName xml.Name `xml:"DescribeTypeResponse"`
		Result  struct {
			TypeName string `xml:"TypeName"`
			Type     string `xml:"Type"`
		} `xml:"DescribeTypeResult"`
		Meta respMeta
	}
	var r result
	r.Result.TypeName = typeName
	r.Result.Type = "RESOURCE"
	r.Meta = newMeta()
	return cfnXMLResponse(http.StatusOK, r)
}

func (p *Provider) handleRegisterType(form url.Values) (*plugin.Response, error) {
	type result struct {
		XMLName xml.Name `xml:"RegisterTypeResponse"`
		Result  struct {
			RegistrationToken string `xml:"RegistrationToken"`
		} `xml:"RegisterTypeResult"`
		Meta respMeta
	}
	var r result
	r.Result.RegistrationToken = shared.GenerateUUID()
	r.Meta = newMeta()
	return cfnXMLResponse(http.StatusOK, r)
}

func (p *Provider) handleDeregisterType(form url.Values) (*plugin.Response, error) {
	type result struct {
		XMLName xml.Name `xml:"DeregisterTypeResponse"`
		Result  struct{} `xml:"DeregisterTypeResult"`
		Meta    respMeta
	}
	return cfnXMLResponse(http.StatusOK, result{Meta: newMeta()})
}

func (p *Provider) handleDescribeTypeRegistration(form url.Values) (*plugin.Response, error) {
	type result struct {
		XMLName xml.Name `xml:"DescribeTypeRegistrationResponse"`
		Result  struct {
			ProgressStatus string `xml:"ProgressStatus"`
		} `xml:"DescribeTypeRegistrationResult"`
		Meta respMeta
	}
	var r result
	r.Result.ProgressStatus = "COMPLETE"
	r.Meta = newMeta()
	return cfnXMLResponse(http.StatusOK, r)
}

func (p *Provider) handleListTypeRegistrations(form url.Values) (*plugin.Response, error) {
	type result struct {
		XMLName xml.Name `xml:"ListTypeRegistrationsResponse"`
		Result  struct {
			RegistrationTokenList []string `xml:"RegistrationTokenList>member"`
		} `xml:"ListTypeRegistrationsResult"`
		Meta respMeta
	}
	return cfnXMLResponse(http.StatusOK, result{Meta: newMeta()})
}

func (p *Provider) handleListTypeVersions(form url.Values) (*plugin.Response, error) {
	type result struct {
		XMLName xml.Name `xml:"ListTypeVersionsResponse"`
		Result  struct {
			TypeVersionSummaries []struct{} `xml:"TypeVersionSummaries>member"`
		} `xml:"ListTypeVersionsResult"`
		Meta respMeta
	}
	return cfnXMLResponse(http.StatusOK, result{Meta: newMeta()})
}

func (p *Provider) handleSetTypeDefaultVersion(form url.Values) (*plugin.Response, error) {
	type result struct {
		XMLName xml.Name `xml:"SetTypeDefaultVersionResponse"`
		Result  struct{} `xml:"SetTypeDefaultVersionResult"`
		Meta    respMeta
	}
	return cfnXMLResponse(http.StatusOK, result{Meta: newMeta()})
}

func (p *Provider) handleSetTypeConfiguration(form url.Values) (*plugin.Response, error) {
	type result struct {
		XMLName xml.Name `xml:"SetTypeConfigurationResponse"`
		Result  struct {
			ConfigurationArn string `xml:"ConfigurationArn"`
		} `xml:"SetTypeConfigurationResult"`
		Meta respMeta
	}
	var r result
	r.Result.ConfigurationArn = shared.BuildARN("cloudformation", "type-configuration", shared.GenerateUUID())
	r.Meta = newMeta()
	return cfnXMLResponse(http.StatusOK, r)
}

func (p *Provider) handleBatchDescribeTypeConfigurations(form url.Values) (*plugin.Response, error) {
	type result struct {
		XMLName xml.Name `xml:"BatchDescribeTypeConfigurationsResponse"`
		Result  struct {
			TypeConfigurations []struct{} `xml:"TypeConfigurations>member"`
		} `xml:"BatchDescribeTypeConfigurationsResult"`
		Meta respMeta
	}
	return cfnXMLResponse(http.StatusOK, result{Meta: newMeta()})
}

func (p *Provider) handleActivateType(form url.Values) (*plugin.Response, error) {
	type result struct {
		XMLName xml.Name `xml:"ActivateTypeResponse"`
		Result  struct {
			Arn string `xml:"Arn"`
		} `xml:"ActivateTypeResult"`
		Meta respMeta
	}
	var r result
	r.Result.Arn = shared.BuildARN("cloudformation", "type", form.Get("TypeName"))
	r.Meta = newMeta()
	return cfnXMLResponse(http.StatusOK, r)
}

func (p *Provider) handleDeactivateType(form url.Values) (*plugin.Response, error) {
	type result struct {
		XMLName xml.Name `xml:"DeactivateTypeResponse"`
		Result  struct{} `xml:"DeactivateTypeResult"`
		Meta    respMeta
	}
	return cfnXMLResponse(http.StatusOK, result{Meta: newMeta()})
}

func (p *Provider) handlePublishType(form url.Values) (*plugin.Response, error) {
	type result struct {
		XMLName xml.Name `xml:"PublishTypeResponse"`
		Result  struct {
			PublicTypeArn string `xml:"PublicTypeArn"`
		} `xml:"PublishTypeResult"`
		Meta respMeta
	}
	var r result
	r.Result.PublicTypeArn = shared.BuildARN("cloudformation", "type", form.Get("TypeName"))
	r.Meta = newMeta()
	return cfnXMLResponse(http.StatusOK, r)
}

func (p *Provider) handleTestType(form url.Values) (*plugin.Response, error) {
	type result struct {
		XMLName xml.Name `xml:"TestTypeResponse"`
		Result  struct {
			TypeVersionArn string `xml:"TypeVersionArn"`
		} `xml:"TestTypeResult"`
		Meta respMeta
	}
	var r result
	r.Result.TypeVersionArn = shared.BuildARN("cloudformation", "type", form.Get("TypeName"))
	r.Meta = newMeta()
	return cfnXMLResponse(http.StatusOK, r)
}

func (p *Provider) handleRegisterPublisher(form url.Values) (*plugin.Response, error) {
	type result struct {
		XMLName xml.Name `xml:"RegisterPublisherResponse"`
		Result  struct {
			PublisherID string `xml:"PublisherId"`
		} `xml:"RegisterPublisherResult"`
		Meta respMeta
	}
	var r result
	r.Result.PublisherID = shared.GenerateID("pub-", 20)
	r.Meta = newMeta()
	return cfnXMLResponse(http.StatusOK, r)
}

func (p *Provider) handleDescribePublisher(form url.Values) (*plugin.Response, error) {
	type result struct {
		XMLName xml.Name `xml:"DescribePublisherResponse"`
		Result  struct {
			PublisherID     string `xml:"PublisherId"`
			PublisherStatus string `xml:"PublisherStatus"`
		} `xml:"DescribePublisherResult"`
		Meta respMeta
	}
	var r result
	r.Result.PublisherID = form.Get("PublisherId")
	r.Result.PublisherStatus = "VERIFIED"
	r.Meta = newMeta()
	return cfnXMLResponse(http.StatusOK, r)
}

// ============================================================
// Helpers
// ============================================================

type respMeta struct {
	RequestID string `xml:"RequestId"`
}

func newMeta() respMeta {
	return respMeta{RequestID: shared.GenerateUUID()}
}

func cfnError(code, msg string, status int) *plugin.Response {
	return shared.QueryXMLError(code, msg, status)
}

func cfnXMLResponse(status int, v any) (*plugin.Response, error) {
	return shared.XMLResponse(status, v)
}

func sqlite_IsUnique(err error) bool {
	return err != nil && strings.Contains(err.Error(), "UNIQUE constraint failed")
}

// parseTagsForm parses Tags.member.N.Key / Tags.member.N.Value from form values.
func parseTagsForm(form url.Values) map[string]string {
	tags := make(map[string]string)
	for i := 1; ; i++ {
		k := form.Get(fmt.Sprintf("Tags.member.%d.Key", i))
		v := form.Get(fmt.Sprintf("Tags.member.%d.Value", i))
		if k == "" {
			break
		}
		tags[k] = v
	}
	return tags
}

// parseParametersForm parses Parameters.member.N.ParameterKey/Value from form values.
func parseParametersForm(form url.Values) string {
	type param struct {
		Key   string `json:"ParameterKey"`
		Value string `json:"ParameterValue"`
	}
	var params []param
	for i := 1; ; i++ {
		k := form.Get(fmt.Sprintf("Parameters.member.%d.ParameterKey", i))
		v := form.Get(fmt.Sprintf("Parameters.member.%d.ParameterValue", i))
		if k == "" {
			break
		}
		params = append(params, param{Key: k, Value: v})
	}
	if len(params) == 0 {
		return "[]"
	}
	b, _ := json.Marshal(params)
	return string(b)
}

// parseCapabilitiesForm parses Capabilities.member.N from form values.
func parseCapabilitiesForm(form url.Values) string {
	var caps []string
	for i := 1; ; i++ {
		c := form.Get(fmt.Sprintf("Capabilities.member.%d", i))
		if c == "" {
			break
		}
		caps = append(caps, c)
	}
	if len(caps) == 0 {
		return "[]"
	}
	b, _ := json.Marshal(caps)
	return string(b)
}

// extractDescriptionFromTemplate parses a JSON template body and returns the Description field.
func extractDescriptionFromTemplate(templateBody string) string {
	if templateBody == "" {
		return ""
	}
	var tmpl map[string]json.RawMessage
	if err := json.Unmarshal([]byte(templateBody), &tmpl); err != nil {
		return ""
	}
	raw, ok := tmpl["Description"]
	if !ok {
		return ""
	}
	var desc string
	if err := json.Unmarshal(raw, &desc); err != nil {
		return ""
	}
	return desc
}

// extractParametersFromTemplate parses a JSON template body and returns parameter names.
func extractParametersFromTemplate(templateBody string) []string {
	if templateBody == "" || templateBody == "{}" {
		return nil
	}
	var tmpl map[string]json.RawMessage
	if err := json.Unmarshal([]byte(templateBody), &tmpl); err != nil {
		return nil
	}
	rawParams, ok := tmpl["Parameters"]
	if !ok {
		return nil
	}
	var params map[string]json.RawMessage
	if err := json.Unmarshal(rawParams, &params); err != nil {
		return nil
	}
	var keys []string
	for k := range params {
		keys = append(keys, k)
	}
	return keys
}

// parseParamsMap flattens the CloudFormation Parameters.member.N.ParameterKey /
// ParameterValue form values into a simple map[string]string that the engine
// can feed to Fn::Ref lookups.
func parseParamsMap(form url.Values) map[string]string {
	out := map[string]string{}
	for i := 1; ; i++ {
		k := form.Get(fmt.Sprintf("Parameters.member.%d.ParameterKey", i))
		v := form.Get(fmt.Sprintf("Parameters.member.%d.ParameterValue", i))
		if k == "" {
			break
		}
		out[k] = v
	}
	return out
}
