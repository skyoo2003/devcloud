// SPDX-License-Identifier: Apache-2.0

// internal/services/support/provider.go
package support

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

type Provider struct {
	store *Store
}

func (p *Provider) ServiceID() string             { return "support" }
func (p *Provider) ServiceName() string           { return "Support_20130415" }
func (p *Provider) Protocol() plugin.ProtocolType { return plugin.ProtocolJSON11 }

func (p *Provider) Init(cfg plugin.PluginConfig) error {
	dataDir := cfg.DataDir
	if dataDir == "" {
		dataDir = "."
	}
	var err error
	p.store, err = NewStore(filepath.Join(dataDir, "support"))
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
	case "CreateCase":
		return p.createCase(params)
	case "DescribeCases":
		return p.describeCases(params)
	case "ResolveCase":
		return p.resolveCase(params)
	case "AddCommunicationToCase":
		return p.addCommunicationToCase(params)
	case "DescribeCommunications":
		return p.describeCommunications(params)
	case "AddAttachmentsToSet":
		return p.addAttachmentsToSet(params)
	case "DescribeAttachment":
		return p.describeAttachment(params)
	case "DescribeServices":
		return p.describeServices(params)
	case "DescribeSeverityLevels":
		return p.describeSeverityLevels(params)
	case "DescribeCreateCaseOptions":
		return p.describeCreateCaseOptions(params)
	case "DescribeSupportedLanguages":
		return p.describeSupportedLanguages(params)
	case "DescribeTrustedAdvisorChecks":
		return p.describeTrustedAdvisorChecks(params)
	case "DescribeTrustedAdvisorCheckResult":
		return p.describeTrustedAdvisorCheckResult(params)
	case "DescribeTrustedAdvisorCheckSummaries":
		return p.describeTrustedAdvisorCheckSummaries(params)
	case "DescribeTrustedAdvisorCheckRefreshStatuses":
		return p.describeTrustedAdvisorCheckRefreshStatuses(params)
	case "RefreshTrustedAdvisorCheck":
		return p.refreshTrustedAdvisorCheck(params)
	// Extended operations
	case "DescribeServiceHealth":
		return p.describeServiceHealth(params)
	case "DescribeTopics":
		return p.describeTopics(params)
	case "DescribeTopicPaths":
		return p.describeTopicPaths(params)
	case "UpdateCaseAttributes":
		return p.updateCaseAttributes(params)
	case "ReopenCase":
		return p.reopenCase(params)
	case "EscalateCase":
		return p.escalateCase(params)
	case "TransferCase":
		return p.transferCase(params)
	case "ListCaseCategories":
		return p.listCaseCategories(params)
	case "DescribeCategories":
		return p.describeCategories(params)
	case "DescribeSecureCaseOperations":
		return p.describeSecureCaseOperations(params)
	case "ModifySecureCaseParameters":
		return p.modifySecureCaseParameters(params)
	case "ListAttachments":
		return p.listAttachments(params)
	case "DescribeAttachmentSet":
		return p.describeAttachmentSet(params)
	case "DeleteAttachmentSet":
		return p.deleteAttachmentSet(params)
	case "DescribeCaseHistory":
		return p.describeCaseHistory(params)
	case "TagResource":
		return p.tagResource(params)
	case "UntagResource":
		return p.untagResource(params)
	case "ListTagsForResource":
		return p.listTagsForResource(params)
	case "DescribeSupportLevel":
		return p.describeSupportLevel(params)
	case "CreateSupportPlan":
		return p.createSupportPlan(params)
	case "DescribeSupportPlan":
		return p.describeSupportPlan(params)
	case "ListSupportPlans":
		return p.listSupportPlans(params)
	case "UpdateSupportPlan":
		return p.updateSupportPlan(params)
	case "GetOnboardingStatus":
		return p.getOnboardingStatus(params)
	case "StartOnboarding":
		return p.startOnboarding(params)
	case "ListCases":
		return p.listCases(params)
	case "SearchCases":
		return p.searchCases(params)
	default:
		return shared.JSONError("InvalidAction", fmt.Sprintf("unknown action: %s", action), http.StatusBadRequest), nil
	}
}

func (p *Provider) ListResources(_ context.Context) ([]plugin.Resource, error) {
	cases, err := p.store.ListCases(true, nil)
	if err != nil {
		return nil, err
	}
	res := make([]plugin.Resource, 0, len(cases))
	for _, c := range cases {
		res = append(res, plugin.Resource{Type: "case", ID: c.ID, Name: c.Subject})
	}
	return res, nil
}

func (p *Provider) GetMetrics(_ context.Context) (*plugin.ServiceMetrics, error) {
	return &plugin.ServiceMetrics{}, nil
}

func (p *Provider) createCase(params map[string]any) (*plugin.Response, error) {
	subject, _ := params["subject"].(string)
	if subject == "" {
		return shared.JSONError("ValidationException", "subject is required", http.StatusBadRequest), nil
	}
	serviceCode, _ := params["serviceCode"].(string)
	if serviceCode == "" {
		serviceCode = "general-info"
	}
	categoryCode, _ := params["categoryCode"].(string)
	if categoryCode == "" {
		categoryCode = "other"
	}
	severityCode, _ := params["severityCode"].(string)
	if severityCode == "" {
		severityCode = "low"
	}
	language, _ := params["language"].(string)
	if language == "" {
		language = "en"
	}
	submittedBy, _ := params["ccEmailAddresses"].(string)

	id := "case-" + shared.GenerateID("", 12)
	c, err := p.store.CreateCase(id, subject, serviceCode, categoryCode, severityCode, language, submittedBy)
	if err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"caseId": c.ID})
}

func (p *Provider) describeCases(params map[string]any) (*plugin.Response, error) {
	includeResolved, _ := params["includeResolvedCases"].(bool)
	var filterIDs []string
	if rawIDs, ok := params["caseIdList"].([]any); ok {
		for _, id := range rawIDs {
			if s, ok := id.(string); ok {
				filterIDs = append(filterIDs, s)
			}
		}
	}
	cases, err := p.store.ListCases(includeResolved, filterIDs)
	if err != nil {
		return nil, err
	}
	caseList := make([]map[string]any, 0, len(cases))
	for _, c := range cases {
		caseList = append(caseList, caseToMap(&c))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"cases":     caseList,
		"nextToken": "",
	})
}

func (p *Provider) resolveCase(params map[string]any) (*plugin.Response, error) {
	caseID, _ := params["caseId"].(string)
	if caseID == "" {
		return shared.JSONError("ValidationException", "caseId is required", http.StatusBadRequest), nil
	}
	initial, err := p.store.ResolveCase(caseID)
	if err != nil {
		return shared.JSONError("CaseIdNotFound", "case not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"initialCaseStatus": initial,
		"finalCaseStatus":   "resolved",
	})
}

func (p *Provider) addCommunicationToCase(params map[string]any) (*plugin.Response, error) {
	caseID, _ := params["caseId"].(string)
	body, _ := params["communicationBody"].(string)
	if caseID == "" {
		return shared.JSONError("ValidationException", "caseId is required", http.StatusBadRequest), nil
	}
	if body == "" {
		return shared.JSONError("ValidationException", "communicationBody is required", http.StatusBadRequest), nil
	}
	if _, err := p.store.GetCase(caseID); err != nil {
		return shared.JSONError("CaseIdNotFound", "case not found", http.StatusBadRequest), nil
	}
	id := shared.GenerateUUID()
	if _, err := p.store.AddCommunication(id, caseID, body, "customer"); err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"result": true})
}

func (p *Provider) describeCommunications(params map[string]any) (*plugin.Response, error) {
	caseID, _ := params["caseId"].(string)
	if caseID == "" {
		return shared.JSONError("ValidationException", "caseId is required", http.StatusBadRequest), nil
	}
	comms, err := p.store.ListCommunications(caseID)
	if err != nil {
		return nil, err
	}
	commList := make([]map[string]any, 0, len(comms))
	for _, c := range comms {
		commList = append(commList, map[string]any{
			"caseId":      c.CaseID,
			"body":        c.Body,
			"submittedBy": c.SubmittedBy,
			"timeCreated": c.CreatedAt.UTC().Format(time.RFC3339),
		})
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"communications": commList,
		"nextToken":      "",
	})
}

func (p *Provider) addAttachmentsToSet(params map[string]any) (*plugin.Response, error) {
	attachmentSetID, _ := params["attachmentSetId"].(string)
	if attachmentSetID == "" {
		attachmentSetID = shared.GenerateUUID()
	}
	expiry := time.Now().Add(1 * time.Hour)
	_ = p.store.CreateAttachmentSet(attachmentSetID, expiry)
	attachmentIDs := make([]string, 0)
	if raw, ok := params["attachments"].([]any); ok {
		for _, a := range raw {
			if m, ok := a.(map[string]any); ok {
				fn, _ := m["fileName"].(string)
				data, _ := m["data"].(string)
				id := shared.GenerateUUID()
				if _, err := p.store.AddAttachment(id, attachmentSetID, fn, data); err == nil {
					attachmentIDs = append(attachmentIDs, id)
				}
			}
		}
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"attachmentSetId": attachmentSetID,
		"attachmentIds":   attachmentIDs,
		"expiryTime":      expiry.UTC().Format(time.RFC3339),
	})
}

func (p *Provider) describeAttachment(params map[string]any) (*plugin.Response, error) {
	id, _ := params["attachmentId"].(string)
	if id != "" {
		a, err := p.store.GetAttachment(id)
		if err == nil {
			return shared.JSONResponse(http.StatusOK, map[string]any{
				"attachment": map[string]any{
					"fileName": a.FileName,
					"data":     a.Data,
				},
			})
		}
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"attachment": map[string]any{
			"fileName": "dummy.txt",
			"data":     "",
		},
	})
}

func (p *Provider) describeServices(_ map[string]any) (*plugin.Response, error) {
	services := []map[string]any{
		{
			"code": "general-info",
			"name": "General Info and Getting Started",
			"categories": []map[string]any{
				{"code": "other", "name": "Other"},
			},
		},
		{
			"code": "amazon-s3",
			"name": "Amazon Simple Storage Service (S3)",
			"categories": []map[string]any{
				{"code": "connectivity", "name": "Connectivity"},
				{"code": "other", "name": "Other"},
			},
		},
		{
			"code": "amazon-ec2",
			"name": "Amazon Elastic Compute Cloud (EC2)",
			"categories": []map[string]any{
				{"code": "instance", "name": "Instance"},
				{"code": "other", "name": "Other"},
			},
		},
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"services": services})
}

func (p *Provider) describeSeverityLevels(_ map[string]any) (*plugin.Response, error) {
	levels := []map[string]any{
		{"code": "low", "name": "Low"},
		{"code": "normal", "name": "Normal"},
		{"code": "high", "name": "High"},
		{"code": "urgent", "name": "Urgent"},
		{"code": "critical", "name": "Critical"},
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"severityLevels": levels})
}

func (p *Provider) describeCreateCaseOptions(_ map[string]any) (*plugin.Response, error) {
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"communicationTypes":   []any{},
		"languageAvailability": "available",
	})
}

func (p *Provider) describeSupportedLanguages(_ map[string]any) (*plugin.Response, error) {
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"supportedLanguages": []map[string]any{
			{"code": "en", "language": "English", "display": "English"},
		},
	})
}

func (p *Provider) describeTrustedAdvisorChecks(_ map[string]any) (*plugin.Response, error) {
	return shared.JSONResponse(http.StatusOK, map[string]any{"checks": []any{}})
}

func (p *Provider) describeTrustedAdvisorCheckResult(params map[string]any) (*plugin.Response, error) {
	checkID, _ := params["checkId"].(string)
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"result": map[string]any{
			"checkId":   checkID,
			"status":    "ok",
			"timestamp": time.Now().UTC().Format(time.RFC3339),
			"resourcesSummary": map[string]any{
				"resourcesProcessed":  0,
				"resourcesFlagged":    0,
				"resourcesIgnored":    0,
				"resourcesSuppressed": 0,
			},
			"categorySpecificSummary": map[string]any{},
			"flaggedResources":        []any{},
		},
	})
}

func (p *Provider) describeTrustedAdvisorCheckSummaries(_ map[string]any) (*plugin.Response, error) {
	return shared.JSONResponse(http.StatusOK, map[string]any{"summaries": []any{}})
}

func (p *Provider) describeTrustedAdvisorCheckRefreshStatuses(_ map[string]any) (*plugin.Response, error) {
	return shared.JSONResponse(http.StatusOK, map[string]any{"statuses": []any{}})
}

func (p *Provider) refreshTrustedAdvisorCheck(params map[string]any) (*plugin.Response, error) {
	checkID, _ := params["checkId"].(string)
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"status": map[string]any{
			"checkId":                    checkID,
			"status":                     "enqueued",
			"millisUntilNextRefreshable": 0,
		},
	})
}

// --- Extended operations ---

func (p *Provider) describeServiceHealth(_ map[string]any) (*plugin.Response, error) {
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"services": []map[string]any{
			{"code": "amazon-ec2", "name": "Amazon EC2", "healthStatus": "OK"},
			{"code": "amazon-s3", "name": "Amazon S3", "healthStatus": "OK"},
		},
	})
}

func (p *Provider) describeTopics(_ map[string]any) (*plugin.Response, error) {
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"topics": []map[string]any{
			{"id": "1", "name": "Service Health", "arn": shared.BuildARN("support", "topic", "1")},
			{"id": "2", "name": "Billing", "arn": shared.BuildARN("support", "topic", "2")},
		},
	})
}

func (p *Provider) describeTopicPaths(params map[string]any) (*plugin.Response, error) {
	topicArn, _ := params["topicArn"].(string)
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"paths": []map[string]any{
			{"path": "Service Health > EC2", "topicArn": topicArn},
		},
	})
}

func (p *Provider) updateCaseAttributes(params map[string]any) (*plugin.Response, error) {
	caseID, _ := params["caseId"].(string)
	if caseID == "" {
		return shared.JSONError("ValidationException", "caseId is required", http.StatusBadRequest), nil
	}
	if _, err := p.store.GetCase(caseID); err != nil {
		return shared.JSONError("CaseIdNotFound", "case not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"result": map[string]any{"caseId": caseID}})
}

func (p *Provider) reopenCase(params map[string]any) (*plugin.Response, error) {
	caseID, _ := params["caseId"].(string)
	if caseID == "" {
		return shared.JSONError("ValidationException", "caseId is required", http.StatusBadRequest), nil
	}
	if _, err := p.store.GetCase(caseID); err != nil {
		return shared.JSONError("CaseIdNotFound", "case not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"caseId": caseID,
		"status": "reopened",
	})
}

func (p *Provider) escalateCase(params map[string]any) (*plugin.Response, error) {
	caseID, _ := params["caseId"].(string)
	return shared.JSONResponse(http.StatusOK, map[string]any{"caseId": caseID, "escalated": true})
}

func (p *Provider) transferCase(params map[string]any) (*plugin.Response, error) {
	caseID, _ := params["caseId"].(string)
	return shared.JSONResponse(http.StatusOK, map[string]any{"caseId": caseID, "transferred": true})
}

func (p *Provider) listCaseCategories(_ map[string]any) (*plugin.Response, error) {
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"categories": []map[string]any{
			{"code": "other", "name": "Other"},
			{"code": "connectivity", "name": "Connectivity"},
			{"code": "instance", "name": "Instance"},
		},
	})
}

func (p *Provider) describeCategories(_ map[string]any) (*plugin.Response, error) {
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"categories": []map[string]any{
			{"code": "other", "name": "Other"},
		},
	})
}

func (p *Provider) describeSecureCaseOperations(_ map[string]any) (*plugin.Response, error) {
	return shared.JSONResponse(http.StatusOK, map[string]any{"operations": []any{}})
}

func (p *Provider) modifySecureCaseParameters(_ map[string]any) (*plugin.Response, error) {
	return shared.JSONResponse(http.StatusOK, map[string]any{"result": true})
}

func (p *Provider) listAttachments(_ map[string]any) (*plugin.Response, error) {
	return shared.JSONResponse(http.StatusOK, map[string]any{"attachments": []any{}})
}

func (p *Provider) describeAttachmentSet(params map[string]any) (*plugin.Response, error) {
	id, _ := params["attachmentSetId"].(string)
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"attachmentSetId": id,
		"attachments":     []any{},
	})
}

func (p *Provider) deleteAttachmentSet(params map[string]any) (*plugin.Response, error) {
	id, _ := params["attachmentSetId"].(string)
	return shared.JSONResponse(http.StatusOK, map[string]any{"attachmentSetId": id})
}

func (p *Provider) describeCaseHistory(params map[string]any) (*plugin.Response, error) {
	caseID, _ := params["caseId"].(string)
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"caseId":  caseID,
		"history": []any{},
	})
}

func (p *Provider) tagResource(_ map[string]any) (*plugin.Response, error) {
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) untagResource(_ map[string]any) (*plugin.Response, error) {
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) listTagsForResource(_ map[string]any) (*plugin.Response, error) {
	return shared.JSONResponse(http.StatusOK, map[string]any{"tags": map[string]any{}})
}

func (p *Provider) describeSupportLevel(_ map[string]any) (*plugin.Response, error) {
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"supportLevel": map[string]any{
			"name":  "Business",
			"plan":  "business",
			"tiers": []string{"basic", "developer", "business", "enterprise"},
		},
	})
}

func (p *Provider) createSupportPlan(params map[string]any) (*plugin.Response, error) {
	name, _ := params["name"].(string)
	if name == "" {
		name = "default-plan"
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"supportPlan": map[string]any{
			"name":   name,
			"status": "active",
		},
	})
}

func (p *Provider) describeSupportPlan(_ map[string]any) (*plugin.Response, error) {
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"supportPlan": map[string]any{
			"name":   "default-plan",
			"status": "active",
		},
	})
}

func (p *Provider) listSupportPlans(_ map[string]any) (*plugin.Response, error) {
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"supportPlans": []map[string]any{
			{"name": "default-plan", "status": "active"},
		},
	})
}

func (p *Provider) updateSupportPlan(params map[string]any) (*plugin.Response, error) {
	name, _ := params["name"].(string)
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"supportPlan": map[string]any{
			"name":   name,
			"status": "active",
		},
	})
}

func (p *Provider) getOnboardingStatus(_ map[string]any) (*plugin.Response, error) {
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"status": "completed",
	})
}

func (p *Provider) startOnboarding(_ map[string]any) (*plugin.Response, error) {
	return shared.JSONResponse(http.StatusOK, map[string]any{"status": "started"})
}

func (p *Provider) listCases(params map[string]any) (*plugin.Response, error) {
	return p.describeCases(params)
}

func (p *Provider) searchCases(params map[string]any) (*plugin.Response, error) {
	return p.describeCases(params)
}

func caseToMap(c *Case) map[string]any {
	return map[string]any{
		"caseId":       c.ID,
		"displayId":    c.ID,
		"subject":      c.Subject,
		"status":       c.Status,
		"serviceCode":  c.ServiceCode,
		"categoryCode": c.CategoryCode,
		"severityCode": c.SeverityCode,
		"language":     c.Language,
		"submittedBy":  c.SubmittedBy,
		"timeCreated":  c.CreatedAt.UTC().Format(time.RFC3339),
		"recentCommunications": map[string]any{
			"communications": []any{},
			"nextToken":      "",
		},
	}
}
