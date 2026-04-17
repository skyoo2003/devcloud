// SPDX-License-Identifier: Apache-2.0

// internal/services/sesv2/provider.go
package sesv2

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

// Provider implements the SimpleEmailService_v2 service.
type Provider struct {
	store *Store
}

func (p *Provider) ServiceID() string             { return "sesv2" }
func (p *Provider) ServiceName() string           { return "SimpleEmailService_v2" }
func (p *Provider) Protocol() plugin.ProtocolType { return plugin.ProtocolRESTJSON }

func (p *Provider) Init(cfg plugin.PluginConfig) error {
	dataDir := cfg.DataDir
	if dataDir == "" {
		dataDir = "."
	}
	var err error
	p.store, err = NewStore(filepath.Join(dataDir, "sesv2"))
	return err
}

func (p *Provider) Shutdown(_ context.Context) error {
	if p.store != nil {
		return p.store.Close()
	}
	return nil
}

func (p *Provider) HandleRequest(_ context.Context, op string, req *http.Request) (*plugin.Response, error) {
	if op == "" {
		op = resolveOp(req.Method, req.URL.Path)
	}

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

	switch op {
	// --- EmailIdentity ---
	case "CreateEmailIdentity":
		return p.createEmailIdentity(params)
	case "GetEmailIdentity":
		return p.getEmailIdentity(pathParam(req, "identities"))
	case "ListEmailIdentities":
		return p.listEmailIdentities()
	case "DeleteEmailIdentity":
		return p.deleteEmailIdentity(pathParam(req, "identities"))
	case "PutEmailIdentityDkimAttributes":
		return p.putEmailIdentityDkimAttributes(pathParam(req, "identities"), params)
	case "PutEmailIdentityFeedbackAttributes":
		return p.putEmailIdentitySimpleUpdate(pathParam(req, "identities"))
	case "PutEmailIdentityMailFromAttributes":
		return p.putEmailIdentitySimpleUpdate(pathParam(req, "identities"))
	case "PutEmailIdentityDkimSigningAttributes":
		return p.putEmailIdentitySimpleUpdate(pathParam(req, "identities"))
	case "PutEmailIdentityConfigurationSetAttributes":
		return p.putEmailIdentityConfigSet(pathParam(req, "identities"), params)
	case "CreateEmailIdentityPolicy":
		return p.createEmailIdentityPolicy(pathParam(req, "identities"), pathParam2(req, "policies"), params)
	case "GetEmailIdentityPolicies":
		return p.getEmailIdentityPolicies(pathParam(req, "identities"))
	case "UpdateEmailIdentityPolicy":
		return p.updateEmailIdentityPolicy(pathParam(req, "identities"), pathParam2(req, "policies"), params)
	case "DeleteEmailIdentityPolicy":
		return p.deleteEmailIdentityPolicy(pathParam(req, "identities"), pathParam2(req, "policies"))

	// --- EmailTemplate ---
	case "CreateEmailTemplate":
		return p.createEmailTemplate(params)
	case "GetEmailTemplate":
		return p.getEmailTemplate(pathParam(req, "templates"))
	case "ListEmailTemplates":
		return p.listEmailTemplates()
	case "UpdateEmailTemplate":
		return p.updateEmailTemplate(pathParam(req, "templates"), params)
	case "DeleteEmailTemplate":
		return p.deleteEmailTemplate(pathParam(req, "templates"))
	case "TestRenderEmailTemplate":
		return p.testRenderEmailTemplate(pathParam(req, "templates"), params)

	// --- ConfigurationSet ---
	case "CreateConfigurationSet":
		return p.createConfigurationSet(params)
	case "GetConfigurationSet":
		return p.getConfigurationSet(pathParam(req, "configuration-sets"))
	case "ListConfigurationSets":
		return p.listConfigurationSets()
	case "DeleteConfigurationSet":
		return p.deleteConfigurationSet(pathParam(req, "configuration-sets"))
	case "PutConfigurationSetSendingOptions":
		return p.putConfigSetSending(pathParam(req, "configuration-sets"), params)
	case "PutConfigurationSetReputationOptions":
		return p.putConfigSetSimple(pathParam(req, "configuration-sets"))
	case "PutConfigurationSetSuppressionOptions":
		return p.putConfigSetSimple(pathParam(req, "configuration-sets"))
	case "PutConfigurationSetTrackingOptions":
		return p.putConfigSetSimple(pathParam(req, "configuration-sets"))
	case "PutConfigurationSetDeliveryOptions":
		return p.putConfigSetSimple(pathParam(req, "configuration-sets"))
	case "PutConfigurationSetVdmOptions":
		return p.putConfigSetSimple(pathParam(req, "configuration-sets"))
	case "PutConfigurationSetArchivingOptions":
		return p.putConfigSetSimple(pathParam(req, "configuration-sets"))
	case "CreateConfigurationSetEventDestination":
		return p.createConfigSetEventDest(pathParam(req, "configuration-sets"), params)
	case "GetConfigurationSetEventDestinations":
		return p.getConfigSetEventDests(pathParam(req, "configuration-sets"))
	case "UpdateConfigurationSetEventDestination":
		return p.updateConfigSetEventDest(pathParam(req, "configuration-sets"), pathParam2(req, "event-destinations"), params)
	case "DeleteConfigurationSetEventDestination":
		return p.deleteConfigSetEventDest(pathParam(req, "configuration-sets"), pathParam2(req, "event-destinations"))

	// --- ContactList ---
	case "CreateContactList":
		return p.createContactList(params)
	case "GetContactList":
		return p.getContactList(pathParam(req, "contact-lists"))
	case "ListContactLists":
		return p.listContactLists()
	case "UpdateContactList":
		return p.updateContactList(pathParam(req, "contact-lists"), params)
	case "DeleteContactList":
		return p.deleteContactList(pathParam(req, "contact-lists"))

	// --- Contact ---
	case "CreateContact":
		return p.createContact(pathParam(req, "contact-lists"), params)
	case "GetContact":
		return p.getContact(pathParam(req, "contact-lists"), pathParam2(req, "contacts"))
	case "ListContacts":
		return p.listContacts(pathParam(req, "contact-lists"))
	case "UpdateContact":
		return p.updateContact(pathParam(req, "contact-lists"), pathParam2(req, "contacts"), params)
	case "DeleteContact":
		return p.deleteContact(pathParam(req, "contact-lists"), pathParam2(req, "contacts"))

	// --- DedicatedIpPool ---
	case "CreateDedicatedIpPool":
		return p.createDedicatedIpPool(params)
	case "ListDedicatedIpPools":
		return p.listDedicatedIpPools()
	case "DeleteDedicatedIpPool":
		return p.deleteDedicatedIpPool(pathParam(req, "dedicated-ip-pools"))
	case "GetDedicatedIpPool":
		return p.getDedicatedIpPool(pathParam(req, "dedicated-ip-pools"))
	case "GetDedicatedIp":
		return shared.JSONResponse(http.StatusOK, map[string]any{
			"DedicatedIp": map[string]any{"Ip": pathParam(req, "dedicated-ips"), "WarmupStatus": "DONE", "WarmupPercentage": 100},
		})
	case "GetDedicatedIps":
		return shared.JSONResponse(http.StatusOK, map[string]any{"DedicatedIps": []any{}})
	case "PutDedicatedIpInPool":
		return shared.JSONResponse(http.StatusOK, map[string]any{})
	case "PutDedicatedIpWarmupAttributes":
		return shared.JSONResponse(http.StatusOK, map[string]any{})
	case "PutDedicatedIpPoolScalingAttributes":
		return shared.JSONResponse(http.StatusOK, map[string]any{})

	// --- SuppressedDestination ---
	case "PutSuppressedDestination":
		return p.putSuppressedDestination(params)
	case "GetSuppressedDestination":
		return p.getSuppressedDestination(pathParam(req, "addresses"))
	case "ListSuppressedDestinations":
		return p.listSuppressedDestinations()
	case "DeleteSuppressedDestination":
		return p.deleteSuppressedDestination(pathParam(req, "addresses"))

	// --- Send ---
	case "SendEmail":
		return p.sendEmail(params)
	case "SendBulkEmail":
		return p.sendBulkEmail(params)
	case "SendCustomVerificationEmail":
		return p.sendCustomVerificationEmail(params)

	// --- Account ---
	case "GetAccount":
		return p.getAccount()
	case "PutAccountDetails":
		return shared.JSONResponse(http.StatusOK, map[string]any{})
	case "PutAccountSendingAttributes":
		return shared.JSONResponse(http.StatusOK, map[string]any{})
	case "PutAccountSuppressionAttributes":
		return shared.JSONResponse(http.StatusOK, map[string]any{})
	case "PutAccountDedicatedIpWarmupAttributes":
		return shared.JSONResponse(http.StatusOK, map[string]any{})
	case "PutAccountVdmAttributes":
		return shared.JSONResponse(http.StatusOK, map[string]any{})

	// --- Tags ---
	case "TagResource":
		return p.tagResource(req, params)
	case "UntagResource":
		return p.untagResource(req)
	case "ListTagsForResource":
		return p.listTagsForResource(req)

	// --- Stubs (Deliverability, Export, Import, Metrics, Recommendations, etc.) ---
	case "BatchGetMetricData":
		return shared.JSONResponse(http.StatusOK, map[string]any{"Results": []any{}, "Errors": []any{}})
	case "CreateDeliverabilityTestReport":
		return shared.JSONResponse(http.StatusOK, map[string]any{"ReportId": shared.GenerateID("", 32), "DeliverabilityTestStatus": "IN_PROGRESS"})
	case "GetDeliverabilityDashboardOptions":
		return shared.JSONResponse(http.StatusOK, map[string]any{"DashboardEnabled": false})
	case "PutDeliverabilityDashboardOption":
		return shared.JSONResponse(http.StatusOK, map[string]any{})
	case "GetDeliverabilityTestReport":
		return shared.JSONResponse(http.StatusOK, map[string]any{"DeliverabilityTestReport": map[string]any{}, "OverallPlacement": map[string]any{}, "IspPlacements": []any{}})
	case "ListDeliverabilityTestReports":
		return shared.JSONResponse(http.StatusOK, map[string]any{"DeliverabilityTestReports": []any{}})
	case "GetDomainDeliverabilityCampaign":
		return shared.JSONResponse(http.StatusOK, map[string]any{"DomainDeliverabilityCampaign": map[string]any{}})
	case "ListDomainDeliverabilityCampaigns":
		return shared.JSONResponse(http.StatusOK, map[string]any{"DomainDeliverabilityCampaigns": []any{}})
	case "GetDomainStatisticsReport":
		return shared.JSONResponse(http.StatusOK, map[string]any{"DailyVolumes": []any{}, "OverallVolume": map[string]any{}})
	case "GetBlacklistReports":
		return shared.JSONResponse(http.StatusOK, map[string]any{"BlacklistReport": map[string]any{}})
	case "CreateExportJob":
		return shared.JSONResponse(http.StatusOK, map[string]any{"JobId": shared.GenerateID("", 32)})
	case "GetExportJob":
		return shared.JSONResponse(http.StatusOK, map[string]any{"JobStatus": "COMPLETE"})
	case "ListExportJobs":
		return shared.JSONResponse(http.StatusOK, map[string]any{"ExportJobs": []any{}})
	case "CancelExportJob":
		return shared.JSONResponse(http.StatusOK, map[string]any{})
	case "CreateImportJob":
		return shared.JSONResponse(http.StatusOK, map[string]any{"JobId": shared.GenerateID("", 32)})
	case "GetImportJob":
		return shared.JSONResponse(http.StatusOK, map[string]any{"JobStatus": "COMPLETE"})
	case "ListImportJobs":
		return shared.JSONResponse(http.StatusOK, map[string]any{"ImportJobs": []any{}})
	case "ListRecommendations":
		return shared.JSONResponse(http.StatusOK, map[string]any{"Recommendations": []any{}})
	case "GetMessageInsights":
		return shared.JSONResponse(http.StatusOK, map[string]any{"EmailTags": []any{}, "Insights": []any{}})
	case "GetEmailAddressInsights":
		return shared.JSONResponse(http.StatusOK, map[string]any{"EmailAddressInsights": []any{}})
	case "CreateCustomVerificationEmailTemplate":
		return shared.JSONResponse(http.StatusOK, map[string]any{})
	case "GetCustomVerificationEmailTemplate":
		return shared.JSONResponse(http.StatusOK, map[string]any{})
	case "ListCustomVerificationEmailTemplates":
		return shared.JSONResponse(http.StatusOK, map[string]any{"CustomVerificationEmailTemplates": []any{}})
	case "UpdateCustomVerificationEmailTemplate":
		return shared.JSONResponse(http.StatusOK, map[string]any{})
	case "DeleteCustomVerificationEmailTemplate":
		return shared.JSONResponse(http.StatusOK, map[string]any{})
	case "CreateMultiRegionEndpoint":
		return shared.JSONResponse(http.StatusOK, map[string]any{"EndpointId": shared.GenerateID("", 32), "Status": "CREATING"})
	case "GetMultiRegionEndpoint":
		return shared.JSONResponse(http.StatusOK, map[string]any{"Status": "READY"})
	case "ListMultiRegionEndpoints":
		return shared.JSONResponse(http.StatusOK, map[string]any{"MultiRegionEndpoints": []any{}})
	case "DeleteMultiRegionEndpoint":
		return shared.JSONResponse(http.StatusOK, map[string]any{"Status": "DELETING"})
	case "CreateTenant":
		return shared.JSONResponse(http.StatusOK, map[string]any{"TenantId": shared.GenerateID("", 32)})
	case "GetTenant":
		return shared.JSONResponse(http.StatusOK, map[string]any{})
	case "ListTenants":
		return shared.JSONResponse(http.StatusOK, map[string]any{"Tenants": []any{}})
	case "DeleteTenant":
		return shared.JSONResponse(http.StatusOK, map[string]any{})
	case "CreateTenantResourceAssociation":
		return shared.JSONResponse(http.StatusOK, map[string]any{})
	case "DeleteTenantResourceAssociation":
		return shared.JSONResponse(http.StatusOK, map[string]any{})
	case "ListTenantResources":
		return shared.JSONResponse(http.StatusOK, map[string]any{"TenantResources": []any{}})
	case "ListResourceTenants":
		return shared.JSONResponse(http.StatusOK, map[string]any{"Tenants": []any{}})
	case "GetReputationEntity":
		return shared.JSONResponse(http.StatusOK, map[string]any{})
	case "ListReputationEntities":
		return shared.JSONResponse(http.StatusOK, map[string]any{"ReputationEntities": []any{}})
	case "UpdateReputationEntityCustomerManagedStatus":
		return shared.JSONResponse(http.StatusOK, map[string]any{})
	case "UpdateReputationEntityPolicy":
		return shared.JSONResponse(http.StatusOK, map[string]any{})
	default:
		return shared.JSONError("InvalidAction", fmt.Sprintf("unknown action: %s", op), http.StatusBadRequest), nil
	}
}

func (p *Provider) ListResources(_ context.Context) ([]plugin.Resource, error) {
	ids, err := p.store.ListEmailIdentities()
	if err != nil {
		return nil, err
	}
	res := make([]plugin.Resource, 0, len(ids))
	for _, id := range ids {
		res = append(res, plugin.Resource{Type: "ses-email-identity", ID: id.Name, Name: id.Name})
	}
	return res, nil
}

func (p *Provider) GetMetrics(_ context.Context) (*plugin.ServiceMetrics, error) {
	return &plugin.ServiceMetrics{}, nil
}

// --- EmailIdentity ---

func (p *Provider) createEmailIdentity(params map[string]any) (*plugin.Response, error) {
	name, _ := params["EmailIdentity"].(string)
	if name == "" {
		return shared.JSONError("ValidationException", "EmailIdentity is required", http.StatusBadRequest), nil
	}
	identityType := "EMAIL_ADDRESS"
	if strings.Contains(name, ".") && !strings.Contains(name, "@") {
		identityType = "DOMAIN"
	}
	e := &EmailIdentity{
		Name:       name,
		Type:       identityType,
		Verified:   true,
		DkimStatus: "SUCCESS",
		ConfigSet:  "",
	}
	if err := p.store.CreateEmailIdentity(e); err != nil {
		if isUniqueErr(err) {
			return shared.JSONError("AlreadyExistsException", "identity already exists", http.StatusBadRequest), nil
		}
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"IdentityType":             identityType,
		"VerifiedForSendingStatus": true,
		"DkimAttributes": map[string]any{
			"SigningEnabled":          true,
			"Status":                  "SUCCESS",
			"SigningAttributesOrigin": "AWS_SES",
		},
	})
}

func (p *Provider) getEmailIdentity(name string) (*plugin.Response, error) {
	if name == "" {
		return shared.JSONError("ValidationException", "EmailIdentity is required", http.StatusBadRequest), nil
	}
	e, err := p.store.GetEmailIdentity(name)
	if err != nil {
		return shared.JSONError("NotFoundException", "identity not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, emailIdentityToMap(e))
}

func (p *Provider) listEmailIdentities() (*plugin.Response, error) {
	ids, err := p.store.ListEmailIdentities()
	if err != nil {
		return nil, err
	}
	items := make([]map[string]any, 0, len(ids))
	for _, id := range ids {
		items = append(items, map[string]any{
			"IdentityName":   id.Name,
			"IdentityType":   id.Type,
			"SendingEnabled": true,
		})
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"EmailIdentities": items})
}

func (p *Provider) deleteEmailIdentity(name string) (*plugin.Response, error) {
	if name == "" {
		return shared.JSONError("ValidationException", "EmailIdentity is required", http.StatusBadRequest), nil
	}
	if err := p.store.DeleteEmailIdentity(name); err != nil {
		return shared.JSONError("NotFoundException", "identity not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) putEmailIdentityDkimAttributes(name string, params map[string]any) (*plugin.Response, error) {
	if name == "" {
		return shared.JSONError("ValidationException", "EmailIdentity is required", http.StatusBadRequest), nil
	}
	if _, err := p.store.GetEmailIdentity(name); err != nil {
		return shared.JSONError("NotFoundException", "identity not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) putEmailIdentitySimpleUpdate(name string) (*plugin.Response, error) {
	if name == "" {
		return shared.JSONError("ValidationException", "EmailIdentity is required", http.StatusBadRequest), nil
	}
	if _, err := p.store.GetEmailIdentity(name); err != nil {
		return shared.JSONError("NotFoundException", "identity not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) putEmailIdentityConfigSet(name string, params map[string]any) (*plugin.Response, error) {
	if name == "" {
		return shared.JSONError("ValidationException", "EmailIdentity is required", http.StatusBadRequest), nil
	}
	if _, err := p.store.GetEmailIdentity(name); err != nil {
		return shared.JSONError("NotFoundException", "identity not found", http.StatusNotFound), nil
	}
	configSet, _ := params["ConfigurationSetName"].(string)
	if err := p.store.UpdateEmailIdentityConfigSet(name, configSet); err != nil {
		return shared.JSONError("NotFoundException", "identity not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) createEmailIdentityPolicy(identity, policyName string, params map[string]any) (*plugin.Response, error) {
	if identity == "" || policyName == "" {
		return shared.JSONError("ValidationException", "EmailIdentity and PolicyName are required", http.StatusBadRequest), nil
	}
	if _, err := p.store.GetEmailIdentity(identity); err != nil {
		return shared.JSONError("NotFoundException", "identity not found", http.StatusNotFound), nil
	}
	policy, _ := params["Policy"].(string)
	if err := p.store.UpsertEmailIdentityPolicy(identity, policyName, policy); err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) getEmailIdentityPolicies(identity string) (*plugin.Response, error) {
	if identity == "" {
		return shared.JSONError("ValidationException", "EmailIdentity is required", http.StatusBadRequest), nil
	}
	if _, err := p.store.GetEmailIdentity(identity); err != nil {
		return shared.JSONError("NotFoundException", "identity not found", http.StatusNotFound), nil
	}
	policies, err := p.store.GetEmailIdentityPolicies(identity)
	if err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"Policies": policies})
}

func (p *Provider) updateEmailIdentityPolicy(identity, policyName string, params map[string]any) (*plugin.Response, error) {
	if identity == "" || policyName == "" {
		return shared.JSONError("ValidationException", "EmailIdentity and PolicyName are required", http.StatusBadRequest), nil
	}
	if _, err := p.store.GetEmailIdentity(identity); err != nil {
		return shared.JSONError("NotFoundException", "identity not found", http.StatusNotFound), nil
	}
	policy, _ := params["Policy"].(string)
	if err := p.store.UpsertEmailIdentityPolicy(identity, policyName, policy); err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) deleteEmailIdentityPolicy(identity, policyName string) (*plugin.Response, error) {
	if identity == "" || policyName == "" {
		return shared.JSONError("ValidationException", "EmailIdentity and PolicyName are required", http.StatusBadRequest), nil
	}
	if err := p.store.DeleteEmailIdentityPolicy(identity, policyName); err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

// --- EmailTemplate ---

func (p *Provider) createEmailTemplate(params map[string]any) (*plugin.Response, error) {
	name, _ := params["TemplateName"].(string)
	if name == "" {
		return shared.JSONError("ValidationException", "TemplateName is required", http.StatusBadRequest), nil
	}
	content, _ := params["TemplateContent"].(map[string]any)
	subject, _ := content["Subject"].(string)
	html, _ := content["Html"].(string)
	text, _ := content["Text"].(string)

	t := &EmailTemplate{Name: name, Subject: subject, HTML: html, TextContent: text}
	if err := p.store.CreateEmailTemplate(t); err != nil {
		if isUniqueErr(err) {
			return shared.JSONError("AlreadyExistsException", "template already exists", http.StatusBadRequest), nil
		}
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) getEmailTemplate(name string) (*plugin.Response, error) {
	if name == "" {
		return shared.JSONError("ValidationException", "TemplateName is required", http.StatusBadRequest), nil
	}
	t, err := p.store.GetEmailTemplate(name)
	if err != nil {
		return shared.JSONError("NotFoundException", "template not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"TemplateName": t.Name,
		"TemplateContent": map[string]any{
			"Subject": t.Subject,
			"Html":    t.HTML,
			"Text":    t.TextContent,
		},
	})
}

func (p *Provider) listEmailTemplates() (*plugin.Response, error) {
	templates, err := p.store.ListEmailTemplates()
	if err != nil {
		return nil, err
	}
	items := make([]map[string]any, 0, len(templates))
	for _, t := range templates {
		items = append(items, map[string]any{
			"TemplateName":     t.Name,
			"CreatedTimestamp": t.CreatedAt,
		})
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"TemplatesMetadata": items})
}

func (p *Provider) updateEmailTemplate(name string, params map[string]any) (*plugin.Response, error) {
	if name == "" {
		return shared.JSONError("ValidationException", "TemplateName is required", http.StatusBadRequest), nil
	}
	content, _ := params["TemplateContent"].(map[string]any)
	subject, _ := content["Subject"].(string)
	html, _ := content["Html"].(string)
	text, _ := content["Text"].(string)
	if err := p.store.UpdateEmailTemplate(name, subject, html, text); err != nil {
		return shared.JSONError("NotFoundException", "template not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) deleteEmailTemplate(name string) (*plugin.Response, error) {
	if name == "" {
		return shared.JSONError("ValidationException", "TemplateName is required", http.StatusBadRequest), nil
	}
	if err := p.store.DeleteEmailTemplate(name); err != nil {
		return shared.JSONError("NotFoundException", "template not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) testRenderEmailTemplate(name string, params map[string]any) (*plugin.Response, error) {
	if name == "" {
		return shared.JSONError("ValidationException", "TemplateName is required", http.StatusBadRequest), nil
	}
	t, err := p.store.GetEmailTemplate(name)
	if err != nil {
		return shared.JSONError("NotFoundException", "template not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"RenderedTemplate": t.Subject + "\n" + t.HTML,
	})
}

// --- ConfigurationSet ---

func (p *Provider) createConfigurationSet(params map[string]any) (*plugin.Response, error) {
	name, _ := params["ConfigurationSetName"].(string)
	if name == "" {
		return shared.JSONError("ValidationException", "ConfigurationSetName is required", http.StatusBadRequest), nil
	}
	c := &ConfigSet{Name: name, SendingEnabled: true, Suppression: "{}", Tracking: "{}", Delivery: "{}"}
	if err := p.store.CreateConfigSet(c); err != nil {
		if isUniqueErr(err) {
			return shared.JSONError("AlreadyExistsException", "configuration set already exists", http.StatusBadRequest), nil
		}
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) getConfigurationSet(name string) (*plugin.Response, error) {
	if name == "" {
		return shared.JSONError("ValidationException", "ConfigurationSetName is required", http.StatusBadRequest), nil
	}
	c, err := p.store.GetConfigSet(name)
	if err != nil {
		return shared.JSONError("NotFoundException", "configuration set not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"ConfigurationSetName": c.Name,
		"SendingOptions":       map[string]any{"SendingEnabled": c.SendingEnabled},
		"ReputationOptions":    map[string]any{},
		"TrackingOptions":      map[string]any{},
		"DeliveryOptions":      map[string]any{},
		"SuppressionOptions":   map[string]any{},
		"Tags":                 []any{},
	})
}

func (p *Provider) listConfigurationSets() (*plugin.Response, error) {
	sets, err := p.store.ListConfigSets()
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, len(sets))
	for _, c := range sets {
		names = append(names, c.Name)
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"ConfigurationSets": names})
}

func (p *Provider) deleteConfigurationSet(name string) (*plugin.Response, error) {
	if name == "" {
		return shared.JSONError("ValidationException", "ConfigurationSetName is required", http.StatusBadRequest), nil
	}
	if err := p.store.DeleteConfigSet(name); err != nil {
		return shared.JSONError("NotFoundException", "configuration set not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) putConfigSetSending(name string, params map[string]any) (*plugin.Response, error) {
	if name == "" {
		return shared.JSONError("ValidationException", "ConfigurationSetName is required", http.StatusBadRequest), nil
	}
	if _, err := p.store.GetConfigSet(name); err != nil {
		return shared.JSONError("NotFoundException", "configuration set not found", http.StatusNotFound), nil
	}
	enabled := true
	if v, ok := params["SendingEnabled"].(bool); ok {
		enabled = v
	}
	if err := p.store.UpdateConfigSetSending(name, enabled); err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) putConfigSetSimple(name string) (*plugin.Response, error) {
	if name == "" {
		return shared.JSONError("ValidationException", "ConfigurationSetName is required", http.StatusBadRequest), nil
	}
	if _, err := p.store.GetConfigSet(name); err != nil {
		return shared.JSONError("NotFoundException", "configuration set not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) createConfigSetEventDest(configSetName string, params map[string]any) (*plugin.Response, error) {
	if configSetName == "" {
		return shared.JSONError("ValidationException", "ConfigurationSetName is required", http.StatusBadRequest), nil
	}
	if _, err := p.store.GetConfigSet(configSetName); err != nil {
		return shared.JSONError("NotFoundException", "configuration set not found", http.StatusNotFound), nil
	}
	destName, _ := params["EventDestinationName"].(string)
	if destName == "" {
		return shared.JSONError("ValidationException", "EventDestinationName is required", http.StatusBadRequest), nil
	}
	destJSON, _ := json.Marshal(params["EventDestination"])
	d := &ConfigSetEventDest{
		ConfigSetName: configSetName,
		DestName:      destName,
		Enabled:       true,
		MatchingTypes: "[]",
		Destination:   string(destJSON),
	}
	if err := p.store.CreateConfigSetEventDest(d); err != nil {
		if isUniqueErr(err) {
			return shared.JSONError("AlreadyExistsException", "event destination already exists", http.StatusBadRequest), nil
		}
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) getConfigSetEventDests(configSetName string) (*plugin.Response, error) {
	if configSetName == "" {
		return shared.JSONError("ValidationException", "ConfigurationSetName is required", http.StatusBadRequest), nil
	}
	if _, err := p.store.GetConfigSet(configSetName); err != nil {
		return shared.JSONError("NotFoundException", "configuration set not found", http.StatusNotFound), nil
	}
	dests, err := p.store.ListConfigSetEventDests(configSetName)
	if err != nil {
		return nil, err
	}
	items := make([]map[string]any, 0, len(dests))
	for _, d := range dests {
		items = append(items, map[string]any{
			"Name":    d.DestName,
			"Enabled": d.Enabled,
		})
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"EventDestinations": items})
}

func (p *Provider) updateConfigSetEventDest(configSetName, destName string, params map[string]any) (*plugin.Response, error) {
	if configSetName == "" || destName == "" {
		return shared.JSONError("ValidationException", "ConfigurationSetName and EventDestinationName are required", http.StatusBadRequest), nil
	}
	destJSON, _ := json.Marshal(params["EventDestination"])
	d := &ConfigSetEventDest{
		ConfigSetName: configSetName,
		DestName:      destName,
		Enabled:       true,
		MatchingTypes: "[]",
		Destination:   string(destJSON),
	}
	if err := p.store.UpdateConfigSetEventDest(d); err != nil {
		return shared.JSONError("NotFoundException", "event destination not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) deleteConfigSetEventDest(configSetName, destName string) (*plugin.Response, error) {
	if configSetName == "" || destName == "" {
		return shared.JSONError("ValidationException", "ConfigurationSetName and EventDestinationName are required", http.StatusBadRequest), nil
	}
	if err := p.store.DeleteConfigSetEventDest(configSetName, destName); err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

// --- ContactList ---

func (p *Provider) createContactList(params map[string]any) (*plugin.Response, error) {
	name, _ := params["ContactListName"].(string)
	if name == "" {
		return shared.JSONError("ValidationException", "ContactListName is required", http.StatusBadRequest), nil
	}
	desc, _ := params["Description"].(string)
	cl := &ContactList{Name: name, Description: desc, Topics: "[]"}
	if err := p.store.CreateContactList(cl); err != nil {
		if isUniqueErr(err) {
			return shared.JSONError("AlreadyExistsException", "contact list already exists", http.StatusBadRequest), nil
		}
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) getContactList(name string) (*plugin.Response, error) {
	if name == "" {
		return shared.JSONError("ValidationException", "ContactListName is required", http.StatusBadRequest), nil
	}
	cl, err := p.store.GetContactList(name)
	if err != nil {
		return shared.JSONError("NotFoundException", "contact list not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"ContactListName":      cl.Name,
		"Description":          cl.Description,
		"Topics":               []any{},
		"CreatedTimestamp":     cl.CreatedAt,
		"LastUpdatedTimestamp": cl.UpdatedAt,
	})
}

func (p *Provider) listContactLists() (*plugin.Response, error) {
	lists, err := p.store.ListContactLists()
	if err != nil {
		return nil, err
	}
	items := make([]map[string]any, 0, len(lists))
	for _, cl := range lists {
		items = append(items, map[string]any{
			"ContactListName": cl.Name,
			"Description":     cl.Description,
		})
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"ContactLists": items})
}

func (p *Provider) updateContactList(name string, params map[string]any) (*plugin.Response, error) {
	if name == "" {
		return shared.JSONError("ValidationException", "ContactListName is required", http.StatusBadRequest), nil
	}
	desc, _ := params["Description"].(string)
	if err := p.store.UpdateContactList(name, desc, "[]"); err != nil {
		return shared.JSONError("NotFoundException", "contact list not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) deleteContactList(name string) (*plugin.Response, error) {
	if name == "" {
		return shared.JSONError("ValidationException", "ContactListName is required", http.StatusBadRequest), nil
	}
	if err := p.store.DeleteContactList(name); err != nil {
		return shared.JSONError("NotFoundException", "contact list not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

// --- Contact ---

func (p *Provider) createContact(listName string, params map[string]any) (*plugin.Response, error) {
	if listName == "" {
		return shared.JSONError("ValidationException", "ContactListName is required", http.StatusBadRequest), nil
	}
	email, _ := params["EmailAddress"].(string)
	if email == "" {
		return shared.JSONError("ValidationException", "EmailAddress is required", http.StatusBadRequest), nil
	}
	unsub := false
	if v, ok := params["UnsubscribeAll"].(bool); ok {
		unsub = v
	}
	c := &Contact{Email: email, ListName: listName, Topics: "[]", UnsubscribeAll: unsub, Attributes: "{}"}
	if err := p.store.CreateContact(c); err != nil {
		if isUniqueErr(err) {
			return shared.JSONError("AlreadyExistsException", "contact already exists", http.StatusBadRequest), nil
		}
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) getContact(listName, email string) (*plugin.Response, error) {
	if listName == "" || email == "" {
		return shared.JSONError("ValidationException", "ContactListName and EmailAddress are required", http.StatusBadRequest), nil
	}
	c, err := p.store.GetContact(email, listName)
	if err != nil {
		return shared.JSONError("NotFoundException", "contact not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"EmailAddress":         c.Email,
		"ContactListName":      c.ListName,
		"UnsubscribeAll":       c.UnsubscribeAll,
		"TopicPreferences":     []any{},
		"CreatedTimestamp":     c.CreatedAt,
		"LastUpdatedTimestamp": c.UpdatedAt,
	})
}

func (p *Provider) listContacts(listName string) (*plugin.Response, error) {
	if listName == "" {
		return shared.JSONError("ValidationException", "ContactListName is required", http.StatusBadRequest), nil
	}
	contacts, err := p.store.ListContacts(listName)
	if err != nil {
		return nil, err
	}
	items := make([]map[string]any, 0, len(contacts))
	for _, c := range contacts {
		items = append(items, map[string]any{
			"EmailAddress":   c.Email,
			"UnsubscribeAll": c.UnsubscribeAll,
		})
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"Contacts": items})
}

func (p *Provider) updateContact(listName, email string, params map[string]any) (*plugin.Response, error) {
	if listName == "" || email == "" {
		return shared.JSONError("ValidationException", "ContactListName and EmailAddress are required", http.StatusBadRequest), nil
	}
	unsub := false
	if v, ok := params["UnsubscribeAll"].(bool); ok {
		unsub = v
	}
	if err := p.store.UpdateContact(email, listName, "[]", unsub, "{}"); err != nil {
		return shared.JSONError("NotFoundException", "contact not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) deleteContact(listName, email string) (*plugin.Response, error) {
	if listName == "" || email == "" {
		return shared.JSONError("ValidationException", "ContactListName and EmailAddress are required", http.StatusBadRequest), nil
	}
	if err := p.store.DeleteContact(email, listName); err != nil {
		return shared.JSONError("NotFoundException", "contact not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

// --- DedicatedIpPool ---

func (p *Provider) createDedicatedIpPool(params map[string]any) (*plugin.Response, error) {
	name, _ := params["PoolName"].(string)
	if name == "" {
		return shared.JSONError("ValidationException", "PoolName is required", http.StatusBadRequest), nil
	}
	scalingMode := "STANDARD"
	if v, ok := params["ScalingMode"].(string); ok && v != "" {
		scalingMode = v
	}
	pool := &DedicatedIpPool{Name: name, ScalingMode: scalingMode}
	if err := p.store.CreateDedicatedIpPool(pool); err != nil {
		if isUniqueErr(err) {
			return shared.JSONError("AlreadyExistsException", "dedicated IP pool already exists", http.StatusBadRequest), nil
		}
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) listDedicatedIpPools() (*plugin.Response, error) {
	pools, err := p.store.ListDedicatedIpPools()
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, len(pools))
	for _, pool := range pools {
		names = append(names, pool.Name)
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"DedicatedIpPools": names})
}

func (p *Provider) getDedicatedIpPool(name string) (*plugin.Response, error) {
	if name == "" {
		return shared.JSONError("ValidationException", "PoolName is required", http.StatusBadRequest), nil
	}
	pool, err := p.store.GetDedicatedIpPool(name)
	if err != nil {
		return shared.JSONError("NotFoundException", "dedicated IP pool not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"DedicatedIpPool": map[string]any{
			"PoolName":    pool.Name,
			"ScalingMode": pool.ScalingMode,
		},
	})
}

func (p *Provider) deleteDedicatedIpPool(name string) (*plugin.Response, error) {
	if name == "" {
		return shared.JSONError("ValidationException", "PoolName is required", http.StatusBadRequest), nil
	}
	if err := p.store.DeleteDedicatedIpPool(name); err != nil {
		return shared.JSONError("NotFoundException", "dedicated IP pool not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

// --- SuppressedDestination ---

func (p *Provider) putSuppressedDestination(params map[string]any) (*plugin.Response, error) {
	email, _ := params["EmailAddress"].(string)
	if email == "" {
		return shared.JSONError("ValidationException", "EmailAddress is required", http.StatusBadRequest), nil
	}
	reason := "BOUNCE"
	if v, ok := params["Reason"].(string); ok && v != "" {
		reason = v
	}
	sd := &SuppressedDestination{Email: email, Reason: reason}
	if err := p.store.PutSuppressedDestination(sd); err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) getSuppressedDestination(email string) (*plugin.Response, error) {
	if email == "" {
		return shared.JSONError("ValidationException", "EmailAddress is required", http.StatusBadRequest), nil
	}
	sd, err := p.store.GetSuppressedDestination(email)
	if err != nil {
		return shared.JSONError("NotFoundException", "suppressed destination not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"SuppressedDestination": map[string]any{
			"EmailAddress":   sd.Email,
			"Reason":         sd.Reason,
			"LastUpdateTime": sd.CreatedAt,
			"Attributes":     map[string]any{},
		},
	})
}

func (p *Provider) listSuppressedDestinations() (*plugin.Response, error) {
	sds, err := p.store.ListSuppressedDestinations()
	if err != nil {
		return nil, err
	}
	items := make([]map[string]any, 0, len(sds))
	for _, sd := range sds {
		items = append(items, map[string]any{
			"EmailAddress":   sd.Email,
			"Reason":         sd.Reason,
			"LastUpdateTime": sd.CreatedAt,
		})
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"SuppressedDestinationSummaries": items})
}

func (p *Provider) deleteSuppressedDestination(email string) (*plugin.Response, error) {
	if email == "" {
		return shared.JSONError("ValidationException", "EmailAddress is required", http.StatusBadRequest), nil
	}
	if err := p.store.DeleteSuppressedDestination(email); err != nil {
		return shared.JSONError("NotFoundException", "suppressed destination not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

// --- Send ---

func (p *Provider) sendEmail(params map[string]any) (*plugin.Response, error) {
	msgID := "msg-" + shared.GenerateID("", 28)
	source, _ := params["FromEmailAddress"].(string)
	destJSON, _ := json.Marshal(params["Destination"])
	subject := ""
	body := ""
	if content, ok := params["Content"].(map[string]any); ok {
		if simple, ok := content["Simple"].(map[string]any); ok {
			if subj, ok := simple["Subject"].(map[string]any); ok {
				subject, _ = subj["Data"].(string)
			}
			if b, ok := simple["Body"].(map[string]any); ok {
				if html, ok := b["Html"].(map[string]any); ok {
					body, _ = html["Data"].(string)
				} else if text, ok := b["Text"].(map[string]any); ok {
					body, _ = text["Data"].(string)
				}
			}
		}
	}
	e := &SentEmail{
		MessageID:   msgID,
		Source:      source,
		Destination: string(destJSON),
		Subject:     subject,
		Body:        body,
	}
	if err := p.store.CreateSentEmail(e); err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"MessageId": msgID})
}

func (p *Provider) sendBulkEmail(params map[string]any) (*plugin.Response, error) {
	entries, _ := params["BulkEmailEntries"].([]any)
	results := make([]map[string]any, 0, len(entries))
	for range entries {
		msgID := "msg-" + shared.GenerateID("", 28)
		results = append(results, map[string]any{
			"MessageId": msgID,
			"Status":    "SUCCESS",
		})
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"BulkEmailEntryResults": results})
}

func (p *Provider) sendCustomVerificationEmail(params map[string]any) (*plugin.Response, error) {
	msgID := "msg-" + shared.GenerateID("", 28)
	return shared.JSONResponse(http.StatusOK, map[string]any{"MessageId": msgID})
}

// --- Account ---

func (p *Provider) getAccount() (*plugin.Response, error) {
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"DedicatedIpAutoWarmupEnabled": true,
		"EnforcementStatus":            "HEALTHY",
		"ProductionAccessEnabled":      true,
		"SendQuota": map[string]any{
			"Max24HourSend":   50000.0,
			"MaxSendRate":     14.0,
			"SentLast24Hours": 0.0,
		},
		"SendingEnabled": true,
		"SuppressionAttributes": map[string]any{
			"SuppressedReasons": []any{},
		},
	})
}

// --- Tags ---

func (p *Provider) tagResource(req *http.Request, params map[string]any) (*plugin.Response, error) {
	arn, _ := params["ResourceArn"].(string)
	if arn == "" {
		return shared.JSONError("ValidationException", "ResourceArn is required", http.StatusBadRequest), nil
	}
	rawTags, _ := params["Tags"].([]any)
	tags := make(map[string]string)
	for _, item := range rawTags {
		if m, ok := item.(map[string]any); ok {
			k, _ := m["Key"].(string)
			v, _ := m["Value"].(string)
			if k != "" {
				tags[k] = v
			}
		}
	}
	if err := p.store.tags.AddTags(arn, tags); err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) untagResource(req *http.Request) (*plugin.Response, error) {
	arn := req.URL.Query().Get("ResourceArn")
	if arn == "" {
		return shared.JSONError("ValidationException", "ResourceArn is required", http.StatusBadRequest), nil
	}
	keys := req.URL.Query()["TagKeys"]
	if err := p.store.tags.RemoveTags(arn, keys); err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) listTagsForResource(req *http.Request) (*plugin.Response, error) {
	arn := req.URL.Query().Get("ResourceArn")
	if arn == "" {
		return shared.JSONError("ValidationException", "ResourceArn is required", http.StatusBadRequest), nil
	}
	tagsMap, err := p.store.tags.ListTags(arn)
	if err != nil {
		return nil, err
	}
	tags := make([]map[string]any, 0, len(tagsMap))
	for k, v := range tagsMap {
		tags = append(tags, map[string]any{"Key": k, "Value": v})
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"Tags": tags})
}

// --- Helpers ---

func emailIdentityToMap(e *EmailIdentity) map[string]any {
	return map[string]any{
		"IdentityType":             e.Type,
		"FeedbackForwardingStatus": true,
		"VerifiedForSendingStatus": e.Verified,
		"DkimAttributes": map[string]any{
			"SigningEnabled":          true,
			"Status":                  e.DkimStatus,
			"SigningAttributesOrigin": "AWS_SES",
		},
		"MailFromAttributes": map[string]any{
			"BehaviorOnMxFailure": "USE_DEFAULT_VALUE",
		},
		"Policies":             map[string]any{},
		"Tags":                 []any{},
		"ConfigurationSetName": e.ConfigSet,
	}
}

// pathParam extracts the first path segment after `key` in the URL path.
func pathParam(req *http.Request, key string) string {
	parts := strings.Split(req.URL.Path, "/")
	for i, p := range parts {
		if p == key && i+1 < len(parts) {
			return parts[i+1]
		}
	}
	return ""
}

// pathParam2 extracts the second-to-last pathParam in a URL like /a/{key1}/b/{key2}.
// It finds the last occurrence of key's value neighbor.
func pathParam2(req *http.Request, key string) string {
	parts := strings.Split(req.URL.Path, "/")
	for i, p := range parts {
		if p == key && i+1 < len(parts) {
			return parts[i+1]
		}
	}
	return ""
}

// resolveOp maps HTTP method + URL path to an SESv2 operation name.
// This is needed because REST-JSON services don't send X-Amz-Target,
// so the gateway passes op="" and we must resolve from the URL.
func resolveOp(method, urlPath string) string {
	// Strip the /v2/email/ prefix.
	const prefix = "/v2/email/"
	if !strings.HasPrefix(urlPath, prefix) {
		return ""
	}
	rest := strings.TrimPrefix(urlPath, prefix)
	rest = strings.TrimSuffix(rest, "/")
	if rest == "" {
		return ""
	}

	// Split into segments.
	segs := strings.Split(rest, "/")
	n := len(segs)
	resource := segs[0]

	switch resource {
	case "identities":
		if n == 1 {
			if method == "POST" {
				return "CreateEmailIdentity"
			}
			return "ListEmailIdentities" // GET
		}
		if n == 2 {
			if method == "DELETE" {
				return "DeleteEmailIdentity"
			}
			return "GetEmailIdentity" // GET
		}
		if n == 3 {
			// identities/{name}/{sub}
			switch segs[2] {
			case "dkim":
				return "PutEmailIdentityDkimAttributes"
			case "feedback":
				return "PutEmailIdentityFeedbackAttributes"
			case "mail-from":
				return "PutEmailIdentityMailFromAttributes"
			case "configuration-set":
				return "PutEmailIdentityConfigurationSetAttributes"
			case "policies":
				if method == "GET" {
					return "GetEmailIdentityPolicies"
				}
			}
		}
		if n == 4 {
			// identities/{name}/dkim/signing
			if segs[2] == "dkim" && segs[3] == "signing" {
				return "PutEmailIdentityDkimSigningAttributes"
			}
			// identities/{name}/policies/{policyName}
			if segs[2] == "policies" {
				switch method {
				case "POST":
					return "CreateEmailIdentityPolicy"
				case "PUT":
					return "UpdateEmailIdentityPolicy"
				case "DELETE":
					return "DeleteEmailIdentityPolicy"
				}
			}
		}

	case "templates":
		if n == 1 {
			if method == "POST" {
				return "CreateEmailTemplate"
			}
			return "ListEmailTemplates" // GET
		}
		if n == 2 {
			switch method {
			case "GET":
				return "GetEmailTemplate"
			case "PUT":
				return "UpdateEmailTemplate"
			case "DELETE":
				return "DeleteEmailTemplate"
			}
		}
		if n == 3 && segs[2] == "render" {
			return "TestRenderEmailTemplate"
		}

	case "configuration-sets":
		if n == 1 {
			if method == "POST" {
				return "CreateConfigurationSet"
			}
			return "ListConfigurationSets" // GET
		}
		if n == 2 {
			if method == "DELETE" {
				return "DeleteConfigurationSet"
			}
			return "GetConfigurationSet" // GET
		}
		if n == 3 {
			// configuration-sets/{name}/{sub}
			switch segs[2] {
			case "sending":
				return "PutConfigurationSetSendingOptions"
			case "reputation-options":
				return "PutConfigurationSetReputationOptions"
			case "suppression-options":
				return "PutConfigurationSetSuppressionOptions"
			case "tracking-options":
				return "PutConfigurationSetTrackingOptions"
			case "delivery-options":
				return "PutConfigurationSetDeliveryOptions"
			case "vdm-options":
				return "PutConfigurationSetVdmOptions"
			case "archiving-options":
				return "PutConfigurationSetArchivingOptions"
			case "event-destinations":
				if method == "POST" {
					return "CreateConfigurationSetEventDestination"
				}
				return "GetConfigurationSetEventDestinations" // GET
			}
		}
		if n == 4 && segs[2] == "event-destinations" {
			switch method {
			case "PUT":
				return "UpdateConfigurationSetEventDestination"
			case "DELETE":
				return "DeleteConfigurationSetEventDestination"
			}
		}

	case "contact-lists":
		if n == 1 {
			if method == "POST" {
				return "CreateContactList"
			}
			return "ListContactLists" // GET
		}
		if n == 2 {
			switch method {
			case "GET":
				return "GetContactList"
			case "PUT":
				return "UpdateContactList"
			case "DELETE":
				return "DeleteContactList"
			}
		}
		if n == 3 && segs[2] == "contacts" {
			if method == "POST" {
				return "CreateContact"
			}
			return "ListContacts" // GET
		}
		if n == 4 && segs[2] == "contacts" {
			switch method {
			case "GET":
				return "GetContact"
			case "PUT":
				return "UpdateContact"
			case "DELETE":
				return "DeleteContact"
			}
		}

	case "dedicated-ip-pools":
		if n == 1 {
			if method == "POST" {
				return "CreateDedicatedIpPool"
			}
			return "ListDedicatedIpPools" // GET
		}
		if n == 2 {
			if method == "DELETE" {
				return "DeleteDedicatedIpPool"
			}
			return "GetDedicatedIpPool" // GET
		}

	case "suppression":
		// suppression/addresses
		if n >= 2 && segs[1] == "addresses" {
			if n == 2 {
				if method == "POST" {
					return "PutSuppressedDestination"
				}
				return "ListSuppressedDestinations" // GET
			}
			if n == 3 {
				if method == "DELETE" {
					return "DeleteSuppressedDestination"
				}
				return "GetSuppressedDestination" // GET
			}
		}

	case "outbound-emails":
		return "SendEmail"
	case "outbound-bulk-emails":
		return "SendBulkEmail"
	case "outbound-custom-verification-emails":
		return "SendCustomVerificationEmail"

	case "account":
		if n == 1 {
			return "GetAccount" // GET
		}
		if n == 2 {
			switch segs[1] {
			case "details":
				return "PutAccountDetails"
			case "sending":
				return "PutAccountSendingAttributes"
			case "suppression":
				return "PutAccountSuppressionAttributes"
			case "vdm":
				return "PutAccountVdmAttributes"
			}
		}
		if n == 3 && segs[1] == "dedicated-ips" && segs[2] == "warmup" {
			return "PutAccountDedicatedIpWarmupAttributes"
		}

	case "tags":
		switch method {
		case "PUT":
			return "TagResource"
		case "DELETE":
			return "UntagResource"
		case "GET":
			return "ListTagsForResource"
		}
	}

	return ""
}
