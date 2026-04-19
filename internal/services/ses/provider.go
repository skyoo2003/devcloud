// SPDX-License-Identifier: Apache-2.0

package ses

import (
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"

	"github.com/skyoo2003/devcloud/internal/plugin"
	"github.com/skyoo2003/devcloud/internal/shared"
)

// Provider implements the SimpleEmailService (Query/XML protocol).
type Provider struct {
	store *Store
}

func (p *Provider) ServiceID() string             { return "ses" }
func (p *Provider) ServiceName() string           { return "SimpleEmailService" }
func (p *Provider) Protocol() plugin.ProtocolType { return plugin.ProtocolQuery }

func (p *Provider) Init(cfg plugin.PluginConfig) error {
	if err := os.MkdirAll(cfg.DataDir, 0o755); err != nil {
		return fmt.Errorf("init ses: %w", err)
	}
	var err error
	p.store, err = NewStore(cfg.DataDir)
	return err
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
		return sesError("InvalidRequest", "failed to read request body", http.StatusBadRequest), nil
	}
	form, err := url.ParseQuery(string(body))
	if err != nil {
		return sesError("InvalidRequest", "failed to parse form body", http.StatusBadRequest), nil
	}
	action := op
	if action == "" {
		action = form.Get("Action")
	}

	switch action {
	// Identity operations
	case "VerifyEmailIdentity":
		return p.handleVerifyEmailIdentity(form)
	case "VerifyEmailAddress":
		return p.handleVerifyEmailAddress(form)
	case "VerifyDomainIdentity":
		return p.handleVerifyDomainIdentity(form)
	case "VerifyDomainDkim":
		return p.handleVerifyDomainDkim(form)
	case "DeleteIdentity":
		return p.handleDeleteIdentity(form)
	case "ListIdentities":
		return p.handleListIdentities(form)
	case "GetIdentityVerificationAttributes":
		return p.handleGetIdentityVerificationAttributes(form)
	case "GetIdentityDkimAttributes":
		return p.handleGetIdentityDkimAttributes(form)
	case "SetIdentityDkimEnabled":
		return p.handleSetIdentityDkimEnabled(form)
	case "ListVerifiedEmailAddresses":
		return p.handleListVerifiedEmailAddresses(form)
	case "DeleteVerifiedEmailAddress":
		return p.handleDeleteVerifiedEmailAddress(form)

	// Template operations
	case "CreateTemplate":
		return p.handleCreateTemplate(form)
	case "GetTemplate":
		return p.handleGetTemplate(form)
	case "ListTemplates":
		return p.handleListTemplates(form)
	case "UpdateTemplate":
		return p.handleUpdateTemplate(form)
	case "DeleteTemplate":
		return p.handleDeleteTemplate(form)
	case "TestRenderTemplate":
		return p.handleTestRenderTemplate(form)

	// ConfigurationSet operations
	case "CreateConfigurationSet":
		return p.handleCreateConfigurationSet(form)
	case "DescribeConfigurationSet":
		return p.handleDescribeConfigurationSet(form)
	case "ListConfigurationSets":
		return p.handleListConfigurationSets(form)
	case "DeleteConfigurationSet":
		return p.handleDeleteConfigurationSet(form)

	// ReceiptRuleSet operations
	case "CreateReceiptRuleSet":
		return p.handleCreateReceiptRuleSet(form)
	case "DescribeReceiptRuleSet":
		return p.handleDescribeReceiptRuleSet(form)
	case "ListReceiptRuleSets":
		return p.handleListReceiptRuleSets(form)
	case "DeleteReceiptRuleSet":
		return p.handleDeleteReceiptRuleSet(form)
	case "SetActiveReceiptRuleSet":
		return p.handleSetActiveReceiptRuleSet(form)
	case "DescribeActiveReceiptRuleSet":
		return p.handleDescribeActiveReceiptRuleSet(form)
	case "CloneReceiptRuleSet":
		return p.handleCloneReceiptRuleSet(form)
	case "ReorderReceiptRuleSet":
		return p.handleReorderReceiptRuleSet(form)

	// ReceiptRule operations
	case "CreateReceiptRule":
		return p.handleCreateReceiptRule(form)
	case "DescribeReceiptRule":
		return p.handleDescribeReceiptRule(form)
	case "UpdateReceiptRule":
		return p.handleUpdateReceiptRule(form)
	case "DeleteReceiptRule":
		return p.handleDeleteReceiptRule(form)
	case "SetReceiptRulePosition":
		return p.handleSetReceiptRulePosition(form)

	// ReceiptFilter operations
	case "CreateReceiptFilter":
		return p.handleCreateReceiptFilter(form)
	case "ListReceiptFilters":
		return p.handleListReceiptFilters(form)
	case "DeleteReceiptFilter":
		return p.handleDeleteReceiptFilter(form)

	// Send operations
	case "SendEmail":
		return p.handleSendEmail(form)
	case "SendRawEmail":
		return p.handleSendRawEmail(form)
	case "SendTemplatedEmail":
		return p.handleSendTemplatedEmail(form)
	case "SendBulkTemplatedEmail":
		return p.handleSendBulkTemplatedEmail(form)
	case "SendCustomVerificationEmail":
		return p.handleSendCustomVerificationEmail(form)

	// Quota / stats
	case "GetSendQuota":
		return p.handleGetSendQuota(form)
	case "GetSendStatistics":
		return p.handleGetSendStatistics(form)
	case "GetAccountSendingEnabled":
		return p.handleGetAccountSendingEnabled(form)
	case "UpdateAccountSendingEnabled":
		return p.handleUpdateAccountSendingEnabled(form)

	// Tag / notification / misc ops — return success
	case "ListTagsForResource",
		"TagResource",
		"UntagResource",
		"SetIdentityHeadersInNotificationsEnabled",
		"SetIdentityMailFromDomain",
		"SetIdentityNotificationTopic",
		"SetIdentityFeedbackForwardingEnabled",
		"GetIdentityMailFromDomainAttributes",
		"GetIdentityNotificationAttributes",
		"GetIdentityPolicies",
		"ListIdentityPolicies",
		"PutIdentityPolicy",
		"DeleteIdentityPolicy",
		"SendBounce",
		"CreateConfigurationSetEventDestination",
		"UpdateConfigurationSetEventDestination",
		"DeleteConfigurationSetEventDestination",
		"CreateConfigurationSetTrackingOptions",
		"UpdateConfigurationSetTrackingOptions",
		"DeleteConfigurationSetTrackingOptions",
		"DescribeConfigurationSetEventDestinations",
		"ListConfigurationSetEventDestinations",
		"GetCustomVerificationEmailTemplate",
		"ListCustomVerificationEmailTemplates",
		"CreateCustomVerificationEmailTemplate",
		"UpdateCustomVerificationEmailTemplate",
		"DeleteCustomVerificationEmailTemplate":
		return p.handleSuccessEmpty(action)

	default:
		return sesError("InvalidAction", fmt.Sprintf("unknown action: %s", action), http.StatusBadRequest), nil
	}
}

func (p *Provider) ListResources(ctx context.Context) ([]plugin.Resource, error) {
	ids, err := p.store.ListIdentities("")
	if err != nil {
		return nil, err
	}
	out := make([]plugin.Resource, 0, len(ids))
	for _, id := range ids {
		out = append(out, plugin.Resource{Type: "identity", ID: id.Identity, Name: id.Identity})
	}
	return out, nil
}

func (p *Provider) GetMetrics(_ context.Context) (*plugin.ServiceMetrics, error) {
	return &plugin.ServiceMetrics{}, nil
}

// --- helpers ---

func sesError(code, msg string, status int) *plugin.Response {
	return shared.QueryXMLError(code, msg, status)
}

func sesXML(status int, v any) (*plugin.Response, error) {
	return shared.XMLResponse(status, v)
}

// collectMembers collects form values like "Param.member.1", "Param.member.2", ...
func collectMembers(form url.Values, prefix string) []string {
	var out []string
	for i := 1; ; i++ {
		v := form.Get(fmt.Sprintf("%s.member.%d", prefix, i))
		if v == "" {
			break
		}
		out = append(out, v)
	}
	return out
}

// --- Identity handlers ---

func (p *Provider) handleVerifyEmailIdentity(form url.Values) (*plugin.Response, error) {
	addr := form.Get("EmailAddress")
	if addr == "" {
		return sesError("MissingParameter", "EmailAddress is required", http.StatusBadRequest), nil
	}
	if err := p.store.UpsertIdentity(addr, "EmailAddress"); err != nil {
		return nil, err
	}
	type resp struct {
		XMLName xml.Name `xml:"VerifyEmailIdentityResponse"`
		Result  struct{} `xml:"VerifyEmailIdentityResult"`
	}
	return sesXML(http.StatusOK, resp{})
}

func (p *Provider) handleVerifyEmailAddress(form url.Values) (*plugin.Response, error) {
	addr := form.Get("EmailAddress")
	if addr == "" {
		return sesError("MissingParameter", "EmailAddress is required", http.StatusBadRequest), nil
	}
	if err := p.store.UpsertIdentity(addr, "EmailAddress"); err != nil {
		return nil, err
	}
	type resp struct {
		XMLName xml.Name `xml:"VerifyEmailAddressResponse"`
		Result  struct{} `xml:"VerifyEmailAddressResult"`
	}
	return sesXML(http.StatusOK, resp{})
}

func (p *Provider) handleVerifyDomainIdentity(form url.Values) (*plugin.Response, error) {
	domain := form.Get("Domain")
	if domain == "" {
		return sesError("MissingParameter", "Domain is required", http.StatusBadRequest), nil
	}
	if err := p.store.UpsertIdentity(domain, "Domain"); err != nil {
		return nil, err
	}
	type result struct {
		VerificationToken string `xml:"VerificationToken"`
	}
	type resp struct {
		XMLName xml.Name `xml:"VerifyDomainIdentityResponse"`
		Result  result   `xml:"VerifyDomainIdentityResult"`
	}
	return sesXML(http.StatusOK, resp{Result: result{VerificationToken: shared.GenerateID("", 32)}})
}

func (p *Provider) handleVerifyDomainDkim(form url.Values) (*plugin.Response, error) {
	domain := form.Get("Domain")
	if domain == "" {
		return sesError("MissingParameter", "Domain is required", http.StatusBadRequest), nil
	}
	if err := p.store.UpsertIdentity(domain, "Domain"); err != nil {
		return nil, err
	}
	if err := p.store.SetDkimEnabled(domain, true); err != nil {
		return nil, err
	}
	id, err := p.store.GetIdentity(domain)
	if err != nil {
		return nil, err
	}
	type memberList struct {
		Members []string `xml:"member"`
	}
	type result struct {
		DkimTokens memberList `xml:"DkimTokens"`
	}
	type resp struct {
		XMLName xml.Name `xml:"VerifyDomainDkimResponse"`
		Result  result   `xml:"VerifyDomainDkimResult"`
	}
	return sesXML(http.StatusOK, resp{Result: result{DkimTokens: memberList{Members: id.DkimTokens}}})
}

func (p *Provider) handleDeleteIdentity(form url.Values) (*plugin.Response, error) {
	identity := form.Get("Identity")
	if identity == "" {
		return sesError("MissingParameter", "Identity is required", http.StatusBadRequest), nil
	}
	if err := p.store.DeleteIdentity(identity); err != nil && !errors.Is(err, errNotFound) {
		return nil, err
	}
	type resp struct {
		XMLName xml.Name `xml:"DeleteIdentityResponse"`
		Result  struct{} `xml:"DeleteIdentityResult"`
	}
	return sesXML(http.StatusOK, resp{})
}

func (p *Provider) handleListIdentities(form url.Values) (*plugin.Response, error) {
	identityType := form.Get("IdentityType")
	ids, err := p.store.ListIdentities(identityType)
	if err != nil {
		return nil, err
	}
	members := make([]string, 0, len(ids))
	for _, id := range ids {
		members = append(members, id.Identity)
	}
	type result struct {
		Identities struct {
			Members []string `xml:"member"`
		} `xml:"Identities"`
	}
	type resp struct {
		XMLName xml.Name `xml:"ListIdentitiesResponse"`
		Result  result   `xml:"ListIdentitiesResult"`
	}
	r := resp{}
	r.Result.Identities.Members = members
	return sesXML(http.StatusOK, r)
}

func (p *Provider) handleGetIdentityVerificationAttributes(form url.Values) (*plugin.Response, error) {
	identities := collectMembers(form, "Identities")
	type attrEntry struct {
		Key   string `xml:"key"`
		Value struct {
			VerificationStatus string `xml:"VerificationStatus"`
			VerificationToken  string `xml:"VerificationToken,omitempty"`
		} `xml:"value"`
	}
	var entries []attrEntry
	for _, identity := range identities {
		id, err := p.store.GetIdentity(identity)
		status := "Pending"
		if err == nil {
			status = id.Verification
		}
		e := attrEntry{Key: identity}
		e.Value.VerificationStatus = status
		entries = append(entries, e)
	}
	type result struct {
		Attrs struct {
			Entries []attrEntry `xml:"entry"`
		} `xml:"VerificationAttributes"`
	}
	type resp struct {
		XMLName xml.Name `xml:"GetIdentityVerificationAttributesResponse"`
		Result  result   `xml:"GetIdentityVerificationAttributesResult"`
	}
	r := resp{}
	r.Result.Attrs.Entries = entries
	return sesXML(http.StatusOK, r)
}

func (p *Provider) handleGetIdentityDkimAttributes(form url.Values) (*plugin.Response, error) {
	identities := collectMembers(form, "Identities")
	type tokenList struct {
		Members []string `xml:"member"`
	}
	type attrEntry struct {
		Key   string `xml:"key"`
		Value struct {
			DkimEnabled            bool      `xml:"DkimEnabled"`
			DkimVerificationStatus string    `xml:"DkimVerificationStatus"`
			DkimTokens             tokenList `xml:"DkimTokens"`
		} `xml:"value"`
	}
	var entries []attrEntry
	for _, identity := range identities {
		id, _ := p.store.GetIdentity(identity)
		e := attrEntry{Key: identity}
		if id != nil {
			e.Value.DkimEnabled = id.DkimEnabled
			if id.DkimEnabled {
				e.Value.DkimVerificationStatus = "Success"
				e.Value.DkimTokens.Members = id.DkimTokens
			} else {
				e.Value.DkimVerificationStatus = "NotStarted"
			}
		}
		entries = append(entries, e)
	}
	type result struct {
		Attrs struct {
			Entries []attrEntry `xml:"entry"`
		} `xml:"DkimAttributes"`
	}
	type resp struct {
		XMLName xml.Name `xml:"GetIdentityDkimAttributesResponse"`
		Result  result   `xml:"GetIdentityDkimAttributesResult"`
	}
	r := resp{}
	r.Result.Attrs.Entries = entries
	return sesXML(http.StatusOK, r)
}

func (p *Provider) handleSetIdentityDkimEnabled(form url.Values) (*plugin.Response, error) {
	identity := form.Get("Identity")
	enabled := strings.EqualFold(form.Get("DkimEnabled"), "true")
	if err := p.store.SetDkimEnabled(identity, enabled); err != nil && !errors.Is(err, errNotFound) {
		return nil, err
	}
	type resp struct {
		XMLName xml.Name `xml:"SetIdentityDkimEnabledResponse"`
		Result  struct{} `xml:"SetIdentityDkimEnabledResult"`
	}
	return sesXML(http.StatusOK, resp{})
}

func (p *Provider) handleListVerifiedEmailAddresses(form url.Values) (*plugin.Response, error) {
	ids, err := p.store.ListIdentities("EmailAddress")
	if err != nil {
		return nil, err
	}
	members := make([]string, 0, len(ids))
	for _, id := range ids {
		members = append(members, id.Identity)
	}
	type result struct {
		VerifiedEmailAddresses struct {
			Members []string `xml:"member"`
		} `xml:"VerifiedEmailAddresses"`
	}
	type resp struct {
		XMLName xml.Name `xml:"ListVerifiedEmailAddressesResponse"`
		Result  result   `xml:"ListVerifiedEmailAddressesResult"`
	}
	r := resp{}
	r.Result.VerifiedEmailAddresses.Members = members
	return sesXML(http.StatusOK, r)
}

func (p *Provider) handleDeleteVerifiedEmailAddress(form url.Values) (*plugin.Response, error) {
	addr := form.Get("EmailAddress")
	if err := p.store.DeleteIdentity(addr); err != nil && !errors.Is(err, errNotFound) {
		return nil, err
	}
	type resp struct {
		XMLName xml.Name `xml:"DeleteVerifiedEmailAddressResponse"`
	}
	return sesXML(http.StatusOK, resp{})
}

// --- Template handlers ---

func (p *Provider) handleCreateTemplate(form url.Values) (*plugin.Response, error) {
	name := form.Get("Template.TemplateName")
	if name == "" {
		return sesError("MissingParameter", "Template.TemplateName is required", http.StatusBadRequest), nil
	}
	subject := form.Get("Template.SubjectPart")
	htmlBody := form.Get("Template.HtmlPart")
	textBody := form.Get("Template.TextPart")
	if err := p.store.CreateTemplate(name, subject, htmlBody, textBody); err != nil {
		if strings.Contains(err.Error(), "UNIQUE constraint failed") {
			return sesError("AlreadyExists", "template already exists: "+name, http.StatusConflict), nil
		}
		return nil, err
	}
	type resp struct {
		XMLName xml.Name `xml:"CreateTemplateResponse"`
		Result  struct{} `xml:"CreateTemplateResult"`
	}
	return sesXML(http.StatusOK, resp{})
}

func (p *Provider) handleGetTemplate(form url.Values) (*plugin.Response, error) {
	name := form.Get("TemplateName")
	if name == "" {
		return sesError("MissingParameter", "TemplateName is required", http.StatusBadRequest), nil
	}
	t, err := p.store.GetTemplate(name)
	if errors.Is(err, errNotFound) {
		return sesError("TemplateDoesNotExist", "template not found: "+name, http.StatusNotFound), nil
	}
	if err != nil {
		return nil, err
	}
	type templateXML struct {
		TemplateName string `xml:"TemplateName"`
		SubjectPart  string `xml:"SubjectPart"`
		HtmlPart     string `xml:"HtmlPart"`
		TextPart     string `xml:"TextPart"`
	}
	type result struct {
		Template templateXML `xml:"Template"`
	}
	type resp struct {
		XMLName xml.Name `xml:"GetTemplateResponse"`
		Result  result   `xml:"GetTemplateResult"`
	}
	return sesXML(http.StatusOK, resp{Result: result{Template: templateXML{
		TemplateName: t.Name,
		SubjectPart:  t.Subject,
		HtmlPart:     t.HTMLBody,
		TextPart:     t.TextBody,
	}}})
}

func (p *Provider) handleListTemplates(form url.Values) (*plugin.Response, error) {
	templates, err := p.store.ListTemplates()
	if err != nil {
		return nil, err
	}
	type templateMeta struct {
		Name      string `xml:"Name"`
		CreatedAt string `xml:"CreatedTimestamp,omitempty"`
	}
	type result struct {
		TemplatesMetadata struct {
			Members []templateMeta `xml:"member"`
		} `xml:"TemplatesMetadata"`
	}
	type resp struct {
		XMLName xml.Name `xml:"ListTemplatesResponse"`
		Result  result   `xml:"ListTemplatesResult"`
	}
	r := resp{}
	for _, t := range templates {
		r.Result.TemplatesMetadata.Members = append(r.Result.TemplatesMetadata.Members,
			templateMeta{Name: t.Name, CreatedAt: t.CreatedAt.Format("2006-01-02T15:04:05Z")})
	}
	return sesXML(http.StatusOK, r)
}

func (p *Provider) handleUpdateTemplate(form url.Values) (*plugin.Response, error) {
	name := form.Get("Template.TemplateName")
	if name == "" {
		return sesError("MissingParameter", "Template.TemplateName is required", http.StatusBadRequest), nil
	}
	subject := form.Get("Template.SubjectPart")
	htmlBody := form.Get("Template.HtmlPart")
	textBody := form.Get("Template.TextPart")
	if err := p.store.UpdateTemplate(name, subject, htmlBody, textBody); errors.Is(err, errNotFound) {
		return sesError("TemplateDoesNotExist", "template not found: "+name, http.StatusNotFound), nil
	} else if err != nil {
		return nil, err
	}
	type resp struct {
		XMLName xml.Name `xml:"UpdateTemplateResponse"`
		Result  struct{} `xml:"UpdateTemplateResult"`
	}
	return sesXML(http.StatusOK, resp{})
}

func (p *Provider) handleDeleteTemplate(form url.Values) (*plugin.Response, error) {
	name := form.Get("TemplateName")
	if err := p.store.DeleteTemplate(name); err != nil && !errors.Is(err, errNotFound) {
		return nil, err
	}
	type resp struct {
		XMLName xml.Name `xml:"DeleteTemplateResponse"`
		Result  struct{} `xml:"DeleteTemplateResult"`
	}
	return sesXML(http.StatusOK, resp{})
}

func (p *Provider) handleTestRenderTemplate(form url.Values) (*plugin.Response, error) {
	name := form.Get("TemplateName")
	t, err := p.store.GetTemplate(name)
	if errors.Is(err, errNotFound) {
		return sesError("TemplateDoesNotExist", "template not found: "+name, http.StatusNotFound), nil
	}
	if err != nil {
		return nil, err
	}
	type result struct {
		RenderedTemplate string `xml:"RenderedTemplate"`
	}
	type resp struct {
		XMLName xml.Name `xml:"TestRenderTemplateResponse"`
		Result  result   `xml:"TestRenderTemplateResult"`
	}
	return sesXML(http.StatusOK, resp{Result: result{RenderedTemplate: t.Subject + "\n" + t.HTMLBody}})
}

// --- ConfigurationSet handlers ---

func (p *Provider) handleCreateConfigurationSet(form url.Values) (*plugin.Response, error) {
	name := form.Get("ConfigurationSet.Name")
	if name == "" {
		return sesError("MissingParameter", "ConfigurationSet.Name is required", http.StatusBadRequest), nil
	}
	if err := p.store.CreateConfigSet(name); err != nil {
		if strings.Contains(err.Error(), "UNIQUE constraint failed") {
			return sesError("ConfigurationSetAlreadyExists", "configuration set already exists: "+name, http.StatusConflict), nil
		}
		return nil, err
	}
	type resp struct {
		XMLName xml.Name `xml:"CreateConfigurationSetResponse"`
		Result  struct{} `xml:"CreateConfigurationSetResult"`
	}
	return sesXML(http.StatusOK, resp{})
}

func (p *Provider) handleDescribeConfigurationSet(form url.Values) (*plugin.Response, error) {
	name := form.Get("ConfigurationSetName")
	cs, err := p.store.GetConfigSet(name)
	if errors.Is(err, errNotFound) {
		return sesError("ConfigurationSetDoesNotExist", "configuration set not found: "+name, http.StatusNotFound), nil
	}
	if err != nil {
		return nil, err
	}
	type configSetXML struct {
		Name string `xml:"Name"`
	}
	type result struct {
		ConfigurationSet configSetXML `xml:"ConfigurationSet"`
	}
	type resp struct {
		XMLName xml.Name `xml:"DescribeConfigurationSetResponse"`
		Result  result   `xml:"DescribeConfigurationSetResult"`
	}
	return sesXML(http.StatusOK, resp{Result: result{ConfigurationSet: configSetXML{Name: cs.Name}}})
}

func (p *Provider) handleListConfigurationSets(form url.Values) (*plugin.Response, error) {
	sets, err := p.store.ListConfigSets()
	if err != nil {
		return nil, err
	}
	type csItem struct {
		Name string `xml:"Name"`
	}
	type result struct {
		ConfigurationSets struct {
			Members []csItem `xml:"member"`
		} `xml:"ConfigurationSets"`
	}
	type resp struct {
		XMLName xml.Name `xml:"ListConfigurationSetsResponse"`
		Result  result   `xml:"ListConfigurationSetsResult"`
	}
	r := resp{}
	for _, cs := range sets {
		r.Result.ConfigurationSets.Members = append(r.Result.ConfigurationSets.Members, csItem{Name: cs.Name})
	}
	return sesXML(http.StatusOK, r)
}

func (p *Provider) handleDeleteConfigurationSet(form url.Values) (*plugin.Response, error) {
	name := form.Get("ConfigurationSetName")
	if err := p.store.DeleteConfigSet(name); err != nil && !errors.Is(err, errNotFound) {
		return nil, err
	}
	type resp struct {
		XMLName xml.Name `xml:"DeleteConfigurationSetResponse"`
		Result  struct{} `xml:"DeleteConfigurationSetResult"`
	}
	return sesXML(http.StatusOK, resp{})
}

// --- ReceiptRuleSet handlers ---

func (p *Provider) handleCreateReceiptRuleSet(form url.Values) (*plugin.Response, error) {
	name := form.Get("RuleSetName")
	if name == "" {
		return sesError("MissingParameter", "RuleSetName is required", http.StatusBadRequest), nil
	}
	if err := p.store.CreateReceiptRuleSet(name); err != nil {
		if strings.Contains(err.Error(), "UNIQUE constraint failed") {
			return sesError("AlreadyExists", "receipt rule set already exists: "+name, http.StatusConflict), nil
		}
		return nil, err
	}
	type resp struct {
		XMLName xml.Name `xml:"CreateReceiptRuleSetResponse"`
		Result  struct{} `xml:"CreateReceiptRuleSetResult"`
	}
	return sesXML(http.StatusOK, resp{})
}

func (p *Provider) handleDescribeReceiptRuleSet(form url.Values) (*plugin.Response, error) {
	name := form.Get("RuleSetName")
	rs, err := p.store.GetReceiptRuleSet(name)
	if errors.Is(err, errNotFound) {
		return sesError("RuleSetDoesNotExist", "receipt rule set not found: "+name, http.StatusNotFound), nil
	}
	if err != nil {
		return nil, err
	}
	rules, err := p.store.ListReceiptRules(name)
	if err != nil {
		return nil, err
	}
	type ruleXML struct {
		Name    string `xml:"Name"`
		Enabled bool   `xml:"Enabled"`
	}
	type metadata struct {
		Name string `xml:"Name"`
	}
	type result struct {
		Metadata metadata  `xml:"Metadata"`
		Rules    []ruleXML `xml:"Rules>member"`
	}
	type resp struct {
		XMLName xml.Name `xml:"DescribeReceiptRuleSetResponse"`
		Result  result   `xml:"DescribeReceiptRuleSetResult"`
	}
	r := resp{}
	r.Result.Metadata.Name = rs.Name
	for _, rule := range rules {
		r.Result.Rules = append(r.Result.Rules, ruleXML{Name: rule.Name, Enabled: rule.Enabled})
	}
	return sesXML(http.StatusOK, r)
}

func (p *Provider) handleListReceiptRuleSets(form url.Values) (*plugin.Response, error) {
	sets, err := p.store.ListReceiptRuleSets()
	if err != nil {
		return nil, err
	}
	type rsItem struct {
		Name string `xml:"Name"`
	}
	type result struct {
		RuleSets struct {
			Members []rsItem `xml:"member"`
		} `xml:"RuleSets"`
	}
	type resp struct {
		XMLName xml.Name `xml:"ListReceiptRuleSetsResponse"`
		Result  result   `xml:"ListReceiptRuleSetsResult"`
	}
	r := resp{}
	for _, rs := range sets {
		r.Result.RuleSets.Members = append(r.Result.RuleSets.Members, rsItem{Name: rs.Name})
	}
	return sesXML(http.StatusOK, r)
}

func (p *Provider) handleDeleteReceiptRuleSet(form url.Values) (*plugin.Response, error) {
	name := form.Get("RuleSetName")
	if err := p.store.DeleteReceiptRuleSet(name); err != nil && !errors.Is(err, errNotFound) {
		return nil, err
	}
	type resp struct {
		XMLName xml.Name `xml:"DeleteReceiptRuleSetResponse"`
		Result  struct{} `xml:"DeleteReceiptRuleSetResult"`
	}
	return sesXML(http.StatusOK, resp{})
}

func (p *Provider) handleSetActiveReceiptRuleSet(form url.Values) (*plugin.Response, error) {
	name := form.Get("RuleSetName")
	if err := p.store.SetActiveReceiptRuleSet(name); err != nil && !errors.Is(err, errNotFound) {
		return nil, err
	}
	type resp struct {
		XMLName xml.Name `xml:"SetActiveReceiptRuleSetResponse"`
		Result  struct{} `xml:"SetActiveReceiptRuleSetResult"`
	}
	return sesXML(http.StatusOK, resp{})
}

func (p *Provider) handleDescribeActiveReceiptRuleSet(form url.Values) (*plugin.Response, error) {
	rs, err := p.store.GetActiveReceiptRuleSet()
	type result struct {
		Metadata *struct {
			Name string `xml:"Name"`
		} `xml:"Metadata,omitempty"`
	}
	type resp struct {
		XMLName xml.Name `xml:"DescribeActiveReceiptRuleSetResponse"`
		Result  result   `xml:"DescribeActiveReceiptRuleSetResult"`
	}
	if errors.Is(err, errNotFound) {
		return sesXML(http.StatusOK, resp{})
	}
	if err != nil {
		return nil, err
	}
	r := resp{}
	r.Result.Metadata = &struct {
		Name string `xml:"Name"`
	}{Name: rs.Name}
	return sesXML(http.StatusOK, r)
}

func (p *Provider) handleCloneReceiptRuleSet(form url.Values) (*plugin.Response, error) {
	originalName := form.Get("OriginalRuleSetName")
	newName := form.Get("RuleSetName")
	if originalName == "" || newName == "" {
		return sesError("MissingParameter", "OriginalRuleSetName and RuleSetName are required", http.StatusBadRequest), nil
	}
	if err := p.store.CloneReceiptRuleSet(originalName, newName); err != nil {
		if strings.Contains(err.Error(), "UNIQUE constraint failed") {
			return sesError("AlreadyExists", "receipt rule set already exists: "+newName, http.StatusConflict), nil
		}
		return nil, err
	}
	type resp struct {
		XMLName xml.Name `xml:"CloneReceiptRuleSetResponse"`
		Result  struct{} `xml:"CloneReceiptRuleSetResult"`
	}
	return sesXML(http.StatusOK, resp{})
}

func (p *Provider) handleReorderReceiptRuleSet(form url.Values) (*plugin.Response, error) {
	name := form.Get("RuleSetName")
	rules := collectMembers(form, "RuleNames")
	if err := p.store.ReorderReceiptRuleSet(name, rules); err != nil {
		return nil, err
	}
	type resp struct {
		XMLName xml.Name `xml:"ReorderReceiptRuleSetResponse"`
		Result  struct{} `xml:"ReorderReceiptRuleSetResult"`
	}
	return sesXML(http.StatusOK, resp{})
}

// --- ReceiptRule handlers ---

func (p *Provider) handleCreateReceiptRule(form url.Values) (*plugin.Response, error) {
	ruleSetName := form.Get("RuleSetName")
	ruleName := form.Get("Rule.Name")
	if ruleSetName == "" || ruleName == "" {
		return sesError("MissingParameter", "RuleSetName and Rule.Name are required", http.StatusBadRequest), nil
	}
	enabled := !strings.EqualFold(form.Get("Rule.Enabled"), "false")
	scanEnabled := strings.EqualFold(form.Get("Rule.ScanEnabled"), "true")
	recipients := collectMembers(form, "Rule.Recipients")
	after := form.Get("After")
	if err := p.store.CreateReceiptRule(ruleSetName, ruleName, enabled, scanEnabled, recipients, nil, after); err != nil {
		if strings.Contains(err.Error(), "UNIQUE constraint failed") {
			return sesError("AlreadyExists", "receipt rule already exists: "+ruleName, http.StatusConflict), nil
		}
		return nil, err
	}
	type resp struct {
		XMLName xml.Name `xml:"CreateReceiptRuleResponse"`
		Result  struct{} `xml:"CreateReceiptRuleResult"`
	}
	return sesXML(http.StatusOK, resp{})
}

func (p *Provider) handleDescribeReceiptRule(form url.Values) (*plugin.Response, error) {
	ruleSetName := form.Get("RuleSetName")
	ruleName := form.Get("RuleName")
	rr, err := p.store.GetReceiptRule(ruleSetName, ruleName)
	if errors.Is(err, errNotFound) {
		return sesError("RuleDoesNotExist", "receipt rule not found: "+ruleName, http.StatusNotFound), nil
	}
	if err != nil {
		return nil, err
	}
	type ruleXML struct {
		Name        string   `xml:"Name"`
		Enabled     bool     `xml:"Enabled"`
		ScanEnabled bool     `xml:"ScanEnabled"`
		Recipients  []string `xml:"Recipients>member"`
	}
	type result struct {
		Rule ruleXML `xml:"Rule"`
	}
	type resp struct {
		XMLName xml.Name `xml:"DescribeReceiptRuleResponse"`
		Result  result   `xml:"DescribeReceiptRuleResult"`
	}
	return sesXML(http.StatusOK, resp{Result: result{Rule: ruleXML{
		Name:        rr.Name,
		Enabled:     rr.Enabled,
		ScanEnabled: rr.ScanEnabled,
		Recipients:  rr.Recipients,
	}}})
}

func (p *Provider) handleUpdateReceiptRule(form url.Values) (*plugin.Response, error) {
	ruleSetName := form.Get("RuleSetName")
	ruleName := form.Get("Rule.Name")
	enabled := !strings.EqualFold(form.Get("Rule.Enabled"), "false")
	scanEnabled := strings.EqualFold(form.Get("Rule.ScanEnabled"), "true")
	recipients := collectMembers(form, "Rule.Recipients")
	if err := p.store.UpdateReceiptRule(ruleSetName, ruleName, enabled, scanEnabled, recipients, nil); errors.Is(err, errNotFound) {
		return sesError("RuleDoesNotExist", "receipt rule not found: "+ruleName, http.StatusNotFound), nil
	} else if err != nil {
		return nil, err
	}
	type resp struct {
		XMLName xml.Name `xml:"UpdateReceiptRuleResponse"`
		Result  struct{} `xml:"UpdateReceiptRuleResult"`
	}
	return sesXML(http.StatusOK, resp{})
}

func (p *Provider) handleDeleteReceiptRule(form url.Values) (*plugin.Response, error) {
	ruleSetName := form.Get("RuleSetName")
	ruleName := form.Get("RuleName")
	if err := p.store.DeleteReceiptRule(ruleSetName, ruleName); err != nil && !errors.Is(err, errNotFound) {
		return nil, err
	}
	type resp struct {
		XMLName xml.Name `xml:"DeleteReceiptRuleResponse"`
		Result  struct{} `xml:"DeleteReceiptRuleResult"`
	}
	return sesXML(http.StatusOK, resp{})
}

func (p *Provider) handleSetReceiptRulePosition(form url.Values) (*plugin.Response, error) {
	ruleSetName := form.Get("RuleSetName")
	ruleName := form.Get("RuleName")
	pos := 0
	_, _ = fmt.Sscanf(form.Get("Position"), "%d", &pos)
	if err := p.store.SetReceiptRulePosition(ruleSetName, ruleName, pos); err != nil && !errors.Is(err, errNotFound) {
		return nil, err
	}
	type resp struct {
		XMLName xml.Name `xml:"SetReceiptRulePositionResponse"`
		Result  struct{} `xml:"SetReceiptRulePositionResult"`
	}
	return sesXML(http.StatusOK, resp{})
}

// --- ReceiptFilter handlers ---

func (p *Provider) handleCreateReceiptFilter(form url.Values) (*plugin.Response, error) {
	name := form.Get("Filter.Name")
	policy := form.Get("Filter.IpFilter.Policy")
	cidr := form.Get("Filter.IpFilter.Cidr")
	if name == "" {
		return sesError("MissingParameter", "Filter.Name is required", http.StatusBadRequest), nil
	}
	if policy == "" {
		policy = "Block"
	}
	if cidr == "" {
		cidr = "0.0.0.0/0"
	}
	if err := p.store.CreateReceiptFilter(name, policy, cidr); err != nil {
		if strings.Contains(err.Error(), "UNIQUE constraint failed") {
			return sesError("AlreadyExists", "receipt filter already exists: "+name, http.StatusConflict), nil
		}
		return nil, err
	}
	type resp struct {
		XMLName xml.Name `xml:"CreateReceiptFilterResponse"`
		Result  struct{} `xml:"CreateReceiptFilterResult"`
	}
	return sesXML(http.StatusOK, resp{})
}

func (p *Provider) handleListReceiptFilters(form url.Values) (*plugin.Response, error) {
	filters, err := p.store.ListReceiptFilters()
	if err != nil {
		return nil, err
	}
	type ipFilter struct {
		Policy string `xml:"Policy"`
		Cidr   string `xml:"Cidr"`
	}
	type filterXML struct {
		Name     string   `xml:"Name"`
		IpFilter ipFilter `xml:"IpFilter"`
	}
	type result struct {
		Filters struct {
			Members []filterXML `xml:"member"`
		} `xml:"Filters"`
	}
	type resp struct {
		XMLName xml.Name `xml:"ListReceiptFiltersResponse"`
		Result  result   `xml:"ListReceiptFiltersResult"`
	}
	r := resp{}
	for _, f := range filters {
		r.Result.Filters.Members = append(r.Result.Filters.Members, filterXML{
			Name:     f.Name,
			IpFilter: ipFilter{Policy: f.Policy, Cidr: f.CIDR},
		})
	}
	return sesXML(http.StatusOK, r)
}

func (p *Provider) handleDeleteReceiptFilter(form url.Values) (*plugin.Response, error) {
	name := form.Get("FilterName")
	if err := p.store.DeleteReceiptFilter(name); err != nil && !errors.Is(err, errNotFound) {
		return nil, err
	}
	type resp struct {
		XMLName xml.Name `xml:"DeleteReceiptFilterResponse"`
		Result  struct{} `xml:"DeleteReceiptFilterResult"`
	}
	return sesXML(http.StatusOK, resp{})
}

// --- Send handlers ---

func (p *Provider) handleSendEmail(form url.Values) (*plugin.Response, error) {
	source := form.Get("Source")
	if source == "" {
		return sesError("MissingParameter", "Source is required", http.StatusBadRequest), nil
	}
	subject := form.Get("Message.Subject.Data")
	bodyHTML := form.Get("Message.Body.Html.Data")
	bodyText := form.Get("Message.Body.Text.Data")
	body := bodyHTML
	if body == "" {
		body = bodyText
	}

	destination := map[string][]string{
		"ToAddresses":  collectMembers(form, "Destination.ToAddresses"),
		"CcAddresses":  collectMembers(form, "Destination.CcAddresses"),
		"BccAddresses": collectMembers(form, "Destination.BccAddresses"),
	}

	msgID := "ses-" + shared.GenerateID("", 20)
	if err := p.store.StoreSentEmail(msgID, source, destination, subject, body); err != nil {
		return nil, err
	}
	type result struct {
		MessageId string `xml:"MessageId"`
	}
	type resp struct {
		XMLName xml.Name `xml:"SendEmailResponse"`
		Result  result   `xml:"SendEmailResult"`
	}
	return sesXML(http.StatusOK, resp{Result: result{MessageId: msgID}})
}

func (p *Provider) handleSendRawEmail(form url.Values) (*plugin.Response, error) {
	source := form.Get("Source")
	msgID := "ses-raw-" + shared.GenerateID("", 16)
	destination := map[string][]string{}
	if err := p.store.StoreSentEmail(msgID, source, destination, "raw", form.Get("RawMessage.Data")); err != nil {
		return nil, err
	}
	type result struct {
		MessageId string `xml:"MessageId"`
	}
	type resp struct {
		XMLName xml.Name `xml:"SendRawEmailResponse"`
		Result  result   `xml:"SendRawEmailResult"`
	}
	return sesXML(http.StatusOK, resp{Result: result{MessageId: msgID}})
}

func (p *Provider) handleSendTemplatedEmail(form url.Values) (*plugin.Response, error) {
	source := form.Get("Source")
	templateName := form.Get("Template")
	msgID := "ses-tmpl-" + shared.GenerateID("", 16)
	destination := map[string][]string{
		"ToAddresses": collectMembers(form, "Destination.ToAddresses"),
	}
	if err := p.store.StoreSentEmail(msgID, source, destination, "templated:"+templateName, ""); err != nil {
		return nil, err
	}
	type result struct {
		MessageId string `xml:"MessageId"`
	}
	type resp struct {
		XMLName xml.Name `xml:"SendTemplatedEmailResponse"`
		Result  result   `xml:"SendTemplatedEmailResult"`
	}
	return sesXML(http.StatusOK, resp{Result: result{MessageId: msgID}})
}

func (p *Provider) handleSendBulkTemplatedEmail(form url.Values) (*plugin.Response, error) {
	source := form.Get("Source")
	templateName := form.Get("Template")
	msgID := "ses-bulk-" + shared.GenerateID("", 16)
	destination := map[string][]string{}
	if err := p.store.StoreSentEmail(msgID, source, destination, "bulk:"+templateName, ""); err != nil {
		return nil, err
	}
	type statusEntry struct {
		MessageId string `xml:"MessageId"`
		Status    string `xml:"Status"`
	}
	type result struct {
		Status struct {
			Members []statusEntry `xml:"member"`
		} `xml:"Status"`
	}
	type resp struct {
		XMLName xml.Name `xml:"SendBulkTemplatedEmailResponse"`
		Result  result   `xml:"SendBulkTemplatedEmailResult"`
	}
	r := resp{}
	r.Result.Status.Members = []statusEntry{{MessageId: msgID, Status: "Success"}}
	return sesXML(http.StatusOK, r)
}

func (p *Provider) handleSendCustomVerificationEmail(form url.Values) (*plugin.Response, error) {
	emailAddress := form.Get("EmailAddress")
	msgID := "ses-cv-" + shared.GenerateID("", 16)
	destination := map[string][]string{"ToAddresses": {emailAddress}}
	if err := p.store.StoreSentEmail(msgID, "noreply@devcloud", destination, "custom-verification", ""); err != nil {
		return nil, err
	}
	type result struct {
		MessageId string `xml:"MessageId"`
	}
	type resp struct {
		XMLName xml.Name `xml:"SendCustomVerificationEmailResponse"`
		Result  result   `xml:"SendCustomVerificationEmailResult"`
	}
	return sesXML(http.StatusOK, resp{Result: result{MessageId: msgID}})
}

// --- Quota / stats handlers ---

func (p *Provider) handleGetSendQuota(form url.Values) (*plugin.Response, error) {
	type result struct {
		Max24HourSend   float64 `xml:"Max24HourSend"`
		MaxSendRate     float64 `xml:"MaxSendRate"`
		SentLast24Hours float64 `xml:"SentLast24Hours"`
	}
	type resp struct {
		XMLName xml.Name `xml:"GetSendQuotaResponse"`
		Result  result   `xml:"GetSendQuotaResult"`
	}
	return sesXML(http.StatusOK, resp{Result: result{
		Max24HourSend:   50000,
		MaxSendRate:     14,
		SentLast24Hours: 0,
	}})
}

func (p *Provider) handleGetSendStatistics(form url.Values) (*plugin.Response, error) {
	type result struct {
		SendDataPoints struct{} `xml:"SendDataPoints"`
	}
	type resp struct {
		XMLName xml.Name `xml:"GetSendStatisticsResponse"`
		Result  result   `xml:"GetSendStatisticsResult"`
	}
	return sesXML(http.StatusOK, resp{})
}

func (p *Provider) handleGetAccountSendingEnabled(form url.Values) (*plugin.Response, error) {
	type result struct {
		Enabled bool `xml:"Enabled"`
	}
	type resp struct {
		XMLName xml.Name `xml:"GetAccountSendingEnabledResponse"`
		Result  result   `xml:"GetAccountSendingEnabledResult"`
	}
	return sesXML(http.StatusOK, resp{Result: result{Enabled: true}})
}

func (p *Provider) handleUpdateAccountSendingEnabled(form url.Values) (*plugin.Response, error) {
	type resp struct {
		XMLName xml.Name `xml:"UpdateAccountSendingEnabledResponse"`
	}
	return sesXML(http.StatusOK, resp{})
}

// --- Misc success handler ---

func (p *Provider) handleSuccessEmpty(action string) (*plugin.Response, error) {
	type resp struct {
		XMLName xml.Name
	}
	r := resp{XMLName: xml.Name{Local: action + "Response"}}
	return sesXML(http.StatusOK, r)
}
