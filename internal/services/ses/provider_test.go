// SPDX-License-Identifier: Apache-2.0

package ses

import (
	"context"
	"encoding/xml"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/skyoo2003/devcloud/internal/plugin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestProvider(t *testing.T) *Provider {
	t.Helper()
	p := &Provider{}
	require.NoError(t, p.Init(plugin.PluginConfig{DataDir: t.TempDir()}))
	t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
	return p
}

func callQuery(t *testing.T, p *Provider, action string, params map[string]string) *plugin.Response {
	t.Helper()
	form := url.Values{}
	form.Set("Action", action)
	for k, v := range params {
		form.Set(k, v)
	}
	req := httptest.NewRequest("POST", "/", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := p.HandleRequest(context.Background(), "", req)
	require.NoError(t, err)
	return resp
}

func TestIdentityVerification(t *testing.T) {
	p := newTestProvider(t)

	// Verify email identity
	resp := callQuery(t, p, "VerifyEmailIdentity", map[string]string{"EmailAddress": "test@example.com"})
	assert.Equal(t, 200, resp.StatusCode, string(resp.Body))

	// Verify domain identity
	resp = callQuery(t, p, "VerifyDomainIdentity", map[string]string{"Domain": "example.com"})
	assert.Equal(t, 200, resp.StatusCode, string(resp.Body))
	type domainResp struct {
		VerificationToken string `xml:"VerifyDomainIdentityResult>VerificationToken"`
	}
	var dr domainResp
	require.NoError(t, xml.Unmarshal(resp.Body, &dr))
	assert.NotEmpty(t, dr.VerificationToken)

	// List identities
	resp = callQuery(t, p, "ListIdentities", map[string]string{})
	assert.Equal(t, 200, resp.StatusCode, string(resp.Body))
	type listResp struct {
		Identities []string `xml:"ListIdentitiesResult>Identities>member"`
	}
	var lr listResp
	require.NoError(t, xml.Unmarshal(resp.Body, &lr))
	assert.Len(t, lr.Identities, 2)

	// Get verification attributes
	resp = callQuery(t, p, "GetIdentityVerificationAttributes", map[string]string{
		"Identities.member.1": "test@example.com",
	})
	assert.Equal(t, 200, resp.StatusCode, string(resp.Body))
	type attrResp struct {
		Entries []struct {
			Key   string `xml:"key"`
			Value struct {
				VerificationStatus string `xml:"VerificationStatus"`
			} `xml:"value"`
		} `xml:"GetIdentityVerificationAttributesResult>VerificationAttributes>entry"`
	}
	var ar attrResp
	require.NoError(t, xml.Unmarshal(resp.Body, &ar))
	require.Len(t, ar.Entries, 1)
	assert.Equal(t, "Success", ar.Entries[0].Value.VerificationStatus)

	// Delete identity
	resp = callQuery(t, p, "DeleteIdentity", map[string]string{"Identity": "test@example.com"})
	assert.Equal(t, 200, resp.StatusCode, string(resp.Body))

	// Verify DKIM on domain
	resp = callQuery(t, p, "VerifyDomainDkim", map[string]string{"Domain": "example.com"})
	assert.Equal(t, 200, resp.StatusCode, string(resp.Body))
	type dkimResp struct {
		DkimTokens []string `xml:"VerifyDomainDkimResult>DkimTokens>member"`
	}
	var dk dkimResp
	require.NoError(t, xml.Unmarshal(resp.Body, &dk))
	assert.Len(t, dk.DkimTokens, 3)
}

func TestTemplateCRUD(t *testing.T) {
	p := newTestProvider(t)

	// Create template
	resp := callQuery(t, p, "CreateTemplate", map[string]string{
		"Template.TemplateName": "welcome",
		"Template.SubjectPart":  "Welcome!",
		"Template.HtmlPart":     "<h1>Hello</h1>",
		"Template.TextPart":     "Hello",
	})
	assert.Equal(t, 200, resp.StatusCode, string(resp.Body))

	// Get template
	resp = callQuery(t, p, "GetTemplate", map[string]string{"TemplateName": "welcome"})
	assert.Equal(t, 200, resp.StatusCode, string(resp.Body))
	type getResp struct {
		Name    string `xml:"GetTemplateResult>Template>TemplateName"`
		Subject string `xml:"GetTemplateResult>Template>SubjectPart"`
	}
	var gr getResp
	require.NoError(t, xml.Unmarshal(resp.Body, &gr))
	assert.Equal(t, "welcome", gr.Name)
	assert.Equal(t, "Welcome!", gr.Subject)

	// List templates
	resp = callQuery(t, p, "ListTemplates", map[string]string{})
	assert.Equal(t, 200, resp.StatusCode, string(resp.Body))
	type listResp struct {
		Names []string `xml:"ListTemplatesResult>TemplatesMetadata>member>Name"`
	}
	var lr listResp
	require.NoError(t, xml.Unmarshal(resp.Body, &lr))
	assert.Contains(t, lr.Names, "welcome")

	// Update template
	resp = callQuery(t, p, "UpdateTemplate", map[string]string{
		"Template.TemplateName": "welcome",
		"Template.SubjectPart":  "Updated Subject",
		"Template.HtmlPart":     "<h1>Updated</h1>",
	})
	assert.Equal(t, 200, resp.StatusCode, string(resp.Body))

	// Verify update
	resp = callQuery(t, p, "GetTemplate", map[string]string{"TemplateName": "welcome"})
	var gr2 getResp
	require.NoError(t, xml.Unmarshal(resp.Body, &gr2))
	assert.Equal(t, "Updated Subject", gr2.Subject)

	// Delete template
	resp = callQuery(t, p, "DeleteTemplate", map[string]string{"TemplateName": "welcome"})
	assert.Equal(t, 200, resp.StatusCode, string(resp.Body))

	// Confirm deletion
	resp = callQuery(t, p, "GetTemplate", map[string]string{"TemplateName": "welcome"})
	assert.Equal(t, 404, resp.StatusCode)
}

func TestConfigSetCRUD(t *testing.T) {
	p := newTestProvider(t)

	// Create config set
	resp := callQuery(t, p, "CreateConfigurationSet", map[string]string{
		"ConfigurationSet.Name": "my-config-set",
	})
	assert.Equal(t, 200, resp.StatusCode, string(resp.Body))

	// Describe config set
	resp = callQuery(t, p, "DescribeConfigurationSet", map[string]string{
		"ConfigurationSetName": "my-config-set",
	})
	assert.Equal(t, 200, resp.StatusCode, string(resp.Body))
	type descResp struct {
		Name string `xml:"DescribeConfigurationSetResult>ConfigurationSet>Name"`
	}
	var dr descResp
	require.NoError(t, xml.Unmarshal(resp.Body, &dr))
	assert.Equal(t, "my-config-set", dr.Name)

	// List config sets
	resp = callQuery(t, p, "ListConfigurationSets", map[string]string{})
	assert.Equal(t, 200, resp.StatusCode, string(resp.Body))
	type listResp struct {
		Names []string `xml:"ListConfigurationSetsResult>ConfigurationSets>member>Name"`
	}
	var lr listResp
	require.NoError(t, xml.Unmarshal(resp.Body, &lr))
	assert.Contains(t, lr.Names, "my-config-set")

	// Delete config set
	resp = callQuery(t, p, "DeleteConfigurationSet", map[string]string{
		"ConfigurationSetName": "my-config-set",
	})
	assert.Equal(t, 200, resp.StatusCode, string(resp.Body))

	// Confirm deletion
	resp = callQuery(t, p, "DescribeConfigurationSet", map[string]string{
		"ConfigurationSetName": "my-config-set",
	})
	assert.Equal(t, 404, resp.StatusCode)
}

func TestReceiptRuleSetAndRule(t *testing.T) {
	p := newTestProvider(t)

	// Create rule set
	resp := callQuery(t, p, "CreateReceiptRuleSet", map[string]string{"RuleSetName": "my-rules"})
	assert.Equal(t, 200, resp.StatusCode, string(resp.Body))

	// Create receipt rule
	resp = callQuery(t, p, "CreateReceiptRule", map[string]string{
		"RuleSetName":              "my-rules",
		"Rule.Name":                "rule1",
		"Rule.Enabled":             "true",
		"Rule.Recipients.member.1": "user@example.com",
	})
	assert.Equal(t, 200, resp.StatusCode, string(resp.Body))

	// Describe rule set
	resp = callQuery(t, p, "DescribeReceiptRuleSet", map[string]string{"RuleSetName": "my-rules"})
	assert.Equal(t, 200, resp.StatusCode, string(resp.Body))
	type descResp struct {
		Name  string   `xml:"DescribeReceiptRuleSetResult>Metadata>Name"`
		Rules []string `xml:"DescribeReceiptRuleSetResult>Rules>member>Name"`
	}
	var dr descResp
	require.NoError(t, xml.Unmarshal(resp.Body, &dr))
	assert.Equal(t, "my-rules", dr.Name)
	assert.Contains(t, dr.Rules, "rule1")

	// Describe rule
	resp = callQuery(t, p, "DescribeReceiptRule", map[string]string{
		"RuleSetName": "my-rules",
		"RuleName":    "rule1",
	})
	assert.Equal(t, 200, resp.StatusCode, string(resp.Body))
	type ruleResp struct {
		Name       string   `xml:"DescribeReceiptRuleResult>Rule>Name"`
		Enabled    bool     `xml:"DescribeReceiptRuleResult>Rule>Enabled"`
		Recipients []string `xml:"DescribeReceiptRuleResult>Rule>Recipients>member"`
	}
	var rr ruleResp
	require.NoError(t, xml.Unmarshal(resp.Body, &rr))
	assert.Equal(t, "rule1", rr.Name)
	assert.True(t, rr.Enabled)
	assert.Contains(t, rr.Recipients, "user@example.com")

	// Set active rule set
	resp = callQuery(t, p, "SetActiveReceiptRuleSet", map[string]string{"RuleSetName": "my-rules"})
	assert.Equal(t, 200, resp.StatusCode, string(resp.Body))

	// Describe active rule set
	resp = callQuery(t, p, "DescribeActiveReceiptRuleSet", map[string]string{})
	assert.Equal(t, 200, resp.StatusCode, string(resp.Body))
	type activeResp struct {
		Name string `xml:"DescribeActiveReceiptRuleSetResult>Metadata>Name"`
	}
	var ar activeResp
	require.NoError(t, xml.Unmarshal(resp.Body, &ar))
	assert.Equal(t, "my-rules", ar.Name)

	// Delete rule
	resp = callQuery(t, p, "DeleteReceiptRule", map[string]string{
		"RuleSetName": "my-rules",
		"RuleName":    "rule1",
	})
	assert.Equal(t, 200, resp.StatusCode, string(resp.Body))

	// Delete rule set
	resp = callQuery(t, p, "DeleteReceiptRuleSet", map[string]string{"RuleSetName": "my-rules"})
	assert.Equal(t, 200, resp.StatusCode, string(resp.Body))
}

func TestSendEmail(t *testing.T) {
	p := newTestProvider(t)

	// Verify sender
	callQuery(t, p, "VerifyEmailIdentity", map[string]string{"EmailAddress": "sender@example.com"})

	// Send email
	resp := callQuery(t, p, "SendEmail", map[string]string{
		"Source":                           "sender@example.com",
		"Destination.ToAddresses.member.1": "recipient@example.com",
		"Message.Subject.Data":             "Test Subject",
		"Message.Body.Text.Data":           "Hello, world!",
	})
	assert.Equal(t, 200, resp.StatusCode, string(resp.Body))
	type sendResp struct {
		MessageId string `xml:"SendEmailResult>MessageId"`
	}
	var sr sendResp
	require.NoError(t, xml.Unmarshal(resp.Body, &sr))
	assert.NotEmpty(t, sr.MessageId)
	assert.True(t, strings.HasPrefix(sr.MessageId, "ses-"))

	// Send raw email
	resp = callQuery(t, p, "SendRawEmail", map[string]string{
		"Source":          "sender@example.com",
		"RawMessage.Data": "From: sender@example.com\r\nTo: recipient@example.com\r\n\r\nBody",
	})
	assert.Equal(t, 200, resp.StatusCode, string(resp.Body))
	type rawResp struct {
		MessageId string `xml:"SendRawEmailResult>MessageId"`
	}
	var rawR rawResp
	require.NoError(t, xml.Unmarshal(resp.Body, &rawR))
	assert.NotEmpty(t, rawR.MessageId)

	// Send templated email
	callQuery(t, p, "CreateTemplate", map[string]string{
		"Template.TemplateName": "my-tmpl",
		"Template.SubjectPart":  "Hello",
		"Template.TextPart":     "Hi {{name}}",
	})
	resp = callQuery(t, p, "SendTemplatedEmail", map[string]string{
		"Source":                           "sender@example.com",
		"Template":                         "my-tmpl",
		"Destination.ToAddresses.member.1": "recipient@example.com",
		"TemplateData":                     `{"name":"World"}`,
	})
	assert.Equal(t, 200, resp.StatusCode, string(resp.Body))
	type tmplResp struct {
		MessageId string `xml:"SendTemplatedEmailResult>MessageId"`
	}
	var tr tmplResp
	require.NoError(t, xml.Unmarshal(resp.Body, &tr))
	assert.NotEmpty(t, tr.MessageId)
}

func TestSendQuota(t *testing.T) {
	p := newTestProvider(t)

	// GetSendQuota
	resp := callQuery(t, p, "GetSendQuota", map[string]string{})
	assert.Equal(t, 200, resp.StatusCode, string(resp.Body))
	type quotaResp struct {
		Max24HourSend   float64 `xml:"GetSendQuotaResult>Max24HourSend"`
		MaxSendRate     float64 `xml:"GetSendQuotaResult>MaxSendRate"`
		SentLast24Hours float64 `xml:"GetSendQuotaResult>SentLast24Hours"`
	}
	var qr quotaResp
	require.NoError(t, xml.Unmarshal(resp.Body, &qr))
	assert.Equal(t, float64(50000), qr.Max24HourSend)
	assert.Equal(t, float64(14), qr.MaxSendRate)
	assert.Equal(t, float64(0), qr.SentLast24Hours)

	// GetSendStatistics
	resp = callQuery(t, p, "GetSendStatistics", map[string]string{})
	assert.Equal(t, 200, resp.StatusCode, string(resp.Body))

	// GetAccountSendingEnabled
	resp = callQuery(t, p, "GetAccountSendingEnabled", map[string]string{})
	assert.Equal(t, 200, resp.StatusCode, string(resp.Body))
	type enabledResp struct {
		Enabled bool `xml:"GetAccountSendingEnabledResult>Enabled"`
	}
	var er enabledResp
	require.NoError(t, xml.Unmarshal(resp.Body, &er))
	assert.True(t, er.Enabled)

	// UpdateAccountSendingEnabled
	resp = callQuery(t, p, "UpdateAccountSendingEnabled", map[string]string{"Enabled": "false"})
	assert.Equal(t, 200, resp.StatusCode, string(resp.Body))

	// CreateReceiptFilter
	resp = callQuery(t, p, "CreateReceiptFilter", map[string]string{
		"Filter.Name":            "block-range",
		"Filter.IpFilter.Policy": "Block",
		"Filter.IpFilter.Cidr":   "10.0.0.0/8",
	})
	assert.Equal(t, 200, resp.StatusCode, string(resp.Body))

	// ListReceiptFilters
	resp = callQuery(t, p, "ListReceiptFilters", map[string]string{})
	assert.Equal(t, 200, resp.StatusCode, string(resp.Body))
	type filtersResp struct {
		Names []string `xml:"ListReceiptFiltersResult>Filters>member>Name"`
	}
	var fr filtersResp
	require.NoError(t, xml.Unmarshal(resp.Body, &fr))
	assert.Contains(t, fr.Names, "block-range")
}
