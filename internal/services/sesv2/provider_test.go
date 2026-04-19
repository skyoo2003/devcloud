// SPDX-License-Identifier: Apache-2.0

package sesv2

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
)

func newProvider(t *testing.T) *Provider {
	t.Helper()
	dir, err := os.MkdirTemp("", "sesv2-test-*")
	if err != nil {
		t.Fatalf("create temp dir: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })

	p := &Provider{}
	store, err := NewStore(dir)
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	p.store = store
	t.Cleanup(func() { _ = store.Close() })
	return p
}

func callOp(t *testing.T, p *Provider, op, path, method string, body map[string]any) map[string]any {
	t.Helper()
	var buf bytes.Buffer
	if body != nil {
		if err := json.NewEncoder(&buf).Encode(body); err != nil {
			t.Fatalf("encode body: %v", err)
		}
	}
	req := httptest.NewRequest(method, path, &buf)
	resp, err := p.HandleRequest(context.Background(), op, req)
	if err != nil {
		t.Fatalf("op %s: %v", op, err)
	}
	if resp == nil {
		t.Fatalf("op %s: nil response", op)
	}
	if resp.StatusCode >= 400 {
		t.Fatalf("op %s: status %d: %s", op, resp.StatusCode, string(resp.Body))
	}
	var result map[string]any
	if len(resp.Body) > 0 {
		if err := json.Unmarshal(resp.Body, &result); err != nil {
			t.Fatalf("decode response: %v", err)
		}
	}
	return result
}

func callOpStatus(t *testing.T, p *Provider, op, path, method string, body map[string]any, wantStatus int) map[string]any {
	t.Helper()
	var buf bytes.Buffer
	if body != nil {
		if err := json.NewEncoder(&buf).Encode(body); err != nil {
			t.Fatalf("encode body: %v", err)
		}
	}
	req := httptest.NewRequest(method, path, &buf)
	resp, err := p.HandleRequest(context.Background(), op, req)
	if err != nil {
		t.Fatalf("op %s: %v", op, err)
	}
	if resp == nil {
		t.Fatalf("op %s: nil response", op)
	}
	if resp.StatusCode != wantStatus {
		t.Fatalf("op %s: want status %d, got %d: %s", op, wantStatus, resp.StatusCode, string(resp.Body))
	}
	var result map[string]any
	if len(resp.Body) > 0 {
		_ = json.Unmarshal(resp.Body, &result)
	}
	return result
}

// TestEmailIdentityCRUD tests create/get/list/delete of email identities.
func TestEmailIdentityCRUD(t *testing.T) {
	p := newProvider(t)

	// Create
	callOp(t, p, "CreateEmailIdentity", "/v2/email/identities", http.MethodPost, map[string]any{
		"EmailIdentity": "test@example.com",
	})

	// Get
	resp := callOp(t, p, "GetEmailIdentity", "/v2/email/identities/test@example.com", http.MethodGet, nil)
	if resp["IdentityType"] != "EMAIL_ADDRESS" {
		t.Errorf("expected EMAIL_ADDRESS, got %v", resp["IdentityType"])
	}

	// List
	listResp := callOp(t, p, "ListEmailIdentities", "/v2/email/identities", http.MethodGet, nil)
	ids, ok := listResp["EmailIdentities"].([]any)
	if !ok || len(ids) != 1 {
		t.Errorf("expected 1 identity, got %v", listResp["EmailIdentities"])
	}

	// Duplicate -> 400
	callOpStatus(t, p, "CreateEmailIdentity", "/v2/email/identities", http.MethodPost, map[string]any{
		"EmailIdentity": "test@example.com",
	}, http.StatusBadRequest)

	// Put DKIM attributes
	callOp(t, p, "PutEmailIdentityDkimAttributes", "/v2/email/identities/test@example.com/dkim", http.MethodPut, map[string]any{
		"SigningEnabled": true,
	})

	// Put config set
	callOp(t, p, "CreateConfigurationSet", "/v2/email/configuration-sets", http.MethodPost, map[string]any{
		"ConfigurationSetName": "my-config",
	})
	callOp(t, p, "PutEmailIdentityConfigurationSetAttributes", "/v2/email/identities/test@example.com/configuration-set", http.MethodPut, map[string]any{
		"ConfigurationSetName": "my-config",
	})

	// Identity policies
	callOp(t, p, "CreateEmailIdentityPolicy", "/v2/email/identities/test@example.com/policies/MyPolicy", http.MethodPost, map[string]any{
		"Policy": `{"Version":"2012-10-17"}`,
	})
	polResp := callOp(t, p, "GetEmailIdentityPolicies", "/v2/email/identities/test@example.com/policies", http.MethodGet, nil)
	policies, _ := polResp["Policies"].(map[string]any)
	if _, exists := policies["MyPolicy"]; !exists {
		t.Errorf("expected MyPolicy, got %v", polResp)
	}
	callOp(t, p, "DeleteEmailIdentityPolicy", "/v2/email/identities/test@example.com/policies/MyPolicy", http.MethodDelete, nil)

	// Delete
	callOp(t, p, "DeleteEmailIdentity", "/v2/email/identities/test@example.com", http.MethodDelete, nil)

	// Get after delete -> 404
	callOpStatus(t, p, "GetEmailIdentity", "/v2/email/identities/test@example.com", http.MethodGet, nil, http.StatusNotFound)
}

// TestEmailTemplateCRUD tests create/get/list/update/delete/render of templates.
func TestEmailTemplateCRUD(t *testing.T) {
	p := newProvider(t)

	// Create
	callOp(t, p, "CreateEmailTemplate", "/v2/email/templates", http.MethodPost, map[string]any{
		"TemplateName": "welcome",
		"TemplateContent": map[string]any{
			"Subject": "Welcome!",
			"Html":    "<h1>Hello</h1>",
			"Text":    "Hello",
		},
	})

	// Get
	resp := callOp(t, p, "GetEmailTemplate", "/v2/email/templates/welcome", http.MethodGet, nil)
	if resp["TemplateName"] != "welcome" {
		t.Errorf("expected template name 'welcome', got %v", resp["TemplateName"])
	}
	content, _ := resp["TemplateContent"].(map[string]any)
	if content["Subject"] != "Welcome!" {
		t.Errorf("expected subject 'Welcome!', got %v", content["Subject"])
	}

	// List
	listResp := callOp(t, p, "ListEmailTemplates", "/v2/email/templates", http.MethodGet, nil)
	templates, _ := listResp["TemplatesMetadata"].([]any)
	if len(templates) != 1 {
		t.Errorf("expected 1 template, got %d", len(templates))
	}

	// Update
	callOp(t, p, "UpdateEmailTemplate", "/v2/email/templates/welcome", http.MethodPut, map[string]any{
		"TemplateContent": map[string]any{
			"Subject": "Updated!",
			"Html":    "<h1>Updated</h1>",
			"Text":    "Updated",
		},
	})
	resp2 := callOp(t, p, "GetEmailTemplate", "/v2/email/templates/welcome", http.MethodGet, nil)
	content2, _ := resp2["TemplateContent"].(map[string]any)
	if content2["Subject"] != "Updated!" {
		t.Errorf("expected 'Updated!', got %v", content2["Subject"])
	}

	// TestRender
	renderResp := callOp(t, p, "TestRenderEmailTemplate", "/v2/email/templates/welcome/render", http.MethodPost, map[string]any{
		"TemplateData": "{}",
	})
	if renderResp["RenderedTemplate"] == nil {
		t.Errorf("expected RenderedTemplate in response")
	}

	// Delete
	callOp(t, p, "DeleteEmailTemplate", "/v2/email/templates/welcome", http.MethodDelete, nil)
	callOpStatus(t, p, "GetEmailTemplate", "/v2/email/templates/welcome", http.MethodGet, nil, http.StatusNotFound)
}

// TestConfigurationSetCRUD tests configuration set operations.
func TestConfigurationSetCRUD(t *testing.T) {
	p := newProvider(t)

	// Create
	callOp(t, p, "CreateConfigurationSet", "/v2/email/configuration-sets", http.MethodPost, map[string]any{
		"ConfigurationSetName": "prod-config",
	})

	// Get
	resp := callOp(t, p, "GetConfigurationSet", "/v2/email/configuration-sets/prod-config", http.MethodGet, nil)
	if resp["ConfigurationSetName"] != "prod-config" {
		t.Errorf("expected 'prod-config', got %v", resp["ConfigurationSetName"])
	}

	// List
	listResp := callOp(t, p, "ListConfigurationSets", "/v2/email/configuration-sets", http.MethodGet, nil)
	sets, _ := listResp["ConfigurationSets"].([]any)
	if len(sets) != 1 {
		t.Errorf("expected 1 config set, got %d", len(sets))
	}

	// Put sending options
	callOp(t, p, "PutConfigurationSetSendingOptions", "/v2/email/configuration-sets/prod-config/sending", http.MethodPut, map[string]any{
		"SendingEnabled": false,
	})

	// Put other options (stubs that verify config set existence)
	callOp(t, p, "PutConfigurationSetReputationOptions", "/v2/email/configuration-sets/prod-config/reputation-options", http.MethodPut, map[string]any{})
	callOp(t, p, "PutConfigurationSetSuppressionOptions", "/v2/email/configuration-sets/prod-config/suppression-options", http.MethodPut, map[string]any{})
	callOp(t, p, "PutConfigurationSetTrackingOptions", "/v2/email/configuration-sets/prod-config/tracking-options", http.MethodPut, map[string]any{})
	callOp(t, p, "PutConfigurationSetDeliveryOptions", "/v2/email/configuration-sets/prod-config/delivery-options", http.MethodPut, map[string]any{})

	// Create event destination
	callOp(t, p, "CreateConfigurationSetEventDestination", "/v2/email/configuration-sets/prod-config/event-destinations", http.MethodPost, map[string]any{
		"EventDestinationName": "my-dest",
		"EventDestination": map[string]any{
			"Enabled":            true,
			"MatchingEventTypes": []string{"SEND"},
		},
	})

	// Get event destinations
	destResp := callOp(t, p, "GetConfigurationSetEventDestinations", "/v2/email/configuration-sets/prod-config/event-destinations", http.MethodGet, nil)
	dests, _ := destResp["EventDestinations"].([]any)
	if len(dests) != 1 {
		t.Errorf("expected 1 event destination, got %d", len(dests))
	}

	// Update event destination
	callOp(t, p, "UpdateConfigurationSetEventDestination", "/v2/email/configuration-sets/prod-config/event-destinations/my-dest", http.MethodPut, map[string]any{
		"EventDestination": map[string]any{"Enabled": false},
	})

	// Delete event destination
	callOp(t, p, "DeleteConfigurationSetEventDestination", "/v2/email/configuration-sets/prod-config/event-destinations/my-dest", http.MethodDelete, nil)

	// Delete config set
	callOp(t, p, "DeleteConfigurationSet", "/v2/email/configuration-sets/prod-config", http.MethodDelete, nil)
	callOpStatus(t, p, "GetConfigurationSet", "/v2/email/configuration-sets/prod-config", http.MethodGet, nil, http.StatusNotFound)
}

// TestContactListAndContactCRUD tests contact list and contact operations.
func TestContactListAndContactCRUD(t *testing.T) {
	p := newProvider(t)

	// Create list
	callOp(t, p, "CreateContactList", "/v2/email/contact-lists", http.MethodPost, map[string]any{
		"ContactListName": "subscribers",
		"Description":     "Main subscribers",
	})

	// Get list
	listResp := callOp(t, p, "GetContactList", "/v2/email/contact-lists/subscribers", http.MethodGet, nil)
	if listResp["ContactListName"] != "subscribers" {
		t.Errorf("expected 'subscribers', got %v", listResp["ContactListName"])
	}

	// List contact lists
	allListsResp := callOp(t, p, "ListContactLists", "/v2/email/contact-lists", http.MethodGet, nil)
	lists, _ := allListsResp["ContactLists"].([]any)
	if len(lists) != 1 {
		t.Errorf("expected 1 contact list, got %d", len(lists))
	}

	// Update list
	callOp(t, p, "UpdateContactList", "/v2/email/contact-lists/subscribers", http.MethodPut, map[string]any{
		"Description": "Updated description",
	})

	// Create contact
	callOp(t, p, "CreateContact", "/v2/email/contact-lists/subscribers/contacts", http.MethodPost, map[string]any{
		"EmailAddress":   "user@example.com",
		"UnsubscribeAll": false,
	})

	// Get contact
	contResp := callOp(t, p, "GetContact", "/v2/email/contact-lists/subscribers/contacts/user@example.com", http.MethodGet, nil)
	if contResp["EmailAddress"] != "user@example.com" {
		t.Errorf("expected 'user@example.com', got %v", contResp["EmailAddress"])
	}

	// List contacts
	contactsResp := callOp(t, p, "ListContacts", "/v2/email/contact-lists/subscribers/contacts/list", http.MethodPost, map[string]any{})
	contacts, _ := contactsResp["Contacts"].([]any)
	if len(contacts) != 1 {
		t.Errorf("expected 1 contact, got %d", len(contacts))
	}

	// Update contact
	callOp(t, p, "UpdateContact", "/v2/email/contact-lists/subscribers/contacts/user@example.com", http.MethodPut, map[string]any{
		"UnsubscribeAll": true,
	})

	// Delete contact
	callOp(t, p, "DeleteContact", "/v2/email/contact-lists/subscribers/contacts/user@example.com", http.MethodDelete, nil)
	callOpStatus(t, p, "GetContact", "/v2/email/contact-lists/subscribers/contacts/user@example.com", http.MethodGet, nil, http.StatusNotFound)

	// Delete list
	callOp(t, p, "DeleteContactList", "/v2/email/contact-lists/subscribers", http.MethodDelete, nil)
	callOpStatus(t, p, "GetContactList", "/v2/email/contact-lists/subscribers", http.MethodGet, nil, http.StatusNotFound)
}

// TestSuppressedDestinationCRUD tests suppressed destination operations.
func TestSuppressedDestinationCRUD(t *testing.T) {
	p := newProvider(t)

	// Put
	callOp(t, p, "PutSuppressedDestination", "/v2/email/suppression/addresses", http.MethodPut, map[string]any{
		"EmailAddress": "bad@example.com",
		"Reason":       "BOUNCE",
	})

	// Get
	resp := callOp(t, p, "GetSuppressedDestination", "/v2/email/suppression/addresses/bad@example.com", http.MethodGet, nil)
	sd, _ := resp["SuppressedDestination"].(map[string]any)
	if sd["EmailAddress"] != "bad@example.com" {
		t.Errorf("expected 'bad@example.com', got %v", sd["EmailAddress"])
	}

	// List
	listResp := callOp(t, p, "ListSuppressedDestinations", "/v2/email/suppression/addresses", http.MethodGet, nil)
	items, _ := listResp["SuppressedDestinationSummaries"].([]any)
	if len(items) != 1 {
		t.Errorf("expected 1 suppressed destination, got %d", len(items))
	}

	// Update via put (upsert)
	callOp(t, p, "PutSuppressedDestination", "/v2/email/suppression/addresses", http.MethodPut, map[string]any{
		"EmailAddress": "bad@example.com",
		"Reason":       "COMPLAINT",
	})

	// Delete
	callOp(t, p, "DeleteSuppressedDestination", "/v2/email/suppression/addresses/bad@example.com", http.MethodDelete, nil)
	callOpStatus(t, p, "GetSuppressedDestination", "/v2/email/suppression/addresses/bad@example.com", http.MethodGet, nil, http.StatusNotFound)
}

// TestSendEmail tests the SendEmail, SendBulkEmail, and SendCustomVerificationEmail operations.
func TestSendEmail(t *testing.T) {
	p := newProvider(t)

	// SendEmail
	resp := callOp(t, p, "SendEmail", "/v2/email/outbound-emails", http.MethodPost, map[string]any{
		"FromEmailAddress": "sender@example.com",
		"Destination": map[string]any{
			"ToAddresses": []string{"recipient@example.com"},
		},
		"Content": map[string]any{
			"Simple": map[string]any{
				"Subject": map[string]any{"Data": "Hello"},
				"Body": map[string]any{
					"Text": map[string]any{"Data": "Hello World"},
				},
			},
		},
	})
	msgID, ok := resp["MessageId"].(string)
	if !ok || msgID == "" {
		t.Errorf("expected MessageId, got %v", resp)
	}

	// SendBulkEmail
	bulkResp := callOp(t, p, "SendBulkEmail", "/v2/email/outbound-bulk-emails", http.MethodPost, map[string]any{
		"DefaultContent": map[string]any{},
		"BulkEmailEntries": []any{
			map[string]any{"Destination": map[string]any{"ToAddresses": []string{"a@example.com"}}},
			map[string]any{"Destination": map[string]any{"ToAddresses": []string{"b@example.com"}}},
		},
	})
	results, _ := bulkResp["BulkEmailEntryResults"].([]any)
	if len(results) != 2 {
		t.Errorf("expected 2 bulk results, got %d", len(results))
	}

	// SendCustomVerificationEmail
	cvResp := callOp(t, p, "SendCustomVerificationEmail", "/v2/email/outbound-custom-verification-emails", http.MethodPost, map[string]any{
		"EmailAddress": "verify@example.com",
		"TemplateName": "VerifyTemplate",
	})
	if cvResp["MessageId"] == nil {
		t.Errorf("expected MessageId in custom verification response")
	}

	// GetAccount
	acctResp := callOp(t, p, "GetAccount", "/v2/email/account", http.MethodGet, nil)
	if acctResp["SendingEnabled"] != true {
		t.Errorf("expected SendingEnabled=true, got %v", acctResp["SendingEnabled"])
	}
}

// TestTags tests TagResource, UntagResource, ListTagsForResource.
func TestTags(t *testing.T) {
	p := newProvider(t)

	arn := "arn:aws:ses:us-east-1:000000000000:identity/test@example.com"

	// Tag
	tagBody := map[string]any{
		"ResourceArn": arn,
		"Tags": []any{
			map[string]any{"Key": "Env", "Value": "prod"},
			map[string]any{"Key": "Team", "Value": "platform"},
		},
	}
	callOp(t, p, "TagResource", "/v2/email/tags", http.MethodPost, tagBody)

	// List tags
	req := httptest.NewRequest(http.MethodGet, "/v2/email/tags?ResourceArn="+arn, nil)
	resp, err := p.HandleRequest(context.Background(), "ListTagsForResource", req)
	if err != nil {
		t.Fatalf("ListTagsForResource: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", resp.StatusCode, string(resp.Body))
	}
	var result map[string]any
	_ = json.Unmarshal(resp.Body, &result)
	tags, _ := result["Tags"].([]any)
	if len(tags) != 2 {
		t.Errorf("expected 2 tags, got %d: %v", len(tags), tags)
	}

	// Untag
	untagReq := httptest.NewRequest(http.MethodDelete, "/v2/email/tags?ResourceArn="+arn+"&TagKeys=Env", nil)
	resp2, err := p.HandleRequest(context.Background(), "UntagResource", untagReq)
	if err != nil {
		t.Fatalf("UntagResource: %v", err)
	}
	if resp2.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp2.StatusCode)
	}

	// List again
	req3 := httptest.NewRequest(http.MethodGet, "/v2/email/tags?ResourceArn="+arn, nil)
	resp3, _ := p.HandleRequest(context.Background(), "ListTagsForResource", req3)
	var result3 map[string]any
	_ = json.Unmarshal(resp3.Body, &result3)
	tags3, _ := result3["Tags"].([]any)
	if len(tags3) != 1 {
		t.Errorf("expected 1 tag after untag, got %d: %v", len(tags3), tags3)
	}
}
