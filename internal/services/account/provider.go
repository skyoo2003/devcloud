// SPDX-License-Identifier: Apache-2.0

// Package account implements AWS Account Management service.
package account

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"path/filepath"
	"strings"

	"github.com/skyoo2003/devcloud/internal/plugin"
)

const defaultAccountID = plugin.DefaultAccountID

// AccountProvider implements plugin.ServicePlugin for AWS Account Management.
type AccountProvider struct {
	dataDir string
	store   *Store
}

func (p *AccountProvider) ServiceID() string             { return "account" }
func (p *AccountProvider) ServiceName() string           { return "AWS Account Management" }
func (p *AccountProvider) Protocol() plugin.ProtocolType { return plugin.ProtocolRESTJSON }

func (p *AccountProvider) Init(cfg plugin.PluginConfig) error {
	p.dataDir = cfg.DataDir
	dir := cfg.DataDir
	if dir == "" {
		dir = "."
	}
	var err error
	p.store, err = NewStore(filepath.Join(dir, "account"))
	return err
}

func (p *AccountProvider) Shutdown(_ context.Context) error {
	if p.store != nil {
		return p.store.Close()
	}
	return nil
}

// HandleRequest routes the incoming HTTP request to the appropriate Account operation.
func (p *AccountProvider) HandleRequest(_ context.Context, op string, req *http.Request) (*plugin.Response, error) {
	body, _ := io.ReadAll(req.Body)
	var bodyMap map[string]any
	if len(body) > 0 {
		_ = json.Unmarshal(body, &bodyMap)
	}
	if bodyMap == nil {
		bodyMap = map[string]any{}
	}

	path := req.URL.Path
	method := req.Method
	q := req.URL.Query()

	// Normalize path: strip leading slash and lowercase for matching
	pathLower := strings.ToLower(strings.TrimPrefix(path, "/"))

	switch pathLower {
	// --- Contact Information ---
	case "getcontactinformation":
		return p.getContactInformation()
	case "putcontactinformation":
		return p.putContactInformation(bodyMap)
	case "deletecontactinformation":
		return p.deleteContactInformation()

	// --- Alternate Contact ---
	case "getalternatecontact":
		contactType := strVal(bodyMap, "AlternateContactType")
		if contactType == "" {
			contactType = q.Get("AlternateContactType")
		}
		return p.getAlternateContact(contactType)
	case "putalternatecontact":
		return p.putAlternateContact(bodyMap)
	case "deletealternatecontact":
		contactType := strVal(bodyMap, "AlternateContactType")
		if contactType == "" {
			contactType = q.Get("AlternateContactType")
		}
		return p.deleteAlternateContact(contactType)

	// --- Regions ---
	case "listregions":
		return p.listRegions()
	case "getregionoptstatus":
		regionName := strVal(bodyMap, "RegionName")
		if regionName == "" {
			regionName = q.Get("RegionName")
		}
		return p.getRegionOptStatus(regionName)
	case "enableregion":
		return p.enableRegion(bodyMap)
	case "disableregion":
		return p.disableRegion(bodyMap)

	// --- Primary Email ---
	case "getprimaryemail":
		return p.getPrimaryEmail()
	case "startprimaryemailupdate":
		return p.startPrimaryEmailUpdate(bodyMap)
	case "acceptprimaryemailupdate":
		return p.acceptPrimaryEmailUpdate(bodyMap)

	// --- Legacy paths ---
	case "contactinformation":
		switch method {
		case http.MethodGet:
			return p.getContactInformation()
		case http.MethodPut:
			return p.putContactInformation(bodyMap)
		case http.MethodDelete:
			return p.deleteContactInformation()
		}
	case "alternatecontact":
		switch method {
		case http.MethodGet:
			contactType := strVal(bodyMap, "AlternateContactType")
			if contactType == "" {
				contactType = q.Get("AlternateContactType")
			}
			return p.getAlternateContact(contactType)
		case http.MethodPut:
			return p.putAlternateContact(bodyMap)
		case http.MethodDelete:
			contactType := strVal(bodyMap, "AlternateContactType")
			if contactType == "" {
				contactType = q.Get("AlternateContactType")
			}
			return p.deleteAlternateContact(contactType)
		}
	case "regions":
		return p.listRegions()
	case "regionoptstatus":
		regionName := strVal(bodyMap, "RegionName")
		if regionName == "" {
			regionName = q.Get("RegionName")
		}
		return p.getRegionOptStatus(regionName)
	case "primaryemail":
		return p.getPrimaryEmail()
	case "primaryemailupdate":
		return p.startPrimaryEmailUpdate(bodyMap)
	case "primaryemailupdate/accept":
		return p.acceptPrimaryEmailUpdate(bodyMap)
	}

	// Op-based fallback routing
	switch op {
	case "GetContactInformation":
		return p.getContactInformation()
	case "PutContactInformation":
		return p.putContactInformation(bodyMap)
	case "DeleteContactInformation":
		return p.deleteContactInformation()
	case "GetAlternateContact":
		contactType := strVal(bodyMap, "AlternateContactType")
		if contactType == "" {
			contactType = q.Get("AlternateContactType")
		}
		return p.getAlternateContact(contactType)
	case "PutAlternateContact":
		return p.putAlternateContact(bodyMap)
	case "DeleteAlternateContact":
		contactType := strVal(bodyMap, "AlternateContactType")
		if contactType == "" {
			contactType = q.Get("AlternateContactType")
		}
		return p.deleteAlternateContact(contactType)
	case "ListRegions":
		return p.listRegions()
	case "GetRegionOptStatus":
		regionName := strVal(bodyMap, "RegionName")
		if regionName == "" {
			regionName = q.Get("RegionName")
		}
		return p.getRegionOptStatus(regionName)
	case "EnableRegion":
		return p.enableRegion(bodyMap)
	case "DisableRegion":
		return p.disableRegion(bodyMap)
	case "GetPrimaryEmail":
		return p.getPrimaryEmail()
	case "StartPrimaryEmailUpdate":
		return p.startPrimaryEmailUpdate(bodyMap)
	case "AcceptPrimaryEmailUpdate":
		return p.acceptPrimaryEmailUpdate(bodyMap)
	case "GetAccount":
		return p.handleGetAccount()
	case "ListAccountManagedUsers":
		return p.handleListAccountManagedUsers()
	}

	return jsonError("UnsupportedOperation", "operation not supported", http.StatusBadRequest), nil
}

func (p *AccountProvider) ListResources(_ context.Context) ([]plugin.Resource, error) {
	return []plugin.Resource{
		{Type: "account", ID: defaultAccountID, Name: "Default Account"},
	}, nil
}

func (p *AccountProvider) GetMetrics(_ context.Context) (*plugin.ServiceMetrics, error) {
	return &plugin.ServiceMetrics{}, nil
}

// --- helpers ---

func jsonError(code, message string, status int) *plugin.Response {
	body, _ := json.Marshal(map[string]string{"__type": code, "message": message})
	return &plugin.Response{
		StatusCode:  status,
		ContentType: "application/json",
		Body:        body,
	}
}

func jsonResponse(status int, v any) (*plugin.Response, error) {
	body, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	return &plugin.Response{
		StatusCode:  status,
		ContentType: "application/json",
		Body:        body,
	}, nil
}

func strVal(m map[string]any, key string) string {
	if v, ok := m[key]; ok {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}

// --- Contact Information ---

func (p *AccountProvider) getContactInformation() (*plugin.Response, error) {
	c, err := p.store.GetContactInfo(defaultAccountID)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return jsonError("ResourceNotFoundException", "contact information not found", http.StatusNotFound), nil
		}
		return jsonError("InternalError", err.Error(), http.StatusInternalServerError), nil
	}
	return jsonResponse(http.StatusOK, map[string]any{
		"ContactInformation": map[string]any{
			"FullName":     c.FullName,
			"CompanyName":  c.CompanyName,
			"PhoneNumber":  c.PhoneNumber,
			"AddressLine1": c.AddressLine1,
			"City":         c.City,
			"PostalCode":   c.PostalCode,
			"CountryCode":  c.CountryCode,
			"WebsiteUrl":   c.WebsiteURL,
		},
	})
}

func (p *AccountProvider) putContactInformation(body map[string]any) (*plugin.Response, error) {
	ci := &ContactInfo{}
	if contactRaw, ok := body["ContactInformation"].(map[string]any); ok {
		ci.FullName = strVal(contactRaw, "FullName")
		ci.CompanyName = strVal(contactRaw, "CompanyName")
		ci.PhoneNumber = strVal(contactRaw, "PhoneNumber")
		ci.AddressLine1 = strVal(contactRaw, "AddressLine1")
		ci.City = strVal(contactRaw, "City")
		ci.PostalCode = strVal(contactRaw, "PostalCode")
		ci.CountryCode = strVal(contactRaw, "CountryCode")
		ci.WebsiteURL = strVal(contactRaw, "WebsiteUrl")
	} else {
		// Flat body
		ci.FullName = strVal(body, "FullName")
		ci.CompanyName = strVal(body, "CompanyName")
		ci.PhoneNumber = strVal(body, "PhoneNumber")
		ci.AddressLine1 = strVal(body, "AddressLine1")
		ci.City = strVal(body, "City")
		ci.PostalCode = strVal(body, "PostalCode")
		ci.CountryCode = strVal(body, "CountryCode")
		ci.WebsiteURL = strVal(body, "WebsiteUrl")
	}
	if err := p.store.PutContactInfo(defaultAccountID, ci); err != nil {
		return jsonError("InternalError", err.Error(), http.StatusInternalServerError), nil
	}
	return &plugin.Response{StatusCode: http.StatusNoContent, ContentType: "application/json", Body: []byte{}}, nil
}

func (p *AccountProvider) deleteContactInformation() (*plugin.Response, error) {
	if err := p.store.DeleteContactInfo(defaultAccountID); err != nil {
		return jsonError("InternalError", err.Error(), http.StatusInternalServerError), nil
	}
	return &plugin.Response{StatusCode: http.StatusNoContent, ContentType: "application/json", Body: []byte{}}, nil
}

// --- Alternate Contacts ---

func (p *AccountProvider) getAlternateContact(contactType string) (*plugin.Response, error) {
	c, err := p.store.GetAlternateContact(defaultAccountID, contactType)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return jsonError("ResourceNotFoundException", "alternate contact not found", http.StatusNotFound), nil
		}
		return jsonError("InternalError", err.Error(), http.StatusInternalServerError), nil
	}
	return jsonResponse(http.StatusOK, map[string]any{
		"AlternateContact": map[string]any{
			"AlternateContactType": c.ContactType,
			"Name":                 c.Name,
			"Title":                c.Title,
			"EmailAddress":         c.Email,
			"PhoneNumber":          c.PhoneNumber,
		},
	})
}

func (p *AccountProvider) putAlternateContact(body map[string]any) (*plugin.Response, error) {
	c := &AlternateContact{}
	if acRaw, ok := body["AlternateContact"].(map[string]any); ok {
		c.ContactType = strVal(acRaw, "AlternateContactType")
		c.Name = strVal(acRaw, "Name")
		c.Title = strVal(acRaw, "Title")
		c.Email = strVal(acRaw, "EmailAddress")
		c.PhoneNumber = strVal(acRaw, "PhoneNumber")
	} else {
		c.ContactType = strVal(body, "AlternateContactType")
		c.Name = strVal(body, "Name")
		c.Title = strVal(body, "Title")
		c.Email = strVal(body, "EmailAddress")
		c.PhoneNumber = strVal(body, "PhoneNumber")
	}
	if err := p.store.PutAlternateContact(defaultAccountID, c); err != nil {
		return jsonError("InternalError", err.Error(), http.StatusInternalServerError), nil
	}
	return &plugin.Response{StatusCode: http.StatusNoContent, ContentType: "application/json", Body: []byte{}}, nil
}

func (p *AccountProvider) deleteAlternateContact(contactType string) (*plugin.Response, error) {
	if err := p.store.DeleteAlternateContact(defaultAccountID, contactType); err != nil {
		return jsonError("InternalError", err.Error(), http.StatusInternalServerError), nil
	}
	return &plugin.Response{StatusCode: http.StatusNoContent, ContentType: "application/json", Body: []byte{}}, nil
}

// --- Regions ---

func (p *AccountProvider) listRegions() (*plugin.Response, error) {
	regions, err := p.store.ListRegions(defaultAccountID)
	if err != nil {
		return jsonError("InternalError", err.Error(), http.StatusInternalServerError), nil
	}
	regionList := make([]map[string]any, 0, len(regions))
	for _, r := range regions {
		regionList = append(regionList, map[string]any{
			"RegionName":      r.RegionName,
			"RegionOptStatus": r.OptStatus,
		})
	}
	return jsonResponse(http.StatusOK, map[string]any{"Regions": regionList})
}

func (p *AccountProvider) getRegionOptStatus(regionName string) (*plugin.Response, error) {
	r, err := p.store.GetRegionOptStatus(defaultAccountID, regionName)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return jsonError("ResourceNotFoundException", "region not found", http.StatusNotFound), nil
		}
		return jsonError("InternalError", err.Error(), http.StatusInternalServerError), nil
	}
	return jsonResponse(http.StatusOK, map[string]any{
		"RegionName":      r.RegionName,
		"RegionOptStatus": r.OptStatus,
	})
}

func (p *AccountProvider) enableRegion(body map[string]any) (*plugin.Response, error) {
	regionName := strVal(body, "RegionName")
	if err := p.store.SetRegionOptStatus(defaultAccountID, regionName, "ENABLED"); err != nil {
		return jsonError("InternalError", err.Error(), http.StatusInternalServerError), nil
	}
	return &plugin.Response{StatusCode: http.StatusNoContent, ContentType: "application/json", Body: []byte{}}, nil
}

func (p *AccountProvider) disableRegion(body map[string]any) (*plugin.Response, error) {
	regionName := strVal(body, "RegionName")
	if err := p.store.SetRegionOptStatus(defaultAccountID, regionName, "DISABLED"); err != nil {
		return jsonError("InternalError", err.Error(), http.StatusInternalServerError), nil
	}
	return &plugin.Response{StatusCode: http.StatusNoContent, ContentType: "application/json", Body: []byte{}}, nil
}

// --- Primary Email ---

func (p *AccountProvider) getPrimaryEmail() (*plugin.Response, error) {
	email, _ := p.store.GetPrimaryEmail(defaultAccountID)
	if email == "" {
		email = "admin@example.com"
	}
	return jsonResponse(http.StatusOK, map[string]any{"PrimaryEmail": email})
}

func (p *AccountProvider) startPrimaryEmailUpdate(body map[string]any) (*plugin.Response, error) {
	pendingEmail := strVal(body, "PrimaryEmail")
	if err := p.store.StartPrimaryEmailUpdate(defaultAccountID, pendingEmail); err != nil {
		return jsonError("InternalError", err.Error(), http.StatusInternalServerError), nil
	}
	return jsonResponse(http.StatusCreated, map[string]any{"Status": "PENDING", "PendingPrimaryEmail": pendingEmail})
}

func (p *AccountProvider) acceptPrimaryEmailUpdate(body map[string]any) (*plugin.Response, error) {
	// Accept OTP — in emulator just accept it
	if err := p.store.AcceptPrimaryEmailUpdate(defaultAccountID); err != nil {
		if errors.Is(err, ErrNotFound) {
			return jsonError("ResourceNotFoundException", "no pending email update", http.StatusNotFound), nil
		}
		return jsonError("InternalError", err.Error(), http.StatusInternalServerError), nil
	}
	email, _ := p.store.GetPrimaryEmail(defaultAccountID)
	return jsonResponse(http.StatusOK, map[string]any{"Status": "ACCEPTED", "PrimaryEmail": email})
}

// --- Legacy ops ---

func (p *AccountProvider) handleGetAccount() (*plugin.Response, error) {
	return jsonResponse(http.StatusOK, map[string]any{
		"AccountID":        defaultAccountID,
		"Arn":              "arn:aws:iam::" + defaultAccountID + ":root",
		"BillingAddress":   "",
		"CompanyName":      "",
		"ContactEmail":     "",
		"ContactName":      "",
		"ContactPhone":     "",
		"Id":               defaultAccountID,
		"RegistrationDate": "2024-01-01T00:00:00Z",
		"Status":           "ACTIVE",
		"StatusMessage":    "",
	})
}

func (p *AccountProvider) handleListAccountManagedUsers() (*plugin.Response, error) {
	return jsonResponse(http.StatusOK, map[string]any{"Users": []any{}})
}

func init() {
	plugin.DefaultRegistry.Register("account", func(cfg plugin.PluginConfig) plugin.ServicePlugin {
		return &AccountProvider{}
	})
}
