// SPDX-License-Identifier: Apache-2.0
package sns

import (
	"github.com/skyoo2003/devcloud/internal/plugin"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"unicode/utf8"
)

var mobileName = regexp.MustCompile(`^[A-Za-z0-9_.-]{1,256}$`)
var mobilePlatforms = map[string]bool{"ADM": true, "APNS": true, "APNS_SANDBOX": true, "GCM": true}
var applicationKeys = map[string]bool{"PlatformCredential": true, "PlatformPrincipal": true, "EventEndpointCreated": true, "EventEndpointDeleted": true, "EventEndpointUpdated": true, "EventDeliveryFailure": true, "SuccessFeedbackRoleArn": true, "FailureFeedbackRoleArn": true, "SuccessFeedbackSampleRate": true, "ApplePlatformTeamID": true, "ApplePlatformBundleID": true}

func validateApplicationAttributes(attrs map[string]string, mutation bool) error {
	if mutation && len(attrs) == 0 {
		return invalidSNS("Attributes required")
	}
	for k, v := range attrs {
		if !applicationKeys[k] {
			return invalidSNS("unknown application attribute")
		}
		if k == "SuccessFeedbackSampleRate" {
			n, e := strconv.Atoi(v)
			if e != nil || n < 0 || n > 100 {
				return invalidSNS("invalid SuccessFeedbackSampleRate")
			}
		}
	}
	return nil
}
func validateEndpointAttributes(attrs map[string]string, mutation bool) error {
	if mutation && len(attrs) == 0 {
		return invalidSNS("Attributes required")
	}
	for k, v := range attrs {
		switch k {
		case "Token":
			if strings.TrimSpace(v) == "" {
				return invalidSNS("Token required")
			}
		case "Enabled":
			if v != "true" && v != "false" {
				return invalidSNS("Enabled must be true or false")
			}
		case "CustomUserData":
			if !utf8.ValidString(v) || len(v) >= 2048 {
				return invalidSNS("CustomUserData must be UTF-8 and less than 2 KiB")
			}
		default:
			return invalidSNS("unknown endpoint attribute")
		}
	}
	return nil
}

type applicationMember struct {
	ARN        string           `xml:"PlatformApplicationArn"`
	Attributes []queryAttribute `xml:"Attributes>entry"`
}
type endpointMember struct {
	ARN        string           `xml:"EndpointArn"`
	Attributes []queryAttribute `xml:"Attributes>entry"`
}

func (p *Provider) handleMobile(action string, req *http.Request) (*plugin.Response, error) {
	applicationARN, endpointARN := req.FormValue("PlatformApplicationArn"), req.FormValue("EndpointArn")
	switch action {
	case "CreatePlatformApplication":
		attrs, e := queryStringMap(req.Form, "Attributes")
		if e == nil {
			e = validateApplicationAttributes(attrs, false)
		}
		name, platform := req.FormValue("Name"), req.FormValue("Platform")
		if e != nil {
			return canonicalSNSError(e)
		}
		if !mobileName.MatchString(name) || !mobilePlatforms[platform] {
			return canonicalSNSError(invalidSNS("invalid Name or Platform"))
		}
		v, e := p.store.CreatePlatformApplication(PlatformApplication{ARN: "arn:aws:sns:" + defaultRegion + ":" + defaultAccountID + ":app/" + platform + "/" + name, Name: name, Platform: platform, AccountID: defaultAccountID, Attributes: attrs})
		if e != nil {
			return canonicalSNSError(e)
		}
		return snsXML(action, struct {
			ARN string `xml:"PlatformApplicationArn"`
		}{v.ARN})
	case "CreatePlatformEndpoint":
		attrs, e := queryStringMap(req.Form, "Attributes")
		if e != nil {
			return canonicalSNSError(e)
		}
		token := req.FormValue("Token")
		if t, ok := attrs["Token"]; ok && t != token {
			return canonicalSNSError(invalidSNS("conflicting Token"))
		}
		attrs["Token"] = token
		if _, ok := attrs["Enabled"]; !ok {
			attrs["Enabled"] = "true"
		}
		if data, ok := req.Form["CustomUserData"]; ok {
			attrs["CustomUserData"] = data[0]
		}
		if e = validateEndpointAttributes(attrs, false); e != nil {
			return canonicalSNSError(e)
		}
		app, e := p.store.GetPlatformApplication(applicationARN)
		if e != nil {
			return canonicalSNSError(e)
		}
		v, e := p.store.CreatePlatformEndpoint(PlatformEndpoint{ARN: "arn:aws:sns:" + defaultRegion + ":" + defaultAccountID + ":endpoint/" + app.Platform + "/" + app.Name + "/" + randomID(16), ApplicationARN: app.ARN, AccountID: app.AccountID, Token: token, Attributes: attrs})
		if e != nil {
			return canonicalSNSError(e)
		}
		return snsXML(action, struct {
			ARN string `xml:"EndpointArn"`
		}{v.ARN})
	case "GetPlatformApplicationAttributes":
		v, e := p.store.GetPlatformApplication(applicationARN)
		if e != nil {
			return canonicalSNSError(e)
		}
		return snsXML(action, struct {
			Attrs []queryAttribute `xml:"Attributes>entry"`
		}{queryAttributes(v.Attributes)})
	case "GetEndpointAttributes":
		v, e := p.store.GetPlatformEndpoint(endpointARN)
		if e != nil {
			return canonicalSNSError(e)
		}
		return snsXML(action, struct {
			Attrs []queryAttribute `xml:"Attributes>entry"`
		}{queryAttributes(v.Attributes)})
	case "SetPlatformApplicationAttributes", "SetEndpointAttributes":
		attrs, e := queryStringMap(req.Form, "Attributes")
		if e == nil {
			if action == "SetEndpointAttributes" {
				e = validateEndpointAttributes(attrs, true)
			} else {
				e = validateApplicationAttributes(attrs, true)
			}
		}
		if e != nil {
			return canonicalSNSError(e)
		}
		if action == "SetEndpointAttributes" {
			_, e = p.store.UpdatePlatformEndpoint(endpointARN, func(v *PlatformEndpoint) error {
				for k, x := range attrs {
					v.Attributes[k] = x
				}
				return nil
			})
		} else {
			_, e = p.store.UpdatePlatformApplication(applicationARN, func(v *PlatformApplication) error {
				for k, x := range attrs {
					v.Attributes[k] = x
				}
				return nil
			})
		}
		if e != nil {
			return canonicalSNSError(e)
		}
		return snsUnit(action)
	case "DeletePlatformApplication":
		if e := p.store.DeletePlatformApplication(applicationARN); e != nil {
			return canonicalSNSError(e)
		}
		return snsUnit(action)
	case "DeleteEndpoint":
		if e := p.store.DeletePlatformEndpoint(endpointARN); e != nil {
			return canonicalSNSError(e)
		}
		return snsUnit(action)
	case "ListPlatformApplications":
		values, e := p.store.ListPlatformApplications(defaultAccountID)
		if e != nil {
			return canonicalSNSError(e)
		}
		values, next, e := pageSNS(action, map[string]string{"account": defaultAccountID}, values, func(v PlatformApplication) string { return v.ARN }, 100, req.FormValue("NextToken"))
		if e != nil {
			return canonicalSNSError(e)
		}
		members := []applicationMember{}
		for _, v := range values {
			members = append(members, applicationMember{v.ARN, queryAttributes(v.Attributes)})
		}
		return snsXML(action, struct {
			Values []applicationMember `xml:"PlatformApplications>member"`
			Next   string              `xml:"NextToken,omitempty"`
		}{members, next})
	default:
		values, e := p.store.ListPlatformEndpoints(applicationARN)
		if e != nil {
			return canonicalSNSError(e)
		}
		values, next, e := pageSNS(action, map[string]string{"parent": applicationARN}, values, func(v PlatformEndpoint) string { return v.ARN }, 100, req.FormValue("NextToken"))
		if e != nil {
			return canonicalSNSError(e)
		}
		members := []endpointMember{}
		for _, v := range values {
			members = append(members, endpointMember{v.ARN, queryAttributes(v.Attributes)})
		}
		return snsXML(action, struct {
			Values []endpointMember `xml:"Endpoints>member"`
			Next   string           `xml:"NextToken,omitempty"`
		}{members, next})
	}
}
