// SPDX-License-Identifier: Apache-2.0
package sns

import (
	"github.com/skyoo2003/devcloud/internal/plugin"
	"net/http"
	"regexp"
	"time"
)

var sandboxPhonePattern = regexp.MustCompile(`^(\+[0-9]{8,}|[0-9]{1,9})$`)
var sandboxOTP = regexp.MustCompile(`^[0-9]{5,8}$`)
var sandboxLanguages = map[string]bool{"en-US": true, "en-GB": true, "es-419": true, "es-ES": true, "de-DE": true, "fr-CA": true, "fr-FR": true, "it-IT": true, "ja-JP": true, "pt-BR": true, "kr-KR": true, "zh-CN": true, "zh-TW": true}

func validateSandboxPhone(phone string) error {
	if len(phone) > 20 || !sandboxPhonePattern.MatchString(phone) {
		return invalidSNS("invalid PhoneNumber")
	}
	return nil
}

type sandboxMember struct {
	PhoneNumber string `xml:"PhoneNumber"`
	Status      string `xml:"Status"`
}

func (p *Provider) handleSandbox(action string, req *http.Request) (*plugin.Response, error) {
	phone := req.FormValue("PhoneNumber")
	switch action {
	case "CreateSMSSandboxPhoneNumber":
		language := req.FormValue("LanguageCode")
		if language == "" {
			language = "en-US"
		}
		_, e := p.store.IssueSandboxChallenge(phone, defaultAccountID, language, time.Now().UTC())
		if e != nil {
			return canonicalSNSError(e)
		}
		return snsUnit(action)
	case "VerifySMSSandboxPhoneNumber":
		if e := p.store.VerifySandboxChallenge(phone, defaultAccountID, req.FormValue("OneTimePassword"), time.Now().UTC()); e != nil {
			return canonicalSNSError(e)
		}
		return snsUnit(action)
	case "DeleteSMSSandboxPhoneNumber":
		if e := p.store.DeleteSandboxPhone(phone, defaultAccountID); e != nil {
			return canonicalSNSError(e)
		}
		return snsUnit(action)
	case "GetSMSSandboxAccountStatus":
		return snsXML(action, struct {
			IsInSandbox bool `xml:"IsInSandbox"`
		}{true})
	default:
		limit, e := snsPageLimit(req)
		if e != nil {
			return canonicalSNSError(e)
		}
		values, e := p.store.ListSandboxPhones(defaultAccountID)
		if e != nil {
			return canonicalSNSError(e)
		}
		values, next, e := pageSNS(action, map[string]string{"account": defaultAccountID}, values, func(v SandboxPhone) string { return v.PhoneNumber }, limit, req.FormValue("NextToken"))
		if e != nil {
			return canonicalSNSError(e)
		}
		members := []sandboxMember{}
		for _, v := range values {
			members = append(members, sandboxMember{v.PhoneNumber, v.Status})
		}
		return snsXML(action, struct {
			Phones []sandboxMember `xml:"PhoneNumbers>member"`
			Next   string          `xml:"NextToken,omitempty"`
		}{members, next})
	}
}
