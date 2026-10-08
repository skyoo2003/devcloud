// SPDX-License-Identifier: Apache-2.0
package sns

import (
	"github.com/skyoo2003/devcloud/internal/plugin"
	"math"
	"net/http"
	"regexp"
	"strconv"
	"strings"
)

var e164Phone = regexp.MustCompile(`^\+[1-9][0-9]{7,14}$`)
var senderID = regexp.MustCompile(`^[A-Za-z0-9]{1,11}$`)
var decimalAmount = regexp.MustCompile(`^[0-9]+(\.[0-9]+)?$`)
var roleARN = regexp.MustCompile(`^arn:aws[a-z-]*:iam::[0-9]{12}:role/.+$`)

func validateSMSAttributes(attrs map[string]string) error {
	if len(attrs) == 0 {
		return invalidSNS("attributes required")
	}
	for k, v := range attrs {
		var valid bool
		switch k {
		case "MonthlySpendLimit":
			n, e := strconv.ParseFloat(v, 64)
			valid = decimalAmount.MatchString(v) && e == nil && !math.IsInf(n, 0) && !math.IsNaN(n) && n >= 0
		case "DeliveryStatusSuccessSamplingRate":
			n, e := strconv.Atoi(v)
			valid = e == nil && n >= 0 && n <= 100
		case "DefaultSenderID":
			valid = senderID.MatchString(v) && strings.ContainsAny(v, "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
		case "DefaultSMSType":
			valid = v == "Promotional" || v == "Transactional"
		case "DeliveryStatusIAMRole":
			valid = roleARN.MatchString(v)
		case "UsageReportS3Bucket":
			valid = strings.TrimSpace(v) != ""
		default:
			valid = false
		}
		if !valid {
			return invalidSNS("invalid SMS attribute " + k)
		}
	}
	return nil
}

func (p *Provider) handleSMSSettings(action string, req *http.Request) (*plugin.Response, error) {
	switch action {
	case "SetSMSAttributes":
		attrs, e := queryStringMap(req.Form, "attributes")
		if e == nil {
			e = p.store.SetSMSAttributes(defaultAccountID, attrs)
		}
		if e != nil {
			return canonicalSNSError(e)
		}
		return snsUnit(action)
	case "GetSMSAttributes":
		names, e := queryStringList(req.Form, "attributes")
		if e != nil {
			return canonicalSNSError(e)
		}
		a, e := p.store.GetSMSAttributes(defaultAccountID, names)
		if e != nil {
			return canonicalSNSError(e)
		}
		return snsXML(action, struct {
			Attributes []queryAttribute `xml:"attributes>entry"`
		}{queryAttributes(a)})
	case "OptInPhoneNumber":
		if e := p.store.OptInPhoneNumber(req.FormValue("phoneNumber"), defaultAccountID); e != nil {
			return canonicalSNSError(e)
		}
		return snsUnit(action)
	case "CheckIfPhoneNumberIsOptedOut":
		phone := req.FormValue("phoneNumber")
		if !e164Phone.MatchString(phone) {
			return canonicalSNSError(invalidSNS("phoneNumber must be E.164"))
		}
		out, e := p.store.IsPhoneOptedOut(phone, defaultAccountID)
		if e != nil {
			return canonicalSNSError(e)
		}
		return snsXML(action, struct {
			IsOptedOut bool `xml:"isOptedOut"`
		}{out})
	default:
		phones, e := p.store.ListOptedOutPhones(defaultAccountID)
		if e != nil {
			return canonicalSNSError(e)
		}
		phones, token, e := pageSNS(action, map[string]string{"account": defaultAccountID}, phones, func(s string) string { return s }, 100, req.FormValue("nextToken"))
		if e != nil {
			return canonicalSNSError(e)
		}
		return snsXML(action, struct {
			PhoneNumbers []string `xml:"phoneNumbers>member"`
			NextToken    string   `xml:"nextToken,omitempty"`
		}{phones, token})
	}
}
