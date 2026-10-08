// SPDX-License-Identifier: Apache-2.0
package sns

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/skyoo2003/devcloud/internal/shared/crud"
	"net/url"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"
)

var ErrSNSLegacyConflict = errors.New("legacy SNS identity or configuration conflict")

type snsLegacyEntry struct {
	record   crud.ResourceSnapshot
	status   string
	app      *PlatformApplication
	endpoint *PlatformEndpoint
	phone    *SandboxPhone
	settings map[string]string
	account  string
}

func snsLegacyString(v map[string]any, keys ...string) string {
	for _, k := range keys {
		if s, ok := v[k].(string); ok && s != "" {
			return s
		}
	}
	return ""
}
func snsLegacyTime(v map[string]any, now time.Time) (time.Time, error) {
	for _, k := range []string{"CreationTime", "CreatedAt", "CreationTimestamp", "created_at"} {
		x, ok := v[k]
		if !ok {
			continue
		}
		switch n := x.(type) {
		case float64:
			return time.Unix(int64(n), int64((n-float64(int64(n)))*1e9)).UTC(), nil
		case string:
			if t, e := time.Parse(time.RFC3339Nano, n); e == nil {
				return t, nil
			}
			if f, e := strconv.ParseFloat(n, 64); e == nil {
				return time.Unix(int64(f), int64((f-float64(int64(f)))*1e9)).UTC(), nil
			}
		}
		return time.Time{}, fmt.Errorf("%w: invalid creation time", ErrSNSLegacyConflict)
	}
	return now, nil
}
func snsLegacyAttributes(v map[string]any) (map[string]string, error) {
	attrs := map[string]string{}
	merge := func(values map[string]string) error {
		for key, value := range values {
			if old, ok := attrs[key]; ok && old != value {
				return ErrSNSLegacyConflict
			}
			attrs[key] = value
		}
		return nil
	}
	for _, prefix := range []string{"Attributes", "attributes"} {
		if x, ok := v[prefix]; ok {
			values, ok := x.(map[string]any)
			if !ok {
				return nil, ErrSNSLegacyConflict
			}
			nested := map[string]string{}
			for key, x := range values {
				value, ok := x.(string)
				if !ok {
					return nil, ErrSNSLegacyConflict
				}
				nested[key] = value
			}
			if err := merge(nested); err != nil {
				return nil, err
			}
		}
		// Generic Query CRUD stores serialized map members as flat JSON keys.
		form := url.Values{}
		for key, x := range v {
			if !strings.HasPrefix(key, prefix+".entry.") {
				continue
			}
			value, ok := x.(string)
			if !ok {
				return nil, ErrSNSLegacyConflict
			}
			form.Set(key, value)
		}
		flat, err := queryStringMap(form, prefix)
		if err != nil {
			return nil, ErrSNSLegacyConflict
		}
		if err := merge(flat); err != nil {
			return nil, err
		}
	}
	return attrs, nil
}
func snsLegacyAccount(v map[string]any, arn string) (string, error) {
	account := snsLegacyString(v, "AccountID", "AccountId", "account_id")
	parts := strings.SplitN(arn, ":", 6)
	if len(parts) == 6 && parts[0] == "arn" {
		if account != "" && account != parts[4] {
			return "", ErrSNSLegacyConflict
		}
		account = parts[4]
	}
	if account == "" {
		account = defaultAccountID
	}
	return account, nil
}

func normalizeSNSLegacy(records []crud.ResourceSnapshot, now time.Time) ([]snsLegacyEntry, error) {
	entries := make([]snsLegacyEntry, 0, len(records))
	for _, r := range records {
		var v map[string]any
		if e := json.Unmarshal(r.JSON, &v); e != nil || v == nil {
			return nil, ErrSNSLegacyConflict
		}
		created, e := snsLegacyTime(v, now)
		if e != nil {
			return nil, e
		}
		attrs, e := snsLegacyAttributes(v)
		if e != nil {
			return nil, e
		}
		entry := snsLegacyEntry{record: r, status: "retained-unaddressable"}
		arn := snsLegacyString(v, "PlatformApplicationArn", "EndpointArn", "Arn", "ARN")
		if arn == "" && strings.HasPrefix(r.ID, "arn:") {
			arn = r.ID
		}
		account, e := snsLegacyAccount(v, arn)
		if e != nil {
			return nil, e
		}
		entry.account = account
		switch r.Resource {
		case "PlatformApplication", "PlatformApplicationAttribute":
			arn = snsLegacyString(v, "PlatformApplicationArn", "Arn", "ARN")
			if arn == "" && strings.HasPrefix(r.ID, "arn:") {
				arn = r.ID
			}
			name, platform := snsLegacyString(v, "Name", "name"), snsLegacyString(v, "Platform", "platform")
			if i := strings.Index(arn, ":app/"); i >= 0 {
				parts := strings.Split(arn[i+5:], "/")
				if len(parts) == 2 {
					if name != "" && name != parts[1] || platform != "" && platform != parts[0] {
						return nil, ErrSNSLegacyConflict
					}
					platform, name = parts[0], parts[1]
				}
			}
			if arn == "" && name != "" && platform != "" {
				arn = "arn:aws:sns:" + defaultRegion + ":" + account + ":app/" + platform + "/" + name
			}
			if strings.HasPrefix(arn, "arn:") && name != "" && platform != "" {
				entry.app = &PlatformApplication{ARN: arn, Name: name, Platform: platform, AccountID: account, Attributes: attrs, CreatedAt: created}
			}
		case "PlatformEndpoint", "EndpointAttribute", "EndpointsByPlatformApplication":
			arn = ""
			for _, key := range []string{"EndpointArn", "PlatformEndpointArn", "Arn", "ARN"} {
				x, exists := v[key]
				if !exists {
					continue
				}
				value, ok := x.(string)
				if !ok || arn != "" && value != "" && arn != value {
					return nil, ErrSNSLegacyConflict
				}
				if value != "" {
					arn = value
				}
			}
			if arn == "" && strings.HasPrefix(r.ID, "arn:") {
				arn = r.ID
			}
			parent := snsLegacyString(v, "PlatformApplicationArn", "ApplicationARN")
			token := snsLegacyString(v, "Token")
			if token == "" {
				token = attrs["Token"]
			}
			if token != "" {
				attrs["Token"] = token
			}
			if _, ok := attrs["Enabled"]; !ok {
				attrs["Enabled"] = "true"
			}
			if x, ok := v["CustomUserData"].(string); ok {
				attrs["CustomUserData"] = x
			}
			if strings.HasPrefix(arn, "arn:") && parent != "" && token != "" {
				entry.endpoint = &PlatformEndpoint{ARN: arn, ApplicationARN: parent, AccountID: account, Token: token, Attributes: attrs, CreatedAt: created}
			}
		case "SMSSandboxPhoneNumber":
			phone := snsLegacyString(v, "PhoneNumber", "phoneNumber")
			if phone == "" && validateSandboxPhone(r.ID) == nil {
				phone = r.ID
			}
			if validateSandboxPhone(phone) == nil {
				entry.phone = &SandboxPhone{PhoneNumber: phone, AccountID: account, LanguageCode: snsLegacyString(v, "LanguageCode"), Status: "UNVERIFIED", CreatedAt: created, Consumed: true}
				entry.status = "normalized-unverified"
			}
		case "SMSAttribute":
			if len(attrs) > 0 {
				entry.settings = attrs
			}
		}
		if entry.endpoint != nil {
			endpointAccount, err := snsLegacyAccount(v, entry.endpoint.ARN)
			if err != nil {
				return nil, err
			}
			entry.endpoint.AccountID = endpointAccount
		}
		entries = append(entries, entry)
	}
	sort.SliceStable(entries, func(i, j int) bool { return snsLegacyOrder(entries[i]) < snsLegacyOrder(entries[j]) })
	return entries, nil
}
func snsLegacyOrder(e snsLegacyEntry) int {
	switch {
	case e.app != nil:
		return 0
	case e.endpoint != nil:
		return 1
	case e.phone != nil:
		return 2
	case e.settings != nil:
		return 3
	}
	return 4
}
func snsExistingIDs(tx *sql.Tx, query string) (map[string]bool, error) {
	rows, e := tx.Query(query)
	if e != nil {
		return nil, e
	}
	defer func() { _ = rows.Close() }()
	ids := map[string]bool{}
	for rows.Next() {
		var id string
		if e = rows.Scan(&id); e != nil {
			return nil, e
		}
		ids[id] = true
	}
	return ids, rows.Err()
}

func (s *SNSStore) ImportLegacy(records []crud.ResourceSnapshot) error {
	return s.withStateTx(func(tx *sql.Tx) error {
		pending := []crud.ResourceSnapshot{}
		for _, r := range records {
			var n int
			if e := tx.QueryRow(`SELECT COUNT(*) FROM sns_legacy_imports WHERE resource=? AND resource_id=?`, r.Resource, r.ID).Scan(&n); e != nil {
				return e
			}
			if n == 0 {
				pending = append(pending, r)
			}
		}
		entries, e := normalizeSNSLegacy(pending, time.Now().UTC())
		if e != nil {
			return e
		}
		apps, e := snsExistingIDs(tx, `SELECT arn FROM sns_platform_applications`)
		if e != nil {
			return e
		}
		endpoints, e := snsExistingIDs(tx, `SELECT arn FROM sns_platform_endpoints`)
		if e != nil {
			return e
		}
		phones, e := snsExistingIDs(tx, `SELECT account_id||':'||phone_number FROM sns_sandbox_phones`)
		if e != nil {
			return e
		}
		settings, e := snsExistingIDs(tx, `SELECT account_id FROM sns_sms_settings`)
		if e != nil {
			return e
		}
		merged := map[string]map[string]string{}
		for _, entry := range entries {
			switch {
			case entry.app != nil:
				v := entry.app
				old, e := snsDocument[PlatformApplication](tx, `SELECT document_json FROM sns_platform_applications WHERE arn=? OR (account_id=? AND platform=? AND name=?)`, ErrMobileApplicationNotFound, v.ARN, v.AccountID, v.Platform, v.Name)
				if e == nil {
					if apps[old.ARN] {
						entry.status = "canonical-preserved"
					} else if !reflect.DeepEqual(*old, *v) {
						return ErrSNSLegacyConflict
					} else {
						entry.status = "imported"
					}
				} else if !errors.Is(e, ErrMobileApplicationNotFound) {
					return e
				} else {
					raw, _ := json.Marshal(v)
					if _, e = tx.Exec(`INSERT INTO sns_platform_applications VALUES (?,?,?,?,?)`, v.ARN, v.AccountID, v.Name, v.Platform, raw); e != nil {
						return e
					}
					entry.status = "imported"
				}
			case entry.endpoint != nil:
				v := entry.endpoint
				old, e := snsDocument[PlatformEndpoint](tx, `SELECT document_json FROM sns_platform_endpoints WHERE arn=? OR (application_arn=? AND token=?)`, ErrMobileEndpointNotFound, v.ARN, v.ApplicationARN, v.Token)
				if e == nil {
					if endpoints[old.ARN] {
						entry.status = "canonical-preserved"
					} else if !reflect.DeepEqual(*old, *v) {
						return ErrSNSLegacyConflict
					} else {
						entry.status = "imported"
					}
				} else if !errors.Is(e, ErrMobileEndpointNotFound) {
					return e
				} else {
					app, e := snsDocument[PlatformApplication](tx, `SELECT document_json FROM sns_platform_applications WHERE arn=?`, ErrMobileApplicationNotFound, v.ApplicationARN)
					if errors.Is(e, ErrMobileApplicationNotFound) {
						break
					}
					if e != nil {
						return e
					}
					if app.AccountID != v.AccountID {
						return ErrSNSLegacyConflict
					}
					raw, _ := json.Marshal(v)
					if _, e = tx.Exec(`INSERT INTO sns_platform_endpoints VALUES (?,?,?,?,?)`, v.ARN, v.ApplicationARN, v.AccountID, v.Token, raw); e != nil {
						return e
					}
					entry.status = "imported"
				}
			case entry.phone != nil:
				v := entry.phone
				identity := v.AccountID + ":" + v.PhoneNumber
				if phones[identity] {
					entry.status = "canonical-preserved"
				} else {
					old, e := snsDocument[SandboxPhone](tx, `SELECT document_json FROM sns_sandbox_phones WHERE account_id=? AND phone_number=?`, ErrSandboxPhoneNotFound, v.AccountID, v.PhoneNumber)
					if e == nil {
						if !reflect.DeepEqual(*old, *v) {
							return ErrSNSLegacyConflict
						}
					} else if !errors.Is(e, ErrSandboxPhoneNotFound) {
						return e
					} else {
						raw, _ := json.Marshal(v)
						if _, e = tx.Exec(`INSERT INTO sns_sandbox_phones VALUES (?,?,?)`, v.AccountID, v.PhoneNumber, raw); e != nil {
							return e
						}
					}
				}
			case entry.settings != nil:
				if settings[entry.account] {
					entry.status = "canonical-preserved"
				} else {
					if merged[entry.account] == nil {
						merged[entry.account] = map[string]string{}
					}
					for k, v := range entry.settings {
						if old, ok := merged[entry.account][k]; ok && old != v {
							return ErrSNSLegacyConflict
						}
						merged[entry.account][k] = v
					}
					raw, _ := json.Marshal(merged[entry.account])
					if _, e = tx.Exec(`INSERT INTO sns_sms_settings VALUES (?,?) ON CONFLICT(account_id) DO UPDATE SET attributes_json=excluded.attributes_json`, entry.account, raw); e != nil {
						return e
					}
					entry.status = "imported"
				}
			}
			if _, e = tx.Exec(`INSERT INTO sns_legacy_imports VALUES (?,?,?,?)`, entry.record.Resource, entry.record.ID, []byte(entry.record.JSON), entry.status); e != nil {
				return e
			}
		}
		return nil
	})
}
