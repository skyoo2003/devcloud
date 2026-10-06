// SPDX-License-Identifier: Apache-2.0
package eventbridge

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/skyoo2003/devcloud/internal/shared/crud"
	"math"
	"reflect"
	"regexp"
	"time"
)

var ErrLegacyConflict = errors.New("conflicting legacy EventBridge records")

type legacyPolicy struct {
	BusName, AccountID string
	Document           map[string]any
}
type legacyEntry struct {
	Raw        crud.ResourceSnapshot
	Status     string
	Source     *PartnerSource
	Connection *Connection
	Policy     *legacyPolicy
}

var partnerSourceNamePattern = regexp.MustCompile(`^aws\.partner(/[.\-_A-Za-z0-9]+){2,}$`)
var connectionNamePattern = regexp.MustCompile(`^[.\-_A-Za-z0-9]{1,64}$`)

func legacyString(doc map[string]any, fields ...string) string {
	for _, field := range fields {
		if value, ok := doc[field].(string); ok && value != "" {
			return value
		}
	}
	return ""
}

func legacyTime(doc map[string]any, field string, fallback time.Time) (time.Time, error) {
	value, exists := doc[field]
	if !exists {
		return fallback, nil
	}
	switch value := value.(type) {
	case string:
		parsed, err := time.Parse(time.RFC3339Nano, value)
		if err == nil {
			return parsed.UTC(), nil
		}
	case float64:
		if !math.IsNaN(value) && !math.IsInf(value, 0) && value >= 0 && value < 253402300800 {
			seconds, fraction := math.Modf(value)
			return time.Unix(int64(seconds), int64(fraction*1e9)).UTC(), nil
		}
	}
	return time.Time{}, fmt.Errorf("invalid legacy %s", field)
}

func normalizeLegacy(records []crud.ResourceSnapshot, now time.Time) ([]legacyEntry, error) {
	result := make([]legacyEntry, 0, len(records))
	identities := map[string]any{}
	for _, raw := range records {
		var doc map[string]any
		if err := json.Unmarshal(raw.JSON, &doc); err != nil {
			return nil, fmt.Errorf("legacy %s/%s: invalid JSON: %w", raw.Resource, raw.ID, err)
		}
		if doc == nil {
			return nil, fmt.Errorf("legacy %s/%s: expected an object", raw.Resource, raw.ID)
		}
		entry := legacyEntry{Raw: raw, Status: "imported"}
		identity := ""
		var normalized any
		account := legacyString(doc, "Account", "AccountID")
		if account == "" {
			account = defaultAccountID
		}
		switch raw.Resource {
		case "Connection":
			name := legacyString(doc, "Name", "ConnectionName")
			if name == "" {
				name = raw.ID
			}
			if !connectionNamePattern.MatchString(name) || account != defaultAccountID {
				entry.Status = "retained-unaddressable"
				break
			}
			created, err := legacyTime(doc, "CreationTime", now)
			if err != nil {
				return nil, err
			}
			modified, err := legacyTime(doc, "LastModifiedTime", created)
			if err != nil {
				return nil, err
			}
			c := Connection{Name: name, AccountID: account, ARN: legacyString(doc, "ConnectionArn", "Arn"), Description: legacyString(doc, "Description"), AuthorizationType: legacyString(doc, "AuthorizationType"), State: legacyString(doc, "ConnectionState", "State"), CreationTime: created, LastModifiedTime: modified, Metadata: map[string]any{}}
			if c.ARN == "" {
				c.ARN = "arn:aws:events:us-east-1:" + account + ":connection/" + name + "/" + name
			}
			if c.State == "" {
				c.State = "DEAUTHORIZED"
			}
			c.AuthParameters, _ = doc["AuthParameters"].(map[string]any)
			for _, key := range []string{"KmsKeyIdentifier", "InvocationConnectivityParameters"} {
				if value, ok := doc[key]; ok {
					c.Metadata[key] = value
				}
			}
			if _, ok := doc["LastAuthorizedTime"]; ok {
				authorized, err := legacyTime(doc, "LastAuthorizedTime", now)
				if err != nil {
					return nil, err
				}
				c.LastAuthorizedTime = &authorized
			}
			entry.Connection = &c
			identity = "connection/" + account + "/" + name
			normalized = c
		case "PartnerEventSource", "EventSource", "PartnerEventSourceAccount":
			name := legacyString(doc, "Name", "EventSourceName", "PartnerEventSourceName")
			if name == "" {
				name = raw.ID
			}
			if len(name) > 256 || !partnerSourceNamePattern.MatchString(name) || account != defaultAccountID {
				entry.Status = "retained-unaddressable"
				break
			}
			created, err := legacyTime(doc, "CreationTime", now)
			if err != nil {
				return nil, err
			}
			source := PartnerSource{Name: name, AccountID: account, ARN: legacyString(doc, "EventSourceArn", "PartnerEventSourceArn", "Arn"), CreatedBy: legacyString(doc, "CreatedBy"), State: legacyString(doc, "State"), CreationTime: created}
			if source.ARN == "" {
				source.ARN = "arn:aws:events:us-east-1:" + account + ":event-source/" + name
			}
			if source.CreatedBy == "" {
				source.CreatedBy = account
			}
			if source.State == "" {
				source.State = "PENDING"
			}
			entry.Source = &source
			identity = "source/" + account + "/" + name
			normalized = source
		case "Permission":
			bus, err := permissionBusName(doc)
			if err != nil || account != defaultAccountID {
				entry.Status = "retained-ambiguous-policy"
				break
			}
			change, err := permissionChange(doc)
			if err != nil {
				entry.Status = "retained-ambiguous-policy"
				break
			}
			policy, err := change(nil)
			if err != nil {
				entry.Status = "retained-ambiguous-policy"
				break
			}
			for _, value := range policy["Statement"].([]any) {
				statement := value.(map[string]any)
				key := "policy/" + account + "/" + bus + "/" + statement["Sid"].(string)
				if earlier, ok := identities[key]; ok && !reflect.DeepEqual(earlier, statement) {
					return nil, ErrLegacyConflict
				}
				identities[key] = statement
			}
			entry.Policy = &legacyPolicy{BusName: bus, AccountID: account, Document: policy}
		default:
			entry.Status = "retained-unaddressable"
		}
		if identity != "" {
			if earlier, ok := identities[identity]; ok && !reflect.DeepEqual(earlier, normalized) {
				return nil, ErrLegacyConflict
			}
			identities[identity] = normalized
		}
		result = append(result, entry)
	}
	return result, nil
}

func (s *EBStore) ImportLegacy(records []crud.ResourceSnapshot) error {
	return s.withStateTx(func(tx *sql.Tx) error {
		pending := make([]crud.ResourceSnapshot, 0, len(records))
		for _, row := range records {
			var exists int
			err := tx.QueryRow("SELECT 1 FROM legacy_imports WHERE resource=? AND resource_id=?", row.Resource, row.ID).Scan(&exists)
			if err == nil {
				continue
			}
			if !errors.Is(err, sql.ErrNoRows) {
				return err
			}
			pending = append(pending, row)
		}
		entries, err := normalizeLegacy(pending, time.Now().UTC())
		if err != nil {
			return err
		}
		canonicalPolicies := map[string]bool{}
		for _, entry := range entries {
			if entry.Policy == nil {
				continue
			}
			policy := entry.Policy
			key := policy.AccountID + "/" + policy.BusName
			if _, seen := canonicalPolicies[key]; seen {
				continue
			}
			current, err := readBusPolicy(tx, policy.BusName, policy.AccountID)
			if err != nil && !errors.Is(err, ErrBusNotFound) {
				return err
			}
			canonicalPolicies[key] = current != nil
		}
		for _, entry := range entries {
			switch {
			case entry.Source != nil:
				source := entry.Source
				_, err := readDocument[PartnerSource](tx, "partner_sources", source.Name, source.AccountID, ErrSourceNotFound)
				if err == nil {
					entry.Status = "canonical-preserved"
				} else if errors.Is(err, ErrSourceNotFound) {
					var exists int
					busErr := tx.QueryRow("SELECT 1 FROM event_buses WHERE name=? AND account_id=?", source.Name, source.AccountID).Scan(&exists)
					if busErr == nil {
						source.BusName = source.Name
					} else if !errors.Is(busErr, sql.ErrNoRows) {
						return busErr
					} else if source.State == "ACTIVE" {
						source.State = "PENDING"
						entry.Status = "normalized-pending"
					}
					if err := createDocument(tx, "partner_sources", source.Name, source.AccountID, source); err != nil {
						return err
					}
				} else {
					return err
				}
			case entry.Connection != nil:
				c := entry.Connection
				_, err := readDocument[Connection](tx, "connections", c.Name, c.AccountID, ErrConnectionNotFound)
				if err == nil {
					entry.Status = "canonical-preserved"
				} else if errors.Is(err, ErrConnectionNotFound) {
					if err := createDocument(tx, "connections", c.Name, c.AccountID, c); err != nil {
						return err
					}
				} else {
					return err
				}
			case entry.Policy != nil:
				p := entry.Policy
				current, err := readBusPolicy(tx, p.BusName, p.AccountID)
				if errors.Is(err, ErrBusNotFound) {
					entry.Status = "retained-ambiguous-policy"
				} else if err != nil {
					return err
				} else if canonicalPolicies[p.AccountID+"/"+p.BusName] {
					entry.Status = "canonical-preserved"
				} else {
					raw, err := json.Marshal(p.Document)
					if err != nil {
						return err
					}
					merge, err := permissionChange(map[string]any{"EventBusName": p.BusName, "Policy": string(raw)})
					if err != nil {
						return err
					}
					combined, err := merge(current)
					if err != nil {
						return err
					}
					raw, err = json.Marshal(combined)
					if err != nil {
						return err
					}
					if _, err = tx.Exec("INSERT INTO bus_policies (bus_name,account_id,policy_json) VALUES (?,?,?) ON CONFLICT(bus_name,account_id) DO UPDATE SET policy_json=excluded.policy_json", p.BusName, p.AccountID, raw); err != nil {
						return err
					}
				}
			}
			if _, err := tx.Exec("INSERT INTO legacy_imports (resource,resource_id,document_json,status) VALUES (?,?,?,?)", entry.Raw.Resource, entry.Raw.ID, []byte(entry.Raw.JSON), entry.Status); err != nil {
				return err
			}
		}
		return nil
	})
}
