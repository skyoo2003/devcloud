// SPDX-License-Identifier: Apache-2.0
package sns

import (
	"database/sql"
	"encoding/json"
	"errors"
	"maps"
	"time"
)

type PlatformApplication struct {
	ARN, Name, Platform, AccountID string
	Attributes                     map[string]string
	CreatedAt                      time.Time
}
type PlatformEndpoint struct {
	ARN, ApplicationARN, AccountID, Token string
	Attributes                            map[string]string
	CreatedAt                             time.Time
}

func snsDocument[T any](q snsQuerier, query string, notFound error, args ...any) (*T, error) {
	var raw []byte
	e := q.QueryRow(query, args...).Scan(&raw)
	if errors.Is(e, sql.ErrNoRows) {
		return nil, notFound
	}
	if e != nil {
		return nil, e
	}
	var v T
	if e = json.Unmarshal(raw, &v); e != nil {
		return nil, e
	}
	return &v, nil
}
func listSNSDocuments[T any](rows *sql.Rows, e error) ([]T, error) {
	if e != nil {
		return nil, e
	}
	defer func() { _ = rows.Close() }()
	items := []T{}
	for rows.Next() {
		var raw []byte
		if e = rows.Scan(&raw); e != nil {
			return nil, e
		}
		var v T
		if e = json.Unmarshal(raw, &v); e != nil {
			return nil, e
		}
		items = append(items, v)
	}
	return items, rows.Err()
}
func compatibleSNSAttributes(a, b map[string]string) bool {
	for k, v := range b {
		if a[k] != v {
			return false
		}
	}
	return true
}

func changedSNSAttributes(before, after map[string]string) map[string]string {
	changed := map[string]string{}
	for k, v := range after {
		if old, ok := before[k]; !ok || old != v {
			changed[k] = v
		}
	}
	return changed
}

func (s *SNSStore) CreatePlatformApplication(v PlatformApplication) (*PlatformApplication, error) {
	var out *PlatformApplication
	e := s.withStateTx(func(tx *sql.Tx) error {
		old, e := snsDocument[PlatformApplication](tx, `SELECT document_json FROM sns_platform_applications WHERE account_id=? AND platform=? AND name=?`, ErrMobileApplicationNotFound, v.AccountID, v.Platform, v.Name)
		if e == nil {
			if !compatibleSNSAttributes(old.Attributes, v.Attributes) {
				return ErrSNSConflict
			}
			out = old
			return nil
		}
		if !errors.Is(e, ErrMobileApplicationNotFound) {
			return e
		}
		if v.CreatedAt.IsZero() {
			v.CreatedAt = time.Now().UTC()
		}
		if v.Attributes == nil {
			v.Attributes = map[string]string{}
		}
		raw, e := json.Marshal(v)
		if e != nil {
			return e
		}
		_, e = tx.Exec(`INSERT INTO sns_platform_applications VALUES (?,?,?,?,?)`, v.ARN, v.AccountID, v.Name, v.Platform, raw)
		if e == nil {
			out = &v
		}
		return e
	})
	return out, e
}
func (s *SNSStore) GetPlatformApplication(arn string) (*PlatformApplication, error) {
	return snsDocument[PlatformApplication](s.store.DB(), `SELECT document_json FROM sns_platform_applications WHERE arn=?`, ErrMobileApplicationNotFound, arn)
}
func (s *SNSStore) ListPlatformApplications(accountID string) ([]PlatformApplication, error) {
	rows, e := s.store.DB().Query(`SELECT document_json FROM sns_platform_applications WHERE account_id=? ORDER BY arn`, accountID)
	return listSNSDocuments[PlatformApplication](rows, e)
}
func (s *SNSStore) UpdatePlatformApplication(arn string, change func(*PlatformApplication) error) (*PlatformApplication, error) {
	var out *PlatformApplication
	e := s.withStateTx(func(tx *sql.Tx) error {
		v, e := snsDocument[PlatformApplication](tx, `SELECT document_json FROM sns_platform_applications WHERE arn=?`, ErrMobileApplicationNotFound, arn)
		if e != nil {
			return e
		}
		before := maps.Clone(v.Attributes)
		if e = change(v); e != nil {
			return e
		}
		if e = validateApplicationAttributes(changedSNSAttributes(before, v.Attributes), false); e != nil {
			return e
		}
		raw, e := json.Marshal(v)
		if e != nil {
			return e
		}
		_, e = tx.Exec(`UPDATE sns_platform_applications SET document_json=? WHERE arn=?`, raw, arn)
		if e == nil {
			out = v
		}
		return e
	})
	return out, e
}
func (s *SNSStore) DeletePlatformApplication(arn string) error {
	return s.withStateTx(func(tx *sql.Tx) error {
		_, e := tx.Exec(`DELETE FROM sns_platform_endpoints WHERE application_arn=?`, arn)
		if e != nil {
			return e
		}
		_, e = tx.Exec(`DELETE FROM sns_platform_applications WHERE arn=?`, arn)
		return e
	})
}

func (s *SNSStore) CreatePlatformEndpoint(v PlatformEndpoint) (*PlatformEndpoint, error) {
	var out *PlatformEndpoint
	e := s.withStateTx(func(tx *sql.Tx) error {
		_, e := snsDocument[PlatformApplication](tx, `SELECT document_json FROM sns_platform_applications WHERE arn=?`, ErrMobileApplicationNotFound, v.ApplicationARN)
		if e != nil {
			return e
		}
		old, e := snsDocument[PlatformEndpoint](tx, `SELECT document_json FROM sns_platform_endpoints WHERE application_arn=? AND token=?`, ErrMobileEndpointNotFound, v.ApplicationARN, v.Token)
		if e == nil {
			if !compatibleSNSAttributes(old.Attributes, v.Attributes) {
				return ErrSNSConflict
			}
			out = old
			return nil
		}
		if !errors.Is(e, ErrMobileEndpointNotFound) {
			return e
		}
		if v.CreatedAt.IsZero() {
			v.CreatedAt = time.Now().UTC()
		}
		raw, e := json.Marshal(v)
		if e != nil {
			return e
		}
		_, e = tx.Exec(`INSERT INTO sns_platform_endpoints VALUES (?,?,?,?,?)`, v.ARN, v.ApplicationARN, v.AccountID, v.Token, raw)
		if e == nil {
			out = &v
		}
		return e
	})
	return out, e
}
func (s *SNSStore) GetPlatformEndpoint(arn string) (*PlatformEndpoint, error) {
	return snsDocument[PlatformEndpoint](s.store.DB(), `SELECT document_json FROM sns_platform_endpoints WHERE arn=?`, ErrMobileEndpointNotFound, arn)
}
func (s *SNSStore) ListPlatformEndpoints(parent string) ([]PlatformEndpoint, error) {
	if _, e := s.GetPlatformApplication(parent); e != nil {
		return nil, e
	}
	rows, e := s.store.DB().Query(`SELECT document_json FROM sns_platform_endpoints WHERE application_arn=? ORDER BY arn`, parent)
	return listSNSDocuments[PlatformEndpoint](rows, e)
}
func (s *SNSStore) UpdatePlatformEndpoint(arn string, change func(*PlatformEndpoint) error) (*PlatformEndpoint, error) {
	var out *PlatformEndpoint
	e := s.withStateTx(func(tx *sql.Tx) error {
		v, e := snsDocument[PlatformEndpoint](tx, `SELECT document_json FROM sns_platform_endpoints WHERE arn=?`, ErrMobileEndpointNotFound, arn)
		if e != nil {
			return e
		}
		before := maps.Clone(v.Attributes)
		if e = change(v); e != nil {
			return e
		}
		if e = validateEndpointAttributes(changedSNSAttributes(before, v.Attributes), false); e != nil {
			return e
		}
		v.Token = v.Attributes["Token"]
		var other string
		e = tx.QueryRow(`SELECT arn FROM sns_platform_endpoints WHERE application_arn=? AND token=? AND arn<>?`, v.ApplicationARN, v.Token, arn).Scan(&other)
		if e == nil {
			return ErrSNSConflict
		}
		if !errors.Is(e, sql.ErrNoRows) {
			return e
		}
		raw, e := json.Marshal(v)
		if e != nil {
			return e
		}
		_, e = tx.Exec(`UPDATE sns_platform_endpoints SET token=?,document_json=? WHERE arn=?`, v.Token, raw, arn)
		if e == nil {
			out = v
		}
		return e
	})
	return out, e
}
func (s *SNSStore) DeletePlatformEndpoint(arn string) error {
	return s.withStateTx(func(tx *sql.Tx) error {
		_, e := tx.Exec(`DELETE FROM sns_platform_endpoints WHERE arn=?`, arn)
		return e
	})
}
