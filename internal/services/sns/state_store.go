// SPDX-License-Identifier: Apache-2.0
package sns

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
)

var (
	ErrMobileApplicationNotFound = errors.New("platform application not found")
	ErrMobileEndpointNotFound    = errors.New("platform endpoint not found")
	ErrSandboxPhoneNotFound      = errors.New("sandbox phone not found")
	ErrSNSInvalidParameter       = errors.New("invalid parameter")
	ErrSNSConflict               = errors.New("resource conflict")
	ErrOTPVerification           = errors.New("OTP verification failed")
)

func invalidSNS(message string) error { return fmt.Errorf("%w: %s", ErrSNSInvalidParameter, message) }

func (s *SNSStore) withStateTx(change func(*sql.Tx) error) error {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	tx, e := s.store.DB().Begin()
	if e != nil {
		return e
	}
	defer func() { _ = tx.Rollback() }()
	if e = change(tx); e != nil {
		return e
	}
	return tx.Commit()
}

type snsQuerier interface{ QueryRow(string, ...any) *sql.Row }

func smsSettings(q snsQuerier, accountID string) (map[string]string, error) {
	attrs := map[string]string{"DefaultSMSType": "Promotional", "DeliveryStatusSuccessSamplingRate": "0"}
	var raw []byte
	e := q.QueryRow(`SELECT attributes_json FROM sns_sms_settings WHERE account_id=?`, accountID).Scan(&raw)
	if errors.Is(e, sql.ErrNoRows) {
		return attrs, nil
	}
	if e != nil {
		return nil, e
	}
	var stored map[string]string
	if e = json.Unmarshal(raw, &stored); e != nil {
		return nil, e
	}
	for k, v := range stored {
		attrs[k] = v
	}
	return attrs, nil
}

func (s *SNSStore) GetSMSAttributes(accountID string, names []string) (map[string]string, error) {
	attrs, e := smsSettings(s.store.DB(), accountID)
	if e != nil {
		return nil, e
	}
	if len(names) == 0 {
		return attrs, nil
	}
	filtered := map[string]string{}
	for _, k := range names {
		if v, ok := attrs[k]; ok {
			filtered[k] = v
		}
	}
	return filtered, nil
}

func (s *SNSStore) SetSMSAttributes(accountID string, attributes map[string]string) error {
	if e := validateSMSAttributes(attributes); e != nil {
		return e
	}
	return s.withStateTx(func(tx *sql.Tx) error {
		attrs, e := smsSettings(tx, accountID)
		if e != nil {
			return e
		}
		for k, v := range attributes {
			attrs[k] = v
		}
		raw, e := json.Marshal(attrs)
		if e != nil {
			return e
		}
		_, e = tx.Exec(`INSERT INTO sns_sms_settings VALUES (?,?) ON CONFLICT(account_id) DO UPDATE SET attributes_json=excluded.attributes_json`, accountID, raw)
		return e
	})
}

func (s *SNSStore) OptInPhoneNumber(phone, accountID string) error {
	if !e164Phone.MatchString(phone) {
		return invalidSNS("phoneNumber must be E.164")
	}
	return s.withStateTx(func(tx *sql.Tx) error {
		_, e := tx.Exec(`DELETE FROM sms_opt_outs WHERE phone_number=? AND account_id=?`, phone, accountID)
		return e
	})
}

func (s *SNSStore) IsPhoneOptedOut(phone, accountID string) (bool, error) {
	var n int
	e := s.store.DB().QueryRow(`SELECT COUNT(*) FROM sms_opt_outs WHERE phone_number=? AND account_id=?`, phone, accountID).Scan(&n)
	return n != 0, e
}

func (s *SNSStore) ListOptedOutPhones(accountID string) ([]string, error) {
	rows, e := s.store.DB().Query(`SELECT phone_number FROM sms_opt_outs WHERE account_id=? ORDER BY phone_number`, accountID)
	if e != nil {
		return nil, e
	}
	defer func() { _ = rows.Close() }()
	phones := []string{}
	for rows.Next() {
		var phone string
		if e = rows.Scan(&phone); e != nil {
			return nil, e
		}
		phones = append(phones, phone)
	}
	return phones, rows.Err()
}
