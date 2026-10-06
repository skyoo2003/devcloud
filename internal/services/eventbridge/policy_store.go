// SPDX-License-Identifier: Apache-2.0
package eventbridge

import (
	"database/sql"
	"encoding/json"
	"errors"
)

func readBusPolicy(db stateReader, busName, accountID string) (map[string]any, error) {
	var exists int
	err := db.QueryRow("SELECT 1 FROM event_buses WHERE name=? AND account_id=?", busName, accountID).Scan(&exists)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrBusNotFound
	}
	if err != nil {
		return nil, err
	}
	var raw []byte
	err = db.QueryRow("SELECT policy_json FROM bus_policies WHERE bus_name=? AND account_id=?", busName, accountID).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var policy map[string]any
	err = json.Unmarshal(raw, &policy)
	return policy, err
}
func (s *EBStore) GetBusPolicy(busName, accountID string) (map[string]any, error) {
	tx, err := s.store.DB().Begin()
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()
	policy, err := readBusPolicy(tx, busName, accountID)
	if err != nil {
		return nil, err
	}
	if err = tx.Commit(); err != nil {
		return nil, err
	}
	return policy, nil
}
func (s *EBStore) UpdateBusPolicy(busName, accountID string, change func(map[string]any) (map[string]any, error)) error {
	return s.withStateTx(func(tx *sql.Tx) error {
		policy, err := readBusPolicy(tx, busName, accountID)
		if err != nil {
			return err
		}
		policy, err = change(policy)
		if err != nil {
			return err
		}
		if policy == nil {
			_, err = tx.Exec("DELETE FROM bus_policies WHERE bus_name=? AND account_id=?", busName, accountID)
			return err
		}
		raw, err := json.Marshal(policy)
		if err != nil {
			return err
		}
		_, err = tx.Exec("INSERT INTO bus_policies (bus_name,account_id,policy_json) VALUES (?,?,?) ON CONFLICT(bus_name,account_id) DO UPDATE SET policy_json=excluded.policy_json", busName, accountID, raw)
		return err
	})
}
