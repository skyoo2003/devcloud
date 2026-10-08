// SPDX-License-Identifier: Apache-2.0
package eventbridge

import (
	"database/sql"
	"errors"
	"time"
)

var (
	ErrSourceNotFound     = errors.New("event source not found")
	ErrConnectionNotFound = errors.New("connection not found")
	ErrAlreadyExists      = errors.New("resource already exists")
	ErrInvalidSourceState = errors.New("invalid event source state")
	ErrPermissionNotFound = errors.New("permission not found")
)

type PartnerSource struct {
	Name, AccountID, ARN, CreatedBy, State, BusName string
	CreationTime                                    time.Time
}

func (s *EBStore) CreatePartnerSource(source PartnerSource) error {
	return s.withStateTx(func(tx *sql.Tx) error {
		return createDocument(tx, "partner_sources", source.Name, source.AccountID, source)
	})
}
func (s *EBStore) GetPartnerSource(name, accountID string) (*PartnerSource, error) {
	return readDocument[PartnerSource](s.store.DB(), "partner_sources", name, accountID, ErrSourceNotFound)
}
func (s *EBStore) ListPartnerSources(accountID string) ([]PartnerSource, error) {
	return listDocuments[PartnerSource](s.store.DB(), "partner_sources", accountID)
}
func (s *EBStore) DeletePartnerSource(name, accountID string) error {
	return s.withStateTx(func(tx *sql.Tx) error {
		if _, err := readDocument[PartnerSource](tx, "partner_sources", name, accountID, ErrSourceNotFound); err != nil {
			return err
		}
		_, err := tx.Exec("DELETE FROM partner_sources WHERE name=? AND account_id=?", name, accountID)
		return err
	})
}
func (s *EBStore) CreatePartnerEventBus(name, sourceName, accountID string) error {
	return s.withStateTx(func(tx *sql.Tx) error {
		source, err := readDocument[PartnerSource](tx, "partner_sources", sourceName, accountID, ErrSourceNotFound)
		if err != nil {
			return err
		}
		if name != sourceName || source.BusName != "" {
			return ErrInvalidSourceState
		}
		var exists int
		err = tx.QueryRow("SELECT 1 FROM event_buses WHERE name=? AND account_id=?", name, accountID).Scan(&exists)
		if err == nil {
			return ErrAlreadyExists
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		if _, err = tx.Exec("INSERT INTO event_buses (name,arn,account_id) VALUES (?,?,?)", name, busARN(name, accountID), accountID); err != nil {
			return err
		}
		source.BusName = name
		source.State = "ACTIVE"
		return updateDocument(tx, "partner_sources", sourceName, accountID, source)
	})
}
func (s *EBStore) SetPartnerSourceState(name, accountID, state string) error {
	return s.withStateTx(func(tx *sql.Tx) error {
		source, err := readDocument[PartnerSource](tx, "partner_sources", name, accountID, ErrSourceNotFound)
		if err != nil {
			return err
		}
		if (state != "ACTIVE" && state != "PENDING") || source.BusName == "" {
			return ErrInvalidSourceState
		}
		var exists int
		err = tx.QueryRow("SELECT 1 FROM event_buses WHERE name=? AND account_id=?", source.BusName, accountID).Scan(&exists)
		if errors.Is(err, sql.ErrNoRows) {
			return ErrInvalidSourceState
		}
		if err != nil {
			return err
		}
		source.State = state
		return updateDocument(tx, "partner_sources", name, accountID, source)
	})
}
