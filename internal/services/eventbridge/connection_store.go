// SPDX-License-Identifier: Apache-2.0
package eventbridge

import (
	"database/sql"
	"time"
)

type Connection struct {
	Name, AccountID, ARN, Description, AuthorizationType, State string
	AuthParameters, Metadata                                    map[string]any
	CreationTime, LastModifiedTime                              time.Time
	LastAuthorizedTime                                          *time.Time
}

func (s *EBStore) CreateConnection(c Connection) error {
	return s.withStateTx(func(tx *sql.Tx) error { return createDocument(tx, "connections", c.Name, c.AccountID, c) })
}
func (s *EBStore) GetConnection(name, accountID string) (*Connection, error) {
	return readDocument[Connection](s.store.DB(), "connections", name, accountID, ErrConnectionNotFound)
}
func (s *EBStore) ListConnections(accountID string) ([]Connection, error) {
	return listDocuments[Connection](s.store.DB(), "connections", accountID)
}
func (s *EBStore) UpdateConnection(name, accountID string, change func(*Connection) error) (*Connection, error) {
	var result *Connection
	err := s.withStateTx(func(tx *sql.Tx) error {
		c, err := readDocument[Connection](tx, "connections", name, accountID, ErrConnectionNotFound)
		if err != nil {
			return err
		}
		if err = change(c); err != nil {
			return err
		}
		c.Name = name
		c.AccountID = accountID
		if err = updateDocument(tx, "connections", name, accountID, c); err != nil {
			return err
		}
		result = c
		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}
func (s *EBStore) DeleteConnection(name, accountID string) (*Connection, error) {
	var result *Connection
	err := s.withStateTx(func(tx *sql.Tx) error {
		c, err := readDocument[Connection](tx, "connections", name, accountID, ErrConnectionNotFound)
		if err != nil {
			return err
		}
		if _, err = tx.Exec("DELETE FROM connections WHERE name=? AND account_id=?", name, accountID); err != nil {
			return err
		}
		result = c
		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}
