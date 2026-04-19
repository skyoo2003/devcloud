// SPDX-License-Identifier: Apache-2.0

package shared

import (
	"fmt"
	"strings"

	"github.com/skyoo2003/devcloud/internal/storage/sqlite"
)

type Scanner interface {
	Scan(dest ...any) error
}

type ResourceStore[T any] struct {
	db      *sqlite.Store
	table   string
	idCol   string
	cols    string
	scanner func(Scanner) (T, error)
}

func NewResourceStore[T any](db *sqlite.Store, table, idCol, cols string, scanner func(Scanner) (T, error)) (*ResourceStore[T], error) {
	if err := validateIdentifier(table, "table"); err != nil {
		return nil, err
	}
	if err := validateIdentifier(idCol, "idCol"); err != nil {
		return nil, err
	}
	for _, c := range strings.Split(cols, ",") {
		if err := validateIdentifier(strings.TrimSpace(c), "col"); err != nil {
			return nil, err
		}
	}
	return &ResourceStore[T]{db: db, table: table, idCol: idCol, cols: cols, scanner: scanner}, nil
}

func validateIdentifier(s, kind string) error {
	if len(s) == 0 {
		return fmt.Errorf("shared: empty %s identifier", kind)
	}
	for _, r := range s {
		isLower := r >= 'a' && r <= 'z'
		isUpper := r >= 'A' && r <= 'Z'
		isDigit := r >= '0' && r <= '9'
		isUnderscore := r == '_'
		if !isLower && !isUpper && !isDigit && !isUnderscore {
			return fmt.Errorf("shared: invalid %s identifier: %q", kind, s)
		}
	}
	return nil
}

func (s *ResourceStore[T]) DB() *sqlite.Store { return s.db }

func (s *ResourceStore[T]) Get(id string) (T, error) {
	row := s.db.DB().QueryRow("SELECT "+s.cols+" FROM "+s.table+" WHERE "+s.idCol+" = ?", id)
	return s.scanner(row)
}

func (s *ResourceStore[T]) List(where string, args []any) ([]T, error) {
	query := "SELECT " + s.cols + " FROM " + s.table
	if where != "" {
		query += " WHERE " + where
	}
	rows, err := s.db.DB().Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var items []T
	for rows.Next() {
		item, err := s.scanner(rows)
		if err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

func (s *ResourceStore[T]) Delete(id string) (bool, error) {
	res, err := s.db.DB().Exec("DELETE FROM "+s.table+" WHERE "+s.idCol+" = ?", id)
	if err != nil {
		return false, err
	}
	n, _ := res.RowsAffected()
	return n > 0, nil
}

func (s *ResourceStore[T]) Count(where string, args []any) (int, error) {
	query := "SELECT COUNT(*) FROM " + s.table
	if where != "" {
		query += " WHERE " + where
	}
	var n int
	err := s.db.DB().QueryRow(query, args...).Scan(&n)
	return n, err
}

func (s *ResourceStore[T]) Exec(query string, args ...any) error {
	_, err := s.db.DB().Exec(query, args...)
	return err
}
