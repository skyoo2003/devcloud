// SPDX-License-Identifier: Apache-2.0

package shared

import (
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

func NewResourceStore[T any](db *sqlite.Store, table, idCol, cols string, scanner func(Scanner) (T, error)) *ResourceStore[T] {
	return &ResourceStore[T]{db: db, table: table, idCol: idCol, cols: cols, scanner: scanner}
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
