// SPDX-License-Identifier: Apache-2.0

// internal/services/lakeformation/store.go
package lakeformation

import (
	"database/sql"
	"errors"
	"path/filepath"
	"time"

	"github.com/skyoo2003/devcloud/internal/storage/sqlite"
)

var (
	errNotFound = errors.New("not found")
)

var migrations = []sqlite.Migration{
	{Version: 1, SQL: `
		CREATE TABLE IF NOT EXISTS lf_tags (
			catalog_id  TEXT NOT NULL DEFAULT '000000000000',
			key         TEXT NOT NULL,
			values_json TEXT NOT NULL DEFAULT '[]',
			PRIMARY KEY (catalog_id, key)
		);
		CREATE TABLE IF NOT EXISTS resources (
			arn           TEXT PRIMARY KEY,
			role_arn      TEXT NOT NULL DEFAULT '',
			last_modified INTEGER NOT NULL
		);
		CREATE TABLE IF NOT EXISTS permissions (
			id               TEXT PRIMARY KEY,
			principal        TEXT NOT NULL,
			resource_json    TEXT NOT NULL DEFAULT '{}',
			permissions_json TEXT NOT NULL DEFAULT '[]',
			grant_option     TEXT NOT NULL DEFAULT '[]'
		);
		CREATE TABLE IF NOT EXISTS settings (
			catalog_id TEXT PRIMARY KEY DEFAULT '000000000000',
			admins     TEXT NOT NULL DEFAULT '[]',
			config     TEXT NOT NULL DEFAULT '{}'
		);
		CREATE TABLE IF NOT EXISTS data_cells_filters (
			name          TEXT NOT NULL,
			database_name TEXT NOT NULL,
			table_name    TEXT NOT NULL,
			column_names  TEXT NOT NULL DEFAULT '[]',
			row_filter    TEXT NOT NULL DEFAULT '{}',
			PRIMARY KEY (name, database_name, table_name)
		);
		CREATE TABLE IF NOT EXISTS lf_tag_expressions (
			name        TEXT PRIMARY KEY,
			catalog_id  TEXT NOT NULL DEFAULT '000000000000',
			description TEXT NOT NULL DEFAULT '',
			expression  TEXT NOT NULL DEFAULT '[]'
		);
	`},
}

// LFTag represents a Lake Formation tag.
type LFTag struct {
	CatalogID  string
	Key        string
	ValuesJSON string
}

// Resource represents a registered Lake Formation resource.
type Resource struct {
	ARN          string
	RoleARN      string
	LastModified time.Time
}

// Permission represents a Lake Formation permission grant.
type Permission struct {
	ID              string
	Principal       string
	ResourceJSON    string
	PermissionsJSON string
	GrantOption     string
}

// Settings represents data lake settings for a catalog.
type Settings struct {
	CatalogID string
	Admins    string
	Config    string
}

// DataCellsFilter represents a data cells filter.
type DataCellsFilter struct {
	Name         string
	DatabaseName string
	TableName    string
	ColumnNames  string
	RowFilter    string
}

// LFTagExpression represents a Lake Formation tag expression.
type LFTagExpression struct {
	Name        string
	CatalogID   string
	Description string
	Expression  string
}

// Store wraps the SQLite backend.
type Store struct {
	store *sqlite.Store
}

func NewStore(dataDir string) (*Store, error) {
	dbPath := filepath.Join(dataDir, "lakeformation.db")
	s, err := sqlite.Open(dbPath, migrations)
	if err != nil {
		return nil, err
	}
	return &Store{store: s}, nil
}

func (s *Store) Close() error { return s.store.Close() }

// ---- LFTag ----

func (s *Store) CreateLFTag(catalogID, key, valuesJSON string) error {
	_, err := s.store.DB().Exec(
		`INSERT INTO lf_tags (catalog_id, key, values_json) VALUES (?, ?, ?)`,
		catalogID, key, valuesJSON,
	)
	return err
}

func (s *Store) GetLFTag(catalogID, key string) (*LFTag, error) {
	row := s.store.DB().QueryRow(
		`SELECT catalog_id, key, values_json FROM lf_tags WHERE catalog_id = ? AND key = ?`,
		catalogID, key,
	)
	var t LFTag
	if err := row.Scan(&t.CatalogID, &t.Key, &t.ValuesJSON); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errNotFound
		}
		return nil, err
	}
	return &t, nil
}

func (s *Store) UpdateLFTag(catalogID, key, valuesJSON string) error {
	res, err := s.store.DB().Exec(
		`UPDATE lf_tags SET values_json = ? WHERE catalog_id = ? AND key = ?`,
		valuesJSON, catalogID, key,
	)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errNotFound
	}
	return nil
}

func (s *Store) DeleteLFTag(catalogID, key string) error {
	res, err := s.store.DB().Exec(
		`DELETE FROM lf_tags WHERE catalog_id = ? AND key = ?`, catalogID, key,
	)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errNotFound
	}
	return nil
}

func (s *Store) ListLFTags(catalogID string) ([]LFTag, error) {
	rows, err := s.store.DB().Query(
		`SELECT catalog_id, key, values_json FROM lf_tags WHERE catalog_id = ? ORDER BY key`,
		catalogID,
	)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var tags []LFTag
	for rows.Next() {
		var t LFTag
		if err := rows.Scan(&t.CatalogID, &t.Key, &t.ValuesJSON); err != nil {
			return nil, err
		}
		tags = append(tags, t)
	}
	return tags, rows.Err()
}

// ---- LFTagExpression ----

func (s *Store) CreateLFTagExpression(name, catalogID, description, expression string) error {
	_, err := s.store.DB().Exec(
		`INSERT INTO lf_tag_expressions (name, catalog_id, description, expression) VALUES (?, ?, ?, ?)`,
		name, catalogID, description, expression,
	)
	return err
}

func (s *Store) GetLFTagExpression(name string) (*LFTagExpression, error) {
	row := s.store.DB().QueryRow(
		`SELECT name, catalog_id, description, expression FROM lf_tag_expressions WHERE name = ?`, name,
	)
	var e LFTagExpression
	if err := row.Scan(&e.Name, &e.CatalogID, &e.Description, &e.Expression); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errNotFound
		}
		return nil, err
	}
	return &e, nil
}

func (s *Store) UpdateLFTagExpression(name, description, expression string) error {
	res, err := s.store.DB().Exec(
		`UPDATE lf_tag_expressions SET description = ?, expression = ? WHERE name = ?`,
		description, expression, name,
	)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errNotFound
	}
	return nil
}

func (s *Store) DeleteLFTagExpression(name string) error {
	res, err := s.store.DB().Exec(`DELETE FROM lf_tag_expressions WHERE name = ?`, name)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errNotFound
	}
	return nil
}

func (s *Store) ListLFTagExpressions(catalogID string) ([]LFTagExpression, error) {
	rows, err := s.store.DB().Query(
		`SELECT name, catalog_id, description, expression FROM lf_tag_expressions WHERE catalog_id = ? ORDER BY name`,
		catalogID,
	)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var exprs []LFTagExpression
	for rows.Next() {
		var e LFTagExpression
		if err := rows.Scan(&e.Name, &e.CatalogID, &e.Description, &e.Expression); err != nil {
			return nil, err
		}
		exprs = append(exprs, e)
	}
	return exprs, rows.Err()
}

// ---- Resource ----

func (s *Store) RegisterResource(arn, roleARN string) error {
	now := time.Now().Unix()
	_, err := s.store.DB().Exec(
		`INSERT INTO resources (arn, role_arn, last_modified) VALUES (?, ?, ?)`,
		arn, roleARN, now,
	)
	return err
}

func (s *Store) GetResource(arn string) (*Resource, error) {
	row := s.store.DB().QueryRow(
		`SELECT arn, role_arn, last_modified FROM resources WHERE arn = ?`, arn,
	)
	var r Resource
	var lastMod int64
	if err := row.Scan(&r.ARN, &r.RoleARN, &lastMod); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errNotFound
		}
		return nil, err
	}
	r.LastModified = time.Unix(lastMod, 0)
	return &r, nil
}

func (s *Store) DeregisterResource(arn string) error {
	res, err := s.store.DB().Exec(`DELETE FROM resources WHERE arn = ?`, arn)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errNotFound
	}
	return nil
}

func (s *Store) UpdateResource(arn, roleARN string) error {
	now := time.Now().Unix()
	res, err := s.store.DB().Exec(
		`UPDATE resources SET role_arn = ?, last_modified = ? WHERE arn = ?`,
		roleARN, now, arn,
	)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errNotFound
	}
	return nil
}

func (s *Store) ListResources() ([]Resource, error) {
	rows, err := s.store.DB().Query(
		`SELECT arn, role_arn, last_modified FROM resources ORDER BY arn`,
	)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var resources []Resource
	for rows.Next() {
		var r Resource
		var lastMod int64
		if err := rows.Scan(&r.ARN, &r.RoleARN, &lastMod); err != nil {
			return nil, err
		}
		r.LastModified = time.Unix(lastMod, 0)
		resources = append(resources, r)
	}
	return resources, rows.Err()
}

// ---- Permission ----

func (s *Store) GrantPermission(id, principal, resourceJSON, permissionsJSON, grantOption string) error {
	_, err := s.store.DB().Exec(
		`INSERT INTO permissions (id, principal, resource_json, permissions_json, grant_option) VALUES (?, ?, ?, ?, ?)`,
		id, principal, resourceJSON, permissionsJSON, grantOption,
	)
	return err
}

func (s *Store) GetPermission(id string) (*Permission, error) {
	row := s.store.DB().QueryRow(
		`SELECT id, principal, resource_json, permissions_json, grant_option FROM permissions WHERE id = ?`, id,
	)
	var p Permission
	if err := row.Scan(&p.ID, &p.Principal, &p.ResourceJSON, &p.PermissionsJSON, &p.GrantOption); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errNotFound
		}
		return nil, err
	}
	return &p, nil
}

func (s *Store) RevokePermission(id string) error {
	res, err := s.store.DB().Exec(`DELETE FROM permissions WHERE id = ?`, id)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errNotFound
	}
	return nil
}

func (s *Store) ListPermissions() ([]Permission, error) {
	rows, err := s.store.DB().Query(
		`SELECT id, principal, resource_json, permissions_json, grant_option FROM permissions ORDER BY id`,
	)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var perms []Permission
	for rows.Next() {
		var p Permission
		if err := rows.Scan(&p.ID, &p.Principal, &p.ResourceJSON, &p.PermissionsJSON, &p.GrantOption); err != nil {
			return nil, err
		}
		perms = append(perms, p)
	}
	return perms, rows.Err()
}

// ---- Settings ----

func (s *Store) GetSettings(catalogID string) (*Settings, error) {
	row := s.store.DB().QueryRow(
		`SELECT catalog_id, admins, config FROM settings WHERE catalog_id = ?`, catalogID,
	)
	var st Settings
	if err := row.Scan(&st.CatalogID, &st.Admins, &st.Config); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return &Settings{CatalogID: catalogID, Admins: "[]", Config: "{}"}, nil
		}
		return nil, err
	}
	return &st, nil
}

func (s *Store) PutSettings(catalogID, admins, config string) error {
	_, err := s.store.DB().Exec(
		`INSERT INTO settings (catalog_id, admins, config) VALUES (?, ?, ?)
		 ON CONFLICT(catalog_id) DO UPDATE SET admins=excluded.admins, config=excluded.config`,
		catalogID, admins, config,
	)
	return err
}

// ---- DataCellsFilter ----

func (s *Store) CreateDataCellsFilter(name, databaseName, tableName, columnNames, rowFilter string) error {
	_, err := s.store.DB().Exec(
		`INSERT INTO data_cells_filters (name, database_name, table_name, column_names, row_filter) VALUES (?, ?, ?, ?, ?)`,
		name, databaseName, tableName, columnNames, rowFilter,
	)
	return err
}

func (s *Store) GetDataCellsFilter(name, databaseName, tableName string) (*DataCellsFilter, error) {
	row := s.store.DB().QueryRow(
		`SELECT name, database_name, table_name, column_names, row_filter FROM data_cells_filters
		 WHERE name = ? AND database_name = ? AND table_name = ?`,
		name, databaseName, tableName,
	)
	var f DataCellsFilter
	if err := row.Scan(&f.Name, &f.DatabaseName, &f.TableName, &f.ColumnNames, &f.RowFilter); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errNotFound
		}
		return nil, err
	}
	return &f, nil
}

func (s *Store) UpdateDataCellsFilter(name, databaseName, tableName, columnNames, rowFilter string) error {
	res, err := s.store.DB().Exec(
		`UPDATE data_cells_filters SET column_names = ?, row_filter = ?
		 WHERE name = ? AND database_name = ? AND table_name = ?`,
		columnNames, rowFilter, name, databaseName, tableName,
	)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errNotFound
	}
	return nil
}

func (s *Store) DeleteDataCellsFilter(name, databaseName, tableName string) error {
	res, err := s.store.DB().Exec(
		`DELETE FROM data_cells_filters WHERE name = ? AND database_name = ? AND table_name = ?`,
		name, databaseName, tableName,
	)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errNotFound
	}
	return nil
}

func (s *Store) ListDataCellsFilters() ([]DataCellsFilter, error) {
	rows, err := s.store.DB().Query(
		`SELECT name, database_name, table_name, column_names, row_filter FROM data_cells_filters ORDER BY name`,
	)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var filters []DataCellsFilter
	for rows.Next() {
		var f DataCellsFilter
		if err := rows.Scan(&f.Name, &f.DatabaseName, &f.TableName, &f.ColumnNames, &f.RowFilter); err != nil {
			return nil, err
		}
		filters = append(filters, f)
	}
	return filters, rows.Err()
}
