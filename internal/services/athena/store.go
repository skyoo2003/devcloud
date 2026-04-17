// SPDX-License-Identifier: Apache-2.0

// internal/services/athena/store.go
package athena

import (
	"database/sql"
	"errors"
	"path/filepath"
	"time"

	"github.com/skyoo2003/devcloud/internal/shared"
	"github.com/skyoo2003/devcloud/internal/storage/sqlite"
)

var (
	errWorkGroupNotFound         = errors.New("workgroup not found")
	errNamedQueryNotFound        = errors.New("named query not found")
	errQueryExecutionNotFound    = errors.New("query execution not found")
	errDataCatalogNotFound       = errors.New("data catalog not found")
	errPreparedStatementNotFound = errors.New("prepared statement not found")
)

var migrations = []sqlite.Migration{
	{Version: 1, SQL: `
		CREATE TABLE IF NOT EXISTS workgroups (
			name        TEXT PRIMARY KEY,
			arn         TEXT NOT NULL UNIQUE,
			state       TEXT NOT NULL DEFAULT 'ENABLED',
			description TEXT NOT NULL DEFAULT '',
			config      TEXT NOT NULL DEFAULT '{}',
			created_at  INTEGER NOT NULL
		);
		CREATE TABLE IF NOT EXISTS named_queries (
			id            TEXT PRIMARY KEY,
			name          TEXT NOT NULL,
			workgroup     TEXT NOT NULL DEFAULT 'primary',
			database_name TEXT NOT NULL DEFAULT '',
			query_string  TEXT NOT NULL DEFAULT '',
			description   TEXT NOT NULL DEFAULT ''
		);
		CREATE TABLE IF NOT EXISTS query_executions (
			id            TEXT PRIMARY KEY,
			workgroup     TEXT NOT NULL DEFAULT 'primary',
			query         TEXT NOT NULL DEFAULT '',
			database_name TEXT NOT NULL DEFAULT '',
			status        TEXT NOT NULL DEFAULT 'SUCCEEDED',
			submitted_at  INTEGER NOT NULL
		);
		CREATE TABLE IF NOT EXISTS data_catalogs (
			name        TEXT PRIMARY KEY,
			arn         TEXT NOT NULL UNIQUE,
			type        TEXT NOT NULL DEFAULT 'HIVE',
			description TEXT NOT NULL DEFAULT '',
			parameters  TEXT NOT NULL DEFAULT '{}'
		);
		CREATE TABLE IF NOT EXISTS prepared_statements (
			name            TEXT NOT NULL,
			workgroup       TEXT NOT NULL,
			query_statement TEXT NOT NULL DEFAULT '',
			description     TEXT NOT NULL DEFAULT '',
			updated_at      INTEGER NOT NULL,
			PRIMARY KEY (name, workgroup)
		);
	`},
}

// ---- model types ----

type WorkGroup struct {
	Name        string
	ARN         string
	State       string
	Description string
	Config      string
	CreatedAt   time.Time
}

type NamedQuery struct {
	ID           string
	Name         string
	WorkGroup    string
	DatabaseName string
	QueryString  string
	Description  string
}

type QueryExecution struct {
	ID           string
	WorkGroup    string
	Query        string
	DatabaseName string
	Status       string
	SubmittedAt  time.Time
}

type DataCatalog struct {
	Name        string
	ARN         string
	Type        string
	Description string
	Parameters  string
}

type PreparedStatement struct {
	Name           string
	WorkGroup      string
	QueryStatement string
	Description    string
	UpdatedAt      time.Time
}

// ---- Store ----

type Store struct {
	store *sqlite.Store
	tags  *shared.TagStore
}

func NewStore(dataDir string) (*Store, error) {
	allMigrations := append(migrations, shared.TagMigrations...)
	dbPath := filepath.Join(dataDir, "athena.db")
	s, err := sqlite.Open(dbPath, allMigrations)
	if err != nil {
		return nil, err
	}
	return &Store{store: s, tags: shared.NewTagStore(s)}, nil
}

func (s *Store) Close() error { return s.store.Close() }

// ---- WorkGroup ----

func (s *Store) CreateWorkGroup(name, arn, description, config string) (*WorkGroup, error) {
	now := time.Now().Unix()
	_, err := s.store.DB().Exec(
		`INSERT INTO workgroups (name, arn, description, config, created_at) VALUES (?, ?, ?, ?, ?)`,
		name, arn, description, config, now,
	)
	if err != nil {
		return nil, err
	}
	return &WorkGroup{Name: name, ARN: arn, State: "ENABLED", Description: description, Config: config, CreatedAt: time.Unix(now, 0)}, nil
}

func (s *Store) GetWorkGroup(name string) (*WorkGroup, error) {
	row := s.store.DB().QueryRow(
		`SELECT name, arn, state, description, config, created_at FROM workgroups WHERE name = ?`, name)
	return scanWorkGroup(row)
}

func (s *Store) ListWorkGroups() ([]WorkGroup, error) {
	rows, err := s.store.DB().Query(
		`SELECT name, arn, state, description, config, created_at FROM workgroups ORDER BY created_at`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var wgs []WorkGroup
	for rows.Next() {
		wg, err := scanWorkGroup(rows)
		if err != nil {
			return nil, err
		}
		wgs = append(wgs, *wg)
	}
	return wgs, rows.Err()
}

func (s *Store) DeleteWorkGroup(name string) error {
	res, err := s.store.DB().Exec(`DELETE FROM workgroups WHERE name = ?`, name)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errWorkGroupNotFound
	}
	return nil
}

func (s *Store) UpdateWorkGroup(name, description, state, config string) error {
	res, err := s.store.DB().Exec(
		`UPDATE workgroups SET description = ?, state = ?, config = ? WHERE name = ?`,
		description, state, config, name)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errWorkGroupNotFound
	}
	return nil
}

type scanner interface{ Scan(dest ...any) error }

func scanWorkGroup(sc scanner) (*WorkGroup, error) {
	var wg WorkGroup
	var createdAt int64
	err := sc.Scan(&wg.Name, &wg.ARN, &wg.State, &wg.Description, &wg.Config, &createdAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errWorkGroupNotFound
		}
		return nil, err
	}
	wg.CreatedAt = time.Unix(createdAt, 0)
	return &wg, nil
}

// ---- NamedQuery ----

func (s *Store) CreateNamedQuery(id, name, workgroup, database, queryString, description string) (*NamedQuery, error) {
	_, err := s.store.DB().Exec(
		`INSERT INTO named_queries (id, name, workgroup, database_name, query_string, description) VALUES (?, ?, ?, ?, ?, ?)`,
		id, name, workgroup, database, queryString, description,
	)
	if err != nil {
		return nil, err
	}
	return &NamedQuery{ID: id, Name: name, WorkGroup: workgroup, DatabaseName: database, QueryString: queryString, Description: description}, nil
}

func (s *Store) GetNamedQuery(id string) (*NamedQuery, error) {
	row := s.store.DB().QueryRow(
		`SELECT id, name, workgroup, database_name, query_string, description FROM named_queries WHERE id = ?`, id)
	return scanNamedQuery(row)
}

func (s *Store) ListNamedQueries(workgroup string) ([]NamedQuery, error) {
	var rows *sql.Rows
	var err error
	if workgroup != "" {
		rows, err = s.store.DB().Query(
			`SELECT id, name, workgroup, database_name, query_string, description FROM named_queries WHERE workgroup = ? ORDER BY name`,
			workgroup)
	} else {
		rows, err = s.store.DB().Query(
			`SELECT id, name, workgroup, database_name, query_string, description FROM named_queries ORDER BY name`)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var queries []NamedQuery
	for rows.Next() {
		nq, err := scanNamedQuery(rows)
		if err != nil {
			return nil, err
		}
		queries = append(queries, *nq)
	}
	return queries, rows.Err()
}

func (s *Store) DeleteNamedQuery(id string) error {
	res, err := s.store.DB().Exec(`DELETE FROM named_queries WHERE id = ?`, id)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errNamedQueryNotFound
	}
	return nil
}

func scanNamedQuery(sc scanner) (*NamedQuery, error) {
	var nq NamedQuery
	err := sc.Scan(&nq.ID, &nq.Name, &nq.WorkGroup, &nq.DatabaseName, &nq.QueryString, &nq.Description)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errNamedQueryNotFound
		}
		return nil, err
	}
	return &nq, nil
}

// ---- QueryExecution ----

func (s *Store) CreateQueryExecution(id, workgroup, query, database string) (*QueryExecution, error) {
	now := time.Now().Unix()
	_, err := s.store.DB().Exec(
		`INSERT INTO query_executions (id, workgroup, query, database_name, submitted_at) VALUES (?, ?, ?, ?, ?)`,
		id, workgroup, query, database, now,
	)
	if err != nil {
		return nil, err
	}
	return &QueryExecution{ID: id, WorkGroup: workgroup, Query: query, DatabaseName: database, Status: "SUCCEEDED", SubmittedAt: time.Unix(now, 0)}, nil
}

func (s *Store) GetQueryExecution(id string) (*QueryExecution, error) {
	row := s.store.DB().QueryRow(
		`SELECT id, workgroup, query, database_name, status, submitted_at FROM query_executions WHERE id = ?`, id)
	return scanQueryExecution(row)
}

func (s *Store) ListQueryExecutions(workgroup string) ([]QueryExecution, error) {
	var rows *sql.Rows
	var err error
	if workgroup != "" {
		rows, err = s.store.DB().Query(
			`SELECT id, workgroup, query, database_name, status, submitted_at FROM query_executions WHERE workgroup = ? ORDER BY submitted_at`,
			workgroup)
	} else {
		rows, err = s.store.DB().Query(
			`SELECT id, workgroup, query, database_name, status, submitted_at FROM query_executions ORDER BY submitted_at`)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var execs []QueryExecution
	for rows.Next() {
		qe, err := scanQueryExecution(rows)
		if err != nil {
			return nil, err
		}
		execs = append(execs, *qe)
	}
	return execs, rows.Err()
}

func (s *Store) UpdateQueryExecutionStatus(id, status string) error {
	res, err := s.store.DB().Exec(`UPDATE query_executions SET status = ? WHERE id = ?`, status, id)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errQueryExecutionNotFound
	}
	return nil
}

func scanQueryExecution(sc scanner) (*QueryExecution, error) {
	var qe QueryExecution
	var submittedAt int64
	err := sc.Scan(&qe.ID, &qe.WorkGroup, &qe.Query, &qe.DatabaseName, &qe.Status, &submittedAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errQueryExecutionNotFound
		}
		return nil, err
	}
	qe.SubmittedAt = time.Unix(submittedAt, 0)
	return &qe, nil
}

// ---- DataCatalog ----

func (s *Store) CreateDataCatalog(name, arn, catalogType, description, parameters string) (*DataCatalog, error) {
	_, err := s.store.DB().Exec(
		`INSERT INTO data_catalogs (name, arn, type, description, parameters) VALUES (?, ?, ?, ?, ?)`,
		name, arn, catalogType, description, parameters,
	)
	if err != nil {
		return nil, err
	}
	return &DataCatalog{Name: name, ARN: arn, Type: catalogType, Description: description, Parameters: parameters}, nil
}

func (s *Store) GetDataCatalog(name string) (*DataCatalog, error) {
	row := s.store.DB().QueryRow(
		`SELECT name, arn, type, description, parameters FROM data_catalogs WHERE name = ?`, name)
	return scanDataCatalog(row)
}

func (s *Store) ListDataCatalogs() ([]DataCatalog, error) {
	rows, err := s.store.DB().Query(
		`SELECT name, arn, type, description, parameters FROM data_catalogs ORDER BY name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var catalogs []DataCatalog
	for rows.Next() {
		dc, err := scanDataCatalog(rows)
		if err != nil {
			return nil, err
		}
		catalogs = append(catalogs, *dc)
	}
	return catalogs, rows.Err()
}

func (s *Store) UpdateDataCatalog(name, description, parameters string) error {
	res, err := s.store.DB().Exec(
		`UPDATE data_catalogs SET description = ?, parameters = ? WHERE name = ?`,
		description, parameters, name)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errDataCatalogNotFound
	}
	return nil
}

func (s *Store) DeleteDataCatalog(name string) error {
	res, err := s.store.DB().Exec(`DELETE FROM data_catalogs WHERE name = ?`, name)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errDataCatalogNotFound
	}
	return nil
}

func scanDataCatalog(sc scanner) (*DataCatalog, error) {
	var dc DataCatalog
	err := sc.Scan(&dc.Name, &dc.ARN, &dc.Type, &dc.Description, &dc.Parameters)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errDataCatalogNotFound
		}
		return nil, err
	}
	return &dc, nil
}

// ---- PreparedStatement ----

func (s *Store) CreatePreparedStatement(name, workgroup, queryStatement, description string) (*PreparedStatement, error) {
	now := time.Now().Unix()
	_, err := s.store.DB().Exec(
		`INSERT INTO prepared_statements (name, workgroup, query_statement, description, updated_at) VALUES (?, ?, ?, ?, ?)`,
		name, workgroup, queryStatement, description, now,
	)
	if err != nil {
		return nil, err
	}
	return &PreparedStatement{Name: name, WorkGroup: workgroup, QueryStatement: queryStatement, Description: description, UpdatedAt: time.Unix(now, 0)}, nil
}

func (s *Store) GetPreparedStatement(name, workgroup string) (*PreparedStatement, error) {
	row := s.store.DB().QueryRow(
		`SELECT name, workgroup, query_statement, description, updated_at FROM prepared_statements WHERE name = ? AND workgroup = ?`,
		name, workgroup)
	return scanPreparedStatement(row)
}

func (s *Store) ListPreparedStatements(workgroup string) ([]PreparedStatement, error) {
	rows, err := s.store.DB().Query(
		`SELECT name, workgroup, query_statement, description, updated_at FROM prepared_statements WHERE workgroup = ? ORDER BY name`,
		workgroup)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var stmts []PreparedStatement
	for rows.Next() {
		ps, err := scanPreparedStatement(rows)
		if err != nil {
			return nil, err
		}
		stmts = append(stmts, *ps)
	}
	return stmts, rows.Err()
}

func (s *Store) UpdatePreparedStatement(name, workgroup, queryStatement, description string) error {
	now := time.Now().Unix()
	res, err := s.store.DB().Exec(
		`UPDATE prepared_statements SET query_statement = ?, description = ?, updated_at = ? WHERE name = ? AND workgroup = ?`,
		queryStatement, description, now, name, workgroup)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errPreparedStatementNotFound
	}
	return nil
}

func (s *Store) DeletePreparedStatement(name, workgroup string) error {
	res, err := s.store.DB().Exec(
		`DELETE FROM prepared_statements WHERE name = ? AND workgroup = ?`, name, workgroup)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errPreparedStatementNotFound
	}
	return nil
}

func scanPreparedStatement(sc scanner) (*PreparedStatement, error) {
	var ps PreparedStatement
	var updatedAt int64
	err := sc.Scan(&ps.Name, &ps.WorkGroup, &ps.QueryStatement, &ps.Description, &updatedAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errPreparedStatementNotFound
		}
		return nil, err
	}
	ps.UpdatedAt = time.Unix(updatedAt, 0)
	return &ps, nil
}
