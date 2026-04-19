// SPDX-License-Identifier: Apache-2.0

// internal/services/ssm/store.go
package ssm

import (
	"database/sql"
	"errors"
	"path/filepath"
	"strings"
	"time"

	"github.com/skyoo2003/devcloud/internal/storage/sqlite"
)

var (
	ErrParameterNotFound = errors.New("parameter not found")
	ErrParameterExists   = errors.New("parameter already exists")
	ErrDocumentNotFound  = errors.New("document not found")
	ErrDocumentExists    = errors.New("document already exists")
)

var migrations = []sqlite.Migration{
	{Version: 1, SQL: `
		CREATE TABLE IF NOT EXISTS parameters (
			name          TEXT NOT NULL,
			account_id    TEXT NOT NULL,
			type          TEXT NOT NULL DEFAULT 'String',
			value         TEXT NOT NULL,
			description   TEXT NOT NULL DEFAULT '',
			version       INTEGER NOT NULL DEFAULT 1,
			last_modified INTEGER NOT NULL,
			PRIMARY KEY (name, account_id)
		);
		CREATE TABLE IF NOT EXISTS parameter_tags (
			name       TEXT NOT NULL,
			account_id TEXT NOT NULL,
			key        TEXT NOT NULL,
			value      TEXT NOT NULL,
			PRIMARY KEY (name, account_id, key)
		);
	`},
	{Version: 2, SQL: `
		CREATE TABLE IF NOT EXISTS parameter_history (
			name          TEXT NOT NULL,
			account_id    TEXT NOT NULL,
			version       INTEGER NOT NULL,
			value         TEXT NOT NULL,
			type          TEXT NOT NULL,
			description   TEXT,
			last_modified INTEGER NOT NULL,
			labels        TEXT,
			PRIMARY KEY (name, account_id, version)
		);
		CREATE TABLE IF NOT EXISTS documents (
			name            TEXT NOT NULL,
			account_id      TEXT NOT NULL,
			content         TEXT NOT NULL,
			document_type   TEXT DEFAULT 'Command',
			document_format TEXT DEFAULT 'JSON',
			version         INTEGER DEFAULT 1,
			default_version INTEGER DEFAULT 1,
			status          TEXT DEFAULT 'Active',
			created_at      INTEGER NOT NULL,
			PRIMARY KEY (name, account_id)
		);
		CREATE TABLE IF NOT EXISTS document_versions (
			name       TEXT NOT NULL,
			account_id TEXT NOT NULL,
			version    INTEGER NOT NULL,
			content    TEXT NOT NULL,
			created_at INTEGER NOT NULL,
			PRIMARY KEY (name, account_id, version)
		);
	`},
}

type Parameter struct {
	Name         string
	AccountID    string
	Type         string
	Value        string
	Description  string
	Version      int
	LastModified time.Time
}

type ParameterHistory struct {
	Name         string
	AccountID    string
	Version      int
	Value        string
	Type         string
	Description  string
	LastModified time.Time
	Labels       []string
}

type Document struct {
	Name           string
	AccountID      string
	Content        string
	DocumentType   string
	DocumentFormat string
	Version        int
	DefaultVersion int
	Status         string
	CreatedAt      time.Time
}

type SSMStore struct {
	store *sqlite.Store
}

func NewSSMStore(dataDir string) (*SSMStore, error) {
	dbPath := filepath.Join(dataDir, "ssm.db")
	s, err := sqlite.Open(dbPath, migrations)
	if err != nil {
		return nil, err
	}
	return &SSMStore{store: s}, nil
}

func (s *SSMStore) Close() error { return s.store.Close() }

// PutParameter creates or overwrites a parameter. overwrite=false returns ErrParameterExists if it exists.
func (s *SSMStore) PutParameter(name, accountID, paramType, value, description string, overwrite bool) (*Parameter, error) {
	now := time.Now().Unix()
	existing, err := s.GetParameter(name, accountID)
	if err == nil {
		// Parameter exists.
		if !overwrite {
			return nil, ErrParameterExists
		}
		newVersion := existing.Version + 1
		_, err = s.store.DB().Exec(
			`UPDATE parameters SET type = ?, value = ?, description = ?, version = ?, last_modified = ? WHERE name = ? AND account_id = ?`,
			paramType, value, description, newVersion, now, name, accountID,
		)
		if err != nil {
			return nil, err
		}
		param := &Parameter{Name: name, AccountID: accountID, Type: paramType, Value: value, Description: description, Version: newVersion, LastModified: time.Unix(now, 0)}
		_ = s.insertHistory(param)
		return param, nil
	}
	// Create new.
	_, err = s.store.DB().Exec(
		`INSERT INTO parameters (name, account_id, type, value, description, version, last_modified) VALUES (?, ?, ?, ?, ?, 1, ?)`,
		name, accountID, paramType, value, description, now,
	)
	if err != nil {
		return nil, err
	}
	param := &Parameter{Name: name, AccountID: accountID, Type: paramType, Value: value, Description: description, Version: 1, LastModified: time.Unix(now, 0)}
	_ = s.insertHistory(param)
	return param, nil
}

func (s *SSMStore) insertHistory(p *Parameter) error {
	_, err := s.store.DB().Exec(
		`INSERT OR IGNORE INTO parameter_history (name, account_id, version, value, type, description, last_modified, labels) VALUES (?, ?, ?, ?, ?, ?, ?, '')`,
		p.Name, p.AccountID, p.Version, p.Value, p.Type, p.Description, p.LastModified.Unix(),
	)
	return err
}

func (s *SSMStore) GetParameter(name, accountID string) (*Parameter, error) {
	row := s.store.DB().QueryRow(
		`SELECT name, account_id, type, value, description, version, last_modified FROM parameters WHERE name = ? AND account_id = ?`,
		name, accountID,
	)
	return scanParameter(row)
}

func scanParameter(row *sql.Row) (*Parameter, error) {
	var p Parameter
	var lastModified int64
	if err := row.Scan(&p.Name, &p.AccountID, &p.Type, &p.Value, &p.Description, &p.Version, &lastModified); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrParameterNotFound
		}
		return nil, err
	}
	p.LastModified = time.Unix(lastModified, 0)
	return &p, nil
}

func (s *SSMStore) GetParameters(names []string, accountID string) ([]Parameter, []string) {
	var found []Parameter
	var invalid []string
	for _, name := range names {
		p, err := s.GetParameter(name, accountID)
		if err != nil {
			invalid = append(invalid, name)
		} else {
			found = append(found, *p)
		}
	}
	return found, invalid
}

func (s *SSMStore) GetParametersByPath(path, accountID string, recursive bool) ([]Parameter, error) {
	pattern := path + "%"
	rows, err := s.store.DB().Query(
		`SELECT name, account_id, type, value, description, version, last_modified FROM parameters WHERE account_id = ? AND name LIKE ? ORDER BY name`,
		accountID, pattern,
	)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var params []Parameter
	for rows.Next() {
		var p Parameter
		var lastModified int64
		if err := rows.Scan(&p.Name, &p.AccountID, &p.Type, &p.Value, &p.Description, &p.Version, &lastModified); err != nil {
			return nil, err
		}
		p.LastModified = time.Unix(lastModified, 0)
		// For non-recursive, filter out deeper-nested names.
		if !recursive {
			suffix := strings.TrimPrefix(p.Name, path)
			if strings.Contains(strings.TrimPrefix(suffix, "/"), "/") {
				continue
			}
		}
		params = append(params, p)
	}
	return params, rows.Err()
}

func (s *SSMStore) DeleteParameter(name, accountID string) error {
	res, err := s.store.DB().Exec(`DELETE FROM parameters WHERE name = ? AND account_id = ?`, name, accountID)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrParameterNotFound
	}
	return nil
}

type Tag struct {
	Key   string
	Value string
}

func (s *SSMStore) PutTag(name, accountID, key, value string) error {
	_, err := s.store.DB().Exec(
		`INSERT INTO parameter_tags (name, account_id, key, value) VALUES (?, ?, ?, ?)
		 ON CONFLICT(name, account_id, key) DO UPDATE SET value = excluded.value`,
		name, accountID, key, value,
	)
	return err
}

func (s *SSMStore) DeleteTag(name, accountID, key string) error {
	_, err := s.store.DB().Exec(
		`DELETE FROM parameter_tags WHERE name = ? AND account_id = ? AND key = ?`,
		name, accountID, key,
	)
	return err
}

func (s *SSMStore) ListTags(name, accountID string) ([]Tag, error) {
	rows, err := s.store.DB().Query(
		`SELECT key, value FROM parameter_tags WHERE name = ? AND account_id = ? ORDER BY key`,
		name, accountID,
	)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var tags []Tag
	for rows.Next() {
		var t Tag
		if err := rows.Scan(&t.Key, &t.Value); err != nil {
			return nil, err
		}
		tags = append(tags, t)
	}
	return tags, rows.Err()
}

func (s *SSMStore) DescribeParameters(accountID string) ([]Parameter, error) {
	rows, err := s.store.DB().Query(
		`SELECT name, account_id, type, value, description, version, last_modified FROM parameters WHERE account_id = ? ORDER BY name`,
		accountID,
	)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var params []Parameter
	for rows.Next() {
		var p Parameter
		var lastModified int64
		if err := rows.Scan(&p.Name, &p.AccountID, &p.Type, &p.Value, &p.Description, &p.Version, &lastModified); err != nil {
			return nil, err
		}
		p.LastModified = time.Unix(lastModified, 0)
		params = append(params, p)
	}
	return params, rows.Err()
}

// GetParameterHistory returns all versions of a parameter.
func (s *SSMStore) GetParameterHistory(name, accountID string) ([]ParameterHistory, error) {
	rows, err := s.store.DB().Query(
		`SELECT name, account_id, version, value, type, description, last_modified, labels FROM parameter_history WHERE name = ? AND account_id = ? ORDER BY version`,
		name, accountID,
	)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var history []ParameterHistory
	for rows.Next() {
		var h ParameterHistory
		var lastModified int64
		var labelsStr string
		if err := rows.Scan(&h.Name, &h.AccountID, &h.Version, &h.Value, &h.Type, &h.Description, &lastModified, &labelsStr); err != nil {
			return nil, err
		}
		h.LastModified = time.Unix(lastModified, 0)
		if labelsStr != "" {
			h.Labels = strings.Split(labelsStr, ",")
		}
		history = append(history, h)
	}
	return history, rows.Err()
}

// LabelParameterVersion adds labels to a specific version of a parameter.
func (s *SSMStore) LabelParameterVersion(name, accountID string, version int, labels []string) error {
	var existing string
	row := s.store.DB().QueryRow(
		`SELECT labels FROM parameter_history WHERE name = ? AND account_id = ? AND version = ?`,
		name, accountID, version,
	)
	if err := row.Scan(&existing); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ErrParameterNotFound
		}
		return err
	}
	var allLabels []string
	if existing != "" {
		allLabels = strings.Split(existing, ",")
	}
	for _, l := range labels {
		found := false
		for _, el := range allLabels {
			if el == l {
				found = true
				break
			}
		}
		if !found {
			allLabels = append(allLabels, l)
		}
	}
	newLabels := strings.Join(allLabels, ",")
	_, err := s.store.DB().Exec(
		`UPDATE parameter_history SET labels = ? WHERE name = ? AND account_id = ? AND version = ?`,
		newLabels, name, accountID, version,
	)
	return err
}

// Document operations

func (s *SSMStore) CreateDocument(name, accountID, content, docType, docFormat string) (*Document, error) {
	now := time.Now().Unix()
	_, err := s.store.DB().Exec(
		`INSERT INTO documents (name, account_id, content, document_type, document_format, version, default_version, status, created_at) VALUES (?, ?, ?, ?, ?, 1, 1, 'Active', ?)`,
		name, accountID, content, docType, docFormat, now,
	)
	if err != nil {
		if strings.Contains(err.Error(), "UNIQUE") {
			return nil, ErrDocumentExists
		}
		return nil, err
	}
	_, err = s.store.DB().Exec(
		`INSERT INTO document_versions (name, account_id, version, content, created_at) VALUES (?, ?, 1, ?, ?)`,
		name, accountID, content, now,
	)
	if err != nil {
		return nil, err
	}
	return &Document{
		Name: name, AccountID: accountID, Content: content,
		DocumentType: docType, DocumentFormat: docFormat,
		Version: 1, DefaultVersion: 1, Status: "Active",
		CreatedAt: time.Unix(now, 0),
	}, nil
}

func (s *SSMStore) GetDocument(name, accountID string, version int) (*Document, error) {
	var d Document
	var createdAt int64
	var err error
	if version > 0 {
		row := s.store.DB().QueryRow(
			`SELECT d.name, d.account_id, dv.content, d.document_type, d.document_format, dv.version, d.default_version, d.status, d.created_at
			 FROM documents d JOIN document_versions dv ON d.name = dv.name AND d.account_id = dv.account_id
			 WHERE d.name = ? AND d.account_id = ? AND dv.version = ?`,
			name, accountID, version,
		)
		err = row.Scan(&d.Name, &d.AccountID, &d.Content, &d.DocumentType, &d.DocumentFormat, &d.Version, &d.DefaultVersion, &d.Status, &createdAt)
	} else {
		row := s.store.DB().QueryRow(
			`SELECT name, account_id, content, document_type, document_format, version, default_version, status, created_at FROM documents WHERE name = ? AND account_id = ?`,
			name, accountID,
		)
		err = row.Scan(&d.Name, &d.AccountID, &d.Content, &d.DocumentType, &d.DocumentFormat, &d.Version, &d.DefaultVersion, &d.Status, &createdAt)
	}
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrDocumentNotFound
		}
		return nil, err
	}
	d.CreatedAt = time.Unix(createdAt, 0)
	return &d, nil
}

func (s *SSMStore) UpdateDocument(name, accountID, content string) (*Document, error) {
	existing, err := s.GetDocument(name, accountID, 0)
	if err != nil {
		return nil, err
	}
	newVersion := existing.Version + 1
	now := time.Now().Unix()
	_, err = s.store.DB().Exec(
		`UPDATE documents SET content = ?, version = ?, default_version = ? WHERE name = ? AND account_id = ?`,
		content, newVersion, newVersion, name, accountID,
	)
	if err != nil {
		return nil, err
	}
	_, err = s.store.DB().Exec(
		`INSERT INTO document_versions (name, account_id, version, content, created_at) VALUES (?, ?, ?, ?, ?)`,
		name, accountID, newVersion, content, now,
	)
	if err != nil {
		return nil, err
	}
	existing.Content = content
	existing.Version = newVersion
	existing.DefaultVersion = newVersion
	return existing, nil
}

func (s *SSMStore) DeleteDocument(name, accountID string) error {
	res, err := s.store.DB().Exec(`DELETE FROM documents WHERE name = ? AND account_id = ?`, name, accountID)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrDocumentNotFound
	}
	_, _ = s.store.DB().Exec(`DELETE FROM document_versions WHERE name = ? AND account_id = ?`, name, accountID)
	return nil
}

func (s *SSMStore) ListDocuments(accountID string) ([]Document, error) {
	rows, err := s.store.DB().Query(
		`SELECT name, account_id, content, document_type, document_format, version, default_version, status, created_at FROM documents WHERE account_id = ? ORDER BY name`,
		accountID,
	)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var docs []Document
	for rows.Next() {
		var d Document
		var createdAt int64
		if err := rows.Scan(&d.Name, &d.AccountID, &d.Content, &d.DocumentType, &d.DocumentFormat, &d.Version, &d.DefaultVersion, &d.Status, &createdAt); err != nil {
			return nil, err
		}
		d.CreatedAt = time.Unix(createdAt, 0)
		docs = append(docs, d)
	}
	return docs, rows.Err()
}
