// SPDX-License-Identifier: Apache-2.0

// internal/services/support/store.go
package support

import (
	"database/sql"
	"errors"
	"path/filepath"
	"time"

	"github.com/skyoo2003/devcloud/internal/storage/sqlite"
)

var errCaseNotFound = errors.New("case not found")

var migrations = []sqlite.Migration{
	{Version: 1, SQL: `
		CREATE TABLE IF NOT EXISTS cases (
			id              TEXT PRIMARY KEY,
			subject         TEXT NOT NULL DEFAULT '',
			status          TEXT NOT NULL DEFAULT 'opened',
			service_code    TEXT NOT NULL DEFAULT 'general-info',
			category_code   TEXT NOT NULL DEFAULT 'other',
			severity_code   TEXT NOT NULL DEFAULT 'low',
			language        TEXT NOT NULL DEFAULT 'en',
			submitted_by    TEXT NOT NULL DEFAULT '',
			created_at      INTEGER NOT NULL
		);
		CREATE TABLE IF NOT EXISTS communications (
			id              TEXT PRIMARY KEY,
			case_id         TEXT NOT NULL,
			body            TEXT NOT NULL DEFAULT '',
			submitted_by    TEXT NOT NULL DEFAULT 'customer',
			created_at      INTEGER NOT NULL
		);
	`},
	{Version: 2, SQL: `
		CREATE TABLE IF NOT EXISTS support_attachment_sets (
			id              TEXT PRIMARY KEY,
			expiry_at       INTEGER NOT NULL,
			created_at      INTEGER NOT NULL
		);
		CREATE TABLE IF NOT EXISTS support_attachments (
			id              TEXT PRIMARY KEY,
			attachment_set_id TEXT NOT NULL,
			file_name       TEXT NOT NULL DEFAULT '',
			data            TEXT NOT NULL DEFAULT '',
			created_at      INTEGER NOT NULL
		);
	`},
}

// Attachment records an uploaded attachment stub.
type Attachment struct {
	ID              string
	AttachmentSetID string
	FileName        string
	Data            string
	CreatedAt       time.Time
}

type Case struct {
	ID           string
	Subject      string
	Status       string
	ServiceCode  string
	CategoryCode string
	SeverityCode string
	Language     string
	SubmittedBy  string
	CreatedAt    time.Time
}

type Communication struct {
	ID          string
	CaseID      string
	Body        string
	SubmittedBy string
	CreatedAt   time.Time
}

type Store struct {
	store *sqlite.Store
}

func NewStore(dataDir string) (*Store, error) {
	dbPath := filepath.Join(dataDir, "support.db")
	s, err := sqlite.Open(dbPath, migrations)
	if err != nil {
		return nil, err
	}
	return &Store{store: s}, nil
}

func (s *Store) Close() error { return s.store.Close() }

func (s *Store) CreateCase(id, subject, serviceCode, categoryCode, severityCode, language, submittedBy string) (*Case, error) {
	now := time.Now().Unix()
	_, err := s.store.DB().Exec(
		`INSERT INTO cases (id, subject, service_code, category_code, severity_code, language, submitted_by, created_at)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		id, subject, serviceCode, categoryCode, severityCode, language, submittedBy, now,
	)
	if err != nil {
		return nil, err
	}
	return &Case{
		ID: id, Subject: subject, Status: "opened",
		ServiceCode: serviceCode, CategoryCode: categoryCode,
		SeverityCode: severityCode, Language: language,
		SubmittedBy: submittedBy, CreatedAt: time.Unix(now, 0),
	}, nil
}

func (s *Store) GetCase(id string) (*Case, error) {
	row := s.store.DB().QueryRow(
		`SELECT id, subject, status, service_code, category_code, severity_code, language, submitted_by, created_at
         FROM cases WHERE id = ?`, id)
	return scanCase(row)
}

func (s *Store) ListCases(includeResolved bool, filterIDs []string) ([]Case, error) {
	query := `SELECT id, subject, status, service_code, category_code, severity_code, language, submitted_by, created_at FROM cases`
	var args []any
	var conditions []string
	if !includeResolved {
		conditions = append(conditions, "status != 'resolved'")
	}
	if len(filterIDs) > 0 {
		placeholders := make([]string, len(filterIDs))
		for i, id := range filterIDs {
			placeholders[i] = "?"
			args = append(args, id)
		}
		conditions = append(conditions, "id IN ("+join(placeholders, ",")+")")
	}
	if len(conditions) > 0 {
		query += " WHERE " + join(conditions, " AND ")
	}
	query += " ORDER BY created_at"
	rows, err := s.store.DB().Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var cases []Case
	for rows.Next() {
		c, err := scanCase(rows)
		if err != nil {
			return nil, err
		}
		cases = append(cases, *c)
	}
	return cases, rows.Err()
}

func (s *Store) ResolveCase(id string) (string, error) {
	c, err := s.GetCase(id)
	if err != nil {
		return "", err
	}
	initial := c.Status
	_, err = s.store.DB().Exec(`UPDATE cases SET status = 'resolved' WHERE id = ?`, id)
	if err != nil {
		return "", err
	}
	return initial, nil
}

func (s *Store) AddCommunication(id, caseID, body, submittedBy string) (*Communication, error) {
	now := time.Now().Unix()
	_, err := s.store.DB().Exec(
		`INSERT INTO communications (id, case_id, body, submitted_by, created_at) VALUES (?, ?, ?, ?, ?)`,
		id, caseID, body, submittedBy, now,
	)
	if err != nil {
		return nil, err
	}
	return &Communication{
		ID: id, CaseID: caseID, Body: body,
		SubmittedBy: submittedBy, CreatedAt: time.Unix(now, 0),
	}, nil
}

// --- Attachment sets ---

func (s *Store) CreateAttachmentSet(id string, expiry time.Time) error {
	now := time.Now().Unix()
	_, err := s.store.DB().Exec(
		`INSERT INTO support_attachment_sets (id, expiry_at, created_at) VALUES (?, ?, ?)`,
		id, expiry.Unix(), now,
	)
	return err
}

func (s *Store) AddAttachment(id, setID, fileName, data string) (*Attachment, error) {
	now := time.Now().Unix()
	_, err := s.store.DB().Exec(
		`INSERT INTO support_attachments (id, attachment_set_id, file_name, data, created_at) VALUES (?, ?, ?, ?, ?)`,
		id, setID, fileName, data, now,
	)
	if err != nil {
		return nil, err
	}
	return &Attachment{
		ID: id, AttachmentSetID: setID, FileName: fileName,
		Data: data, CreatedAt: time.Unix(now, 0),
	}, nil
}

func (s *Store) GetAttachment(id string) (*Attachment, error) {
	row := s.store.DB().QueryRow(
		`SELECT id, attachment_set_id, file_name, data, created_at FROM support_attachments WHERE id = ?`, id)
	var a Attachment
	var createdAt int64
	err := row.Scan(&a.ID, &a.AttachmentSetID, &a.FileName, &a.Data, &createdAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errors.New("attachment not found")
		}
		return nil, err
	}
	a.CreatedAt = time.Unix(createdAt, 0)
	return &a, nil
}

func (s *Store) ListCommunications(caseID string) ([]Communication, error) {
	rows, err := s.store.DB().Query(
		`SELECT id, case_id, body, submitted_by, created_at FROM communications WHERE case_id = ? ORDER BY created_at`,
		caseID,
	)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var comms []Communication
	for rows.Next() {
		var c Communication
		var createdAt int64
		if err := rows.Scan(&c.ID, &c.CaseID, &c.Body, &c.SubmittedBy, &createdAt); err != nil {
			return nil, err
		}
		c.CreatedAt = time.Unix(createdAt, 0)
		comms = append(comms, c)
	}
	return comms, rows.Err()
}

type scanner interface{ Scan(dest ...any) error }

func scanCase(s scanner) (*Case, error) {
	var c Case
	var createdAt int64
	err := s.Scan(&c.ID, &c.Subject, &c.Status, &c.ServiceCode, &c.CategoryCode, &c.SeverityCode, &c.Language, &c.SubmittedBy, &createdAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errCaseNotFound
		}
		return nil, err
	}
	c.CreatedAt = time.Unix(createdAt, 0)
	return &c, nil
}

func join(parts []string, sep string) string {
	result := ""
	for i, p := range parts {
		if i > 0 {
			result += sep
		}
		result += p
	}
	return result
}
