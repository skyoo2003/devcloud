// SPDX-License-Identifier: Apache-2.0

// internal/services/pinpoint/store.go
package pinpoint

import (
	"database/sql"
	"errors"
	"path/filepath"
	"time"

	"github.com/skyoo2003/devcloud/internal/shared"
	"github.com/skyoo2003/devcloud/internal/storage/sqlite"
)

var (
	errAppNotFound      = errors.New("app not found")
	errCampaignNotFound = errors.New("campaign not found")
	errSegmentNotFound  = errors.New("segment not found")
	errJourneyNotFound  = errors.New("journey not found")
	errTemplateNotFound = errors.New("template not found")
)

var migrations = []sqlite.Migration{
	{Version: 1, SQL: `
		CREATE TABLE IF NOT EXISTS apps (
			id         TEXT PRIMARY KEY,
			arn        TEXT NOT NULL UNIQUE,
			name       TEXT NOT NULL,
			created_at INTEGER NOT NULL
		);
		CREATE TABLE IF NOT EXISTS campaigns (
			id             TEXT PRIMARY KEY,
			app_id         TEXT NOT NULL,
			arn            TEXT NOT NULL UNIQUE,
			name           TEXT NOT NULL,
			state          TEXT NOT NULL DEFAULT 'DRAFT',
			description    TEXT NOT NULL DEFAULT '',
			segment_id     TEXT NOT NULL DEFAULT '',
			schedule       TEXT NOT NULL DEFAULT '{}',
			message_config TEXT NOT NULL DEFAULT '{}',
			version        INTEGER NOT NULL DEFAULT 1,
			created_at     INTEGER NOT NULL
		);
		CREATE TABLE IF NOT EXISTS segments (
			id         TEXT PRIMARY KEY,
			app_id     TEXT NOT NULL,
			arn        TEXT NOT NULL UNIQUE,
			name       TEXT NOT NULL,
			type       TEXT NOT NULL DEFAULT 'DIMENSIONAL',
			dimensions TEXT NOT NULL DEFAULT '{}',
			version    INTEGER NOT NULL DEFAULT 1,
			created_at INTEGER NOT NULL
		);
		CREATE TABLE IF NOT EXISTS journeys (
			id         TEXT PRIMARY KEY,
			app_id     TEXT NOT NULL,
			arn        TEXT NOT NULL UNIQUE,
			name       TEXT NOT NULL,
			state      TEXT NOT NULL DEFAULT 'DRAFT',
			activities TEXT NOT NULL DEFAULT '{}',
			schedule   TEXT NOT NULL DEFAULT '{}',
			created_at INTEGER NOT NULL,
			updated_at INTEGER NOT NULL
		);
		CREATE TABLE IF NOT EXISTS templates (
			name       TEXT NOT NULL,
			type       TEXT NOT NULL,
			subject    TEXT NOT NULL DEFAULT '',
			html_body  TEXT NOT NULL DEFAULT '',
			text_body  TEXT NOT NULL DEFAULT '',
			body       TEXT NOT NULL DEFAULT '',
			version    TEXT NOT NULL DEFAULT '1',
			created_at INTEGER NOT NULL,
			updated_at INTEGER NOT NULL,
			PRIMARY KEY (name, type)
		);
	`},
}

// --- Model types ---

type App struct {
	ID        string
	ARN       string
	Name      string
	CreatedAt time.Time
}

type Campaign struct {
	ID            string
	AppID         string
	ARN           string
	Name          string
	State         string
	Description   string
	SegmentID     string
	Schedule      string
	MessageConfig string
	Version       int
	CreatedAt     time.Time
}

type Segment struct {
	ID         string
	AppID      string
	ARN        string
	Name       string
	Type       string
	Dimensions string
	Version    int
	CreatedAt  time.Time
}

type Journey struct {
	ID         string
	AppID      string
	ARN        string
	Name       string
	State      string
	Activities string
	Schedule   string
	CreatedAt  time.Time
	UpdatedAt  time.Time
}

type Template struct {
	Name      string
	Type      string
	Subject   string
	HTMLBody  string
	TextBody  string
	Body      string
	Version   string
	CreatedAt time.Time
	UpdatedAt time.Time
}

// --- Store ---

type Store struct {
	store *sqlite.Store
	tags  *shared.TagStore
}

func NewStore(dataDir string) (*Store, error) {
	allMigrations := append(migrations, shared.TagMigrations...)
	dbPath := filepath.Join(dataDir, "pinpoint.db")
	s, err := sqlite.Open(dbPath, allMigrations)
	if err != nil {
		return nil, err
	}
	return &Store{store: s, tags: shared.NewTagStore(s)}, nil
}

func (s *Store) Close() error { return s.store.Close() }

// --- App ---

func (s *Store) CreateApp(id, arn, name string) (*App, error) {
	now := time.Now().Unix()
	_, err := s.store.DB().Exec(
		`INSERT INTO apps (id, arn, name, created_at) VALUES (?, ?, ?, ?)`,
		id, arn, name, now,
	)
	if err != nil {
		return nil, err
	}
	return &App{ID: id, ARN: arn, Name: name, CreatedAt: time.Unix(now, 0)}, nil
}

func (s *Store) GetApp(id string) (*App, error) {
	row := s.store.DB().QueryRow(
		`SELECT id, arn, name, created_at FROM apps WHERE id = ?`, id)
	return scanApp(row)
}

func (s *Store) ListApps() ([]App, error) {
	rows, err := s.store.DB().Query(
		`SELECT id, arn, name, created_at FROM apps ORDER BY created_at`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []App
	for rows.Next() {
		a, err := scanApp(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *a)
	}
	return out, rows.Err()
}

func (s *Store) DeleteApp(id string) error {
	res, err := s.store.DB().Exec(`DELETE FROM apps WHERE id = ?`, id)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errAppNotFound
	}
	return nil
}

type scanner interface {
	Scan(dest ...any) error
}

func scanApp(row scanner) (*App, error) {
	var a App
	var ts int64
	err := row.Scan(&a.ID, &a.ARN, &a.Name, &ts)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, errAppNotFound
	}
	if err != nil {
		return nil, err
	}
	a.CreatedAt = time.Unix(ts, 0)
	return &a, nil
}

// --- Campaign ---

func (s *Store) CreateCampaign(id, appID, arn, name, description, segmentID string) (*Campaign, error) {
	now := time.Now().Unix()
	_, err := s.store.DB().Exec(
		`INSERT INTO campaigns (id, app_id, arn, name, description, segment_id, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)`,
		id, appID, arn, name, description, segmentID, now,
	)
	if err != nil {
		return nil, err
	}
	return &Campaign{
		ID: id, AppID: appID, ARN: arn, Name: name,
		State: "DRAFT", Description: description, SegmentID: segmentID,
		Schedule: "{}", MessageConfig: "{}", Version: 1,
		CreatedAt: time.Unix(now, 0),
	}, nil
}

func (s *Store) GetCampaign(appID, id string) (*Campaign, error) {
	row := s.store.DB().QueryRow(
		`SELECT id, app_id, arn, name, state, description, segment_id, schedule, message_config, version, created_at
		 FROM campaigns WHERE app_id = ? AND id = ?`, appID, id)
	return scanCampaign(row)
}

func (s *Store) ListCampaigns(appID string) ([]Campaign, error) {
	rows, err := s.store.DB().Query(
		`SELECT id, app_id, arn, name, state, description, segment_id, schedule, message_config, version, created_at
		 FROM campaigns WHERE app_id = ? ORDER BY created_at`, appID)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []Campaign
	for rows.Next() {
		c, err := scanCampaign(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *c)
	}
	return out, rows.Err()
}

func (s *Store) UpdateCampaign(appID, id, name, description, segmentID string) (*Campaign, error) {
	_, err := s.store.DB().Exec(
		`UPDATE campaigns SET name = ?, description = ?, segment_id = ?, version = version + 1
		 WHERE app_id = ? AND id = ?`,
		name, description, segmentID, appID, id,
	)
	if err != nil {
		return nil, err
	}
	return s.GetCampaign(appID, id)
}

func (s *Store) DeleteCampaign(appID, id string) error {
	res, err := s.store.DB().Exec(`DELETE FROM campaigns WHERE app_id = ? AND id = ?`, appID, id)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errCampaignNotFound
	}
	return nil
}

func scanCampaign(row scanner) (*Campaign, error) {
	var c Campaign
	var ts int64
	err := row.Scan(&c.ID, &c.AppID, &c.ARN, &c.Name, &c.State, &c.Description,
		&c.SegmentID, &c.Schedule, &c.MessageConfig, &c.Version, &ts)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, errCampaignNotFound
	}
	if err != nil {
		return nil, err
	}
	c.CreatedAt = time.Unix(ts, 0)
	return &c, nil
}

// --- Segment ---

func (s *Store) CreateSegment(id, appID, arn, name, segType string) (*Segment, error) {
	now := time.Now().Unix()
	_, err := s.store.DB().Exec(
		`INSERT INTO segments (id, app_id, arn, name, type, created_at) VALUES (?, ?, ?, ?, ?, ?)`,
		id, appID, arn, name, segType, now,
	)
	if err != nil {
		return nil, err
	}
	return &Segment{
		ID: id, AppID: appID, ARN: arn, Name: name,
		Type: segType, Dimensions: "{}", Version: 1,
		CreatedAt: time.Unix(now, 0),
	}, nil
}

func (s *Store) GetSegment(appID, id string) (*Segment, error) {
	row := s.store.DB().QueryRow(
		`SELECT id, app_id, arn, name, type, dimensions, version, created_at
		 FROM segments WHERE app_id = ? AND id = ?`, appID, id)
	return scanSegment(row)
}

func (s *Store) ListSegments(appID string) ([]Segment, error) {
	rows, err := s.store.DB().Query(
		`SELECT id, app_id, arn, name, type, dimensions, version, created_at
		 FROM segments WHERE app_id = ? ORDER BY created_at`, appID)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []Segment
	for rows.Next() {
		seg, err := scanSegment(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *seg)
	}
	return out, rows.Err()
}

func (s *Store) UpdateSegment(appID, id, name string) (*Segment, error) {
	_, err := s.store.DB().Exec(
		`UPDATE segments SET name = ?, version = version + 1 WHERE app_id = ? AND id = ?`,
		name, appID, id,
	)
	if err != nil {
		return nil, err
	}
	return s.GetSegment(appID, id)
}

func (s *Store) DeleteSegment(appID, id string) error {
	res, err := s.store.DB().Exec(`DELETE FROM segments WHERE app_id = ? AND id = ?`, appID, id)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errSegmentNotFound
	}
	return nil
}

func scanSegment(row scanner) (*Segment, error) {
	var seg Segment
	var ts int64
	err := row.Scan(&seg.ID, &seg.AppID, &seg.ARN, &seg.Name, &seg.Type, &seg.Dimensions, &seg.Version, &ts)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, errSegmentNotFound
	}
	if err != nil {
		return nil, err
	}
	seg.CreatedAt = time.Unix(ts, 0)
	return &seg, nil
}

// --- Journey ---

func (s *Store) CreateJourney(id, appID, arn, name string) (*Journey, error) {
	now := time.Now().Unix()
	_, err := s.store.DB().Exec(
		`INSERT INTO journeys (id, app_id, arn, name, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?)`,
		id, appID, arn, name, now, now,
	)
	if err != nil {
		return nil, err
	}
	return &Journey{
		ID: id, AppID: appID, ARN: arn, Name: name,
		State: "DRAFT", Activities: "{}", Schedule: "{}",
		CreatedAt: time.Unix(now, 0), UpdatedAt: time.Unix(now, 0),
	}, nil
}

func (s *Store) GetJourney(appID, id string) (*Journey, error) {
	row := s.store.DB().QueryRow(
		`SELECT id, app_id, arn, name, state, activities, schedule, created_at, updated_at
		 FROM journeys WHERE app_id = ? AND id = ?`, appID, id)
	return scanJourney(row)
}

func (s *Store) ListJourneys(appID string) ([]Journey, error) {
	rows, err := s.store.DB().Query(
		`SELECT id, app_id, arn, name, state, activities, schedule, created_at, updated_at
		 FROM journeys WHERE app_id = ? ORDER BY created_at`, appID)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []Journey
	for rows.Next() {
		j, err := scanJourney(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *j)
	}
	return out, rows.Err()
}

func (s *Store) UpdateJourney(appID, id, name string) (*Journey, error) {
	now := time.Now().Unix()
	_, err := s.store.DB().Exec(
		`UPDATE journeys SET name = ?, updated_at = ? WHERE app_id = ? AND id = ?`,
		name, now, appID, id,
	)
	if err != nil {
		return nil, err
	}
	return s.GetJourney(appID, id)
}

func (s *Store) UpdateJourneyState(appID, id, state string) (*Journey, error) {
	now := time.Now().Unix()
	_, err := s.store.DB().Exec(
		`UPDATE journeys SET state = ?, updated_at = ? WHERE app_id = ? AND id = ?`,
		state, now, appID, id,
	)
	if err != nil {
		return nil, err
	}
	return s.GetJourney(appID, id)
}

func (s *Store) DeleteJourney(appID, id string) error {
	res, err := s.store.DB().Exec(`DELETE FROM journeys WHERE app_id = ? AND id = ?`, appID, id)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errJourneyNotFound
	}
	return nil
}

func scanJourney(row scanner) (*Journey, error) {
	var j Journey
	var createdTs, updatedTs int64
	err := row.Scan(&j.ID, &j.AppID, &j.ARN, &j.Name, &j.State, &j.Activities, &j.Schedule, &createdTs, &updatedTs)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, errJourneyNotFound
	}
	if err != nil {
		return nil, err
	}
	j.CreatedAt = time.Unix(createdTs, 0)
	j.UpdatedAt = time.Unix(updatedTs, 0)
	return &j, nil
}

// --- Template ---

func (s *Store) CreateTemplate(name, tType, subject, htmlBody, textBody, body string) (*Template, error) {
	now := time.Now().Unix()
	_, err := s.store.DB().Exec(
		`INSERT INTO templates (name, type, subject, html_body, text_body, body, created_at, updated_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		name, tType, subject, htmlBody, textBody, body, now, now,
	)
	if err != nil {
		return nil, err
	}
	return &Template{
		Name: name, Type: tType, Subject: subject,
		HTMLBody: htmlBody, TextBody: textBody, Body: body,
		Version: "1", CreatedAt: time.Unix(now, 0), UpdatedAt: time.Unix(now, 0),
	}, nil
}

func (s *Store) GetTemplate(name, tType string) (*Template, error) {
	row := s.store.DB().QueryRow(
		`SELECT name, type, subject, html_body, text_body, body, version, created_at, updated_at
		 FROM templates WHERE name = ? AND type = ?`, name, tType)
	return scanTemplate(row)
}

func (s *Store) ListTemplates() ([]Template, error) {
	rows, err := s.store.DB().Query(
		`SELECT name, type, subject, html_body, text_body, body, version, created_at, updated_at
		 FROM templates ORDER BY created_at`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []Template
	for rows.Next() {
		t, err := scanTemplate(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *t)
	}
	return out, rows.Err()
}

func (s *Store) UpdateTemplate(name, tType, subject, htmlBody, textBody, body string) (*Template, error) {
	now := time.Now().Unix()
	_, err := s.store.DB().Exec(
		`UPDATE templates SET subject = ?, html_body = ?, text_body = ?, body = ?, updated_at = ?
		 WHERE name = ? AND type = ?`,
		subject, htmlBody, textBody, body, now, name, tType,
	)
	if err != nil {
		return nil, err
	}
	return s.GetTemplate(name, tType)
}

func (s *Store) DeleteTemplate(name, tType string) error {
	res, err := s.store.DB().Exec(`DELETE FROM templates WHERE name = ? AND type = ?`, name, tType)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errTemplateNotFound
	}
	return nil
}

func scanTemplate(row scanner) (*Template, error) {
	var t Template
	var createdTs, updatedTs int64
	err := row.Scan(&t.Name, &t.Type, &t.Subject, &t.HTMLBody, &t.TextBody, &t.Body, &t.Version, &createdTs, &updatedTs)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, errTemplateNotFound
	}
	if err != nil {
		return nil, err
	}
	t.CreatedAt = time.Unix(createdTs, 0)
	t.UpdatedAt = time.Unix(updatedTs, 0)
	return &t, nil
}

// --- Tags ---

func (s *Store) AddTags(arn string, tags map[string]string) error {
	return s.tags.AddTags(arn, tags)
}

func (s *Store) RemoveTags(arn string, keys []string) error {
	return s.tags.RemoveTags(arn, keys)
}

func (s *Store) ListTags(arn string) (map[string]string, error) {
	return s.tags.ListTags(arn)
}
