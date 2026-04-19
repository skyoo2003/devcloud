// SPDX-License-Identifier: Apache-2.0

// internal/services/mediaconvert/store.go
package mediaconvert

import (
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"time"

	"github.com/skyoo2003/devcloud/internal/storage/sqlite"
)

const region = "us-east-1"

var (
	errJobTemplateNotFound = errors.New("job template not found")
	errQueueNotFound       = errors.New("queue not found")
)

var migrations = []sqlite.Migration{
	{Version: 1, SQL: `
		CREATE TABLE IF NOT EXISTS mc_jobs (
			id         TEXT PRIMARY KEY,
			arn        TEXT NOT NULL,
			status     TEXT NOT NULL DEFAULT 'SUBMITTED',
			settings   TEXT NOT NULL DEFAULT '{}',
			queue      TEXT NOT NULL DEFAULT '',
			account_id TEXT NOT NULL,
			created_at INTEGER NOT NULL
		);
		CREATE TABLE IF NOT EXISTS mc_job_templates (
			name        TEXT NOT NULL,
			description TEXT NOT NULL DEFAULT '',
			category    TEXT NOT NULL DEFAULT '',
			arn         TEXT NOT NULL,
			settings    TEXT NOT NULL DEFAULT '{}',
			queue       TEXT NOT NULL DEFAULT '',
			account_id  TEXT NOT NULL,
			created_at  INTEGER NOT NULL,
			PRIMARY KEY (name, account_id)
		);
		CREATE TABLE IF NOT EXISTS mc_queues (
			name         TEXT NOT NULL,
			description  TEXT NOT NULL DEFAULT '',
			arn          TEXT NOT NULL,
			status       TEXT NOT NULL DEFAULT 'ACTIVE',
			pricing_plan TEXT NOT NULL DEFAULT 'ON_DEMAND',
			account_id   TEXT NOT NULL,
			created_at   INTEGER NOT NULL,
			PRIMARY KEY (name, account_id)
		);
	`},
	{Version: 2, SQL: `
		CREATE TABLE IF NOT EXISTS mc_presets (
			name        TEXT NOT NULL,
			description TEXT NOT NULL DEFAULT '',
			category    TEXT NOT NULL DEFAULT '',
			arn         TEXT NOT NULL,
			settings    TEXT NOT NULL DEFAULT '{}',
			account_id  TEXT NOT NULL,
			created_at  INTEGER NOT NULL,
			PRIMARY KEY (name, account_id)
		);
		CREATE TABLE IF NOT EXISTS mc_tags (
			arn        TEXT NOT NULL,
			key        TEXT NOT NULL,
			value      TEXT NOT NULL DEFAULT '',
			PRIMARY KEY (arn, key)
		);
		CREATE TABLE IF NOT EXISTS mc_policy (
			account_id TEXT PRIMARY KEY,
			policy     TEXT NOT NULL DEFAULT '{}'
		);
	`},
}

// MCPreset represents a MediaConvert transcoding preset.
type MCPreset struct {
	Name        string
	Description string
	Category    string
	ARN         string
	Settings    string
	AccountID   string
	CreatedAt   time.Time
}

var errPresetNotFound = errors.New("preset not found")

func presetARN(accountID, name string) string {
	return fmt.Sprintf("arn:aws:mediaconvert:%s:%s:presets/%s", region, accountID, name)
}

type MCJob struct {
	ID        string
	ARN       string
	Status    string
	Settings  string
	Queue     string
	AccountID string
	CreatedAt time.Time
}

type MCJobTemplate struct {
	Name        string
	Description string
	Category    string
	ARN         string
	Settings    string
	Queue       string
	AccountID   string
	CreatedAt   time.Time
}

type MCQueue struct {
	Name        string
	Description string
	ARN         string
	Status      string
	PricingPlan string
	AccountID   string
	CreatedAt   time.Time
}

type Store struct {
	store *sqlite.Store
}

func NewStore(dataDir string) (*Store, error) {
	dbPath := filepath.Join(dataDir, "mediaconvert.db")
	s, err := sqlite.Open(dbPath, migrations)
	if err != nil {
		return nil, err
	}
	return &Store{store: s}, nil
}

func (s *Store) Close() error { return s.store.Close() }
func (s *Store) db() *sql.DB  { return s.store.DB() }

func jobARN(accountID, id string) string {
	return fmt.Sprintf("arn:aws:mediaconvert:%s:%s:jobs/%s", region, accountID, id)
}

func jobTemplateARN(accountID, name string) string {
	return fmt.Sprintf("arn:aws:mediaconvert:%s:%s:jobTemplates/%s", region, accountID, name)
}

func queueARN(accountID, name string) string {
	return fmt.Sprintf("arn:aws:mediaconvert:%s:%s:queues/%s", region, accountID, name)
}

// --- Jobs ---

func (s *Store) CreateJob(accountID, id, settings, queue string) (*MCJob, error) {
	arn := jobARN(accountID, id)
	now := time.Now().Unix()
	_, err := s.db().Exec(
		`INSERT INTO mc_jobs (id, arn, status, settings, queue, account_id, created_at) VALUES (?, ?, 'SUBMITTED', ?, ?, ?, ?)`,
		id, arn, settings, queue, accountID, now,
	)
	if err != nil {
		return nil, err
	}
	return &MCJob{ID: id, ARN: arn, Status: "SUBMITTED", Settings: settings, Queue: queue, AccountID: accountID, CreatedAt: time.Unix(now, 0)}, nil
}

func (s *Store) GetJob(accountID, id string) (*MCJob, error) {
	var j MCJob
	var createdAt int64
	err := s.db().QueryRow(
		`SELECT id, arn, status, settings, queue, account_id, created_at FROM mc_jobs WHERE id=? AND account_id=?`,
		id, accountID,
	).Scan(&j.ID, &j.ARN, &j.Status, &j.Settings, &j.Queue, &j.AccountID, &createdAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, errJobTemplateNotFound
	}
	if err != nil {
		return nil, err
	}
	j.CreatedAt = time.Unix(createdAt, 0)
	return &j, nil
}

func (s *Store) ListJobs(accountID string) ([]MCJob, error) {
	rows, err := s.db().Query(
		`SELECT id, arn, status, settings, queue, account_id, created_at FROM mc_jobs WHERE account_id=? ORDER BY created_at DESC`,
		accountID,
	)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var jobs []MCJob
	for rows.Next() {
		var j MCJob
		var createdAt int64
		if err := rows.Scan(&j.ID, &j.ARN, &j.Status, &j.Settings, &j.Queue, &j.AccountID, &createdAt); err != nil {
			return nil, err
		}
		j.CreatedAt = time.Unix(createdAt, 0)
		jobs = append(jobs, j)
	}
	return jobs, rows.Err()
}

// --- Job Templates ---

func (s *Store) CreateJobTemplate(accountID, name, description, category, settings, queue string) (*MCJobTemplate, error) {
	arn := jobTemplateARN(accountID, name)
	now := time.Now().Unix()
	_, err := s.db().Exec(
		`INSERT INTO mc_job_templates (name, description, category, arn, settings, queue, account_id, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		name, description, category, arn, settings, queue, accountID, now,
	)
	if err != nil {
		return nil, err
	}
	return &MCJobTemplate{
		Name: name, Description: description, Category: category,
		ARN: arn, Settings: settings, Queue: queue,
		AccountID: accountID, CreatedAt: time.Unix(now, 0),
	}, nil
}

func (s *Store) GetJobTemplate(accountID, name string) (*MCJobTemplate, error) {
	var t MCJobTemplate
	var createdAt int64
	err := s.db().QueryRow(
		`SELECT name, description, category, arn, settings, queue, account_id, created_at FROM mc_job_templates WHERE name=? AND account_id=?`,
		name, accountID,
	).Scan(&t.Name, &t.Description, &t.Category, &t.ARN, &t.Settings, &t.Queue, &t.AccountID, &createdAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, errJobTemplateNotFound
	}
	if err != nil {
		return nil, err
	}
	t.CreatedAt = time.Unix(createdAt, 0)
	return &t, nil
}

func (s *Store) ListJobTemplates(accountID string) ([]MCJobTemplate, error) {
	rows, err := s.db().Query(
		`SELECT name, description, category, arn, settings, queue, account_id, created_at FROM mc_job_templates WHERE account_id=? ORDER BY name`,
		accountID,
	)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var templates []MCJobTemplate
	for rows.Next() {
		var t MCJobTemplate
		var createdAt int64
		if err := rows.Scan(&t.Name, &t.Description, &t.Category, &t.ARN, &t.Settings, &t.Queue, &t.AccountID, &createdAt); err != nil {
			return nil, err
		}
		t.CreatedAt = time.Unix(createdAt, 0)
		templates = append(templates, t)
	}
	return templates, rows.Err()
}

func (s *Store) DeleteJobTemplate(accountID, name string) error {
	res, err := s.db().Exec(`DELETE FROM mc_job_templates WHERE name=? AND account_id=?`, name, accountID)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errJobTemplateNotFound
	}
	return nil
}

// --- Queues ---

func (s *Store) CreateQueue(accountID, name, description, pricingPlan string) (*MCQueue, error) {
	arn := queueARN(accountID, name)
	now := time.Now().Unix()
	if pricingPlan == "" {
		pricingPlan = "ON_DEMAND"
	}
	_, err := s.db().Exec(
		`INSERT INTO mc_queues (name, description, arn, status, pricing_plan, account_id, created_at) VALUES (?, ?, ?, 'ACTIVE', ?, ?, ?)`,
		name, description, arn, pricingPlan, accountID, now,
	)
	if err != nil {
		return nil, err
	}
	return &MCQueue{
		Name: name, Description: description, ARN: arn,
		Status: "ACTIVE", PricingPlan: pricingPlan,
		AccountID: accountID, CreatedAt: time.Unix(now, 0),
	}, nil
}

func (s *Store) GetQueue(accountID, name string) (*MCQueue, error) {
	var q MCQueue
	var createdAt int64
	err := s.db().QueryRow(
		`SELECT name, description, arn, status, pricing_plan, account_id, created_at FROM mc_queues WHERE name=? AND account_id=?`,
		name, accountID,
	).Scan(&q.Name, &q.Description, &q.ARN, &q.Status, &q.PricingPlan, &q.AccountID, &createdAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, errQueueNotFound
	}
	if err != nil {
		return nil, err
	}
	q.CreatedAt = time.Unix(createdAt, 0)
	return &q, nil
}

func (s *Store) ListQueues(accountID string) ([]MCQueue, error) {
	rows, err := s.db().Query(
		`SELECT name, description, arn, status, pricing_plan, account_id, created_at FROM mc_queues WHERE account_id=? ORDER BY name`,
		accountID,
	)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var queues []MCQueue
	for rows.Next() {
		var q MCQueue
		var createdAt int64
		if err := rows.Scan(&q.Name, &q.Description, &q.ARN, &q.Status, &q.PricingPlan, &q.AccountID, &createdAt); err != nil {
			return nil, err
		}
		q.CreatedAt = time.Unix(createdAt, 0)
		queues = append(queues, q)
	}
	return queues, rows.Err()
}

func (s *Store) DeleteQueue(accountID, name string) error {
	res, err := s.db().Exec(`DELETE FROM mc_queues WHERE name=? AND account_id=?`, name, accountID)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errQueueNotFound
	}
	return nil
}

func (s *Store) UpdateQueue(accountID, name, description, pricingPlan string) (*MCQueue, error) {
	q, err := s.GetQueue(accountID, name)
	if err != nil {
		return nil, err
	}
	if description != "" {
		q.Description = description
	}
	if pricingPlan != "" {
		q.PricingPlan = pricingPlan
	}
	_, err = s.db().Exec(`UPDATE mc_queues SET description=?, pricing_plan=? WHERE name=? AND account_id=?`,
		q.Description, q.PricingPlan, name, accountID)
	if err != nil {
		return nil, err
	}
	return q, nil
}

func (s *Store) UpdateJobTemplate(accountID, name, description, category, settings, queue string) (*MCJobTemplate, error) {
	t, err := s.GetJobTemplate(accountID, name)
	if err != nil {
		return nil, err
	}
	if description != "" {
		t.Description = description
	}
	if category != "" {
		t.Category = category
	}
	if settings != "" && settings != "{}" {
		t.Settings = settings
	}
	if queue != "" {
		t.Queue = queue
	}
	_, err = s.db().Exec(
		`UPDATE mc_job_templates SET description=?, category=?, settings=?, queue=? WHERE name=? AND account_id=?`,
		t.Description, t.Category, t.Settings, t.Queue, name, accountID,
	)
	if err != nil {
		return nil, err
	}
	return t, nil
}

func (s *Store) CancelJob(accountID, id string) error {
	res, err := s.db().Exec(`UPDATE mc_jobs SET status='CANCELED' WHERE id=? AND account_id=?`, id, accountID)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errJobTemplateNotFound
	}
	return nil
}

// --- Presets ---

func (s *Store) CreatePreset(accountID, name, description, category, settings string) (*MCPreset, error) {
	arn := presetARN(accountID, name)
	now := time.Now().Unix()
	_, err := s.db().Exec(
		`INSERT INTO mc_presets (name, description, category, arn, settings, account_id, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)`,
		name, description, category, arn, settings, accountID, now,
	)
	if err != nil {
		return nil, err
	}
	return &MCPreset{
		Name: name, Description: description, Category: category,
		ARN: arn, Settings: settings, AccountID: accountID,
		CreatedAt: time.Unix(now, 0),
	}, nil
}

func (s *Store) GetPreset(accountID, name string) (*MCPreset, error) {
	var p MCPreset
	var createdAt int64
	err := s.db().QueryRow(
		`SELECT name, description, category, arn, settings, account_id, created_at FROM mc_presets WHERE name=? AND account_id=?`,
		name, accountID,
	).Scan(&p.Name, &p.Description, &p.Category, &p.ARN, &p.Settings, &p.AccountID, &createdAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, errPresetNotFound
	}
	if err != nil {
		return nil, err
	}
	p.CreatedAt = time.Unix(createdAt, 0)
	return &p, nil
}

func (s *Store) ListPresets(accountID string) ([]MCPreset, error) {
	rows, err := s.db().Query(
		`SELECT name, description, category, arn, settings, account_id, created_at FROM mc_presets WHERE account_id=? ORDER BY name`,
		accountID,
	)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var presets []MCPreset
	for rows.Next() {
		var p MCPreset
		var createdAt int64
		if err := rows.Scan(&p.Name, &p.Description, &p.Category, &p.ARN, &p.Settings, &p.AccountID, &createdAt); err != nil {
			return nil, err
		}
		p.CreatedAt = time.Unix(createdAt, 0)
		presets = append(presets, p)
	}
	return presets, rows.Err()
}

func (s *Store) DeletePreset(accountID, name string) error {
	res, err := s.db().Exec(`DELETE FROM mc_presets WHERE name=? AND account_id=?`, name, accountID)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errPresetNotFound
	}
	return nil
}

func (s *Store) UpdatePreset(accountID, name, description, category, settings string) (*MCPreset, error) {
	p, err := s.GetPreset(accountID, name)
	if err != nil {
		return nil, err
	}
	if description != "" {
		p.Description = description
	}
	if category != "" {
		p.Category = category
	}
	if settings != "" && settings != "{}" {
		p.Settings = settings
	}
	_, err = s.db().Exec(
		`UPDATE mc_presets SET description=?, category=?, settings=? WHERE name=? AND account_id=?`,
		p.Description, p.Category, p.Settings, name, accountID,
	)
	if err != nil {
		return nil, err
	}
	return p, nil
}

// --- Tags ---

func (s *Store) AddTags(arn string, tags map[string]string) error {
	for k, v := range tags {
		_, err := s.db().Exec(
			`INSERT INTO mc_tags (arn, key, value) VALUES (?, ?, ?) ON CONFLICT(arn, key) DO UPDATE SET value=excluded.value`,
			arn, k, v,
		)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) RemoveTags(arn string, keys []string) error {
	for _, k := range keys {
		if _, err := s.db().Exec(`DELETE FROM mc_tags WHERE arn=? AND key=?`, arn, k); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) ListTags(arn string) (map[string]string, error) {
	rows, err := s.db().Query(`SELECT key, value FROM mc_tags WHERE arn=?`, arn)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	tags := map[string]string{}
	for rows.Next() {
		var k, v string
		if err := rows.Scan(&k, &v); err != nil {
			return nil, err
		}
		tags[k] = v
	}
	return tags, rows.Err()
}

// --- Policy ---

func (s *Store) GetPolicy(accountID string) (string, error) {
	var p string
	err := s.db().QueryRow(`SELECT policy FROM mc_policy WHERE account_id=?`, accountID).Scan(&p)
	if errors.Is(err, sql.ErrNoRows) {
		return "{}", nil
	}
	return p, err
}

func (s *Store) PutPolicy(accountID, policy string) error {
	_, err := s.db().Exec(
		`INSERT INTO mc_policy (account_id, policy) VALUES (?, ?) ON CONFLICT(account_id) DO UPDATE SET policy=excluded.policy`,
		accountID, policy,
	)
	return err
}

func (s *Store) DeletePolicy(accountID string) error {
	_, err := s.db().Exec(`DELETE FROM mc_policy WHERE account_id=?`, accountID)
	return err
}
