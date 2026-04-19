// SPDX-License-Identifier: Apache-2.0

// internal/services/transcribe/store.go
package transcribe

import (
	"database/sql"
	"errors"
	"path/filepath"
	"time"

	"github.com/skyoo2003/devcloud/internal/shared"
	"github.com/skyoo2003/devcloud/internal/storage/sqlite"
)

var (
	errNotFound = errors.New("not found")
)

var migrations = []sqlite.Migration{
	{Version: 1, SQL: `
		CREATE TABLE IF NOT EXISTS transcription_jobs (
			name        TEXT PRIMARY KEY,
			status      TEXT NOT NULL DEFAULT 'COMPLETED',
			language    TEXT NOT NULL DEFAULT 'en-US',
			media_uri   TEXT NOT NULL DEFAULT '',
			media_format TEXT NOT NULL DEFAULT 'mp4',
			output_uri  TEXT NOT NULL DEFAULT '',
			created_at  INTEGER NOT NULL,
			completed_at INTEGER NOT NULL DEFAULT 0
		);
		CREATE TABLE IF NOT EXISTS vocabularies (
			name        TEXT PRIMARY KEY,
			language    TEXT NOT NULL DEFAULT 'en-US',
			status      TEXT NOT NULL DEFAULT 'READY',
			phrases     TEXT NOT NULL DEFAULT '[]',
			created_at  INTEGER NOT NULL,
			updated_at  INTEGER NOT NULL
		);
		CREATE TABLE IF NOT EXISTS vocabulary_filters (
			name        TEXT PRIMARY KEY,
			language    TEXT NOT NULL DEFAULT 'en-US',
			words       TEXT NOT NULL DEFAULT '[]',
			created_at  INTEGER NOT NULL,
			updated_at  INTEGER NOT NULL
		);
		CREATE TABLE IF NOT EXISTS language_models (
			name        TEXT PRIMARY KEY,
			status      TEXT NOT NULL DEFAULT 'COMPLETED',
			language    TEXT NOT NULL DEFAULT 'en-US',
			base_model  TEXT NOT NULL DEFAULT 'NarrowBand',
			created_at  INTEGER NOT NULL
		);
		CREATE TABLE IF NOT EXISTS call_analytics_categories (
			name        TEXT PRIMARY KEY,
			rules       TEXT NOT NULL DEFAULT '[]',
			input_type  TEXT NOT NULL DEFAULT 'REAL_TIME',
			created_at  INTEGER NOT NULL,
			updated_at  INTEGER NOT NULL
		);
	`},
}

// ---- Model types ----

type TranscriptionJob struct {
	Name        string
	Status      string
	Language    string
	MediaURI    string
	MediaFormat string
	OutputURI   string
	CreatedAt   time.Time
	CompletedAt time.Time
}

type Vocabulary struct {
	Name      string
	Language  string
	Status    string
	Phrases   string
	CreatedAt time.Time
	UpdatedAt time.Time
}

type VocabularyFilter struct {
	Name      string
	Language  string
	Words     string
	CreatedAt time.Time
	UpdatedAt time.Time
}

type LanguageModel struct {
	Name      string
	Status    string
	Language  string
	BaseModel string
	CreatedAt time.Time
}

type CallAnalyticsCategory struct {
	Name      string
	Rules     string
	InputType string
	CreatedAt time.Time
	UpdatedAt time.Time
}

// ---- Store ----

type Store struct {
	store *sqlite.Store
	tags  *shared.TagStore
}

func NewStore(dataDir string) (*Store, error) {
	allMigrations := append(migrations, shared.TagMigrations...)
	dbPath := filepath.Join(dataDir, "transcribe.db")
	s, err := sqlite.Open(dbPath, allMigrations)
	if err != nil {
		return nil, err
	}
	return &Store{store: s, tags: shared.NewTagStore(s)}, nil
}

func (s *Store) Close() error { return s.store.Close() }

// ---- TranscriptionJob ----

func (s *Store) CreateTranscriptionJob(name, language, mediaURI, mediaFormat string) (*TranscriptionJob, error) {
	now := time.Now().Unix()
	_, err := s.store.DB().Exec(
		`INSERT INTO transcription_jobs (name, language, media_uri, media_format, created_at, completed_at)
		 VALUES (?, ?, ?, ?, ?, ?)`,
		name, language, mediaURI, mediaFormat, now, now,
	)
	if err != nil {
		return nil, err
	}
	return &TranscriptionJob{
		Name: name, Status: "COMPLETED", Language: language,
		MediaURI: mediaURI, MediaFormat: mediaFormat,
		CreatedAt: time.Unix(now, 0), CompletedAt: time.Unix(now, 0),
	}, nil
}

func (s *Store) GetTranscriptionJob(name string) (*TranscriptionJob, error) {
	row := s.store.DB().QueryRow(
		`SELECT name, status, language, media_uri, media_format, output_uri, created_at, completed_at
		 FROM transcription_jobs WHERE name = ?`, name)
	return scanTranscriptionJob(row)
}

func (s *Store) ListTranscriptionJobs(statusFilter string) ([]TranscriptionJob, error) {
	query := `SELECT name, status, language, media_uri, media_format, output_uri, created_at, completed_at FROM transcription_jobs`
	var args []any
	if statusFilter != "" {
		query += ` WHERE status = ?`
		args = append(args, statusFilter)
	}
	query += ` ORDER BY created_at`
	rows, err := s.store.DB().Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var jobs []TranscriptionJob
	for rows.Next() {
		j, err := scanTranscriptionJob(rows)
		if err != nil {
			return nil, err
		}
		jobs = append(jobs, *j)
	}
	return jobs, rows.Err()
}

func (s *Store) DeleteTranscriptionJob(name string) error {
	res, err := s.store.DB().Exec(`DELETE FROM transcription_jobs WHERE name = ?`, name)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errNotFound
	}
	return nil
}

type scanner interface{ Scan(dest ...any) error }

func scanTranscriptionJob(sc scanner) (*TranscriptionJob, error) {
	var j TranscriptionJob
	var createdAt, completedAt int64
	err := sc.Scan(&j.Name, &j.Status, &j.Language, &j.MediaURI, &j.MediaFormat, &j.OutputURI, &createdAt, &completedAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errNotFound
		}
		return nil, err
	}
	j.CreatedAt = time.Unix(createdAt, 0)
	j.CompletedAt = time.Unix(completedAt, 0)
	return &j, nil
}

// ---- Vocabulary ----

func (s *Store) CreateVocabulary(name, language, phrases string) (*Vocabulary, error) {
	now := time.Now().Unix()
	_, err := s.store.DB().Exec(
		`INSERT INTO vocabularies (name, language, phrases, created_at, updated_at) VALUES (?, ?, ?, ?, ?)`,
		name, language, phrases, now, now,
	)
	if err != nil {
		return nil, err
	}
	return &Vocabulary{
		Name: name, Language: language, Status: "READY", Phrases: phrases,
		CreatedAt: time.Unix(now, 0), UpdatedAt: time.Unix(now, 0),
	}, nil
}

func (s *Store) GetVocabulary(name string) (*Vocabulary, error) {
	row := s.store.DB().QueryRow(
		`SELECT name, language, status, phrases, created_at, updated_at FROM vocabularies WHERE name = ?`, name)
	return scanVocabulary(row)
}

func (s *Store) ListVocabularies(language string) ([]Vocabulary, error) {
	query := `SELECT name, language, status, phrases, created_at, updated_at FROM vocabularies`
	var args []any
	if language != "" {
		query += ` WHERE language = ?`
		args = append(args, language)
	}
	query += ` ORDER BY created_at`
	rows, err := s.store.DB().Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var vocabs []Vocabulary
	for rows.Next() {
		v, err := scanVocabulary(rows)
		if err != nil {
			return nil, err
		}
		vocabs = append(vocabs, *v)
	}
	return vocabs, rows.Err()
}

func (s *Store) UpdateVocabulary(name, language, phrases string) error {
	now := time.Now().Unix()
	res, err := s.store.DB().Exec(
		`UPDATE vocabularies SET language = ?, phrases = ?, updated_at = ? WHERE name = ?`,
		language, phrases, now, name,
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

func (s *Store) DeleteVocabulary(name string) error {
	res, err := s.store.DB().Exec(`DELETE FROM vocabularies WHERE name = ?`, name)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errNotFound
	}
	return nil
}

func scanVocabulary(sc scanner) (*Vocabulary, error) {
	var v Vocabulary
	var createdAt, updatedAt int64
	err := sc.Scan(&v.Name, &v.Language, &v.Status, &v.Phrases, &createdAt, &updatedAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errNotFound
		}
		return nil, err
	}
	v.CreatedAt = time.Unix(createdAt, 0)
	v.UpdatedAt = time.Unix(updatedAt, 0)
	return &v, nil
}

// ---- VocabularyFilter ----

func (s *Store) CreateVocabularyFilter(name, language, words string) (*VocabularyFilter, error) {
	now := time.Now().Unix()
	_, err := s.store.DB().Exec(
		`INSERT INTO vocabulary_filters (name, language, words, created_at, updated_at) VALUES (?, ?, ?, ?, ?)`,
		name, language, words, now, now,
	)
	if err != nil {
		return nil, err
	}
	return &VocabularyFilter{
		Name: name, Language: language, Words: words,
		CreatedAt: time.Unix(now, 0), UpdatedAt: time.Unix(now, 0),
	}, nil
}

func (s *Store) GetVocabularyFilter(name string) (*VocabularyFilter, error) {
	row := s.store.DB().QueryRow(
		`SELECT name, language, words, created_at, updated_at FROM vocabulary_filters WHERE name = ?`, name)
	return scanVocabularyFilter(row)
}

func (s *Store) ListVocabularyFilters(language string) ([]VocabularyFilter, error) {
	query := `SELECT name, language, words, created_at, updated_at FROM vocabulary_filters`
	var args []any
	if language != "" {
		query += ` WHERE language = ?`
		args = append(args, language)
	}
	query += ` ORDER BY created_at`
	rows, err := s.store.DB().Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var filters []VocabularyFilter
	for rows.Next() {
		f, err := scanVocabularyFilter(rows)
		if err != nil {
			return nil, err
		}
		filters = append(filters, *f)
	}
	return filters, rows.Err()
}

func (s *Store) UpdateVocabularyFilter(name, language, words string) error {
	now := time.Now().Unix()
	res, err := s.store.DB().Exec(
		`UPDATE vocabulary_filters SET language = ?, words = ?, updated_at = ? WHERE name = ?`,
		language, words, now, name,
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

func (s *Store) DeleteVocabularyFilter(name string) error {
	res, err := s.store.DB().Exec(`DELETE FROM vocabulary_filters WHERE name = ?`, name)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errNotFound
	}
	return nil
}

func scanVocabularyFilter(sc scanner) (*VocabularyFilter, error) {
	var f VocabularyFilter
	var createdAt, updatedAt int64
	err := sc.Scan(&f.Name, &f.Language, &f.Words, &createdAt, &updatedAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errNotFound
		}
		return nil, err
	}
	f.CreatedAt = time.Unix(createdAt, 0)
	f.UpdatedAt = time.Unix(updatedAt, 0)
	return &f, nil
}

// ---- LanguageModel ----

func (s *Store) CreateLanguageModel(name, language, baseModel string) (*LanguageModel, error) {
	now := time.Now().Unix()
	_, err := s.store.DB().Exec(
		`INSERT INTO language_models (name, language, base_model, created_at) VALUES (?, ?, ?, ?)`,
		name, language, baseModel, now,
	)
	if err != nil {
		return nil, err
	}
	return &LanguageModel{
		Name: name, Status: "COMPLETED", Language: language,
		BaseModel: baseModel, CreatedAt: time.Unix(now, 0),
	}, nil
}

func (s *Store) GetLanguageModel(name string) (*LanguageModel, error) {
	row := s.store.DB().QueryRow(
		`SELECT name, status, language, base_model, created_at FROM language_models WHERE name = ?`, name)
	return scanLanguageModel(row)
}

func (s *Store) ListLanguageModels(language string) ([]LanguageModel, error) {
	query := `SELECT name, status, language, base_model, created_at FROM language_models`
	var args []any
	if language != "" {
		query += ` WHERE language = ?`
		args = append(args, language)
	}
	query += ` ORDER BY created_at`
	rows, err := s.store.DB().Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var models []LanguageModel
	for rows.Next() {
		m, err := scanLanguageModel(rows)
		if err != nil {
			return nil, err
		}
		models = append(models, *m)
	}
	return models, rows.Err()
}

func (s *Store) DeleteLanguageModel(name string) error {
	res, err := s.store.DB().Exec(`DELETE FROM language_models WHERE name = ?`, name)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errNotFound
	}
	return nil
}

func scanLanguageModel(sc scanner) (*LanguageModel, error) {
	var m LanguageModel
	var createdAt int64
	err := sc.Scan(&m.Name, &m.Status, &m.Language, &m.BaseModel, &createdAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errNotFound
		}
		return nil, err
	}
	m.CreatedAt = time.Unix(createdAt, 0)
	return &m, nil
}

// ---- CallAnalyticsCategory ----

func (s *Store) CreateCallAnalyticsCategory(name, rules, inputType string) (*CallAnalyticsCategory, error) {
	now := time.Now().Unix()
	_, err := s.store.DB().Exec(
		`INSERT INTO call_analytics_categories (name, rules, input_type, created_at, updated_at) VALUES (?, ?, ?, ?, ?)`,
		name, rules, inputType, now, now,
	)
	if err != nil {
		return nil, err
	}
	return &CallAnalyticsCategory{
		Name: name, Rules: rules, InputType: inputType,
		CreatedAt: time.Unix(now, 0), UpdatedAt: time.Unix(now, 0),
	}, nil
}

func (s *Store) GetCallAnalyticsCategory(name string) (*CallAnalyticsCategory, error) {
	row := s.store.DB().QueryRow(
		`SELECT name, rules, input_type, created_at, updated_at FROM call_analytics_categories WHERE name = ?`, name)
	return scanCallAnalyticsCategory(row)
}

func (s *Store) ListCallAnalyticsCategories() ([]CallAnalyticsCategory, error) {
	rows, err := s.store.DB().Query(
		`SELECT name, rules, input_type, created_at, updated_at FROM call_analytics_categories ORDER BY created_at`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var cats []CallAnalyticsCategory
	for rows.Next() {
		c, err := scanCallAnalyticsCategory(rows)
		if err != nil {
			return nil, err
		}
		cats = append(cats, *c)
	}
	return cats, rows.Err()
}

func (s *Store) UpdateCallAnalyticsCategory(name, rules, inputType string) error {
	now := time.Now().Unix()
	res, err := s.store.DB().Exec(
		`UPDATE call_analytics_categories SET rules = ?, input_type = ?, updated_at = ? WHERE name = ?`,
		rules, inputType, now, name,
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

func (s *Store) DeleteCallAnalyticsCategory(name string) error {
	res, err := s.store.DB().Exec(`DELETE FROM call_analytics_categories WHERE name = ?`, name)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errNotFound
	}
	return nil
}

func scanCallAnalyticsCategory(sc scanner) (*CallAnalyticsCategory, error) {
	var c CallAnalyticsCategory
	var createdAt, updatedAt int64
	err := sc.Scan(&c.Name, &c.Rules, &c.InputType, &createdAt, &updatedAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errNotFound
		}
		return nil, err
	}
	c.CreatedAt = time.Unix(createdAt, 0)
	c.UpdatedAt = time.Unix(updatedAt, 0)
	return &c, nil
}
