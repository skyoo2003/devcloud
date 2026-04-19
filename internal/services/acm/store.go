// SPDX-License-Identifier: Apache-2.0

// internal/services/acm/store.go
package acm

import (
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"time"

	"github.com/skyoo2003/devcloud/internal/storage/sqlite"
)

var (
	ErrCertNotFound = errors.New("certificate not found")
)

var migrations = []sqlite.Migration{
	{Version: 1, SQL: `
		CREATE TABLE IF NOT EXISTS certificates (
			arn        TEXT PRIMARY KEY,
			domain_name TEXT NOT NULL,
			account_id  TEXT NOT NULL,
			status      TEXT NOT NULL DEFAULT 'ISSUED',
			type        TEXT NOT NULL DEFAULT 'AMAZON_ISSUED',
			key_algo    TEXT NOT NULL DEFAULT 'EC_prime256v1',
			serial      TEXT NOT NULL DEFAULT '',
			not_before  INTEGER NOT NULL DEFAULT 0,
			not_after   INTEGER NOT NULL DEFAULT 0,
			cert_pem    TEXT NOT NULL DEFAULT '',
			key_pem     TEXT NOT NULL DEFAULT '',
			created_at  INTEGER NOT NULL
		);
		CREATE TABLE IF NOT EXISTS cert_tags (
			cert_arn TEXT NOT NULL,
			key      TEXT NOT NULL,
			value    TEXT NOT NULL DEFAULT '',
			PRIMARY KEY (cert_arn, key)
		);
	`},
	{Version: 2, SQL: `
		CREATE TABLE IF NOT EXISTS acm_account_config (
			account_id         TEXT PRIMARY KEY,
			days_before_expiry INTEGER DEFAULT 45
		);
	`},
}

type Certificate struct {
	ARN        string
	DomainName string
	AccountID  string
	Status     string
	Type       string
	KeyAlgo    string
	Serial     string
	NotBefore  time.Time
	NotAfter   time.Time
	CertPEM    string
	KeyPEM     string
	CreatedAt  time.Time
}

type CertTag struct {
	Key   string
	Value string
}

type ACMStore struct {
	store *sqlite.Store
}

func NewACMStore(dataDir string) (*ACMStore, error) {
	dbPath := filepath.Join(dataDir, "acm.db")
	s, err := sqlite.Open(dbPath, migrations)
	if err != nil {
		return nil, err
	}
	return &ACMStore{store: s}, nil
}

func (s *ACMStore) Close() error { return s.store.Close() }

func (s *ACMStore) PutCertificate(cert *Certificate) error {
	_, err := s.store.DB().Exec(`
		INSERT INTO certificates
		  (arn, domain_name, account_id, status, type, key_algo, serial, not_before, not_after, cert_pem, key_pem, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(arn) DO UPDATE SET
		  status=excluded.status, cert_pem=excluded.cert_pem, key_pem=excluded.key_pem,
		  serial=excluded.serial, not_before=excluded.not_before, not_after=excluded.not_after,
		  type=excluded.type, key_algo=excluded.key_algo`,
		cert.ARN, cert.DomainName, cert.AccountID, cert.Status, cert.Type, cert.KeyAlgo,
		cert.Serial, cert.NotBefore.Unix(), cert.NotAfter.Unix(), cert.CertPEM, cert.KeyPEM,
		cert.CreatedAt.Unix(),
	)
	return err
}

func (s *ACMStore) GetCertificate(arn, accountID string) (*Certificate, error) {
	row := s.store.DB().QueryRow(`
		SELECT arn, domain_name, account_id, status, type, key_algo, serial,
		       not_before, not_after, cert_pem, key_pem, created_at
		FROM certificates WHERE arn = ? AND account_id = ?`, arn, accountID)
	return scanCert(row)
}

func (s *ACMStore) ListCertificates(accountID string) ([]Certificate, error) {
	rows, err := s.store.DB().Query(`
		SELECT arn, domain_name, account_id, status, type, key_algo, serial,
		       not_before, not_after, cert_pem, key_pem, created_at
		FROM certificates WHERE account_id = ? ORDER BY created_at`, accountID)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var certs []Certificate
	for rows.Next() {
		c, err := scanCert(rows)
		if err != nil {
			return nil, err
		}
		certs = append(certs, *c)
	}
	return certs, rows.Err()
}

func (s *ACMStore) DeleteCertificate(arn, accountID string) error {
	res, err := s.store.DB().Exec(
		`DELETE FROM certificates WHERE arn = ? AND account_id = ?`, arn, accountID)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrCertNotFound
	}
	_, _ = s.store.DB().Exec(`DELETE FROM cert_tags WHERE cert_arn = ?`, arn)
	return nil
}

func (s *ACMStore) AddTags(arn string, tags []CertTag) error {
	for _, t := range tags {
		_, err := s.store.DB().Exec(`
			INSERT INTO cert_tags (cert_arn, key, value) VALUES (?, ?, ?)
			ON CONFLICT(cert_arn, key) DO UPDATE SET value=excluded.value`,
			arn, t.Key, t.Value)
		if err != nil {
			return fmt.Errorf("add tag %q: %w", t.Key, err)
		}
	}
	return nil
}

func (s *ACMStore) ListTags(arn string) ([]CertTag, error) {
	rows, err := s.store.DB().Query(
		`SELECT key, value FROM cert_tags WHERE cert_arn = ? ORDER BY key`, arn)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var tags []CertTag
	for rows.Next() {
		var t CertTag
		if err := rows.Scan(&t.Key, &t.Value); err != nil {
			return nil, err
		}
		tags = append(tags, t)
	}
	return tags, rows.Err()
}

func (s *ACMStore) RemoveTags(arn string, tags []CertTag) error {
	for _, t := range tags {
		_, err := s.store.DB().Exec(
			`DELETE FROM cert_tags WHERE cert_arn = ? AND key = ?`, arn, t.Key)
		if err != nil {
			return err
		}
	}
	return nil
}

// --- Account Configuration ---

func (s *ACMStore) GetAccountConfig(accountID string) (int, error) {
	var days int
	err := s.store.DB().QueryRow(
		`SELECT days_before_expiry FROM acm_account_config WHERE account_id = ?`, accountID,
	).Scan(&days)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 45, nil // default
		}
		return 0, err
	}
	return days, nil
}

func (s *ACMStore) PutAccountConfig(accountID string, daysBeforeExpiry int) error {
	_, err := s.store.DB().Exec(
		`INSERT INTO acm_account_config (account_id, days_before_expiry) VALUES (?, ?)
		 ON CONFLICT(account_id) DO UPDATE SET days_before_expiry=excluded.days_before_expiry`,
		accountID, daysBeforeExpiry,
	)
	return err
}

type certScanner interface {
	Scan(dest ...any) error
}

func scanCert(s certScanner) (*Certificate, error) {
	var c Certificate
	var notBefore, notAfter, createdAt int64
	err := s.Scan(&c.ARN, &c.DomainName, &c.AccountID, &c.Status, &c.Type, &c.KeyAlgo,
		&c.Serial, &notBefore, &notAfter, &c.CertPEM, &c.KeyPEM, &createdAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrCertNotFound
		}
		return nil, err
	}
	c.NotBefore = time.Unix(notBefore, 0)
	c.NotAfter = time.Unix(notAfter, 0)
	c.CreatedAt = time.Unix(createdAt, 0)
	return &c, nil
}
