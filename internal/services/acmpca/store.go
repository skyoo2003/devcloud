// SPDX-License-Identifier: Apache-2.0

package acmpca

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"time"

	"github.com/skyoo2003/devcloud/internal/storage/sqlite"
)

var (
	ErrCANotFound   = errors.New("certificate authority not found")
	ErrCertNotFound = errors.New("certificate not found")
	ErrPermNotFound = errors.New("permission not found")
)

var migrations = []sqlite.Migration{
	{Version: 1, SQL: `
		CREATE TABLE IF NOT EXISTS certificate_authorities (
			arn          TEXT PRIMARY KEY,
			serial       TEXT NOT NULL DEFAULT '',
			type         TEXT NOT NULL DEFAULT 'ROOT',
			key_algo     TEXT NOT NULL DEFAULT 'RSA_2048',
			signing_algo TEXT NOT NULL DEFAULT 'SHA256WITHRSA',
			subject      TEXT NOT NULL DEFAULT '{}',
			status       TEXT NOT NULL DEFAULT 'ACTIVE',
			usage_mode   TEXT NOT NULL DEFAULT 'SHORT_LIVED_CERTIFICATE',
			csr          TEXT NOT NULL DEFAULT '',
			cert_pem     TEXT NOT NULL DEFAULT '',
			cert_chain   TEXT NOT NULL DEFAULT '',
			policy       TEXT NOT NULL DEFAULT '',
			created_at   INTEGER NOT NULL
		);
		CREATE TABLE IF NOT EXISTS issued_certificates (
			arn        TEXT PRIMARY KEY,
			ca_arn     TEXT NOT NULL,
			cert_pem   TEXT NOT NULL DEFAULT '',
			cert_chain TEXT NOT NULL DEFAULT '',
			created_at INTEGER NOT NULL
		);
		CREATE TABLE IF NOT EXISTS permissions (
			ca_arn    TEXT NOT NULL,
			principal TEXT NOT NULL,
			actions   TEXT NOT NULL DEFAULT '[]',
			PRIMARY KEY (ca_arn, principal)
		);
		CREATE TABLE IF NOT EXISTS ca_tags (
			ca_arn TEXT NOT NULL,
			key    TEXT NOT NULL,
			value  TEXT NOT NULL DEFAULT '',
			PRIMARY KEY (ca_arn, key)
		);
	`},
}

type CertificateAuthority struct {
	ARN         string
	Serial      string
	Type        string
	KeyAlgo     string
	SigningAlgo string
	Subject     map[string]string
	Status      string
	UsageMode   string
	CSR         string
	CertPEM     string
	CertChain   string
	Policy      string
	CreatedAt   time.Time
}

type IssuedCertificate struct {
	ARN       string
	CAARN     string
	CertPEM   string
	CertChain string
	CreatedAt time.Time
}

type Permission struct {
	CAARN     string
	Principal string
	Actions   []string
}

type ACMPCAStore struct {
	store *sqlite.Store
}

func NewACMPCAStore(dataDir string) (*ACMPCAStore, error) {
	dbPath := filepath.Join(dataDir, "acmpca.db")
	s, err := sqlite.Open(dbPath, migrations)
	if err != nil {
		return nil, err
	}
	return &ACMPCAStore{store: s}, nil
}

func (s *ACMPCAStore) Close() error { return s.store.Close() }

func (s *ACMPCAStore) PutCA(ca *CertificateAuthority) error {
	subject, _ := json.Marshal(ca.Subject)
	_, err := s.store.DB().Exec(`
		INSERT INTO certificate_authorities
		  (arn, serial, type, key_algo, signing_algo, subject, status, usage_mode, csr, cert_pem, cert_chain, policy, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(arn) DO UPDATE SET
		  serial=excluded.serial, status=excluded.status,
		  csr=excluded.csr, cert_pem=excluded.cert_pem, cert_chain=excluded.cert_chain,
		  policy=excluded.policy`,
		ca.ARN, ca.Serial, ca.Type, ca.KeyAlgo, ca.SigningAlgo,
		string(subject), ca.Status, ca.UsageMode,
		ca.CSR, ca.CertPEM, ca.CertChain, ca.Policy,
		ca.CreatedAt.Unix(),
	)
	return err
}

func (s *ACMPCAStore) GetCA(arn string) (*CertificateAuthority, error) {
	row := s.store.DB().QueryRow(`
		SELECT arn, serial, type, key_algo, signing_algo, subject, status, usage_mode,
		       csr, cert_pem, cert_chain, policy, created_at
		FROM certificate_authorities WHERE arn = ?`, arn)
	return scanCA(row)
}

func (s *ACMPCAStore) ListCAs() ([]CertificateAuthority, error) {
	rows, err := s.store.DB().Query(`
		SELECT arn, serial, type, key_algo, signing_algo, subject, status, usage_mode,
		       csr, cert_pem, cert_chain, policy, created_at
		FROM certificate_authorities ORDER BY created_at`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var cas []CertificateAuthority
	for rows.Next() {
		ca, err := scanCA(rows)
		if err != nil {
			return nil, err
		}
		cas = append(cas, *ca)
	}
	return cas, rows.Err()
}

func (s *ACMPCAStore) DeleteCA(arn string) error {
	res, err := s.store.DB().Exec(`DELETE FROM certificate_authorities WHERE arn = ?`, arn)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrCANotFound
	}
	return nil
}

func (s *ACMPCAStore) PutCert(cert *IssuedCertificate) error {
	_, err := s.store.DB().Exec(`
		INSERT INTO issued_certificates (arn, ca_arn, cert_pem, cert_chain, created_at)
		VALUES (?, ?, ?, ?, ?)
		ON CONFLICT(arn) DO UPDATE SET cert_pem=excluded.cert_pem, cert_chain=excluded.cert_chain`,
		cert.ARN, cert.CAARN, cert.CertPEM, cert.CertChain, cert.CreatedAt.Unix(),
	)
	return err
}

func (s *ACMPCAStore) GetCert(arn string) (*IssuedCertificate, error) {
	row := s.store.DB().QueryRow(`
		SELECT arn, ca_arn, cert_pem, cert_chain, created_at
		FROM issued_certificates WHERE arn = ?`, arn)
	return scanCert(row)
}

func (s *ACMPCAStore) PutPermission(perm *Permission) error {
	actions, _ := json.Marshal(perm.Actions)
	_, err := s.store.DB().Exec(`
		INSERT INTO permissions (ca_arn, principal, actions) VALUES (?, ?, ?)
		ON CONFLICT(ca_arn, principal) DO UPDATE SET actions=excluded.actions`,
		perm.CAARN, perm.Principal, string(actions),
	)
	return err
}

func (s *ACMPCAStore) ListPermissions(caARN string) ([]Permission, error) {
	rows, err := s.store.DB().Query(`
		SELECT ca_arn, principal, actions FROM permissions WHERE ca_arn = ?`, caARN)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var perms []Permission
	for rows.Next() {
		var p Permission
		var actions string
		if err := rows.Scan(&p.CAARN, &p.Principal, &actions); err != nil {
			return nil, err
		}
		_ = json.Unmarshal([]byte(actions), &p.Actions)
		if p.Actions == nil {
			p.Actions = []string{}
		}
		perms = append(perms, p)
	}
	return perms, rows.Err()
}

func (s *ACMPCAStore) DeletePermission(caARN, principal string) error {
	res, err := s.store.DB().Exec(
		`DELETE FROM permissions WHERE ca_arn = ? AND principal = ?`, caARN, principal)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrPermNotFound
	}
	return nil
}

func (s *ACMPCAStore) AddTags(caARN string, tags []Tag) error {
	for _, t := range tags {
		_, err := s.store.DB().Exec(`
			INSERT INTO ca_tags (ca_arn, key, value) VALUES (?, ?, ?)
			ON CONFLICT(ca_arn, key) DO UPDATE SET value=excluded.value`,
			caARN, t.Key, t.Value)
		if err != nil {
			return fmt.Errorf("add tag %q: %w", t.Key, err)
		}
	}
	return nil
}

func (s *ACMPCAStore) ListTags(caARN string) ([]Tag, error) {
	rows, err := s.store.DB().Query(
		`SELECT key, value FROM ca_tags WHERE ca_arn = ? ORDER BY key`, caARN)
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

func (s *ACMPCAStore) RemoveTags(caARN string, tags []Tag) error {
	for _, t := range tags {
		_, err := s.store.DB().Exec(
			`DELETE FROM ca_tags WHERE ca_arn = ? AND key = ?`, caARN, t.Key)
		if err != nil {
			return err
		}
	}
	return nil
}

type Tag struct {
	Key   string
	Value string
}

type caScanner interface {
	Scan(dest ...any) error
}

func scanCA(s caScanner) (*CertificateAuthority, error) {
	var ca CertificateAuthority
	var subject string
	var createdAt int64
	err := s.Scan(
		&ca.ARN, &ca.Serial, &ca.Type, &ca.KeyAlgo, &ca.SigningAlgo,
		&subject, &ca.Status, &ca.UsageMode,
		&ca.CSR, &ca.CertPEM, &ca.CertChain, &ca.Policy, &createdAt,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrCANotFound
		}
		return nil, err
	}
	ca.CreatedAt = time.Unix(createdAt, 0)
	_ = json.Unmarshal([]byte(subject), &ca.Subject)
	if ca.Subject == nil {
		ca.Subject = map[string]string{}
	}
	return &ca, nil
}

type certScanner interface {
	Scan(dest ...any) error
}

func scanCert(s certScanner) (*IssuedCertificate, error) {
	var c IssuedCertificate
	var createdAt int64
	err := s.Scan(&c.ARN, &c.CAARN, &c.CertPEM, &c.CertChain, &createdAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrCertNotFound
		}
		return nil, err
	}
	c.CreatedAt = time.Unix(createdAt, 0)
	return &c, nil
}
