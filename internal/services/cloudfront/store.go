// SPDX-License-Identifier: Apache-2.0

// internal/services/cloudfront/store.go
package cloudfront

import (
	"database/sql"
	"errors"
	"path/filepath"
	"time"

	"github.com/skyoo2003/devcloud/internal/storage/sqlite"
)

var (
	ErrDistributionNotFound          = errors.New("distribution not found")
	ErrCachePolicyNotFound           = errors.New("cache policy not found")
	ErrOriginRequestPolicyNotFound   = errors.New("origin request policy not found")
	ErrResponseHeadersPolicyNotFound = errors.New("response headers policy not found")
	ErrOriginAccessControlNotFound   = errors.New("origin access control not found")
	ErrFunctionNotFound              = errors.New("function not found")
	ErrInvalidationNotFound          = errors.New("invalidation not found")
)

var migrations = []sqlite.Migration{
	{Version: 1, SQL: `
		CREATE TABLE IF NOT EXISTS distributions (
			id TEXT PRIMARY KEY,
			arn TEXT NOT NULL UNIQUE,
			domain_name TEXT NOT NULL DEFAULT '',
			status TEXT NOT NULL DEFAULT 'Deployed',
			etag TEXT NOT NULL DEFAULT '',
			config TEXT NOT NULL DEFAULT '',
			comment TEXT NOT NULL DEFAULT '',
			enabled INTEGER NOT NULL DEFAULT 1,
			created_at INTEGER NOT NULL,
			updated_at INTEGER NOT NULL
		);
		CREATE TABLE IF NOT EXISTS cache_policies (
			id TEXT PRIMARY KEY,
			etag TEXT NOT NULL DEFAULT '',
			name TEXT NOT NULL UNIQUE,
			comment TEXT NOT NULL DEFAULT '',
			config TEXT NOT NULL DEFAULT '{}',
			created_at INTEGER NOT NULL
		);
		CREATE TABLE IF NOT EXISTS origin_request_policies (
			id TEXT PRIMARY KEY,
			etag TEXT NOT NULL DEFAULT '',
			name TEXT NOT NULL UNIQUE,
			comment TEXT NOT NULL DEFAULT '',
			config TEXT NOT NULL DEFAULT '{}',
			created_at INTEGER NOT NULL
		);
		CREATE TABLE IF NOT EXISTS response_headers_policies (
			id TEXT PRIMARY KEY,
			etag TEXT NOT NULL DEFAULT '',
			name TEXT NOT NULL UNIQUE,
			comment TEXT NOT NULL DEFAULT '',
			config TEXT NOT NULL DEFAULT '{}',
			created_at INTEGER NOT NULL
		);
		CREATE TABLE IF NOT EXISTS origin_access_controls (
			id TEXT PRIMARY KEY,
			etag TEXT NOT NULL DEFAULT '',
			name TEXT NOT NULL,
			description TEXT NOT NULL DEFAULT '',
			signing_protocol TEXT NOT NULL DEFAULT 'sigv4',
			signing_behavior TEXT NOT NULL DEFAULT 'always',
			origin_type TEXT NOT NULL DEFAULT 's3',
			created_at INTEGER NOT NULL
		);
		CREATE TABLE IF NOT EXISTS functions (
			name TEXT PRIMARY KEY,
			etag TEXT NOT NULL DEFAULT '',
			arn TEXT NOT NULL UNIQUE,
			status TEXT NOT NULL DEFAULT 'UNASSOCIATED',
			stage TEXT NOT NULL DEFAULT 'DEVELOPMENT',
			runtime TEXT NOT NULL DEFAULT 'cloudfront-js-2.0',
			code TEXT NOT NULL DEFAULT '',
			comment TEXT NOT NULL DEFAULT '',
			created_at INTEGER NOT NULL,
			updated_at INTEGER NOT NULL
		);
		CREATE TABLE IF NOT EXISTS invalidations (
			id TEXT PRIMARY KEY,
			distribution_id TEXT NOT NULL,
			status TEXT NOT NULL DEFAULT 'Completed',
			paths TEXT NOT NULL DEFAULT '[]',
			created_at INTEGER NOT NULL
		);
		CREATE TABLE IF NOT EXISTS tags (
			resource_arn TEXT NOT NULL,
			tag_key TEXT NOT NULL,
			tag_value TEXT NOT NULL DEFAULT '',
			PRIMARY KEY (resource_arn, tag_key)
		);
	`},
}

// --- model structs ---

type Distribution struct {
	ID         string
	ARN        string
	DomainName string
	Status     string
	ETag       string
	Config     string
	Comment    string
	Enabled    bool
	CreatedAt  time.Time
	UpdatedAt  time.Time
}

type CachePolicy struct {
	ID        string
	ETag      string
	Name      string
	Comment   string
	Config    string
	CreatedAt time.Time
}

type OriginRequestPolicy struct {
	ID        string
	ETag      string
	Name      string
	Comment   string
	Config    string
	CreatedAt time.Time
}

type ResponseHeadersPolicy struct {
	ID        string
	ETag      string
	Name      string
	Comment   string
	Config    string
	CreatedAt time.Time
}

type OriginAccessControl struct {
	ID              string
	ETag            string
	Name            string
	Description     string
	SigningProtocol string
	SigningBehavior string
	OriginType      string
	CreatedAt       time.Time
}

type Function struct {
	Name      string
	ETag      string
	ARN       string
	Status    string
	Stage     string
	Runtime   string
	Code      string
	Comment   string
	CreatedAt time.Time
	UpdatedAt time.Time
}

type Invalidation struct {
	ID             string
	DistributionID string
	Status         string
	Paths          string
	CreatedAt      time.Time
}

// --- store ---

type CloudFrontStore struct {
	store *sqlite.Store
}

func NewCloudFrontStore(dataDir string) (*CloudFrontStore, error) {
	dbPath := filepath.Join(dataDir, "cloudfront.db")
	s, err := sqlite.Open(dbPath, migrations)
	if err != nil {
		return nil, err
	}
	return &CloudFrontStore{store: s}, nil
}

func (s *CloudFrontStore) Close() error { return s.store.Close() }

// --- distributions ---

func (s *CloudFrontStore) CreateDistribution(d *Distribution) error {
	_, err := s.store.DB().Exec(
		`INSERT INTO distributions (id, arn, domain_name, status, etag, config, comment, enabled, created_at, updated_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		d.ID, d.ARN, d.DomainName, d.Status, d.ETag, d.Config, d.Comment, boolToInt(d.Enabled),
		d.CreatedAt.Unix(), d.UpdatedAt.Unix(),
	)
	return err
}

func (s *CloudFrontStore) GetDistribution(id string) (*Distribution, error) {
	row := s.store.DB().QueryRow(
		`SELECT id, arn, domain_name, status, etag, config, comment, enabled, created_at, updated_at
		 FROM distributions WHERE id = ?`, id,
	)
	return scanDistribution(row)
}

func (s *CloudFrontStore) ListDistributions() ([]Distribution, error) {
	rows, err := s.store.DB().Query(
		`SELECT id, arn, domain_name, status, etag, config, comment, enabled, created_at, updated_at
		 FROM distributions ORDER BY created_at`,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Distribution
	for rows.Next() {
		d, err := scanDistribution(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *d)
	}
	return out, rows.Err()
}

func (s *CloudFrontStore) UpdateDistribution(d *Distribution) error {
	res, err := s.store.DB().Exec(
		`UPDATE distributions SET domain_name=?, status=?, etag=?, config=?, comment=?, enabled=?, updated_at=?
		 WHERE id=?`,
		d.DomainName, d.Status, d.ETag, d.Config, d.Comment, boolToInt(d.Enabled), d.UpdatedAt.Unix(), d.ID,
	)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrDistributionNotFound
	}
	return nil
}

func (s *CloudFrontStore) DeleteDistribution(id string) error {
	res, err := s.store.DB().Exec(`DELETE FROM distributions WHERE id = ?`, id)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrDistributionNotFound
	}
	return nil
}

func scanDistribution(sc scanner) (*Distribution, error) {
	var d Distribution
	var enabled int
	var createdAt, updatedAt int64
	err := sc.Scan(&d.ID, &d.ARN, &d.DomainName, &d.Status, &d.ETag, &d.Config, &d.Comment, &enabled, &createdAt, &updatedAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrDistributionNotFound
		}
		return nil, err
	}
	d.Enabled = enabled != 0
	d.CreatedAt = time.Unix(createdAt, 0)
	d.UpdatedAt = time.Unix(updatedAt, 0)
	return &d, nil
}

// --- cache policies ---

func (s *CloudFrontStore) CreateCachePolicy(cp *CachePolicy) error {
	_, err := s.store.DB().Exec(
		`INSERT INTO cache_policies (id, etag, name, comment, config, created_at)
		 VALUES (?, ?, ?, ?, ?, ?)`,
		cp.ID, cp.ETag, cp.Name, cp.Comment, cp.Config, cp.CreatedAt.Unix(),
	)
	if sqlite.IsUniqueConstraintError(err) {
		return errors.New("cache policy name already exists")
	}
	return err
}

func (s *CloudFrontStore) GetCachePolicy(id string) (*CachePolicy, error) {
	row := s.store.DB().QueryRow(
		`SELECT id, etag, name, comment, config, created_at FROM cache_policies WHERE id = ?`, id,
	)
	return scanCachePolicy(row)
}

func (s *CloudFrontStore) ListCachePolicies() ([]CachePolicy, error) {
	rows, err := s.store.DB().Query(
		`SELECT id, etag, name, comment, config, created_at FROM cache_policies ORDER BY created_at`,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []CachePolicy
	for rows.Next() {
		cp, err := scanCachePolicy(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *cp)
	}
	return out, rows.Err()
}

func (s *CloudFrontStore) UpdateCachePolicy(cp *CachePolicy) error {
	res, err := s.store.DB().Exec(
		`UPDATE cache_policies SET etag=?, name=?, comment=?, config=? WHERE id=?`,
		cp.ETag, cp.Name, cp.Comment, cp.Config, cp.ID,
	)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrCachePolicyNotFound
	}
	return nil
}

func (s *CloudFrontStore) DeleteCachePolicy(id string) error {
	res, err := s.store.DB().Exec(`DELETE FROM cache_policies WHERE id = ?`, id)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrCachePolicyNotFound
	}
	return nil
}

func scanCachePolicy(sc scanner) (*CachePolicy, error) {
	var cp CachePolicy
	var createdAt int64
	err := sc.Scan(&cp.ID, &cp.ETag, &cp.Name, &cp.Comment, &cp.Config, &createdAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrCachePolicyNotFound
		}
		return nil, err
	}
	cp.CreatedAt = time.Unix(createdAt, 0)
	return &cp, nil
}

// --- origin request policies ---

func (s *CloudFrontStore) CreateOriginRequestPolicy(p *OriginRequestPolicy) error {
	_, err := s.store.DB().Exec(
		`INSERT INTO origin_request_policies (id, etag, name, comment, config, created_at)
		 VALUES (?, ?, ?, ?, ?, ?)`,
		p.ID, p.ETag, p.Name, p.Comment, p.Config, p.CreatedAt.Unix(),
	)
	if sqlite.IsUniqueConstraintError(err) {
		return errors.New("origin request policy name already exists")
	}
	return err
}

func (s *CloudFrontStore) GetOriginRequestPolicy(id string) (*OriginRequestPolicy, error) {
	row := s.store.DB().QueryRow(
		`SELECT id, etag, name, comment, config, created_at FROM origin_request_policies WHERE id = ?`, id,
	)
	return scanOriginRequestPolicy(row)
}

func (s *CloudFrontStore) ListOriginRequestPolicies() ([]OriginRequestPolicy, error) {
	rows, err := s.store.DB().Query(
		`SELECT id, etag, name, comment, config, created_at FROM origin_request_policies ORDER BY created_at`,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []OriginRequestPolicy
	for rows.Next() {
		p, err := scanOriginRequestPolicy(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *p)
	}
	return out, rows.Err()
}

func (s *CloudFrontStore) UpdateOriginRequestPolicy(p *OriginRequestPolicy) error {
	res, err := s.store.DB().Exec(
		`UPDATE origin_request_policies SET etag=?, name=?, comment=?, config=? WHERE id=?`,
		p.ETag, p.Name, p.Comment, p.Config, p.ID,
	)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrOriginRequestPolicyNotFound
	}
	return nil
}

func (s *CloudFrontStore) DeleteOriginRequestPolicy(id string) error {
	res, err := s.store.DB().Exec(`DELETE FROM origin_request_policies WHERE id = ?`, id)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrOriginRequestPolicyNotFound
	}
	return nil
}

func scanOriginRequestPolicy(sc scanner) (*OriginRequestPolicy, error) {
	var p OriginRequestPolicy
	var createdAt int64
	err := sc.Scan(&p.ID, &p.ETag, &p.Name, &p.Comment, &p.Config, &createdAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrOriginRequestPolicyNotFound
		}
		return nil, err
	}
	p.CreatedAt = time.Unix(createdAt, 0)
	return &p, nil
}

// --- response headers policies ---

func (s *CloudFrontStore) CreateResponseHeadersPolicy(p *ResponseHeadersPolicy) error {
	_, err := s.store.DB().Exec(
		`INSERT INTO response_headers_policies (id, etag, name, comment, config, created_at)
		 VALUES (?, ?, ?, ?, ?, ?)`,
		p.ID, p.ETag, p.Name, p.Comment, p.Config, p.CreatedAt.Unix(),
	)
	if sqlite.IsUniqueConstraintError(err) {
		return errors.New("response headers policy name already exists")
	}
	return err
}

func (s *CloudFrontStore) GetResponseHeadersPolicy(id string) (*ResponseHeadersPolicy, error) {
	row := s.store.DB().QueryRow(
		`SELECT id, etag, name, comment, config, created_at FROM response_headers_policies WHERE id = ?`, id,
	)
	return scanResponseHeadersPolicy(row)
}

func (s *CloudFrontStore) ListResponseHeadersPolicies() ([]ResponseHeadersPolicy, error) {
	rows, err := s.store.DB().Query(
		`SELECT id, etag, name, comment, config, created_at FROM response_headers_policies ORDER BY created_at`,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ResponseHeadersPolicy
	for rows.Next() {
		p, err := scanResponseHeadersPolicy(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *p)
	}
	return out, rows.Err()
}

func (s *CloudFrontStore) UpdateResponseHeadersPolicy(p *ResponseHeadersPolicy) error {
	res, err := s.store.DB().Exec(
		`UPDATE response_headers_policies SET etag=?, name=?, comment=?, config=? WHERE id=?`,
		p.ETag, p.Name, p.Comment, p.Config, p.ID,
	)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrResponseHeadersPolicyNotFound
	}
	return nil
}

func (s *CloudFrontStore) DeleteResponseHeadersPolicy(id string) error {
	res, err := s.store.DB().Exec(`DELETE FROM response_headers_policies WHERE id = ?`, id)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrResponseHeadersPolicyNotFound
	}
	return nil
}

func scanResponseHeadersPolicy(sc scanner) (*ResponseHeadersPolicy, error) {
	var p ResponseHeadersPolicy
	var createdAt int64
	err := sc.Scan(&p.ID, &p.ETag, &p.Name, &p.Comment, &p.Config, &createdAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrResponseHeadersPolicyNotFound
		}
		return nil, err
	}
	p.CreatedAt = time.Unix(createdAt, 0)
	return &p, nil
}

// --- origin access controls ---

func (s *CloudFrontStore) CreateOriginAccessControl(oac *OriginAccessControl) error {
	_, err := s.store.DB().Exec(
		`INSERT INTO origin_access_controls (id, etag, name, description, signing_protocol, signing_behavior, origin_type, created_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		oac.ID, oac.ETag, oac.Name, oac.Description, oac.SigningProtocol, oac.SigningBehavior, oac.OriginType,
		oac.CreatedAt.Unix(),
	)
	return err
}

func (s *CloudFrontStore) GetOriginAccessControl(id string) (*OriginAccessControl, error) {
	row := s.store.DB().QueryRow(
		`SELECT id, etag, name, description, signing_protocol, signing_behavior, origin_type, created_at
		 FROM origin_access_controls WHERE id = ?`, id,
	)
	return scanOriginAccessControl(row)
}

func (s *CloudFrontStore) ListOriginAccessControls() ([]OriginAccessControl, error) {
	rows, err := s.store.DB().Query(
		`SELECT id, etag, name, description, signing_protocol, signing_behavior, origin_type, created_at
		 FROM origin_access_controls ORDER BY created_at`,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []OriginAccessControl
	for rows.Next() {
		oac, err := scanOriginAccessControl(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *oac)
	}
	return out, rows.Err()
}

func (s *CloudFrontStore) UpdateOriginAccessControl(oac *OriginAccessControl) error {
	res, err := s.store.DB().Exec(
		`UPDATE origin_access_controls SET etag=?, name=?, description=?, signing_protocol=?, signing_behavior=?, origin_type=?
		 WHERE id=?`,
		oac.ETag, oac.Name, oac.Description, oac.SigningProtocol, oac.SigningBehavior, oac.OriginType, oac.ID,
	)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrOriginAccessControlNotFound
	}
	return nil
}

func (s *CloudFrontStore) DeleteOriginAccessControl(id string) error {
	res, err := s.store.DB().Exec(`DELETE FROM origin_access_controls WHERE id = ?`, id)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrOriginAccessControlNotFound
	}
	return nil
}

func scanOriginAccessControl(sc scanner) (*OriginAccessControl, error) {
	var oac OriginAccessControl
	var createdAt int64
	err := sc.Scan(&oac.ID, &oac.ETag, &oac.Name, &oac.Description, &oac.SigningProtocol, &oac.SigningBehavior, &oac.OriginType, &createdAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrOriginAccessControlNotFound
		}
		return nil, err
	}
	oac.CreatedAt = time.Unix(createdAt, 0)
	return &oac, nil
}

// --- functions ---

func (s *CloudFrontStore) CreateFunction(fn *Function) error {
	_, err := s.store.DB().Exec(
		`INSERT INTO functions (name, etag, arn, status, stage, runtime, code, comment, created_at, updated_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		fn.Name, fn.ETag, fn.ARN, fn.Status, fn.Stage, fn.Runtime, fn.Code, fn.Comment,
		fn.CreatedAt.Unix(), fn.UpdatedAt.Unix(),
	)
	if sqlite.IsUniqueConstraintError(err) {
		return errors.New("function already exists")
	}
	return err
}

func (s *CloudFrontStore) GetFunction(name string) (*Function, error) {
	row := s.store.DB().QueryRow(
		`SELECT name, etag, arn, status, stage, runtime, code, comment, created_at, updated_at
		 FROM functions WHERE name = ?`, name,
	)
	return scanFunction(row)
}

func (s *CloudFrontStore) ListFunctions() ([]Function, error) {
	rows, err := s.store.DB().Query(
		`SELECT name, etag, arn, status, stage, runtime, code, comment, created_at, updated_at
		 FROM functions ORDER BY created_at`,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Function
	for rows.Next() {
		fn, err := scanFunction(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *fn)
	}
	return out, rows.Err()
}

func (s *CloudFrontStore) UpdateFunction(fn *Function) error {
	res, err := s.store.DB().Exec(
		`UPDATE functions SET etag=?, status=?, stage=?, runtime=?, code=?, comment=?, updated_at=?
		 WHERE name=?`,
		fn.ETag, fn.Status, fn.Stage, fn.Runtime, fn.Code, fn.Comment, fn.UpdatedAt.Unix(), fn.Name,
	)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrFunctionNotFound
	}
	return nil
}

func (s *CloudFrontStore) DeleteFunction(name string) error {
	res, err := s.store.DB().Exec(`DELETE FROM functions WHERE name = ?`, name)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrFunctionNotFound
	}
	return nil
}

func scanFunction(sc scanner) (*Function, error) {
	var fn Function
	var createdAt, updatedAt int64
	err := sc.Scan(&fn.Name, &fn.ETag, &fn.ARN, &fn.Status, &fn.Stage, &fn.Runtime, &fn.Code, &fn.Comment, &createdAt, &updatedAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrFunctionNotFound
		}
		return nil, err
	}
	fn.CreatedAt = time.Unix(createdAt, 0)
	fn.UpdatedAt = time.Unix(updatedAt, 0)
	return &fn, nil
}

// --- invalidations ---

func (s *CloudFrontStore) CreateInvalidation(inv *Invalidation) error {
	_, err := s.store.DB().Exec(
		`INSERT INTO invalidations (id, distribution_id, status, paths, created_at)
		 VALUES (?, ?, ?, ?, ?)`,
		inv.ID, inv.DistributionID, inv.Status, inv.Paths, inv.CreatedAt.Unix(),
	)
	return err
}

func (s *CloudFrontStore) GetInvalidation(distributionID, id string) (*Invalidation, error) {
	row := s.store.DB().QueryRow(
		`SELECT id, distribution_id, status, paths, created_at FROM invalidations
		 WHERE id = ? AND distribution_id = ?`, id, distributionID,
	)
	return scanInvalidation(row)
}

func (s *CloudFrontStore) ListInvalidations(distributionID string) ([]Invalidation, error) {
	rows, err := s.store.DB().Query(
		`SELECT id, distribution_id, status, paths, created_at FROM invalidations
		 WHERE distribution_id = ? ORDER BY created_at DESC`, distributionID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Invalidation
	for rows.Next() {
		inv, err := scanInvalidation(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *inv)
	}
	return out, rows.Err()
}

func scanInvalidation(sc scanner) (*Invalidation, error) {
	var inv Invalidation
	var createdAt int64
	err := sc.Scan(&inv.ID, &inv.DistributionID, &inv.Status, &inv.Paths, &createdAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrInvalidationNotFound
		}
		return nil, err
	}
	inv.CreatedAt = time.Unix(createdAt, 0)
	return &inv, nil
}

// --- tags ---

func (s *CloudFrontStore) PutTags(arn string, tags map[string]string) error {
	for k, v := range tags {
		_, err := s.store.DB().Exec(
			`INSERT INTO tags (resource_arn, tag_key, tag_value) VALUES (?, ?, ?)
			 ON CONFLICT(resource_arn, tag_key) DO UPDATE SET tag_value=excluded.tag_value`,
			arn, k, v,
		)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *CloudFrontStore) RemoveTags(arn string, keys []string) error {
	for _, k := range keys {
		_, err := s.store.DB().Exec(`DELETE FROM tags WHERE resource_arn = ? AND tag_key = ?`, arn, k)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *CloudFrontStore) ListTags(arn string) (map[string]string, error) {
	rows, err := s.store.DB().Query(
		`SELECT tag_key, tag_value FROM tags WHERE resource_arn = ?`, arn,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make(map[string]string)
	for rows.Next() {
		var k, v string
		if err := rows.Scan(&k, &v); err != nil {
			return nil, err
		}
		out[k] = v
	}
	return out, rows.Err()
}

// --- helpers ---

type scanner interface {
	Scan(dest ...any) error
}

func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}
