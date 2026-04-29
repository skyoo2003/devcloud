// SPDX-License-Identifier: Apache-2.0

// internal/services/ecr/store.go
package ecr

import (
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/skyoo2003/devcloud/internal/shared"
	"github.com/skyoo2003/devcloud/internal/storage/sqlite"
)

// isWithinDir returns true if child resolves to a path within parent.
func isWithinDir(child, parent string) bool {
	absChild, err := filepath.Abs(filepath.Clean(child))
	if err != nil {
		return false
	}
	absParent, err := filepath.Abs(filepath.Clean(parent))
	if err != nil {
		return false
	}
	rel, err := filepath.Rel(absParent, absChild)
	if err != nil {
		return false
	}
	return rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)) && !filepath.IsAbs(rel)
}

var (
	ErrRepositoryNotFound      = errors.New("repository not found")
	ErrRepositoryAlreadyExists = errors.New("repository already exists")
	ErrImageNotFound           = errors.New("image not found")
	ErrPolicyNotFound          = errors.New("policy not found")
	ErrLifecyclePolicyNotFound = errors.New("lifecycle policy not found")
	ErrLayerUploadNotFound     = errors.New("layer upload not found")
)

const region = "us-east-1"

var migrations = []sqlite.Migration{
	{Version: 1, SQL: `
		CREATE TABLE IF NOT EXISTS repositories (
			arn        TEXT PRIMARY KEY,
			name       TEXT NOT NULL,
			account_id TEXT NOT NULL,
			registry_id TEXT NOT NULL,
			uri        TEXT NOT NULL,
			created_at DATETIME NOT NULL
		);
		CREATE UNIQUE INDEX IF NOT EXISTS idx_repo_name_account ON repositories(name, account_id);
		CREATE TABLE IF NOT EXISTS repository_policies (
			repo_name   TEXT NOT NULL,
			account_id  TEXT NOT NULL,
			policy_text TEXT NOT NULL,
			PRIMARY KEY (repo_name, account_id)
		);
		CREATE TABLE IF NOT EXISTS images (
			id           INTEGER PRIMARY KEY AUTOINCREMENT,
			repo_name    TEXT NOT NULL,
			account_id   TEXT NOT NULL,
			image_digest TEXT NOT NULL,
			image_tag    TEXT NOT NULL DEFAULT '',
			size_bytes   INTEGER NOT NULL DEFAULT 0,
			pushed_at    DATETIME NOT NULL
		);
	`},
	{Version: 2, SQL: `
		CREATE TABLE IF NOT EXISTS layers (
			upload_id   TEXT PRIMARY KEY,
			repo_name   TEXT NOT NULL,
			account_id  TEXT NOT NULL,
			digest      TEXT,
			total_size  INTEGER DEFAULT 0,
			completed   INTEGER DEFAULT 0,
			created_at  DATETIME NOT NULL
		);
		CREATE TABLE IF NOT EXISTS layer_parts (
			upload_id   TEXT NOT NULL,
			part_first  INTEGER NOT NULL,
			part_last   INTEGER NOT NULL,
			size        INTEGER NOT NULL,
			PRIMARY KEY (upload_id, part_first)
		);
		CREATE TABLE IF NOT EXISTS lifecycle_policies (
			repo_name   TEXT NOT NULL,
			account_id  TEXT NOT NULL,
			policy_text TEXT NOT NULL,
			last_eval   DATETIME,
			PRIMARY KEY (repo_name, account_id)
		);
		CREATE TABLE IF NOT EXISTS repo_tags (
			repo_arn   TEXT NOT NULL,
			tag_key    TEXT NOT NULL,
			tag_value  TEXT NOT NULL,
			PRIMARY KEY (repo_arn, tag_key)
		);
		CREATE TABLE IF NOT EXISTS scan_findings (
			repo_name       TEXT NOT NULL,
			image_digest    TEXT NOT NULL,
			account_id      TEXT NOT NULL,
			scan_status     TEXT DEFAULT 'COMPLETE',
			findings_json   TEXT,
			scanned_at      DATETIME NOT NULL,
			PRIMARY KEY (repo_name, image_digest, account_id)
		);
		ALTER TABLE repositories ADD COLUMN scan_on_push INTEGER DEFAULT 0;
	`},
}

type Repository struct {
	ARN        string
	Name       string
	AccountID  string
	RegistryID string
	URI        string
	CreatedAt  time.Time
}

type Image struct {
	ID          int64
	RepoName    string
	AccountID   string
	ImageDigest string
	ImageTag    string
	SizeBytes   int64
	PushedAt    time.Time
}

type ECRStore struct {
	store   *sqlite.Store
	dataDir string
	port    int // HTTP port used when building download/layer URLs.
}

// NewECRStore opens the ECR SQLite DB under dataDir and binds the store to
// the configured HTTP port so response URLs match the active server. Pass 0
// to accept the default (4747).
func NewECRStore(dataDir string, port int) (*ECRStore, error) {
	if port <= 0 {
		port = 4747
	}
	dbPath := filepath.Join(dataDir, "ecr.db")
	s, err := sqlite.Open(dbPath, migrations)
	if err != nil {
		return nil, err
	}
	return &ECRStore{store: s, dataDir: dataDir, port: port}, nil
}

func (s *ECRStore) Close() error { return s.store.Close() }
func (s *ECRStore) db() *sql.DB  { return s.store.DB() }

func repoURI(accountID, name string) string {
	return fmt.Sprintf("%s.dkr.ecr.%s.amazonaws.com/%s", accountID, region, name)
}

func repoARN(accountID, name string) string {
	return fmt.Sprintf("arn:aws:ecr:%s:%s:repository/%s", region, accountID, name)
}

// CreateRepository creates a new ECR repository.
func (s *ECRStore) CreateRepository(accountID, name string) (*Repository, error) {
	arn := repoARN(accountID, name)
	uri := repoURI(accountID, name)
	now := time.Now().UTC()
	_, err := s.db().Exec(
		`INSERT INTO repositories (arn, name, account_id, registry_id, uri, created_at) VALUES (?, ?, ?, ?, ?, ?)`,
		arn, name, accountID, accountID, uri, now,
	)
	if err != nil {
		if sqlite.IsUniqueConstraintError(err) {
			return nil, ErrRepositoryAlreadyExists
		}
		return nil, err
	}
	return &Repository{ARN: arn, Name: name, AccountID: accountID, RegistryID: accountID, URI: uri, CreatedAt: now}, nil
}

// DeleteRepository deletes a repository and all its images.
func (s *ECRStore) DeleteRepository(accountID, name string) error {
	res, err := s.db().Exec(`DELETE FROM repositories WHERE name=? AND account_id=?`, name, accountID)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrRepositoryNotFound
	}
	_, _ = s.db().Exec(`DELETE FROM images WHERE repo_name=? AND account_id=?`, name, accountID)
	return nil
}

// DescribeRepositories returns repositories for the account, optionally filtered by names.
func (s *ECRStore) DescribeRepositories(accountID string, names []string) ([]Repository, error) {
	query := `SELECT arn, name, account_id, registry_id, uri, created_at FROM repositories WHERE account_id=?`
	args := []any{accountID}
	if len(names) > 0 {
		ph := ""
		for i, n := range names {
			if i > 0 {
				ph += ","
			}
			ph += "?"
			args = append(args, n)
		}
		query += " AND name IN (" + ph + ")"
	}
	query += " ORDER BY name"
	rows, err := s.db().Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []Repository
	for rows.Next() {
		var r Repository
		if err := rows.Scan(&r.ARN, &r.Name, &r.AccountID, &r.RegistryID, &r.URI, &r.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// PutImage stores an image manifest and tag in a repository.
func (s *ECRStore) PutImage(accountID, repoName, imageManifest, imageTag string) (*Image, error) {
	// Compute digest from manifest content.
	h := sha256.Sum256([]byte(imageManifest))
	digest := "sha256:" + hex.EncodeToString(h[:])
	sizeBytes := int64(len(imageManifest))
	now := time.Now().UTC()

	// Upsert: update tag if digest+repo already exists, else insert.
	_, err := s.db().Exec(
		`INSERT INTO images (repo_name, account_id, image_digest, image_tag, size_bytes, pushed_at) VALUES (?, ?, ?, ?, ?, ?)`,
		repoName, accountID, digest, imageTag, sizeBytes, now,
	)
	if err != nil {
		return nil, err
	}
	return &Image{RepoName: repoName, AccountID: accountID, ImageDigest: digest, ImageTag: imageTag, SizeBytes: sizeBytes, PushedAt: now}, nil
}

// DescribeImages returns images in a repository.
func (s *ECRStore) DescribeImages(accountID, repoName string) ([]Image, error) {
	rows, err := s.db().Query(
		`SELECT id, repo_name, account_id, image_digest, image_tag, size_bytes, pushed_at FROM images
		 WHERE repo_name=? AND account_id=? ORDER BY pushed_at DESC`,
		repoName, accountID,
	)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []Image
	for rows.Next() {
		var img Image
		if err := rows.Scan(&img.ID, &img.RepoName, &img.AccountID, &img.ImageDigest, &img.ImageTag, &img.SizeBytes, &img.PushedAt); err != nil {
			return nil, err
		}
		out = append(out, img)
	}
	return out, rows.Err()
}

// BatchGetImage returns images by digest or tag.
func (s *ECRStore) BatchGetImage(accountID, repoName string, imageIDs []map[string]string) ([]Image, error) {
	var out []Image
	for _, id := range imageIDs {
		digest := id["imageDigest"]
		tag := id["imageTag"]
		var row *sql.Row
		if digest != "" {
			row = s.db().QueryRow(
				`SELECT id, repo_name, account_id, image_digest, image_tag, size_bytes, pushed_at FROM images
				 WHERE repo_name=? AND account_id=? AND image_digest=? LIMIT 1`,
				repoName, accountID, digest,
			)
		} else if tag != "" {
			row = s.db().QueryRow(
				`SELECT id, repo_name, account_id, image_digest, image_tag, size_bytes, pushed_at FROM images
				 WHERE repo_name=? AND account_id=? AND image_tag=? ORDER BY pushed_at DESC LIMIT 1`,
				repoName, accountID, tag,
			)
		} else {
			continue
		}
		var img Image
		if err := row.Scan(&img.ID, &img.RepoName, &img.AccountID, &img.ImageDigest, &img.ImageTag, &img.SizeBytes, &img.PushedAt); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				continue
			}
			return nil, err
		}
		out = append(out, img)
	}
	return out, nil
}

// BatchDeleteImage deletes images by digest or tag.
func (s *ECRStore) BatchDeleteImage(accountID, repoName string, imageIDs []map[string]string) error {
	for _, id := range imageIDs {
		digest := id["imageDigest"]
		tag := id["imageTag"]
		if digest != "" {
			_, _ = s.db().Exec(`DELETE FROM images WHERE repo_name=? AND account_id=? AND image_digest=?`, repoName, accountID, digest)
		} else if tag != "" {
			_, _ = s.db().Exec(`DELETE FROM images WHERE repo_name=? AND account_id=? AND image_tag=?`, repoName, accountID, tag)
		}
	}
	return nil
}

// ListImages returns image identifiers (digest + tag) for a repository.
func (s *ECRStore) ListImages(accountID, repoName string) ([]map[string]string, error) {
	rows, err := s.db().Query(
		`SELECT image_digest, image_tag FROM images WHERE repo_name=? AND account_id=? ORDER BY pushed_at DESC`,
		repoName, accountID,
	)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []map[string]string
	for rows.Next() {
		var digest, tag string
		if err := rows.Scan(&digest, &tag); err != nil {
			return nil, err
		}
		m := map[string]string{"imageDigest": digest}
		if tag != "" {
			m["imageTag"] = tag
		}
		out = append(out, m)
	}
	return out, rows.Err()
}

// SetRepositoryPolicy sets the policy for a repository.
func (s *ECRStore) SetRepositoryPolicy(accountID, repoName, policyText string) error {
	// Verify repository exists.
	var exists int
	_ = s.db().QueryRow(`SELECT COUNT(*) FROM repositories WHERE name=? AND account_id=?`, repoName, accountID).Scan(&exists)
	if exists == 0 {
		return ErrRepositoryNotFound
	}
	_, err := s.db().Exec(
		`INSERT INTO repository_policies (repo_name, account_id, policy_text) VALUES (?, ?, ?)
		 ON CONFLICT(repo_name, account_id) DO UPDATE SET policy_text=excluded.policy_text`,
		repoName, accountID, policyText,
	)
	return err
}

// GetRepositoryPolicy returns the policy for a repository.
func (s *ECRStore) GetRepositoryPolicy(accountID, repoName string) (string, error) {
	// Verify repository exists.
	var exists int
	_ = s.db().QueryRow(`SELECT COUNT(*) FROM repositories WHERE name=? AND account_id=?`, repoName, accountID).Scan(&exists)
	if exists == 0 {
		return "", ErrRepositoryNotFound
	}
	var policyText string
	err := s.db().QueryRow(
		`SELECT policy_text FROM repository_policies WHERE repo_name=? AND account_id=?`,
		repoName, accountID,
	).Scan(&policyText)
	if errors.Is(err, sql.ErrNoRows) {
		return "", ErrPolicyNotFound
	}
	if err != nil {
		return "", err
	}
	return policyText, nil
}

// --- Layer operations ---

// InitiateLayerUpload creates a new layer upload record and returns the uploadId.
func (s *ECRStore) InitiateLayerUpload(accountID, repoName string) (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	uploadID := hex.EncodeToString(b)
	now := time.Now().UTC()
	_, err := s.db().Exec(
		`INSERT INTO layers (upload_id, repo_name, account_id, created_at) VALUES (?, ?, ?, ?)`,
		uploadID, repoName, accountID, now,
	)
	if err != nil {
		return "", err
	}
	return uploadID, nil
}

// UploadLayerPart saves layer part blob to the filesystem and records the part metadata.
func (s *ECRStore) UploadLayerPart(accountID, repoName, uploadID string, partFirst, partLast int64, blob []byte) error {
	// uploadID is expected to be a 32-char lowercase hex value generated by InitiateLayerUpload.
	// Reject anything else to prevent path traversal/path injection.
	if !shared.ValidateUploadID(uploadID) {
		return ErrLayerUploadNotFound
	}

	// Verify upload exists.
	var exists int
	_ = s.db().QueryRow(`SELECT COUNT(*) FROM layers WHERE upload_id=? AND repo_name=? AND account_id=?`, uploadID, repoName, accountID).Scan(&exists)
	if exists == 0 {
		return ErrLayerUploadNotFound
	}

	// Save blob to filesystem.
	dir := filepath.Join(s.dataDir, "_layers", uploadID, "parts")
	if !isWithinDir(dir, s.dataDir) {
		return fmt.Errorf("path traversal detected: %s", dir)
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("mkdir layer parts: %w", err)
	}
	partFile := filepath.Join(dir, fmt.Sprintf("%d-%d", partFirst, partLast))
	if err := os.WriteFile(partFile, blob, 0o644); err != nil {
		return fmt.Errorf("write layer part: %w", err)
	}

	// Record part metadata.
	size := int64(len(blob))
	_, err := s.db().Exec(
		`INSERT INTO layer_parts (upload_id, part_first, part_last, size) VALUES (?, ?, ?, ?)
		 ON CONFLICT(upload_id, part_first) DO UPDATE SET part_last=excluded.part_last, size=excluded.size`,
		uploadID, partFirst, partLast, size,
	)
	if err != nil {
		return err
	}
	// Update total_size.
	_, err = s.db().Exec(
		`UPDATE layers SET total_size = (SELECT COALESCE(SUM(size),0) FROM layer_parts WHERE upload_id=?) WHERE upload_id=?`,
		uploadID, uploadID,
	)
	return err
}

// CompleteLayerUpload marks a layer upload as completed and records the digest.
func (s *ECRStore) CompleteLayerUpload(accountID, repoName, uploadID, digest string) error {
	res, err := s.db().Exec(
		`UPDATE layers SET completed=1, digest=? WHERE upload_id=? AND repo_name=? AND account_id=?`,
		digest, uploadID, repoName, accountID,
	)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrLayerUploadNotFound
	}
	return nil
}

// BatchCheckLayerAvailability checks if layers with the given digests are available.
func (s *ECRStore) BatchCheckLayerAvailability(accountID, repoName string, digests []string) ([]map[string]any, error) {
	out := make([]map[string]any, 0, len(digests))
	for _, d := range digests {
		var count int
		s.db().QueryRow(
			`SELECT COUNT(*) FROM layers WHERE digest=? AND completed=1`,
			d,
		).Scan(&count) //nolint:errcheck
		availability := "UNAVAILABLE"
		if count > 0 {
			availability = "AVAILABLE"
		}
		out = append(out, map[string]any{
			"layerDigest":       d,
			"layerAvailability": availability,
		})
	}
	return out, nil
}

// GetDownloadURLForLayer returns a stub download URL for a layer by digest.
func (s *ECRStore) GetDownloadURLForLayer(accountID, repoName, digest string) (string, error) {
	var count int
	_ = s.db().QueryRow(`SELECT COUNT(*) FROM layers WHERE digest=? AND completed=1`, digest).Scan(&count)
	if count == 0 {
		return "", ErrLayerUploadNotFound
	}
	return fmt.Sprintf("http://localhost:%d/_layers/%s", s.port, digest), nil
}

// --- Lifecycle Policy operations ---

// PutLifecyclePolicy upserts a lifecycle policy for a repository.
func (s *ECRStore) PutLifecyclePolicy(accountID, repoName, policyText string) error {
	var exists int
	_ = s.db().QueryRow(`SELECT COUNT(*) FROM repositories WHERE name=? AND account_id=?`, repoName, accountID).Scan(&exists)
	if exists == 0 {
		return ErrRepositoryNotFound
	}
	_, err := s.db().Exec(
		`INSERT INTO lifecycle_policies (repo_name, account_id, policy_text) VALUES (?, ?, ?)
		 ON CONFLICT(repo_name, account_id) DO UPDATE SET policy_text=excluded.policy_text`,
		repoName, accountID, policyText,
	)
	return err
}

// GetLifecyclePolicy returns the lifecycle policy for a repository.
func (s *ECRStore) GetLifecyclePolicy(accountID, repoName string) (string, error) {
	var exists int
	_ = s.db().QueryRow(`SELECT COUNT(*) FROM repositories WHERE name=? AND account_id=?`, repoName, accountID).Scan(&exists)
	if exists == 0 {
		return "", ErrRepositoryNotFound
	}
	var policyText string
	err := s.db().QueryRow(
		`SELECT policy_text FROM lifecycle_policies WHERE repo_name=? AND account_id=?`,
		repoName, accountID,
	).Scan(&policyText)
	if errors.Is(err, sql.ErrNoRows) {
		return "", ErrLifecyclePolicyNotFound
	}
	if err != nil {
		return "", err
	}
	return policyText, nil
}

// DeleteLifecyclePolicy deletes the lifecycle policy for a repository.
func (s *ECRStore) DeleteLifecyclePolicy(accountID, repoName string) error {
	var exists int
	_ = s.db().QueryRow(`SELECT COUNT(*) FROM repositories WHERE name=? AND account_id=?`, repoName, accountID).Scan(&exists)
	if exists == 0 {
		return ErrRepositoryNotFound
	}
	res, err := s.db().Exec(
		`DELETE FROM lifecycle_policies WHERE repo_name=? AND account_id=?`,
		repoName, accountID,
	)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrLifecyclePolicyNotFound
	}
	return nil
}

// --- Tag operations ---

// TagResource adds or updates tags for a resource ARN.
func (s *ECRStore) TagResource(repoARN string, tags []map[string]string) error {
	for _, tag := range tags {
		key := tag["Key"]
		val := tag["Value"]
		if key == "" {
			continue
		}
		_, err := s.db().Exec(
			`INSERT INTO repo_tags (repo_arn, tag_key, tag_value) VALUES (?, ?, ?)
			 ON CONFLICT(repo_arn, tag_key) DO UPDATE SET tag_value=excluded.tag_value`,
			repoARN, key, val,
		)
		if err != nil {
			return err
		}
	}
	return nil
}

// UntagResource removes tags by key for a resource ARN.
func (s *ECRStore) UntagResource(repoARN string, tagKeys []string) error {
	for _, key := range tagKeys {
		_, _ = s.db().Exec(`DELETE FROM repo_tags WHERE repo_arn=? AND tag_key=?`, repoARN, key)
	}
	return nil
}

// ListTagsForResource returns all tags for a resource ARN.
func (s *ECRStore) ListTagsForResource(repoARN string) ([]map[string]string, error) {
	rows, err := s.db().Query(
		`SELECT tag_key, tag_value FROM repo_tags WHERE repo_arn=? ORDER BY tag_key`,
		repoARN,
	)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []map[string]string
	for rows.Next() {
		var k, v string
		if err := rows.Scan(&k, &v); err != nil {
			return nil, err
		}
		out = append(out, map[string]string{"Key": k, "Value": v})
	}
	return out, rows.Err()
}

// --- Image Scan operations ---

// StartImageScan creates a scan findings record for an image.
func (s *ECRStore) StartImageScan(accountID, repoName, imageDigest string) error {
	now := time.Now().UTC()
	_, err := s.db().Exec(
		`INSERT INTO scan_findings (repo_name, image_digest, account_id, scan_status, findings_json, scanned_at)
		 VALUES (?, ?, ?, 'COMPLETE', '[]', ?)
		 ON CONFLICT(repo_name, image_digest, account_id) DO UPDATE SET scan_status='COMPLETE', scanned_at=excluded.scanned_at`,
		repoName, imageDigest, accountID, now,
	)
	return err
}

// DescribeImageScanFindings returns scan findings for an image.
func (s *ECRStore) DescribeImageScanFindings(accountID, repoName, imageDigest string) (string, string, error) {
	var status, findingsJSON string
	err := s.db().QueryRow(
		`SELECT scan_status, findings_json FROM scan_findings WHERE repo_name=? AND image_digest=? AND account_id=?`,
		repoName, imageDigest, accountID,
	).Scan(&status, &findingsJSON)
	if errors.Is(err, sql.ErrNoRows) {
		return "", "", ErrImageNotFound
	}
	if err != nil {
		return "", "", err
	}
	return status, findingsJSON, nil
}

// PutImageScanningConfiguration updates the scan_on_push setting for a repository.
func (s *ECRStore) PutImageScanningConfiguration(accountID, repoName string, scanOnPush bool) error {
	var exists int
	_ = s.db().QueryRow(`SELECT COUNT(*) FROM repositories WHERE name=? AND account_id=?`, repoName, accountID).Scan(&exists)
	if exists == 0 {
		return ErrRepositoryNotFound
	}
	scanVal := 0
	if scanOnPush {
		scanVal = 1
	}
	_, err := s.db().Exec(
		`UPDATE repositories SET scan_on_push=? WHERE name=? AND account_id=?`,
		scanVal, repoName, accountID,
	)
	return err
}

// GetImageDigestByTag looks up an image digest by tag.
func (s *ECRStore) GetImageDigestByTag(accountID, repoName, imageTag string) (string, error) {
	var digest string
	err := s.db().QueryRow(
		`SELECT image_digest FROM images WHERE repo_name=? AND account_id=? AND image_tag=? ORDER BY pushed_at DESC LIMIT 1`,
		repoName, accountID, imageTag,
	).Scan(&digest)
	if errors.Is(err, sql.ErrNoRows) {
		return "", ErrImageNotFound
	}
	return digest, err
}
