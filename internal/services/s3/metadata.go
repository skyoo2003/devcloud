// SPDX-License-Identifier: Apache-2.0

package s3

import (
	"database/sql"
	"errors"
	"strings"
	"time"

	"github.com/skyoo2003/devcloud/internal/storage/sqlite"
)

// Sentinel errors.
var (
	ErrBucketAlreadyExists = errors.New("bucket already exists")
	ErrBucketNotFound      = errors.New("bucket not found")
	ErrObjectNotFound      = errors.New("object not found")
	ErrUploadNotFound      = errors.New("multipart upload not found")
)

// BucketInfo holds metadata about an S3 bucket.
type BucketInfo struct {
	Name      string
	Region    string
	AccountID string
	CreatedAt time.Time
}

// ObjectMeta holds metadata about an S3 object.
type ObjectMeta struct {
	Bucket       string
	Key          string
	Size         int64
	ContentType  string
	ETag         string
	AccountID    string
	LastModified time.Time
}

// MultipartUploadInfo holds metadata about an active multipart upload.
type MultipartUploadInfo struct {
	UploadID  string
	Bucket    string
	Key       string
	AccountID string
	CreatedAt time.Time
}

// UploadPartInfo holds metadata about a single uploaded part.
type UploadPartInfo struct {
	UploadID   string
	PartNumber int
	ETag       string
	Size       int64
}

var metadataMigrations = []sqlite.Migration{
	{
		Version: 1,
		SQL: `CREATE TABLE IF NOT EXISTS buckets (
			name       TEXT NOT NULL,
			region     TEXT NOT NULL,
			account_id TEXT NOT NULL,
			created_at INTEGER NOT NULL,
			PRIMARY KEY (name, account_id)
		);`,
	},
	{
		Version: 2,
		SQL: `CREATE TABLE IF NOT EXISTS objects (
			bucket        TEXT NOT NULL,
			key           TEXT NOT NULL,
			size          INTEGER NOT NULL,
			content_type  TEXT NOT NULL,
			etag          TEXT NOT NULL,
			account_id    TEXT NOT NULL,
			last_modified INTEGER NOT NULL,
			PRIMARY KEY (bucket, key, account_id)
		);`,
	},
	{
		Version: 3,
		SQL: `CREATE TABLE IF NOT EXISTS multipart_uploads (
			upload_id  TEXT NOT NULL PRIMARY KEY,
			bucket     TEXT NOT NULL,
			key        TEXT NOT NULL,
			account_id TEXT NOT NULL,
			created_at INTEGER NOT NULL
		);`,
	},
	{
		Version: 4,
		SQL: `CREATE TABLE IF NOT EXISTS upload_parts (
			upload_id   TEXT NOT NULL,
			part_number INTEGER NOT NULL,
			etag        TEXT NOT NULL,
			size        INTEGER NOT NULL,
			PRIMARY KEY (upload_id, part_number)
		);`,
	},
	{
		Version: 5,
		SQL: `CREATE TABLE IF NOT EXISTS bucket_policies (
			bucket     TEXT NOT NULL,
			account_id TEXT NOT NULL,
			policy     TEXT NOT NULL,
			PRIMARY KEY (bucket, account_id)
		);`,
	},
	{
		Version: 6,
		SQL: `CREATE TABLE IF NOT EXISTS bucket_versioning (
			bucket     TEXT NOT NULL,
			account_id TEXT NOT NULL,
			status     TEXT NOT NULL,
			PRIMARY KEY (bucket, account_id)
		);`,
	},
	{
		Version: 7,
		SQL: `CREATE TABLE IF NOT EXISTS bucket_cors (
			bucket     TEXT NOT NULL,
			account_id TEXT NOT NULL,
			cors_xml   TEXT NOT NULL,
			PRIMARY KEY (bucket, account_id)
		);`,
	},
	{
		Version: 8,
		SQL: `CREATE TABLE IF NOT EXISTS bucket_tags (
			bucket     TEXT NOT NULL,
			tag_key    TEXT NOT NULL,
			tag_value  TEXT NOT NULL,
			account_id TEXT NOT NULL,
			PRIMARY KEY (bucket, tag_key, account_id)
		);`,
	},
	{
		Version: 9,
		SQL: `CREATE TABLE IF NOT EXISTS object_tags (
			bucket     TEXT NOT NULL,
			key        TEXT NOT NULL,
			tag_key    TEXT NOT NULL,
			tag_value  TEXT NOT NULL,
			account_id TEXT NOT NULL,
			PRIMARY KEY (bucket, key, tag_key, account_id)
		);`,
	},
	{
		Version: 10,
		SQL: `CREATE TABLE IF NOT EXISTS bucket_acls (
			bucket     TEXT NOT NULL,
			account_id TEXT NOT NULL,
			acl_xml    TEXT NOT NULL,
			PRIMARY KEY (bucket, account_id)
		);`,
	},
	{
		Version: 11,
		SQL: `CREATE TABLE IF NOT EXISTS bucket_notifications (
			bucket     TEXT NOT NULL,
			account_id TEXT NOT NULL,
			config_xml TEXT NOT NULL,
			PRIMARY KEY (bucket, account_id)
		);`,
	},
}

// MetadataStore is a SQLite-backed store for S3 bucket and object metadata.
type MetadataStore struct {
	store *sqlite.Store
}

// NewMetadataStore opens (or creates) a SQLite database at dbPath, enables WAL
// journal mode, and creates the buckets and objects tables if they don't exist.
func NewMetadataStore(dbPath string) (*MetadataStore, error) {
	store, err := sqlite.Open(dbPath, metadataMigrations)
	if err != nil {
		return nil, err
	}
	return &MetadataStore{store: store}, nil
}

// Close closes the underlying database connection.
func (s *MetadataStore) Close() error {
	return s.store.Close()
}

// CreateBucket inserts a new bucket record. Returns ErrBucketAlreadyExists if
// a bucket with the same name and account already exists.
func (s *MetadataStore) CreateBucket(name, region, accountID string) error {
	_, err := s.store.DB().Exec(
		`INSERT INTO buckets (name, region, account_id, created_at) VALUES (?, ?, ?, ?);`,
		name, region, accountID, time.Now().Unix(),
	)
	if err != nil {
		if sqlite.IsUniqueConstraintError(err) {
			return ErrBucketAlreadyExists
		}
		return err
	}
	return nil
}

// DeleteBucket removes a bucket record.
func (s *MetadataStore) DeleteBucket(name, accountID string) error {
	res, err := s.store.DB().Exec(
		`DELETE FROM buckets WHERE name = ? AND account_id = ?;`,
		name, accountID,
	)
	if err != nil {
		return err
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 0 {
		return ErrBucketNotFound
	}
	return nil
}

// ListBuckets returns all buckets belonging to the given account.
func (s *MetadataStore) ListBuckets(accountID string) ([]BucketInfo, error) {
	rows, err := s.store.DB().Query(
		`SELECT name, region, account_id, created_at FROM buckets WHERE account_id = ? ORDER BY name;`,
		accountID,
	)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	var buckets []BucketInfo
	for rows.Next() {
		var b BucketInfo
		var createdAtUnix int64
		if err := rows.Scan(&b.Name, &b.Region, &b.AccountID, &createdAtUnix); err != nil {
			return nil, err
		}
		b.CreatedAt = time.Unix(createdAtUnix, 0)
		buckets = append(buckets, b)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return buckets, nil
}

// PutObjectMeta inserts or replaces object metadata.
func (s *MetadataStore) PutObjectMeta(meta ObjectMeta) error {
	lastModified := meta.LastModified
	if lastModified.IsZero() {
		lastModified = time.Now()
	}
	_, err := s.store.DB().Exec(
		`INSERT OR REPLACE INTO objects (bucket, key, size, content_type, etag, account_id, last_modified)
		 VALUES (?, ?, ?, ?, ?, ?, ?);`,
		meta.Bucket, meta.Key, meta.Size, meta.ContentType, meta.ETag, meta.AccountID, lastModified.Unix(),
	)
	return err
}

// GetObjectMeta retrieves metadata for a specific object.
func (s *MetadataStore) GetObjectMeta(bucket, key, accountID string) (*ObjectMeta, error) {
	row := s.store.DB().QueryRow(
		`SELECT bucket, key, size, content_type, etag, account_id, last_modified
		 FROM objects WHERE bucket = ? AND key = ? AND account_id = ?;`,
		bucket, key, accountID,
	)
	var meta ObjectMeta
	var lastModifiedUnix int64
	if err := row.Scan(&meta.Bucket, &meta.Key, &meta.Size, &meta.ContentType, &meta.ETag, &meta.AccountID, &lastModifiedUnix); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrObjectNotFound
		}
		return nil, err
	}
	meta.LastModified = time.Unix(lastModifiedUnix, 0)
	return &meta, nil
}

// DeleteObjectMeta removes metadata for a specific object.
func (s *MetadataStore) DeleteObjectMeta(bucket, key, accountID string) error {
	res, err := s.store.DB().Exec(
		`DELETE FROM objects WHERE bucket = ? AND key = ? AND account_id = ?;`,
		bucket, key, accountID,
	)
	if err != nil {
		return err
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 0 {
		return ErrObjectNotFound
	}
	return nil
}

// ListObjects returns objects in a bucket matching the given prefix, up to
// maxKeys results. Pass maxKeys <= 0 for no limit.
func (s *MetadataStore) ListObjects(bucket, prefix, accountID string, maxKeys int) ([]ObjectMeta, error) {
	query := `SELECT bucket, key, size, content_type, etag, account_id, last_modified
	          FROM objects WHERE bucket = ? AND account_id = ? AND key LIKE ? ESCAPE '\' ORDER BY key`
	likePrefix := strings.ReplaceAll(prefix, `\`, `\\`)
	likePrefix = strings.ReplaceAll(likePrefix, "%", `\%`)
	likePrefix = strings.ReplaceAll(likePrefix, "_", `\_`)
	likePrefix += "%"

	var rows *sql.Rows
	var err error
	if maxKeys > 0 {
		query += " LIMIT ?"
		rows, err = s.store.DB().Query(query, bucket, accountID, likePrefix, maxKeys)
	} else {
		rows, err = s.store.DB().Query(query, bucket, accountID, likePrefix)
	}
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	var objects []ObjectMeta
	for rows.Next() {
		var meta ObjectMeta
		var lastModifiedUnix int64
		if err := rows.Scan(&meta.Bucket, &meta.Key, &meta.Size, &meta.ContentType, &meta.ETag, &meta.AccountID, &lastModifiedUnix); err != nil {
			return nil, err
		}
		meta.LastModified = time.Unix(lastModifiedUnix, 0)
		objects = append(objects, meta)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return objects, nil
}

// --- Multipart Upload methods ---

// CreateMultipartUpload records a new multipart upload.
func (s *MetadataStore) CreateMultipartUpload(uploadID, bucket, key, accountID string) error {
	_, err := s.store.DB().Exec(
		`INSERT INTO multipart_uploads (upload_id, bucket, key, account_id, created_at) VALUES (?, ?, ?, ?, ?);`,
		uploadID, bucket, key, accountID, time.Now().Unix(),
	)
	return err
}

// GetMultipartUpload retrieves a multipart upload by ID.
func (s *MetadataStore) GetMultipartUpload(uploadID string) (*MultipartUploadInfo, error) {
	row := s.store.DB().QueryRow(
		`SELECT upload_id, bucket, key, account_id, created_at FROM multipart_uploads WHERE upload_id = ?;`,
		uploadID,
	)
	var u MultipartUploadInfo
	var createdAtUnix int64
	if err := row.Scan(&u.UploadID, &u.Bucket, &u.Key, &u.AccountID, &createdAtUnix); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrUploadNotFound
		}
		return nil, err
	}
	u.CreatedAt = time.Unix(createdAtUnix, 0)
	return &u, nil
}

// DeleteMultipartUpload removes a multipart upload record and all its parts.
func (s *MetadataStore) DeleteMultipartUpload(uploadID string) error {
	_, err := s.store.DB().Exec(`DELETE FROM upload_parts WHERE upload_id = ?;`, uploadID)
	if err != nil {
		return err
	}
	_, err = s.store.DB().Exec(`DELETE FROM multipart_uploads WHERE upload_id = ?;`, uploadID)
	return err
}

// ListMultipartUploads returns all active multipart uploads for a bucket.
func (s *MetadataStore) ListMultipartUploads(bucket, accountID string) ([]MultipartUploadInfo, error) {
	rows, err := s.store.DB().Query(
		`SELECT upload_id, bucket, key, account_id, created_at FROM multipart_uploads WHERE bucket = ? AND account_id = ? ORDER BY created_at;`,
		bucket, accountID,
	)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	var uploads []MultipartUploadInfo
	for rows.Next() {
		var u MultipartUploadInfo
		var createdAtUnix int64
		if err := rows.Scan(&u.UploadID, &u.Bucket, &u.Key, &u.AccountID, &createdAtUnix); err != nil {
			return nil, err
		}
		u.CreatedAt = time.Unix(createdAtUnix, 0)
		uploads = append(uploads, u)
	}
	return uploads, rows.Err()
}

// PutUploadPart records or replaces a part's metadata.
func (s *MetadataStore) PutUploadPart(part UploadPartInfo) error {
	_, err := s.store.DB().Exec(
		`INSERT OR REPLACE INTO upload_parts (upload_id, part_number, etag, size) VALUES (?, ?, ?, ?);`,
		part.UploadID, part.PartNumber, part.ETag, part.Size,
	)
	return err
}

// ListUploadParts returns all parts for an upload, ordered by part number.
func (s *MetadataStore) ListUploadParts(uploadID string) ([]UploadPartInfo, error) {
	rows, err := s.store.DB().Query(
		`SELECT upload_id, part_number, etag, size FROM upload_parts WHERE upload_id = ? ORDER BY part_number;`,
		uploadID,
	)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	var parts []UploadPartInfo
	for rows.Next() {
		var p UploadPartInfo
		if err := rows.Scan(&p.UploadID, &p.PartNumber, &p.ETag, &p.Size); err != nil {
			return nil, err
		}
		parts = append(parts, p)
	}
	return parts, rows.Err()
}

// --- Bucket Policy methods ---

// PutBucketPolicy stores a raw JSON policy for a bucket.
func (s *MetadataStore) PutBucketPolicy(bucket, accountID, policy string) error {
	_, err := s.store.DB().Exec(
		`INSERT OR REPLACE INTO bucket_policies (bucket, account_id, policy) VALUES (?, ?, ?);`,
		bucket, accountID, policy,
	)
	return err
}

// GetBucketPolicy retrieves the raw JSON policy for a bucket.
func (s *MetadataStore) GetBucketPolicy(bucket, accountID string) (string, error) {
	var policy string
	err := s.store.DB().QueryRow(
		`SELECT policy FROM bucket_policies WHERE bucket = ? AND account_id = ?;`,
		bucket, accountID,
	).Scan(&policy)
	if errors.Is(err, sql.ErrNoRows) {
		return "", ErrObjectNotFound
	}
	return policy, err
}

// DeleteBucketPolicy removes the policy for a bucket.
func (s *MetadataStore) DeleteBucketPolicy(bucket, accountID string) error {
	_, err := s.store.DB().Exec(
		`DELETE FROM bucket_policies WHERE bucket = ? AND account_id = ?;`,
		bucket, accountID,
	)
	return err
}

// --- Bucket Versioning methods ---

// PutBucketVersioning stores the versioning status for a bucket.
func (s *MetadataStore) PutBucketVersioning(bucket, accountID, status string) error {
	_, err := s.store.DB().Exec(
		`INSERT OR REPLACE INTO bucket_versioning (bucket, account_id, status) VALUES (?, ?, ?);`,
		bucket, accountID, status,
	)
	return err
}

// GetBucketVersioning retrieves the versioning status for a bucket.
func (s *MetadataStore) GetBucketVersioning(bucket, accountID string) (string, error) {
	var status string
	err := s.store.DB().QueryRow(
		`SELECT status FROM bucket_versioning WHERE bucket = ? AND account_id = ?;`,
		bucket, accountID,
	).Scan(&status)
	if errors.Is(err, sql.ErrNoRows) {
		return "Suspended", nil
	}
	return status, err
}

// --- Bucket CORS methods ---

// PutBucketCors stores raw CORS XML for a bucket.
func (s *MetadataStore) PutBucketCors(bucket, accountID, corsXML string) error {
	_, err := s.store.DB().Exec(
		`INSERT OR REPLACE INTO bucket_cors (bucket, account_id, cors_xml) VALUES (?, ?, ?);`,
		bucket, accountID, corsXML,
	)
	return err
}

// GetBucketCors retrieves the stored CORS XML for a bucket.
func (s *MetadataStore) GetBucketCors(bucket, accountID string) (string, error) {
	var corsXML string
	err := s.store.DB().QueryRow(
		`SELECT cors_xml FROM bucket_cors WHERE bucket = ? AND account_id = ?;`,
		bucket, accountID,
	).Scan(&corsXML)
	if errors.Is(err, sql.ErrNoRows) {
		return "", ErrObjectNotFound
	}
	return corsXML, err
}

// DeleteBucketCors removes the CORS configuration for a bucket.
func (s *MetadataStore) DeleteBucketCors(bucket, accountID string) error {
	_, err := s.store.DB().Exec(
		`DELETE FROM bucket_cors WHERE bucket = ? AND account_id = ?;`,
		bucket, accountID,
	)
	return err
}

// --- Bucket Tag methods ---

// PutBucketTags replaces all tags for a bucket.
func (s *MetadataStore) PutBucketTags(bucket, accountID string, tags map[string]string) error {
	tx, err := s.store.DB().Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck
	if _, err := tx.Exec(`DELETE FROM bucket_tags WHERE bucket = ? AND account_id = ?;`, bucket, accountID); err != nil {
		return err
	}
	for k, v := range tags {
		if _, err := tx.Exec(
			`INSERT INTO bucket_tags (bucket, tag_key, tag_value, account_id) VALUES (?, ?, ?, ?);`,
			bucket, k, v, accountID,
		); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// GetBucketTags retrieves all tags for a bucket.
func (s *MetadataStore) GetBucketTags(bucket, accountID string) (map[string]string, error) {
	rows, err := s.store.DB().Query(
		`SELECT tag_key, tag_value FROM bucket_tags WHERE bucket = ? AND account_id = ?;`,
		bucket, accountID,
	)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	tags := make(map[string]string)
	for rows.Next() {
		var k, v string
		if err := rows.Scan(&k, &v); err != nil {
			return nil, err
		}
		tags[k] = v
	}
	return tags, rows.Err()
}

// DeleteBucketTags removes all tags for a bucket.
func (s *MetadataStore) DeleteBucketTags(bucket, accountID string) error {
	_, err := s.store.DB().Exec(
		`DELETE FROM bucket_tags WHERE bucket = ? AND account_id = ?;`,
		bucket, accountID,
	)
	return err
}

// --- Object Tag methods ---

// PutObjectTags replaces all tags for an object.
func (s *MetadataStore) PutObjectTags(bucket, key, accountID string, tags map[string]string) error {
	tx, err := s.store.DB().Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck
	if _, err := tx.Exec(`DELETE FROM object_tags WHERE bucket = ? AND key = ? AND account_id = ?;`, bucket, key, accountID); err != nil {
		return err
	}
	for k, v := range tags {
		if _, err := tx.Exec(
			`INSERT INTO object_tags (bucket, key, tag_key, tag_value, account_id) VALUES (?, ?, ?, ?, ?);`,
			bucket, key, k, v, accountID,
		); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// GetObjectTags retrieves all tags for an object.
func (s *MetadataStore) GetObjectTags(bucket, key, accountID string) (map[string]string, error) {
	rows, err := s.store.DB().Query(
		`SELECT tag_key, tag_value FROM object_tags WHERE bucket = ? AND key = ? AND account_id = ?;`,
		bucket, key, accountID,
	)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	tags := make(map[string]string)
	for rows.Next() {
		var k, v string
		if err := rows.Scan(&k, &v); err != nil {
			return nil, err
		}
		tags[k] = v
	}
	return tags, rows.Err()
}

// DeleteObjectTags removes all tags for an object.
func (s *MetadataStore) DeleteObjectTags(bucket, key, accountID string) error {
	_, err := s.store.DB().Exec(
		`DELETE FROM object_tags WHERE bucket = ? AND key = ? AND account_id = ?;`,
		bucket, key, accountID,
	)
	return err
}

// --- Bucket ACL methods ---

// PutBucketACL stores raw ACL XML for a bucket.
func (s *MetadataStore) PutBucketACL(bucket, accountID, aclXML string) error {
	_, err := s.store.DB().Exec(
		`INSERT OR REPLACE INTO bucket_acls (bucket, account_id, acl_xml) VALUES (?, ?, ?);`,
		bucket, accountID, aclXML,
	)
	return err
}

// GetBucketACL retrieves the stored ACL XML for a bucket.
func (s *MetadataStore) GetBucketACL(bucket, accountID string) (string, error) {
	var aclXML string
	err := s.store.DB().QueryRow(
		`SELECT acl_xml FROM bucket_acls WHERE bucket = ? AND account_id = ?;`,
		bucket, accountID,
	).Scan(&aclXML)
	if errors.Is(err, sql.ErrNoRows) {
		return "", ErrObjectNotFound
	}
	return aclXML, err
}

// --- Bucket Notification methods ---

// PutBucketNotification stores raw notification config XML for a bucket.
func (s *MetadataStore) PutBucketNotification(bucket, accountID, configXML string) error {
	_, err := s.store.DB().Exec(
		`INSERT OR REPLACE INTO bucket_notifications (bucket, account_id, config_xml) VALUES (?, ?, ?);`,
		bucket, accountID, configXML,
	)
	return err
}

// GetBucketNotification retrieves the stored notification config XML for a bucket.
func (s *MetadataStore) GetBucketNotification(bucket, accountID string) (string, error) {
	var configXML string
	err := s.store.DB().QueryRow(
		`SELECT config_xml FROM bucket_notifications WHERE bucket = ? AND account_id = ?;`,
		bucket, accountID,
	).Scan(&configXML)
	if errors.Is(err, sql.ErrNoRows) {
		return "", ErrObjectNotFound
	}
	return configXML, err
}
