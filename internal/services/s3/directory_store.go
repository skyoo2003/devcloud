// SPDX-License-Identifier: Apache-2.0
package s3

import (
	"database/sql"
	"errors"
	"time"
)

type DirectoryBucketInfo struct {
	BucketInfo
	Zone, Incarnation string
}

func (s *MetadataStore) GetBucketInfo(name, accountID string) (*BucketInfo, error) {
	var value BucketInfo
	var created int64
	err := s.store.DB().QueryRow(`SELECT name,region,account_id,created_at FROM buckets WHERE name=? AND account_id=?`, name, accountID).Scan(&value.Name, &value.Region, &value.AccountID, &created)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrBucketNotFound
	}
	if err != nil {
		return nil, err
	}
	value.CreatedAt = time.Unix(created, 0)
	return &value, nil
}
func (s *MetadataStore) GetDirectoryBucket(name, accountID string) (*DirectoryBucketInfo, error) {
	bucket, err := s.GetBucketInfo(name, accountID)
	if err != nil {
		return nil, err
	}
	value := DirectoryBucketInfo{BucketInfo: *bucket}
	err = s.store.DB().QueryRow(`SELECT zone,incarnation FROM directory_buckets WHERE bucket=? AND account_id=?`, name, accountID).Scan(&value.Zone, &value.Incarnation)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &value, nil
}
func (s *MetadataStore) CreateDirectoryBucket(value DirectoryBucketInfo) error {
	tx, err := s.store.DB().Begin()
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	_, err = tx.Exec(`INSERT INTO buckets(name,region,account_id,created_at) VALUES (?,?,?,?)`, value.Name, value.Region, value.AccountID, value.CreatedAt.Unix())
	if err != nil {
		return err
	}
	_, err = tx.Exec(`INSERT INTO directory_buckets(account_id,bucket,zone,incarnation) VALUES (?,?,?,?)`, value.AccountID, value.Name, value.Zone, value.Incarnation)
	if err != nil {
		return err
	}
	return tx.Commit()
}
func (s *MetadataStore) DeleteDirectoryBucket(name, accountID string) error {
	tx, err := s.store.DB().Begin()
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	var incarnation string
	err = tx.QueryRow(`SELECT incarnation FROM directory_buckets WHERE bucket=? AND account_id=?`, name, accountID).Scan(&incarnation)
	if errors.Is(err, sql.ErrNoRows) {
		return ErrBucketNotFound
	}
	if err != nil {
		return err
	}
	var n int
	if err = tx.QueryRow(`SELECT (SELECT count(*) FROM objects WHERE bucket=? AND account_id=?)+(SELECT count(*) FROM multipart_uploads WHERE bucket=? AND account_id=?)`, name, accountID, name, accountID).Scan(&n); err != nil {
		return err
	}
	if n != 0 {
		return s3Error("BucketNotEmpty", 409, "directory bucket is not empty")
	}
	for _, table := range []string{"directory_sessions", "rename_receipts"} {
		if _, err = tx.Exec("DELETE FROM "+table+" WHERE account_id=? AND incarnation=?", accountID, incarnation); err != nil {
			return err
		}
	}
	for _, table := range []string{"directory_buckets", "bucket_policies", "bucket_versioning", "bucket_cors", "bucket_tags", "object_tags", "bucket_acls", "bucket_notifications"} {
		if _, err = tx.Exec("DELETE FROM "+table+" WHERE account_id=? AND bucket=?", accountID, name); err != nil {
			return err
		}
	}
	if _, err = tx.Exec(`DELETE FROM buckets WHERE account_id=? AND name=?`, accountID, name); err != nil {
		return err
	}
	return tx.Commit()
}
