// SPDX-License-Identifier: Apache-2.0
package s3

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"time"
)

func readRenameReceiptTx(ctx context.Context, tx *sql.Tx, accountID, incarnation, token string) (canonical string, found bool, err error) {
	err = tx.QueryRowContext(ctx, `SELECT canonical_request FROM rename_receipts WHERE account_id=? AND incarnation=? AND client_token=?`, accountID, incarnation, token).Scan(&canonical)
	if errors.Is(err, sql.ErrNoRows) {
		return "", false, nil
	}
	return canonical, err == nil, err
}
func getRenameObjectMetaTx(ctx context.Context, tx *sql.Tx, bucket, key, accountID string) (*ObjectMeta, error) {
	var value ObjectMeta
	var modified int64
	err := tx.QueryRowContext(ctx, `SELECT bucket,key,size,content_type,etag,account_id,last_modified FROM objects WHERE bucket=? AND key=? AND account_id=?`, bucket, key, accountID).Scan(&value.Bucket, &value.Key, &value.Size, &value.ContentType, &value.ETag, &value.AccountID, &modified)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrObjectNotFound
	}
	if err != nil {
		return nil, err
	}
	value.LastModified = time.Unix(modified, 0)
	return &value, nil
}
func applyRenameMetadataTx(ctx context.Context, tx *sql.Tx, info DirectoryBucketInfo, value renameRequest, source ObjectMeta, now time.Time) error {
	if value.SourceKey != value.DestinationKey {
		rows, err := tx.QueryContext(ctx, `SELECT tag_key,tag_value FROM object_tags WHERE bucket=? AND key=? AND account_id=? ORDER BY tag_key`, value.Bucket, value.SourceKey, value.AccountID)
		if err != nil {
			return err
		}
		var tags [][2]string
		for rows.Next() {
			var tag [2]string
			if err = rows.Scan(&tag[0], &tag[1]); err != nil {
				_ = rows.Close()
				return err
			}
			tags = append(tags, tag)
		}
		err = errors.Join(rows.Err(), rows.Close())
		if err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, `INSERT OR REPLACE INTO objects(bucket,key,size,content_type,etag,account_id,last_modified) VALUES (?,?,?,?,?,?,?)`, value.Bucket, value.DestinationKey, source.Size, source.ContentType, source.ETag, value.AccountID, source.LastModified.Unix())
		if err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, `DELETE FROM object_tags WHERE bucket=? AND account_id=? AND key IN (?,?)`, value.Bucket, value.AccountID, value.SourceKey, value.DestinationKey)
		if err != nil {
			return err
		}
		for _, tag := range tags {
			if _, err = tx.ExecContext(ctx, `INSERT INTO object_tags(bucket,key,tag_key,tag_value,account_id) VALUES (?,?,?,?,?)`, value.Bucket, value.DestinationKey, tag[0], tag[1], value.AccountID); err != nil {
				return err
			}
		}
		if _, err = tx.ExecContext(ctx, `DELETE FROM objects WHERE bucket=? AND key=? AND account_id=?`, value.Bucket, value.SourceKey, value.AccountID); err != nil {
			return err
		}
	}
	if value.HasClientToken {
		_, err := tx.ExecContext(ctx, `INSERT INTO rename_receipts(account_id,incarnation,client_token,canonical_request,completed_at) VALUES (?,?,?,?,?)`, value.AccountID, info.Incarnation, value.ClientToken, value.Fingerprint, now.Unix())
		return err
	}
	return nil
}

// Caller owns objectMu, including admission. All queries after BeginTx use tx.
func (p *S3Provider) renameStoredObject(ctx context.Context, info DirectoryBucketInfo, value renameRequest, now time.Time) error {
	tx, err := p.metaStore.store.DB().BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	if value.HasClientToken {
		canonical, found, err := readRenameReceiptTx(ctx, tx, value.AccountID, info.Incarnation, value.ClientToken)
		if err != nil {
			return err
		}
		if found {
			if canonical != value.Fingerprint {
				return s3Error("IdempotencyParameterMismatch", 400, "client token was used with different parameters")
			}
			return nil
		}
	}
	source, err := getRenameObjectMetaTx(ctx, tx, value.Bucket, value.SourceKey, value.AccountID)
	if errors.Is(err, ErrObjectNotFound) {
		return s3Error("NoSuchKey", 404, "rename source not found")
	}
	if err != nil {
		return err
	}
	destination, err := getRenameObjectMetaTx(ctx, tx, value.Bucket, value.DestinationKey, value.AccountID)
	if err != nil && !errors.Is(err, ErrObjectNotFound) {
		return err
	}
	if err = checkRenameConditions(value.Conditions, *source, destination); err != nil {
		return err
	}
	move, err := p.fileStore.prepareRename(value.AccountID, value.Bucket, value.SourceKey, value.DestinationKey)
	abort := func(cause error) error {
		sqlErr := tx.Rollback()
		if errors.Is(sqlErr, sql.ErrTxDone) {
			sqlErr = nil
		}
		return errors.Join(cause, sqlErr, move.rollback())
	}
	if err != nil {
		return abort(err)
	}
	if err = applyRenameMetadataTx(ctx, tx, info, value, *source, now); err != nil {
		return abort(err)
	}
	if err = tx.Commit(); err != nil {
		return abort(err)
	}
	if err = move.cleanup(); err != nil {
		slog.Warn("s3 rename backup cleanup failed", "error", err)
	}
	return nil
}
