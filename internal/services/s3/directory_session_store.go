// SPDX-License-Identifier: Apache-2.0
package s3

import "time"

type directorySession struct {
	AccessKeyID, TokenDigest, AccountID, Bucket, Incarnation, Mode string
	ExpiresAt                                                      time.Time
}

func (s *MetadataStore) PutDirectorySession(value directorySession) error {
	_, err := s.store.DB().Exec(`INSERT INTO directory_sessions(access_key_id,token_digest,account_id,bucket,incarnation,mode,expires_at) VALUES (?,?,?,?,?,?,?)`, value.AccessKeyID, value.TokenDigest, value.AccountID, value.Bucket, value.Incarnation, value.Mode, value.ExpiresAt.Unix())
	return err
}
func (s *MetadataStore) GetDirectorySession(accessKeyID string) (*directorySession, error) {
	var value directorySession
	var expires int64
	err := s.store.DB().QueryRow(`SELECT access_key_id,token_digest,account_id,bucket,incarnation,mode,expires_at FROM directory_sessions WHERE access_key_id=?`, accessKeyID).Scan(&value.AccessKeyID, &value.TokenDigest, &value.AccountID, &value.Bucket, &value.Incarnation, &value.Mode, &expires)
	if err != nil {
		return nil, err
	}
	value.ExpiresAt = time.Unix(expires, 0)
	return &value, nil
}
