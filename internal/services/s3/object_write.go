// SPDX-License-Identifier: Apache-2.0
package s3

import (
	"errors"
)

// Caller holds objectMu so conditional copies cannot observe replacement bytes
// with the previous metadata, including after a rejected metadata write.
func (p *S3Provider) storeObjectWithMetadata(meta ObjectMeta, data []byte) error {
	previous, existed, err := p.fileStore.ReadObject(meta.AccountID, meta.Bucket, meta.Key)
	if err != nil {
		return err
	}
	if err = p.fileStore.WriteObjectAtomic(meta.AccountID, meta.Bucket, meta.Key, data); err != nil {
		return err
	}
	if err = p.metaStore.PutObjectMeta(meta); err != nil {
		var restore error
		if existed {
			restore = p.fileStore.WriteObjectAtomic(meta.AccountID, meta.Bucket, meta.Key, previous)
		} else {
			restore = p.fileStore.DeleteObject(meta.AccountID, meta.Bucket, meta.Key)
		}
		return errors.Join(err, restore)
	}
	return nil
}
