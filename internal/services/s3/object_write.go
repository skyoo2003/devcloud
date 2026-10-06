// SPDX-License-Identifier: Apache-2.0
package s3

import (
	"errors"
	"os"
	"path/filepath"
)

// Caller holds objectMu so conditional copies cannot observe replacement bytes
// with the previous metadata, including after a rejected metadata write.
func (p *S3Provider) storeObjectWithMetadata(meta ObjectMeta, data []byte) error {
	path, err := p.fileStore.objectPath(meta.AccountID, meta.Bucket, meta.Key)
	if err != nil {
		return err
	}
	previous, err := os.ReadFile(path)
	existed := err == nil
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err = os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	if err = writeMultipartPartFile(path, data); err != nil {
		return err
	}
	if err = p.metaStore.PutObjectMeta(meta); err != nil {
		var restore error
		if existed {
			restore = writeMultipartPartFile(path, previous)
		} else {
			restore = os.Remove(path)
		}
		return errors.Join(err, restore)
	}
	return nil
}
