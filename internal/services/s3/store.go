// SPDX-License-Identifier: Apache-2.0

package s3

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// FileStore is a simple filesystem-based object storage backend.
type FileStore struct {
	baseDir string
}

// NewFileStore creates a new FileStore rooted at baseDir.
func NewFileStore(baseDir string) *FileStore {
	abs, err := filepath.Abs(baseDir)
	if err != nil {
		abs = filepath.Clean(baseDir)
	}
	return &FileStore{baseDir: filepath.Clean(abs)}
}

// safePath joins the components under baseDir and verifies the result does not
// escape the base directory. It returns an error on path traversal attempts.
func (fs *FileStore) safePath(parts ...string) (string, error) {
	for _, part := range parts {
		if part == "" || part == "." || part == ".." ||
			strings.ContainsAny(part, "/\\") ||
			strings.ContainsFunc(part, func(r rune) bool { return os.IsPathSeparator(byte(r)) }) {
			return "", fmt.Errorf("invalid path component: %q", part)
		}
	}

	joined := filepath.Join(append([]string{fs.baseDir}, parts...)...)
	cleaned := filepath.Clean(joined)

	rel, err := filepath.Rel(fs.baseDir, cleaned)
	if err != nil {
		return "", fmt.Errorf("invalid path: %w", err)
	}
	// Ensure the resolved path is still under baseDir.
	if rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) || filepath.IsAbs(rel) {
		return "", fmt.Errorf("path traversal detected: %s", cleaned)
	}
	return cleaned, nil
}

// objectPath returns the absolute filesystem path for the given object.
func (fs *FileStore) objectPath(accountID, bucket, key string) (string, error) {
	return fs.safePath(accountID, bucket, key)
}

// bucketDir returns the absolute filesystem path for the given bucket.
func (fs *FileStore) bucketDir(accountID, bucket string) (string, error) {
	return fs.safePath(accountID, bucket)
}

// CreateBucketDir creates the directory for the given bucket.
func (fs *FileStore) CreateBucketDir(accountID, bucket string) error {
	dir, err := fs.bucketDir(accountID, bucket)
	if err != nil {
		return err
	}
	return os.MkdirAll(dir, 0o755)
}

// DeleteBucketDir removes the directory for the given bucket and all its contents.
func (fs *FileStore) DeleteBucketDir(accountID, bucket string) error {
	dir, err := fs.bucketDir(accountID, bucket)
	if err != nil {
		return err
	}
	return os.RemoveAll(dir)
}

// PutObject writes data to the object identified by accountID, bucket, and key.
// Intermediate directories are created automatically.
func (fs *FileStore) PutObject(accountID, bucket, key string, data []byte) error {
	path, err := fs.objectPath(accountID, bucket, key)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	return os.WriteFile(path, data, 0o644)
}

// GetObject reads and returns the data for the given object.
func (fs *FileStore) GetObject(accountID, bucket, key string) ([]byte, error) {
	path, err := fs.objectPath(accountID, bucket, key)
	if err != nil {
		return nil, err
	}
	return os.ReadFile(path)
}

// DeleteObject removes the given object from the filesystem.
func (fs *FileStore) DeleteObject(accountID, bucket, key string) error {
	path, err := fs.objectPath(accountID, bucket, key)
	if err != nil {
		return err
	}
	return os.Remove(path)
}

// ObjectExists reports whether the given object exists on the filesystem.
func (fs *FileStore) ObjectExists(accountID, bucket, key string) bool {
	path, err := fs.objectPath(accountID, bucket, key)
	if err != nil {
		return false
	}
	_, err = os.Stat(path)
	return err == nil
}
