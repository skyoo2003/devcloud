// SPDX-License-Identifier: Apache-2.0

package s3

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
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
	resolved, err := filepath.EvalSymlinks(abs)
	if err != nil {
		resolved = abs
	}
	return &FileStore{baseDir: resolved}
}

// validPathComponent checks that a single path segment contains no path
// separators, dot-only components, or empty values.
func validPathComponent(part string) error {
	// IsLocal covers "", "..", "../x" and absolute paths, and is the guard
	// CodeQL recognises as a path-injection barrier.
	if !filepath.IsLocal(part) || part == "." {
		return fmt.Errorf("invalid path component: %q", part)
	}
	if strings.ContainsAny(part, "/\\") ||
		strings.ContainsFunc(part, func(r rune) bool { return os.IsPathSeparator(byte(r)) }) {
		return fmt.Errorf("invalid path component: %q", part)
	}
	return nil
}

// safePath joins the components under baseDir and verifies the result does not
// escape the base directory. It returns an error on path traversal attempts.
// All components must be single path segments (no separators).
func (fs *FileStore) safePath(parts ...string) (string, error) {
	cleanParts := make([]string, 0, len(parts))
	for _, part := range parts {
		if part == "" {
			continue
		}
		if !filepath.IsLocal(part) {
			return "", fmt.Errorf("path traversal detected in segment: %s", part)
		}
		cleanParts = append(cleanParts, filepath.Clean(part))
	}

	candidate := filepath.Join(append([]string{fs.baseDir}, cleanParts...)...)

	// Resolve symlinks if the candidate path exists; otherwise use the joined path.
	if resolved, err := filepath.EvalSymlinks(candidate); err == nil {
		candidate = resolved
	}

	rel, err := filepath.Rel(fs.baseDir, candidate)
	if err != nil {
		return "", fmt.Errorf("resolve relative path: %w", err)
	}
	if rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) || filepath.IsAbs(rel) {
		return "", fmt.Errorf("path traversal detected: %s", candidate)
	}

	return candidate, nil
}

// objectPath returns the absolute filesystem path for the given object.
// Unlike safePath, the key may contain '/' (e.g. "photos/a.jpg") which is
// valid for S3 object keys. Containment under the bucket is still enforced.
func (fs *FileStore) objectPath(accountID, bucket, key string) (string, error) {
	if err := validPathComponent(accountID); err != nil {
		return "", err
	}
	if err := validPathComponent(bucket); err != nil {
		return "", err
	}
	// The key gets IsLocal rather than validPathComponent because '/' is legal
	// in it. Checking the joined path against baseDir alone is not enough: a key
	// like "../victim/secret" leaves its own bucket while staying under baseDir.
	if !filepath.IsLocal(key) {
		return "", fmt.Errorf("invalid path component: %q", key)
	}

	joined := filepath.Join(fs.baseDir, accountID, bucket, key)
	cleaned := filepath.Clean(joined)

	rel, err := filepath.Rel(fs.baseDir, cleaned)
	if err != nil {
		return "", fmt.Errorf("invalid path: %w", err)
	}
	if rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) || filepath.IsAbs(rel) {
		return "", fmt.Errorf("path traversal detected: %s", cleaned)
	}
	return cleaned, nil
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

// multipartDir returns the directory used to store parts for an upload.
func (fs *FileStore) multipartDir(uploadID string) (string, error) {
	if err := validPathComponent(uploadID); err != nil {
		return "", err
	}
	return fs.safePath("_multipart", uploadID)
}

// partPath returns the path to a specific part file.
func (fs *FileStore) partPath(uploadID string, partNumber int) (string, error) {
	if partNumber < 1 {
		return "", fmt.Errorf("invalid part number")
	}
	if err := validPathComponent(uploadID); err != nil {
		return "", err
	}
	partStr := strconv.Itoa(partNumber)
	if err := validPathComponent(partStr); err != nil {
		return "", err
	}
	return fs.safePath("_multipart", uploadID, partStr)
}

// ReadMultipartPart reads the bytes of a stored part file. If the file does not exist,
// it returns nil, false, nil.
func (fs *FileStore) ReadMultipartPart(uploadID string, partNumber int) ([]byte, bool, error) {
	path, err := fs.partPath(uploadID, partNumber)
	if err != nil {
		return nil, false, err
	}
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	return data, true, nil
}

func writeAtomicFile(path string, data []byte) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	f, err := os.CreateTemp(dir, ".tmp-*")
	if err != nil {
		return err
	}
	defer func() { _ = f.Close(); _ = os.Remove(f.Name()) }()
	if err = f.Chmod(0o644); err != nil {
		return err
	}
	if _, err = f.Write(data); err != nil {
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	return os.Rename(f.Name(), path)
}

// ReadObject reads and returns the data for the given object, along with whether it existed.
func (fs *FileStore) ReadObject(accountID, bucket, key string) ([]byte, bool, error) {
	path, err := fs.objectPath(accountID, bucket, key)
	if err != nil {
		return nil, false, err
	}
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	return data, true, nil
}

// WriteObjectAtomic safely and atomically writes object data to disk using a temporary file.
func (fs *FileStore) WriteObjectAtomic(accountID, bucket, key string, data []byte) error {
	path, err := fs.objectPath(accountID, bucket, key)
	if err != nil {
		return err
	}
	return writeAtomicFile(path, data)
}

// WriteMultipartPartAtomic safely and atomically writes part data to disk using a temporary file.
func (fs *FileStore) WriteMultipartPartAtomic(uploadID string, partNumber int, data []byte) error {
	path, err := fs.partPath(uploadID, partNumber)
	if err != nil {
		return err
	}
	return writeAtomicFile(path, data)
}

// DeleteMultipartPart removes a specific part file.
func (fs *FileStore) DeleteMultipartPart(uploadID string, partNumber int) error {
	path, err := fs.partPath(uploadID, partNumber)
	if err != nil {
		return err
	}
	return os.Remove(path)
}
