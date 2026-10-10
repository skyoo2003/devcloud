// SPDX-License-Identifier: Apache-2.0

package blob

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

var (
	ErrContainerNotFound      = errors.New("ContainerNotFound")
	ErrContainerAlreadyExists = errors.New("ContainerAlreadyExists")
	ErrBlobNotFound           = errors.New("BlobNotFound")
	ErrInvalidName            = errors.New("InvalidName")
)

type ContainerInfo struct {
	Name      string    `json:"name"`
	Account   string    `json:"account"`
	CreatedAt time.Time `json:"createdAt"`
	ETag      string    `json:"etag"`
}

type BlobInfo struct {
	Name         string    `json:"name"`
	Container    string    `json:"container"`
	Account      string    `json:"account"`
	Size         int64     `json:"size"`
	ContentType  string    `json:"contentType"`
	MD5          string    `json:"md5"`
	ETag         string    `json:"etag"`
	CreatedAt    time.Time `json:"createdAt"`
	LastModified time.Time `json:"lastModified"`
	BlobType     string    `json:"blobType"`
}

type FileStore struct {
	mu      sync.RWMutex
	baseDir string
}

func NewFileStore(baseDir string) *FileStore {
	abs, err := filepath.Abs(baseDir)
	if err != nil {
		abs = filepath.Clean(baseDir)
	}
	cleaned := filepath.Clean(abs)
	_ = os.MkdirAll(cleaned, 0755)
	return &FileStore{baseDir: cleaned}
}

// validPathComponent checks that a single path segment contains no path
// separators, dot-only components, or empty values.
func validPathComponent(part string) error {
	// IsLocal covers "", "..", "../x" and absolute paths, and is the guard
	// CodeQL recognises as a path-injection barrier.
	if !filepath.IsLocal(part) || part == "." {
		return ErrInvalidName
	}
	if strings.ContainsAny(part, "/\\") ||
		strings.ContainsFunc(part, func(r rune) bool { return os.IsPathSeparator(byte(r)) }) {
		return ErrInvalidName
	}
	return nil
}

func (fs *FileStore) safeContainerDir(account, container string) (string, error) {
	if err := validPathComponent(account); err != nil {
		return "", err
	}
	if err := validPathComponent(container); err != nil {
		return "", err
	}
	dir := filepath.Join(fs.baseDir, account, container)
	cleaned := filepath.Clean(dir)
	rel, err := filepath.Rel(fs.baseDir, cleaned)
	if err != nil || !filepath.IsLocal(rel) || rel == "." {
		return "", ErrInvalidName
	}
	return cleaned, nil
}

func (fs *FileStore) safeBlobPaths(account, container, blob string) (dataPath, metaPath string, err error) {
	containerDir, err := fs.safeContainerDir(account, container)
	if err != nil {
		return "", "", err
	}
	// The blob gets IsLocal rather than validPathComponent because '/' is legal
	// in it (virtual directories). IsLocal is the guard CodeQL recognises as a
	// path-injection barrier.
	if !filepath.IsLocal(blob) || blob == "." {
		return "", "", ErrInvalidName
	}
	if strings.Contains(blob, "\\") ||
		strings.ContainsFunc(blob, func(r rune) bool { return os.IsPathSeparator(byte(r)) && r != '/' }) {
		return "", "", ErrInvalidName
	}

	cleanBlob := filepath.Clean(blob)
	if !filepath.IsLocal(cleanBlob) || cleanBlob == "." {
		return "", "", ErrInvalidName
	}

	dataDir := filepath.Join(containerDir, "data")
	dataPath = filepath.Join(dataDir, cleanBlob)
	cleanedData := filepath.Clean(dataPath)
	relData, err := filepath.Rel(dataDir, cleanedData)
	if err != nil || !filepath.IsLocal(relData) || relData == "." {
		return "", "", ErrInvalidName
	}

	metaDir := filepath.Join(containerDir, "meta")
	metaPath = filepath.Join(metaDir, cleanBlob+".json")
	cleanedMeta := filepath.Clean(metaPath)
	relMeta, err := filepath.Rel(metaDir, cleanedMeta)
	if err != nil || !filepath.IsLocal(relMeta) || relMeta == "." {
		return "", "", ErrInvalidName
	}

	return cleanedData, cleanedMeta, nil
}

func (fs *FileStore) CreateContainer(account, container string) (*ContainerInfo, error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	dir, err := fs.safeContainerDir(account, container)
	if err != nil {
		return nil, err
	}

	metaFile := filepath.Join(dir, "container.json")
	if _, err := os.Stat(metaFile); err == nil {
		return nil, ErrContainerAlreadyExists
	}

	if err := os.MkdirAll(filepath.Join(dir, "data"), 0755); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Join(dir, "meta"), 0755); err != nil {
		return nil, err
	}

	now := time.Now().UTC()
	etag := fmt.Sprintf("\"0x%X\"", now.UnixNano())
	info := &ContainerInfo{
		Name:      container,
		Account:   account,
		CreatedAt: now,
		ETag:      etag,
	}

	data, err := json.Marshal(info)
	if err != nil {
		return nil, err
	}
	if err := os.WriteFile(metaFile, data, 0644); err != nil {
		return nil, err
	}
	return info, nil
}

func (fs *FileStore) DeleteContainer(account, container string) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	dir, err := fs.safeContainerDir(account, container)
	if err != nil {
		return err
	}

	metaFile := filepath.Join(dir, "container.json")
	if _, err := os.Stat(metaFile); os.IsNotExist(err) {
		return ErrContainerNotFound
	}

	return os.RemoveAll(dir)
}

func (fs *FileStore) GetContainer(account, container string) (*ContainerInfo, error) {
	fs.mu.RLock()
	defer fs.mu.RUnlock()

	dir, err := fs.safeContainerDir(account, container)
	if err != nil {
		return nil, err
	}

	metaFile := filepath.Join(dir, "container.json")
	data, err := os.ReadFile(metaFile)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, ErrContainerNotFound
		}
		return nil, err
	}

	var info ContainerInfo
	if err := json.Unmarshal(data, &info); err != nil {
		return nil, err
	}
	return &info, nil
}

func (fs *FileStore) ListContainers(account string) ([]ContainerInfo, error) {
	fs.mu.RLock()
	defer fs.mu.RUnlock()

	if err := validPathComponent(account); err != nil {
		return nil, err
	}

	accountDir := filepath.Join(fs.baseDir, account)
	cleaned := filepath.Clean(accountDir)
	rel, err := filepath.Rel(fs.baseDir, cleaned)
	if err != nil || !filepath.IsLocal(rel) || rel == "." {
		return nil, ErrInvalidName
	}

	entries, err := os.ReadDir(cleaned)
	if err != nil {
		if os.IsNotExist(err) {
			return []ContainerInfo{}, nil
		}
		return nil, err
	}

	var list []ContainerInfo
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		metaFile := filepath.Join(cleaned, entry.Name(), "container.json")
		data, err := os.ReadFile(metaFile)
		if err != nil {
			continue
		}
		var info ContainerInfo
		if err := json.Unmarshal(data, &info); err == nil {
			list = append(list, info)
		}
	}
	return list, nil
}

func (fs *FileStore) PutBlob(account, container, blob string, contentType string, r io.Reader) (*BlobInfo, error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	// Ensure container exists
	containerDir, err := fs.safeContainerDir(account, container)
	if err != nil {
		return nil, err
	}
	if _, err := os.Stat(filepath.Join(containerDir, "container.json")); os.IsNotExist(err) {
		return nil, ErrContainerNotFound
	}

	dataPath, metaPath, err := fs.safeBlobPaths(account, container, blob)
	if err != nil {
		return nil, err
	}

	if err := os.MkdirAll(filepath.Dir(dataPath), 0755); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Dir(metaPath), 0755); err != nil {
		return nil, err
	}

	// Read content and compute MD5
	tmpFile := filepath.Clean(dataPath + ".tmp")
	relTmp, err := filepath.Rel(filepath.Dir(dataPath), tmpFile)
	if err != nil || !filepath.IsLocal(relTmp) || relTmp == "." {
		return nil, ErrInvalidName
	}
	f, err := os.Create(tmpFile)
	if err != nil {
		return nil, err
	}

	hasher := md5.New()
	mw := io.MultiWriter(f, hasher)
	size, err := io.Copy(mw, r)
	_ = f.Close()
	if err != nil {
		_ = os.Remove(tmpFile)
		return nil, err
	}

	if err := os.Rename(tmpFile, dataPath); err != nil {
		_ = os.Remove(tmpFile)
		return nil, err
	}

	md5Sum := hex.EncodeToString(hasher.Sum(nil))
	now := time.Now().UTC()
	etag := fmt.Sprintf("\"0x%X\"", now.UnixNano())
	if contentType == "" {
		contentType = "application/octet-stream"
	}

	// Check existing metadata to preserve creation time
	createdAt := now
	if existingData, err := os.ReadFile(metaPath); err == nil {
		var existing BlobInfo
		if json.Unmarshal(existingData, &existing) == nil && !existing.CreatedAt.IsZero() {
			createdAt = existing.CreatedAt
		}
	}

	info := &BlobInfo{
		Name:         blob,
		Container:    container,
		Account:      account,
		Size:         size,
		ContentType:  contentType,
		MD5:          md5Sum,
		ETag:         etag,
		CreatedAt:    createdAt,
		LastModified: now,
		BlobType:     "BlockBlob",
	}

	metaBytes, err := json.Marshal(info)
	if err != nil {
		return nil, err
	}
	if err := os.WriteFile(metaPath, metaBytes, 0644); err != nil {
		return nil, err
	}
	return info, nil
}

func (fs *FileStore) GetBlob(account, container, blob string) (*BlobInfo, []byte, error) {
	fs.mu.RLock()
	defer fs.mu.RUnlock()

	containerDir, err := fs.safeContainerDir(account, container)
	if err != nil {
		return nil, nil, err
	}
	if _, err := os.Stat(filepath.Join(containerDir, "container.json")); os.IsNotExist(err) {
		return nil, nil, ErrContainerNotFound
	}

	dataPath, metaPath, err := fs.safeBlobPaths(account, container, blob)
	if err != nil {
		return nil, nil, err
	}

	metaBytes, err := os.ReadFile(metaPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil, ErrBlobNotFound
		}
		return nil, nil, err
	}

	var info BlobInfo
	if err := json.Unmarshal(metaBytes, &info); err != nil {
		return nil, nil, err
	}

	data, err := os.ReadFile(dataPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil, ErrBlobNotFound
		}
		return nil, nil, err
	}
	return &info, data, nil
}

func (fs *FileStore) GetBlobProperties(account, container, blob string) (*BlobInfo, error) {
	fs.mu.RLock()
	defer fs.mu.RUnlock()

	containerDir, err := fs.safeContainerDir(account, container)
	if err != nil {
		return nil, err
	}
	if _, err := os.Stat(filepath.Join(containerDir, "container.json")); os.IsNotExist(err) {
		return nil, ErrContainerNotFound
	}

	_, metaPath, err := fs.safeBlobPaths(account, container, blob)
	if err != nil {
		return nil, err
	}

	metaBytes, err := os.ReadFile(metaPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, ErrBlobNotFound
		}
		return nil, err
	}

	var info BlobInfo
	if err := json.Unmarshal(metaBytes, &info); err != nil {
		return nil, err
	}
	return &info, nil
}

func (fs *FileStore) DeleteBlob(account, container, blob string) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	containerDir, err := fs.safeContainerDir(account, container)
	if err != nil {
		return err
	}
	if _, err := os.Stat(filepath.Join(containerDir, "container.json")); os.IsNotExist(err) {
		return ErrContainerNotFound
	}

	dataPath, metaPath, err := fs.safeBlobPaths(account, container, blob)
	if err != nil {
		return err
	}

	if _, err := os.Stat(metaPath); os.IsNotExist(err) {
		return ErrBlobNotFound
	}

	_ = os.Remove(dataPath)
	_ = os.Remove(metaPath)
	return nil
}

func (fs *FileStore) ListBlobs(account, container string) ([]BlobInfo, error) {
	fs.mu.RLock()
	defer fs.mu.RUnlock()

	containerDir, err := fs.safeContainerDir(account, container)
	if err != nil {
		return nil, err
	}
	if _, err := os.Stat(filepath.Join(containerDir, "container.json")); os.IsNotExist(err) {
		return nil, ErrContainerNotFound
	}

	metaDir := filepath.Join(containerDir, "meta")
	if _, err := os.Stat(metaDir); os.IsNotExist(err) {
		return []BlobInfo{}, nil
	}

	var blobs []BlobInfo
	err = filepath.Walk(metaDir, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() || !strings.HasSuffix(path, ".json") {
			return nil
		}
		data, readErr := os.ReadFile(path)
		if readErr != nil {
			return nil
		}
		var bi BlobInfo
		if json.Unmarshal(data, &bi) == nil {
			blobs = append(blobs, bi)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return blobs, nil
}
