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
	_ = os.MkdirAll(abs, 0755)
	return &FileStore{baseDir: abs}
}

func (fs *FileStore) safeContainerDir(account, container string) (string, error) {
	if account == "" || container == "" {
		return "", ErrInvalidName
	}
	if strings.Contains(account, "..") || strings.Contains(container, "..") ||
		strings.ContainsAny(account, "/\\") || strings.ContainsAny(container, "/\\") {
		return "", ErrInvalidName
	}
	dir := filepath.Join(fs.baseDir, account, container)
	rel, err := filepath.Rel(fs.baseDir, dir)
	if err != nil || strings.HasPrefix(rel, "..") {
		return "", ErrInvalidName
	}
	return dir, nil
}

func (fs *FileStore) safeBlobPaths(account, container, blob string) (dataPath, metaPath string, err error) {
	containerDir, err := fs.safeContainerDir(account, container)
	if err != nil {
		return "", "", err
	}
	if blob == "" || strings.Contains(blob, "\\") {
		return "", "", ErrInvalidName
	}
	// Blobs can have '/' in their name (virtual directories)
	cleanBlob := filepath.Clean(blob)
	if strings.HasPrefix(cleanBlob, "..") || cleanBlob == "." {
		return "", "", ErrInvalidName
	}

	dataPath = filepath.Join(containerDir, "data", cleanBlob)
	metaPath = filepath.Join(containerDir, "meta", cleanBlob+".json")

	relData, err := filepath.Rel(containerDir, dataPath)
	if err != nil || strings.HasPrefix(relData, "..") {
		return "", "", ErrInvalidName
	}
	return dataPath, metaPath, nil
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

	accountDir := filepath.Join(fs.baseDir, account)
	entries, err := os.ReadDir(accountDir)
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
		metaFile := filepath.Join(accountDir, entry.Name(), "container.json")
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
	tmpFile := dataPath + ".tmp"
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
