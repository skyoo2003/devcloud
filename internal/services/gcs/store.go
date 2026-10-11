// SPDX-License-Identifier: Apache-2.0

package gcs

import (
	"crypto/md5"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

var (
	ErrBucketNotFound      = errors.New("notFound: bucket not found")
	ErrBucketAlreadyExists = errors.New("conflict: bucket already exists")
	ErrBucketNotEmpty      = errors.New("conflict: bucket is not empty")
	ErrObjectNotFound      = errors.New("notFound: object not found")
	ErrConditionNotMet     = errors.New("conditionNotMet: precondition failed")
	ErrInvalidName         = errors.New("invalid: invalid bucket or object name")
)

type BucketInfo struct {
	Kind          string    `json:"kind"`
	ID            string    `json:"id"`
	Name          string    `json:"name"`
	ProjectNumber string    `json:"projectNumber,omitempty"`
	Location      string    `json:"location"`
	StorageClass  string    `json:"storageClass"`
	TimeCreated   time.Time `json:"timeCreated"`
	Updated       time.Time `json:"updated"`
	ETag          string    `json:"etag"`
}

type ObjectInfo struct {
	Kind        string            `json:"kind"`
	ID          string            `json:"id"`
	Name        string            `json:"name"`
	Bucket      string            `json:"bucket"`
	Size        int64             `json:"size,string"`
	ContentType string            `json:"contentType"`
	MD5Hash     string            `json:"md5Hash"`
	CRC32c      string            `json:"crc32c"`
	ETag        string            `json:"etag"`
	TimeCreated time.Time         `json:"timeCreated"`
	Updated     time.Time         `json:"updated"`
	Metadata    map[string]string `json:"metadata,omitempty"`
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

func (fs *FileStore) safeBucketDir(project, bucket string) (string, error) {
	if project == "" {
		project = "default"
	}
	if bucket == "" || bucket == "." || strings.Contains(bucket, "..") || strings.Contains(bucket, "/") || strings.Contains(bucket, "\\") {
		return "", ErrInvalidName
	}
	if !filepath.IsLocal(bucket) || !filepath.IsLocal(project) {
		return "", ErrInvalidName
	}

	dir := filepath.Clean(filepath.Join(fs.baseDir, project, bucket))
	basePrefix := fs.baseDir + string(filepath.Separator)
	if !strings.HasPrefix(dir, basePrefix) {
		return "", ErrInvalidName
	}
	rel, err := filepath.Rel(fs.baseDir, dir)
	if err != nil || !filepath.IsLocal(rel) || rel == "." {
		return "", ErrInvalidName
	}
	return dir, nil
}

func (fs *FileStore) safeObjectPaths(project, bucket, object string) (dataPath, metaPath string, err error) {
	bucketDir, err := fs.safeBucketDir(project, bucket)
	if err != nil {
		return "", "", err
	}
	if object == "" || strings.Contains(object, "..") || strings.Contains(object, "\\") || strings.HasPrefix(object, "/") {
		return "", "", ErrInvalidName
	}
	if !filepath.IsLocal(object) || object == "." {
		return "", "", ErrInvalidName
	}

	cleanObject := filepath.Clean(object)
	if strings.Contains(cleanObject, "..") || strings.HasPrefix(cleanObject, "/") || cleanObject == "." {
		return "", "", ErrInvalidName
	}
	if !filepath.IsLocal(cleanObject) {
		return "", "", ErrInvalidName
	}

	dataDir := filepath.Clean(filepath.Join(bucketDir, "data"))
	dataPath = filepath.Clean(filepath.Join(dataDir, cleanObject))
	dataPrefix := dataDir + string(filepath.Separator)
	if !strings.HasPrefix(dataPath, dataPrefix) {
		return "", "", ErrInvalidName
	}
	relData, err := filepath.Rel(dataDir, dataPath)
	if err != nil || !filepath.IsLocal(relData) || relData == "." {
		return "", "", ErrInvalidName
	}

	metaDir := filepath.Clean(filepath.Join(bucketDir, "meta"))
	metaPath = filepath.Clean(filepath.Join(metaDir, cleanObject+".json"))
	metaPrefix := metaDir + string(filepath.Separator)
	if !strings.HasPrefix(metaPath, metaPrefix) {
		return "", "", ErrInvalidName
	}
	relMeta, err := filepath.Rel(metaDir, metaPath)
	if err != nil || !filepath.IsLocal(relMeta) || relMeta == "." {
		return "", "", ErrInvalidName
	}

	return dataPath, metaPath, nil
}

func (fs *FileStore) CreateBucket(project, bucket, location, storageClass string) (*BucketInfo, error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	dir, err := fs.safeBucketDir(project, bucket)
	if err != nil {
		return nil, err
	}

	metaFile := filepath.Join(dir, "bucket.json")
	if _, err := os.Stat(metaFile); err == nil {
		return nil, ErrBucketAlreadyExists
	}

	if err := os.MkdirAll(filepath.Join(dir, "data"), 0755); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Join(dir, "meta"), 0755); err != nil {
		return nil, err
	}

	if location == "" {
		location = "US"
	}
	if storageClass == "" {
		storageClass = "STANDARD"
	}
	now := time.Now().UTC()
	info := &BucketInfo{
		Kind:         "storage#bucket",
		ID:           bucket,
		Name:         bucket,
		Location:     location,
		StorageClass: storageClass,
		TimeCreated:  now,
		Updated:      now,
		ETag:         fmt.Sprintf("\"%d\"", now.UnixNano()),
	}

	raw, err := json.MarshalIndent(info, "", "  ")
	if err != nil {
		return nil, err
	}
	if err := os.WriteFile(metaFile, raw, 0644); err != nil {
		return nil, err
	}
	return info, nil
}

func (fs *FileStore) GetBucket(project, bucket string) (*BucketInfo, error) {
	fs.mu.RLock()
	defer fs.mu.RUnlock()

	dir, err := fs.safeBucketDir(project, bucket)
	if err != nil {
		return nil, err
	}

	metaFile := filepath.Join(dir, "bucket.json")
	raw, err := os.ReadFile(metaFile)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, ErrBucketNotFound
		}
		return nil, err
	}

	var info BucketInfo
	if err := json.Unmarshal(raw, &info); err != nil {
		return nil, err
	}
	return &info, nil
}

func (fs *FileStore) DeleteBucket(project, bucket string) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	dir, err := fs.safeBucketDir(project, bucket)
	if err != nil {
		return err
	}

	metaFile := filepath.Join(dir, "bucket.json")
	if _, err := os.Stat(metaFile); os.IsNotExist(err) {
		return ErrBucketNotFound
	}

	// Check if bucket has objects
	dataDir := filepath.Join(dir, "data")
	entries, err := os.ReadDir(dataDir)
	if err == nil && len(entries) > 0 {
		return ErrBucketNotEmpty
	}

	return os.RemoveAll(dir)
}

func (fs *FileStore) ListBuckets(project string) ([]BucketInfo, error) {
	fs.mu.RLock()
	defer fs.mu.RUnlock()

	if project == "" {
		project = "default"
	}
	projDir := filepath.Clean(filepath.Join(fs.baseDir, project))
	entries, err := os.ReadDir(projDir)
	if err != nil {
		if os.IsNotExist(err) {
			return []BucketInfo{}, nil
		}
		return nil, err
	}

	var list []BucketInfo
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		metaFile := filepath.Join(projDir, entry.Name(), "bucket.json")
		raw, err := os.ReadFile(metaFile)
		if err == nil {
			var info BucketInfo
			if json.Unmarshal(raw, &info) == nil {
				list = append(list, info)
			}
		}
	}
	sort.Slice(list, func(i, j int) bool {
		return list[i].Name < list[j].Name
	})
	return list, nil
}

func computeMD5Base64(data []byte) string {
	sum := md5.Sum(data)
	return base64.StdEncoding.EncodeToString(sum[:])
}

func computeCRC32cBase64(data []byte) string {
	table := crc32.MakeTable(crc32.Castagnoli)
	checksum := crc32.Checksum(data, table)
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, checksum)
	return base64.StdEncoding.EncodeToString(b)
}

func (fs *FileStore) PutObject(project, bucket, object, contentType string, data []byte, metadata map[string]string) (*ObjectInfo, error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	dataPath, metaPath, err := fs.safeObjectPaths(project, bucket, object)
	if err != nil {
		return nil, err
	}

	// Verify bucket exists
	bucketDir, err := fs.safeBucketDir(project, bucket)
	if err != nil {
		return nil, err
	}
	if _, err := os.Stat(filepath.Join(bucketDir, "bucket.json")); os.IsNotExist(err) {
		return nil, ErrBucketNotFound
	}

	if err := os.MkdirAll(filepath.Dir(dataPath), 0755); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Dir(metaPath), 0755); err != nil {
		return nil, err
	}

	if err := os.WriteFile(dataPath, data, 0644); err != nil {
		return nil, err
	}

	if contentType == "" {
		contentType = "application/octet-stream"
	}
	now := time.Now().UTC()
	info := &ObjectInfo{
		Kind:        "storage#object",
		ID:          fmt.Sprintf("%s/%s/%d", bucket, object, now.UnixNano()),
		Name:        object,
		Bucket:      bucket,
		Size:        int64(len(data)),
		ContentType: contentType,
		MD5Hash:     computeMD5Base64(data),
		CRC32c:      computeCRC32cBase64(data),
		ETag:        fmt.Sprintf("\"%d\"", now.UnixNano()),
		TimeCreated: now,
		Updated:     now,
		Metadata:    metadata,
	}

	raw, err := json.MarshalIndent(info, "", "  ")
	if err != nil {
		return nil, err
	}
	if err := os.WriteFile(metaPath, raw, 0644); err != nil {
		return nil, err
	}
	return info, nil
}

func (fs *FileStore) GetObject(project, bucket, object string) (*ObjectInfo, []byte, error) {
	fs.mu.RLock()
	defer fs.mu.RUnlock()

	dataPath, metaPath, err := fs.safeObjectPaths(project, bucket, object)
	if err != nil {
		return nil, nil, err
	}

	rawMeta, err := os.ReadFile(metaPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil, ErrObjectNotFound
		}
		return nil, nil, err
	}

	var info ObjectInfo
	if err := json.Unmarshal(rawMeta, &info); err != nil {
		return nil, nil, err
	}

	data, err := os.ReadFile(dataPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil, ErrObjectNotFound
		}
		return nil, nil, err
	}

	return &info, data, nil
}

func (fs *FileStore) GetObjectMetadata(project, bucket, object string) (*ObjectInfo, error) {
	fs.mu.RLock()
	defer fs.mu.RUnlock()

	_, metaPath, err := fs.safeObjectPaths(project, bucket, object)
	if err != nil {
		return nil, err
	}

	rawMeta, err := os.ReadFile(metaPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, ErrObjectNotFound
		}
		return nil, err
	}

	var info ObjectInfo
	if err := json.Unmarshal(rawMeta, &info); err != nil {
		return nil, err
	}
	return &info, nil
}

func (fs *FileStore) DeleteObject(project, bucket, object string) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	dataPath, metaPath, err := fs.safeObjectPaths(project, bucket, object)
	if err != nil {
		return err
	}

	if _, err := os.Stat(metaPath); os.IsNotExist(err) {
		return ErrObjectNotFound
	}

	_ = os.Remove(dataPath)
	_ = os.Remove(metaPath)
	return nil
}

func (fs *FileStore) ListObjects(project, bucket, prefix string) ([]ObjectInfo, error) {
	fs.mu.RLock()
	defer fs.mu.RUnlock()

	bucketDir, err := fs.safeBucketDir(project, bucket)
	if err != nil {
		return nil, err
	}

	if _, err := os.Stat(filepath.Join(bucketDir, "bucket.json")); os.IsNotExist(err) {
		return nil, ErrBucketNotFound
	}

	metaDir := filepath.Join(bucketDir, "meta")
	var objects []ObjectInfo

	_ = filepath.Walk(metaDir, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() || !strings.HasSuffix(info.Name(), ".json") {
			return nil
		}
		raw, rerr := os.ReadFile(path)
		if rerr == nil {
			var obj ObjectInfo
			if json.Unmarshal(raw, &obj) == nil {
				if prefix == "" || strings.HasPrefix(obj.Name, prefix) {
					objects = append(objects, obj)
				}
			}
		}
		return nil
	})

	sort.Slice(objects, func(i, j int) bool {
		return objects[i].Name < objects[j].Name
	})
	return objects, nil
}
