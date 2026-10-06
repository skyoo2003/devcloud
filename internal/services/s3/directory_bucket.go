// SPDX-License-Identifier: Apache-2.0
package s3

import (
	"context"
	"crypto/rand"
	"encoding/xml"
	"errors"
	"fmt"
	"github.com/skyoo2003/devcloud/internal/plugin"
	"io"
	"io/fs"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"
)

type directoryBucketConfig struct{ Zone, Region string }

func parseDirectoryBucketConfig(bucket string, req *http.Request) (*directoryBucketConfig, error) {
	if req.Body == nil {
		return nil, nil
	}
	body, err := io.ReadAll(io.LimitReader(req.Body, 1024*1024+1))
	if err != nil {
		return nil, err
	}
	if len(body) == 0 {
		return nil, nil
	}
	if len(body) > 1024*1024 {
		return nil, s3Error("InvalidRequest", 400, "bucket configuration too large")
	}
	var config struct {
		XMLName  xml.Name `xml:"CreateBucketConfiguration"`
		Location struct{ Type, Name string }
		Bucket   struct{ Type, DataRedundancy string }
	}
	if err = xml.Unmarshal(body, &config); err != nil {
		return nil, s3Error("MalformedXML", 400, "malformed bucket configuration")
	}
	if config.Location.Type == "LocalZone" || config.Bucket.DataRedundancy == "SingleLocalZone" {
		return nil, s3Error("NotImplemented", 501, "LocalZone buckets are not implemented")
	}
	if config.Bucket.Type == "" && config.Bucket.DataRedundancy == "" && config.Location.Type == "" && config.Location.Name == "" {
		return nil, nil
	}
	if config.Bucket.Type != "Directory" || config.Bucket.DataRedundancy != "SingleAvailabilityZone" || config.Location.Type != "AvailabilityZone" || config.Location.Name == "" {
		return nil, s3Error("InvalidRequest", 400, "invalid directory bucket configuration")
	}
	if len(bucket) < 3 || len(bucket) > 63 || !regexp.MustCompile(`^[a-z0-9][a-z0-9-]*[a-z0-9]$`).MatchString(bucket) || !strings.HasSuffix(bucket, "--"+config.Location.Name+"--x-s3") {
		return nil, s3Error("InvalidRequest", 400, "directory bucket name and zone must match")
	}
	return &directoryBucketConfig{Zone: config.Location.Name, Region: "us-east-1"}, nil
}
func randomDirectoryIncarnation() (string, error) {
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		return "", err
	}
	id[6] = (id[6] & 0x0f) | 0x40
	id[8] = (id[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", id[0:4], id[4:6], id[6:8], id[8:10], id[10:16]), nil
}

func (p *S3Provider) createDirectoryBucket(bucket string, config directoryBucketConfig) (*plugin.Response, error) {
	id, err := randomDirectoryIncarnation()
	if err != nil {
		return nil, err
	}
	dir, err := p.fileStore.bucketDir(defaultAccountID, bucket)
	if err != nil {
		return nil, err
	}
	_, statErr := os.Stat(dir)
	owned := errors.Is(statErr, os.ErrNotExist)
	if statErr != nil && !owned {
		return nil, statErr
	}
	if err = p.fileStore.CreateBucketDir(defaultAccountID, bucket); err != nil {
		return nil, err
	}
	info := DirectoryBucketInfo{BucketInfo: BucketInfo{Name: bucket, AccountID: defaultAccountID, Region: config.Region, CreatedAt: time.Now()}, Zone: config.Zone, Incarnation: id}
	if err = p.metaStore.CreateDirectoryBucket(info); err != nil {
		if owned {
			err = errors.Join(err, os.Remove(dir))
		}
		return responseForS3Error(err)
	}
	return &plugin.Response{StatusCode: 200}, nil
}
func (p *S3Provider) deleteDirectoryBucket(_ context.Context, bucket string) (*plugin.Response, error) {
	objects, err := p.metaStore.ListObjects(bucket, "", defaultAccountID, 1)
	if err != nil {
		return nil, err
	}
	uploads, err := p.metaStore.ListMultipartUploads(bucket, defaultAccountID)
	if err != nil {
		return nil, err
	}
	if len(objects) > 0 || len(uploads) > 0 {
		return responseForS3Error(s3Error("BucketNotEmpty", 409, "directory bucket is not empty"))
	}
	dir, err := p.fileStore.bucketDir(defaultAccountID, bucket)
	if err != nil {
		return nil, err
	}
	if err = filepath.WalkDir(dir, func(_ string, e fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if !e.IsDir() {
			return s3Error("BucketNotEmpty", 409, "directory bucket contains files")
		}
		return nil
	}); err != nil && !errors.Is(err, os.ErrNotExist) {
		return responseForS3Error(err)
	}
	if err = os.RemoveAll(dir); err != nil {
		return nil, err
	}
	if err = p.metaStore.DeleteDirectoryBucket(bucket, defaultAccountID); err != nil {
		return responseForS3Error(errors.Join(err, os.MkdirAll(dir, 0755)))
	}
	return &plugin.Response{StatusCode: 204}, nil
}
