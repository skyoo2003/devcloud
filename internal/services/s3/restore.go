// SPDX-License-Identifier: Apache-2.0

package s3

import (
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/skyoo2003/devcloud/internal/plugin"
)

type restoreRequestXML struct {
	XMLName              xml.Name              `xml:"RestoreRequest"`
	Days                 int                   `xml:"Days"`
	Tier                 string                `xml:"Tier"`
	GlacierJobParameters *glacierJobParameters `xml:"GlacierJobParameters"`
}

type glacierJobParameters struct {
	Tier string `xml:"Tier"`
}

func (p *S3Provider) restoreObject(_ context.Context, bucket, key string, req *http.Request) (*plugin.Response, error) {
	if bucket == "" || key == "" {
		return xmlError("InvalidRequest", "bucket and key required", http.StatusBadRequest), nil
	}

	// Verify bucket existence
	buckets, err := p.metaStore.ListBuckets(defaultAccountID)
	if err != nil {
		return nil, err
	}
	bucketFound := false
	for _, b := range buckets {
		if b.Name == bucket {
			bucketFound = true
			break
		}
	}
	if !bucketFound {
		return xmlError("NoSuchBucket", fmt.Sprintf("bucket %q not found", bucket), http.StatusNotFound), nil
	}

	// Verify object existence
	_, err = p.metaStore.GetObjectMeta(bucket, key, defaultAccountID)
	if err != nil {
		if errors.Is(err, ErrObjectNotFound) {
			return xmlError("NoSuchKey", fmt.Sprintf("key %q not found", key), http.StatusNotFound), nil
		}
		return nil, err
	}

	days := 1
	if req.Body != nil {
		body, err := io.ReadAll(req.Body)
		if err == nil && len(body) > 0 {
			var parsed restoreRequestXML
			if err := xml.Unmarshal(body, &parsed); err == nil && parsed.Days > 0 {
				days = parsed.Days
			}
		}
	}

	expiresAt := time.Now().Add(time.Duration(days) * 24 * time.Hour)
	if err := p.metaStore.SetObjectRestore(bucket, key, defaultAccountID, `ongoing-request="false"`, expiresAt); err != nil {
		return nil, err
	}

	return &plugin.Response{
		StatusCode: http.StatusOK,
	}, nil
}
