// SPDX-License-Identifier: Apache-2.0
package s3

import (
	"context"
	"github.com/skyoo2003/devcloud/internal/plugin"
	"net/http"
	"time"
)

func (p *S3Provider) renameObject(ctx context.Context, bucket, key string, req *http.Request) (*plugin.Response, error) {
	if bucket == "" {
		return responseForS3Error(s3Error("InvalidRequest", 400, "bucket required"))
	}
	value, err := parseRenameRequest(bucket, key, req)
	if err != nil {
		return responseForS3Error(err)
	}
	info, err := p.metaStore.GetDirectoryBucket(bucket, defaultAccountID)
	if err != nil {
		return responseForS3Error(err)
	}
	if info == nil {
		return responseForS3Error(s3Error("InvalidRequest", 400, "rename requires a directory bucket"))
	}
	now := time.Now()
	if err = p.authorizeDirectoryOperation(info, "RenameObject", req, now); err != nil {
		return responseForS3Error(err)
	}
	if err = p.renameStoredObject(ctx, *info, value, now); err != nil {
		return responseForS3Error(err)
	}
	return &plugin.Response{StatusCode: 200}, nil
}
