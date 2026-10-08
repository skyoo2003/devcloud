// SPDX-License-Identifier: Apache-2.0

package s3

import (
	"context"
	"io"
	"net/http"

	"github.com/skyoo2003/devcloud/internal/plugin"
)

func (p *S3Provider) writeGetObjectResponse(_ context.Context, req *http.Request) (*plugin.Response, error) {
	if req.Body != nil {
		_, _ = io.Copy(io.Discard, req.Body)
	}
	return &plugin.Response{
		StatusCode: http.StatusOK,
	}, nil
}
