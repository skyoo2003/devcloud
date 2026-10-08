// SPDX-License-Identifier: Apache-2.0
package s3

import (
	"errors"
	"github.com/skyoo2003/devcloud/internal/plugin"
)

type s3OperationError struct {
	Code, Message string
	StatusCode    int
}

func (e *s3OperationError) Error() string { return e.Code + ": " + e.Message }
func s3Error(code string, status int, message string) error {
	return &s3OperationError{Code: code, Message: message, StatusCode: status}
}
func responseForS3Error(err error) (*plugin.Response, error) {
	var api *s3OperationError
	if errors.As(err, &api) {
		return xmlError(api.Code, api.Message, api.StatusCode), nil
	}
	if errors.Is(err, ErrBucketNotFound) {
		return xmlError("NoSuchBucket", "bucket not found", 404), nil
	}
	return nil, err
}
