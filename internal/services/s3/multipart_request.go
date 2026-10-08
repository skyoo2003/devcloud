// SPDX-License-Identifier: Apache-2.0

package s3

// Caller holds objectMu. An upload must belong to the same request identity
// whose directory session was admitted, before parts are read or changed.
func (p *S3Provider) multipartUploadForRequest(bucket, key, uploadID string) (*MultipartUploadInfo, error) {
	upload, err := p.metaStore.GetMultipartUpload(uploadID)
	if err != nil {
		return nil, err
	}
	if upload.Bucket != bucket || upload.Key != key || upload.AccountID != defaultAccountID {
		return nil, ErrUploadNotFound
	}
	return upload, nil
}
