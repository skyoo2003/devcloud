// SPDX-License-Identifier: Apache-2.0

package s3

import (
	"bytes"
	"encoding/xml"
	"fmt"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

// These requests used to be classified as exempt copies or as no data operation,
// while dispatch served a protected read or write.
func TestDirectoryAdmissionMatchesDispatchedOperation(t *testing.T) {
	for _, tc := range []struct {
		name, method, suffix, body string
		copyHeader                 bool
		status                     int
	}{
		{"explicit-copy-get", "GET", "/source?x-id=UploadPartCopy", "", false, 405},
		{"explicit-copy-delete", "DELETE", "/source?x-id=UploadPartCopy", "", false, 405},
		{"empty-copy-put", "PUT", "/source", "unauthorized", true, 403},
		{"empty-copy-empty-upload", "PUT", "/source?uploadId=", "unauthorized", false, 403},
		{"object-path-batch-delete", "POST", "/any?delete", "<Delete><Object><Key>source</Key></Object></Delete>", false, 403},
	} {
		for _, mode := range []string{"missing", "ReadOnly"} {
			t.Run(tc.name+"/"+mode, func(t *testing.T) {
				p, _ := renameSetup(t)
				before := snapshotDirectoryLegacy(t, p.metaStore.store.DB())
				r := httptest.NewRequest(tc.method, "/"+directoryTestBucket+tc.suffix, bytes.NewBufferString(tc.body))
				if tc.copyHeader {
					r.Header.Set("X-Amz-Copy-Source", "")
				}
				if mode != "missing" {
					for name, value := range directoryAuth(directoryIssue(t, p, directoryTestBucket, mode)) {
						r.Header.Set(name, value)
					}
				}
				response := renameRun(t, p, r)
				require.Equal(t, tc.status, response.StatusCode)
				require.Equal(t, before, snapshotDirectoryLegacy(t, p.metaStore.store.DB()))
				data, err := p.fileStore.GetObject(defaultAccountID, directoryTestBucket, "source")
				require.NoError(t, err)
				require.Equal(t, []byte("original bytes"), data)
			})
		}
	}
}

func TestDirectoryAdmissionRespectsMultipartAndHeadDispatch(t *testing.T) {
	p, rw := renameSetup(t)
	ro := directoryIssue(t, p, directoryTestBucket, "ReadOnly")
	// HEAD does not dispatch object tagging or multipart reads, even if selectors exist.
	for _, suffix := range []string{"/source?tagging", "/source?uploadId=ignored", "/source?uploadId="} {
		require.Equal(t, 200, directoryCall(t, p, "HEAD", "/"+directoryTestBucket+suffix, nil, ro).StatusCode)
	}
	// A present partNumber with no uploadId dispatches UploadPart, never PutObject.
	require.Equal(t, 403, directoryCall(t, p, "PUT", "/"+directoryTestBucket+"/source?partNumber=1", nil, ro).StatusCode)
	// Existing control handlers take precedence over listing selectors.
	require.Equal(t, 200, partCopyCall(t, p, "GET", "/"+directoryTestBucket+"?location&list-type=2", nil, nil).StatusCode)
	// Correctly admitted legacy object-path batch deletion still functions.
	require.Equal(t, 200, directoryCall(t, p, "POST", "/"+directoryTestBucket+"/any?delete", []byte("<Delete><Object><Key>source</Key></Object></Delete>"), rw).StatusCode)
}

func TestMultipartUploadBoundToAdmittedBucketAndKey(t *testing.T) {
	for _, operation := range []string{"put", "list", "abort", "complete"} {
		for _, route := range []string{"general", "other-directory", "wrong-key", "wrong-account"} {
			t.Run(operation+"/"+route, func(t *testing.T) {
				p, rw := renameSetup(t)
				response := directoryCall(t, p, "POST", "/"+directoryTestBucket+"/multipart?uploads", nil, rw)
				var created struct {
					ID string `xml:"UploadId"`
				}
				require.NoError(t, xml.Unmarshal(response.Body, &created))
				uploadID := created.ID
				part := directoryCall(t, p, "PUT", "/"+directoryTestBucket+"/multipart?uploadId="+uploadID+"&partNumber=1", []byte("keep"), rw)
				require.Equal(t, 200, part.StatusCode)
				bucket, key, auth := directoryTestBucket, "multipart", rw
				switch route {
				case "general":
					bucket, key, auth = "ordinary", "unrelated", testDirectoryCredentials{}
					require.Equal(t, 200, partCopyCall(t, p, "PUT", "/ordinary", nil, nil).StatusCode)
				case "other-directory":
					bucket = "other--use1-az1--x-s3"
					require.Equal(t, 200, partCopyCall(t, p, "PUT", "/"+bucket, directoryConfig("use1-az1", "AvailabilityZone"), nil).StatusCode)
					auth = directoryIssue(t, p, bucket, "ReadWrite")
				case "wrong-key":
					key = "unrelated"
				case "wrong-account":
					_, err := p.metaStore.store.DB().Exec("UPDATE multipart_uploads SET account_id='other' WHERE upload_id=?", uploadID)
					require.NoError(t, err)
				}
				before := snapshotDirectoryLegacy(t, p.metaStore.store.DB())
				method, query, body := "GET", "?uploadId="+uploadID, ""
				switch operation {
				case "put":
					method, query, body = "PUT", query+"&partNumber=1", "bad"
				case "abort":
					method = "DELETE"
				case "complete":
					method, body = "POST", fmt.Sprintf("<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>%s</ETag></Part></CompleteMultipartUpload>", part.Headers["ETag"])
				}
				r := httptest.NewRequest(method, "/"+bucket+"/"+key+query, bytes.NewBufferString(body))
				if auth.AccessKeyID != "" {
					for name, value := range directoryAuth(auth) {
						r.Header.Set(name, value)
					}
				}
				require.Equal(t, 404, renameRun(t, p, r).StatusCode)
				require.Equal(t, before, snapshotDirectoryLegacy(t, p.metaStore.store.DB()))
				path, err := p.partPath(uploadID, 1)
				require.NoError(t, err)
				saved, err := os.ReadFile(path)
				require.NoError(t, err)
				require.Equal(t, []byte("keep"), saved)
				if route == "wrong-account" {
					_, err := p.metaStore.store.DB().Exec("UPDATE multipart_uploads SET account_id=? WHERE upload_id=?", defaultAccountID, uploadID)
					require.NoError(t, err)
				}
				require.Equal(t, 200, directoryCall(t, p, "GET", "/"+directoryTestBucket+"/multipart?uploadId="+uploadID, nil, rw).StatusCode)
				require.Equal(t, 204, directoryCall(t, p, "DELETE", "/"+directoryTestBucket+"/multipart?uploadId="+uploadID, nil, rw).StatusCode)
			})
		}
	}
}
