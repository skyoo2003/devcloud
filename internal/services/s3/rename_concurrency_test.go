// SPDX-License-Identifier: Apache-2.0
package s3

import (
	"bytes"
	"context"
	"database/sql/driver"
	"encoding/xml"
	"fmt"
	"github.com/skyoo2003/devcloud/internal/plugin"
	"github.com/stretchr/testify/require"
	modernc "modernc.org/sqlite"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type renameOutcome struct {
	response *plugin.Response
	err      error
}

var renamePauseSequence atomic.Int64

func TestRenameConcurrentTokenConflictAndReplay(t *testing.T) {
	p, c := renameSetup(t)
	start := make(chan struct{})
	out := make(chan struct {
		destination string
		result      renameOutcome
	}, 12)
	for i := 0; i < 12; i++ {
		destination := "destination"
		if i%2 == 1 {
			destination = "other"
		}
		go func(dest string) {
			<-start
			r, err := p.HandleRequest(context.Background(), "", renameHTTP(directoryTestBucket, "source", dest, "concurrent", c))
			out <- struct {
				destination string
				result      renameOutcome
			}{dest, renameOutcome{r, err}}
		}(destination)
	}
	close(start)
	successes := map[string]int{}
	failures := 0
	for i := 0; i < 12; i++ {
		select {
		case value := <-out:
			require.NoError(t, value.result.err)
			if value.result.response.StatusCode == 200 {
				successes[value.destination]++
			} else {
				require.Equal(t, 400, value.result.response.StatusCode)
				require.Contains(t, string(value.result.response.Body), "IdempotencyParameterMismatch")
				failures++
			}
		case <-time.After(5 * time.Second):
			t.Fatal("concurrent rename stalled")
		}
	}
	require.Len(t, successes, 1)
	require.Equal(t, 6, failures)
	require.Equal(t, 1, renameReceiptCount(t, p))
	for destination, n := range successes {
		require.Equal(t, 6, n)
		require.Equal(t, []byte("original bytes"), directoryCall(t, p, "GET", "/"+directoryTestBucket+"/"+destination, nil, c).Body)
	}
}

func TestRenameConcurrentReadsAndMutationsSeeCommittedState(t *testing.T) {
	for _, operation := range []string{"get", "head", "list", "tag", "put", "delete", "batch-delete", "copy", "complete", "part-copy"} {
		t.Run(operation, func(t *testing.T) {
			entered, release := make(chan struct{}), make(chan struct{})
			var once sync.Once
			unblock := func() { once.Do(func() { close(release) }) }
			defer unblock()
			name := fmt.Sprintf("rename_pause_%d", renamePauseSequence.Add(1))
			require.NoError(t, modernc.RegisterScalarFunction(name, 0, func(_ *modernc.FunctionContext, _ []driver.Value) (driver.Value, error) {
				select {
				case <-entered:
				default:
					close(entered)
				}
				<-release
				return int64(1), nil
			}))
			p, c := renameSetup(t)
			upload := directoryCall(t, p, "POST", "/"+directoryTestBucket+"/complete-target?uploads", nil, c)
			var multipart struct {
				ID string `xml:"UploadId"`
			}
			require.NoError(t, xml.Unmarshal(upload.Body, &multipart))
			require.NotEmpty(t, multipart.ID)
			part := directoryCall(t, p, "PUT", "/"+directoryTestBucket+"/complete-target?uploadId="+multipart.ID+"&partNumber=1", []byte("completed"), c)
			require.Equal(t, 200, part.StatusCode)
			_, err := p.metaStore.store.DB().Exec(fmt.Sprintf(`CREATE TRIGGER pause_rename BEFORE INSERT ON objects WHEN NEW.key='destination' BEGIN SELECT %s(); SELECT RAISE(ABORT,'rename test rollback'); END`, name))
			require.NoError(t, err)
			renamed := make(chan renameOutcome, 1)
			go func() {
				r, err := p.HandleRequest(context.Background(), "", renameHTTP(directoryTestBucket, "source", "destination", "rollback", c))
				renamed <- renameOutcome{r, err}
			}()
			select {
			case <-entered:
			case <-time.After(5 * time.Second):
				t.Fatal("rename did not reach metadata write")
			}
			// At this point the real file is moved but the real SQLite writer is paused.
			staged, err := p.fileStore.GetObject(defaultAccountID, directoryTestBucket, "destination")
			require.NoError(t, err)
			require.Equal(t, []byte("original bytes"), staged)
			method, target, body := "GET", "/"+directoryTestBucket+"/destination", []byte(nil)
			headers := directoryAuth(c)
			switch operation {
			case "head":
				method = "HEAD"
			case "list":
				target = "/" + directoryTestBucket + "?list-type=2"
			case "tag":
				target += "?tagging"
			case "put":
				method = "PUT"
				target = "/" + directoryTestBucket + "/source"
				body = []byte("after")
			case "delete":
				method = "DELETE"
				target = "/" + directoryTestBucket + "/source"
			case "batch-delete":
				method = "POST"
				target = "/" + directoryTestBucket + "?delete"
				body = []byte(`<Delete><Object><Key>source</Key></Object></Delete>`)
			case "copy":
				method = "PUT"
				target = "/" + directoryTestBucket + "/copied"
				headers["X-Amz-Copy-Source"] = directoryTestBucket + "/source"
			case "complete":
				method = "POST"
				target = "/" + directoryTestBucket + "/complete-target?uploadId=" + multipart.ID
				body = []byte(fmt.Sprintf(`<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>%s</ETag></Part></CompleteMultipartUpload>`, part.Headers["ETag"]))
			case "part-copy":
				method = "PUT"
				target = "/" + directoryTestBucket + "/complete-target?uploadId=" + multipart.ID + "&partNumber=1"
				headers["X-Amz-Copy-Source"] = directoryTestBucket + "/destination"
			}
			request := httptest.NewRequest(method, target, bytes.NewReader(body))
			for k, v := range headers {
				request.Header.Set(k, v)
			}
			observed := make(chan renameOutcome, 1)
			go func() {
				r, err := p.HandleRequest(context.Background(), "", request)
				observed <- renameOutcome{r, err}
			}()
			var premature *renameOutcome
			select {
			case value := <-observed:
				premature = &value
				t.Errorf("%s responded while files and metadata disagreed", operation)
			case <-time.After(50 * time.Millisecond):
			}
			unblock()
			select {
			case result := <-renamed:
				require.Error(t, result.err)
			case <-time.After(5 * time.Second):
				t.Fatal("rename rollback stalled")
			}
			var actual renameOutcome
			if premature != nil {
				actual = *premature
			} else {
				select {
				case actual = <-observed:
				case <-time.After(5 * time.Second):
					t.Fatal("concurrent operation stalled")
				}
			}
			require.NoError(t, actual.err)
			require.Contains(t, []int{200, 204}, actual.response.StatusCode)
			switch operation {
			case "get":
				require.Equal(t, []byte("previous destination"), actual.response.Body)
			case "head":
				require.Equal(t, "20", actual.response.Headers["Content-Length"])
			case "tag":
				require.Contains(t, string(actual.response.Body), "replace")
			case "put":
				require.Equal(t, []byte("after"), directoryCall(t, p, "GET", "/"+directoryTestBucket+"/source", nil, c).Body)
			case "delete", "batch-delete":
				require.Equal(t, 404, directoryCall(t, p, "GET", "/"+directoryTestBucket+"/source", nil, c).StatusCode)
			case "copy":
				require.Equal(t, []byte("original bytes"), directoryCall(t, p, "GET", "/"+directoryTestBucket+"/copied", nil, c).Body)
			case "complete":
				require.Equal(t, []byte("completed"), directoryCall(t, p, "GET", "/"+directoryTestBucket+"/complete-target", nil, c).Body)
			case "part-copy":
				parts, err := p.metaStore.ListUploadParts(multipart.ID)
				require.NoError(t, err)
				require.Len(t, parts, 1)
				require.Equal(t, int64(20), parts[0].Size)
			}
			require.Zero(t, renameReceiptCount(t, p))
		})
	}
}

func TestDirectorySessionDeleteRecreateAndMultipartAdmission(t *testing.T) {
	p := directorySetup(t)
	c := directoryIssue(t, p, directoryTestBucket, "")
	created := directoryCall(t, p, "POST", "/"+directoryTestBucket+"/pending?uploads", nil, c)
	var upload struct {
		ID string `xml:"UploadId"`
	}
	require.NoError(t, xml.Unmarshal(created.Body, &upload))
	require.NotEmpty(t, upload.ID)
	require.Equal(t, 409, partCopyCall(t, p, "DELETE", "/"+directoryTestBucket, nil, nil).StatusCode)
	require.Equal(t, 204, directoryCall(t, p, "DELETE", "/"+directoryTestBucket+"/pending?uploadId="+upload.ID, nil, c).StatusCode)
	require.Equal(t, 204, partCopyCall(t, p, "DELETE", "/"+directoryTestBucket, nil, nil).StatusCode)
	require.Zero(t, renameReceiptCount(t, p))
	require.Equal(t, 200, partCopyCall(t, p, "PUT", "/"+directoryTestBucket, directoryConfig("use1-az1", "AvailabilityZone"), nil).StatusCode)
	require.Equal(t, 403, directoryCall(t, p, "GET", "/"+directoryTestBucket+"?list-type=2", nil, c).StatusCode)
}
func TestDirectoryReadOnlyOperationMatrix(t *testing.T) {
	p, c := renameSetup(t)
	ro := directoryIssue(t, p, directoryTestBucket, "ReadOnly")
	upload := directoryCall(t, p, "POST", "/"+directoryTestBucket+"/multipart?uploads", nil, c)
	var mp struct {
		ID string `xml:"UploadId"`
	}
	require.NoError(t, xml.Unmarshal(upload.Body, &mp))
	for _, tc := range []struct {
		method, target string
		status         int
	}{
		{"GET", "/source", 200}, {"HEAD", "/source", 200}, {"GET", "?list-type=2", 200}, {"GET", "?uploads", 200}, {"GET", "/multipart?uploadId=" + mp.ID, 200},
		{"PUT", "/source", 403}, {"DELETE", "/source", 403}, {"POST", "/new?uploads", 403}, {"GET", "/source?tagging", 403}, {"PUT", "/source?tagging", 403}, {"GET", "?analytics", 501},
	} {
		require.Equal(t, tc.status, directoryCall(t, p, tc.method, "/"+directoryTestBucket+tc.target, nil, ro).StatusCode)
	}
	require.Equal(t, 200, partCopyCall(t, p, "HEAD", "/"+directoryTestBucket, nil, nil).StatusCode)
	require.Equal(t, 200, partCopyCall(t, p, "PUT", "/"+directoryTestBucket+"/copied", nil, map[string]string{"X-Amz-Copy-Source": directoryTestBucket + "/source"}).StatusCode)
	require.Equal(t, 200, partCopyCall(t, p, "PUT", "/"+directoryTestBucket+"/multipart?uploadId="+mp.ID+"&partNumber=1", nil, map[string]string{"X-Amz-Copy-Source": directoryTestBucket + "/source"}).StatusCode)
}
func TestDirectoryHeadStorageClassAndGeneralBucketCompatibility(t *testing.T) {
	p, c := renameSetup(t)
	require.Equal(t, "EXPRESS_ONEZONE", directoryCall(t, p, "HEAD", "/"+directoryTestBucket+"/source", nil, c).Headers["x-amz-storage-class"])
	require.Equal(t, 200, partCopyCall(t, p, "PUT", "/general", nil, nil).StatusCode)
	require.Equal(t, 200, partCopyCall(t, p, "PUT", "/general/key", []byte("data"), nil).StatusCode)
	require.Empty(t, partCopyCall(t, p, "HEAD", "/general/key", nil, nil).Headers["x-amz-storage-class"])
}
