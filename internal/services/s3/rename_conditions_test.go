// SPDX-License-Identifier: Apache-2.0
package s3

import (
	"github.com/stretchr/testify/require"
	"net/http"
	"testing"
	"time"
)

func TestRenameConditionsPresenceETagDateAndPrecedence(t *testing.T) {
	modified := time.Date(2026, 10, 7, 1, 0, 0, 900000000, time.UTC)
	source := ObjectMeta{ETag: "src", LastModified: modified}
	dest := ObjectMeta{ETag: "dst", LastModified: modified}
	before := "Wed, 07 Oct 2026 00:59:59 GMT"
	same := "Wed, 07 Oct 2026 01:00:00 GMT"
	after := "Wed, 07 Oct 2026 01:00:01 GMT"
	for _, tc := range []struct {
		name         string
		headers      map[string]string
		noDest, fail bool
	}{
		{name: "no-conditions"},
		{name: "source-match", headers: map[string]string{"X-Amz-Rename-Source-If-Match": `"src"`}},
		{name: "source-list", headers: map[string]string{"X-Amz-Rename-Source-If-Match": `"other", "src"`}},
		{name: "source-match-star", headers: map[string]string{"X-Amz-Rename-Source-If-Match": "*"}},
		{name: "source-mismatch", headers: map[string]string{"X-Amz-Rename-Source-If-Match": "bad"}, fail: true},
		{name: "source-none-equal", headers: map[string]string{"X-Amz-Rename-Source-If-None-Match": "src"}, fail: true},
		{name: "source-none-star", headers: map[string]string{"X-Amz-Rename-Source-If-None-Match": "*"}, fail: true},
		{name: "source-none-other", headers: map[string]string{"X-Amz-Rename-Source-If-None-Match": "other"}},
		{name: "source-mod-before", headers: map[string]string{"X-Amz-Rename-Source-If-Modified-Since": before}},
		{name: "source-mod-equal", headers: map[string]string{"X-Amz-Rename-Source-If-Modified-Since": same}, fail: true},
		{name: "source-unmod-before", headers: map[string]string{"X-Amz-Rename-Source-If-Unmodified-Since": before}, fail: true},
		{name: "source-unmod-equal", headers: map[string]string{"X-Amz-Rename-Source-If-Unmodified-Since": same}},
		{name: "dest-match", headers: map[string]string{"If-Match": "dst"}},
		{name: "dest-match-absent", headers: map[string]string{"If-Match": "*"}, noDest: true, fail: true},
		{name: "dest-none-absent", headers: map[string]string{"If-None-Match": "*"}, noDest: true},
		{name: "dest-none-present", headers: map[string]string{"If-None-Match": "*"}, fail: true},
		{name: "dest-none-tag", headers: map[string]string{"If-None-Match": `"dst"`}, fail: true},
		{name: "dest-mod-before", headers: map[string]string{"If-Modified-Since": before}},
		{name: "dest-mod-after", headers: map[string]string{"If-Modified-Since": after}, fail: true},
		{name: "dest-mod-absent", headers: map[string]string{"If-Modified-Since": before}, noDest: true, fail: true},
		{name: "dest-unmod-after", headers: map[string]string{"If-Unmodified-Since": after}},
		{name: "dest-unmod-before", headers: map[string]string{"If-Unmodified-Since": before}, fail: true},
		{name: "dest-unmod-absent", headers: map[string]string{"If-Unmodified-Since": before}, noDest: true},
		{name: "match-precedence", headers: map[string]string{"X-Amz-Rename-Source-If-Match": "src", "X-Amz-Rename-Source-If-Unmodified-Since": before}},
		{name: "none-precedence", headers: map[string]string{"If-None-Match": "dst", "If-Modified-Since": before}, fail: true},
		{name: "independent-objects", headers: map[string]string{"X-Amz-Rename-Source-If-Match": "src", "If-Unmodified-Since": before}, fail: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := http.Header{}
			for k, v := range tc.headers {
				h.Set(k, v)
			}
			conditions, err := parseRenameConditions(h)
			require.NoError(t, err)
			target := &dest
			if tc.noDest {
				target = nil
			}
			err = checkRenameConditions(conditions, source, target)
			if tc.fail {
				require.Equal(t, "PreconditionFailed", renameErrorCode(t, err))
			} else {
				require.NoError(t, err)
			}
		})
	}
	for _, tc := range []struct{ key, value string }{{"If-Match", `"unterminated`}, {"If-Match", `W/"weak"`}, {"If-Match", "*,tag"}, {"If-Match", "a,,b"}, {"If-Match", ""}, {"If-Modified-Since", "wrong"}, {"X-Amz-Rename-Source-If-Unmodified-Since", "bad"}} {
		h := http.Header{}
		h.Set(tc.key, tc.value)
		_, err := parseRenameConditions(h)
		require.Equal(t, "InvalidArgument", renameErrorCode(t, err))
	}
}
