// SPDX-License-Identifier: Apache-2.0
package s3

import (
	"net/http"
	"slices"
	"strings"
	"time"
)

type renameETagCondition struct {
	Present, Wildcard bool
	Tags              []string
}
type renameDateCondition struct {
	Present bool
	Time    time.Time
}
type renameObjectConditions struct {
	IfMatch, IfNoneMatch               renameETagCondition
	IfModifiedSince, IfUnmodifiedSince renameDateCondition
}
type renameConditions struct{ Source, Destination renameObjectConditions }

func parseRenameConditions(headers http.Header) (renameConditions, error) {
	var value renameConditions
	for _, part := range []struct {
		prefix string
		out    *renameObjectConditions
	}{{"X-Amz-Rename-Source-", &value.Source}, {"", &value.Destination}} {
		for _, etag := range []struct {
			name string
			out  *renameETagCondition
		}{{"If-Match", &part.out.IfMatch}, {"If-None-Match", &part.out.IfNoneMatch}} {
			entries, present := headers[http.CanonicalHeaderKey(part.prefix+etag.name)]
			if !present {
				continue
			}
			if len(entries) != 1 {
				return value, s3Error("InvalidArgument", 400, "invalid ETag condition")
			}
			parsed, err := parseRenameETags(entries[0])
			if err != nil {
				return value, err
			}
			*etag.out = parsed
		}
		for _, date := range []struct {
			name string
			out  *renameDateCondition
		}{{"If-Modified-Since", &part.out.IfModifiedSince}, {"If-Unmodified-Since", &part.out.IfUnmodifiedSince}} {
			entries, present := headers[http.CanonicalHeaderKey(part.prefix+date.name)]
			if !present {
				continue
			}
			if len(entries) != 1 {
				return value, s3Error("InvalidArgument", 400, "invalid date condition")
			}
			parsed, err := http.ParseTime(entries[0])
			if err != nil {
				return value, s3Error("InvalidArgument", 400, "invalid HTTP date")
			}
			*date.out = renameDateCondition{Present: true, Time: parsed.UTC().Truncate(time.Second)}
		}
	}
	return value, nil
}

func parseRenameETags(raw string) (renameETagCondition, error) {
	value := renameETagCondition{Present: true}
	rest := strings.TrimSpace(raw)
	invalid := func() (renameETagCondition, error) {
		return value, s3Error("InvalidArgument", 400, "invalid ETag condition")
	}
	if rest == "*" {
		value.Wildcard = true
		return value, nil
	}
	if rest == "" {
		return invalid()
	}
	for {
		var tag string
		if strings.HasPrefix(rest, `"`) {
			end := strings.IndexByte(rest[1:], '"')
			if end < 0 {
				return invalid()
			}
			tag = rest[1 : end+1]
			rest = strings.TrimSpace(rest[end+2:])
		} else {
			tag, rest, _ = strings.Cut(rest, ",")
			tag = strings.TrimSpace(tag)
			rest = strings.TrimSpace(rest)
			if strings.HasPrefix(tag, "W/") || strings.ContainsAny(tag, `"*`) {
				return invalid()
			}
			if rest != "" {
				rest = "," + rest
			} else if strings.HasSuffix(strings.TrimSpace(raw), ",") {
				return invalid()
			}
		}
		if tag == "" {
			return invalid()
		}
		for _, b := range []byte(tag) {
			if b < 33 || b == 127 {
				return invalid()
			}
		}
		value.Tags = append(value.Tags, tag)
		if rest == "" {
			break
		}
		if !strings.HasPrefix(rest, ",") {
			return invalid()
		}
		rest = strings.TrimSpace(rest[1:])
		if rest == "" {
			return invalid()
		}
	}
	slices.Sort(value.Tags)
	value.Tags = slices.Compact(value.Tags)
	return value, nil
}

func checkRenameConditions(value renameConditions, source ObjectMeta, destination *ObjectMeta) error {
	for _, part := range []struct {
		condition renameObjectConditions
		meta      *ObjectMeta
	}{{value.Source, &source}, {value.Destination, destination}} {
		c, m := part.condition, part.meta
		matches := func(e renameETagCondition) bool {
			if m == nil {
				return false
			}
			return e.Wildcard || slices.Contains(e.Tags, strings.Trim(m.ETag, `"`))
		}
		failed := c.IfMatch.Present && !matches(c.IfMatch) || c.IfNoneMatch.Present && matches(c.IfNoneMatch)
		if !c.IfNoneMatch.Present && c.IfModifiedSince.Present && (m == nil || !m.LastModified.Truncate(time.Second).After(c.IfModifiedSince.Time)) {
			failed = true
		}
		if !c.IfMatch.Present && c.IfUnmodifiedSince.Present && m != nil && m.LastModified.Truncate(time.Second).After(c.IfUnmodifiedSince.Time) {
			failed = true
		}
		if failed {
			return s3Error("PreconditionFailed", 412, "rename condition failed")
		}
	}
	return nil
}
