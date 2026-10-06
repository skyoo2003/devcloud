// SPDX-License-Identifier: Apache-2.0
package eventbridge

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"sort"
)

var errInvalidPageToken = errors.New("invalid pagination token")

type pageToken struct {
	Version int               `json:"v"`
	Kind    string            `json:"k"`
	Filters map[string]string `json:"f"`
	Last    string            `json:"l"`
}

func pageItems[T any](kind string, filters map[string]string, items []T, key func(T) string, limit int, token string) ([]T, string, error) {
	if limit == 0 {
		limit = 100
	}
	if limit < 1 || limit > 100 {
		return nil, "", errors.New("limit must be between 1 and 100")
	}
	cursor := ""
	if token != "" {
		raw, err := base64.RawURLEncoding.DecodeString(token)
		if err != nil {
			return nil, "", errInvalidPageToken
		}
		var decoded pageToken
		if json.Unmarshal(raw, &decoded) != nil || decoded.Version != 1 || decoded.Kind != kind || decoded.Last == "" {
			return nil, "", errInvalidPageToken
		}
		expected, _ := json.Marshal(filters)
		actual, _ := json.Marshal(decoded.Filters)
		if string(expected) != string(actual) {
			return nil, "", errInvalidPageToken
		}
		cursor = decoded.Last
	}
	sorted := append([]T{}, items...)
	sort.Slice(sorted, func(i, j int) bool { return key(sorted[i]) < key(sorted[j]) })
	start := sort.Search(len(sorted), func(i int) bool { return key(sorted[i]) > cursor })
	end := min(start+limit, len(sorted))
	result := sorted[start:end]
	next := ""
	if end < len(sorted) {
		raw, err := json.Marshal(pageToken{1, kind, filters, key(sorted[end-1])})
		if err != nil {
			return nil, "", err
		}
		next = base64.RawURLEncoding.EncodeToString(raw)
	}
	return result, next, nil
}
