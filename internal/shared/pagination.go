// SPDX-License-Identifier: Apache-2.0

package shared

import (
	"encoding/base64"
	"strconv"
)

type Page[T any] struct {
	Items     []T
	NextToken string
}

func Paginate[T any](items []T, token string, maxResults int) Page[T] {
	if len(items) == 0 {
		return Page[T]{}
	}
	if maxResults <= 0 {
		return Page[T]{Items: items}
	}
	offset := 0
	if token != "" {
		if decoded, err := base64.StdEncoding.DecodeString(token); err == nil {
			if n, err := strconv.Atoi(string(decoded)); err == nil && n > 0 && n < len(items) {
				offset = n
			}
		}
	}
	end := offset + maxResults
	if end >= len(items) {
		return Page[T]{Items: items[offset:]}
	}
	nextToken := base64.StdEncoding.EncodeToString([]byte(strconv.Itoa(end)))
	return Page[T]{Items: items[offset:end], NextToken: nextToken}
}
