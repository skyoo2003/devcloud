// SPDX-License-Identifier: Apache-2.0

package shared

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestPaginate_FirstPage(t *testing.T) {
	items := []string{"a", "b", "c", "d", "e"}
	page := Paginate(items, "", 2)
	assert.Equal(t, []string{"a", "b"}, page.Items)
	assert.NotEmpty(t, page.NextToken)
}

func TestPaginate_MiddlePage(t *testing.T) {
	items := []string{"a", "b", "c", "d", "e"}
	first := Paginate(items, "", 2)
	second := Paginate(items, first.NextToken, 2)
	assert.Equal(t, []string{"c", "d"}, second.Items)
	assert.NotEmpty(t, second.NextToken)
}

func TestPaginate_LastPage(t *testing.T) {
	items := []string{"a", "b", "c", "d", "e"}
	first := Paginate(items, "", 2)
	second := Paginate(items, first.NextToken, 2)
	third := Paginate(items, second.NextToken, 2)
	assert.Equal(t, []string{"e"}, third.Items)
	assert.Empty(t, third.NextToken)
}

func TestPaginate_NoToken_AllFit(t *testing.T) {
	items := []string{"a", "b"}
	page := Paginate(items, "", 10)
	assert.Equal(t, []string{"a", "b"}, page.Items)
	assert.Empty(t, page.NextToken)
}

func TestPaginate_EmptyItems(t *testing.T) {
	page := Paginate([]string{}, "", 10)
	assert.Empty(t, page.Items)
	assert.Empty(t, page.NextToken)
}

func TestPaginate_ZeroMaxResults(t *testing.T) {
	items := []string{"a", "b", "c"}
	page := Paginate(items, "", 0)
	assert.Equal(t, []string{"a", "b", "c"}, page.Items)
	assert.Empty(t, page.NextToken)
}

func TestPaginate_InvalidToken(t *testing.T) {
	items := []string{"a", "b", "c"}
	page := Paginate(items, "garbage", 2)
	assert.Equal(t, []string{"a", "b"}, page.Items)
	assert.NotEmpty(t, page.NextToken)
}
