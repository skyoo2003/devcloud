// SPDX-License-Identifier: Apache-2.0
package sns

import (
	"github.com/stretchr/testify/require"
	"net/url"
	"testing"
)

func TestQueryNestedMapsAndEntryIndexes(t *testing.T) {
	f := url.Values{"Entries.member.1.Id": {"a"}, "Entries.member.1.Attributes.entry.1.key": {"Enabled"}, "Entries.member.1.Attributes.entry.1.value": {"false"}, "Entries.member.2.Id": {"b"}}
	entries, e := queryEntries(f, "Entries", 10)
	require.NoError(t, e)
	require.Len(t, entries, 2)
	m, e := queryStringMap(entries[0], "Attributes")
	require.NoError(t, e)
	require.Equal(t, map[string]string{"Enabled": "false"}, m)
	for _, f := range []url.Values{{"Entries.member.0.Id": {"a"}}, {"Entries.member.2.Id": {"a"}}, {"Entries.member.01.Id": {"a"}}, {"Entries.member.x.Id": {"a"}}} {
		_, e := queryEntries(f, "Entries", 10)
		require.Error(t, e)
	}
	_, e = queryStringMap(url.Values{"Attributes.entry.1.key": {"a"}, "Attributes.entry.2.key": {"a"}, "Attributes.entry.1.value": {"x"}, "Attributes.entry.2.value": {"y"}}, "Attributes")
	require.Error(t, e)
	v, e := queryStringList(url.Values{"attributes.member.1": {"a"}, "attributes.member.2": {"b"}}, "attributes")
	require.NoError(t, e)
	require.Equal(t, []string{"a", "b"}, v)
}

func TestSNSPaginationContextAndDeletedCursor(t *testing.T) {
	key := func(s string) string { return s }
	f := map[string]string{"parent": "p"}
	first, tok, e := pageSNS("endpoints", f, []string{"c", "a", "b"}, key, 1, "")
	require.NoError(t, e)
	require.Equal(t, []string{"a"}, first)
	require.NotEmpty(t, tok)
	next, _, e := pageSNS("endpoints", f, []string{"b", "c"}, key, 1, tok)
	require.NoError(t, e)
	require.Equal(t, []string{"b"}, next)
	_, _, e = pageSNS("applications", f, []string{"b"}, key, 1, tok)
	require.Error(t, e)
	_, _, e = pageSNS("endpoints", map[string]string{"parent": "q"}, []string{"b"}, key, 1, tok)
	require.Error(t, e)
	_, _, e = pageSNS("endpoints", f, []string{"b"}, key, -1, "")
	require.Error(t, e)
}
