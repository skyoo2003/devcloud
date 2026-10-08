// SPDX-License-Identifier: Apache-2.0
package eventbridge

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPageItemsTokenContextAndDeletedCursor(t *testing.T) {
	key := func(s string) string { return s }
	filters := map[string]string{"NamePrefix": "a"}
	first, token, err := pageItems("sources", filters, []string{"ac", "aa", "ab"}, key, 1, "")
	require.NoError(t, err)
	require.Equal(t, []string{"aa"}, first)
	require.NotEmpty(t, token)
	second, _, err := pageItems("sources", filters, []string{"ac", "ab"}, key, 1, token)
	require.NoError(t, err)
	require.Equal(t, []string{"ab"}, second)
	_, _, err = pageItems("connections", filters, []string{"ab"}, key, 1, token)
	require.ErrorIs(t, err, errInvalidPageToken)
	_, _, err = pageItems("sources", map[string]string{"NamePrefix": "b"}, []string{"ab"}, key, 1, token)
	require.ErrorIs(t, err, errInvalidPageToken)
	_, _, err = pageItems("sources", filters, []string{"ab"}, key, 1, "bad")
	require.ErrorIs(t, err, errInvalidPageToken)
	_, _, err = pageItems("sources", filters, []string{"ab"}, key, 101, "")
	require.Error(t, err)
}
