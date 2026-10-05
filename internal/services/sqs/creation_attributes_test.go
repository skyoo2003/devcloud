// SPDX-License-Identifier: Apache-2.0
package sqs

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestQueueCreationPreservesVisibility(t *testing.T) {
	store := NewQueueStore(0)
	require.NoError(t, store.CreateQueueWithAttributes("source", defaultAccountID, map[string]string{"VisibilityTimeout": "1"}))
	attrs, err := store.GetQueueAttributes("source", defaultAccountID, []string{"VisibilityTimeout"})
	require.NoError(t, err)
	require.Equal(t, "1", attrs["VisibilityTimeout"])
}
