// SPDX-License-Identifier: Apache-2.0
package dynamodbstreams

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestShardBoundaryAndNewGeneration(t *testing.T) {
	dir := t.TempDir()
	s, err := NewStore(dir)
	require.NoError(t, err)
	arn := "arn:aws:dynamodb:us-east-1:000000000000:table/t/stream/1"
	_, err = s.CreateStream(arn, "t", "1", "NEW_IMAGE")
	require.NoError(t, err)
	g, seq, err := s.ShardBoundary(arn, streamShardID(arn))
	require.NoError(t, err)
	require.Equal(t, "0", seq)
	require.NoError(t, s.PublishRecord("t", "INSERT", map[string]any{"id": "value"}, nil, nil))
	same, last, err := s.ShardBoundary(arn, streamShardID(arn))
	require.NoError(t, err)
	require.Equal(t, g, same)
	require.NotEqual(t, "0", last)
	require.NoError(t, s.Close())
	s, err = NewStore(dir)
	require.NoError(t, err)
	defer func() { require.NoError(t, s.Close()) }()
	next, seq, err := s.ShardBoundary(arn, streamShardID(arn))
	require.NoError(t, err)
	require.NotEqual(t, g, next)
	require.Equal(t, "0", seq)
}
