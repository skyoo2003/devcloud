// SPDX-License-Identifier: Apache-2.0
package sns

import (
	"errors"
	"github.com/stretchr/testify/require"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"
)

func fifoStore(t *testing.T, dir string) *SNSStore {
	t.Helper()
	s, e := NewSNSStore(dir)
	require.NoError(t, e)
	t.Cleanup(func() { _ = s.Close() })
	_, e = s.CreateTopicWithAttributes("fifo", "fifo.fifo", defaultAccountID, map[string]string{"FifoTopic": "true", "ContentBasedDeduplication": "true"})
	require.NoError(t, e)
	return s
}
func TestFIFOConcurrencyOrderAndReopen(t *testing.T) {
	dir := t.TempDir()
	s := fifoStore(t, dir)
	now := time.Now()
	seen := []string{}
	var wg sync.WaitGroup
	errs := make(chan error, 10)
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, e := s.PublishMessage("fifo", PublishEntry{Message: strconv.Itoa(i), MessageGroupID: "g"}, now, func(v Publication, _ []Subscription) error { seen = append(seen, v.SequenceNumber); return nil })
			errs <- e
		}(i)
	}
	wg.Wait()
	close(errs)
	for e := range errs {
		require.NoError(t, e)
	}
	require.Len(t, seen, 10)
	for i := 1; i < len(seen); i++ {
		require.Less(t, seen[i-1], seen[i])
	}
	require.NoError(t, s.Close())
	s, e := NewSNSStore(filepath.Clean(dir))
	require.NoError(t, e)
	defer func() { _ = s.Close() }()
	n := 0
	v, e := s.PublishMessage("fifo", PublishEntry{Message: "0", MessageGroupID: "g"}, now.Add(time.Minute), func(Publication, []Subscription) error { n++; return nil })
	require.NoError(t, e)
	require.True(t, v.Duplicate)
	require.Zero(t, n)
	_, e = s.PublishMessage("fifo", PublishEntry{Message: "0", MessageGroupID: "g"}, now.Add(5*time.Minute), func(Publication, []Subscription) error { n++; return nil })
	require.NoError(t, e)
	require.Equal(t, 1, n)
}
func TestFIFODeliveryFailureRollbackAndRetry(t *testing.T) {
	s := fifoStore(t, t.TempDir())
	now := time.Now()
	entry := PublishEntry{Message: "retry", MessageGroupID: "g"}
	failure := errors.New("delivery failed")
	_, e := s.PublishMessage("fifo", entry, now, func(Publication, []Subscription) error { return failure })
	require.ErrorIs(t, e, failure)
	calls := 0
	v, e := s.PublishMessage("fifo", entry, now, func(Publication, []Subscription) error { calls++; return nil })
	require.NoError(t, e)
	require.False(t, v.Duplicate)
	require.Equal(t, "00000000000000000001", v.SequenceNumber)
	require.Equal(t, 1, calls)
	require.NoError(t, s.SetTopicAttribute("fifo", "FifoThroughputScope", "MessageGroup"))
	_, e = s.PublishMessage("fifo", PublishEntry{Message: "m", MessageGroupID: "a", MessageDeduplicationID: "shared"}, now, func(Publication, []Subscription) error { calls++; return nil })
	require.NoError(t, e)
	_, e = s.PublishMessage("fifo", PublishEntry{Message: "m", MessageGroupID: "b", MessageDeduplicationID: "shared"}, now, func(Publication, []Subscription) error { calls++; return nil })
	require.NoError(t, e)
	require.Equal(t, 3, calls)
	require.NoError(t, s.DeleteTopic("fifo"))
	_, e = s.CreateTopicWithAttributes("fifo", "fifo.fifo", defaultAccountID, map[string]string{"FifoTopic": "true", "ContentBasedDeduplication": "true"})
	require.NoError(t, e)
	v, e = s.PublishMessage("fifo", entry, now, func(Publication, []Subscription) error { return nil })
	require.NoError(t, e)
	require.Equal(t, "00000000000000000001", v.SequenceNumber)
}
