// SPDX-License-Identifier: Apache-2.0
package eventbridge

import (
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBusPolicyConcurrentChangesAndRollback(t *testing.T) {
	s := newTestProvider(t).store
	var wg sync.WaitGroup
	errorsFound := make(chan error, 20)
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			errorsFound <- s.UpdateBusPolicy("default", defaultAccountID, func(p map[string]any) (map[string]any, error) {
				if p == nil {
					p = map[string]any{"Statement": []any{}}
				}
				p["Statement"] = append(p["Statement"].([]any), map[string]any{"Sid": fmt.Sprint(i)})
				return p, nil
			})
		}(i)
	}
	wg.Wait()
	close(errorsFound)
	for err := range errorsFound {
		require.NoError(t, err)
	}
	policy, err := s.GetBusPolicy("default", defaultAccountID)
	require.NoError(t, err)
	require.Len(t, policy["Statement"], 20)
	stop := errors.New("rollback")
	err = s.UpdateBusPolicy("default", defaultAccountID, func(p map[string]any) (map[string]any, error) { p["Statement"] = []any{}; return p, stop })
	require.ErrorIs(t, err, stop)
	policy, err = s.GetBusPolicy("default", defaultAccountID)
	require.NoError(t, err)
	require.Len(t, policy["Statement"], 20)
	require.NoError(t, s.DeleteEventBus("default", defaultAccountID))
	_, err = s.GetBusPolicy("default", defaultAccountID)
	require.ErrorIs(t, err, ErrBusNotFound)
}
