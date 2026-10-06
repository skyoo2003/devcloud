// SPDX-License-Identifier: Apache-2.0
package eventbridge

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestCanonicalConnectionRollbackAndReopen(t *testing.T) {
	path := t.TempDir()
	s, err := NewEBStore(path)
	require.NoError(t, err)
	conn := Connection{Name: "key", AccountID: defaultAccountID, ARN: "stable", State: "AUTHORIZED", CreationTime: time.Date(2026, 10, 6, 0, 0, 0, 0, time.UTC), AuthParameters: map[string]any{"ApiKeyAuthParameters": map[string]any{"ApiKeyValue": "secret"}}}
	require.NoError(t, s.CreateConnection(conn))
	require.ErrorIs(t, s.CreateConnection(conn), ErrAlreadyExists)
	stop := errors.New("reject update")
	_, err = s.UpdateConnection("key", defaultAccountID, func(c *Connection) error { c.State = "DEAUTHORIZED"; c.AuthParameters = nil; return stop })
	require.ErrorIs(t, err, stop)
	loaded, err := s.GetConnection("key", defaultAccountID)
	require.NoError(t, err)
	loaded.AuthParameters["ApiKeyAuthParameters"].(map[string]any)["ApiKeyValue"] = "changed"
	require.NoError(t, s.Close())
	s, err = NewEBStore(path)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	loaded, err = s.GetConnection("key", defaultAccountID)
	require.NoError(t, err)
	require.Equal(t, "AUTHORIZED", loaded.State)
	require.Equal(t, "secret", loaded.AuthParameters["ApiKeyAuthParameters"].(map[string]any)["ApiKeyValue"])
	require.Equal(t, conn.CreationTime, loaded.CreationTime)
	_, err = s.DeleteConnection("key", defaultAccountID)
	require.NoError(t, err)
	_, err = s.GetConnection("key", defaultAccountID)
	require.ErrorIs(t, err, ErrConnectionNotFound)
}
