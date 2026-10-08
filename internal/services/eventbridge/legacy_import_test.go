// SPDX-License-Identifier: Apache-2.0
package eventbridge

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/skyoo2003/devcloud/internal/shared/crud"
	"github.com/stretchr/testify/require"
)

func legacyRow(resource, id, raw string) crud.ResourceSnapshot {
	return crud.ResourceSnapshot{Resource: resource, ID: id, JSON: json.RawMessage(raw)}
}

func TestLegacyImportPreservesARNAndTime(t *testing.T) {
	s := newTestProvider(t).store
	rows := []crud.ResourceSnapshot{
		legacyRow("Connection", "old", `{"Name":"old","ConnectionArn":"old-arn","CreationTime":"2024-01-02T03:04:05Z","LastModifiedTime":1704164645,"AuthorizationType":"API_KEY","AuthParameters":{"ApiKeyAuthParameters":{"ApiKeyValue":"secret"}}}`),
		legacyRow("PartnerEventSource", "src", `{"Name":"aws.partner/devcloud/old","Account":"000000000000","EventSourceArn":"source-arn","CreationTime":1704164645,"State":"ACTIVE"}`),
	}
	require.NoError(t, s.ImportLegacy(rows))
	c, err := s.GetConnection("old", defaultAccountID)
	require.NoError(t, err)
	require.Equal(t, "old-arn", c.ARN)
	require.Equal(t, time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC), c.CreationTime)
	require.Equal(t, c.CreationTime, c.LastModifiedTime)
	source, err := s.GetPartnerSource("aws.partner/devcloud/old", defaultAccountID)
	require.NoError(t, err)
	require.Equal(t, "source-arn", source.ARN)
	require.Equal(t, "PENDING", source.State)
	var status string
	require.NoError(t, s.store.DB().QueryRow("SELECT status FROM legacy_imports WHERE resource='PartnerEventSource'").Scan(&status))
	require.Equal(t, "normalized-pending", status)
}

func TestLegacyImportKeepsRawJSON(t *testing.T) {
	s := newTestProvider(t).store
	row := legacyRow("Connection", "original", `{ "Name": "original", "Extra": {"preserve": true} }`)
	require.NoError(t, s.ImportLegacy([]crud.ResourceSnapshot{row}))
	var raw []byte
	require.NoError(t, s.store.DB().QueryRow("SELECT document_json FROM legacy_imports WHERE resource_id=?", row.ID).Scan(&raw))
	require.Equal(t, []byte(row.JSON), raw)
}

func TestLegacyImportDoesNotResurrectDeletedResource(t *testing.T) {
	path := t.TempDir()
	s, err := NewEBStore(path)
	require.NoError(t, err)
	rows := []crud.ResourceSnapshot{legacyRow("Connection", "old", `{"Name":"old"}`)}
	require.NoError(t, s.ImportLegacy(rows))
	_, err = s.DeleteConnection("old", defaultAccountID)
	require.NoError(t, err)
	require.NoError(t, s.Close())
	s, err = NewEBStore(path)
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })
	require.NoError(t, s.ImportLegacy(rows))
	_, err = s.GetConnection("old", defaultAccountID)
	require.ErrorIs(t, err, ErrConnectionNotFound)
}

func TestLegacyImportCanonicalWins(t *testing.T) {
	s := newTestProvider(t).store
	require.NoError(t, s.CreateConnection(Connection{Name: "keep", AccountID: defaultAccountID, ARN: "new-arn", State: "DEAUTHORIZED"}))
	require.NoError(t, s.ImportLegacy([]crud.ResourceSnapshot{legacyRow("Connection", "old", `{"Name":"keep","ConnectionArn":"old-arn"}`)}))
	c, err := s.GetConnection("keep", defaultAccountID)
	require.NoError(t, err)
	require.Equal(t, "new-arn", c.ARN)
	require.Equal(t, "DEAUTHORIZED", c.State)
	var status string
	require.NoError(t, s.store.DB().QueryRow("SELECT status FROM legacy_imports WHERE resource_id='old'").Scan(&status))
	require.Equal(t, "canonical-preserved", status)
}

func TestLegacyImportConflictingRowsRollback(t *testing.T) {
	s := newTestProvider(t).store
	rows := []crud.ResourceSnapshot{
		legacyRow("Connection", "first", `{"Name":"first"}`),
		legacyRow("PartnerEventSource", "one", `{"Name":"aws.partner/devcloud/conflict","Arn":"one"}`),
		legacyRow("EventSource", "two", `{"Name":"aws.partner/devcloud/conflict","Arn":"two"}`),
	}
	require.ErrorIs(t, s.ImportLegacy(rows), ErrLegacyConflict)
	var count int
	require.NoError(t, s.store.DB().QueryRow("SELECT count(*) FROM legacy_imports").Scan(&count))
	require.Zero(t, count)
	_, err := s.GetConnection("first", defaultAccountID)
	require.ErrorIs(t, err, ErrConnectionNotFound)
}

func TestLegacyImportMalformedRowRetained(t *testing.T) {
	s := newTestProvider(t).store
	rows := []crud.ResourceSnapshot{legacyRow("Connection", "first", `{"Name":"first"}`), legacyRow("Connection", "bad", `{"Name":`)}
	require.Error(t, s.ImportLegacy(rows))
	_, err := s.GetConnection("first", defaultAccountID)
	require.ErrorIs(t, err, ErrConnectionNotFound)
	require.Equal(t, `{"Name":`, string(rows[1].JSON))
	require.Error(t, s.ImportLegacy([]crud.ResourceSnapshot{legacyRow("Connection", "time", `{"Name":"time","CreationTime":"invalid"}`)}))
}

func TestLegacyImportFailureCanRetry(t *testing.T) {
	s := newTestProvider(t).store
	rows := []crud.ResourceSnapshot{legacyRow("Connection", "retry", `{"Name":"retry","CreationTime":"invalid"}`)}
	require.Error(t, s.ImportLegacy(rows))
	rows[0].JSON = json.RawMessage(`{"Name":"retry","CreationTime":1704164645}`)
	require.NoError(t, s.ImportLegacy(rows))
	require.NoError(t, s.ImportLegacy(rows))
	c, err := s.GetConnection("retry", defaultAccountID)
	require.NoError(t, err)
	require.Equal(t, int64(1704164645), c.CreationTime.Unix())
	var count int
	require.NoError(t, s.store.DB().QueryRow("SELECT count(*) FROM legacy_imports").Scan(&count))
	require.Equal(t, 1, count)
}

func TestLegacyImportAmbiguousPermissionDoesNotGrant(t *testing.T) {
	s := newTestProvider(t).store
	require.NoError(t, s.ImportLegacy([]crud.ResourceSnapshot{legacyRow("Permission", "unclear", `{"Principal":"*"}`)}))
	policy, err := s.GetBusPolicy("default", defaultAccountID)
	require.NoError(t, err)
	require.Nil(t, policy)
	var status string
	require.NoError(t, s.store.DB().QueryRow("SELECT status FROM legacy_imports").Scan(&status))
	require.Equal(t, "retained-ambiguous-policy", status)
}

func TestLegacyImportIncompleteExplicitPolicyDoesNotGrant(t *testing.T) {
	s := newTestProvider(t).store
	require.NoError(t, s.ImportLegacy([]crud.ResourceSnapshot{legacyRow("Permission", "partial", `{"EventBusName":"default","Policy":"{\"Statement\":[{\"Sid\":\"partial\"}]}"}`)}))
	policy, err := s.GetBusPolicy("default", defaultAccountID)
	require.NoError(t, err)
	require.Nil(t, policy)
}

func TestLegacyImportStatementPermissionsAndDefaultBus(t *testing.T) {
	path := t.TempDir()
	s, err := NewEBStore(path)
	require.NoError(t, err)
	rows := []crud.ResourceSnapshot{
		legacyRow("Permission", "first", `{"EventBusName":"default","StatementId":"first","Action":"events:PutEvents","Principal":"123456789012"}`),
		legacyRow("Permission", "second", `{"StatementId":"second","Action":"events:PutEvents","Principal":"*"}`),
		legacyRow("Permission", "full", `{"Policy":"{\"Version\":\"2012-10-17\",\"Statement\":{\"Sid\":\"full\",\"Effect\":\"Allow\",\"Action\":\"events:PutEvents\",\"Principal\":\"*\"}}"}`),
	}
	require.NoError(t, s.ImportLegacy(rows))
	policy, err := s.GetBusPolicy("default", defaultAccountID)
	require.NoError(t, err)
	require.Len(t, policy["Statement"], 3)
	require.NoError(t, s.Close())
	s, err = NewEBStore(path)
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })
	p := &Provider{store: s}
	require.Equal(t, 200, call(t, p, "RemovePermission", `{"StatementId":"first"}`).StatusCode)
	require.NoError(t, s.ImportLegacy(rows))
	policy, err = s.GetBusPolicy("default", defaultAccountID)
	require.NoError(t, err)
	require.Len(t, policy["Statement"], 2)
	for _, value := range policy["Statement"].([]any) {
		require.NotEqual(t, "first", value.(map[string]any)["Sid"])
	}
}

func TestLegacyImportConflictingPermissionsRollback(t *testing.T) {
	s := newTestProvider(t).store
	rows := []crud.ResourceSnapshot{legacyRow("Permission", "one", `{"StatementId":"same","Action":"events:PutEvents","Principal":"123456789012"}`), legacyRow("Permission", "two", `{"StatementId":"same","Action":"events:PutEvents","Principal":"*"}`)}
	require.ErrorIs(t, s.ImportLegacy(rows), ErrLegacyConflict)
	policy, err := s.GetBusPolicy("default", defaultAccountID)
	require.NoError(t, err)
	require.Nil(t, policy)
	var count int
	require.NoError(t, s.store.DB().QueryRow("SELECT count(*) FROM legacy_imports").Scan(&count))
	require.Zero(t, count)
}

func TestLegacyImportCanonicalPolicyWins(t *testing.T) {
	p := newTestProvider(t)
	require.Equal(t, 200, call(t, p, "PutPermission", `{"StatementId":"canonical","Action":"events:PutEvents","Principal":"*"}`).StatusCode)
	rows := []crud.ResourceSnapshot{legacyRow("Permission", "legacy", `{"StatementId":"legacy","Action":"events:PutEvents","Principal":"123456789012"}`)}
	require.NoError(t, p.store.ImportLegacy(rows))
	policy, err := p.store.GetBusPolicy("default", defaultAccountID)
	require.NoError(t, err)
	require.Len(t, policy["Statement"], 1)
	require.Equal(t, "canonical", policy["Statement"].([]any)[0].(map[string]any)["Sid"])
	var status string
	require.NoError(t, p.store.store.DB().QueryRow("SELECT status FROM legacy_imports WHERE resource_id='legacy'").Scan(&status))
	require.Equal(t, "canonical-preserved", status)
}
