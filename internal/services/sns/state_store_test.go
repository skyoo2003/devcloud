// SPDX-License-Identifier: Apache-2.0
package sns

import (
	"path/filepath"
	"sync"
	"testing"

	"github.com/skyoo2003/devcloud/internal/storage/sqlite"
	"github.com/stretchr/testify/require"
)

func stateTestStore(t *testing.T) *SNSStore {
	t.Helper()
	s, e := NewSNSStore(t.TempDir())
	require.NoError(t, e)
	t.Cleanup(func() { _ = s.Close() })
	return s
}

// A destructive upgrade or non-idempotent DDL loses existing subscription data.
func TestSNSMigrationUpgradeAndPartialRetry(t *testing.T) {
	dir := t.TempDir()
	old, e := sqlite.Open(filepath.Join(dir, "sns.db"), migrations[:3])
	require.NoError(t, e)
	_, e = old.DB().Exec(`INSERT INTO topics VALUES ('original','old','000000000000','{}',123); INSERT INTO subscriptions VALUES ('sub','original','sqs','queue','000000000000',1,'{"RawMessageDelivery":"true"}'); INSERT INTO sms_opt_outs VALUES ('+12065550101','000000000000'); CREATE TABLE sns_sms_settings(account_id TEXT NOT NULL PRIMARY KEY, attributes_json BLOB NOT NULL);`)
	require.NoError(t, e)
	require.NoError(t, old.Close())
	for i := 0; i < 2; i++ {
		s, e := NewSNSStore(dir)
		require.NoError(t, e)
		topic, e := s.GetTopic("original")
		require.NoError(t, e)
		require.Equal(t, int64(123), topic.CreatedAt.Unix())
		var a string
		require.NoError(t, s.store.DB().QueryRow(`SELECT attributes FROM subscriptions WHERE arn='sub'`).Scan(&a))
		require.Equal(t, `{"RawMessageDelivery":"true"}`, a)
		var n int
		require.NoError(t, s.store.DB().QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE name='sns_sms_outbox'`).Scan(&n))
		require.Equal(t, 1, n)
		require.NoError(t, s.Close())
	}
}

// Replacing the whole settings map or validating after write loses old keys.
func TestSMSSettingsPersistAndRollback(t *testing.T) {
	dir := t.TempDir()
	s, e := NewSNSStore(dir)
	require.NoError(t, e)
	require.NoError(t, s.SetSMSAttributes(defaultAccountID, map[string]string{"DefaultSMSType": "Transactional", "MonthlySpendLimit": "1.50"}))
	for _, bad := range []map[string]string{{"DefaultSenderID": "123"}, {"MonthlySpendLimit": "NaN"}, {"DeliveryStatusSuccessSamplingRate": "101"}, {"DefaultSMSType": "wrong"}, {"unknown": "value"}, {}} {
		require.Error(t, s.SetSMSAttributes(defaultAccountID, bad))
	}
	require.NoError(t, s.Close())
	s, e = NewSNSStore(dir)
	require.NoError(t, e)
	defer func() { _ = s.Close() }()
	attrs, e := s.GetSMSAttributes(defaultAccountID, nil)
	require.NoError(t, e)
	require.Equal(t, "Transactional", attrs["DefaultSMSType"])
	require.Equal(t, "1.50", attrs["MonthlySpendLimit"])
	require.Equal(t, "0", attrs["DeliveryStatusSuccessSamplingRate"])
	filtered, e := s.GetSMSAttributes(defaultAccountID, []string{"MonthlySpendLimit", "missing"})
	require.NoError(t, e)
	require.Equal(t, map[string]string{"MonthlySpendLimit": "1.50"}, filtered)
	require.NoError(t, s.Close())
	_, e = s.GetSMSAttributes(defaultAccountID, nil)
	require.Error(t, e)
}

// Opt-in must mutate the existing table, not return a fabricated success.
func TestOptInReadsExistingOptOuts(t *testing.T) {
	p := newTestProvider(t)
	_, e := p.store.store.DB().Exec(`INSERT INTO sms_opt_outs VALUES ('+12065550101',?),('+12065550102',?)`, defaultAccountID, defaultAccountID)
	require.NoError(t, e)
	r := handle(t, p, "Action=CheckIfPhoneNumberIsOptedOut&phoneNumber=%2B12065550101")
	require.Contains(t, string(r.Body), "<isOptedOut>true</isOptedOut>")
	require.Equal(t, 200, handle(t, p, "Action=OptInPhoneNumber&phoneNumber=%2B12065550101").StatusCode)
	out, e := p.store.IsPhoneOptedOut("+12065550101", defaultAccountID)
	require.NoError(t, e)
	require.False(t, out)
	phones, e := p.store.ListOptedOutPhones(defaultAccountID)
	require.NoError(t, e)
	require.Equal(t, []string{"+12065550102"}, phones)
	require.Equal(t, 200, handle(t, p, "Action=OptInPhoneNumber&phoneNumber=%2B12065550101").StatusCode)
	require.Equal(t, 400, handle(t, p, "Action=OptInPhoneNumber&phoneNumber=bad").StatusCode)
	require.NoError(t, p.store.Close())
	require.Equal(t, 500, handle(t, p, "Action=ListPhoneNumbersOptedOut").StatusCode)
}

func TestSMSConcurrentDistinctKeys(t *testing.T) {
	s := stateTestStore(t)
	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for k, v := range map[string]string{"DefaultSenderID": "DevCloud", "DefaultSMSType": "Transactional"} {
		wg.Add(1)
		go func(k, v string) {
			defer wg.Done()
			errs <- s.SetSMSAttributes(defaultAccountID, map[string]string{k: v})
		}(k, v)
	}
	wg.Wait()
	close(errs)
	for e := range errs {
		require.NoError(t, e)
	}
	a, e := s.GetSMSAttributes(defaultAccountID, nil)
	require.NoError(t, e)
	require.Equal(t, "DevCloud", a["DefaultSenderID"])
	require.Equal(t, "Transactional", a["DefaultSMSType"])
}
