// SPDX-License-Identifier: Apache-2.0
package sns

import (
	"encoding/json"
	"github.com/stretchr/testify/require"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func outboxOTP(t *testing.T, s *SNSStore, phone string) string {
	t.Helper()
	var raw []byte
	require.NoError(t, s.store.DB().QueryRow(`SELECT body_json FROM sns_sms_outbox WHERE phone_number=? ORDER BY created_at DESC,rowid DESC LIMIT 1`, phone).Scan(&raw))
	var body map[string]string
	require.NoError(t, json.Unmarshal(raw, &body))
	return body["OneTimePassword"]
}

// Verification must consume the real emitted code and persist VERIFIED.
func TestSandboxOTPOutboxAndVerification(t *testing.T) {
	p := newTestProvider(t)
	phone := "+12065550103"
	r := snsCall(t, p, "CreateSMSSandboxPhoneNumber", map[string]string{"PhoneNumber": phone})
	require.Equal(t, 200, r.StatusCode)
	otp := outboxOTP(t, p.store, phone)
	require.Len(t, otp, 6)
	require.Equal(t, 200, snsCall(t, p, "VerifySMSSandboxPhoneNumber", map[string]string{"PhoneNumber": phone, "OneTimePassword": otp}).StatusCode)
	r = snsCall(t, p, "ListSMSSandboxPhoneNumbers", nil)
	require.Contains(t, string(r.Body), "VERIFIED")
	require.Contains(t, string(r.Body), phone)
	require.NotContains(t, string(r.Body), otp)
	r = snsCall(t, p, "VerifySMSSandboxPhoneNumber", map[string]string{"PhoneNumber": phone, "OneTimePassword": otp})
	require.Equal(t, 400, r.StatusCode)
	require.Contains(t, string(r.Body), "Verification")
	require.Contains(t, string(snsCall(t, p, "GetSMSSandboxAccountStatus", nil).Body), "<IsInSandbox>true</IsInSandbox>")
}

func TestSandboxOTPExpiryReissueAndConsumption(t *testing.T) {
	s := stateTestStore(t)
	phone := "+12065550104"
	now := time.Now().UTC()
	_, e := s.IssueSandboxChallenge(phone, defaultAccountID, "en-US", now)
	require.NoError(t, e)
	old := outboxOTP(t, s, phone)
	require.ErrorIs(t, s.VerifySandboxChallenge(phone, defaultAccountID, old, now.Add(10*time.Minute)), ErrOTPVerification)
	_, e = s.IssueSandboxChallenge(phone, defaultAccountID, "kr-KR", now.Add(time.Minute))
	require.NoError(t, e)
	fresh := outboxOTP(t, s, phone)
	require.NotEqual(t, old, fresh)
	require.ErrorIs(t, s.VerifySandboxChallenge(phone, defaultAccountID, old, now.Add(2*time.Minute)), ErrOTPVerification)
	require.NoError(t, s.VerifySandboxChallenge(phone, defaultAccountID, fresh, now.Add(2*time.Minute)))
	require.ErrorIs(t, s.VerifySandboxChallenge(phone, defaultAccountID, fresh, now.Add(2*time.Minute)), ErrOTPVerification)
	require.ErrorIs(t, s.VerifySandboxChallenge("+12065550105", defaultAccountID, fresh, now), ErrSandboxPhoneNotFound)
}

func TestSandboxConcurrentVerificationAndReissue(t *testing.T) {
	s := stateTestStore(t)
	phone := "+12065550106"
	now := time.Now().UTC()
	_, e := s.IssueSandboxChallenge(phone, defaultAccountID, "en-US", now)
	require.NoError(t, e)
	otp := outboxOTP(t, s, phone)
	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); errs <- s.VerifySandboxChallenge(phone, defaultAccountID, otp, now) }()
	}
	wg.Wait()
	close(errs)
	ok, bad := 0, 0
	for e := range errs {
		if e == nil {
			ok++
		} else {
			require.ErrorIs(t, e, ErrOTPVerification)
			bad++
		}
	}
	require.Equal(t, 1, ok)
	require.Equal(t, 1, bad)
	_, e = s.IssueSandboxChallenge(phone, defaultAccountID, "en-US", now.Add(time.Second))
	require.NoError(t, e)
	require.ErrorIs(t, s.VerifySandboxChallenge(phone, defaultAccountID, otp, now.Add(time.Second)), ErrOTPVerification)
}

func TestSandboxOptOutAndRollback(t *testing.T) {
	s := stateTestStore(t)
	phone := "+12065550107"
	now := time.Now().UTC()
	_, e := s.store.DB().Exec(`INSERT INTO sms_opt_outs VALUES (?,?)`, phone, defaultAccountID)
	require.NoError(t, e)
	_, e = s.IssueSandboxChallenge(phone, defaultAccountID, "en-US", now)
	require.ErrorIs(t, e, ErrSNSInvalidParameter)
	require.NoError(t, s.OptInPhoneNumber(phone, defaultAccountID))
	before, e := s.IssueSandboxChallenge(phone, defaultAccountID, "en-US", now)
	require.NoError(t, e)
	otp := outboxOTP(t, s, phone)
	_, e = s.store.DB().Exec(`CREATE TRIGGER block_sms_outbox BEFORE INSERT ON sns_sms_outbox BEGIN SELECT RAISE(ABORT,'blocked'); END;`)
	require.NoError(t, e)
	_, e = s.IssueSandboxChallenge(phone, defaultAccountID, "en-US", now.Add(time.Second))
	require.Error(t, e)
	items, e := s.ListSandboxPhones(defaultAccountID)
	require.NoError(t, e)
	require.Equal(t, *before, items[0])
	require.Equal(t, otp, outboxOTP(t, s, phone))
}

func TestSandboxListsAndReopen(t *testing.T) {
	dir := t.TempDir()
	s, e := NewSNSStore(dir)
	require.NoError(t, e)
	phone := "+12065550108"
	_, e = s.IssueSandboxChallenge(phone, defaultAccountID, "en-US", time.Now().UTC())
	require.NoError(t, e)
	require.NoError(t, s.Close())
	s, e = NewSNSStore(filepath.Clean(dir))
	require.NoError(t, e)
	defer func() { _ = s.Close() }()
	items, e := s.ListSandboxPhones(defaultAccountID)
	require.NoError(t, e)
	require.Len(t, items, 1)
	require.Equal(t, "UNVERIFIED", items[0].Status)
	require.NoError(t, s.DeleteSandboxPhone(phone, defaultAccountID))
	items, e = s.ListSandboxPhones(defaultAccountID)
	require.NoError(t, e)
	require.Empty(t, items)
	require.NotEmpty(t, outboxOTP(t, s, phone))
}
