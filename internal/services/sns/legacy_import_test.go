// SPDX-License-Identifier: Apache-2.0
package sns

import (
	"encoding/json"
	"github.com/skyoo2003/devcloud/internal/shared/crud"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

const legacyAppARN = "arn:aws:sns:us-east-1:000000000000:app/GCM/legacy"

func TestSNSLegacyEndpointAccountMismatchRollsBackAll(t *testing.T) {
	s := stateTestStore(t)
	endpoint := legacySNS("PlatformEndpoint", "e", `{"EndpointArn":"arn:aws:sns:us-east-1:111111111111:endpoint/GCM/legacy/e","PlatformApplicationArn":"`+legacyAppARN+`","Token":"token"}`)
	require.ErrorIs(t, s.ImportLegacy([]crud.ResourceSnapshot{legacyApp("app", legacyAppARN, "local"), endpoint}), ErrSNSLegacyConflict)
	apps, e := s.ListPlatformApplications(defaultAccountID)
	require.NoError(t, e)
	require.Empty(t, apps)
}

func TestSNSLegacyUnknownAttributesSurviveNativeSetter(t *testing.T) {
	p := newTestProvider(t)
	record := legacySNS("PlatformApplication", "a", `{"PlatformApplicationArn":"`+legacyAppARN+`","Attributes":{"FutureMetadata":"preserve"}}`)
	require.NoError(t, p.store.ImportLegacy([]crud.ResourceSnapshot{record}))
	require.Equal(t, 200, mobileSet(t, p, "SetPlatformApplicationAttributes", "PlatformApplicationArn", legacyAppARN, map[string]string{"SuccessFeedbackSampleRate": "10"}).StatusCode)
	attrs := snsAttrs(t, snsCall(t, p, "GetPlatformApplicationAttributes", map[string]string{"PlatformApplicationArn": legacyAppARN}))
	require.Equal(t, "preserve", attrs["FutureMetadata"])
	require.Equal(t, "10", attrs["SuccessFeedbackSampleRate"])
}

func legacySNS(resource, id, raw string) crud.ResourceSnapshot {
	return crud.ResourceSnapshot{Resource: resource, ID: id, JSON: json.RawMessage(raw)}
}
func legacyApp(id, arn, credential string) crud.ResourceSnapshot {
	return legacySNS("PlatformApplication", id, `{"PlatformApplicationArn":"`+arn+`","Name":"legacy","Platform":"GCM","CreationTime":"2020-01-02T03:04:05Z","Attributes":{"PlatformCredential":"`+credential+`"}}`)
}

func TestSNSLegacyARNTimeAndRawPreservation(t *testing.T) {
	s := stateTestStore(t)
	r := legacyApp("legacy", legacyAppARN, "preserved")
	require.NoError(t, s.ImportLegacy([]crud.ResourceSnapshot{r}))
	v, e := s.GetPlatformApplication(legacyAppARN)
	require.NoError(t, e)
	require.Equal(t, legacyAppARN, v.ARN)
	require.Equal(t, time.Date(2020, 1, 2, 3, 4, 5, 0, time.UTC), v.CreatedAt)
	var raw []byte
	require.NoError(t, s.store.DB().QueryRow(`SELECT document_json FROM sns_legacy_imports WHERE resource_id='legacy'`).Scan(&raw))
	require.Equal(t, []byte(r.JSON), raw)
}

func TestSNSLegacyCanonicalWinsAndNoResurrection(t *testing.T) {
	s := stateTestStore(t)
	_, e := s.CreatePlatformApplication(PlatformApplication{ARN: legacyAppARN, AccountID: defaultAccountID, Name: "legacy", Platform: "GCM", Attributes: map[string]string{"PlatformCredential": "canonical"}})
	require.NoError(t, e)
	records := []crud.ResourceSnapshot{legacyApp("a", legacyAppARN, "different")}
	require.NoError(t, s.ImportLegacy(records))
	v, e := s.GetPlatformApplication(legacyAppARN)
	require.NoError(t, e)
	require.Equal(t, "canonical", v.Attributes["PlatformCredential"])
	require.NoError(t, s.DeletePlatformApplication(legacyAppARN))
	require.NoError(t, s.ImportLegacy(records))
	_, e = s.GetPlatformApplication(legacyAppARN)
	require.ErrorIs(t, e, ErrMobileApplicationNotFound)
}

func TestSNSLegacyConflictRollsBackAll(t *testing.T) {
	s := stateTestStore(t)
	require.ErrorIs(t, s.ImportLegacy([]crud.ResourceSnapshot{legacyApp("a", legacyAppARN, "a"), legacyApp("b", legacyAppARN, "b")}), ErrSNSLegacyConflict)
	items, e := s.ListPlatformApplications(defaultAccountID)
	require.NoError(t, e)
	require.Empty(t, items)
	var n int
	require.NoError(t, s.store.DB().QueryRow(`SELECT COUNT(*) FROM sns_legacy_imports`).Scan(&n))
	require.Zero(t, n)
}

func TestSNSLegacyMissingParentAndVerification(t *testing.T) {
	s := stateTestStore(t)
	records := []crud.ResourceSnapshot{legacySNS("PlatformEndpoint", "e", `{"EndpointArn":"arn:aws:sns:us-east-1:000000000000:endpoint/GCM/missing/e","PlatformApplicationArn":"missing","Token":"t"}`), legacySNS("SMSSandboxPhoneNumber", "p", `{"PhoneNumber":"+12065550109","Status":"VERIFIED","CreationTime":100}`)}
	require.NoError(t, s.ImportLegacy(records))
	phones, e := s.ListSandboxPhones(defaultAccountID)
	require.NoError(t, e)
	require.Len(t, phones, 1)
	require.Equal(t, "UNVERIFIED", phones[0].Status)
	require.Empty(t, phones[0].OTPHash)
	require.True(t, phones[0].Consumed)
	require.ErrorIs(t, s.VerifySandboxChallenge(phones[0].PhoneNumber, defaultAccountID, "123456", time.Now()), ErrOTPVerification)
	var n int
	require.NoError(t, s.store.DB().QueryRow(`SELECT COUNT(*) FROM sns_sms_outbox`).Scan(&n))
	require.Zero(t, n)
	require.NoError(t, s.store.DB().QueryRow(`SELECT COUNT(*) FROM sns_platform_endpoints`).Scan(&n))
	require.Zero(t, n)
}

func TestSNSLegacyInvalidJSONAndRetry(t *testing.T) {
	s := stateTestStore(t)
	good := legacyApp("a", legacyAppARN, "v")
	for _, bad := range []crud.ResourceSnapshot{legacySNS("PlatformApplication", "b", `{`), legacySNS("PlatformApplication", "b", `{"CreationTime":"invalid"}`)} {
		require.Error(t, s.ImportLegacy([]crud.ResourceSnapshot{good, bad}))
		var n int
		require.NoError(t, s.store.DB().QueryRow(`SELECT COUNT(*) FROM sns_legacy_imports`).Scan(&n))
		require.Zero(t, n)
	}
	require.NoError(t, s.ImportLegacy([]crud.ResourceSnapshot{good}))
	require.NoError(t, s.ImportLegacy([]crud.ResourceSnapshot{good}))
}

func TestSNSLegacyMergeDistinctSMSKeys(t *testing.T) {
	s := stateTestStore(t)
	r := []crud.ResourceSnapshot{legacySNS("SMSAttribute", "a", `{"attributes":{"DefaultSenderID":"Legacy"}}`), legacySNS("SMSAttribute", "b", `{"attributes":{"DefaultSMSType":"Transactional"}}`)}
	require.NoError(t, s.ImportLegacy(r))
	attrs, e := s.GetSMSAttributes(defaultAccountID, nil)
	require.NoError(t, e)
	require.Equal(t, "Legacy", attrs["DefaultSenderID"])
	require.Equal(t, "Transactional", attrs["DefaultSMSType"])
	s2 := stateTestStore(t)
	r = append(r, legacySNS("SMSAttribute", "c", `{"attributes":{"DefaultSenderID":"Other"}}`))
	require.ErrorIs(t, s2.ImportLegacy(r), ErrSNSLegacyConflict)
	attrs, e = s2.GetSMSAttributes(defaultAccountID, nil)
	require.NoError(t, e)
	require.Equal(t, "Promotional", attrs["DefaultSMSType"])
}
