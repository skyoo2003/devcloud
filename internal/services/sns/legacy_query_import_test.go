// SPDX-License-Identifier: Apache-2.0
package sns

import (
	"encoding/json"
	"net/url"
	"testing"
	"time"

	"github.com/skyoo2003/devcloud/internal/shared/crud"
	"github.com/stretchr/testify/require"
)

// Produce the previous service's records rather than inventing its JSON shape.
func genericSNSMobileRecords(t *testing.T) ([]crud.ResourceSnapshot, string, string) {
	t.Helper()
	before := crud.RegisteredOps("sns")
	crud.Register("sns", map[string]crud.OpMeta{
		"CreatePlatformApplication": {Verb: "Create", Resource: "PlatformApplication"},
		"CreatePlatformEndpoint":    {Verb: "Create", Resource: "PlatformEndpoint"},
	})
	require.NoError(t, crud.Reset())
	t.Cleanup(func() {
		require.NoError(t, crud.Reset())
		crud.Register("sns", before)
	})
	create := func(form url.Values) {
		r, err := crud.Handle(crud.Call{Service: "sns", Protocol: "query", Body: []byte(form.Encode())})
		require.NoError(t, err)
		require.Equal(t, 200, r.Status, string(r.Body))
	}
	create(url.Values{"Action": {"CreatePlatformApplication"}, "Name": {"legacy-query"}, "Platform": {"GCM"}, "Attributes.entry.1.key": {"PlatformCredential"}, "Attributes.entry.1.value": {"local-secret"}, "Attributes.entry.2.key": {"FutureMetadata"}, "Attributes.entry.2.value": {"keep"}})
	apps, err := crud.Snapshot("sns", []string{"PlatformApplication"})
	require.NoError(t, err)
	require.Len(t, apps, 1)
	var app map[string]any
	require.NoError(t, json.Unmarshal(apps[0].JSON, &app))
	appARN := app["PlatformApplicationArn"].(string)
	create(url.Values{"Action": {"CreatePlatformEndpoint"}, "PlatformApplicationArn": {appARN}, "Token": {"legacy-token"}, "Attributes.entry.1.key": {"Enabled"}, "Attributes.entry.1.value": {"false"}})
	records, err := crud.Snapshot("sns", []string{"PlatformApplication", "PlatformEndpoint"})
	require.NoError(t, err)
	require.Len(t, records, 2)
	var endpoint map[string]any
	require.NoError(t, json.Unmarshal(records[1].JSON, &endpoint))
	return records, appARN, endpoint["PlatformEndpointArn"].(string)
}

func TestSNSLegacyGenericQueryAttributesSurviveSetterAndReopen(t *testing.T) {
	records, arn, _ := genericSNSMobileRecords(t)
	dir := t.TempDir()
	s, err := NewSNSStore(dir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })
	require.NoError(t, s.ImportLegacy(records[:1]))
	p := &Provider{store: s}
	attrs := snsAttrs(t, snsCall(t, p, "GetPlatformApplicationAttributes", map[string]string{"PlatformApplicationArn": arn}))
	require.Equal(t, "local-secret", attrs["PlatformCredential"])
	require.Equal(t, "keep", attrs["FutureMetadata"])
	require.Equal(t, 200, mobileSet(t, p, "SetPlatformApplicationAttributes", "PlatformApplicationArn", arn, map[string]string{"SuccessFeedbackSampleRate": "11"}).StatusCode)
	require.NoError(t, s.Close())
	s, err = NewSNSStore(dir)
	require.NoError(t, err)
	require.NoError(t, s.ImportLegacy(records[:1]))
	v, err := s.GetPlatformApplication(arn)
	require.NoError(t, err)
	require.Equal(t, map[string]string{"PlatformCredential": "local-secret", "FutureMetadata": "keep", "SuccessFeedbackSampleRate": "11"}, v.Attributes)
	var raw map[string]any
	require.NoError(t, json.Unmarshal(records[0].JSON, &raw))
	created, err := time.Parse(time.RFC3339, raw["CreationTime"].(string))
	require.NoError(t, err)
	require.Equal(t, created, v.CreatedAt)
	var retained []byte
	require.NoError(t, s.store.DB().QueryRow(`SELECT document_json FROM sns_legacy_imports WHERE resource=? AND resource_id=?`, records[0].Resource, records[0].ID).Scan(&retained))
	require.Equal(t, []byte(records[0].JSON), retained)
}

func TestSNSLegacyGenericEndpointARNRemainsAddressable(t *testing.T) {
	records, parent, arn := genericSNSMobileRecords(t)
	dir := t.TempDir()
	s, err := NewSNSStore(dir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })
	require.NoError(t, s.ImportLegacy(records))
	v, err := s.GetPlatformEndpoint(arn)
	require.NoError(t, err)
	require.Equal(t, parent, v.ApplicationARN)
	require.Equal(t, defaultAccountID, v.AccountID)
	require.Equal(t, "false", v.Attributes["Enabled"])
	p := &Provider{store: s}
	require.Equal(t, "false", snsAttrs(t, snsCall(t, p, "GetEndpointAttributes", map[string]string{"EndpointArn": arn}))["Enabled"])
	require.Equal(t, 200, mobileSet(t, p, "SetEndpointAttributes", "EndpointArn", arn, map[string]string{"CustomUserData": "updated"}).StatusCode)
	require.NoError(t, s.Close())
	s, err = NewSNSStore(dir)
	require.NoError(t, err)
	require.NoError(t, s.ImportLegacy(records))
	items, err := s.ListPlatformEndpoints(parent)
	require.NoError(t, err)
	require.Len(t, items, 1)
	require.Equal(t, arn, items[0].ARN)
	require.Equal(t, "false", items[0].Attributes["Enabled"])
	require.Equal(t, "updated", items[0].Attributes["CustomUserData"])
}

func TestSNSLegacyQueryRepresentationConflictsRollBack(t *testing.T) {
	for name, raw := range map[string]string{
		"attributes":   `{"PlatformApplicationArn":"` + legacyAppARN + `","Attributes":{"PlatformCredential":"nested"},"Attributes.entry.1.key":"PlatformCredential","Attributes.entry.1.value":"flat"}`,
		"endpoint ARN": `{"PlatformEndpointArn":"arn:aws:sns:us-east-1:000000000000:platformendpoint/one","EndpointArn":"arn:aws:sns:us-east-1:000000000000:platformendpoint/two","PlatformApplicationArn":"` + legacyAppARN + `","Token":"token"}`,
	} {
		t.Run(name, func(t *testing.T) {
			s := stateTestStore(t)
			resource := "PlatformApplication"
			if name == "endpoint ARN" {
				resource = "PlatformEndpoint"
			}
			good := legacySNS("PlatformApplication", "good", `{"PlatformApplicationArn":"arn:aws:sns:us-east-1:000000000000:app/GCM/good"}`)
			require.ErrorIs(t, s.ImportLegacy([]crud.ResourceSnapshot{good, legacySNS(resource, "bad", raw)}), ErrSNSLegacyConflict)
			var n int
			require.NoError(t, s.store.DB().QueryRow(`SELECT COUNT(*) FROM sns_legacy_imports`).Scan(&n))
			require.Zero(t, n)
			apps, err := s.ListPlatformApplications(defaultAccountID)
			require.NoError(t, err)
			require.Empty(t, apps)
		})
	}
}
