// SPDX-License-Identifier: Apache-2.0
package sns

import (
	"encoding/xml"
	"errors"
	"github.com/skyoo2003/devcloud/internal/plugin"
	"github.com/stretchr/testify/require"
	"net/url"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
)

func snsCall(t *testing.T, p *Provider, action string, fields map[string]string) *plugin.Response {
	t.Helper()
	v := url.Values{"Action": {action}}
	for k, x := range fields {
		v.Set(k, x)
	}
	return handle(t, p, v.Encode())
}
func snsText(t *testing.T, r *plugin.Response, name string) string {
	t.Helper()
	d := xml.NewDecoder(strings.NewReader(string(r.Body)))
	for {
		tok, e := d.Token()
		if e != nil {
			t.Fatalf("missing XML %s: %s", name, r.Body)
		}
		if s, ok := tok.(xml.StartElement); ok && s.Name.Local == name {
			var text string
			require.NoError(t, d.DecodeElement(&text, &s))
			return text
		}
	}
}
func snsAttrs(t *testing.T, r *plugin.Response) map[string]string {
	t.Helper()
	d := xml.NewDecoder(strings.NewReader(string(r.Body)))
	m := map[string]string{}
	for {
		tok, e := d.Token()
		if e != nil {
			break
		}
		if s, ok := tok.(xml.StartElement); ok && s.Name.Local == "entry" {
			var a queryAttribute
			require.NoError(t, d.DecodeElement(&a, &s))
			m[a.Key] = a.Value
		}
	}
	return m
}
func attributeFields(attrs map[string]string) map[string]string {
	v := map[string]string{}
	i := 1
	for k, x := range attrs {
		n := strconv.Itoa(i)
		v["Attributes.entry."+n+".key"] = k
		v["Attributes.entry."+n+".value"] = x
		i++
	}
	return v
}
func mobileSetup(t *testing.T) (*Provider, string, string) {
	t.Helper()
	p := newTestProvider(t)
	f := attributeFields(map[string]string{"PlatformCredential": "local-credential"})
	f["Name"] = "app.name"
	f["Platform"] = "GCM"
	r := snsCall(t, p, "CreatePlatformApplication", f)
	require.Equal(t, 200, r.StatusCode)
	a := snsText(t, r, "PlatformApplicationArn")
	r = snsCall(t, p, "CreatePlatformEndpoint", map[string]string{"PlatformApplicationArn": a, "Token": "token-a", "CustomUserData": "before"})
	require.Equal(t, 200, r.StatusCode)
	return p, a, snsText(t, r, "EndpointArn")
}
func mobileSet(t *testing.T, p *Provider, action, field, arn string, attrs map[string]string) *plugin.Response {
	t.Helper()
	f := attributeFields(attrs)
	f[field] = arn
	return snsCall(t, p, action, f)
}

// A partial setter must not overwrite credentials or change resource identity.
func TestMobileApplicationSetterAndReopen(t *testing.T) {
	p, a, _ := mobileSetup(t)
	require.Equal(t, 200, mobileSet(t, p, "SetPlatformApplicationAttributes", "PlatformApplicationArn", a, map[string]string{"SuccessFeedbackSampleRate": "25"}).StatusCode)
	attrs := snsAttrs(t, snsCall(t, p, "GetPlatformApplicationAttributes", map[string]string{"PlatformApplicationArn": a}))
	require.Equal(t, "local-credential", attrs["PlatformCredential"])
	require.Equal(t, "25", attrs["SuccessFeedbackSampleRate"])
	require.Equal(t, 400, mobileSet(t, p, "SetPlatformApplicationAttributes", "PlatformApplicationArn", a, map[string]string{"SuccessFeedbackSampleRate": "101"}).StatusCode)
	require.Equal(t, 400, mobileSet(t, p, "SetPlatformApplicationAttributes", "PlatformApplicationArn", a, map[string]string{"unknown": "x"}).StatusCode)
	var path string
	require.NoError(t, p.store.store.DB().QueryRow(`SELECT file FROM pragma_database_list WHERE name='main'`).Scan(&path))
	require.NoError(t, p.store.Close())
	s, e := NewSNSStore(filepath.Dir(path))
	require.NoError(t, e)
	defer func() { _ = s.Close() }()
	v, e := s.GetPlatformApplication(a)
	require.NoError(t, e)
	require.Equal(t, "25", v.Attributes["SuccessFeedbackSampleRate"])
}

func TestMobileEndpointSetterPreservesOtherKeys(t *testing.T) {
	p, _, a := mobileSetup(t)
	r := mobileSet(t, p, "SetEndpointAttributes", "EndpointArn", a, map[string]string{"Enabled": "false", "CustomUserData": "한글 + data"})
	require.Equal(t, 200, r.StatusCode)
	attrs := snsAttrs(t, snsCall(t, p, "GetEndpointAttributes", map[string]string{"EndpointArn": a}))
	require.Equal(t, "token-a", attrs["Token"])
	require.Equal(t, "false", attrs["Enabled"])
	require.Equal(t, "한글 + data", attrs["CustomUserData"])
	for _, bad := range []map[string]string{{"Token": ""}, {"Enabled": "yes"}, {"CustomUserData": strings.Repeat("x", 2048)}, {"unknown": "x"}, {}} {
		require.Equal(t, 400, mobileSet(t, p, "SetEndpointAttributes", "EndpointArn", a, bad).StatusCode)
	}
	require.Equal(t, attrs, snsAttrs(t, snsCall(t, p, "GetEndpointAttributes", map[string]string{"EndpointArn": a})))
}

func TestMobileTokenIdempotenceAndConflict(t *testing.T) {
	p, parent, a := mobileSetup(t)
	r := snsCall(t, p, "CreatePlatformEndpoint", map[string]string{"PlatformApplicationArn": parent, "Token": "token-a", "CustomUserData": "before"})
	require.Equal(t, 200, r.StatusCode)
	require.Equal(t, a, snsText(t, r, "EndpointArn"))
	require.Equal(t, 400, snsCall(t, p, "CreatePlatformEndpoint", map[string]string{"PlatformApplicationArn": parent, "Token": "token-a", "CustomUserData": "different"}).StatusCode)
	require.Equal(t, 404, snsCall(t, p, "CreatePlatformEndpoint", map[string]string{"PlatformApplicationArn": "missing", "Token": "x"}).StatusCode)
}

func TestMobileDeleteApplicationCascades(t *testing.T) {
	p, parent, a := mobileSetup(t)
	require.Equal(t, 200, snsCall(t, p, "DeletePlatformApplication", map[string]string{"PlatformApplicationArn": parent}).StatusCode)
	require.Equal(t, 404, snsCall(t, p, "GetEndpointAttributes", map[string]string{"EndpointArn": a}).StatusCode)
	_, e := p.store.GetPlatformEndpoint(a)
	require.ErrorIs(t, e, ErrMobileEndpointNotFound)
}

func TestMobileConcurrentSettersAndTokenCollision(t *testing.T) {
	p, parent, a := mobileSetup(t)
	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for k, v := range map[string]string{"Enabled": "false", "CustomUserData": "parallel"} {
		wg.Add(1)
		go func(k, v string) {
			defer wg.Done()
			_, e := p.store.UpdatePlatformEndpoint(a, func(x *PlatformEndpoint) error { x.Attributes[k] = v; return nil })
			errs <- e
		}(k, v)
	}
	wg.Wait()
	close(errs)
	for e := range errs {
		require.NoError(t, e)
	}
	x, e := p.store.GetPlatformEndpoint(a)
	require.NoError(t, e)
	require.Equal(t, "false", x.Attributes["Enabled"])
	require.Equal(t, "parallel", x.Attributes["CustomUserData"])
	r := snsCall(t, p, "CreatePlatformEndpoint", map[string]string{"PlatformApplicationArn": parent, "Token": "token-b"})
	require.Equal(t, 200, r.StatusCode)
	b := snsText(t, r, "EndpointArn")
	require.Equal(t, 400, mobileSet(t, p, "SetEndpointAttributes", "EndpointArn", b, map[string]string{"Token": "token-a"}).StatusCode)
	stop := errors.New("stop")
	_, e = p.store.UpdatePlatformEndpoint(a, func(v *PlatformEndpoint) error { v.Attributes["Enabled"] = "true"; return stop })
	require.ErrorIs(t, e, stop)
	after, e := p.store.GetPlatformEndpoint(a)
	require.NoError(t, e)
	require.Equal(t, x, after)
}

func TestMobileListsAndClosedStore(t *testing.T) {
	p, parent, a := mobileSetup(t)
	r := snsCall(t, p, "ListEndpointsByPlatformApplication", map[string]string{"PlatformApplicationArn": parent})
	require.Equal(t, 200, r.StatusCode)
	require.Contains(t, string(r.Body), a)
	require.Contains(t, string(snsCall(t, p, "ListPlatformApplications", nil).Body), parent)
	require.NoError(t, p.store.Close())
	require.Equal(t, 500, snsCall(t, p, "GetEndpointAttributes", map[string]string{"EndpointArn": a}).StatusCode)
	require.Equal(t, 500, snsCall(t, p, "ListPlatformApplications", nil).StatusCode)
}
