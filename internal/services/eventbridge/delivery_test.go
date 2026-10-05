// SPDX-License-Identifier: Apache-2.0
package eventbridge

import (
	"github.com/stretchr/testify/require"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"testing"
)

func TestEventBridgeQualifiedLambdaTarget(t *testing.T) {
	path := ""
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { path = r.URL.Path; _, _ = w.Write([]byte(`{}`)) }))
	defer s.Close()
	u, err := url.Parse(s.URL)
	require.NoError(t, err)
	port, err := strconv.Atoi(u.Port())
	require.NoError(t, err)
	p := &Provider{serverPort: port}
	p.dispatchToTarget("arn:aws:lambda:us-east-1:000000000000:function:function:published", []byte(`{"source":"phase1"}`))
	require.Equal(t, "/2015-03-31/functions/function:published/invocations", path)
}

func TestEventBridgeSNSUsesQueryProtocol(t *testing.T) {
	action := ""
	message := ""
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		action = r.FormValue("Action")
		message = r.FormValue("Message")
		_, _ = w.Write([]byte(`<PublishResponse/>`))
	}))
	defer s.Close()
	u, _ := url.Parse(s.URL)
	port, _ := strconv.Atoi(u.Port())
	p := &Provider{serverPort: port}
	p.dispatchToTarget("arn:aws:sns:us-east-1:000000000000:topic", []byte(`{"value":"+ & ="}`))
	require.Equal(t, "Publish", action)
	require.Equal(t, `{"value":"+ & ="}`, message)
}
