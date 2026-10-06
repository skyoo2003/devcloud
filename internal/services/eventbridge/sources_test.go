// SPDX-License-Identifier: Apache-2.0
package eventbridge

import (
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestSourceOperationLifecycle(t *testing.T) {
	p := newTestProvider(t)
	const source = `aws.partner/devcloud/lifecycle`
	params := `{"Name":"` + source + `","Account":"000000000000"}`
	require.Equal(t, 200, call(t, p, "CreatePartnerEventSource", params).StatusCode)
	require.Equal(t, 400, call(t, p, "CreatePartnerEventSource", params).StatusCode)
	name := `{"Name":"` + source + `"}`
	require.Equal(t, "PENDING", parseJSON(t, call(t, p, "DescribeEventSource", name))["State"])
	require.Equal(t, 400, call(t, p, "ActivateEventSource", name).StatusCode)
	require.Equal(t, 400, call(t, p, "CreateEventBus", `{"Name":"wrong","EventSourceName":"`+source+`"}`).StatusCode)
	require.Equal(t, 200, call(t, p, "CreateEventBus", `{"Name":"`+source+`","EventSourceName":"`+source+`"}`).StatusCode)
	require.Equal(t, "ACTIVE", parseJSON(t, call(t, p, "DescribeEventSource", name))["State"])
	require.Equal(t, 200, call(t, p, "PutRule", `{"Name":"keep","EventBusName":"`+source+`"}`).StatusCode)
	for _, op := range []string{"DeactivateEventSource", "DeactivateEventSource", "ActivateEventSource", "ActivateEventSource"} {
		response := call(t, p, op, name)
		require.Equal(t, 200, response.StatusCode)
		require.Empty(t, response.Body)
	}
	rules, err := p.store.ListRules(source, defaultAccountID)
	require.NoError(t, err)
	require.Len(t, rules, 1)
	require.Equal(t, 200, call(t, p, "DeleteEventBus", name).StatusCode)
	require.Equal(t, "PENDING", parseJSON(t, call(t, p, "DescribeEventSource", name))["State"])
	response := call(t, p, "DeletePartnerEventSource", params)
	require.Equal(t, 200, response.StatusCode)
	require.Empty(t, response.Body)
	require.Equal(t, 400, call(t, p, "DescribeEventSource", name).StatusCode)
}

func TestSourceOperationValidationAndPagination(t *testing.T) {
	p := newTestProvider(t)
	for _, body := range []string{`{"Name":"invalid","Account":"000000000000"}`, `{"Name":"aws.partner/devcloud/a","Account":"111111111111"}`, `{"Name":"aws.partner/devcloud/a","Account":"wrong"}`} {
		require.Equal(t, 400, call(t, p, "CreatePartnerEventSource", body).StatusCode)
	}
	for _, suffix := range []string{"b", "a", "c"} {
		require.Equal(t, 200, call(t, p, "CreatePartnerEventSource", `{"Name":"aws.partner/devcloud/`+suffix+`","Account":"000000000000"}`).StatusCode)
	}
	first := parseJSON(t, call(t, p, "ListEventSources", `{"NamePrefix":"aws.partner/devcloud/","Limit":1}`))
	entries := first["EventSources"].([]any)
	require.Len(t, entries, 1)
	require.Equal(t, "aws.partner/devcloud/a", entries[0].(map[string]any)["Name"])
	token := first["NextToken"].(string)
	body, _ := json.Marshal(map[string]any{"NamePrefix": "aws.partner/devcloud/", "Limit": 1, "NextToken": token})
	next := parseJSON(t, call(t, p, "ListEventSources", string(body)))
	require.Equal(t, "aws.partner/devcloud/b", next["EventSources"].([]any)[0].(map[string]any)["Name"])
	require.Equal(t, 400, call(t, p, "ListPartnerEventSources", string(body)).StatusCode)
	require.Equal(t, 400, call(t, p, "ListEventSources", `{"Limit":0}`).StatusCode)
	require.Equal(t, 400, call(t, p, "ListEventSources", `{"Limit":1.5}`).StatusCode)
	partners := parseJSON(t, call(t, p, "ListPartnerEventSources", `{"NamePrefix":"aws.partner/devcloud/"}`))
	require.Len(t, partners["PartnerEventSources"], 3)
	account := parseJSON(t, call(t, p, "ListPartnerEventSourceAccounts", `{"EventSourceName":"aws.partner/devcloud/a"}`))["PartnerEventSourceAccounts"].([]any)[0].(map[string]any)
	require.Equal(t, defaultAccountID, account["Account"])
	require.Equal(t, "PENDING", account["State"])
}

func TestPartnerEventsAdmissionAndDelivery(t *testing.T) {
	p := newTestProvider(t)
	delivered := make(chan []byte, 2)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		payload, _ := io.ReadAll(r.Body)
		delivered <- payload
		w.Header().Set("Content-Type", "application/x-amz-json-1.0")
		_, _ = w.Write([]byte(`{}`))
	}))
	defer server.Close()
	_, port, err := net.SplitHostPort(server.Listener.Addr().String())
	require.NoError(t, err)
	p.serverPort, err = strconv.Atoi(port)
	require.NoError(t, err)
	const source = "aws.partner/devcloud/delivery"
	require.Equal(t, 200, call(t, p, "CreatePartnerEventSource", `{"Name":"`+source+`","Account":"000000000000"}`).StatusCode)
	require.Equal(t, 200, call(t, p, "CreateEventBus", `{"Name":"`+source+`","EventSourceName":"`+source+`"}`).StatusCode)
	require.Equal(t, 200, call(t, p, "PutRule", `{"Name":"send","EventBusName":"`+source+`"}`).StatusCode)
	require.Equal(t, 200, call(t, p, "PutTargets", `{"Rule":"send","EventBusName":"`+source+`","Targets":[{"Id":"queue","Arn":"arn:aws:sqs:us-east-1:000000000000:queue"}]}`).StatusCode)
	detail := `{"text":"한글 + payload & key=value %20\nsecond line"}`
	body, _ := json.Marshal(map[string]any{"Entries": []any{map[string]any{"Source": source, "DetailType": "demo", "Detail": detail}, map[string]any{"Source": "aws.partner/devcloud/missing", "DetailType": "demo", "Detail": detail}, map[string]any{"Source": source, "DetailType": "demo", "Detail": "bad"}}})
	result := parseJSON(t, call(t, p, "PutPartnerEvents", string(body)))
	require.Equal(t, float64(2), result["FailedEntryCount"])
	entries := result["Entries"].([]any)
	require.Len(t, entries, 3)
	require.Contains(t, entries[0], "EventId")
	require.NotContains(t, entries[1], "EventId")
	require.NotContains(t, entries[2], "EventId")
	select {
	case payload := <-delivered:
		var request map[string]any
		require.NoError(t, json.Unmarshal(payload, &request))
		var event map[string]any
		require.NoError(t, json.Unmarshal([]byte(request["MessageBody"].(string)), &event))
		require.JSONEq(t, detail, event["Detail"].(string))
	case <-time.After(2 * time.Second):
		t.Fatal("partner event was not delivered")
	}
	require.Equal(t, 200, call(t, p, "DeactivateEventSource", `{"Name":"`+source+`"}`).StatusCode)
	result = parseJSON(t, call(t, p, "PutPartnerEvents", string(body)))
	require.Equal(t, float64(3), result["FailedEntryCount"])
	select {
	case <-delivered:
		t.Fatal("pending source delivered an event")
	case <-time.After(50 * time.Millisecond):
	}
	require.Equal(t, 200, call(t, p, "ActivateEventSource", `{"Name":"`+source+`"}`).StatusCode)
	p.serverPort = 0
	result = parseJSON(t, call(t, p, "PutPartnerEvents", string(body)))
	require.Equal(t, "InternalFailure", result["Entries"].([]any)[0].(map[string]any)["ErrorCode"])
}

func TestCanonicalStorageFailureReturnsInternalException(t *testing.T) {
	p := newTestProvider(t)
	require.NoError(t, p.store.Close())
	for _, op := range []string{"DescribeEventSource", "ListEventSources", "ActivateEventSource", "DeleteEventBus", "DescribeEventBus"} {
		response := call(t, p, op, `{"Name":"aws.partner/devcloud/missing"}`)
		require.Equal(t, 500, response.StatusCode, op)
		require.Equal(t, "InternalException", parseJSON(t, response)["__type"])
	}
}
