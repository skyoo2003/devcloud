// SPDX-License-Identifier: Apache-2.0
package eventbridge

import (
	"encoding/json"
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func describedPolicy(t *testing.T, p *Provider, name string) map[string]any {
	t.Helper()
	body, _ := json.Marshal(map[string]any{"Name": name})
	response := call(t, p, "DescribeEventBus", string(body))
	require.Equal(t, 200, response.StatusCode)
	view := parseJSON(t, response)
	var policy map[string]any
	require.NoError(t, json.Unmarshal([]byte(view["Policy"].(string)), &policy))
	return policy
}

func TestPermissionOperationConcurrentWriteAndRemove(t *testing.T) {
	p := newTestProvider(t)
	for i := 0; i < 20; i++ {
		response, err := p.handlePermissionOperation("PutPermission", map[string]any{"StatementId": fmt.Sprint(i), "Action": "events:PutEvents", "Principal": "*"})
		require.NoError(t, err)
		require.Equal(t, 200, response.StatusCode)
	}
	var wg sync.WaitGroup
	results := make(chan int, 16)
	for i := 0; i < 8; i++ {
		wg.Add(2)
		go func(i int) {
			defer wg.Done()
			response, err := p.handlePermissionOperation("RemovePermission", map[string]any{"StatementId": fmt.Sprint(i)})
			if err != nil {
				results <- 0
			} else {
				results <- response.StatusCode
			}
		}(i)
		go func(i int) {
			defer wg.Done()
			response, err := p.handlePermissionOperation("PutPermission", map[string]any{"StatementId": fmt.Sprint(i + 20), "Action": "events:PutEvents", "Principal": "*"})
			if err != nil {
				results <- 0
			} else {
				results <- response.StatusCode
			}
		}(i)
	}
	wg.Wait()
	close(results)
	for status := range results {
		require.Equal(t, 200, status)
	}
	policy := describedPolicy(t, p, "default")
	require.Len(t, policy["Statement"], 20)
	ids := map[string]bool{}
	for _, value := range policy["Statement"].([]any) {
		ids[value.(map[string]any)["Sid"].(string)] = true
	}
	for i := 0; i < 28; i++ {
		require.Equal(t, i >= 8, ids[fmt.Sprint(i)])
	}
}

func TestPermissionOperationStatementLifecycle(t *testing.T) {
	p := newTestProvider(t)
	put := `{"StatementId":"first","Action":"events:PutEvents","Principal":"*","Condition":{"Type":"StringEquals","Key":"aws:PrincipalOrgID","Value":"o-local"}}`
	response := call(t, p, "PutPermission", put)
	require.Equal(t, 200, response.StatusCode)
	require.Empty(t, response.Body)
	policy := describedPolicy(t, p, "default")
	require.Equal(t, "2012-10-17", policy["Version"])
	require.Len(t, policy["Statement"], 1)
	statement := policy["Statement"].([]any)[0].(map[string]any)
	require.Equal(t, "first", statement["Sid"])
	require.Contains(t, statement, "Condition")
	require.Contains(t, statement["Resource"], "event-bus/default")
	require.Equal(t, 200, call(t, p, "PutPermission", `{"StatementId":"first","Action":"events:PutEvents","Principal":"123456789012"}`).StatusCode)
	policy = describedPolicy(t, p, "default")
	require.Len(t, policy["Statement"], 1)
	require.Equal(t, map[string]any{"AWS": "123456789012"}, policy["Statement"].([]any)[0].(map[string]any)["Principal"])
	require.Equal(t, 200, call(t, p, "PutPermission", `{"StatementId":"second","Action":"events:PutEvents","Principal":"*"}`).StatusCode)
	response = call(t, p, "RemovePermission", `{"StatementId":"first"}`)
	require.Equal(t, 200, response.StatusCode)
	require.Empty(t, response.Body)
	policy = describedPolicy(t, p, "default")
	require.Len(t, policy["Statement"], 1)
	require.Equal(t, "second", policy["Statement"].([]any)[0].(map[string]any)["Sid"])
	require.Equal(t, 400, call(t, p, "RemovePermission", `{"StatementId":"missing"}`).StatusCode)
	response = call(t, p, "RemovePermission", `{"RemoveAllPermissions":true}`)
	require.Equal(t, 200, response.StatusCode)
	require.Empty(t, response.Body)
	require.NotContains(t, parseJSON(t, call(t, p, "DescribeEventBus", `{}`)), "Policy")
	require.Equal(t, 200, call(t, p, "RemovePermission", `{"RemoveAllPermissions":true}`).StatusCode)
}

func TestPermissionOperationPolicyFormsAndBusIsolation(t *testing.T) {
	p := newTestProvider(t)
	require.Equal(t, 200, call(t, p, "CreateEventBus", `{"Name":"custom"}`).StatusCode)
	raw := `{"Version":"2012-10-17","Id":"preserved","Statement":{"Sid":"manual","Effect":"Allow","Action":"events:PutEvents","Principal":{"AWS":"*"},"Resource":"custom-resource","Condition":{"StringEquals":{"custom":"value"}}}}`
	body, _ := json.Marshal(map[string]any{"EventBusName": "custom", "Policy": raw})
	require.Equal(t, 200, call(t, p, "PutPermission", string(body)).StatusCode)
	policy := describedPolicy(t, p, "custom")
	require.Equal(t, "preserved", policy["Id"])
	require.Len(t, policy["Statement"], 1)
	require.Contains(t, policy["Statement"].([]any)[0], "Condition")
	require.NotContains(t, parseJSON(t, call(t, p, "DescribeEventBus", `{}`)), "Policy")
	require.Equal(t, 400, call(t, p, "RemovePermission", `{"StatementId":"manual"}`).StatusCode)
	require.Equal(t, 200, call(t, p, "RemovePermission", `{"EventBusName":"custom","StatementId":"manual"}`).StatusCode)
	require.NotContains(t, parseJSON(t, call(t, p, "DescribeEventBus", `{"Name":"custom"}`)), "Policy")
	require.Equal(t, 200, call(t, p, "PutPermission", string(body)).StatusCode)
	require.Equal(t, 200, call(t, p, "DeleteEventBus", `{"Name":"custom"}`).StatusCode)
	require.Equal(t, 200, call(t, p, "CreateEventBus", `{"Name":"custom"}`).StatusCode)
	require.NotContains(t, parseJSON(t, call(t, p, "DescribeEventBus", `{"Name":"custom"}`)), "Policy")
}

func TestPermissionOperationInvalidInputPreservesPolicy(t *testing.T) {
	p := newTestProvider(t)
	require.Equal(t, 200, call(t, p, "PutPermission", `{"StatementId":"keep","Action":"events:PutEvents","Principal":"*"}`).StatusCode)
	before := describedPolicy(t, p, "default")
	for _, body := range []string{`{}`, `{"StatementId":"bad","Action":"invalid","Principal":"*"}`, `{"StatementId":"bad","Action":"events:PutEvents","Principal":"not-an-account"}`, `{"StatementId":"bad","Action":"events:PutEvents","Principal":"*","Condition":{"Type":"StringEquals"}}`, `{"StatementId":"bad","Policy":"{}"}`, `{"Policy":"{bad"}`, `{"Policy":"[]"}`, `{"Policy":"{\"Statement\":[]}"}`, `{"Policy":"{\"Statement\":[{\"Sid\":\"duplicate\",\"Action\":\"events:PutEvents\",\"Principal\":\"*\"},{\"Sid\":\"duplicate\",\"Action\":\"events:PutEvents\",\"Principal\":\"*\"}]}"}`} {
		require.Equal(t, 400, call(t, p, "PutPermission", body).StatusCode, body)
		require.Equal(t, before, describedPolicy(t, p, "default"))
	}
	for _, body := range []string{`{}`, `{"RemoveAllPermissions":false}`, `{"StatementId":"keep","RemoveAllPermissions":true}`, `{"RemoveAllPermissions":"true"}`, `{"EventBusName":"missing","StatementId":"keep"}`} {
		require.Equal(t, 400, call(t, p, "RemovePermission", body).StatusCode, body)
		require.Equal(t, before, describedPolicy(t, p, "default"))
	}
}

func TestPermissionOperationStorageFailure(t *testing.T) {
	p := newTestProvider(t)
	require.NoError(t, p.store.Close())
	for _, item := range []struct{ op, body string }{{"PutPermission", `{"StatementId":"sid","Action":"events:PutEvents","Principal":"*"}`}, {"RemovePermission", `{"StatementId":"sid"}`}} {
		response := call(t, p, item.op, item.body)
		require.Equal(t, 500, response.StatusCode)
		require.Equal(t, "InternalException", parseJSON(t, response)["__type"])
	}
}
