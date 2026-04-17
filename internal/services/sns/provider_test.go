// SPDX-License-Identifier: Apache-2.0

// internal/services/sns/provider_test.go
package sns

import (
	"context"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/skyoo2003/devcloud/internal/plugin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestProvider(t *testing.T) *Provider {
	t.Helper()
	p := &Provider{}
	err := p.Init(plugin.PluginConfig{DataDir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { p.Shutdown(context.Background()) })
	return p
}

func handle(t *testing.T, p *Provider, formBody string) *plugin.Response {
	t.Helper()
	req := httptest.NewRequest("POST", "/", strings.NewReader(formBody))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := p.HandleRequest(context.Background(), "", req)
	require.NoError(t, err)
	return resp
}

func TestSNS_CreateTopic(t *testing.T) {
	p := newTestProvider(t)
	resp := handle(t, p, "Action=CreateTopic&Name=my-topic")
	assert.Equal(t, 200, resp.StatusCode)
	assert.Contains(t, string(resp.Body), "my-topic")
	assert.Contains(t, string(resp.Body), "TopicArn")
}

func TestSNS_CreateTopic_Idempotent(t *testing.T) {
	p := newTestProvider(t)
	resp1 := handle(t, p, "Action=CreateTopic&Name=idem-topic")
	resp2 := handle(t, p, "Action=CreateTopic&Name=idem-topic")
	assert.Equal(t, 200, resp1.StatusCode)
	assert.Equal(t, 200, resp2.StatusCode)
}

func TestSNS_ListTopics(t *testing.T) {
	p := newTestProvider(t)
	handle(t, p, "Action=CreateTopic&Name=topic-a")
	handle(t, p, "Action=CreateTopic&Name=topic-b")
	resp := handle(t, p, "Action=ListTopics")
	assert.Equal(t, 200, resp.StatusCode)
	body := string(resp.Body)
	assert.Contains(t, body, "topic-a")
	assert.Contains(t, body, "topic-b")
}

func TestSNS_DeleteTopic(t *testing.T) {
	p := newTestProvider(t)
	handle(t, p, "Action=CreateTopic&Name=delete-me")
	arn := "arn:aws:sns:us-east-1:000000000000:delete-me"
	resp := handle(t, p, "Action=DeleteTopic&TopicArn="+arn)
	assert.Equal(t, 200, resp.StatusCode)
	list := handle(t, p, "Action=ListTopics")
	assert.NotContains(t, string(list.Body), "delete-me")
}

func TestSNS_Subscribe_And_ListSubscriptions(t *testing.T) {
	p := newTestProvider(t)
	handle(t, p, "Action=CreateTopic&Name=sub-topic")
	topicARN := "arn:aws:sns:us-east-1:000000000000:sub-topic"
	resp := handle(t, p, "Action=Subscribe&TopicArn="+topicARN+"&Protocol=sqs&Endpoint=http://localhost:4747/000000000000/my-queue")
	assert.Equal(t, 200, resp.StatusCode)
	assert.Contains(t, string(resp.Body), "SubscriptionArn")

	list := handle(t, p, "Action=ListSubscriptions")
	assert.Equal(t, 200, list.StatusCode)
	assert.Contains(t, string(list.Body), "sub-topic")
}

func TestSNS_Publish_TopicNotFound(t *testing.T) {
	p := newTestProvider(t)
	resp := handle(t, p, "Action=Publish&TopicArn=arn:aws:sns:us-east-1:000000000000:ghost&Message=hello")
	assert.Equal(t, 400, resp.StatusCode)
}

func TestSNS_UnknownAction(t *testing.T) {
	p := newTestProvider(t)
	resp := handle(t, p, "Action=FooBar")
	assert.Equal(t, 400, resp.StatusCode)
	assert.Contains(t, string(resp.Body), "InvalidAction")
}

func TestSNS_ListResources(t *testing.T) {
	p := newTestProvider(t)
	handle(t, p, "Action=CreateTopic&Name=res-topic")
	resources, err := p.ListResources(context.Background())
	require.NoError(t, err)
	require.Len(t, resources, 1)
	assert.Equal(t, "topic", resources[0].Type)
	assert.Equal(t, "res-topic", resources[0].Name)
}

func TestSNS_StoreDir(t *testing.T) {
	dir := t.TempDir()
	p := &Provider{}
	err := p.Init(plugin.PluginConfig{DataDir: dir})
	require.NoError(t, err)
	defer p.Shutdown(context.Background())
	// DB file should exist inside DataDir/sns/
	_, err2 := filepath.Glob(filepath.Join(dir, "sns", "*.db"))
	require.NoError(t, err2)
}

func TestSNS_SetTopicAttributes(t *testing.T) {
	p := newTestProvider(t)
	handle(t, p, "Action=CreateTopic&Name=attr-topic")
	arn := "arn:aws:sns:us-east-1:000000000000:attr-topic"
	resp := handle(t, p, "Action=SetTopicAttributes&TopicArn="+arn+"&AttributeName=DisplayName&AttributeValue=MyTopic")
	assert.Equal(t, 200, resp.StatusCode)
	// Verify via GetTopicAttributes
	get := handle(t, p, "Action=GetTopicAttributes&TopicArn="+arn)
	assert.Contains(t, string(get.Body), "MyTopic")
}

func TestSNS_GetSubscriptionAttributes(t *testing.T) {
	p := newTestProvider(t)
	handle(t, p, "Action=CreateTopic&Name=sub-attr-topic")
	topicARN := "arn:aws:sns:us-east-1:000000000000:sub-attr-topic"
	subResp := handle(t, p, "Action=Subscribe&TopicArn="+topicARN+"&Protocol=sqs&Endpoint=arn:aws:sqs:us-east-1:000000000000:test-q")
	assert.Equal(t, 200, subResp.StatusCode)
	// Extract the sub ARN from the response body by looking at subscriptions
	listResp := handle(t, p, "Action=ListSubscriptions")
	body := string(listResp.Body)
	assert.Contains(t, body, topicARN)
	// Get subscription ARN from list
	subs, err := p.store.ListSubscriptions(defaultAccountID)
	require.NoError(t, err)
	require.NotEmpty(t, subs)
	attrResp := handle(t, p, "Action=GetSubscriptionAttributes&SubscriptionArn="+subs[0].ARN)
	assert.Equal(t, 200, attrResp.StatusCode)
	assert.Contains(t, string(attrResp.Body), "Protocol")
}

func TestSNS_AddRemovePermission(t *testing.T) {
	p := newTestProvider(t)
	handle(t, p, "Action=CreateTopic&Name=perm-topic")
	arn := "arn:aws:sns:us-east-1:000000000000:perm-topic"
	addResp := handle(t, p, "Action=AddPermission&TopicArn="+arn+"&Label=allow-account&AWSAccountId.member.1=000000000000&ActionName.member.1=Publish")
	assert.Equal(t, 200, addResp.StatusCode)
	removeResp := handle(t, p, "Action=RemovePermission&TopicArn="+arn+"&Label=allow-account")
	assert.Equal(t, 200, removeResp.StatusCode)
}

func TestSNS_TagResource(t *testing.T) {
	p := newTestProvider(t)
	handle(t, p, "Action=CreateTopic&Name=tagged-topic")
	arn := "arn:aws:sns:us-east-1:000000000000:tagged-topic"
	tagResp := handle(t, p, "Action=TagResource&ResourceArn="+arn+"&Tags.member.1.Key=env&Tags.member.1.Value=test")
	assert.Equal(t, 200, tagResp.StatusCode)
	listResp := handle(t, p, "Action=ListTagsForResource&ResourceArn="+arn)
	assert.Equal(t, 200, listResp.StatusCode)
	assert.Contains(t, string(listResp.Body), "env")
	untagResp := handle(t, p, "Action=UntagResource&ResourceArn="+arn+"&TagKeys.member.1=env")
	assert.Equal(t, 200, untagResp.StatusCode)
	listResp2 := handle(t, p, "Action=ListTagsForResource&ResourceArn="+arn)
	assert.NotContains(t, string(listResp2.Body), "env")
}

func TestSNS_ListPhoneNumbersOptedOut(t *testing.T) {
	p := newTestProvider(t)
	resp := handle(t, p, "Action=ListPhoneNumbersOptedOut")
	assert.Equal(t, 200, resp.StatusCode)
	assert.Contains(t, string(resp.Body), "phoneNumbers")
}

func TestSNS_CheckIfPhoneNumberIsOptedOut(t *testing.T) {
	p := newTestProvider(t)
	resp := handle(t, p, "Action=CheckIfPhoneNumberIsOptedOut&phoneNumber=%2B15555551234")
	assert.Equal(t, 200, resp.StatusCode)
	assert.Contains(t, string(resp.Body), "false")
}

func TestSNS_DataProtectionPolicy(t *testing.T) {
	p := newTestProvider(t)
	handle(t, p, "Action=CreateTopic&Name=dp-topic")
	arn := "arn:aws:sns:us-east-1:000000000000:dp-topic"
	policy := `{"Name":"test","Version":"2021-06-01","Statement":[]}`
	putResp := handle(t, p, "Action=PutDataProtectionPolicy&ResourceArn="+arn+"&DataProtectionPolicy="+policy)
	assert.Equal(t, 200, putResp.StatusCode)
	getResp := handle(t, p, "Action=GetDataProtectionPolicy&ResourceArn="+arn)
	assert.Equal(t, 200, getResp.StatusCode)
	assert.Contains(t, string(getResp.Body), "Statement")
}

func TestSNS_ConfirmSubscription(t *testing.T) {
	p := newTestProvider(t)
	handle(t, p, "Action=CreateTopic&Name=confirm-topic")
	topicARN := "arn:aws:sns:us-east-1:000000000000:confirm-topic"
	handle(t, p, "Action=Subscribe&TopicArn="+topicARN+"&Protocol=email&Endpoint=test@example.com")
	resp := handle(t, p, "Action=ConfirmSubscription&TopicArn="+topicARN+"&Token=dummytoken")
	assert.Equal(t, 200, resp.StatusCode)
	assert.Contains(t, string(resp.Body), "SubscriptionArn")
}
