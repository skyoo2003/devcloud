// SPDX-License-Identifier: Apache-2.0

// internal/services/sns/provider.go
package sns

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"net/http"
	"path/filepath"
	"strings"

	"github.com/skyoo2003/devcloud/internal/plugin"
)

const defaultAccountID = plugin.DefaultAccountID
const defaultRegion = "us-east-1"

type Provider struct {
	store *SNSStore
}

func (p *Provider) ServiceID() string             { return "sns" }
func (p *Provider) ServiceName() string           { return "SimpleNotificationService" }
func (p *Provider) Protocol() plugin.ProtocolType { return plugin.ProtocolQuery }

func (p *Provider) Init(cfg plugin.PluginConfig) error {
	dataDir := cfg.DataDir
	if dataDir == "" {
		dataDir = "."
	}
	var err error
	p.store, err = NewSNSStore(filepath.Join(dataDir, "sns"))
	return err
}

func (p *Provider) Shutdown(_ context.Context) error {
	if p.store != nil {
		return p.store.Close()
	}
	return nil
}

func (p *Provider) HandleRequest(_ context.Context, op string, req *http.Request) (*plugin.Response, error) {
	if err := req.ParseForm(); err != nil {
		return snsError("InvalidParameterValue", "failed to parse form body", http.StatusBadRequest), nil
	}
	action := op
	if action == "" {
		action = req.FormValue("Action")
	}
	switch action {
	case "CreateTopic":
		return p.createTopic(req)
	case "DeleteTopic":
		return p.deleteTopic(req)
	case "ListTopics":
		return p.listTopics(req)
	case "Subscribe":
		return p.subscribe(req)
	case "Unsubscribe":
		return p.unsubscribe(req)
	case "ListSubscriptions":
		return p.listSubscriptions(req)
	case "ListSubscriptionsByTopic":
		return p.listSubscriptionsByTopic(req)
	case "Publish":
		return p.publish(req)
	case "GetTopicAttributes":
		return p.getTopicAttributes(req)
	case "SetSubscriptionAttributes":
		return p.setSubscriptionAttributes(req)
	case "SetTopicAttributes":
		return p.setTopicAttributes(req)
	case "GetSubscriptionAttributes":
		return p.getSubscriptionAttributes(req)
	case "AddPermission":
		return p.addPermission(req)
	case "RemovePermission":
		return p.removePermission(req)
	case "TagResource":
		return p.tagResource(req)
	case "UntagResource":
		return p.untagResource(req)
	case "ListTagsForResource":
		return p.listTagsForResource(req)
	case "ListPhoneNumbersOptedOut":
		return p.listPhoneNumbersOptedOut(req)
	case "CheckIfPhoneNumberIsOptedOut":
		return p.checkIfPhoneNumberIsOptedOut(req)
	case "PutDataProtectionPolicy":
		return p.putDataProtectionPolicy(req)
	case "GetDataProtectionPolicy":
		return p.getDataProtectionPolicy(req)
	case "ConfirmSubscription":
		return p.confirmSubscription(req)
	default:
		return snsError("InvalidAction", fmt.Sprintf("unknown action: %s", action), http.StatusBadRequest), nil
	}
}

func (p *Provider) ListResources(_ context.Context) ([]plugin.Resource, error) {
	topics, err := p.store.ListTopics(defaultAccountID)
	if err != nil {
		return nil, err
	}
	resources := make([]plugin.Resource, 0, len(topics))
	for _, t := range topics {
		resources = append(resources, plugin.Resource{Type: "topic", ID: t.ARN, Name: t.Name})
	}
	return resources, nil
}

func (p *Provider) GetMetrics(_ context.Context) (*plugin.ServiceMetrics, error) {
	return &plugin.ServiceMetrics{}, nil
}

// --- operation handlers ---

func (p *Provider) createTopic(req *http.Request) (*plugin.Response, error) {
	name := req.FormValue("Name")
	if name == "" {
		return snsError("InvalidParameter", "Name is required", http.StatusBadRequest), nil
	}
	arn := fmt.Sprintf("arn:aws:sns:%s:%s:%s", defaultRegion, defaultAccountID, name)
	t, err := p.store.CreateTopic(arn, name, defaultAccountID)
	if err != nil {
		return nil, err
	}
	type result struct {
		TopicArn string `xml:"TopicArn"`
	}
	type response struct {
		XMLName xml.Name `xml:"CreateTopicResponse"`
		Result  result   `xml:"CreateTopicResult"`
	}
	return xmlResp(http.StatusOK, response{Result: result{TopicArn: t.ARN}})
}

func (p *Provider) deleteTopic(req *http.Request) (*plugin.Response, error) {
	arn := req.FormValue("TopicArn")
	if arn == "" {
		return snsError("InvalidParameter", "TopicArn is required", http.StatusBadRequest), nil
	}
	if err := p.store.DeleteTopic(arn); err != nil {
		if err == ErrTopicNotFound {
			return snsError("NotFound", "topic not found", http.StatusBadRequest), nil
		}
		return nil, err
	}
	type response struct {
		XMLName xml.Name `xml:"DeleteTopicResponse"`
	}
	return xmlResp(http.StatusOK, response{})
}

func (p *Provider) listTopics(req *http.Request) (*plugin.Response, error) {
	topics, err := p.store.ListTopics(defaultAccountID)
	if err != nil {
		return nil, err
	}
	type member struct {
		TopicArn string `xml:"TopicArn"`
	}
	type result struct {
		Topics []member `xml:"Topics>member"`
	}
	type response struct {
		XMLName xml.Name `xml:"ListTopicsResponse"`
		Result  result   `xml:"ListTopicsResult"`
	}
	resp := response{}
	for _, t := range topics {
		resp.Result.Topics = append(resp.Result.Topics, member{TopicArn: t.ARN})
	}
	return xmlResp(http.StatusOK, resp)
}

func (p *Provider) subscribe(req *http.Request) (*plugin.Response, error) {
	topicARN := req.FormValue("TopicArn")
	protocol := req.FormValue("Protocol")
	endpoint := req.FormValue("Endpoint")
	if topicARN == "" || protocol == "" {
		return snsError("InvalidParameter", "TopicArn and Protocol are required", http.StatusBadRequest), nil
	}
	subARN := fmt.Sprintf("arn:aws:sns:%s:%s:%s:%s", defaultRegion, defaultAccountID, topicARNName(topicARN), randomID(8))
	sub, err := p.store.Subscribe(subARN, topicARN, protocol, endpoint, defaultAccountID)
	if err != nil {
		return nil, err
	}
	type result struct {
		SubscriptionArn string `xml:"SubscriptionArn"`
	}
	type response struct {
		XMLName xml.Name `xml:"SubscribeResponse"`
		Result  result   `xml:"SubscribeResult"`
	}
	return xmlResp(http.StatusOK, response{Result: result{SubscriptionArn: sub.ARN}})
}

func (p *Provider) unsubscribe(req *http.Request) (*plugin.Response, error) {
	arn := req.FormValue("SubscriptionArn")
	if arn == "" {
		return snsError("InvalidParameter", "SubscriptionArn is required", http.StatusBadRequest), nil
	}
	if err := p.store.Unsubscribe(arn); err != nil {
		return snsError("NotFound", "subscription not found", http.StatusBadRequest), nil
	}
	type response struct {
		XMLName xml.Name `xml:"UnsubscribeResponse"`
	}
	return xmlResp(http.StatusOK, response{})
}

func (p *Provider) listSubscriptions(req *http.Request) (*plugin.Response, error) {
	subs, err := p.store.ListSubscriptions(defaultAccountID)
	if err != nil {
		return nil, err
	}
	return xmlSubscriptionList("ListSubscriptionsResponse", "ListSubscriptionsResult", subs)
}

func (p *Provider) listSubscriptionsByTopic(req *http.Request) (*plugin.Response, error) {
	topicARN := req.FormValue("TopicArn")
	if topicARN == "" {
		return snsError("InvalidParameter", "TopicArn is required", http.StatusBadRequest), nil
	}
	subs, err := p.store.ListSubscriptionsByTopic(topicARN)
	if err != nil {
		return nil, err
	}
	return xmlSubscriptionList("ListSubscriptionsByTopicResponse", "ListSubscriptionsByTopicResult", subs)
}

func (p *Provider) publish(req *http.Request) (*plugin.Response, error) {
	topicARN := req.FormValue("TopicArn")
	message := req.FormValue("Message")
	if topicARN == "" || message == "" {
		return snsError("InvalidParameter", "TopicArn and Message are required", http.StatusBadRequest), nil
	}
	// Verify topic exists.
	if _, err := p.store.GetTopic(topicARN); err != nil {
		return snsError("NotFound", "topic not found", http.StatusBadRequest), nil
	}
	// SQS fanout: deliver to sqs subscriptions via registry.
	subs, _ := p.store.ListSubscriptionsByTopic(topicARN)
	for _, sub := range subs {
		if sub.Protocol == "sqs" {
			p.fanoutToSQS(sub.Endpoint, message)
		}
	}
	msgID := randomID(16)
	type result struct {
		MessageId string `xml:"MessageId"`
	}
	type response struct {
		XMLName xml.Name `xml:"PublishResponse"`
		Result  result   `xml:"PublishResult"`
	}
	return xmlResp(http.StatusOK, response{Result: result{MessageId: msgID}})
}

func (p *Provider) fanoutToSQS(queueURL, message string) {
	svc, ok := plugin.DefaultRegistry.Get("sqs")
	if !ok {
		return
	}
	body := fmt.Sprintf("Action=SendMessage&QueueUrl=%s&MessageBody=%s", queueURL, message)
	req, err := http.NewRequest("POST", "/", strings.NewReader(body))
	if err != nil {
		return
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	svc.HandleRequest(context.Background(), "SendMessage", req) //nolint:errcheck
}

func (p *Provider) getTopicAttributes(req *http.Request) (*plugin.Response, error) {
	arn := req.FormValue("TopicArn")
	if arn == "" {
		return snsError("InvalidParameter", "TopicArn is required", http.StatusBadRequest), nil
	}
	topic, err := p.store.GetTopic(arn)
	if err != nil {
		if err == ErrTopicNotFound {
			return snsError("NotFound", "topic not found", http.StatusBadRequest), nil
		}
		return nil, err
	}
	// Count confirmed subscriptions
	subs, _ := p.store.ListSubscriptionsByTopic(arn)
	confirmedCount := 0
	for _, s := range subs {
		if s.Confirmed {
			confirmedCount++
		}
	}

	type entry struct {
		Key   string `xml:"key"`
		Value string `xml:"value"`
	}
	type result struct {
		Attributes []entry `xml:"Attributes>entry"`
	}
	type response struct {
		XMLName xml.Name `xml:"GetTopicAttributesResponse"`
		Result  result   `xml:"GetTopicAttributesResult"`
	}
	attrs := []entry{
		{Key: "TopicArn", Value: topic.ARN},
		{Key: "DisplayName", Value: topic.Name},
		{Key: "Owner", Value: topic.AccountID},
		{Key: "Policy", Value: ""},
		{Key: "SubscriptionsConfirmed", Value: fmt.Sprintf("%d", confirmedCount)},
		{Key: "SubscriptionsPending", Value: fmt.Sprintf("%d", len(subs)-confirmedCount)},
		{Key: "SubscriptionsDeleted", Value: "0"},
	}
	// Include any custom attributes stored on the topic
	for k, v := range topic.Attributes {
		attrs = append(attrs, entry{Key: k, Value: v})
	}
	return xmlResp(http.StatusOK, response{Result: result{Attributes: attrs}})
}

func (p *Provider) setSubscriptionAttributes(req *http.Request) (*plugin.Response, error) {
	subARN := req.FormValue("SubscriptionArn")
	attrName := req.FormValue("AttributeName")
	attrValue := req.FormValue("AttributeValue")
	if subARN == "" || attrName == "" {
		return snsError("InvalidParameter", "SubscriptionArn and AttributeName are required", http.StatusBadRequest), nil
	}
	if err := p.store.SetSubscriptionAttribute(subARN, attrName, attrValue); err != nil {
		if err == ErrSubscriptionNotFound {
			return snsError("NotFound", "subscription not found", http.StatusBadRequest), nil
		}
		return nil, err
	}
	type response struct {
		XMLName xml.Name `xml:"SetSubscriptionAttributesResponse"`
	}
	return xmlResp(http.StatusOK, response{})
}

func (p *Provider) setTopicAttributes(req *http.Request) (*plugin.Response, error) {
	arn := req.FormValue("TopicArn")
	attrName := req.FormValue("AttributeName")
	attrValue := req.FormValue("AttributeValue")
	if arn == "" || attrName == "" {
		return snsError("InvalidParameter", "TopicArn and AttributeName are required", http.StatusBadRequest), nil
	}
	if err := p.store.SetTopicAttribute(arn, attrName, attrValue); err != nil {
		if err == ErrTopicNotFound {
			return snsError("NotFound", "topic not found", http.StatusBadRequest), nil
		}
		return nil, err
	}
	type response struct {
		XMLName xml.Name `xml:"SetTopicAttributesResponse"`
	}
	return xmlResp(http.StatusOK, response{})
}

func (p *Provider) getSubscriptionAttributes(req *http.Request) (*plugin.Response, error) {
	subARN := req.FormValue("SubscriptionArn")
	if subARN == "" {
		return snsError("InvalidParameter", "SubscriptionArn is required", http.StatusBadRequest), nil
	}
	attrs, sub, err := p.store.GetSubscriptionAttributes(subARN)
	if err != nil {
		if err == ErrSubscriptionNotFound {
			return snsError("NotFound", "subscription not found", http.StatusBadRequest), nil
		}
		return nil, err
	}
	// Always include built-in attributes
	type entry struct {
		Key   string `xml:"key"`
		Value string `xml:"value"`
	}
	type result struct {
		Attributes []entry `xml:"Attributes>entry"`
	}
	type response struct {
		XMLName xml.Name `xml:"GetSubscriptionAttributesResponse"`
		Result  result   `xml:"GetSubscriptionAttributesResult"`
	}
	entries := []entry{
		{Key: "SubscriptionArn", Value: sub.ARN},
		{Key: "TopicArn", Value: sub.TopicARN},
		{Key: "Protocol", Value: sub.Protocol},
		{Key: "Endpoint", Value: sub.Endpoint},
		{Key: "Owner", Value: sub.AccountID},
	}
	for k, v := range attrs {
		entries = append(entries, entry{Key: k, Value: v})
	}
	return xmlResp(http.StatusOK, response{Result: result{Attributes: entries}})
}

func (p *Provider) addPermission(req *http.Request) (*plugin.Response, error) {
	topicARN := req.FormValue("TopicArn")
	label := req.FormValue("Label")
	if topicARN == "" || label == "" {
		return snsError("InvalidParameter", "TopicArn and Label are required", http.StatusBadRequest), nil
	}
	// Collect list params: AWSAccountId.member.1, AWSAccountId.member.2, ...
	var accountIDs []string
	for i := 1; ; i++ {
		v := req.FormValue(fmt.Sprintf("AWSAccountId.member.%d", i))
		if v == "" {
			break
		}
		accountIDs = append(accountIDs, v)
	}
	var actions []string
	for i := 1; ; i++ {
		v := req.FormValue(fmt.Sprintf("ActionName.member.%d", i))
		if v == "" {
			break
		}
		actions = append(actions, v)
	}
	if err := p.store.AddPermission(topicARN, label, accountIDs, actions); err != nil {
		return nil, err
	}
	type response struct {
		XMLName xml.Name `xml:"AddPermissionResponse"`
	}
	return xmlResp(http.StatusOK, response{})
}

func (p *Provider) removePermission(req *http.Request) (*plugin.Response, error) {
	topicARN := req.FormValue("TopicArn")
	label := req.FormValue("Label")
	if topicARN == "" || label == "" {
		return snsError("InvalidParameter", "TopicArn and Label are required", http.StatusBadRequest), nil
	}
	if err := p.store.RemovePermission(topicARN, label); err != nil {
		return nil, err
	}
	type response struct {
		XMLName xml.Name `xml:"RemovePermissionResponse"`
	}
	return xmlResp(http.StatusOK, response{})
}

func (p *Provider) tagResource(req *http.Request) (*plugin.Response, error) {
	resourceARN := req.FormValue("ResourceArn")
	if resourceARN == "" {
		return snsError("InvalidParameter", "ResourceArn is required", http.StatusBadRequest), nil
	}
	tags := map[string]string{}
	for i := 1; ; i++ {
		k := req.FormValue(fmt.Sprintf("Tags.member.%d.Key", i))
		v := req.FormValue(fmt.Sprintf("Tags.member.%d.Value", i))
		if k == "" {
			break
		}
		tags[k] = v
	}
	if err := p.store.TagResource(resourceARN, tags); err != nil {
		return nil, err
	}
	type result struct {
		XMLName xml.Name `xml:"TagResourceResult"`
	}
	type response struct {
		XMLName           xml.Name `xml:"TagResourceResponse"`
		TagResourceResult result   `xml:"TagResourceResult"`
	}
	return xmlResp(http.StatusOK, response{})
}

func (p *Provider) untagResource(req *http.Request) (*plugin.Response, error) {
	resourceARN := req.FormValue("ResourceArn")
	if resourceARN == "" {
		return snsError("InvalidParameter", "ResourceArn is required", http.StatusBadRequest), nil
	}
	var keys []string
	for i := 1; ; i++ {
		k := req.FormValue(fmt.Sprintf("TagKeys.member.%d", i))
		if k == "" {
			break
		}
		keys = append(keys, k)
	}
	if err := p.store.UntagResource(resourceARN, keys); err != nil {
		return nil, err
	}
	type untagResult struct {
		XMLName xml.Name `xml:"UntagResourceResult"`
	}
	type response struct {
		XMLName             xml.Name    `xml:"UntagResourceResponse"`
		UntagResourceResult untagResult `xml:"UntagResourceResult"`
	}
	return xmlResp(http.StatusOK, response{})
}

func (p *Provider) listTagsForResource(req *http.Request) (*plugin.Response, error) {
	resourceARN := req.FormValue("ResourceArn")
	if resourceARN == "" {
		return snsError("InvalidParameter", "ResourceArn is required", http.StatusBadRequest), nil
	}
	tags, err := p.store.ListTagsForResource(resourceARN)
	if err != nil {
		return nil, err
	}
	type tagMember struct {
		Key   string `xml:"Key"`
		Value string `xml:"Value"`
	}
	type result struct {
		Tags []tagMember `xml:"Tags>member"`
	}
	type response struct {
		XMLName xml.Name `xml:"ListTagsForResourceResponse"`
		Result  result   `xml:"ListTagsForResourceResult"`
	}
	resp := response{}
	for k, v := range tags {
		resp.Result.Tags = append(resp.Result.Tags, tagMember{Key: k, Value: v})
	}
	return xmlResp(http.StatusOK, resp)
}

func (p *Provider) listPhoneNumbersOptedOut(_ *http.Request) (*plugin.Response, error) {
	type result struct {
		PhoneNumbers []string `xml:"phoneNumbers>member"`
	}
	type response struct {
		XMLName xml.Name `xml:"ListPhoneNumbersOptedOutResponse"`
		Result  result   `xml:"ListPhoneNumbersOptedOutResult"`
	}
	return xmlResp(http.StatusOK, response{Result: result{PhoneNumbers: []string{}}})
}

func (p *Provider) checkIfPhoneNumberIsOptedOut(_ *http.Request) (*plugin.Response, error) {
	type result struct {
		IsOptedOut bool `xml:"isOptedOut"`
	}
	type response struct {
		XMLName xml.Name `xml:"CheckIfPhoneNumberIsOptedOutResponse"`
		Result  result   `xml:"CheckIfPhoneNumberIsOptedOutResult"`
	}
	return xmlResp(http.StatusOK, response{Result: result{IsOptedOut: false}})
}

func (p *Provider) putDataProtectionPolicy(req *http.Request) (*plugin.Response, error) {
	resourceARN := req.FormValue("ResourceArn")
	policy := req.FormValue("DataProtectionPolicy")
	if resourceARN == "" {
		return snsError("InvalidParameter", "ResourceArn is required", http.StatusBadRequest), nil
	}
	if err := p.store.PutDataProtectionPolicy(resourceARN, policy); err != nil {
		return nil, err
	}
	type response struct {
		XMLName xml.Name `xml:"PutDataProtectionPolicyResponse"`
	}
	return xmlResp(http.StatusOK, response{})
}

func (p *Provider) getDataProtectionPolicy(req *http.Request) (*plugin.Response, error) {
	resourceARN := req.FormValue("ResourceArn")
	if resourceARN == "" {
		return snsError("InvalidParameter", "ResourceArn is required", http.StatusBadRequest), nil
	}
	policy, err := p.store.GetDataProtectionPolicy(resourceARN)
	if err != nil {
		return nil, err
	}
	type result struct {
		DataProtectionPolicy string `xml:"DataProtectionPolicy"`
	}
	type response struct {
		XMLName xml.Name `xml:"GetDataProtectionPolicyResponse"`
		Result  result   `xml:"GetDataProtectionPolicyResult"`
	}
	return xmlResp(http.StatusOK, response{Result: result{DataProtectionPolicy: policy}})
}

func (p *Provider) confirmSubscription(req *http.Request) (*plugin.Response, error) {
	topicARN := req.FormValue("TopicArn")
	token := req.FormValue("Token")
	if topicARN == "" || token == "" {
		return snsError("InvalidParameter", "TopicArn and Token are required", http.StatusBadRequest), nil
	}
	// For dev/test: find a pending subscription for this topic and confirm it.
	subs, err := p.store.ListSubscriptionsByTopic(topicARN)
	if err != nil {
		return nil, err
	}
	// Pick the first unconfirmed subscription (token matching not required for dev emulator).
	subARN := "pending confirmation"
	for _, sub := range subs {
		if !sub.Confirmed {
			if err := p.store.ConfirmSubscription(sub.ARN); err != nil {
				return nil, err
			}
			subARN = sub.ARN
			break
		}
	}
	type result struct {
		SubscriptionArn string `xml:"SubscriptionArn"`
	}
	type response struct {
		XMLName xml.Name `xml:"ConfirmSubscriptionResponse"`
		Result  result   `xml:"ConfirmSubscriptionResult"`
	}
	return xmlResp(http.StatusOK, response{Result: result{SubscriptionArn: subARN}})
}

// --- helpers ---

func randomID(n int) string {
	b := make([]byte, n)
	rand.Read(b) //nolint:errcheck
	return hex.EncodeToString(b)
}

func topicARNName(arn string) string {
	parts := strings.Split(arn, ":")
	if len(parts) > 0 {
		return parts[len(parts)-1]
	}
	return arn
}

func xmlResp(status int, v any) (*plugin.Response, error) {
	body, err := xml.Marshal(v)
	if err != nil {
		return nil, err
	}
	return &plugin.Response{StatusCode: status, ContentType: "text/xml", Body: body}, nil
}

type snsErrorResponse struct {
	XMLName xml.Name `xml:"ErrorResponse"`
	Error   struct {
		Code    string `xml:"Code"`
		Message string `xml:"Message"`
	} `xml:"Error"`
}

func snsError(code, message string, status int) *plugin.Response {
	e := snsErrorResponse{}
	e.Error.Code = code
	e.Error.Message = message
	body, _ := xml.Marshal(e)
	return &plugin.Response{StatusCode: status, ContentType: "text/xml", Body: body}
}

type subMember struct {
	SubscriptionArn string `xml:"SubscriptionArn"`
	TopicArn        string `xml:"TopicArn"`
	Protocol        string `xml:"Protocol"`
	Endpoint        string `xml:"Endpoint"`
	Owner           string `xml:"Owner"`
}

func xmlSubscriptionList(respTag, resultTag string, subs []Subscription) (*plugin.Response, error) {
	members := make([]subMember, 0, len(subs))
	for _, s := range subs {
		members = append(members, subMember{
			SubscriptionArn: s.ARN,
			TopicArn:        s.TopicARN,
			Protocol:        s.Protocol,
			Endpoint:        s.Endpoint,
			Owner:           s.AccountID,
		})
	}
	// Build <Subscriptions><member>...</member></Subscriptions> without extra wrapper element.
	type subsXML struct {
		XMLName xml.Name    `xml:"Subscriptions"`
		Members []subMember `xml:"member"`
	}
	innerBytes, err := xml.Marshal(subsXML{Members: members})
	if err != nil {
		return nil, err
	}
	xmlBytes := []byte(fmt.Sprintf("<%s><%s>%s</%s></%s>", respTag, resultTag, string(innerBytes), resultTag, respTag))
	return &plugin.Response{StatusCode: http.StatusOK, ContentType: "text/xml", Body: xmlBytes}, nil
}
