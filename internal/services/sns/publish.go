// SPDX-License-Identifier: Apache-2.0
package sns

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"github.com/skyoo2003/devcloud/internal/plugin"
	"math"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"
)

var batchEntryID = regexp.MustCompile(`^[A-Za-z0-9_-]{1,80}$`)
var messageAttributeName = regexp.MustCompile(`^[A-Za-z0-9_.-]{1,256}$`)
var numberAttribute = regexp.MustCompile(`^[+-]?([0-9]+(\.[0-9]*)?|\.[0-9]+)([eE][+-]?[0-9]+)?$`)

func parsePublishEntry(form url.Values) (PublishEntry, error) {
	entry := PublishEntry{ID: form.Get("Id"), Message: form.Get("Message"), Subject: form.Get("Subject"), MessageStructure: form.Get("MessageStructure"), MessageGroupID: form.Get("MessageGroupId"), MessageDeduplicationID: form.Get("MessageDeduplicationId"), Attributes: map[string]PublishAttribute{}}
	attrs, e := indexedForms(form, "MessageAttributes.entry.", 10000)
	if e != nil {
		return entry, e
	}
	for _, v := range attrs {
		k := v.Get("Name")
		if _, ok := entry.Attributes[k]; ok {
			return entry, invalidSNS("duplicate message attribute")
		}
		attribute := PublishAttribute{DataType: v.Get("Value.DataType"), StringValue: v.Get("Value.StringValue")}
		if _, ok := v["Value.BinaryValue"]; ok {
			attribute.BinaryValue, e = base64.StdEncoding.DecodeString(v.Get("Value.BinaryValue"))
			if e != nil {
				return entry, invalidSNS("invalid binary attribute")
			}
		}
		entry.Attributes[k] = attribute
	}
	if e = validatePublishEntry(entry); e != nil {
		return entry, e
	}
	return entry, nil
}
func publishPayloadBytes(entry PublishEntry) int {
	size := len(entry.Message)
	for k, v := range entry.Attributes {
		size += len(k) + len(v.DataType) + len(v.StringValue) + len(v.BinaryValue)
	}
	return size
}

// Count submitted attributes even when parsing stops at an invalid entry.
func publishFormPayloadBytes(form url.Values) int {
	size := len(form.Get("Message"))
	for k, values := range form {
		if !strings.HasPrefix(k, "MessageAttributes.entry.") {
			continue
		}
		for _, v := range values {
			switch {
			case strings.HasSuffix(k, ".Name"), strings.HasSuffix(k, ".Value.DataType"), strings.HasSuffix(k, ".Value.StringValue"):
				size += len(v)
			case strings.HasSuffix(k, ".Value.BinaryValue"):
				decoded, e := base64.StdEncoding.DecodeString(v)
				if e != nil {
					size += len(v)
				} else {
					size += len(decoded)
				}
			}
		}
	}
	return size
}
func validatePublishEntry(entry PublishEntry) error {
	if entry.Message == "" || !utf8.ValidString(entry.Message) {
		return invalidSNS("Message must be nonempty UTF-8")
	}
	if !utf8.ValidString(entry.Subject) || utf8.RuneCountInString(entry.Subject) >= 100 {
		return invalidSNS("Subject must have fewer than 100 UTF-8 characters")
	}
	for _, c := range entry.Subject {
		if unicode.IsControl(c) {
			return invalidSNS("Subject cannot contain control characters")
		}
	}
	if _, e := sqsPublishBody(entry); e != nil {
		return e
	}
	for k, a := range entry.Attributes {
		lower := strings.ToLower(k)
		if !messageAttributeName.MatchString(k) || strings.HasPrefix(lower, "aws.") || strings.HasPrefix(lower, "amazon.") || strings.HasPrefix(k, ".") || strings.HasSuffix(k, ".") || strings.Contains(k, "..") {
			return invalidSNS("invalid message attribute name")
		}
		base := strings.SplitN(a.DataType, ".", 2)[0]
		switch base {
		case "Binary":
			if len(a.BinaryValue) == 0 || a.StringValue != "" {
				return invalidSNS("binary attribute value required")
			}
		case "String", "Number":
			if a.StringValue == "" || !utf8.ValidString(a.StringValue) || len(a.BinaryValue) != 0 {
				return invalidSNS("string attribute value required")
			}
			if base == "Number" {
				n, e := strconv.ParseFloat(a.StringValue, 64)
				if !numberAttribute.MatchString(a.StringValue) || e != nil || math.IsInf(n, 0) || math.IsNaN(n) {
					return invalidSNS("invalid number attribute")
				}
			}
			if a.DataType == "String.Array" {
				var v []any
				if json.Unmarshal([]byte(a.StringValue), &v) != nil || v == nil {
					return invalidSNS("invalid String.Array")
				}
			}
		default:
			return invalidSNS("invalid message attribute DataType")
		}
	}
	return nil
}
func sqsPublishBody(entry PublishEntry) (string, error) {
	if entry.MessageStructure == "" {
		return entry.Message, nil
	}
	if entry.MessageStructure != "json" {
		return "", invalidSNS("MessageStructure must be json")
	}
	var values map[string]any
	if json.Unmarshal([]byte(entry.Message), &values) != nil || values == nil {
		return "", invalidSNS("invalid JSON MessageStructure")
	}
	messages := map[string]string{}
	for k, v := range values {
		text, ok := v.(string)
		if !ok {
			return "", invalidSNS("JSON protocol values must be strings")
		}
		messages[k] = text
	}
	if messages["default"] == "" {
		return "", invalidSNS("JSON default required")
	}
	if body, ok := messages["sqs"]; ok {
		if body == "" {
			return "", invalidSNS("sqs message must be nonempty")
		}
		return body, nil
	}
	return messages["default"], nil
}

func (p *Provider) publishEntry(ctx context.Context, topicARN string, entry PublishEntry) (*Publication, error) {
	return p.store.PublishMessage(topicARN, entry, time.Now().UTC(), func(publication Publication, subs []Subscription) error {
		var failures []error
		for _, s := range subs {
			if s.Protocol == "sqs" {
				if e := p.fanoutPublicationToSQS(ctx, s.Endpoint, entry, publication); e != nil {
					failures = append(failures, e)
				}
			}
		}
		return errors.Join(failures...)
	})
}

type publishedMember struct {
	ID             string `xml:"Id,omitempty"`
	MessageID      string `xml:"MessageId"`
	SequenceNumber string `xml:"SequenceNumber,omitempty"`
}
type failedPublishMember struct {
	ID          string `xml:"Id"`
	Code        string `xml:"Code"`
	Message     string `xml:"Message"`
	SenderFault bool   `xml:"SenderFault"`
}

func (p *Provider) publishBatch(req *http.Request) (*plugin.Response, error) {
	topicARN := req.FormValue("TopicArn")
	if topicARN == "" {
		return canonicalSNSError(invalidSNS("TopicArn required"))
	}
	if _, e := p.store.GetTopic(topicARN); e != nil {
		if errors.Is(e, ErrTopicNotFound) {
			return snsError("NotFound", "topic not found", 400), nil
		}
		return canonicalSNSError(e)
	}
	forms, e := queryEntries(req.Form, "PublishBatchRequestEntries", 10000)
	if e != nil {
		return canonicalSNSError(e)
	}
	if len(forms) == 0 {
		return snsError("EmptyBatchRequest", "batch is empty", 400), nil
	}
	if len(forms) > 10 {
		return snsError("TooManyEntriesInBatchRequest", "maximum 10 entries", 400), nil
	}
	entries := make([]PublishEntry, len(forms))
	entryErrors := make([]error, len(forms))
	ids := map[string]bool{}
	total := 0
	for i, f := range forms {
		id := f.Get("Id")
		if !batchEntryID.MatchString(id) {
			return snsError("InvalidBatchEntryId", "invalid Id", 400), nil
		}
		if ids[id] {
			return snsError("BatchEntryIdsNotDistinct", "duplicate Id", 400), nil
		}
		ids[id] = true
		entries[i], entryErrors[i] = parsePublishEntry(f)
		n := publishFormPayloadBytes(f)
		total += n
		if n > 262144 || total > 262144 {
			return snsError("BatchRequestTooLong", "batch exceeds 256 KiB", 400), nil
		}
	}
	success := []publishedMember{}
	failed := []failedPublishMember{}
	for i, entry := range entries {
		e := entryErrors[i]
		var v *Publication
		if e == nil {
			v, e = p.publishEntry(req.Context(), topicARN, entry)
		}
		if e == nil {
			success = append(success, publishedMember{entry.ID, v.MessageID, v.SequenceNumber})
		} else {
			code, message, sender := "InternalError", "local SNS storage or delivery failed", false
			if errors.Is(e, ErrSNSInvalidParameter) {
				code, message, sender = "InvalidParameter", e.Error(), true
			}
			failed = append(failed, failedPublishMember{entry.ID, code, message, sender})
		}
	}
	return snsXML("PublishBatch", struct {
		Successful []publishedMember     `xml:"Successful>member"`
		Failed     []failedPublishMember `xml:"Failed>member"`
	}{success, failed})
}
