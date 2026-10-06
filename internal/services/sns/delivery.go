// SPDX-License-Identifier: Apache-2.0
package sns

import (
	"context"
	"encoding/base64"
	"encoding/xml"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/skyoo2003/devcloud/internal/plugin"
)

func (p *Provider) fanoutPublicationToSQS(ctx context.Context, endpoint string, entry PublishEntry, publication Publication) error {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	svc, ok := plugin.DefaultRegistry.Get("sqs")
	if !ok {
		return fmt.Errorf("SQS provider unavailable")
	}
	queueURL, err := resolveSQSQueueURL(ctx, svc, endpoint)
	if err != nil {
		return err
	}
	body, err := sqsPublishBody(entry)
	if err != nil {
		return err
	}
	values := url.Values{"Action": {"SendMessage"}, "QueueUrl": {queueURL}, "MessageBody": {body}}
	if entry.MessageGroupID != "" {
		values.Set("MessageGroupId", entry.MessageGroupID)
	}
	if publication.MessageDeduplicationID != "" {
		values.Set("MessageDeduplicationId", publication.MessageDeduplicationID)
	}
	keys := make([]string, 0, len(entry.Attributes))
	for k := range entry.Attributes {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for i, k := range keys {
		a := entry.Attributes[k]
		prefix := "MessageAttribute." + strconv.Itoa(i+1)
		values.Set(prefix+".Name", k)
		values.Set(prefix+".Value.DataType", a.DataType)
		if strings.SplitN(a.DataType, ".", 2)[0] == "Binary" {
			values.Set(prefix+".Value.BinaryValue", base64.StdEncoding.EncodeToString(a.BinaryValue))
		} else {
			values.Set(prefix+".Value.StringValue", a.StringValue)
		}
	}
	_, err = sqsDeliveryRequest(ctx, svc, values)
	return err
}

func (p *Provider) fanoutToSQS(ctx context.Context, endpoint, message string) error {
	return p.fanoutPublicationToSQS(ctx, endpoint, PublishEntry{Message: message}, Publication{})
}

func resolveSQSQueueURL(ctx context.Context, svc plugin.ServicePlugin, endpoint string) (string, error) {
	if !strings.HasPrefix(endpoint, "arn:") {
		u, err := url.Parse(endpoint)
		if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.Path == "" {
			return "", fmt.Errorf("invalid SQS endpoint")
		}
		return endpoint, nil
	}
	parts := strings.Split(endpoint, ":")
	if len(parts) != 6 || parts[1] != "aws" || parts[2] != "sqs" || parts[3] == "" || parts[4] != defaultAccountID || parts[5] == "" || strings.ContainsAny(parts[5], "/\\") {
		return "", fmt.Errorf("invalid or unsupported SQS ARN: %s", endpoint)
	}
	resp, err := sqsDeliveryRequest(ctx, svc, url.Values{"Action": {"GetQueueUrl"}, "QueueName": {parts[5]}, "QueueOwnerAWSAccountId": {parts[4]}})
	if err != nil {
		return "", err
	}
	var result struct {
		URL string `xml:"GetQueueUrlResult>QueueUrl"`
	}
	if err := xml.Unmarshal(resp.Body, &result); err != nil {
		return "", fmt.Errorf("decode SQS queue URL: %w", err)
	}
	if result.URL == "" {
		return "", fmt.Errorf("SQS returned no queue URL")
	}
	return result.URL, nil
}

func sqsDeliveryRequest(ctx context.Context, svc plugin.ServicePlugin, values url.Values) (*plugin.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "/", strings.NewReader(values.Encode()))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := svc.HandleRequest(ctx, values.Get("Action"), req)
	if err != nil {
		return nil, err
	}
	if resp == nil {
		return nil, fmt.Errorf("SQS %s failed: no response", values.Get("Action"))
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("SQS %s failed: status=%d", values.Get("Action"), resp.StatusCode)
	}
	return resp, nil
}
