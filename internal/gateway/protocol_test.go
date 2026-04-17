// SPDX-License-Identifier: Apache-2.0

package gateway

import (
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDetectProtocol_RESTXML(t *testing.T) {
	req := httptest.NewRequest("PUT", "/my-bucket", nil)
	req.Header.Set("Authorization", "AWS4-HMAC-SHA256 ...")
	proto, service := DetectProtocol(req)
	assert.Equal(t, "rest-xml", proto)
	assert.Equal(t, "s3", service)
}

func TestDetectProtocol_JSON10_DynamoDB(t *testing.T) {
	req := httptest.NewRequest("POST", "/", strings.NewReader(`{}`))
	req.Header.Set("X-Amz-Target", "DynamoDB_20120810.PutItem")
	req.Header.Set("Content-Type", "application/x-amz-json-1.0")
	proto, service := DetectProtocol(req)
	assert.Equal(t, "json-1.0", proto)
	assert.Equal(t, "dynamodb", service)
}

func TestDetectProtocol_Query_SQS(t *testing.T) {
	body := "Action=SendMessage&QueueUrl=http://localhost:4747/123456789/test-queue&MessageBody=hello"
	req := httptest.NewRequest("POST", "/", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	proto, service := DetectProtocol(req)
	assert.Equal(t, "query", proto)
	assert.Equal(t, "sqs", service)
}

func TestDetectProtocol_Query_IAM(t *testing.T) {
	req := httptest.NewRequest("POST", "/", strings.NewReader("Action=CreateUser&UserName=test"))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Host = "iam.localhost:4747"
	proto, service := DetectProtocol(req)
	assert.Equal(t, "query", proto)
	assert.Equal(t, "iam", service)
}

func TestDetectProtocol_Query_STS(t *testing.T) {
	req := httptest.NewRequest("POST", "/", strings.NewReader("Action=GetCallerIdentity"))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Host = "sts.localhost:4747"
	proto, service := DetectProtocol(req)
	assert.Equal(t, "query", proto)
	assert.Equal(t, "sts", service)
}
