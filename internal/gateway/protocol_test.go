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

func TestServiceFromTarget(t *testing.T) {
	cases := []struct {
		name, target, want string
	}{
		{"simple", "DynamoDB_20120810.PutItem", "dynamodb"},
		// Dotted target prefixes: only the last dotted segment is the
		// "ServiceName_Date" token; the operation and the com.amazonaws.*
		// namespace must be stripped.
		{"codeconnections", "com.amazonaws.codeconnections.CodeConnections_20231201.ListConnections", "codeconnections"},
		{"cloudtrail", "com.amazonaws.cloudtrail.v20131101.CloudTrail_20131101.CreateTrail", "cloudtrail"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			assert.Equal(t, c.want, serviceFromTarget(c.target))
		})
	}
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

// TestDetectProtocol_Query_Unsigned_BareHost covers the client that has neither
// a SigV4 credential scope nor a service host prefix (curl, an SDK configured
// unsigned, a test harness). Without the Action fallback every one of these
// routes to sqs and gets the wrong provider's error.
func TestDetectProtocol_Query_Unsigned_BareHost(t *testing.T) {
	cases := []struct{ name, body, want string }{
		{"sts_caller_identity", "Action=GetCallerIdentity", "sts"},
		{"sts_assume_role", "Action=AssumeRoleWithWebIdentity&RoleArn=x", "sts"},
		{"sts_session_token", "Action=GetSessionToken", "sts"},
		{"iam_create_user", "Action=CreateUser&UserName=test", "iam"},
		{"iam_delete_role", "Action=DeleteRole&RoleName=r", "iam"},
		{"iam_list_policies", "Action=ListPolicies", "iam"},
		{"iam_create_access_key", "Action=CreateAccessKey&UserName=u", "iam"},
		{"sqs_send", "Action=SendMessage&QueueUrl=http://x/q&MessageBody=hi", "sqs"},
		{"sqs_create", "Action=CreateQueue&QueueName=q", "sqs"},
		{"sqs_list", "Action=ListQueues", "sqs"},
		// An unrecognised Query action still defaults to sqs, as before.
		{"unknown_defaults_sqs", "Action=DescribeDBInstances", "sqs"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			req := httptest.NewRequest("POST", "/", strings.NewReader(c.body))
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			req.Host = "localhost:4747"
			proto, service := DetectProtocol(req)
			assert.Equal(t, "query", proto)
			assert.Equal(t, c.want, service)
		})
	}
}

// TestDetectProtocol_Query_SigV4WinsOverAction confirms the credential scope
// still decides for signed clients, so the Action fallback cannot misroute the
// SDK/CLI/Terraform traffic that makes up every real request.
func TestDetectProtocol_Query_SigV4WinsOverAction(t *testing.T) {
	req := httptest.NewRequest("POST", "/", strings.NewReader("Action=CreateUser&UserName=test"))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Authorization",
		"AWS4-HMAC-SHA256 Credential=AKIA/20130524/us-east-1/sts/aws4_request, Signature=abc")
	req.Host = "localhost:4747"
	_, service := DetectProtocol(req)
	assert.Equal(t, "sts", service, "credential scope must outrank the Action name")
}

func TestDetectProtocol_Query_STS(t *testing.T) {
	req := httptest.NewRequest("POST", "/", strings.NewReader("Action=GetCallerIdentity"))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Host = "sts.localhost:4747"
	proto, service := DetectProtocol(req)
	assert.Equal(t, "query", proto)
	assert.Equal(t, "sts", service)
}
