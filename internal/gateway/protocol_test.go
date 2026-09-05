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

// TestDetectProtocol_S3Control covers the one service that shares S3's signing
// name. S3 Control signs as "s3", so branch 3's `svc != "s3"` guard sends it to
// the REST-XML default — the only branch that never consults SigningSiblings.
// Without a split it lands on the S3 provider, which parses
// /v20180820/accesspoint/ap as bucket "v20180820", key "accesspoint/ap" and
// stores it: a 200 for a service that served nothing.
func TestDetectProtocol_S3Control(t *testing.T) {
	cases := []struct{ name, method, path, want string }{
		// Every S3 Control operation is under /v20180820/ and no S3 operation is.
		{"create_access_point", "PUT", "/v20180820/accesspoint/ap", "s3control"},
		{"list_access_points", "GET", "/v20180820/accesspoint", "s3control"},
		{"delete_access_point", "DELETE", "/v20180820/accesspoint/ap", "s3control"},
		{"get_public_access_block", "GET", "/v20180820/configuration/publicAccessBlock", "s3control"},
		{"create_job", "POST", "/v20180820/jobs", "s3control"},
		// S3 itself must be untouched, including the shapes most likely to be
		// caught by a sloppier prefix test.
		{"s3_create_bucket", "PUT", "/my-bucket", "s3"},
		{"s3_put_object", "PUT", "/my-bucket/some/key", "s3"},
		{"s3_list_buckets", "GET", "/", "s3"},
		{"s3_bucket_named_like_the_prefix", "PUT", "/v20180820", "s3"},
		{"s3_key_containing_the_prefix", "PUT", "/my-bucket/v20180820/x", "s3"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			req := httptest.NewRequest(c.method, c.path, nil)
			// S3 Control signs with S3's own name; that is the whole problem.
			req.Header.Set("Authorization",
				"AWS4-HMAC-SHA256 Credential=AKIA/20130524/us-east-1/s3/aws4_request, Signature=abc")
			proto, service := DetectProtocol(req)
			assert.Equal(t, "rest-xml", proto)
			assert.Equal(t, c.want, service)
		})
	}
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
