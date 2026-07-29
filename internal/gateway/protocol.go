// SPDX-License-Identifier: Apache-2.0

package gateway

import (
	"bytes"
	"io"
	"net/http"
	"strings"
)

// DetectProtocol inspects the incoming HTTP request and returns the AWS protocol
// type and the target service ID.
//
// Detection order:
//  1. X-Amz-Target header → JSON protocol (e.g. DynamoDB)
//  2. Content-Type application/x-www-form-urlencoded with Action= body → Query protocol (SQS/IAM/STS)
//  3. Default → REST-XML / S3
func DetectProtocol(r *http.Request) (protocol string, serviceID string) {
	// 1. JSON protocol via X-Amz-Target
	if target := r.Header.Get("X-Amz-Target"); target != "" {
		contentType := r.Header.Get("Content-Type")
		proto := jsonProtocolFromContentType(contentType)
		service := serviceFromTarget(target)
		return proto, service
	}

	// 2. Query protocol via application/x-www-form-urlencoded
	contentType := r.Header.Get("Content-Type")
	if strings.Contains(contentType, "application/x-www-form-urlencoded") {
		bodyBytes, err := io.ReadAll(r.Body)
		if err == nil {
			// Restore the body so downstream handlers can read it again.
			r.Body = io.NopCloser(bytes.NewReader(bodyBytes))

			if strings.Contains(string(bodyBytes), "Action=") {
				return "query", serviceFromQueryRequest(r)
			}
		}
	}

	// 3. Check SigV4 for REST-style services (Lambda, etc.)
	if svc := serviceFromSigV4(r); svc != "" && svc != "s3" {
		normalized := normalizeServiceID(svc)
		// SES and SESv2 share signing name "ses"; REST-JSON is SESv2.
		if normalized == "ses" {
			normalized = "sesv2"
		}
		// Elasticsearch and OpenSearch share signing name "es"; distinguish by
		// URL path: the legacy ES API uses /2015-01-01/ prefix.
		if normalized == "opensearch" && strings.Contains(r.URL.Path, "/2015-01-01/") {
			normalized = "elasticsearchservice"
		}
		return "rest-json", normalized
	}

	// 4. Default: REST-XML / S3
	return "rest-xml", "s3"
}

// jsonProtocolFromContentType maps an application/x-amz-json-1.x content-type
// to the corresponding protocol label.
func jsonProtocolFromContentType(contentType string) string {
	switch {
	case strings.Contains(contentType, "application/x-amz-json-1.0"):
		return "json-1.0"
	case strings.Contains(contentType, "application/x-amz-json-1.1"):
		return "json-1.1"
	default:
		return "json"
	}
}

// serviceFromTarget extracts the lowercase service name from an X-Amz-Target
// value of the form "ServiceName_YYYYMMDD.OperationName".
func serviceFromTarget(target string) string {
	// X-Amz-Target is "<targetPrefix>.<OperationName>". The prefix itself may
	// contain dots (e.g. "com.amazonaws.codeconnections.CodeConnections_20231201"),
	// so drop the operation (everything after the last '.') and take the last
	// dotted segment of the prefix as the "ServiceName_Date" token.
	prefix := target
	if idx := strings.LastIndex(prefix, "."); idx != -1 {
		prefix = prefix[:idx]
	}
	if idx := strings.LastIndex(prefix, "."); idx != -1 {
		prefix = prefix[idx+1:]
	}
	serviceAndDate := prefix
	full := strings.ToLower(serviceAndDate)

	// Try full string first (handles services that share a prefix but differ
	// by date, e.g. AWSWAF_20150824 → waf vs AWSWAF_20190729 → wafv2).
	normalized := normalizeServiceID(full)
	if normalized != full {
		return normalized
	}

	// Strip the date suffix (everything from the first '_' onward) and retry.
	if idx := strings.Index(serviceAndDate, "_"); idx != -1 {
		return normalizeServiceID(strings.ToLower(serviceAndDate[:idx]))
	}
	return normalized
}

// serviceFromSigV4 extracts the service name from the SigV4 Authorization
// header's credential scope: "AWS4-HMAC-SHA256 Credential=.../region/service/aws4_request"
func serviceFromSigV4(r *http.Request) string {
	auth := r.Header.Get("Authorization")
	if !strings.HasPrefix(auth, "AWS4-HMAC-SHA256") {
		return ""
	}
	// Find "Credential=" part
	credIdx := strings.Index(auth, "Credential=")
	if credIdx < 0 {
		return ""
	}
	credVal := auth[credIdx+len("Credential="):]
	// Credential value ends at comma or end of string
	if commaIdx := strings.IndexByte(credVal, ','); commaIdx >= 0 {
		credVal = credVal[:commaIdx]
	}
	// Format: accessKey/date/region/service/aws4_request
	parts := strings.Split(credVal, "/")
	if len(parts) >= 4 {
		svc := parts[3]
		// Normalize known service aliases
		return normalizeServiceID(svc)
	}
	return ""
}

// normalizeServiceID maps SigV4 signing names, X-Amz-Target prefixes, and
// other AWS identifiers to DevCloud internal service IDs.
func normalizeServiceID(svc string) string {
	svc = strings.ToLower(svc)
	switch svc {
	case "amazonsqs":
		return "sqs"
	case "dynamodb_20120810":
		return "dynamodb"
	case "amazonssm":
		return "ssm"
	case "trentservice":
		return "kms"
	case "logs", "logs_20140328":
		return "cloudwatchlogs"
	case "monitoring", "graniteserviceversion20100801":
		return "cloudwatch"
	case "awsevents":
		return "eventbridge"
	case "amazonec2containerservicev20141113":
		return "ecs"
	case "amazonec2containerregistry", "amazonec2containerregistry_v20150921":
		return "ecr"
	case "certificatemanager":
		return "acm"
	case "awswaf", "awswaf_20150824":
		return "waf"
	case "awswaf_20190729", "awswafv2":
		return "wafv2"
	case "awsglue":
		return "glue"
	case "codepipeline_20150709":
		return "codepipeline"
	case "codebuild_20161006":
		return "codebuild"
	case "codedeploy_20141006":
		return "codedeploy"
	case "codecommit_20150413":
		return "codecommit"
	case "codeartifact_20180409":
		return "codeartifact"
	case "amazonkinesis":
		return "kinesis"
	case "kinesisanalytics", "kinesisanalytics_v2":
		return "kinesisanalyticsv2"
	case "firehose_20150804":
		return "firehose"
	case "amazonathena":
		return "athena"
	case "amazonemr", "elasticmapreduce":
		return "emr"
	case "amazondynamodbstreams", "dynamodbstreams_20120810":
		return "dynamodbstreams"
	case "amazonmwaa", "airflow":
		return "mwaa"
	case "awssfn", "awsstepfunctions":
		return "sfn"
	case "simpleWorkflowService", "simpleworkflowservice":
		return "swf"
	case "swbexternalservice":
		return "ssoadmin"
	case "amazoncognitoidentity", "awscognitoidentityservice":
		return "cognitoidentity"
	case "cognitouseridentityproviderservice", "cognitoidp", "cognito-idp", "awscognitoidentityproviderservice":
		return "cognitoidentityprovider"
	case "amazonmemorydb":
		return "memorydb"
	case "amazonmq":
		return "mq"
	case "awsorganizations", "awsorganizationsv2", "awsorganizationsv20161128":
		return "organizations"
	case "awsshield", "awsshield_20160616":
		return "shield"
	case "sso":
		return "ssoadmin"
	case "awssupport", "awssupport_20130415":
		return "support"
	case "awsfaultinjectionservice":
		return "fis"
	case "awsxray":
		return "xray"
	case "timestream_20181101", "timestream":
		return "timestreamwrite"
	case "awscostexplorer", "awsinsightsindexservice":
		return "costexplorer"
	case "awsbatch", "awsbatch_v20160810":
		return "batch"
	case "msk":
		return "kafka"
	case "amazondmsv20160101":
		return "dms"
	case "config", "starlingdoveservice":
		return "configservice"
	case "application-autoscaling", "anyupfront", "anyscalefrontendservice":
		return "applicationautoscaling"
	case "awsresourcegroups", "resource-groups":
		return "resourcegroups"
	case "resourcegroupstagging":
		return "resourcegroupstaggingapi"
	case "cloudcontrolapi", "cloudapiservice":
		return "cloudcontrol"
	case "elasticloadbalancing":
		return "elasticloadbalancingv2"
	case "es":
		return "opensearch"
	case "aoss":
		return "opensearch"
	case "apigateway":
		return "apigatewayv2"
	case "mobiletargeting":
		return "pinpoint"
	case "data.iot", "iotdata", "iot-data":
		return "iotdataplane"
	case "acm-pca", "acmprivateca":
		return "acmpca"
	case "route53autonaming":
		return "servicediscovery"
	case "elasticfilesystem":
		return "efs"
	case "transferservice":
		return "transfer"
	case "cloudtrail_20131101":
		return "cloudtrail"
	case "opensearchservice":
		return "opensearch"
	case "awsidentitystore", "swbexternaluserservice":
		return "identitystore"
	case "serverlessapplicationrepository":
		return "serverlessrepo"
	case "events":
		return "eventbridge"
	default:
		return svc
	}
}

// serviceFromQueryRequest determines the service for a Query-protocol request
// from the SigV4 credential scope, falling back to the Host header prefix.
// Every AWS SDK, CLI, and Terraform request is signed, so the credential scope
// carries the signing name; sqs is the default for the unsigned, unprefixed
// case because it is the only Query service SDKs address by bare endpoint.
func serviceFromQueryRequest(r *http.Request) string {
	if svc := serviceFromSigV4(r); svc != "" {
		return svc
	}

	switch strings.ToLower(strings.SplitN(r.Host, ".", 2)[0]) {
	case "iam":
		return "iam"
	case "sts":
		return "sts"
	}
	return "sqs"
}
