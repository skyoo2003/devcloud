// SPDX-License-Identifier: Apache-2.0

package gateway

import (
	"bytes"
	"io"
	"net/http"
	"net/url"
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
				service := serviceFromQueryRequest(r, string(bodyBytes))
				return "query", service
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
	// Split on '.' to separate "ServiceName_Date" from "OperationName".
	parts := strings.SplitN(target, ".", 2)
	if len(parts) == 0 {
		return ""
	}
	serviceAndDate := parts[0]
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
	switch strings.ToLower(svc) {
	// Core services
	case "s3":
		return "s3"
	case "sqs", "amazonsqs":
		return "sqs"
	case "dynamodb", "dynamodb_20120810":
		return "dynamodb"
	case "iam":
		return "iam"
	case "sts":
		return "sts"
	case "lambda":
		return "lambda"
	case "sns":
		return "sns"

	// JSON-protocol services (matched by X-Amz-Target prefix, lowercased)
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
	case "secretsmanager":
		return "secretsmanager"
	case "certificatemanager":
		return "acm"
	case "awswaf", "awswaf_20150824":
		return "waf"
	case "awswaf_20190729", "awswafv2":
		return "wafv2"
	case "awsglue":
		return "glue"
	case "sagemaker":
		return "sagemaker"
	case "route53resolver":
		return "route53resolver"
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
	case "kinesisanalytics", "kinesisanalytics_v2", "kinesisanalyticsv2":
		return "kinesisanalyticsv2"
	case "firehose", "firehose_20150804":
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
	case "swf", "simpleWorkflowService", "simpleworkflowservice":
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
	case "ssoadmin", "sso":
		return "ssoadmin"
	case "awssupport", "awssupport_20130415":
		return "support"
	case "awsfaultinjectionservice", "fis":
		return "fis"
	case "xray", "awsxray":
		return "xray"
	case "timestreamwrite", "timestream_20181101", "timestream":
		return "timestreamwrite"
	case "transcribe":
		return "transcribe"
	case "textract":
		return "textract"
	case "bedrock":
		return "bedrock"
	case "costexplorer", "awscostexplorer", "awsinsightsindexservice":
		return "costexplorer"
	case "batch", "awsbatch", "awsbatch_v20160810":
		return "batch"
	case "kafka", "msk":
		return "kafka"
	case "lakeformation":
		return "lakeformation"
	case "configservice", "config", "starlingdoveservice":
		return "configservice"
	case "applicationautoscaling", "application-autoscaling", "anyupfront", "anyscalefrontendservice":
		return "applicationautoscaling"
	case "appconfig":
		return "appconfig"
	case "awsresourcegroups", "resourcegroups", "resource-groups":
		return "resourcegroups"
	case "resourcegroupstaggingapi", "resourcegroupstagging":
		return "resourcegroupstaggingapi"
	case "ram":
		return "ram"
	case "cloudcontrolapi", "cloudapiservice", "cloudcontrol":
		return "cloudcontrol"
	case "pipes":
		return "pipes"
	case "account":
		return "account"

	// SigV4 signing names for REST/Query services
	case "ec2":
		return "ec2"
	case "route53":
		return "route53"
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
	case "backup":
		return "backup"
	case "iot":
		return "iot"
	case "data.iot", "iotdata", "iot-data":
		return "iotdataplane"
	case "iotwireless":
		return "iotwireless"
	case "amplify":
		return "amplify"
	case "appsync":
		return "appsync"
	case "cloudfront":
		return "cloudfront"
	case "acm-pca", "acmpca", "acmprivateca":
		return "acmpca"
	case "servicediscovery", "route53autonaming":
		return "servicediscovery"
	case "eks":
		return "eks"
	case "efs", "elasticfilesystem":
		return "efs"
	case "ebs":
		return "ebs"
	case "glacier":
		return "glacier"
	case "managedblockchain":
		return "managedblockchain"
	case "mediaconvert":
		return "mediaconvert"
	case "transfer", "transferservice":
		return "transfer"
	case "codecommit":
		return "codecommit"
	case "codedeploy":
		return "codedeploy"
	case "codebuild":
		return "codebuild"
	case "codepipeline":
		return "codepipeline"
	case "codeartifact":
		return "codeartifact"
	case "cloudtrail", "cloudtrail_20131101", "com":
		return "cloudtrail"
	case "opensearch", "opensearchservice":
		return "opensearch"
	case "s3tables":
		return "s3tables"
	case "identitystore", "awsidentitystore", "swbexternaluserservice":
		return "identitystore"
	case "serverlessrepo", "serverlessapplicationrepository":
		return "serverlessrepo"
	case "scheduler":
		return "scheduler"

	// Query-protocol services (signing name = service name)
	case "rds":
		return "rds"
	case "cloudformation":
		return "cloudformation"
	case "elasticache":
		return "elasticache"
	case "redshift":
		return "redshift"
	case "ses":
		return "ses"
	case "autoscaling":
		return "autoscaling"
	case "elasticbeanstalk":
		return "elasticbeanstalk"
	case "cloudsearch":
		return "cloudsearch"
	case "ssm":
		return "ssm"
	case "kms":
		return "kms"
	case "ecs":
		return "ecs"
	case "ecr":
		return "ecr"
	case "events":
		return "eventbridge"
	case "acm":
		return "acm"
	case "cloudwatchlogs":
		return "cloudwatchlogs"
	case "cloudwatch":
		return "cloudwatch"
	case "eventbridge":
		return "eventbridge"
	case "glue":
		return "glue"
	case "waf":
		return "waf"

	default:
		return svc
	}
}

// serviceFromQueryRequest determines the service for a Query-protocol request
// by examining the SigV4 credential scope, Host header prefix, and the Action parameter.
func serviceFromQueryRequest(r *http.Request, body string) string {
	// Most reliable: extract service from SigV4 Authorization header
	if svc := serviceFromSigV4(r); svc != "" {
		return svc
	}

	host := r.Host

	// Check the host prefix.
	hostPrefix := strings.ToLower(strings.SplitN(host, ".", 2)[0])
	switch hostPrefix {
	case "iam":
		return "iam"
	case "sts":
		return "sts"
	case "sqs":
		return "sqs"
	}

	// Fall back to Action name inspection.
	values, err := url.ParseQuery(body)
	if err == nil {
		action := values.Get("Action")
		switch {
		case strings.HasPrefix(action, "CreateUser"),
			strings.HasPrefix(action, "DeleteUser"),
			strings.HasPrefix(action, "ListUsers"),
			strings.HasPrefix(action, "AttachRole"),
			strings.HasPrefix(action, "CreateRole"),
			strings.HasPrefix(action, "CreatePolicy"),
			strings.HasPrefix(action, "ListAttachedRole"),
			strings.HasPrefix(action, "CreateAccessKey"),
			action == "CreateGroup",
			action == "DeleteGroup":
			return "iam"
		case action == "GetCallerIdentity",
			action == "AssumeRole",
			action == "GetSessionToken":
			return "sts"
		case action == "SendMessage",
			action == "ReceiveMessage",
			action == "DeleteMessage",
			action == "CreateQueue",
			action == "GetQueueUrl",
			action == "ListQueues",
			action == "DeleteQueue",
			action == "GetQueueAttributes",
			action == "SetQueueAttributes",
			action == "PurgeQueue",
			action == "ChangeMessageVisibility",
			action == "TagQueue",
			action == "UntagQueue":
			return "sqs"
		}

		// Detect SQS by presence of QueueUrl parameter.
		if values.Get("QueueUrl") != "" {
			return "sqs"
		}
	}

	// Default Query service.
	return "sqs"
}
