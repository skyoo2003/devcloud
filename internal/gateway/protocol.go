// SPDX-License-Identifier: Apache-2.0

package gateway

import (
	"bytes"
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/skyoo2003/devcloud/internal/auth"
	"github.com/skyoo2003/devcloud/internal/generated/aliases"
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

			if bytes.Contains(bodyBytes, []byte("Action=")) {
				return "query", serviceFromQueryRequest(r, string(bodyBytes))
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

// serviceFromSigV4 maps the signing name in a request's SigV4 credential scope
// to a DevCloud service ID, or "" when the request carries no scope.
//
// The scope is parsed once, by the auth adapter, which is also what fills the
// request context — routing and identity cannot disagree about what the caller
// signed for.
func serviceFromSigV4(r *http.Request) string {
	id, ok := auth.SigV4{}.Authenticate(r)
	if !ok || id.Service == "" {
		return ""
	}
	return normalizeServiceID(id.Service)
}

// serviceIDOverrides is the exception list to the derived alias table. It exists
// for the three cases generation cannot settle on its own; everything else comes
// from the models via codegen.BuildAliases, so onboarding a service no longer
// means hand-writing a routing entry for it.
//
// Group 1 — CONTESTED. More than one service publishes the identifier, so the
// generator omits it (it appears in aliases.Collisions) and the choice is made
// here. Deleting one of these stops the alias routing anywhere.
//
// Group 2 — LEGACY. Identifiers older SDKs and CLIs still send that no current
// model publishes. Nothing derives them; they go when those callers do.
//
// Group 3 — SUBSTITUTION. DevCloud does not register the service the caller
// asked for, so the request is served by a near neighbour. Each of these is a
// coverage gap wearing a routing entry: registering the real service is what
// deletes it.
var serviceIDOverrides = map[string]string{
	// --- Group 1: contested ---
	//
	// An empty value means "decided, and the decision is not to guess": the
	// alias routes nowhere, exactly as it did before the table existed. It is
	// here so the collision is answered rather than merely unlisted — sending
	// two thirds of docdb and neptune's traffic to rds is worse than an honest
	// UnknownService.
	"amazonrdsv19": "",           // rds/docdb/neptune share a shape name; no basis to pick
	"cognito":      "",           // cognitoidentity vs cognitoidentityprovider; no basis to pick
	"es":           "opensearch", // ES and OpenSearch share "es"; DetectProtocol splits them by URL path
	"awswaf":       "waf",        // WAF classic; wafv2 arrives as AWSWAF_20190729
	// SES v1 (Query) and SESv2 (REST-JSON) share every identifier. The protocol
	// is what tells them apart, and DetectProtocol already does it, so these
	// resolve to v1 and the REST-JSON branch promotes to v2. Mapping them
	// straight to sesv2 routes every SES v1 Query call to the v2 provider.
	// "ses" itself needs no entry: SES names itself, so the generator settles it.
	"email":              "ses",
	"simpleemailservice": "ses",
	// SageMaker Runtime and its HTTP/2 variant share an endpoint prefix and
	// neither is named "runtime.sagemaker". Same API, different transport.
	"runtime.sagemaker": "sagemakerruntime",
	// All four Lex services sign as "lex" and none is named it. They are
	// restJson1, so none can be engine-served, and picking one would route the
	// other three's traffic to a service that answers for a different API. Each
	// still routes under its own unambiguous name. Revisit if one gains a
	// hand-written provider — then URL-path routing, as DetectProtocol already
	// does for opensearch, becomes worth the code.
	"lex": "",

	// --- Group 2: legacy identifiers no model publishes ---
	"amazonkinesis":                      "kinesis",
	"amazondynamodbstreams":              "dynamodbstreams",
	"amazonemr":                          "emr",
	"amazoncognitoidentity":              "cognitoidentity",
	"cognitouseridentityproviderservice": "cognitoidentityprovider",
	"cognitoidp":                         "cognitoidentityprovider",
	"amazondmsv20160101":                 "dms",
	"anyupfront":                         "applicationautoscaling",
	"awsbatch":                           "batch",
	"awsbatch_v20160810":                 "batch",
	"awscostexplorer":                    "costexplorer",
	"awsfaultinjectionservice":           "fis",
	"awsidentitystore":                   "identitystore",
	"swbexternaluserservice":             "identitystore",
	"awsorganizations":                   "organizations",
	"awsorganizationsv2":                 "organizations",
	"awsresourcegroups":                  "resourcegroups",
	"awssfn":                             "sfn",
	"awswafv2":                           "wafv2",
	"cloudcontrolapi":                    "cloudcontrol",
	"cloudapiservice":                    "cloudcontrol",
	"codeartifact_20180409":              "codeartifact",
	"data.iot":                           "iotdataplane",
	"iot-data":                           "iotdataplane",
	"kinesisanalytics_v2":                "kinesisanalyticsv2",
	"opensearchservice":                  "opensearch",
	"resourcegroupstagging":              "resourcegroupstaggingapi",
	"serverlessapplicationrepository":    "serverlessrepo",

	// --- Group 3: substitutions for services DevCloud does not register ---
	//
	// Only one entry left, and the reason the others went is worth keeping.
	// "apigateway" resolves to apigatewayv2 and "sso" to ssoadmin without any
	// help here, because those services publish those names and the service
	// that would contest them is not modelled. That is a substitution the
	// models happen to make, not one anybody chose — and it stops being silent
	// the moment the missing model is added, because the alias becomes a
	// collision and TestServiceIDOverridesResolveEveryCollision fails until
	// somebody decides. Deliberately not pinned here: pinning it would make
	// that decision now, invisibly, for a service that does not exist yet.
	"aoss": "opensearch", // OpenSearch Serverless publishes no model in-tree
}

// normalizeServiceID maps SigV4 signing names, X-Amz-Target prefixes, and other
// AWS identifiers to DevCloud internal service IDs.
//
// Overrides win over the derived table: an entry there is a decision generation
// could not make, and a generated answer must not quietly replace it.
func normalizeServiceID(svc string) string {
	svc = strings.ToLower(svc)
	if id, ok := serviceIDOverrides[svc]; ok {
		if id == "" {
			// Deliberately unrouted; see the Group 1 comment above.
			return svc
		}
		return id
	}
	if id, ok := aliases.ServiceIDs[svc]; ok {
		return id
	}
	return svc
}

// serviceFromQueryRequest determines the service for a Query-protocol request
// from the SigV4 credential scope, then the Host header prefix, then the Action
// parameter. SDKs, the CLI, and Terraform all sign, so the credential scope
// almost always decides it — but an unsigned client posting to a bare endpoint
// has neither a scope nor a host prefix, and defaulting those to sqs sends
// every IAM and STS call to the wrong provider. sqs remains the final default.
func serviceFromQueryRequest(r *http.Request, body string) string {
	if svc := serviceFromSigV4(r); svc != "" {
		return svc
	}

	switch strings.ToLower(strings.SplitN(r.Host, ".", 2)[0]) {
	case "iam":
		return "iam"
	case "sts":
		return "sts"
	}

	values, err := url.ParseQuery(body)
	if err != nil {
		return "sqs"
	}
	action := values.Get("Action")
	switch {
	case action == "GetCallerIdentity", action == "GetSessionToken",
		action == "GetFederationToken", strings.HasPrefix(action, "AssumeRole"):
		return "sts"
	case values.Get("QueueUrl") != "", values.Get("QueueName") != "",
		action == "ListQueues":
		return "sqs"
	case action != "":
		// Every SQS operation names a queue, so it is fully covered above.
		// What is left that talks about IAM entities is IAM. "Polic" rather than
		// "Policy" so the plural in ListPolicies/DeletePolicies still matches.
		for _, entity := range []string{"User", "Role", "Polic", "Group", "AccessKey"} {
			if strings.Contains(action, entity) {
				return "iam"
			}
		}
	}
	return "sqs"
}
