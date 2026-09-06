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
	"github.com/skyoo2003/devcloud/internal/generated/fidelity"
	"github.com/skyoo2003/devcloud/internal/shared/crud"
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
		op := operationFromTarget(target)
		return proto, resolveSharedSigningName(service, func(c string) bool {
			return declaresOperation(c, op)
		})
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
		// API Gateway v1 and v2 share the signing name "apigateway". The alias
		// resolves to v1, which is the service that publishes the name — and
		// every v2 operation is under /v2/, while no v1 operation is, so the
		// path separates them cleanly. Same idea as the two splits above.
		//
		// This is what keeps v2 callers working now that "apigateway" no longer
		// substitutes to v2: before api-gateway was registered, DevCloud served
		// v2 for both, and a v2 client sending /v2/apis would otherwise land on
		// a v1 provider that models no such path.
		if normalized == "apigateway" && strings.HasPrefix(r.URL.Path, "/v2/") {
			normalized = "apigatewayv2"
		}
		uri := r.URL.RequestURI()
		return "rest-json", resolveSharedSigningName(normalized, func(c string) bool {
			return crud.HasRoute(c, r.Method, uri)
		})
	}

	// 4. Default: REST-XML / S3.
	//
	// S3 Control signs with S3's own name ("s3"), so branch 3's `svc != "s3"`
	// guard sends it here — and this is the one branch that never consults
	// SigningSiblings, even though aliases.SigningSiblings["s3"] already lists
	// both. Every S3 Control operation is under /v20180820/ and no S3 operation
	// is, so the path separates them, exactly as it does for opensearch and
	// apigateway above.
	//
	// A route-table split would be wrong here rather than merely bigger: S3's
	// own `PutObject PUT /{Bucket}/{Key+}` matches /v20180820/accesspoint/ap,
	// so asking which service models the path answers "s3". That is also what
	// the bug was — the S3 provider stored the request as bucket "v20180820",
	// key "accesspoint/ap" and returned 200, fabricating a success for a
	// service that served nothing.
	if strings.HasPrefix(r.URL.Path, "/v20180820/") {
		return "rest-xml", "s3control"
	}
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
	// Timestream Query and Timestream Write share the shape name
	// Timestream_20181101 and the version 2018-11-01, so neither the alias nor
	// the protocol separates them. Pinned to write, which is where these
	// aliases already pointed and the service with the hand-written provider;
	// resolveSharedSigningName then hands query the operations write does not
	// declare. Deleting these stops both services routing under the prefix
	// every Timestream SDK actually sends.
	"timestream":          "timestreamwrite",
	"timestream_20181101": "timestreamwrite",
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

// resolveSharedSigningName picks the service that actually models the request,
// among those that sign with the same SigV4 name.
//
// AWS splits a data plane, runtime, or successor version into its own SDK client
// while leaving it signing with the parent's name — 18 signing names are shared
// this way across 50 services. The credential scope therefore cannot separate
// them, and aliases.ServiceIDs answers with one member, so the others were
// registered and routed to a provider that models none of their operations. A
// boto3 mediastore-data client calling ListItems landed on mediastore.
//
// The tie is broken by what each candidate models: the REST route table for a
// path-addressed request, the fidelity manifest for an operation-named one. The
// resolved service keeps the request whenever it models it, so this only ever
// redirects a request its current destination could not have served. When no
// candidate models it, or more than one does, the request stays where it was and
// gets that service's clean error — a guess would be worse than an honest miss.
//
// The grouping is derived (codegen.BuildSigningSiblings), so a new split-out
// service is handled by regenerating, not by editing a list here.
func resolveSharedSigningName(service string, match func(candidate string) bool) string {
	group, shared := aliases.SigningSiblings[signingNameOf(service)]
	if !shared || match(service) {
		return service
	}
	found := ""
	for _, candidate := range group {
		if candidate == service || !match(candidate) {
			continue
		}
		if found != "" {
			// Ambiguous; two siblings model it. Leave the caller where it is.
			return service
		}
		found = candidate
	}
	if found == "" {
		return service
	}
	return found
}

// signingNameOf finds the signing name whose group contains service. The groups
// are small and few, and this runs once per request that reaches a shared name.
//
// A service may also arrive *as* a signing name. Most shared names are the
// parent service's own ID, so the group is found by membership — but four Lex
// services sign as "lex" and none of them is called that, so the alias stays
// contested and normalizeServiceID hands the unresolved name straight through.
// Recognising the group key is what lets those requests reach the split at all;
// the split then answers with the sibling that models the request, or refuses
// when two do.
func signingNameOf(service string) string {
	if _, ok := aliases.SigningSiblings[service]; ok {
		return service
	}
	for name, group := range aliases.SigningSiblings {
		for _, id := range group {
			if id == service {
				return name
			}
		}
	}
	return ""
}

// operationFromTarget returns the operation name from an X-Amz-Target value of
// the form "<prefix>.<Operation>", or "" when there is no operation part.
func operationFromTarget(target string) string {
	if idx := strings.LastIndex(target, "."); idx != -1 {
		return target[idx+1:]
	}
	return ""
}

// declaresOperation reports whether a service's model names this operation. Used
// for the JSON protocols, where the request carries an operation but no path —
// Timestream Query and Timestream Write publish the same shape name *and* the
// same version, so the operation is the only thing that separates them.
func declaresOperation(service, op string) bool {
	if op == "" {
		return false
	}
	_, ok := fidelity.Lookup(service, op)
	return ok
}

// serviceFromQueryRequest determines the service for a Query-protocol request
// from the SigV4 credential scope, then the Host header prefix, then the Action
// parameter. SDKs, the CLI, and Terraform all sign, so the credential scope
// almost always decides it — but an unsigned client posting to a bare endpoint
// has neither a scope nor a host prefix, and defaulting those to sqs sends
// every IAM and STS call to the wrong provider. sqs remains the final default.
func serviceFromQueryRequest(r *http.Request, body string) string {
	if svc := serviceFromSigV4(r); svc != "" {
		// ELB v1 and v2 share the signing name "elasticloadbalancing", and both
		// speak Query, so neither the scope nor the protocol separates them.
		// The API version in the request body does, and every SDK sends it.
		//
		// This is what keeps v2 callers working now that the alias resolves to
		// v1: DevCloud served v2 for both before elastic-load-balancing was
		// registered.
		if svc == "elasticloadbalancing" && strings.Contains(body, "Version=2015-12-01") {
			return "elasticloadbalancingv2"
		}
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
