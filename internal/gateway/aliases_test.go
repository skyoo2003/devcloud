// SPDX-License-Identifier: Apache-2.0

package gateway

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/skyoo2003/devcloud/internal/generated/aliases"
)

// goldenAliases is every alias the hand-written normalizeServiceID switch
// resolved before the table was derived from the models, captured verbatim from
// that switch. Replacing 63 hand-written case clauses with generated code is
// only safe if none of them changes answer, and this is the only check that can
// say so: a caller signing as "trentservice" or targeting "Logs_20140328" is an
// SDK version in the field, not a case someone remembered to write a test for.
var goldenAliases = []struct{ alias, service string }{
	{"acm-pca", "acmpca"},
	{"acmprivateca", "acmpca"},
	{"airflow", "mwaa"},
	{"amazonathena", "athena"},
	{"amazoncognitoidentity", "cognitoidentity"},
	{"amazondmsv20160101", "dms"},
	{"amazondynamodbstreams", "dynamodbstreams"},
	{"amazonec2containerregistry", "ecr"},
	{"amazonec2containerregistry_v20150921", "ecr"},
	{"amazonec2containerservicev20141113", "ecs"},
	{"amazonemr", "emr"},
	{"amazonkinesis", "kinesis"},
	{"amazonmemorydb", "memorydb"},
	{"amazonmq", "mq"},
	{"amazonmwaa", "mwaa"},
	{"amazonsqs", "sqs"},
	{"amazonssm", "ssm"},
	{"anyscalefrontendservice", "applicationautoscaling"},
	{"anyupfront", "applicationautoscaling"},
	{"application-autoscaling", "applicationautoscaling"},
	{"awsbatch", "batch"},
	{"awsbatch_v20160810", "batch"},
	{"awscognitoidentityproviderservice", "cognitoidentityprovider"},
	{"awscognitoidentityservice", "cognitoidentity"},
	{"awscostexplorer", "costexplorer"},
	{"awsevents", "eventbridge"},
	{"awsfaultinjectionservice", "fis"},
	{"awsglue", "glue"},
	{"awsidentitystore", "identitystore"},
	{"awsinsightsindexservice", "costexplorer"},
	{"awsorganizations", "organizations"},
	{"awsorganizationsv2", "organizations"},
	{"awsorganizationsv20161128", "organizations"},
	{"awsresourcegroups", "resourcegroups"},
	{"awssfn", "sfn"},
	{"awsshield", "shield"},
	{"awsshield_20160616", "shield"},
	{"awsstepfunctions", "sfn"},
	{"awssupport", "support"},
	{"awssupport_20130415", "support"},
	{"awswaf", "waf"},
	{"awswaf_20150824", "waf"},
	{"awswaf_20190729", "wafv2"},
	{"awswafv2", "wafv2"},
	{"awsxray", "xray"},
	{"certificatemanager", "acm"},
	{"cloudapiservice", "cloudcontrol"},
	{"cloudcontrolapi", "cloudcontrol"},
	{"cloudtrail_20131101", "cloudtrail"},
	{"codeartifact_20180409", "codeartifact"},
	{"codebuild_20161006", "codebuild"},
	{"codecommit_20150413", "codecommit"},
	{"codedeploy_20141006", "codedeploy"},
	{"codepipeline_20150709", "codepipeline"},
	{"cognito-idp", "cognitoidentityprovider"},
	{"cognitoidp", "cognitoidentityprovider"},
	{"cognitouseridentityproviderservice", "cognitoidentityprovider"},
	{"config", "configservice"},
	{"data.iot", "iotdataplane"},
	{"dynamodb_20120810", "dynamodb"},
	{"dynamodbstreams_20120810", "dynamodbstreams"},
	{"elasticfilesystem", "efs"},
	{"elasticmapreduce", "emr"},
	{"es", "opensearch"},
	{"events", "eventbridge"},
	{"firehose_20150804", "firehose"},
	{"graniteserviceversion20100801", "cloudwatch"},
	{"iot-data", "iotdataplane"},
	{"iotdata", "iotdataplane"},
	{"kinesisanalytics_v2", "kinesisanalyticsv2"},
	{"logs", "cloudwatchlogs"},
	{"logs_20140328", "cloudwatchlogs"},
	{"mobiletargeting", "pinpoint"},
	{"monitoring", "cloudwatch"},
	{"msk", "kafka"},
	{"opensearchservice", "opensearch"},
	{"resource-groups", "resourcegroups"},
	{"resourcegroupstagging", "resourcegroupstaggingapi"},
	{"route53autonaming", "servicediscovery"},
	{"serverlessapplicationrepository", "serverlessrepo"},
	{"simpleworkflowservice", "swf"},
	{"sso", "ssoadmin"},
	{"starlingdoveservice", "configservice"},
	{"swbexternalservice", "ssoadmin"},
	{"swbexternaluserservice", "identitystore"},
	{"timestream", "timestreamwrite"},
	{"timestream_20181101", "timestreamwrite"},
	{"transferservice", "transfer"},
	{"trentservice", "kms"},

	// Below: aliases the old switch never named, so they fell through to its
	// default branch and returned unchanged. They are pinned because the
	// derived table CAN name them, and four SES compatibility tests failed the
	// first time it did — SES v1 and SESv2 publish identical identifiers, and
	// only the protocol tells them apart. That split is DetectProtocol's job
	// (see its rest-json branch), so normalizeServiceID must leave it alone.
	{"ses", "ses"},
	{"email", "ses"},
	{"s3", "s3"},
	{"lambda", "lambda"},
}

// TestNormalizeServiceIDPreservesEveryKnownAlias is the regression lock for
// replacing the hand-written switch with a generated table.
func TestNormalizeServiceIDPreservesEveryKnownAlias(t *testing.T) {
	for _, c := range goldenAliases {
		// t.Errorf, not Fatal: one broken alias must not hide the other 92.
		if got := normalizeServiceID(c.alias); got != c.service {
			t.Errorf("normalizeServiceID(%q) = %q, want %q", c.alias, got, c.service)
		}
	}
}

// TestNormalizeServiceIDDerivesPrefixWithoutAnOverride is the Milestone 1
// failure, at the gateway. rekognition's X-Amz-Target prefix is
// RekognitionService and no hand-written clause covered it, so the request
// arrived as UnknownService: rekognitionservice and the service could not be
// onboarded at all. Nothing here is hand-written — the alias comes from the
// model — which is the property that makes the per-service onboarding cost a
// fleet-wide number instead of a best case.
func TestNormalizeServiceIDDerivesPrefixWithoutAnOverride(t *testing.T) {
	assert.NotContains(t, serviceIDOverrides, "rekognitionservice",
		"this alias must be derived, not hand-written, or the test proves nothing")
	assert.Equal(t, "rekognition", normalizeServiceID("rekognitionservice"))
	assert.Equal(t, "rekognition", serviceFromTarget("RekognitionService.ListCollections"))
}

// TestServiceIDOverridesResolveEveryCollision — a contested alias is omitted
// from the generated table, so routing for it exists only if a human resolved it
// here. An unresolved collision means both claimants stop routing under that
// name, and nothing else would notice.
func TestServiceIDOverridesResolveEveryCollision(t *testing.T) {
	for _, alias := range aliases.Collisions {
		if _, ok := serviceIDOverrides[alias]; !ok {
			t.Errorf("alias %q is claimed by more than one service and has no override; "+
				"pick the service it routes to in serviceIDOverrides", alias)
		}
	}
}

// TestRetiredSubstitutionsNowReachTheRealService records the four aliases whose
// answer this milestone deliberately changes, and why. Each was a Group 3
// substitution: DevCloud did not register the service the caller named, so the
// request was served by a near neighbour. Registering the real service is what
// retires the substitution — the outcome protocol.go's Group 3 comment predicted.
//
// They are here rather than in goldenAliases because that table is the record of
// what must not change; an entry that moved needs its reason next to it.
func TestRetiredSubstitutionsNowReachTheRealService(t *testing.T) {
	cases := []struct{ alias, was, now, why string }{
		{"apigateway", "apigatewayv2", "apigateway",
			"API Gateway v1 is registered now and publishes the name as its own"},
		{"elasticloadbalancing", "elasticloadbalancingv2", "elasticloadbalancing",
			"ELB v1 is registered now and publishes the name as its own"},
		{"kinesisanalytics", "kinesisanalyticsv2", "kinesisanalytics",
			"Kinesis Analytics v1 is registered now and publishes the name as its own"},
		{"aoss", "opensearch", "opensearchserverless",
			"OpenSearch Serverless has an in-tree model now, so the alias is derived"},
	}
	for _, c := range cases {
		if got := normalizeServiceID(c.alias); got != c.now {
			t.Errorf("normalizeServiceID(%q) = %q, want %q (%s)", c.alias, got, c.now, c.why)
		}
	}
}

// TestContestedVersionsStayReachable is the other half of the retirement. Moving
// an alias to v1 is only safe if v2 callers still land on v2 — otherwise the
// milestone trades a missing service for a broken one.
//
// Each pair is separated by the cheapest thing that actually differs, which is
// the same approach DetectProtocol already takes for ses/sesv2 and
// opensearch/elasticsearchservice.
func TestContestedVersionsStayReachable(t *testing.T) {
	sign := func(r *http.Request, service string) *http.Request {
		r.Header.Set("Authorization",
			"AWS4-HMAC-SHA256 Credential=AKIA/20130524/us-east-1/"+service+"/aws4_request, Signature=abc")
		return r
	}

	t.Run("api gateway splits on the URL path", func(t *testing.T) {
		_, v2 := DetectProtocol(sign(httptest.NewRequest(http.MethodGet, "/v2/apis", nil), "apigateway"))
		assert.Equal(t, "apigatewayv2", v2, "a v2 path must still reach v2")

		_, v1 := DetectProtocol(sign(httptest.NewRequest(http.MethodGet, "/restapis", nil), "apigateway"))
		assert.Equal(t, "apigateway", v1, "a v1 path must reach the newly registered v1")
	})

	t.Run("elb splits on the API version in the body", func(t *testing.T) {
		post := func(body string) *http.Request {
			r := sign(httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body)), "elasticloadbalancing")
			r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			return r
		}
		_, v2 := DetectProtocol(post("Action=DescribeLoadBalancers&Version=2015-12-01"))
		assert.Equal(t, "elasticloadbalancingv2", v2, "a v2 call must still reach v2")

		_, v1 := DetectProtocol(post("Action=DescribeLoadBalancers&Version=2012-06-01"))
		assert.Equal(t, "elasticloadbalancing", v1, "a v1 call must reach the newly registered v1")
	})

	t.Run("kinesis analytics splits on the dated target prefix", func(t *testing.T) {
		assert.Equal(t, "kinesisanalyticsv2", serviceFromTarget("KinesisAnalytics_20180523.ListApplications"))
		assert.Equal(t, "kinesisanalytics", serviceFromTarget("KinesisAnalytics_20150814.ListApplications"))
	})

	// Timestream is the hard one: Query and Write publish the same shape name
	// *and* the same version, so only the operation differs.
	t.Run("timestream splits on the operation", func(t *testing.T) {
		jsonReq := func(target string) *http.Request {
			r := httptest.NewRequest(http.MethodPost, "/", nil)
			r.Header.Set("X-Amz-Target", target)
			r.Header.Set("Content-Type", "application/x-amz-json-1.0")
			return r
		}
		_, write := DetectProtocol(jsonReq("Timestream_20181101.CreateDatabase"))
		assert.Equal(t, "timestreamwrite", write, "a write operation must reach write")

		_, query := DetectProtocol(jsonReq("Timestream_20181101.ListScheduledQueries"))
		assert.Equal(t, "timestreamquery", query, "a query operation must reach query")

		// An operation neither service declares stays with the service the
		// caller named, so it gets that service's clean error rather than being
		// quietly rerouted.
		_, unknown := DetectProtocol(jsonReq("Timestream_20181101.NoSuchOperation"))
		assert.Equal(t, "timestreamwrite", unknown)
	})
}

// TestServiceIDOverridesAreAllReachable stops the override map rotting. An entry
// that agrees with the generated table is dead weight; the map is the exception
// list, and every exception costs a reader.
func TestServiceIDOverridesAreAllReachable(t *testing.T) {
	for alias, service := range serviceIDOverrides {
		if derived, ok := aliases.ServiceIDs[alias]; ok && derived == service {
			t.Errorf("override %q -> %q duplicates the generated table; delete it", alias, service)
		}
	}
}
