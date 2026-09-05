// SPDX-License-Identifier: Apache-2.0

// internal/codegen/gen_aliases_test.go
package codegen

import (
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/skyoo2003/devcloud/internal/codegen/ir"
)

// TestBuildAliasesDerivesEveryServiceIdentifier pins the whole derivation in one
// case: every name AWS uses for a service — the target prefix, the signing name,
// the endpoint prefix, the ARN namespace, the CloudFormation name, the SDK id —
// resolves to the same DevCloud service ID.
func TestBuildAliasesDerivesEveryServiceIdentifier(t *testing.T) {
	model := &ir.Model{
		ServiceID:          "cloudwatchlogs",
		ShapeName:          "Logs_20140328",
		SigningName:        "logs",
		EndpointPrefix:     "logs",
		ARNNamespace:       "logs",
		CloudFormationName: "Logs",
		SDKID:              "CloudWatch Logs",
	}

	table, collisions := BuildAliases([]*ir.Model{model})
	assert.Empty(t, collisions)

	for _, alias := range []string{
		"logs_20140328",  // X-Amz-Target prefix, verbatim
		"logs",           // date suffix stripped, and the signing name
		"cloudwatchlogs", // the SDK id, matched with its spaces removed
	} {
		assert.Equal(t, "cloudwatchlogs", table[alias], "alias %q must resolve", alias)
	}
}

// TestBuildAliasesResolvesRekognitionServicePrefix is the Milestone 1 failure as
// a unit test. rekognition could not be onboarded at all because its target
// prefix is RekognitionService and no hand-written case clause covered it.
func TestBuildAliasesResolvesRekognitionServicePrefix(t *testing.T) {
	data, err := os.ReadFile("../../smithy-models/rekognition.json")
	require.NoError(t, err)

	model, err := ParseSmithyJSON(data)
	require.NoError(t, err)

	table, collisions := BuildAliases([]*ir.Model{model})
	assert.Empty(t, collisions)
	assert.Equal(t, "rekognition", table["rekognitionservice"],
		"the X-Amz-Target prefix must route without a hand-written entry")
}

// TestBuildAliasesDoesNotGuessACollision is the load-bearing safety property.
// Several AWS services legitimately share an identifier — es names both
// opensearch and elasticsearchservice, rds names rds, docdb and neptune. Picking
// one silently sends every call for the other to the wrong provider, so a
// contested alias is reported and omitted rather than resolved.
func TestBuildAliasesDoesNotGuessACollision(t *testing.T) {
	models := []*ir.Model{
		{ServiceID: "opensearch", ShapeName: "OpenSearchService", SigningName: "es"},
		{ServiceID: "elasticsearchservice", ShapeName: "AmazonElasticsearchService", SigningName: "es"},
	}

	table, collisions := BuildAliases(models)

	assert.NotContains(t, table, "es",
		"a contested alias must be absent, never resolved to one of the claimants")
	assert.Contains(t, collisions, "es")

	// Uncontested aliases from the same models are unaffected.
	assert.Equal(t, "opensearch", table["opensearchservice"])
	assert.Equal(t, "elasticsearchservice", table["amazonelasticsearchservice"])
}

// TestBuildAliasesIdenticalClaimIsNotACollision guards the common case: the same
// service naming itself twice (endpointPrefix == arnNamespace, which holds for
// most models) must not be reported as contested.
func TestBuildAliasesIdenticalClaimIsNotACollision(t *testing.T) {
	model := &ir.Model{
		ServiceID:      "dynamodb",
		ShapeName:      "DynamoDB_20120810",
		SigningName:    "dynamodb",
		EndpointPrefix: "dynamodb",
		ARNNamespace:   "dynamodb",
	}

	table, collisions := BuildAliases([]*ir.Model{model})

	assert.Empty(t, collisions)
	assert.Equal(t, "dynamodb", table["dynamodb"])
}

// TestBuildAliasesIsDeterministic — the table is rendered into committed
// generated code, so two runs over the same models must produce the same result
// or every `make codegen` shows a spurious diff.
func TestBuildAliasesIsDeterministic(t *testing.T) {
	models := []*ir.Model{
		{ServiceID: "waf", ShapeName: "AWSWAF_20150824", SigningName: "waf"},
		{ServiceID: "wafv2", ShapeName: "AWSWAF_20190729", SigningName: "wafv2"},
		{ServiceID: "sqs", ShapeName: "AmazonSQS", SigningName: "sqs"},
	}

	first, firstCollisions := BuildAliases(models)
	second, secondCollisions := BuildAliases(models)

	assert.Equal(t, first, second)
	assert.Equal(t, firstCollisions, secondCollisions)
	// Both WAF models claim "awswaf" once their date suffix is stripped.
	assert.Contains(t, firstCollisions, "awswaf")
}

// TestBuildAliasesOverTheFleet runs the derivation over every committed model.
// The exact collision set is asserted because each entry is a routing decision a
// human has already made in the gateway's override map: a new one appearing here
// means a service was added whose identifier is contested and unresolved.
func TestBuildAliasesOverTheFleet(t *testing.T) {
	models := loadCommittedModels(t)
	require.Greater(t, len(models), 50, "the models directory looks truncated")

	table, collisions := BuildAliases(models)

	assert.Equal(t, []string{
		"amazonrdsv19",       // rds, docdb, neptune
		"awswaf",             // waf, wafv2
		"cognito",            // cognitoidentity, cognitoidentityprovider
		"dynamodb",           // dynamodb, dynamodbstreams
		"email",              // ses, sesv2
		"es",                 // elasticsearchservice, opensearch
		"rds",                // rds, docdb, neptune
		"ses",                // ses, sesv2
		"simpleemailservice", // ses, sesv2
	}, collisions, "a new collision is a routing decision that needs a human")

	// Spot-check aliases the hand-written switch used to carry, now derived.
	assert.Equal(t, "cloudwatchlogs", table["logs"])
	assert.Equal(t, "kms", table["trentservice"])
	assert.Equal(t, "cloudwatch", table["monitoring"])
	assert.Equal(t, "rekognition", table["rekognitionservice"])
}

// TestGenerateAliasesRendersBothTables — the collisions have to survive into the
// generated package, not just the generator's return value. They are the input
// to the gateway test that demands a resolution for each; dropping them from the
// render turns that check into a no-op that always passes.
func TestGenerateAliasesRendersBothTables(t *testing.T) {
	gen := NewGenerator("templates")

	output, err := gen.GenerateAliases(
		map[string]string{"rekognitionservice": "rekognition", "logs": "cloudwatchlogs"},
		[]string{"es"},
	)
	require.NoError(t, err)

	assert.Contains(t, output, "package aliases")
	assert.Contains(t, output, `"rekognitionservice": "rekognition"`)
	assert.Contains(t, output, `"logs": "cloudwatchlogs"`)
	assert.Contains(t, output, "var Collisions = []string{")
	assert.Contains(t, output, `"es",`)

	// Rendered in sorted order, or every regeneration shows a spurious diff.
	assert.Less(t, strings.Index(output, `"logs"`), strings.Index(output, `"rekognitionservice"`))
}

func loadCommittedModels(t *testing.T) []*ir.Model {
	t.Helper()

	entries, err := os.ReadDir("../../smithy-models")
	require.NoError(t, err)

	var models []*ir.Model
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		data, err := os.ReadFile("../../smithy-models/" + entry.Name())
		require.NoError(t, err)
		model, err := ParseSmithyJSON(data)
		require.NoError(t, err, entry.Name())
		models = append(models, model)
	}
	return models
}
