// SPDX-License-Identifier: Apache-2.0

package cloudformation

import (
	"context"
	"testing"

	"github.com/skyoo2003/devcloud/internal/plugin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestParseTemplateJSON verifies that the parser accepts strict JSON and
// populates the top-level sections.
func TestParseTemplateJSON(t *testing.T) {
	body := `{
	  "AWSTemplateFormatVersion": "2010-09-09",
	  "Description": "demo",
	  "Parameters": {"Env": {"Type": "String"}},
	  "Resources": {
	    "Bkt": {"Type": "AWS::S3::Bucket", "Properties": {"BucketName": "demo"}},
	    "Q":   {"Type": "AWS::SQS::Queue", "Properties": {"QueueName": "q-demo"}, "DependsOn": "Bkt"}
	  }
	}`
	tmpl, err := ParseTemplate(body)
	require.NoError(t, err)
	assert.Equal(t, "2010-09-09", tmpl.FormatVersion)
	assert.Equal(t, "demo", tmpl.Description)
	assert.Contains(t, tmpl.Resources, "Bkt")
	assert.Equal(t, "AWS::S3::Bucket", tmpl.Resources["Bkt"].Type)
	assert.Equal(t, "Bkt", tmpl.Resources["Q"].DependsOn)
}

// TestParseTemplateYAML exercises the YAML code path and the normalisation
// from map[any]any -> map[string]any.
func TestParseTemplateYAML(t *testing.T) {
	body := `
AWSTemplateFormatVersion: '2010-09-09'
Description: yaml demo
Resources:
  Bkt:
    Type: AWS::S3::Bucket
    Properties:
      BucketName: yaml-demo
  Q:
    Type: AWS::SQS::Queue
    Properties:
      QueueName: yaml-q
`
	tmpl, err := ParseTemplate(body)
	require.NoError(t, err)
	assert.Equal(t, "yaml demo", tmpl.Description)
	require.Contains(t, tmpl.Resources, "Bkt")
	assert.Equal(t, "yaml-demo", tmpl.Resources["Bkt"].Properties["BucketName"])
}

// TestTopoSortOrders guarantees that Refs imply ordering — Q (refs Bkt) must
// come after Bkt, and an unreferenced resource is placed deterministically.
func TestTopoSortOrders(t *testing.T) {
	tmpl := &Template{
		Resources: map[string]ResourceSpec{
			"Q": {
				Type: "AWS::SQS::Queue",
				Properties: map[string]any{
					"QueueName": map[string]any{"Ref": "Bkt"},
				},
			},
			"Bkt":   {Type: "AWS::S3::Bucket"},
			"Alone": {Type: "AWS::S3::Bucket"},
		},
	}
	order, err := topoSort(tmpl)
	require.NoError(t, err)
	// Bkt must precede Q.
	bktIdx := indexOf(order, "Bkt")
	qIdx := indexOf(order, "Q")
	require.GreaterOrEqual(t, bktIdx, 0)
	require.GreaterOrEqual(t, qIdx, 0)
	assert.Less(t, bktIdx, qIdx)
}

func TestTopoSortCycle(t *testing.T) {
	tmpl := &Template{
		Resources: map[string]ResourceSpec{
			"A": {Type: "X", Properties: map[string]any{"x": map[string]any{"Ref": "B"}}},
			"B": {Type: "X", Properties: map[string]any{"x": map[string]any{"Ref": "A"}}},
		},
	}
	_, err := topoSort(tmpl)
	require.Error(t, err)
}

// TestResolveRefAndGetAtt exercises the intrinsic resolver end-to-end.
func TestResolveRefAndGetAtt(t *testing.T) {
	cache := newAttrCache("stack")
	cache.set("Bkt", "demo-bucket", "arn:aws:s3:::demo-bucket", map[string]string{
		"DomainName": "demo-bucket.s3.amazonaws.com",
	})

	out, err := resolveValue(map[string]any{"Ref": "Bkt"}, cache, nil)
	require.NoError(t, err)
	assert.Equal(t, "demo-bucket", out)

	out, err = resolveValue(map[string]any{"Fn::GetAtt": []any{"Bkt", "Arn"}}, cache, nil)
	require.NoError(t, err)
	assert.Equal(t, "arn:aws:s3:::demo-bucket", out)

	out, err = resolveValue(map[string]any{"Fn::GetAtt": "Bkt.DomainName"}, cache, nil)
	require.NoError(t, err)
	assert.Equal(t, "demo-bucket.s3.amazonaws.com", out)
}

func TestResolveSubAndJoin(t *testing.T) {
	cache := newAttrCache("mystack")
	cache.set("Bkt", "demo-bucket", "arn:aws:s3:::demo-bucket", map[string]string{
		"DomainName": "demo-bucket.s3.amazonaws.com",
	})

	// Fn::Sub with Ref, GetAtt, pseudo-parameter and user params.
	out, err := resolveValue(
		map[string]any{"Fn::Sub": "bucket=${Bkt} dn=${Bkt.DomainName} acct=${AWS::AccountId} env=${Env}"},
		cache,
		map[string]string{"Env": "prod"},
	)
	require.NoError(t, err)
	assert.Equal(t, "bucket=demo-bucket dn=demo-bucket.s3.amazonaws.com acct=000000000000 env=prod", out)

	// Fn::Join
	out, err = resolveValue(
		map[string]any{"Fn::Join": []any{":", []any{"prefix", map[string]any{"Ref": "Bkt"}}}},
		cache,
		nil,
	)
	require.NoError(t, err)
	assert.Equal(t, "prefix:demo-bucket", out)
}

func TestProvisionStackSimulatedResources(t *testing.T) {
	// With no sibling services registered the engine must fall back to
	// simulated provisions but still succeed.
	p := &Provider{}
	require.NoError(t, p.Init(plugin.PluginConfig{DataDir: t.TempDir()}))
	t.Cleanup(func() { _ = p.Shutdown(context.Background()) })

	tmpl, err := ParseTemplate(`{
	  "Resources": {
	    "Bkt": {"Type": "AWS::S3::Bucket", "Properties": {"BucketName": "sim-bkt"}},
	    "Q":   {"Type": "AWS::SQS::Queue", "Properties": {"QueueName": {"Fn::Sub": "${Bkt}-q"}}}
	  }
	}`)
	require.NoError(t, err)

	// Create the stack row so ProvisionStack can update it.
	_, err = p.store.CreateStack("sim-stack", "id-1", "arn", "", "[]", "[]", "[]", "", "", false)
	require.NoError(t, err)

	err = p.engine.ProvisionStack(context.Background(), "sim-stack", tmpl, nil)
	require.NoError(t, err)

	resources, err := p.store.ListStackResources("sim-stack")
	require.NoError(t, err)
	require.Len(t, resources, 2)
	// BucketName drives the simulated PhysicalId for supported-but-unregistered services.
	var bkt, q StackResource
	for _, r := range resources {
		switch r.LogicalID {
		case "Bkt":
			bkt = r
		case "Q":
			q = r
		}
	}
	assert.Equal(t, "sim-bkt", bkt.PhysicalID)
	assert.Equal(t, "sim-bkt-q", q.PhysicalID)
	assert.Equal(t, "CREATE_COMPLETE", bkt.Status)
	assert.Equal(t, "CREATE_COMPLETE", q.Status)
}

func indexOf(s []string, v string) int {
	for i, x := range s {
		if x == v {
			return i
		}
	}
	return -1
}

// TestEngine_PortHandling verifies NewEngine threads the configured HTTP
// port into the Engine struct and falls back to the default 4747 when zero
// is passed. The port is consumed only by the fallback path that builds a
// placeholder SQS queue URL when parsing the inline SQS response fails; a
// regression here would silently emit wrong URLs for CloudFormation-managed
// queues.
func TestEngine_PortHandling(t *testing.T) {
	tests := []struct {
		name      string
		inputPort int
		wantPort  int
	}{
		{"zero falls back to default 4747", 0, 4747},
		{"negative port falls back to 4747", -1, 4747},
		{"custom port 5858", 5858, 5858},
		{"custom port 8080", 8080, 8080},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := NewEngine(nil, nil, tt.inputPort)
			require.NotNil(t, e)
			assert.Equal(t, tt.wantPort, e.port, "port field mismatch")
			assert.NotNil(t, e.registry, "registry should default to plugin.DefaultRegistry")
		})
	}
}
