// SPDX-License-Identifier: Apache-2.0

// internal/services/kafka/provider_test.go
package kafka

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/skyoo2003/devcloud/internal/plugin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestProvider(t *testing.T) *Provider {
	t.Helper()
	p := &Provider{}
	err := p.Init(plugin.PluginConfig{DataDir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
	return p
}

func callOP(t *testing.T, p *Provider, method, path, op, body string) *plugin.Response {
	t.Helper()
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err := p.HandleRequest(context.Background(), op, req)
	require.NoError(t, err)
	return resp
}

func parseBody(t *testing.T, resp *plugin.Response) map[string]any {
	t.Helper()
	var m map[string]any
	require.NoError(t, json.Unmarshal(resp.Body, &m))
	return m
}

// createTestCluster creates a cluster and returns its ARN.
func createTestCluster(t *testing.T, p *Provider, name string) string {
	t.Helper()
	body := `{"ClusterName":"` + name + `","KafkaVersion":"3.5.1","NumberOfBrokerNodes":3,"BrokerNodeGroupInfo":{"InstanceType":"kafka.m5.large","ClientSubnets":["subnet-1"]}}`
	resp := callOP(t, p, "POST", "/v1/clusters", "CreateCluster", body)
	require.Equal(t, 200, resp.StatusCode)
	rb := parseBody(t, resp)
	arn, _ := rb["clusterArn"].(string)
	require.NotEmpty(t, arn)
	return arn
}

func TestCreateAndDescribeCluster(t *testing.T) {
	p := newTestProvider(t)

	// Create cluster
	resp := callOP(t, p, "POST", "/v1/clusters", "CreateCluster",
		`{"ClusterName":"my-cluster","KafkaVersion":"3.5.1","NumberOfBrokerNodes":3}`)
	assert.Equal(t, 200, resp.StatusCode)
	body := parseBody(t, resp)
	assert.Equal(t, "my-cluster", body["clusterName"])
	arn, _ := body["clusterArn"].(string)
	assert.NotEmpty(t, arn)
	assert.Equal(t, "ACTIVE", body["state"])

	// Describe cluster
	resp2 := callOP(t, p, "GET", "/v1/clusters/"+arn, "DescribeCluster", "")
	assert.Equal(t, 200, resp2.StatusCode)
	body2 := parseBody(t, resp2)
	info, ok := body2["clusterInfo"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "my-cluster", info["clusterName"])
	assert.Equal(t, arn, info["clusterArn"])

	// DescribeClusterV2
	resp3 := callOP(t, p, "GET", "/api/v2/clusters/"+arn, "DescribeClusterV2", "")
	assert.Equal(t, 200, resp3.StatusCode)
	body3 := parseBody(t, resp3)
	info3, ok := body3["clusterInfo"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "PROVISIONED", info3["clusterType"])

	// Duplicate create
	resp4 := callOP(t, p, "POST", "/v1/clusters", "CreateCluster",
		`{"ClusterName":"my-cluster","KafkaVersion":"3.5.1","NumberOfBrokerNodes":3}`)
	assert.Equal(t, 409, resp4.StatusCode)

	// Describe non-existent
	resp5 := callOP(t, p, "GET", "/v1/clusters/arn:aws:kafka:us-east-1:000000000000:cluster/nonexistent/abc", "DescribeCluster", "")
	assert.Equal(t, 404, resp5.StatusCode)
}

func TestListClusters(t *testing.T) {
	p := newTestProvider(t)

	createTestCluster(t, p, "cluster-a")
	createTestCluster(t, p, "cluster-b")
	createTestCluster(t, p, "cluster-c")

	// ListClusters
	resp := callOP(t, p, "GET", "/v1/clusters", "ListClusters", "")
	assert.Equal(t, 200, resp.StatusCode)
	body := parseBody(t, resp)
	list, ok := body["clusterInfoList"].([]any)
	require.True(t, ok)
	assert.Len(t, list, 3)

	// ListClustersV2
	resp2 := callOP(t, p, "GET", "/api/v2/clusters", "ListClustersV2", "")
	assert.Equal(t, 200, resp2.StatusCode)
	body2 := parseBody(t, resp2)
	list2, ok := body2["clusters"].([]any)
	require.True(t, ok)
	assert.Len(t, list2, 3)
}

func TestDeleteCluster(t *testing.T) {
	p := newTestProvider(t)

	arn := createTestCluster(t, p, "to-delete")

	// Delete
	resp := callOP(t, p, "DELETE", "/v1/clusters/"+arn, "DeleteCluster", "")
	assert.Equal(t, 200, resp.StatusCode)
	body := parseBody(t, resp)
	assert.Equal(t, "DELETING", body["state"])

	// Describe after delete → 404
	resp2 := callOP(t, p, "GET", "/v1/clusters/"+arn, "DescribeCluster", "")
	assert.Equal(t, 404, resp2.StatusCode)

	// Delete non-existent → 404
	resp3 := callOP(t, p, "DELETE", "/v1/clusters/"+arn, "DeleteCluster", "")
	assert.Equal(t, 404, resp3.StatusCode)
}

func TestConfigurationCRUD(t *testing.T) {
	p := newTestProvider(t)

	// Create configuration
	resp := callOP(t, p, "POST", "/v1/configurations", "CreateConfiguration",
		`{"Name":"my-config","KafkaVersions":["3.5.1"],"ServerProperties":"auto.create.topics.enable=true"}`)
	assert.Equal(t, 200, resp.StatusCode)
	body := parseBody(t, resp)
	arn, _ := body["arn"].(string)
	assert.NotEmpty(t, arn)
	assert.Equal(t, "my-config", body["name"])

	// Duplicate
	resp2 := callOP(t, p, "POST", "/v1/configurations", "CreateConfiguration",
		`{"Name":"my-config","ServerProperties":""}`)
	assert.Equal(t, 409, resp2.StatusCode)

	// Describe configuration
	resp3 := callOP(t, p, "GET", "/v1/configurations/"+arn, "DescribeConfiguration", "")
	assert.Equal(t, 200, resp3.StatusCode)
	body3 := parseBody(t, resp3)
	assert.Equal(t, arn, body3["arn"])

	// Describe revision
	resp4 := callOP(t, p, "GET", "/v1/configurations/"+arn+"/revisions/1", "DescribeConfigurationRevision", "")
	assert.Equal(t, 200, resp4.StatusCode)
	body4 := parseBody(t, resp4)
	assert.Equal(t, float64(1), body4["revision"])
	assert.Equal(t, "auto.create.topics.enable=true", body4["serverProperties"])

	// List configurations
	resp5 := callOP(t, p, "GET", "/v1/configurations", "ListConfigurations", "")
	assert.Equal(t, 200, resp5.StatusCode)
	body5 := parseBody(t, resp5)
	cfgs, ok := body5["configurations"].([]any)
	require.True(t, ok)
	assert.Len(t, cfgs, 1)

	// Update configuration
	resp6 := callOP(t, p, "PUT", "/v1/configurations/"+arn, "UpdateConfiguration",
		`{"ServerProperties":"auto.create.topics.enable=false"}`)
	assert.Equal(t, 200, resp6.StatusCode)
	body6 := parseBody(t, resp6)
	lr, _ := body6["latestRevision"].(map[string]any)
	assert.Equal(t, float64(2), lr["revision"])

	// List revisions
	resp7 := callOP(t, p, "GET", "/v1/configurations/"+arn+"/revisions", "ListConfigurationRevisions", "")
	assert.Equal(t, 200, resp7.StatusCode)
	body7 := parseBody(t, resp7)
	revs, ok := body7["revisions"].([]any)
	require.True(t, ok)
	assert.Len(t, revs, 2)
}

func TestTopicCRUD(t *testing.T) {
	p := newTestProvider(t)

	clusterArn := createTestCluster(t, p, "topic-cluster")

	// Create topic
	resp := callOP(t, p, "POST", "/v1/clusters/"+clusterArn+"/topics", "CreateTopic",
		`{"TopicName":"my-topic","NumPartitions":6,"ReplicationFactor":3}`)
	assert.Equal(t, 200, resp.StatusCode)
	body := parseBody(t, resp)
	assert.Equal(t, "my-topic", body["topicName"])
	assert.Equal(t, float64(6), body["numPartitions"])

	// Duplicate topic
	resp2 := callOP(t, p, "POST", "/v1/clusters/"+clusterArn+"/topics", "CreateTopic",
		`{"TopicName":"my-topic","NumPartitions":1}`)
	assert.Equal(t, 409, resp2.StatusCode)

	// Describe topic
	resp3 := callOP(t, p, "GET", "/v1/clusters/"+clusterArn+"/topics/my-topic", "DescribeTopic", "")
	assert.Equal(t, 200, resp3.StatusCode)
	body3 := parseBody(t, resp3)
	assert.Equal(t, "my-topic", body3["topicName"])

	// Describe topic partitions
	resp4 := callOP(t, p, "GET", "/v1/clusters/"+clusterArn+"/topics/my-topic/partitions", "DescribeTopicPartitions", "")
	assert.Equal(t, 200, resp4.StatusCode)
	body4 := parseBody(t, resp4)
	parts, ok := body4["partitions"].([]any)
	require.True(t, ok)
	assert.Len(t, parts, 6)

	// List topics
	resp5 := callOP(t, p, "GET", "/v1/clusters/"+clusterArn+"/topics", "ListTopics", "")
	assert.Equal(t, 200, resp5.StatusCode)
	body5 := parseBody(t, resp5)
	list, ok := body5["topicsList"].([]any)
	require.True(t, ok)
	assert.Len(t, list, 1)

	// Update topic
	resp6 := callOP(t, p, "PUT", "/v1/clusters/"+clusterArn+"/topics/my-topic", "UpdateTopic",
		`{"NumPartitions":12}`)
	assert.Equal(t, 200, resp6.StatusCode)
	body6 := parseBody(t, resp6)
	assert.Equal(t, float64(12), body6["numPartitions"])

	// Delete topic
	resp7 := callOP(t, p, "DELETE", "/v1/clusters/"+clusterArn+"/topics/my-topic", "DeleteTopic", "")
	assert.Equal(t, 200, resp7.StatusCode)

	// Describe after delete → 404
	resp8 := callOP(t, p, "GET", "/v1/clusters/"+clusterArn+"/topics/my-topic", "DescribeTopic", "")
	assert.Equal(t, 404, resp8.StatusCode)
}

func TestTags(t *testing.T) {
	p := newTestProvider(t)

	clusterArn := createTestCluster(t, p, "tagged-cluster")

	// TagResource
	tagBody, _ := json.Marshal(map[string]any{
		"Tags": map[string]string{"Env": "test", "Team": "platform"},
	})
	resp := callOP(t, p, "POST", "/v1/tags/"+clusterArn, "TagResource", string(tagBody))
	assert.Equal(t, 200, resp.StatusCode)

	// ListTagsForResource
	resp2 := callOP(t, p, "GET", "/v1/tags/"+clusterArn, "ListTagsForResource", "")
	assert.Equal(t, 200, resp2.StatusCode)
	body2 := parseBody(t, resp2)
	tags, ok := body2["tags"].(map[string]any)
	require.True(t, ok)
	assert.Len(t, tags, 2)
	assert.Equal(t, "test", tags["env"])

	// UntagResource
	resp3 := callOP(t, p, "DELETE", "/v1/tags/"+clusterArn+"?tagKeys=Env", "UntagResource", "")
	assert.Equal(t, 200, resp3.StatusCode)

	// Verify only 1 tag remains
	resp4 := callOP(t, p, "GET", "/v1/tags/"+clusterArn, "ListTagsForResource", "")
	body4 := parseBody(t, resp4)
	tags4, ok := body4["tags"].(map[string]any)
	require.True(t, ok)
	assert.Len(t, tags4, 1)
	assert.Equal(t, "platform", tags4["team"])
}

func TestGetBootstrapBrokers(t *testing.T) {
	p := newTestProvider(t)

	clusterArn := createTestCluster(t, p, "broker-cluster")

	resp := callOP(t, p, "GET", "/v1/clusters/"+clusterArn+"/bootstrap-brokers", "GetBootstrapBrokers", "")
	assert.Equal(t, 200, resp.StatusCode)
	body := parseBody(t, resp)
	brokerStr, _ := body["bootstrapBrokerString"].(string)
	assert.NotEmpty(t, brokerStr)
	// 3 brokers by default
	brokers := strings.Split(brokerStr, ",")
	assert.Len(t, brokers, 3)
	assert.Contains(t, brokers[0], ":9092")

	// Non-existent cluster
	resp2 := callOP(t, p, "GET", "/v1/clusters/nonexistent/bootstrap-brokers", "GetBootstrapBrokers", "")
	assert.Equal(t, 404, resp2.StatusCode)
}
