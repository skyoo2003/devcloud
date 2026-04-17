// SPDX-License-Identifier: Apache-2.0

// internal/services/codeartifact/provider_test.go
package codeartifact

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
	t.Cleanup(func() { p.Shutdown(context.Background()) })
	return p
}

func callREST(t *testing.T, p *Provider, method, path, op, body string) *plugin.Response {
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

func TestDomainCRUD(t *testing.T) {
	p := newTestProvider(t)

	// Create
	createBody := `{"domain": "my-domain", "encryptionKey": "arn:aws:kms:us-east-1:000000000000:key/test"}`
	resp := callREST(t, p, "POST", "/v1/domain", "CreateDomain", createBody)
	assert.Equal(t, 200, resp.StatusCode)
	rb := parseBody(t, resp)
	domain, ok := rb["domain"].(map[string]any)
	require.True(t, ok, "expected domain key")
	assert.Equal(t, "my-domain", domain["name"])
	arn, _ := domain["arn"].(string)
	assert.NotEmpty(t, arn)

	// Create duplicate → conflict
	resp2 := callREST(t, p, "POST", "/v1/domain", "CreateDomain", createBody)
	assert.Equal(t, 409, resp2.StatusCode)

	// Describe
	descResp := callREST(t, p, "GET", "/v1/domain?domain=my-domain", "DescribeDomain", "")
	assert.Equal(t, 200, descResp.StatusCode)
	descBody := parseBody(t, descResp)
	d2 := descBody["domain"].(map[string]any)
	assert.Equal(t, "my-domain", d2["name"])
	assert.Equal(t, "Active", d2["status"])

	// Describe non-existent
	resp3 := callREST(t, p, "GET", "/v1/domain?domain=doesnotexist", "DescribeDomain", "")
	assert.Equal(t, 404, resp3.StatusCode)

	// List
	callREST(t, p, "POST", "/v1/domain", "CreateDomain", `{"domain": "second-domain"}`)
	listResp := callREST(t, p, "POST", "/v1/domains", "ListDomains", "{}")
	assert.Equal(t, 200, listResp.StatusCode)
	listBody := parseBody(t, listResp)
	domains, ok := listBody["domains"].([]any)
	require.True(t, ok)
	assert.Len(t, domains, 2)

	// Delete
	deleteResp := callREST(t, p, "DELETE", "/v1/domain?domain=my-domain", "DeleteDomain", "")
	assert.Equal(t, 200, deleteResp.StatusCode)
	deleteBody := parseBody(t, deleteResp)
	deleted := deleteBody["domain"].(map[string]any)
	assert.Equal(t, "my-domain", deleted["name"])

	// Describe after delete
	resp4 := callREST(t, p, "GET", "/v1/domain?domain=my-domain", "DescribeDomain", "")
	assert.Equal(t, 404, resp4.StatusCode)

	// Delete non-existent
	resp5 := callREST(t, p, "DELETE", "/v1/domain?domain=doesnotexist", "DeleteDomain", "")
	assert.Equal(t, 404, resp5.StatusCode)
}

func TestRepositoryCRUD(t *testing.T) {
	p := newTestProvider(t)

	// Create domain first
	callREST(t, p, "POST", "/v1/domain", "CreateDomain", `{"domain": "test-domain"}`)

	// Create repository
	createBody := `{"domain": "test-domain", "repository": "my-repo", "description": "my repo desc"}`
	resp := callREST(t, p, "POST", "/v1/repository?domain=test-domain", "CreateRepository", createBody)
	assert.Equal(t, 200, resp.StatusCode)
	rb := parseBody(t, resp)
	repo, ok := rb["repository"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "my-repo", repo["name"])
	assert.Equal(t, "test-domain", repo["domainName"])
	repoARN, _ := repo["arn"].(string)
	assert.NotEmpty(t, repoARN)

	// Duplicate → conflict
	resp2 := callREST(t, p, "POST", "/v1/repository?domain=test-domain", "CreateRepository", createBody)
	assert.Equal(t, 409, resp2.StatusCode)

	// Describe
	descResp := callREST(t, p, "GET", "/v1/repository?domain=test-domain&repository=my-repo", "DescribeRepository", "")
	assert.Equal(t, 200, descResp.StatusCode)
	descBody := parseBody(t, descResp)
	r2 := descBody["repository"].(map[string]any)
	assert.Equal(t, "my repo desc", r2["description"])

	// Describe non-existent
	resp3 := callREST(t, p, "GET", "/v1/repository?domain=test-domain&repository=nope", "DescribeRepository", "")
	assert.Equal(t, 404, resp3.StatusCode)

	// Create another repo
	callREST(t, p, "POST", "/v1/repository?domain=test-domain", "CreateRepository",
		`{"domain": "test-domain", "repository": "second-repo"}`)

	// List all
	listResp := callREST(t, p, "POST", "/v1/repositories", "ListRepositories", "{}")
	assert.Equal(t, 200, listResp.StatusCode)
	listBody := parseBody(t, listResp)
	repos, ok := listBody["repositories"].([]any)
	require.True(t, ok)
	assert.Len(t, repos, 2)

	// List in domain
	listDomainResp := callREST(t, p, "POST", "/v1/domain/repositories?domain=test-domain", "ListRepositoriesInDomain", "{}")
	assert.Equal(t, 200, listDomainResp.StatusCode)
	listDomainBody := parseBody(t, listDomainResp)
	domainRepos, ok := listDomainBody["repositories"].([]any)
	require.True(t, ok)
	assert.Len(t, domainRepos, 2)

	// Update
	updateResp := callREST(t, p, "PUT", "/v1/repository?domain=test-domain&repository=my-repo", "UpdateRepository",
		`{"domain": "test-domain", "repository": "my-repo", "description": "updated description"}`)
	assert.Equal(t, 200, updateResp.StatusCode)
	updateBody := parseBody(t, updateResp)
	updRepo := updateBody["repository"].(map[string]any)
	assert.Equal(t, "updated description", updRepo["description"])

	// Delete
	deleteResp := callREST(t, p, "DELETE", "/v1/repository?domain=test-domain&repository=my-repo", "DeleteRepository", "")
	assert.Equal(t, 200, deleteResp.StatusCode)
	deleteBody := parseBody(t, deleteResp)
	deleted := deleteBody["repository"].(map[string]any)
	assert.Equal(t, "my-repo", deleted["name"])

	// Describe after delete
	resp4 := callREST(t, p, "GET", "/v1/repository?domain=test-domain&repository=my-repo", "DescribeRepository", "")
	assert.Equal(t, 404, resp4.StatusCode)

	// Delete non-existent
	resp5 := callREST(t, p, "DELETE", "/v1/repository?domain=test-domain&repository=nope", "DeleteRepository", "")
	assert.Equal(t, 404, resp5.StatusCode)
}

func TestPackageGroupCRUD(t *testing.T) {
	p := newTestProvider(t)

	// Create domain
	callREST(t, p, "POST", "/v1/domain", "CreateDomain", `{"domain": "test-domain"}`)

	// Create package group
	createBody := `{"domain": "test-domain", "packageGroup": "/npm/my-group", "description": "my group"}`
	resp := callREST(t, p, "POST", "/v1/package-group?domain=test-domain", "CreatePackageGroup", createBody)
	assert.Equal(t, 200, resp.StatusCode)
	rb := parseBody(t, resp)
	pg, ok := rb["packageGroup"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "/npm/my-group", pg["pattern"])
	assert.Equal(t, "my group", pg["description"])
	pgARN, _ := pg["arn"].(string)
	assert.NotEmpty(t, pgARN)

	// Duplicate → conflict
	resp2 := callREST(t, p, "POST", "/v1/package-group?domain=test-domain", "CreatePackageGroup", createBody)
	assert.Equal(t, 409, resp2.StatusCode)

	// Describe
	descResp := callREST(t, p, "GET", "/v1/package-group?domain=test-domain&packageGroup=%2Fnpm%2Fmy-group", "DescribePackageGroup", "")
	assert.Equal(t, 200, descResp.StatusCode)
	descBody := parseBody(t, descResp)
	pg2 := descBody["packageGroup"].(map[string]any)
	assert.Equal(t, "my group", pg2["description"])

	// Describe non-existent
	resp3 := callREST(t, p, "GET", "/v1/package-group?domain=test-domain&packageGroup=nope", "DescribePackageGroup", "")
	assert.Equal(t, 404, resp3.StatusCode)

	// Create another group
	callREST(t, p, "POST", "/v1/package-group?domain=test-domain", "CreatePackageGroup",
		`{"domain": "test-domain", "packageGroup": "/npm/second"}`)

	// List
	listResp := callREST(t, p, "POST", "/v1/package-groups?domain=test-domain", "ListPackageGroups", "{}")
	assert.Equal(t, 200, listResp.StatusCode)
	listBody := parseBody(t, listResp)
	groups, ok := listBody["packageGroups"].([]any)
	require.True(t, ok)
	assert.Len(t, groups, 2)

	// Update
	updateResp := callREST(t, p, "PUT", "/v1/package-group?domain=test-domain", "UpdatePackageGroup",
		`{"domain": "test-domain", "packageGroup": "/npm/my-group", "description": "updated group"}`)
	assert.Equal(t, 200, updateResp.StatusCode)
	updateBody := parseBody(t, updateResp)
	updPG := updateBody["packageGroup"].(map[string]any)
	assert.Equal(t, "updated group", updPG["description"])

	// Delete
	deleteResp := callREST(t, p, "DELETE", "/v1/package-group?domain=test-domain&packageGroup=%2Fnpm%2Fmy-group", "DeletePackageGroup", "")
	assert.Equal(t, 200, deleteResp.StatusCode)
	deleteBody := parseBody(t, deleteResp)
	deleted := deleteBody["packageGroup"].(map[string]any)
	assert.Equal(t, "/npm/my-group", deleted["pattern"])

	// Describe after delete
	resp4 := callREST(t, p, "GET", "/v1/package-group?domain=test-domain&packageGroup=%2Fnpm%2Fmy-group", "DescribePackageGroup", "")
	assert.Equal(t, 404, resp4.StatusCode)

	// Delete non-existent
	resp5 := callREST(t, p, "DELETE", "/v1/package-group?domain=test-domain&packageGroup=nope", "DeletePackageGroup", "")
	assert.Equal(t, 404, resp5.StatusCode)
}

func TestGetAuthorizationToken(t *testing.T) {
	p := newTestProvider(t)

	// Create domain
	callREST(t, p, "POST", "/v1/domain", "CreateDomain", `{"domain": "my-domain"}`)

	// Get token
	resp := callREST(t, p, "POST", "/v1/authorization-token?domain=my-domain", "GetAuthorizationToken", "{}")
	assert.Equal(t, 200, resp.StatusCode)
	rb := parseBody(t, resp)
	token, ok := rb["authorizationToken"].(string)
	require.True(t, ok)
	assert.NotEmpty(t, token)
	_, ok = rb["expiration"]
	assert.True(t, ok)

	// Missing domain
	resp2 := callREST(t, p, "POST", "/v1/authorization-token", "GetAuthorizationToken", "{}")
	assert.Equal(t, 400, resp2.StatusCode)
}

func TestTags(t *testing.T) {
	p := newTestProvider(t)

	// Create domain with tags
	createBody := `{"domain": "my-domain", "tags": [{"key": "Env", "value": "prod"}]}`
	resp := callREST(t, p, "POST", "/v1/domain", "CreateDomain", createBody)
	assert.Equal(t, 200, resp.StatusCode)
	rb := parseBody(t, resp)
	domain := rb["domain"].(map[string]any)
	arn, _ := domain["arn"].(string)
	require.NotEmpty(t, arn)

	// ListTagsForResource
	lr := callREST(t, p, "POST", "/v1/tags?resourceArn="+arn, "ListTagsForResource", "{}")
	assert.Equal(t, 200, lr.StatusCode)
	lrb := parseBody(t, lr)
	tagsList, ok := lrb["tags"].([]any)
	require.True(t, ok)
	assert.Len(t, tagsList, 1)

	// TagResource — add more tags
	tagBody := `{"resourceArn": "` + arn + `", "tags": [{"key": "Team", "value": "platform"}, {"key": "Owner", "value": "alice"}]}`
	tr := callREST(t, p, "POST", "/v1/tag?resourceArn="+arn, "TagResource", tagBody)
	assert.Equal(t, 200, tr.StatusCode)

	// Verify 3 tags
	lr2 := callREST(t, p, "POST", "/v1/tags?resourceArn="+arn, "ListTagsForResource", "{}")
	lrb2 := parseBody(t, lr2)
	tagsList2, ok := lrb2["tags"].([]any)
	require.True(t, ok)
	assert.Len(t, tagsList2, 3)

	// UntagResource
	req := httptest.NewRequest("POST", "/v1/untag?resourceArn="+arn+"&tagKeys=Env&tagKeys=Team", strings.NewReader("{}"))
	untagResp, err := p.HandleRequest(context.Background(), "UntagResource", req)
	require.NoError(t, err)
	assert.Equal(t, 200, untagResp.StatusCode)

	// Verify only Owner remains
	lr3 := callREST(t, p, "POST", "/v1/tags?resourceArn="+arn, "ListTagsForResource", "{}")
	lrb3 := parseBody(t, lr3)
	tagsList3, ok := lrb3["tags"].([]any)
	require.True(t, ok)
	assert.Len(t, tagsList3, 1)
	firstTag := tagsList3[0].(map[string]any)
	assert.Equal(t, "Owner", firstTag["key"])
	assert.Equal(t, "alice", firstTag["value"])
}
