// SPDX-License-Identifier: Apache-2.0

// internal/services/codecommit/provider_test.go
package codecommit

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

func call(t *testing.T, p *Provider, op string, body string) *plugin.Response {
	t.Helper()
	req := httptest.NewRequest("POST", "/", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
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

func TestRepositoryCRUD(t *testing.T) {
	p := newTestProvider(t)

	// CreateRepository
	resp := call(t, p, "CreateRepository", `{"repositoryName": "my-repo", "repositoryDescription": "test repo"}`)
	assert.Equal(t, 200, resp.StatusCode)
	rb := parseBody(t, resp)
	meta, _ := rb["repositoryMetadata"].(map[string]any)
	assert.Equal(t, "my-repo", meta["repositoryName"])
	assert.NotEmpty(t, meta["repositoryId"])
	assert.NotEmpty(t, meta["arn"])

	repoARN, _ := meta["arn"].(string)

	// Duplicate create → error
	resp2 := call(t, p, "CreateRepository", `{"repositoryName": "my-repo"}`)
	assert.Equal(t, 400, resp2.StatusCode)
	rb2 := parseBody(t, resp2)
	assert.Equal(t, "RepositoryNameExistsException", rb2["__type"])

	// GetRepository
	resp3 := call(t, p, "GetRepository", `{"repositoryName": "my-repo"}`)
	assert.Equal(t, 200, resp3.StatusCode)
	rb3 := parseBody(t, resp3)
	meta3, _ := rb3["repositoryMetadata"].(map[string]any)
	assert.Equal(t, "my-repo", meta3["repositoryName"])
	assert.Equal(t, "test repo", meta3["repositoryDescription"])

	// GetRepository not found
	resp4 := call(t, p, "GetRepository", `{"repositoryName": "missing"}`)
	assert.Equal(t, 400, resp4.StatusCode)

	// Create second repo
	call(t, p, "CreateRepository", `{"repositoryName": "repo-two"}`)

	// ListRepositories
	listResp := call(t, p, "ListRepositories", `{}`)
	assert.Equal(t, 200, listResp.StatusCode)
	lb := parseBody(t, listResp)
	repos, _ := lb["repositories"].([]any)
	assert.Len(t, repos, 2)

	// BatchGetRepositories
	batchResp := call(t, p, "BatchGetRepositories", `{"repositoryNames": ["my-repo", "repo-two", "missing"]}`)
	assert.Equal(t, 200, batchResp.StatusCode)
	bb := parseBody(t, batchResp)
	found, _ := bb["repositories"].([]any)
	assert.Len(t, found, 2)
	notFound, _ := bb["repositoriesNotFound"].([]any)
	assert.Len(t, notFound, 1)
	assert.Equal(t, "missing", notFound[0])

	// UpdateRepositoryDescription
	updDesc := call(t, p, "UpdateRepositoryDescription", `{"repositoryName": "my-repo", "repositoryDescription": "updated"}`)
	assert.Equal(t, 200, updDesc.StatusCode)
	resp5 := call(t, p, "GetRepository", `{"repositoryName": "my-repo"}`)
	rb5 := parseBody(t, resp5)
	meta5, _ := rb5["repositoryMetadata"].(map[string]any)
	assert.Equal(t, "updated", meta5["repositoryDescription"])

	// UpdateRepositoryName
	updName := call(t, p, "UpdateRepositoryName", `{"oldName": "repo-two", "newName": "repo-renamed"}`)
	assert.Equal(t, 200, updName.StatusCode)
	resp6 := call(t, p, "GetRepository", `{"repositoryName": "repo-renamed"}`)
	assert.Equal(t, 200, resp6.StatusCode)

	// Tags via TagResource
	tagResp := call(t, p, "TagResource", `{"resourceArn": "`+repoARN+`", "tags": {"Env": "test", "Team": "dev"}}`)
	assert.Equal(t, 200, tagResp.StatusCode)

	listTagsResp := call(t, p, "ListTagsForResource", `{"resourceArn": "`+repoARN+`"}`)
	assert.Equal(t, 200, listTagsResp.StatusCode)
	lt := parseBody(t, listTagsResp)
	tags, _ := lt["tags"].(map[string]any)
	assert.Equal(t, "test", tags["Env"])
	assert.Equal(t, "dev", tags["Team"])

	// UntagResource
	untagResp := call(t, p, "UntagResource", `{"resourceArn": "`+repoARN+`", "tagKeys": ["Env"]}`)
	assert.Equal(t, 200, untagResp.StatusCode)
	lt2 := parseBody(t, call(t, p, "ListTagsForResource", `{"resourceArn": "`+repoARN+`"}`))
	tags2, _ := lt2["tags"].(map[string]any)
	assert.NotContains(t, tags2, "Env")
	assert.Contains(t, tags2, "Team")

	// DeleteRepository
	delResp := call(t, p, "DeleteRepository", `{"repositoryName": "my-repo"}`)
	assert.Equal(t, 200, delResp.StatusCode)
	delBody := parseBody(t, delResp)
	assert.NotEmpty(t, delBody["repositoryId"])

	resp7 := call(t, p, "GetRepository", `{"repositoryName": "my-repo"}`)
	assert.Equal(t, 400, resp7.StatusCode)
}

func TestBranchCRUD(t *testing.T) {
	p := newTestProvider(t)

	// Setup repo
	call(t, p, "CreateRepository", `{"repositoryName": "branch-repo"}`)

	// CreateBranch
	resp := call(t, p, "CreateBranch", `{"repositoryName": "branch-repo", "branchName": "feature-1", "commitId": "abc123"}`)
	assert.Equal(t, 200, resp.StatusCode)

	// Duplicate → error
	resp2 := call(t, p, "CreateBranch", `{"repositoryName": "branch-repo", "branchName": "feature-1", "commitId": "abc123"}`)
	assert.Equal(t, 400, resp2.StatusCode)
	rb2 := parseBody(t, resp2)
	assert.Equal(t, "BranchNameExistsException", rb2["__type"])

	// GetBranch
	resp3 := call(t, p, "GetBranch", `{"repositoryName": "branch-repo", "branchName": "feature-1"}`)
	assert.Equal(t, 200, resp3.StatusCode)
	rb3 := parseBody(t, resp3)
	branch, _ := rb3["branch"].(map[string]any)
	assert.Equal(t, "feature-1", branch["branchName"])
	assert.Equal(t, "abc123", branch["commitId"])

	// GetBranch not found
	resp4 := call(t, p, "GetBranch", `{"repositoryName": "branch-repo", "branchName": "missing"}`)
	assert.Equal(t, 400, resp4.StatusCode)

	// Create second branch
	call(t, p, "CreateBranch", `{"repositoryName": "branch-repo", "branchName": "feature-2", "commitId": "def456"}`)

	// ListBranches
	listResp := call(t, p, "ListBranches", `{"repositoryName": "branch-repo"}`)
	assert.Equal(t, 200, listResp.StatusCode)
	lb := parseBody(t, listResp)
	branches, _ := lb["branches"].([]any)
	assert.Len(t, branches, 2)

	// UpdateDefaultBranch
	updResp := call(t, p, "UpdateDefaultBranch", `{"repositoryName": "branch-repo", "defaultBranchName": "feature-1"}`)
	assert.Equal(t, 200, updResp.StatusCode)
	repoResp := call(t, p, "GetRepository", `{"repositoryName": "branch-repo"}`)
	rb := parseBody(t, repoResp)
	meta, _ := rb["repositoryMetadata"].(map[string]any)
	assert.Equal(t, "feature-1", meta["defaultBranch"])

	// DeleteBranch
	delResp := call(t, p, "DeleteBranch", `{"repositoryName": "branch-repo", "branchName": "feature-2"}`)
	assert.Equal(t, 200, delResp.StatusCode)
	delBody := parseBody(t, delResp)
	deleted, _ := delBody["deletedBranch"].(map[string]any)
	assert.Equal(t, "feature-2", deleted["branchName"])

	// ListBranches → 1 left
	listResp2 := call(t, p, "ListBranches", `{"repositoryName": "branch-repo"}`)
	lb2 := parseBody(t, listResp2)
	branches2, _ := lb2["branches"].([]any)
	assert.Len(t, branches2, 1)
}

func TestPullRequestCRUD(t *testing.T) {
	p := newTestProvider(t)

	// Setup repo
	call(t, p, "CreateRepository", `{"repositoryName": "pr-repo"}`)

	// CreatePullRequest
	createBody := `{"title": "My PR", "description": "fix something", "targets": [{"repositoryName": "pr-repo", "sourceReference": "feature", "destinationReference": "main"}]}`
	resp := call(t, p, "CreatePullRequest", createBody)
	assert.Equal(t, 200, resp.StatusCode)
	rb := parseBody(t, resp)
	pr, _ := rb["pullRequest"].(map[string]any)
	assert.Equal(t, "My PR", pr["title"])
	assert.Equal(t, "OPEN", pr["pullRequestStatus"])
	prID, _ := pr["pullRequestId"].(string)
	assert.NotEmpty(t, prID)

	// GetPullRequest
	getResp := call(t, p, "GetPullRequest", `{"pullRequestId": "`+prID+`"}`)
	assert.Equal(t, 200, getResp.StatusCode)
	gb := parseBody(t, getResp)
	gpr, _ := gb["pullRequest"].(map[string]any)
	assert.Equal(t, prID, gpr["pullRequestId"])

	// GetPullRequest not found
	notFoundResp := call(t, p, "GetPullRequest", `{"pullRequestId": "missing"}`)
	assert.Equal(t, 400, notFoundResp.StatusCode)

	// ListPullRequests
	listResp := call(t, p, "ListPullRequests", `{"repositoryName": "pr-repo"}`)
	assert.Equal(t, 200, listResp.StatusCode)
	lb := parseBody(t, listResp)
	ids, _ := lb["pullRequestIds"].([]any)
	assert.Len(t, ids, 1)

	// UpdatePullRequestTitle
	updTitle := call(t, p, "UpdatePullRequestTitle", `{"pullRequestId": "`+prID+`", "title": "Updated PR"}`)
	assert.Equal(t, 200, updTitle.StatusCode)
	ub := parseBody(t, updTitle)
	upr, _ := ub["pullRequest"].(map[string]any)
	assert.Equal(t, "Updated PR", upr["title"])

	// UpdatePullRequestDescription
	updDesc := call(t, p, "UpdatePullRequestDescription", `{"pullRequestId": "`+prID+`", "description": "new desc"}`)
	assert.Equal(t, 200, updDesc.StatusCode)

	// UpdatePullRequestStatus → close
	updStatus := call(t, p, "UpdatePullRequestStatus", `{"pullRequestId": "`+prID+`", "pullRequestStatus": "CLOSED"}`)
	assert.Equal(t, 200, updStatus.StatusCode)
	sb := parseBody(t, updStatus)
	spr, _ := sb["pullRequest"].(map[string]any)
	assert.Equal(t, "CLOSED", spr["pullRequestStatus"])

	// PR Approval Rules
	createRuleResp := call(t, p, "CreatePullRequestApprovalRule", `{"pullRequestId": "`+prID+`", "approvalRuleName": "rule-1", "approvalRuleContent": "{}"}`)
	assert.Equal(t, 200, createRuleResp.StatusCode)

	updRuleResp := call(t, p, "UpdatePullRequestApprovalRuleContent", `{"pullRequestId": "`+prID+`", "approvalRuleName": "rule-1", "newRuleContent": "{\"version\": \"2018-11-08\"}"}`)
	assert.Equal(t, 200, updRuleResp.StatusCode)

	delRuleResp := call(t, p, "DeletePullRequestApprovalRule", `{"pullRequestId": "`+prID+`", "approvalRuleName": "rule-1"}`)
	assert.Equal(t, 200, delRuleResp.StatusCode)

	// PR approval states
	approvalResp := call(t, p, "GetPullRequestApprovalStates", `{"pullRequestId": "`+prID+`"}`)
	assert.Equal(t, 200, approvalResp.StatusCode)

	// Override
	overrideResp := call(t, p, "OverridePullRequestApprovalRules", `{"pullRequestId": "`+prID+`", "overrideStatus": "OVERRIDE"}`)
	assert.Equal(t, 200, overrideResp.StatusCode)

	overrideState := call(t, p, "GetPullRequestOverrideState", `{"pullRequestId": "`+prID+`"}`)
	assert.Equal(t, 200, overrideState.StatusCode)
	osb := parseBody(t, overrideState)
	assert.Equal(t, true, osb["overridden"])

	// Evaluate
	evalResp := call(t, p, "EvaluatePullRequestApprovalRules", `{"pullRequestId": "`+prID+`"}`)
	assert.Equal(t, 200, evalResp.StatusCode)
}

func TestApprovalRuleTemplateCRUD(t *testing.T) {
	p := newTestProvider(t)

	// CreateApprovalRuleTemplate
	resp := call(t, p, "CreateApprovalRuleTemplate", `{"approvalRuleTemplateName": "my-template", "approvalRuleTemplateDescription": "test template", "approvalRuleTemplateContent": "{\"version\": \"2018-11-08\"}"}`)
	assert.Equal(t, 200, resp.StatusCode)
	rb := parseBody(t, resp)
	tpl, _ := rb["approvalRuleTemplate"].(map[string]any)
	assert.Equal(t, "my-template", tpl["approvalRuleTemplateName"])
	assert.NotEmpty(t, tpl["approvalRuleTemplateId"])

	// Duplicate → error
	resp2 := call(t, p, "CreateApprovalRuleTemplate", `{"approvalRuleTemplateName": "my-template"}`)
	assert.Equal(t, 400, resp2.StatusCode)
	rb2 := parseBody(t, resp2)
	assert.Equal(t, "ApprovalRuleTemplateNameAlreadyExistsException", rb2["__type"])

	// GetApprovalRuleTemplate
	resp3 := call(t, p, "GetApprovalRuleTemplate", `{"approvalRuleTemplateName": "my-template"}`)
	assert.Equal(t, 200, resp3.StatusCode)

	// GetApprovalRuleTemplate not found
	resp4 := call(t, p, "GetApprovalRuleTemplate", `{"approvalRuleTemplateName": "missing"}`)
	assert.Equal(t, 400, resp4.StatusCode)

	// Create second template
	call(t, p, "CreateApprovalRuleTemplate", `{"approvalRuleTemplateName": "tpl-two"}`)

	// ListApprovalRuleTemplates
	listResp := call(t, p, "ListApprovalRuleTemplates", `{}`)
	assert.Equal(t, 200, listResp.StatusCode)
	lb := parseBody(t, listResp)
	names, _ := lb["approvalRuleTemplateNames"].([]any)
	assert.Len(t, names, 2)

	// UpdateApprovalRuleTemplateDescription
	updDesc := call(t, p, "UpdateApprovalRuleTemplateDescription", `{"approvalRuleTemplateName": "my-template", "approvalRuleTemplateDescription": "updated desc"}`)
	assert.Equal(t, 200, updDesc.StatusCode)

	// UpdateApprovalRuleTemplateContent
	updContent := call(t, p, "UpdateApprovalRuleTemplateContent", `{"approvalRuleTemplateName": "my-template", "newRuleContent": "{\"version\": \"2018-11-08\", \"statements\": []}"}`)
	assert.Equal(t, 200, updContent.StatusCode)

	// UpdateApprovalRuleTemplateName
	updName := call(t, p, "UpdateApprovalRuleTemplateName", `{"oldApprovalRuleTemplateName": "tpl-two", "newApprovalRuleTemplateName": "tpl-renamed"}`)
	assert.Equal(t, 200, updName.StatusCode)
	rb3 := parseBody(t, updName)
	tpl3, _ := rb3["approvalRuleTemplate"].(map[string]any)
	assert.Equal(t, "tpl-renamed", tpl3["approvalRuleTemplateName"])

	// Associate with repositories
	call(t, p, "CreateRepository", `{"repositoryName": "assoc-repo-1"}`)
	call(t, p, "CreateRepository", `{"repositoryName": "assoc-repo-2"}`)

	assocResp := call(t, p, "AssociateApprovalRuleTemplateWithRepository", `{"approvalRuleTemplateName": "my-template", "repositoryName": "assoc-repo-1"}`)
	assert.Equal(t, 200, assocResp.StatusCode)

	batchAssocResp := call(t, p, "BatchAssociateApprovalRuleTemplateWithRepositories", `{"approvalRuleTemplateName": "my-template", "repositoryNames": ["assoc-repo-1", "assoc-repo-2"]}`)
	assert.Equal(t, 200, batchAssocResp.StatusCode)

	listAssocResp := call(t, p, "ListAssociatedApprovalRuleTemplatesForRepository", `{"repositoryName": "assoc-repo-1"}`)
	assert.Equal(t, 200, listAssocResp.StatusCode)
	lar := parseBody(t, listAssocResp)
	tplNames, _ := lar["approvalRuleTemplateNames"].([]any)
	assert.Len(t, tplNames, 1)

	listReposResp := call(t, p, "ListRepositoriesForApprovalRuleTemplate", `{"approvalRuleTemplateName": "my-template"}`)
	assert.Equal(t, 200, listReposResp.StatusCode)
	lrr := parseBody(t, listReposResp)
	repoNames, _ := lrr["repositoryNames"].([]any)
	assert.Len(t, repoNames, 2)

	disassocResp := call(t, p, "DisassociateApprovalRuleTemplateFromRepository", `{"approvalRuleTemplateName": "my-template", "repositoryName": "assoc-repo-1"}`)
	assert.Equal(t, 200, disassocResp.StatusCode)

	batchDisassocResp := call(t, p, "BatchDisassociateApprovalRuleTemplateFromRepositories", `{"approvalRuleTemplateName": "my-template", "repositoryNames": ["assoc-repo-2"]}`)
	assert.Equal(t, 200, batchDisassocResp.StatusCode)

	listReposResp2 := call(t, p, "ListRepositoriesForApprovalRuleTemplate", `{"approvalRuleTemplateName": "my-template"}`)
	lrr2 := parseBody(t, listReposResp2)
	repoNames2, _ := lrr2["repositoryNames"].([]any)
	assert.Len(t, repoNames2, 0)

	// DeleteApprovalRuleTemplate
	delResp := call(t, p, "DeleteApprovalRuleTemplate", `{"approvalRuleTemplateName": "my-template"}`)
	assert.Equal(t, 200, delResp.StatusCode)
	db := parseBody(t, delResp)
	assert.NotEmpty(t, db["approvalRuleTemplateId"])

	resp5 := call(t, p, "GetApprovalRuleTemplate", `{"approvalRuleTemplateName": "my-template"}`)
	assert.Equal(t, 400, resp5.StatusCode)
}

func TestCommentCRUD(t *testing.T) {
	p := newTestProvider(t)

	// Setup
	call(t, p, "CreateRepository", `{"repositoryName": "comment-repo"}`)
	createPR := call(t, p, "CreatePullRequest", `{"title": "PR for comments", "targets": [{"repositoryName": "comment-repo", "sourceReference": "feat", "destinationReference": "main"}]}`)
	prBody := parseBody(t, createPR)
	prMap, _ := prBody["pullRequest"].(map[string]any)
	prID, _ := prMap["pullRequestId"].(string)

	// PostCommentForPullRequest
	postResp := call(t, p, "PostCommentForPullRequest", `{"pullRequestId": "`+prID+`", "repositoryName": "comment-repo", "content": "This looks good!"}`)
	assert.Equal(t, 200, postResp.StatusCode)
	pb := parseBody(t, postResp)
	commentMap, _ := pb["comment"].(map[string]any)
	assert.Equal(t, "This looks good!", commentMap["content"])
	commentID, _ := commentMap["commentId"].(string)
	assert.NotEmpty(t, commentID)

	// PostCommentForComparedCommit
	postResp2 := call(t, p, "PostCommentForComparedCommit", `{"repositoryName": "comment-repo", "beforeCommitId": "abc", "afterCommitId": "def", "content": "Nice change"}`)
	assert.Equal(t, 200, postResp2.StatusCode)

	// PostCommentReply
	replyResp := call(t, p, "PostCommentReply", `{"inReplyTo": "`+commentID+`", "content": "Thanks!"}`)
	assert.Equal(t, 200, replyResp.StatusCode)
	rb := parseBody(t, replyResp)
	replyMap, _ := rb["comment"].(map[string]any)
	assert.Equal(t, commentID, replyMap["inReplyTo"])

	// GetComment
	getResp := call(t, p, "GetComment", `{"commentId": "`+commentID+`"}`)
	assert.Equal(t, 200, getResp.StatusCode)
	gb := parseBody(t, getResp)
	gc, _ := gb["comment"].(map[string]any)
	assert.Equal(t, "This looks good!", gc["content"])

	// GetComment not found
	getResp2 := call(t, p, "GetComment", `{"commentId": "missing"}`)
	assert.Equal(t, 400, getResp2.StatusCode)

	// GetCommentsForPullRequest
	listResp := call(t, p, "GetCommentsForPullRequest", `{"pullRequestId": "`+prID+`"}`)
	assert.Equal(t, 200, listResp.StatusCode)

	// GetCommentsForComparedCommit
	listResp2 := call(t, p, "GetCommentsForComparedCommit", `{"repositoryName": "comment-repo", "beforeCommitId": "abc", "afterCommitId": "def"}`)
	assert.Equal(t, 200, listResp2.StatusCode)

	// UpdateComment
	updResp := call(t, p, "UpdateComment", `{"commentId": "`+commentID+`", "content": "Updated content"}`)
	assert.Equal(t, 200, updResp.StatusCode)
	ub := parseBody(t, updResp)
	uc, _ := ub["comment"].(map[string]any)
	assert.Equal(t, "Updated content", uc["content"])

	// PutCommentReaction
	reactResp := call(t, p, "PutCommentReaction", `{"commentId": "`+commentID+`", "reactionValue": ":thumbsup:"}`)
	assert.Equal(t, 200, reactResp.StatusCode)

	// GetCommentReactions
	reactionResp := call(t, p, "GetCommentReactions", `{"commentId": "`+commentID+`"}`)
	assert.Equal(t, 200, reactionResp.StatusCode)
	rrb := parseBody(t, reactionResp)
	reactions, _ := rrb["reactionsForComment"].([]any)
	assert.Len(t, reactions, 1)

	// DeleteCommentContent
	delResp := call(t, p, "DeleteCommentContent", `{"commentId": "`+commentID+`"}`)
	assert.Equal(t, 200, delResp.StatusCode)
	db := parseBody(t, delResp)
	dc, _ := db["comment"].(map[string]any)
	assert.Equal(t, true, dc["deleted"])
	assert.Equal(t, "", dc["content"])
}

func TestTags(t *testing.T) {
	p := newTestProvider(t)

	// CreateRepository with tags in request
	resp := call(t, p, "CreateRepository", `{"repositoryName": "tagged-repo", "tags": {"Project": "myapp", "Stage": "prod"}}`)
	assert.Equal(t, 200, resp.StatusCode)
	rb := parseBody(t, resp)
	meta, _ := rb["repositoryMetadata"].(map[string]any)
	arn, _ := meta["arn"].(string)

	// ListTagsForResource
	listResp := call(t, p, "ListTagsForResource", `{"resourceArn": "`+arn+`"}`)
	assert.Equal(t, 200, listResp.StatusCode)
	lb := parseBody(t, listResp)
	tags, _ := lb["tags"].(map[string]any)
	assert.Equal(t, "myapp", tags["Project"])
	assert.Equal(t, "prod", tags["Stage"])

	// TagResource - add more tags
	tagResp := call(t, p, "TagResource", `{"resourceArn": "`+arn+`", "tags": {"Owner": "alice"}}`)
	assert.Equal(t, 200, tagResp.StatusCode)

	listResp2 := call(t, p, "ListTagsForResource", `{"resourceArn": "`+arn+`"}`)
	lb2 := parseBody(t, listResp2)
	tags2, _ := lb2["tags"].(map[string]any)
	assert.Len(t, tags2, 3)

	// UntagResource
	untagResp := call(t, p, "UntagResource", `{"resourceArn": "`+arn+`", "tagKeys": ["Stage", "Owner"]}`)
	assert.Equal(t, 200, untagResp.StatusCode)

	listResp3 := call(t, p, "ListTagsForResource", `{"resourceArn": "`+arn+`"}`)
	lb3 := parseBody(t, listResp3)
	tags3, _ := lb3["tags"].(map[string]any)
	assert.Len(t, tags3, 1)
	assert.Equal(t, "myapp", tags3["Project"])
}
