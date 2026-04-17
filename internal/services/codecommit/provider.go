// SPDX-License-Identifier: Apache-2.0

// internal/services/codecommit/provider.go
package codecommit

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"strings"

	"github.com/skyoo2003/devcloud/internal/plugin"
	"github.com/skyoo2003/devcloud/internal/shared"
)

type Provider struct {
	store *Store
}

func (p *Provider) ServiceID() string             { return "codecommit" }
func (p *Provider) ServiceName() string           { return "CodeCommit_20150413" }
func (p *Provider) Protocol() plugin.ProtocolType { return plugin.ProtocolJSON11 }

func (p *Provider) Init(cfg plugin.PluginConfig) error {
	dataDir := cfg.DataDir
	if dataDir == "" {
		dataDir = "."
	}
	var err error
	p.store, err = NewStore(filepath.Join(dataDir, "codecommit"))
	return err
}

func (p *Provider) Shutdown(_ context.Context) error {
	if p.store != nil {
		return p.store.Close()
	}
	return nil
}

func (p *Provider) HandleRequest(_ context.Context, op string, req *http.Request) (*plugin.Response, error) {
	body, err := io.ReadAll(req.Body)
	if err != nil {
		return shared.JSONError("SerializationException", "failed to read body", http.StatusBadRequest), nil
	}
	var params map[string]any
	if len(body) > 0 {
		if err := json.Unmarshal(body, &params); err != nil {
			return shared.JSONError("SerializationException", "invalid JSON", http.StatusBadRequest), nil
		}
	} else {
		params = map[string]any{}
	}

	action := op
	if action == "" {
		target := req.Header.Get("X-Amz-Target")
		if idx := strings.LastIndex(target, "."); idx >= 0 {
			action = target[idx+1:]
		}
	}

	switch action {
	// Repository ops
	case "CreateRepository":
		return p.createRepository(params)
	case "GetRepository":
		return p.getRepository(params)
	case "ListRepositories":
		return p.listRepositories(params)
	case "UpdateRepositoryName":
		return p.updateRepositoryName(params)
	case "UpdateRepositoryDescription":
		return p.updateRepositoryDescription(params)
	case "DeleteRepository":
		return p.deleteRepository(params)
	case "BatchGetRepositories":
		return p.batchGetRepositories(params)

	// Branch ops
	case "CreateBranch":
		return p.createBranch(params)
	case "GetBranch":
		return p.getBranch(params)
	case "ListBranches":
		return p.listBranches(params)
	case "UpdateDefaultBranch":
		return p.updateDefaultBranch(params)
	case "DeleteBranch":
		return p.deleteBranch(params)

	// Pull Request ops
	case "CreatePullRequest":
		return p.createPullRequest(params)
	case "GetPullRequest":
		return p.getPullRequest(params)
	case "ListPullRequests":
		return p.listPullRequests(params)
	case "UpdatePullRequestTitle":
		return p.updatePullRequestTitle(params)
	case "UpdatePullRequestDescription":
		return p.updatePullRequestDescription(params)
	case "UpdatePullRequestStatus":
		return p.updatePullRequestStatus(params)

	// PR Approval Rule ops
	case "CreatePullRequestApprovalRule":
		return p.createPullRequestApprovalRule(params)
	case "DeletePullRequestApprovalRule":
		return p.deletePullRequestApprovalRule(params)
	case "UpdatePullRequestApprovalRuleContent":
		return p.updatePullRequestApprovalRuleContent(params)
	case "UpdatePullRequestApprovalState":
		return p.updatePullRequestApprovalState(params)
	case "EvaluatePullRequestApprovalRules":
		return p.evaluatePullRequestApprovalRules(params)
	case "GetPullRequestApprovalStates":
		return p.getPullRequestApprovalStates(params)
	case "GetPullRequestOverrideState":
		return p.getPullRequestOverrideState(params)
	case "OverridePullRequestApprovalRules":
		return p.overridePullRequestApprovalRules(params)

	// Approval Rule Template ops
	case "CreateApprovalRuleTemplate":
		return p.createApprovalRuleTemplate(params)
	case "GetApprovalRuleTemplate":
		return p.getApprovalRuleTemplate(params)
	case "ListApprovalRuleTemplates":
		return p.listApprovalRuleTemplates(params)
	case "UpdateApprovalRuleTemplateName":
		return p.updateApprovalRuleTemplateName(params)
	case "UpdateApprovalRuleTemplateDescription":
		return p.updateApprovalRuleTemplateDescription(params)
	case "UpdateApprovalRuleTemplateContent":
		return p.updateApprovalRuleTemplateContent(params)
	case "DeleteApprovalRuleTemplate":
		return p.deleteApprovalRuleTemplate(params)
	case "AssociateApprovalRuleTemplateWithRepository":
		return p.associateApprovalRuleTemplateWithRepository(params)
	case "BatchAssociateApprovalRuleTemplateWithRepositories":
		return p.batchAssociateApprovalRuleTemplateWithRepositories(params)
	case "DisassociateApprovalRuleTemplateFromRepository":
		return p.disassociateApprovalRuleTemplateFromRepository(params)
	case "BatchDisassociateApprovalRuleTemplateFromRepositories":
		return p.batchDisassociateApprovalRuleTemplateFromRepositories(params)
	case "ListAssociatedApprovalRuleTemplatesForRepository":
		return p.listAssociatedApprovalRuleTemplatesForRepository(params)
	case "ListRepositoriesForApprovalRuleTemplate":
		return p.listRepositoriesForApprovalRuleTemplate(params)

	// Comment ops
	case "PostCommentForPullRequest":
		return p.postCommentForPullRequest(params)
	case "PostCommentForComparedCommit":
		return p.postCommentForComparedCommit(params)
	case "PostCommentReply":
		return p.postCommentReply(params)
	case "GetComment":
		return p.getComment(params)
	case "GetCommentsForPullRequest":
		return p.getCommentsForPullRequest(params)
	case "GetCommentsForComparedCommit":
		return p.getCommentsForComparedCommit(params)
	case "UpdateComment":
		return p.updateComment(params)
	case "DeleteCommentContent":
		return p.deleteCommentContent(params)
	case "PutCommentReaction":
		return p.putCommentReaction(params)
	case "GetCommentReactions":
		return p.getCommentReactions(params)

	// Tag ops
	case "TagResource":
		return p.tagResource(params)
	case "UntagResource":
		return p.untagResource(params)
	case "ListTagsForResource":
		return p.listTagsForResource(params)

	// Stub ops (return success/empty)
	case "GetFile", "GetFolder", "GetBlob", "GetCommit", "CreateCommit",
		"PutFile", "DeleteFile", "GetDifferences", "GetRepositoryTriggers",
		"PutRepositoryTriggers", "TestRepositoryTriggers",
		"MergeBranchesByFastForward", "MergeBranchesBySquash", "MergeBranchesByThreeWay",
		"MergePullRequestByFastForward", "MergePullRequestBySquash", "MergePullRequestByThreeWay",
		"GetMergeCommit", "GetMergeConflicts", "GetMergeOptions",
		"BatchDescribeMergeConflicts", "DescribeMergeConflicts", "CreateUnreferencedMergeCommit",
		"BatchGetCommits", "DescribePullRequestEvents", "ListFileCommitHistory",
		"UpdateRepositoryEncryptionKey":
		return shared.JSONResponse(http.StatusOK, map[string]any{})

	default:
		return shared.JSONError("InvalidAction", fmt.Sprintf("unknown action: %s", action), http.StatusBadRequest), nil
	}
}

func (p *Provider) ListResources(_ context.Context) ([]plugin.Resource, error) {
	repos, err := p.store.ListRepositories()
	if err != nil {
		return nil, err
	}
	res := make([]plugin.Resource, 0, len(repos))
	for _, r := range repos {
		res = append(res, plugin.Resource{Type: "repository", ID: r.ID, Name: r.Name})
	}
	return res, nil
}

func (p *Provider) GetMetrics(_ context.Context) (*plugin.ServiceMetrics, error) {
	return &plugin.ServiceMetrics{}, nil
}

// ---- Repository handlers ----

func (p *Provider) createRepository(params map[string]any) (*plugin.Response, error) {
	name, _ := params["repositoryName"].(string)
	if name == "" {
		return shared.JSONError("ValidationException", "repositoryName is required", http.StatusBadRequest), nil
	}
	description, _ := params["repositoryDescription"].(string)
	id := shared.GenerateUUID()
	arn := shared.BuildARN("codecommit", "repository", name)
	cloneURL := fmt.Sprintf("https://git-codecommit.us-east-1.amazonaws.com/v1/repos/%s", name)

	repo, err := p.store.CreateRepository(name, id, arn, description, cloneURL)
	if err != nil {
		if sqliteIsUnique(err) {
			return shared.JSONError("RepositoryNameExistsException", "repository already exists", http.StatusBadRequest), nil
		}
		return nil, err
	}
	// Handle tags
	if rawTags, ok := params["tags"].(map[string]any); ok {
		tags := flatTags(rawTags)
		p.store.tags.AddTags(arn, tags)
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"repositoryMetadata": repoToMap(repo),
	})
}

func (p *Provider) getRepository(params map[string]any) (*plugin.Response, error) {
	name, _ := params["repositoryName"].(string)
	if name == "" {
		return shared.JSONError("ValidationException", "repositoryName is required", http.StatusBadRequest), nil
	}
	repo, err := p.store.GetRepository(name)
	if err != nil {
		return shared.JSONError("RepositoryDoesNotExistException", "repository not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"repositoryMetadata": repoToMap(repo),
	})
}

func (p *Provider) listRepositories(params map[string]any) (*plugin.Response, error) {
	repos, err := p.store.ListRepositories()
	if err != nil {
		return nil, err
	}
	list := make([]map[string]any, 0, len(repos))
	for _, r := range repos {
		list = append(list, map[string]any{
			"repositoryId":   r.ID,
			"repositoryName": r.Name,
		})
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"repositories": list,
	})
}

func (p *Provider) updateRepositoryName(params map[string]any) (*plugin.Response, error) {
	oldName, _ := params["oldName"].(string)
	newName, _ := params["newName"].(string)
	if oldName == "" || newName == "" {
		return shared.JSONError("ValidationException", "oldName and newName are required", http.StatusBadRequest), nil
	}
	if err := p.store.UpdateRepositoryName(oldName, newName); err != nil {
		return shared.JSONError("RepositoryDoesNotExistException", "repository not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) updateRepositoryDescription(params map[string]any) (*plugin.Response, error) {
	name, _ := params["repositoryName"].(string)
	if name == "" {
		return shared.JSONError("ValidationException", "repositoryName is required", http.StatusBadRequest), nil
	}
	description, _ := params["repositoryDescription"].(string)
	if err := p.store.UpdateRepositoryDescription(name, description); err != nil {
		return shared.JSONError("RepositoryDoesNotExistException", "repository not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) deleteRepository(params map[string]any) (*plugin.Response, error) {
	name, _ := params["repositoryName"].(string)
	if name == "" {
		return shared.JSONError("ValidationException", "repositoryName is required", http.StatusBadRequest), nil
	}
	repo, err := p.store.GetRepository(name)
	if err != nil {
		return shared.JSONError("RepositoryDoesNotExistException", "repository not found", http.StatusBadRequest), nil
	}
	p.store.tags.DeleteAllTags(repo.ARN)
	repoID := repo.ID
	if err := p.store.DeleteRepository(name); err != nil {
		return shared.JSONError("RepositoryDoesNotExistException", "repository not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"repositoryId": repoID,
	})
}

func (p *Provider) batchGetRepositories(params map[string]any) (*plugin.Response, error) {
	rawNames, _ := params["repositoryNames"].([]any)
	found := make([]map[string]any, 0)
	notFound := make([]string, 0)
	for _, rn := range rawNames {
		name, _ := rn.(string)
		if name == "" {
			continue
		}
		repo, err := p.store.GetRepository(name)
		if err != nil {
			notFound = append(notFound, name)
		} else {
			found = append(found, repoToMap(repo))
		}
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"repositories":         found,
		"repositoriesNotFound": notFound,
	})
}

// ---- Branch handlers ----

func (p *Provider) createBranch(params map[string]any) (*plugin.Response, error) {
	repoName, _ := params["repositoryName"].(string)
	branchName, _ := params["branchName"].(string)
	commitID, _ := params["commitId"].(string)
	if repoName == "" || branchName == "" {
		return shared.JSONError("ValidationException", "repositoryName and branchName are required", http.StatusBadRequest), nil
	}
	if _, err := p.store.GetRepository(repoName); err != nil {
		return shared.JSONError("RepositoryDoesNotExistException", "repository not found", http.StatusBadRequest), nil
	}
	if _, err := p.store.CreateBranch(repoName, branchName, commitID); err != nil {
		if sqliteIsUnique(err) {
			return shared.JSONError("BranchNameExistsException", "branch already exists", http.StatusBadRequest), nil
		}
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) getBranch(params map[string]any) (*plugin.Response, error) {
	repoName, _ := params["repositoryName"].(string)
	branchName, _ := params["branchName"].(string)
	if repoName == "" || branchName == "" {
		return shared.JSONError("ValidationException", "repositoryName and branchName are required", http.StatusBadRequest), nil
	}
	branch, err := p.store.GetBranch(repoName, branchName)
	if err != nil {
		return shared.JSONError("BranchDoesNotExistException", "branch not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"branch": map[string]any{
			"branchName": branch.Name,
			"commitId":   branch.CommitID,
		},
	})
}

func (p *Provider) listBranches(params map[string]any) (*plugin.Response, error) {
	repoName, _ := params["repositoryName"].(string)
	if repoName == "" {
		return shared.JSONError("ValidationException", "repositoryName is required", http.StatusBadRequest), nil
	}
	branches, err := p.store.ListBranches(repoName)
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, len(branches))
	for _, b := range branches {
		names = append(names, b.Name)
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"branches": names,
	})
}

func (p *Provider) updateDefaultBranch(params map[string]any) (*plugin.Response, error) {
	repoName, _ := params["repositoryName"].(string)
	branchName, _ := params["defaultBranchName"].(string)
	if repoName == "" || branchName == "" {
		return shared.JSONError("ValidationException", "repositoryName and defaultBranchName are required", http.StatusBadRequest), nil
	}
	if err := p.store.UpdateDefaultBranch(repoName, branchName); err != nil {
		return shared.JSONError("RepositoryDoesNotExistException", "repository not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) deleteBranch(params map[string]any) (*plugin.Response, error) {
	repoName, _ := params["repositoryName"].(string)
	branchName, _ := params["branchName"].(string)
	if repoName == "" || branchName == "" {
		return shared.JSONError("ValidationException", "repositoryName and branchName are required", http.StatusBadRequest), nil
	}
	branch, err := p.store.DeleteBranch(repoName, branchName)
	if err != nil {
		return shared.JSONError("BranchDoesNotExistException", "branch not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"deletedBranch": map[string]any{
			"branchName": branch.Name,
			"commitId":   branch.CommitID,
		},
	})
}

// ---- Pull Request handlers ----

func (p *Provider) createPullRequest(params map[string]any) (*plugin.Response, error) {
	title, _ := params["title"].(string)
	if title == "" {
		return shared.JSONError("ValidationException", "title is required", http.StatusBadRequest), nil
	}
	description, _ := params["description"].(string)

	// Parse targets
	var sourceRepo, sourceBranch, destRepo, destBranch string
	if targets, ok := params["targets"].([]any); ok && len(targets) > 0 {
		if t, ok := targets[0].(map[string]any); ok {
			sourceRepo, _ = t["repositoryName"].(string)
			sourceBranch, _ = t["sourceReference"].(string)
			destBranch, _ = t["destinationReference"].(string)
			destRepo = sourceRepo
		}
	}

	id := shared.GenerateID("pr-", 12)
	pr, err := p.store.CreatePullRequest(id, title, description, sourceRepo, sourceBranch, destRepo, destBranch, "")
	if err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"pullRequest": prToMap(pr),
	})
}

func (p *Provider) getPullRequest(params map[string]any) (*plugin.Response, error) {
	id, _ := params["pullRequestId"].(string)
	if id == "" {
		return shared.JSONError("ValidationException", "pullRequestId is required", http.StatusBadRequest), nil
	}
	pr, err := p.store.GetPullRequest(id)
	if err != nil {
		return shared.JSONError("PullRequestDoesNotExistException", "pull request not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"pullRequest": prToMap(pr),
	})
}

func (p *Provider) listPullRequests(params map[string]any) (*plugin.Response, error) {
	repoName, _ := params["repositoryName"].(string)
	status, _ := params["pullRequestStatus"].(string)
	authorARN, _ := params["authorArn"].(string)

	prs, err := p.store.ListPullRequests(repoName, status, authorARN)
	if err != nil {
		return nil, err
	}
	ids := make([]string, 0, len(prs))
	for _, pr := range prs {
		ids = append(ids, pr.ID)
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"pullRequestIds": ids,
	})
}

func (p *Provider) updatePullRequestTitle(params map[string]any) (*plugin.Response, error) {
	id, _ := params["pullRequestId"].(string)
	title, _ := params["title"].(string)
	if id == "" || title == "" {
		return shared.JSONError("ValidationException", "pullRequestId and title are required", http.StatusBadRequest), nil
	}
	pr, err := p.store.UpdatePullRequestTitle(id, title)
	if err != nil {
		return shared.JSONError("PullRequestDoesNotExistException", "pull request not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"pullRequest": prToMap(pr),
	})
}

func (p *Provider) updatePullRequestDescription(params map[string]any) (*plugin.Response, error) {
	id, _ := params["pullRequestId"].(string)
	if id == "" {
		return shared.JSONError("ValidationException", "pullRequestId is required", http.StatusBadRequest), nil
	}
	description, _ := params["description"].(string)
	pr, err := p.store.UpdatePullRequestDescription(id, description)
	if err != nil {
		return shared.JSONError("PullRequestDoesNotExistException", "pull request not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"pullRequest": prToMap(pr),
	})
}

func (p *Provider) updatePullRequestStatus(params map[string]any) (*plugin.Response, error) {
	id, _ := params["pullRequestId"].(string)
	status, _ := params["pullRequestStatus"].(string)
	if id == "" || status == "" {
		return shared.JSONError("ValidationException", "pullRequestId and pullRequestStatus are required", http.StatusBadRequest), nil
	}
	pr, err := p.store.UpdatePullRequestStatus(id, status)
	if err != nil {
		return shared.JSONError("PullRequestDoesNotExistException", "pull request not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"pullRequest": prToMap(pr),
	})
}

// ---- PR Approval Rule handlers ----

func (p *Provider) createPullRequestApprovalRule(params map[string]any) (*plugin.Response, error) {
	prID, _ := params["pullRequestId"].(string)
	name, _ := params["approvalRuleName"].(string)
	content, _ := params["approvalRuleContent"].(string)
	if prID == "" || name == "" {
		return shared.JSONError("ValidationException", "pullRequestId and approvalRuleName are required", http.StatusBadRequest), nil
	}
	if _, err := p.store.GetPullRequest(prID); err != nil {
		return shared.JSONError("PullRequestDoesNotExistException", "pull request not found", http.StatusBadRequest), nil
	}
	if err := p.store.CreatePRApprovalRule(prID, name, content); err != nil {
		return nil, err
	}
	ruleID := shared.GenerateUUID()
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"approvalRule": map[string]any{
			"approvalRuleId":      ruleID,
			"approvalRuleName":    name,
			"approvalRuleContent": content,
		},
	})
}

func (p *Provider) deletePullRequestApprovalRule(params map[string]any) (*plugin.Response, error) {
	prID, _ := params["pullRequestId"].(string)
	name, _ := params["approvalRuleName"].(string)
	if prID == "" || name == "" {
		return shared.JSONError("ValidationException", "pullRequestId and approvalRuleName are required", http.StatusBadRequest), nil
	}
	if err := p.store.DeletePRApprovalRule(prID, name); err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"approvalRuleId": shared.GenerateUUID(),
	})
}

func (p *Provider) updatePullRequestApprovalRuleContent(params map[string]any) (*plugin.Response, error) {
	prID, _ := params["pullRequestId"].(string)
	name, _ := params["approvalRuleName"].(string)
	content, _ := params["newRuleContent"].(string)
	if prID == "" || name == "" {
		return shared.JSONError("ValidationException", "pullRequestId and approvalRuleName are required", http.StatusBadRequest), nil
	}
	if err := p.store.UpdatePRApprovalRuleContent(prID, name, content); err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"approvalRule": map[string]any{
			"approvalRuleName":    name,
			"approvalRuleContent": content,
		},
	})
}

func (p *Provider) updatePullRequestApprovalState(params map[string]any) (*plugin.Response, error) {
	prID, _ := params["pullRequestId"].(string)
	userARN, _ := params["revisionId"].(string)
	state, _ := params["approvalState"].(string)
	if prID == "" {
		return shared.JSONError("ValidationException", "pullRequestId is required", http.StatusBadRequest), nil
	}
	// use a default userARN if not provided
	if userARN == "" {
		userARN = "arn:aws:iam::000000000000:user/default"
	}
	if err := p.store.SetApprovalState(prID, userARN, state); err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) evaluatePullRequestApprovalRules(params map[string]any) (*plugin.Response, error) {
	prID, _ := params["pullRequestId"].(string)
	if prID == "" {
		return shared.JSONError("ValidationException", "pullRequestId is required", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"evaluation": map[string]any{
			"approved":                  true,
			"overridden":                false,
			"approvalRulesNotSatisfied": []any{},
			"approvalRulesSatisfied":    []any{},
		},
	})
}

func (p *Provider) getPullRequestApprovalStates(params map[string]any) (*plugin.Response, error) {
	prID, _ := params["pullRequestId"].(string)
	if prID == "" {
		return shared.JSONError("ValidationException", "pullRequestId is required", http.StatusBadRequest), nil
	}
	approvals, err := p.store.GetApprovals(prID)
	if err != nil {
		return nil, err
	}
	if approvals == nil {
		approvals = []map[string]string{}
	}
	result := make([]map[string]any, 0, len(approvals))
	for _, a := range approvals {
		result = append(result, map[string]any{
			"userArn":       a["userArn"],
			"approvalState": a["approvalState"],
		})
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"approvals": result,
	})
}

func (p *Provider) getPullRequestOverrideState(params map[string]any) (*plugin.Response, error) {
	prID, _ := params["pullRequestId"].(string)
	if prID == "" {
		return shared.JSONError("ValidationException", "pullRequestId is required", http.StatusBadRequest), nil
	}
	overridden, overrider := p.store.GetOverride(prID)
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"overridden": overridden,
		"overrider":  overrider,
	})
}

func (p *Provider) overridePullRequestApprovalRules(params map[string]any) (*plugin.Response, error) {
	prID, _ := params["pullRequestId"].(string)
	overrideStatus, _ := params["overrideStatus"].(string)
	if prID == "" {
		return shared.JSONError("ValidationException", "pullRequestId is required", http.StatusBadRequest), nil
	}
	overridden := overrideStatus == "OVERRIDE"
	if err := p.store.SetOverride(prID, "arn:aws:iam::000000000000:user/default", overridden); err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

// ---- Approval Rule Template handlers ----

func (p *Provider) createApprovalRuleTemplate(params map[string]any) (*plugin.Response, error) {
	name, _ := params["approvalRuleTemplateName"].(string)
	if name == "" {
		return shared.JSONError("ValidationException", "approvalRuleTemplateName is required", http.StatusBadRequest), nil
	}
	description, _ := params["approvalRuleTemplateDescription"].(string)
	content, _ := params["approvalRuleTemplateContent"].(string)
	id := shared.GenerateUUID()
	tpl, err := p.store.CreateApprovalRuleTemplate(name, id, description, content)
	if err != nil {
		if sqliteIsUnique(err) {
			return shared.JSONError("ApprovalRuleTemplateNameAlreadyExistsException", "template already exists", http.StatusBadRequest), nil
		}
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"approvalRuleTemplate": tplToMap(tpl),
	})
}

func (p *Provider) getApprovalRuleTemplate(params map[string]any) (*plugin.Response, error) {
	name, _ := params["approvalRuleTemplateName"].(string)
	if name == "" {
		return shared.JSONError("ValidationException", "approvalRuleTemplateName is required", http.StatusBadRequest), nil
	}
	tpl, err := p.store.GetApprovalRuleTemplate(name)
	if err != nil {
		return shared.JSONError("ApprovalRuleTemplateDoesNotExistException", "template not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"approvalRuleTemplate": tplToMap(tpl),
	})
}

func (p *Provider) listApprovalRuleTemplates(params map[string]any) (*plugin.Response, error) {
	templates, err := p.store.ListApprovalRuleTemplates()
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, len(templates))
	for _, t := range templates {
		names = append(names, t.Name)
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"approvalRuleTemplateNames": names,
	})
}

func (p *Provider) updateApprovalRuleTemplateName(params map[string]any) (*plugin.Response, error) {
	oldName, _ := params["oldApprovalRuleTemplateName"].(string)
	newName, _ := params["newApprovalRuleTemplateName"].(string)
	if oldName == "" || newName == "" {
		return shared.JSONError("ValidationException", "oldApprovalRuleTemplateName and newApprovalRuleTemplateName are required", http.StatusBadRequest), nil
	}
	tpl, err := p.store.UpdateApprovalRuleTemplateName(oldName, newName)
	if err != nil {
		return shared.JSONError("ApprovalRuleTemplateDoesNotExistException", "template not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"approvalRuleTemplate": tplToMap(tpl),
	})
}

func (p *Provider) updateApprovalRuleTemplateDescription(params map[string]any) (*plugin.Response, error) {
	name, _ := params["approvalRuleTemplateName"].(string)
	description, _ := params["approvalRuleTemplateDescription"].(string)
	if name == "" {
		return shared.JSONError("ValidationException", "approvalRuleTemplateName is required", http.StatusBadRequest), nil
	}
	tpl, err := p.store.UpdateApprovalRuleTemplateDescription(name, description)
	if err != nil {
		return shared.JSONError("ApprovalRuleTemplateDoesNotExistException", "template not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"approvalRuleTemplate": tplToMap(tpl),
	})
}

func (p *Provider) updateApprovalRuleTemplateContent(params map[string]any) (*plugin.Response, error) {
	name, _ := params["approvalRuleTemplateName"].(string)
	content, _ := params["newRuleContent"].(string)
	if name == "" {
		return shared.JSONError("ValidationException", "approvalRuleTemplateName is required", http.StatusBadRequest), nil
	}
	tpl, err := p.store.UpdateApprovalRuleTemplateContent(name, content)
	if err != nil {
		return shared.JSONError("ApprovalRuleTemplateDoesNotExistException", "template not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"approvalRuleTemplate": tplToMap(tpl),
	})
}

func (p *Provider) deleteApprovalRuleTemplate(params map[string]any) (*plugin.Response, error) {
	name, _ := params["approvalRuleTemplateName"].(string)
	if name == "" {
		return shared.JSONError("ValidationException", "approvalRuleTemplateName is required", http.StatusBadRequest), nil
	}
	tpl, err := p.store.GetApprovalRuleTemplate(name)
	if err != nil {
		return shared.JSONError("ApprovalRuleTemplateDoesNotExistException", "template not found", http.StatusBadRequest), nil
	}
	tplID := tpl.ID
	if err := p.store.DeleteApprovalRuleTemplate(name); err != nil {
		return shared.JSONError("ApprovalRuleTemplateDoesNotExistException", "template not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"approvalRuleTemplateId": tplID,
	})
}

func (p *Provider) associateApprovalRuleTemplateWithRepository(params map[string]any) (*plugin.Response, error) {
	templateName, _ := params["approvalRuleTemplateName"].(string)
	repoName, _ := params["repositoryName"].(string)
	if templateName == "" || repoName == "" {
		return shared.JSONError("ValidationException", "approvalRuleTemplateName and repositoryName are required", http.StatusBadRequest), nil
	}
	if err := p.store.AssociateTemplate(repoName, templateName); err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) batchAssociateApprovalRuleTemplateWithRepositories(params map[string]any) (*plugin.Response, error) {
	templateName, _ := params["approvalRuleTemplateName"].(string)
	rawNames, _ := params["repositoryNames"].([]any)
	associated := make([]string, 0)
	errors := make([]map[string]any, 0)
	for _, rn := range rawNames {
		name, _ := rn.(string)
		if name == "" {
			continue
		}
		if err := p.store.AssociateTemplate(name, templateName); err != nil {
			errors = append(errors, map[string]any{
				"repositoryName": name,
				"errorCode":      "REPOSITORY_DOES_NOT_EXIST",
				"errorMessage":   err.Error(),
			})
		} else {
			associated = append(associated, name)
		}
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"associatedRepositoryNames": associated,
		"errors":                    errors,
	})
}

func (p *Provider) disassociateApprovalRuleTemplateFromRepository(params map[string]any) (*plugin.Response, error) {
	templateName, _ := params["approvalRuleTemplateName"].(string)
	repoName, _ := params["repositoryName"].(string)
	if templateName == "" || repoName == "" {
		return shared.JSONError("ValidationException", "approvalRuleTemplateName and repositoryName are required", http.StatusBadRequest), nil
	}
	if err := p.store.DisassociateTemplate(repoName, templateName); err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) batchDisassociateApprovalRuleTemplateFromRepositories(params map[string]any) (*plugin.Response, error) {
	templateName, _ := params["approvalRuleTemplateName"].(string)
	rawNames, _ := params["repositoryNames"].([]any)
	disassociated := make([]string, 0)
	errors := make([]map[string]any, 0)
	for _, rn := range rawNames {
		name, _ := rn.(string)
		if name == "" {
			continue
		}
		if err := p.store.DisassociateTemplate(name, templateName); err != nil {
			errors = append(errors, map[string]any{
				"repositoryName": name,
				"errorCode":      "REPOSITORY_DOES_NOT_EXIST",
				"errorMessage":   err.Error(),
			})
		} else {
			disassociated = append(disassociated, name)
		}
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"disassociatedRepositoryNames": disassociated,
		"errors":                       errors,
	})
}

func (p *Provider) listAssociatedApprovalRuleTemplatesForRepository(params map[string]any) (*plugin.Response, error) {
	repoName, _ := params["repositoryName"].(string)
	if repoName == "" {
		return shared.JSONError("ValidationException", "repositoryName is required", http.StatusBadRequest), nil
	}
	names, err := p.store.ListTemplatesForRepo(repoName)
	if err != nil {
		return nil, err
	}
	if names == nil {
		names = []string{}
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"approvalRuleTemplateNames": names,
	})
}

func (p *Provider) listRepositoriesForApprovalRuleTemplate(params map[string]any) (*plugin.Response, error) {
	templateName, _ := params["approvalRuleTemplateName"].(string)
	if templateName == "" {
		return shared.JSONError("ValidationException", "approvalRuleTemplateName is required", http.StatusBadRequest), nil
	}
	names, err := p.store.ListReposForTemplate(templateName)
	if err != nil {
		return nil, err
	}
	if names == nil {
		names = []string{}
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"repositoryNames": names,
	})
}

// ---- Comment handlers ----

func (p *Provider) postCommentForPullRequest(params map[string]any) (*plugin.Response, error) {
	content, _ := params["content"].(string)
	if content == "" {
		return shared.JSONError("ValidationException", "content is required", http.StatusBadRequest), nil
	}
	id := shared.GenerateUUID()
	comment, err := p.store.CreateComment(id, content, "", "")
	if err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"comment": commentToMap(comment),
	})
}

func (p *Provider) postCommentForComparedCommit(params map[string]any) (*plugin.Response, error) {
	content, _ := params["content"].(string)
	if content == "" {
		return shared.JSONError("ValidationException", "content is required", http.StatusBadRequest), nil
	}
	id := shared.GenerateUUID()
	comment, err := p.store.CreateComment(id, content, "", "")
	if err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"comment": commentToMap(comment),
	})
}

func (p *Provider) postCommentReply(params map[string]any) (*plugin.Response, error) {
	inReplyTo, _ := params["inReplyTo"].(string)
	content, _ := params["content"].(string)
	if inReplyTo == "" || content == "" {
		return shared.JSONError("ValidationException", "inReplyTo and content are required", http.StatusBadRequest), nil
	}
	id := shared.GenerateUUID()
	comment, err := p.store.CreateComment(id, content, "", inReplyTo)
	if err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"comment": commentToMap(comment),
	})
}

func (p *Provider) getComment(params map[string]any) (*plugin.Response, error) {
	id, _ := params["commentId"].(string)
	if id == "" {
		return shared.JSONError("ValidationException", "commentId is required", http.StatusBadRequest), nil
	}
	comment, err := p.store.GetComment(id)
	if err != nil {
		return shared.JSONError("CommentDoesNotExistException", "comment not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"comment": commentToMap(comment),
	})
}

func (p *Provider) getCommentsForPullRequest(params map[string]any) (*plugin.Response, error) {
	comments, err := p.store.ListComments()
	if err != nil {
		return nil, err
	}
	list := make([]map[string]any, 0, len(comments))
	for _, c := range comments {
		list = append(list, map[string]any{
			"comment": commentToMap(&c),
		})
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"commentsForPullRequestData": list,
	})
}

func (p *Provider) getCommentsForComparedCommit(params map[string]any) (*plugin.Response, error) {
	comments, err := p.store.ListComments()
	if err != nil {
		return nil, err
	}
	list := make([]map[string]any, 0, len(comments))
	for _, c := range comments {
		list = append(list, map[string]any{
			"comment": commentToMap(&c),
		})
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"commentsForComparedCommitData": list,
	})
}

func (p *Provider) updateComment(params map[string]any) (*plugin.Response, error) {
	id, _ := params["commentId"].(string)
	content, _ := params["content"].(string)
	if id == "" || content == "" {
		return shared.JSONError("ValidationException", "commentId and content are required", http.StatusBadRequest), nil
	}
	comment, err := p.store.UpdateComment(id, content)
	if err != nil {
		return shared.JSONError("CommentDoesNotExistException", "comment not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"comment": commentToMap(comment),
	})
}

func (p *Provider) deleteCommentContent(params map[string]any) (*plugin.Response, error) {
	id, _ := params["commentId"].(string)
	if id == "" {
		return shared.JSONError("ValidationException", "commentId is required", http.StatusBadRequest), nil
	}
	comment, err := p.store.DeleteCommentContent(id)
	if err != nil {
		return shared.JSONError("CommentDoesNotExistException", "comment not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"comment": commentToMap(comment),
	})
}

func (p *Provider) putCommentReaction(params map[string]any) (*plugin.Response, error) {
	commentID, _ := params["commentId"].(string)
	emoji, _ := params["reactionValue"].(string)
	if commentID == "" || emoji == "" {
		return shared.JSONError("ValidationException", "commentId and reactionValue are required", http.StatusBadRequest), nil
	}
	if err := p.store.PutCommentReaction(commentID, "arn:aws:iam::000000000000:user/default", emoji); err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) getCommentReactions(params map[string]any) (*plugin.Response, error) {
	commentID, _ := params["commentId"].(string)
	if commentID == "" {
		return shared.JSONError("ValidationException", "commentId is required", http.StatusBadRequest), nil
	}
	reactions, err := p.store.GetCommentReactions(commentID)
	if err != nil {
		return nil, err
	}
	if reactions == nil {
		reactions = []map[string]string{}
	}
	list := make([]map[string]any, 0, len(reactions))
	for _, r := range reactions {
		list = append(list, map[string]any{
			"reactionValue":                  r["emoji"],
			"reactionsFromDeletedUsersCount": 0,
			"users": []map[string]string{
				{"userArn": r["userArn"]},
			},
		})
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"reactionsForComment": list,
	})
}

// ---- Tag handlers ----

func (p *Provider) tagResource(params map[string]any) (*plugin.Response, error) {
	arn, _ := params["resourceArn"].(string)
	if arn == "" {
		return shared.JSONError("ValidationException", "resourceArn is required", http.StatusBadRequest), nil
	}
	rawTags, _ := params["tags"].(map[string]any)
	tags := flatTags(rawTags)
	if err := p.store.tags.AddTags(arn, tags); err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) untagResource(params map[string]any) (*plugin.Response, error) {
	arn, _ := params["resourceArn"].(string)
	if arn == "" {
		return shared.JSONError("ValidationException", "resourceArn is required", http.StatusBadRequest), nil
	}
	rawKeys, _ := params["tagKeys"].([]any)
	keys := make([]string, 0, len(rawKeys))
	for _, k := range rawKeys {
		if s, ok := k.(string); ok {
			keys = append(keys, s)
		}
	}
	if err := p.store.tags.RemoveTags(arn, keys); err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) listTagsForResource(params map[string]any) (*plugin.Response, error) {
	arn, _ := params["resourceArn"].(string)
	if arn == "" {
		return shared.JSONError("ValidationException", "resourceArn is required", http.StatusBadRequest), nil
	}
	tags, err := p.store.tags.ListTags(arn)
	if err != nil {
		return nil, err
	}
	if tags == nil {
		tags = map[string]string{}
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"tags": tags,
	})
}

// ---- helpers ----

func repoToMap(r *Repository) map[string]any {
	return map[string]any{
		"repositoryId":          r.ID,
		"repositoryName":        r.Name,
		"repositoryDescription": r.Description,
		"defaultBranch":         r.DefaultBranch,
		"cloneUrlHttp":          r.CloneURL,
		"arn":                   r.ARN,
		"creationDate":          r.CreatedAt.Unix(),
		"lastModifiedDate":      r.UpdatedAt.Unix(),
	}
}

func prToMap(pr *PullRequest) map[string]any {
	return map[string]any{
		"pullRequestId":     pr.ID,
		"title":             pr.Title,
		"description":       pr.Description,
		"pullRequestStatus": pr.Status,
		"authorArn":         pr.Author,
		"creationDate":      pr.CreatedAt.Unix(),
		"lastActivityDate":  pr.UpdatedAt.Unix(),
		"pullRequestTargets": []map[string]any{
			{
				"repositoryName":       pr.SourceRepo,
				"sourceReference":      pr.SourceBranch,
				"destinationReference": pr.DestBranch,
			},
		},
	}
}

func tplToMap(t *ApprovalRuleTemplate) map[string]any {
	return map[string]any{
		"approvalRuleTemplateId":          t.ID,
		"approvalRuleTemplateName":        t.Name,
		"approvalRuleTemplateDescription": t.Description,
		"approvalRuleTemplateContent":     t.Content,
		"creationDate":                    t.CreatedAt.Unix(),
		"lastModifiedDate":                t.UpdatedAt.Unix(),
	}
}

func commentToMap(c *Comment) map[string]any {
	return map[string]any{
		"commentId":    c.ID,
		"content":      c.Content,
		"authorArn":    c.Author,
		"inReplyTo":    c.InReplyTo,
		"deleted":      c.Deleted,
		"creationDate": c.CreatedAt.Unix(),
	}
}

func flatTags(raw map[string]any) map[string]string {
	tags := make(map[string]string, len(raw))
	for k, v := range raw {
		if s, ok := v.(string); ok {
			tags[k] = s
		}
	}
	return tags
}

func sqliteIsUnique(err error) bool {
	return err != nil && strings.Contains(err.Error(), "UNIQUE constraint failed")
}
