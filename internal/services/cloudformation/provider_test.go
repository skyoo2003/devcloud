// SPDX-License-Identifier: Apache-2.0

package cloudformation

import (
	"context"
	"encoding/xml"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/skyoo2003/devcloud/internal/plugin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestProvider(t *testing.T) *Provider {
	t.Helper()
	p := &Provider{}
	require.NoError(t, p.Init(plugin.PluginConfig{DataDir: t.TempDir()}))
	t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
	return p
}

func callQuery(t *testing.T, p *Provider, action string, params map[string]string) *plugin.Response {
	t.Helper()
	form := url.Values{}
	form.Set("Action", action)
	for k, v := range params {
		form.Set(k, v)
	}
	req := httptest.NewRequest("POST", "/", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := p.HandleRequest(context.Background(), "", req)
	require.NoError(t, err)
	return resp
}

func TestCreateAndDescribeStack(t *testing.T) {
	p := newTestProvider(t)

	template := `{"Resources":{"MyBucket":{"Type":"AWS::S3::Bucket"}}}`
	resp := callQuery(t, p, "CreateStack", map[string]string{
		"StackName":    "my-stack",
		"TemplateBody": template,
	})
	assert.Equal(t, 200, resp.StatusCode, string(resp.Body))

	type createResp struct {
		Result struct {
			StackID string `xml:"StackId"`
		} `xml:"CreateStackResult"`
	}
	var cr createResp
	require.NoError(t, xml.Unmarshal(resp.Body, &cr))
	assert.Contains(t, cr.Result.StackID, "arn:aws:cloudformation")

	// DescribeStacks
	descResp := callQuery(t, p, "DescribeStacks", map[string]string{
		"StackName": "my-stack",
	})
	assert.Equal(t, 200, descResp.StatusCode, string(descResp.Body))

	type descResult struct {
		Result struct {
			Stacks []struct {
				StackName   string `xml:"StackName"`
				StackStatus string `xml:"StackStatus"`
			} `xml:"Stacks>member"`
		} `xml:"DescribeStacksResult"`
	}
	var dr descResult
	require.NoError(t, xml.Unmarshal(descResp.Body, &dr))
	require.Len(t, dr.Result.Stacks, 1)
	assert.Equal(t, "my-stack", dr.Result.Stacks[0].StackName)
	assert.Equal(t, "CREATE_COMPLETE", dr.Result.Stacks[0].StackStatus)
}

func TestUpdateStack(t *testing.T) {
	p := newTestProvider(t)

	callQuery(t, p, "CreateStack", map[string]string{
		"StackName":    "update-stack",
		"TemplateBody": `{"Resources":{}}`,
	})

	newTemplate := `{"Resources":{"MyQueue":{"Type":"AWS::SQS::Queue"}}}`
	resp := callQuery(t, p, "UpdateStack", map[string]string{
		"StackName":    "update-stack",
		"TemplateBody": newTemplate,
	})
	assert.Equal(t, 200, resp.StatusCode, string(resp.Body))

	// Verify template was updated
	getResp := callQuery(t, p, "GetTemplate", map[string]string{
		"StackName": "update-stack",
	})
	assert.Equal(t, 200, getResp.StatusCode)

	type getTemplResp struct {
		Result struct {
			TemplateBody string `xml:"TemplateBody"`
		} `xml:"GetTemplateResult"`
	}
	var gtr getTemplResp
	require.NoError(t, xml.Unmarshal(getResp.Body, &gtr))
	assert.Equal(t, newTemplate, gtr.Result.TemplateBody)

	// Status should be UPDATE_COMPLETE
	descResp := callQuery(t, p, "DescribeStacks", map[string]string{"StackName": "update-stack"})
	type descResult struct {
		Result struct {
			Stacks []struct {
				StackStatus string `xml:"StackStatus"`
			} `xml:"Stacks>member"`
		} `xml:"DescribeStacksResult"`
	}
	var dr descResult
	require.NoError(t, xml.Unmarshal(descResp.Body, &dr))
	require.Len(t, dr.Result.Stacks, 1)
	assert.Equal(t, "UPDATE_COMPLETE", dr.Result.Stacks[0].StackStatus)
}

func TestDeleteStack(t *testing.T) {
	p := newTestProvider(t)

	callQuery(t, p, "CreateStack", map[string]string{
		"StackName":    "del-stack",
		"TemplateBody": `{"Resources":{}}`,
	})

	resp := callQuery(t, p, "DeleteStack", map[string]string{
		"StackName": "del-stack",
	})
	assert.Equal(t, 200, resp.StatusCode, string(resp.Body))

	// After deletion, DescribeStacks should return error
	descResp := callQuery(t, p, "DescribeStacks", map[string]string{"StackName": "del-stack"})
	assert.NotEqual(t, 200, descResp.StatusCode)

	// Delete non-existent stack should succeed (idempotent)
	resp2 := callQuery(t, p, "DeleteStack", map[string]string{
		"StackName": "non-existent",
	})
	assert.Equal(t, 200, resp2.StatusCode)
}

func TestListStacks(t *testing.T) {
	p := newTestProvider(t)

	callQuery(t, p, "CreateStack", map[string]string{"StackName": "stack-a", "TemplateBody": `{}`})
	callQuery(t, p, "CreateStack", map[string]string{"StackName": "stack-b", "TemplateBody": `{}`})

	resp := callQuery(t, p, "ListStacks", map[string]string{})
	assert.Equal(t, 200, resp.StatusCode, string(resp.Body))

	type listResult struct {
		Result struct {
			Summaries []struct {
				StackName   string `xml:"StackName"`
				StackStatus string `xml:"StackStatus"`
			} `xml:"StackSummaries>member"`
		} `xml:"ListStacksResult"`
	}
	var lr listResult
	require.NoError(t, xml.Unmarshal(resp.Body, &lr))
	assert.Len(t, lr.Result.Summaries, 2)
}

func TestGetTemplate(t *testing.T) {
	p := newTestProvider(t)

	template := `{"AWSTemplateFormatVersion":"2010-09-09","Resources":{"MyBucket":{"Type":"AWS::S3::Bucket"}}}`
	callQuery(t, p, "CreateStack", map[string]string{
		"StackName":    "tmpl-stack",
		"TemplateBody": template,
	})

	resp := callQuery(t, p, "GetTemplate", map[string]string{
		"StackName": "tmpl-stack",
	})
	assert.Equal(t, 200, resp.StatusCode, string(resp.Body))

	type getTemplResp struct {
		Result struct {
			TemplateBody string `xml:"TemplateBody"`
		} `xml:"GetTemplateResult"`
	}
	var gtr getTemplResp
	require.NoError(t, xml.Unmarshal(resp.Body, &gtr))
	assert.Equal(t, template, gtr.Result.TemplateBody)
}

func TestChangeSetCRUD(t *testing.T) {
	p := newTestProvider(t)

	// Create a stack first
	callQuery(t, p, "CreateStack", map[string]string{
		"StackName":    "cs-stack",
		"TemplateBody": `{"Resources":{}}`,
	})

	// CreateChangeSet
	resp := callQuery(t, p, "CreateChangeSet", map[string]string{
		"StackName":     "cs-stack",
		"ChangeSetName": "my-changeset",
		"TemplateBody":  `{"Resources":{"NewBucket":{"Type":"AWS::S3::Bucket"}}}`,
	})
	assert.Equal(t, 200, resp.StatusCode, string(resp.Body))

	type createCSResp struct {
		Result struct {
			ID string `xml:"Id"`
		} `xml:"CreateChangeSetResult"`
	}
	var ccsr createCSResp
	require.NoError(t, xml.Unmarshal(resp.Body, &ccsr))
	assert.Contains(t, ccsr.Result.ID, "arn:aws:cloudformation")

	// DescribeChangeSet
	descResp := callQuery(t, p, "DescribeChangeSet", map[string]string{
		"StackName":     "cs-stack",
		"ChangeSetName": "my-changeset",
	})
	assert.Equal(t, 200, descResp.StatusCode, string(descResp.Body))

	type descCSResp struct {
		Result struct {
			ChangeSetName   string `xml:"ChangeSetName"`
			StackName       string `xml:"StackName"`
			Status          string `xml:"Status"`
			ExecutionStatus string `xml:"ExecutionStatus"`
		} `xml:"DescribeChangeSetResult"`
	}
	var dcsr descCSResp
	require.NoError(t, xml.Unmarshal(descResp.Body, &dcsr))
	assert.Equal(t, "my-changeset", dcsr.Result.ChangeSetName)
	assert.Equal(t, "cs-stack", dcsr.Result.StackName)
	assert.Equal(t, "CREATE_COMPLETE", dcsr.Result.Status)
	assert.Equal(t, "AVAILABLE", dcsr.Result.ExecutionStatus)

	// ListChangeSets
	listResp := callQuery(t, p, "ListChangeSets", map[string]string{
		"StackName": "cs-stack",
	})
	assert.Equal(t, 200, listResp.StatusCode)

	type listCSResp struct {
		Result struct {
			Summaries []struct {
				ChangeSetName string `xml:"ChangeSetName"`
			} `xml:"Summaries>member"`
		} `xml:"ListChangeSetsResult"`
	}
	var lcsr listCSResp
	require.NoError(t, xml.Unmarshal(listResp.Body, &lcsr))
	require.Len(t, lcsr.Result.Summaries, 1)
	assert.Equal(t, "my-changeset", lcsr.Result.Summaries[0].ChangeSetName)

	// ExecuteChangeSet
	execResp := callQuery(t, p, "ExecuteChangeSet", map[string]string{
		"StackName":     "cs-stack",
		"ChangeSetName": "my-changeset",
	})
	assert.Equal(t, 200, execResp.StatusCode, string(execResp.Body))

	// DeleteChangeSet
	delResp := callQuery(t, p, "DeleteChangeSet", map[string]string{
		"StackName":     "cs-stack",
		"ChangeSetName": "my-changeset",
	})
	assert.Equal(t, 200, delResp.StatusCode)

	// After delete, ListChangeSets should be empty
	listResp2 := callQuery(t, p, "ListChangeSets", map[string]string{"StackName": "cs-stack"})
	var lcsr2 listCSResp
	require.NoError(t, xml.Unmarshal(listResp2.Body, &lcsr2))
	assert.Len(t, lcsr2.Result.Summaries, 0)
}

func TestStackSetCRUD(t *testing.T) {
	p := newTestProvider(t)

	// CreateStackSet
	resp := callQuery(t, p, "CreateStackSet", map[string]string{
		"StackSetName": "my-stackset",
		"TemplateBody": `{"Resources":{}}`,
		"Description":  "test stackset",
	})
	assert.Equal(t, 200, resp.StatusCode, string(resp.Body))

	type createSSResp struct {
		Result struct {
			StackSetID string `xml:"StackSetId"`
		} `xml:"CreateStackSetResult"`
	}
	var cssr createSSResp
	require.NoError(t, xml.Unmarshal(resp.Body, &cssr))
	assert.NotEmpty(t, cssr.Result.StackSetID)

	// DescribeStackSet
	descResp := callQuery(t, p, "DescribeStackSet", map[string]string{
		"StackSetName": "my-stackset",
	})
	assert.Equal(t, 200, descResp.StatusCode, string(descResp.Body))

	type descSSResp struct {
		Result struct {
			StackSet struct {
				StackSetName string `xml:"StackSetName"`
				Status       string `xml:"Status"`
				Description  string `xml:"Description"`
			} `xml:"StackSet"`
		} `xml:"DescribeStackSetResult"`
	}
	var dssr descSSResp
	require.NoError(t, xml.Unmarshal(descResp.Body, &dssr))
	assert.Equal(t, "my-stackset", dssr.Result.StackSet.StackSetName)
	assert.Equal(t, "ACTIVE", dssr.Result.StackSet.Status)
	assert.Equal(t, "test stackset", dssr.Result.StackSet.Description)

	// ListStackSets
	listResp := callQuery(t, p, "ListStackSets", map[string]string{})
	assert.Equal(t, 200, listResp.StatusCode)

	type listSSResp struct {
		Result struct {
			Summaries []struct {
				StackSetName string `xml:"StackSetName"`
			} `xml:"Summaries>member"`
		} `xml:"ListStackSetsResult"`
	}
	var lssr listSSResp
	require.NoError(t, xml.Unmarshal(listResp.Body, &lssr))
	require.Len(t, lssr.Result.Summaries, 1)
	assert.Equal(t, "my-stackset", lssr.Result.Summaries[0].StackSetName)

	// UpdateStackSet
	updateResp := callQuery(t, p, "UpdateStackSet", map[string]string{
		"StackSetName": "my-stackset",
		"Description":  "updated desc",
	})
	assert.Equal(t, 200, updateResp.StatusCode)

	// DeleteStackSet
	delResp := callQuery(t, p, "DeleteStackSet", map[string]string{
		"StackSetName": "my-stackset",
	})
	assert.Equal(t, 200, delResp.StatusCode)

	// After delete, DescribeStackSet should fail
	descAfterDel := callQuery(t, p, "DescribeStackSet", map[string]string{"StackSetName": "my-stackset"})
	assert.NotEqual(t, 200, descAfterDel.StatusCode)
}

func TestTags(t *testing.T) {
	p := newTestProvider(t)

	// Create stacks with different statuses
	callQuery(t, p, "CreateStack", map[string]string{
		"StackName":           "tagged-stack",
		"TemplateBody":        `{}`,
		"Tags.member.1.Key":   "Env",
		"Tags.member.1.Value": "prod",
	})

	// ListStacks without filter returns the stack
	listResp := callQuery(t, p, "ListStacks", map[string]string{})
	assert.Equal(t, 200, listResp.StatusCode)

	type listResult struct {
		Result struct {
			Summaries []struct {
				StackName   string `xml:"StackName"`
				StackStatus string `xml:"StackStatus"`
			} `xml:"StackSummaries>member"`
		} `xml:"ListStacksResult"`
	}
	var lr listResult
	require.NoError(t, xml.Unmarshal(listResp.Body, &lr))
	require.Len(t, lr.Result.Summaries, 1)
	assert.Equal(t, "tagged-stack", lr.Result.Summaries[0].StackName)

	// ListStacks with status filter
	filterResp := callQuery(t, p, "ListStacks", map[string]string{
		"StackStatusFilter.member.1": "CREATE_COMPLETE",
	})
	assert.Equal(t, 200, filterResp.StatusCode)
	var lr2 listResult
	require.NoError(t, xml.Unmarshal(filterResp.Body, &lr2))
	assert.Len(t, lr2.Result.Summaries, 1)

	// Filter for non-matching status returns empty
	filterResp2 := callQuery(t, p, "ListStacks", map[string]string{
		"StackStatusFilter.member.1": "DELETE_COMPLETE",
	})
	assert.Equal(t, 200, filterResp2.StatusCode)
	var lr3 listResult
	require.NoError(t, xml.Unmarshal(filterResp2.Body, &lr3))
	assert.Len(t, lr3.Result.Summaries, 0)
}
