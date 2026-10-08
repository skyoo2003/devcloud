// SPDX-License-Identifier: Apache-2.0

package cloudformation

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/skyoo2003/devcloud/internal/plugin"
)

func TestCloudFormationAdvancedOperations(t *testing.T) {
	tmpDir := t.TempDir()
	p := &Provider{}
	err := p.Init(plugin.PluginConfig{DataDir: tmpDir})
	require.NoError(t, err)
	defer func() { _ = p.Shutdown(context.Background()) }()

	ctx := context.Background()

	// 1. Create a stack
	formCreate := url.Values{
		"Action":       {"CreateStack"},
		"StackName":    {"test-stack"},
		"TemplateBody": {`{"Resources":{}}`},
	}
	resp, err := p.HandleRequest(ctx, "CreateStack", httptest.NewRequest(http.MethodPost, "/", strings.NewReader(formCreate.Encode())))
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// 2. CancelUpdateStack
	formCancel := url.Values{
		"Action":    {"CancelUpdateStack"},
		"StackName": {"test-stack"},
	}
	resp, err = p.HandleRequest(ctx, "CancelUpdateStack", httptest.NewRequest(http.MethodPost, "/", strings.NewReader(formCancel.Encode())))
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Contains(t, string(resp.Body), "CancelUpdateStackResponse")

	// 3. CancelUpdateStack with nonexistent stack returns 400
	formCancelMissing := url.Values{
		"Action":    {"CancelUpdateStack"},
		"StackName": {"missing-stack"},
	}
	resp, err = p.HandleRequest(ctx, "CancelUpdateStack", httptest.NewRequest(http.MethodPost, "/", strings.NewReader(formCancelMissing.Encode())))
	require.NoError(t, err)
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)

	// 4. DetectStackSetDrift
	formDrift := url.Values{
		"Action":       {"DetectStackSetDrift"},
		"StackSetName": {"test-stackset"},
	}
	resp, err = p.HandleRequest(ctx, "DetectStackSetDrift", httptest.NewRequest(http.MethodPost, "/", strings.NewReader(formDrift.Encode())))
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Contains(t, string(resp.Body), "OperationId")

	// 5. ExecuteStackRefactor
	formRefactor := url.Values{
		"Action":          {"ExecuteStackRefactor"},
		"StackRefactorId": {"ref-123"},
	}
	resp, err = p.HandleRequest(ctx, "ExecuteStackRefactor", httptest.NewRequest(http.MethodPost, "/", strings.NewReader(formRefactor.Encode())))
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Contains(t, string(resp.Body), "ExecuteStackRefactorResponse")

	// 6. StartResourceScan
	formScan := url.Values{
		"Action": {"StartResourceScan"},
	}
	resp, err = p.HandleRequest(ctx, "StartResourceScan", httptest.NewRequest(http.MethodPost, "/", strings.NewReader(formScan.Encode())))
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Contains(t, string(resp.Body), "ResourceScanId")

	// 7. EstimateTemplateCost
	formCost := url.Values{
		"Action":       {"EstimateTemplateCost"},
		"TemplateBody": {`{"Resources":{}}`},
	}
	resp, err = p.HandleRequest(ctx, "EstimateTemplateCost", httptest.NewRequest(http.MethodPost, "/", strings.NewReader(formCost.Encode())))
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Contains(t, string(resp.Body), "calculator.s3.amazonaws.com")

	// 8. RecordHandlerProgress
	formProg := url.Values{
		"Action":          {"RecordHandlerProgress"},
		"BearerToken":     {"tok-1"},
		"OperationStatus": {"SUCCESS"},
	}
	resp, err = p.HandleRequest(ctx, "RecordHandlerProgress", httptest.NewRequest(http.MethodPost, "/", strings.NewReader(formProg.Encode())))
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// 9. ActivateOrganizationsAccess & Deactivate
	formAct := url.Values{
		"Action": {"ActivateOrganizationsAccess"},
	}
	resp, err = p.HandleRequest(ctx, "ActivateOrganizationsAccess", httptest.NewRequest(http.MethodPost, "/", strings.NewReader(formAct.Encode())))
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	formDeact := url.Values{
		"Action": {"DeactivateOrganizationsAccess"},
	}
	resp, err = p.HandleRequest(ctx, "DeactivateOrganizationsAccess", httptest.NewRequest(http.MethodPost, "/", strings.NewReader(formDeact.Encode())))
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}
