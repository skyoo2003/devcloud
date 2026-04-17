// SPDX-License-Identifier: Apache-2.0

// internal/services/cloudfront/provider_test.go
package cloudfront

import (
	"bytes"
	"context"
	"encoding/xml"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	generated "github.com/skyoo2003/devcloud/internal/generated/cloudfront"
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

func doRequest(t *testing.T, p *Provider, method, path, body string) *plugin.Response {
	t.Helper()
	var bodyReader *bytes.Buffer
	if body != "" {
		bodyReader = bytes.NewBufferString(body)
	} else {
		bodyReader = bytes.NewBuffer(nil)
	}
	req := httptest.NewRequest(method, path, bodyReader)
	op, _ := generated.MatchOperation(method, req.URL.Path)
	resp, err := p.HandleRequest(context.Background(), op, req)
	require.NoError(t, err)
	return resp
}

func TestDistributionCRUD(t *testing.T) {
	p := newTestProvider(t)

	createBody := `<?xml version="1.0"?>
<DistributionConfig>
  <Comment>test distribution</Comment>
  <Enabled>true</Enabled>
  <CallerReference>ref-1</CallerReference>
</DistributionConfig>`

	// CreateDistribution
	req := httptest.NewRequest(http.MethodPost, "/2020-05-31/distribution", strings.NewReader(createBody))
	resp, err := p.HandleRequest(context.Background(), "CreateDistribution", req)
	require.NoError(t, err)
	assert.Equal(t, http.StatusCreated, resp.StatusCode)

	// parse distribution ID from response
	type createResp struct {
		Id string `xml:"Id"`
	}
	var cr createResp
	require.NoError(t, xml.Unmarshal(resp.Body, &cr))
	distID := cr.Id
	require.NotEmpty(t, distID)
	assert.Contains(t, string(resp.Body), "cloudfront.net")

	// GetDistribution
	resp = doRequest(t, p, http.MethodGet, "/2020-05-31/distribution/"+distID, "")
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Contains(t, string(resp.Body), distID)

	// GetDistributionConfig
	resp = doRequest(t, p, http.MethodGet, "/2020-05-31/distribution/"+distID+"/config", "")
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Contains(t, string(resp.Body), "DistributionConfig")

	// ListDistributions
	resp = doRequest(t, p, http.MethodGet, "/2020-05-31/distribution", "")
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Contains(t, string(resp.Body), distID)

	// UpdateDistribution
	updateBody := `<?xml version="1.0"?>
<DistributionConfig>
  <Comment>updated distribution</Comment>
  <Enabled>false</Enabled>
</DistributionConfig>`
	req = httptest.NewRequest(http.MethodPut, "/2020-05-31/distribution/"+distID+"/config", strings.NewReader(updateBody))
	resp, err = p.HandleRequest(context.Background(), "UpdateDistribution", req)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// DeleteDistribution
	resp = doRequest(t, p, http.MethodDelete, "/2020-05-31/distribution/"+distID, "")
	assert.Equal(t, http.StatusNoContent, resp.StatusCode)

	// GetDistribution after delete → 404
	resp = doRequest(t, p, http.MethodGet, "/2020-05-31/distribution/"+distID, "")
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
}

func TestCachePolicyCRUD(t *testing.T) {
	p := newTestProvider(t)

	createBody := `<?xml version="1.0"?>
<CachePolicyConfig>
  <Name>my-cache-policy</Name>
  <Comment>test cache policy</Comment>
</CachePolicyConfig>`

	// CreateCachePolicy
	req := httptest.NewRequest(http.MethodPost, "/2020-05-31/cache-policy", strings.NewReader(createBody))
	resp, err := p.HandleRequest(context.Background(), "CreateCachePolicy", req)
	require.NoError(t, err)
	assert.Equal(t, http.StatusCreated, resp.StatusCode)

	type createResp struct {
		Id string `xml:"Id"`
	}
	var cr createResp
	require.NoError(t, xml.Unmarshal(resp.Body, &cr))
	cpID := cr.Id
	require.NotEmpty(t, cpID)

	// GetCachePolicy
	resp = doRequest(t, p, http.MethodGet, "/2020-05-31/cache-policy/"+cpID, "")
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Contains(t, string(resp.Body), "my-cache-policy")

	// GetCachePolicyConfig
	resp = doRequest(t, p, http.MethodGet, "/2020-05-31/cache-policy/"+cpID+"/config", "")
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Contains(t, string(resp.Body), "CachePolicyConfig")

	// ListCachePolicies
	resp = doRequest(t, p, http.MethodGet, "/2020-05-31/cache-policy", "")
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Contains(t, string(resp.Body), cpID)

	// UpdateCachePolicy
	updateBody := `<?xml version="1.0"?>
<CachePolicyConfig>
  <Name>my-cache-policy</Name>
  <Comment>updated</Comment>
</CachePolicyConfig>`
	req = httptest.NewRequest(http.MethodPut, "/2020-05-31/cache-policy/"+cpID, strings.NewReader(updateBody))
	resp, err = p.HandleRequest(context.Background(), "UpdateCachePolicy", req)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// DeleteCachePolicy
	resp = doRequest(t, p, http.MethodDelete, "/2020-05-31/cache-policy/"+cpID, "")
	assert.Equal(t, http.StatusNoContent, resp.StatusCode)

	// GetCachePolicy after delete → 404
	resp = doRequest(t, p, http.MethodGet, "/2020-05-31/cache-policy/"+cpID, "")
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
}

func TestOriginAccessControlCRUD(t *testing.T) {
	p := newTestProvider(t)

	createBody := `<?xml version="1.0"?>
<OriginAccessControlConfig>
  <Name>my-oac</Name>
  <Description>test OAC</Description>
  <SigningProtocol>sigv4</SigningProtocol>
  <SigningBehavior>always</SigningBehavior>
  <OriginAccessControlOriginType>s3</OriginAccessControlOriginType>
</OriginAccessControlConfig>`

	// CreateOriginAccessControl
	req := httptest.NewRequest(http.MethodPost, "/2020-05-31/origin-access-control", strings.NewReader(createBody))
	resp, err := p.HandleRequest(context.Background(), "CreateOriginAccessControl", req)
	require.NoError(t, err)
	assert.Equal(t, http.StatusCreated, resp.StatusCode)

	type createResp struct {
		Id string `xml:"Id"`
	}
	var cr createResp
	require.NoError(t, xml.Unmarshal(resp.Body, &cr))
	oacID := cr.Id
	require.NotEmpty(t, oacID)

	// GetOriginAccessControl
	resp = doRequest(t, p, http.MethodGet, "/2020-05-31/origin-access-control/"+oacID, "")
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Contains(t, string(resp.Body), "my-oac")

	// GetOriginAccessControlConfig
	resp = doRequest(t, p, http.MethodGet, "/2020-05-31/origin-access-control/"+oacID+"/config", "")
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// ListOriginAccessControls
	resp = doRequest(t, p, http.MethodGet, "/2020-05-31/origin-access-control", "")
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Contains(t, string(resp.Body), oacID)

	// UpdateOriginAccessControl
	updateBody := `<?xml version="1.0"?>
<OriginAccessControlConfig>
  <Name>my-oac-updated</Name>
  <Description>updated</Description>
</OriginAccessControlConfig>`
	req = httptest.NewRequest(http.MethodPut, "/2020-05-31/origin-access-control/"+oacID+"/config", strings.NewReader(updateBody))
	resp, err = p.HandleRequest(context.Background(), "UpdateOriginAccessControl", req)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Contains(t, string(resp.Body), "my-oac-updated")

	// DeleteOriginAccessControl
	resp = doRequest(t, p, http.MethodDelete, "/2020-05-31/origin-access-control/"+oacID, "")
	assert.Equal(t, http.StatusNoContent, resp.StatusCode)

	// GetOriginAccessControl after delete → 404
	resp = doRequest(t, p, http.MethodGet, "/2020-05-31/origin-access-control/"+oacID, "")
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
}

func TestFunctionCRUD(t *testing.T) {
	p := newTestProvider(t)

	createBody := `<?xml version="1.0"?>
<CreateFunctionRequest>
  <Name>my-function</Name>
  <FunctionConfig>
    <Comment>test function</Comment>
    <Runtime>cloudfront-js-2.0</Runtime>
  </FunctionConfig>
  <FunctionCode>ZnVuY3Rpb24gaGFuZGxlcigpIHt9</FunctionCode>
</CreateFunctionRequest>`

	// CreateFunction
	req := httptest.NewRequest(http.MethodPost, "/2020-05-31/function", strings.NewReader(createBody))
	resp, err := p.HandleRequest(context.Background(), "CreateFunction", req)
	require.NoError(t, err)
	assert.Equal(t, http.StatusCreated, resp.StatusCode)
	assert.Contains(t, string(resp.Body), "my-function")

	// DescribeFunction
	resp = doRequest(t, p, http.MethodGet, "/2020-05-31/function/my-function/describe", "")
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Contains(t, string(resp.Body), "my-function")

	// ListFunctions
	resp = doRequest(t, p, http.MethodGet, "/2020-05-31/function", "")
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Contains(t, string(resp.Body), "my-function")

	// UpdateFunction
	updateBody := `<?xml version="1.0"?>
<UpdateFunctionRequest>
  <FunctionConfig>
    <Comment>updated function</Comment>
    <Runtime>cloudfront-js-2.0</Runtime>
  </FunctionConfig>
  <FunctionCode>dXBkYXRlZA==</FunctionCode>
</UpdateFunctionRequest>`
	req = httptest.NewRequest(http.MethodPut, "/2020-05-31/function/my-function", strings.NewReader(updateBody))
	resp, err = p.HandleRequest(context.Background(), "UpdateFunction", req)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// PublishFunction
	req = httptest.NewRequest(http.MethodPost, "/2020-05-31/function/my-function/publish", bytes.NewBuffer(nil))
	resp, err = p.HandleRequest(context.Background(), "PublishFunction", req)
	require.NoError(t, err)
	assert.Equal(t, http.StatusCreated, resp.StatusCode)
	assert.Contains(t, string(resp.Body), "LIVE")

	// TestFunction
	testBody := `<?xml version="1.0"?>
<TestFunctionRequest>
  <EventObject>e30=</EventObject>
  <Stage>LIVE</Stage>
</TestFunctionRequest>`
	req = httptest.NewRequest(http.MethodPost, "/2020-05-31/function/my-function/test", strings.NewReader(testBody))
	resp, err = p.HandleRequest(context.Background(), "TestFunction", req)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// DeleteFunction
	resp = doRequest(t, p, http.MethodDelete, "/2020-05-31/function/my-function", "")
	assert.Equal(t, http.StatusNoContent, resp.StatusCode)

	// DescribeFunction after delete → 404
	resp = doRequest(t, p, http.MethodGet, "/2020-05-31/function/my-function/describe", "")
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
}

func TestInvalidationCRUD(t *testing.T) {
	p := newTestProvider(t)

	// Create a distribution first
	createDistBody := `<?xml version="1.0"?>
<DistributionConfig>
  <Comment>test</Comment>
  <Enabled>true</Enabled>
  <CallerReference>ref-inv</CallerReference>
</DistributionConfig>`
	req := httptest.NewRequest(http.MethodPost, "/2020-05-31/distribution", strings.NewReader(createDistBody))
	resp, err := p.HandleRequest(context.Background(), "CreateDistribution", req)
	require.NoError(t, err)
	require.Equal(t, http.StatusCreated, resp.StatusCode)
	type distResp struct {
		Id string `xml:"Id"`
	}
	var dr distResp
	require.NoError(t, xml.Unmarshal(resp.Body, &dr))
	distID := dr.Id
	require.NotEmpty(t, distID)

	// CreateInvalidation
	createInvBody := `<?xml version="1.0"?>
<InvalidationBatch>
  <Paths>
    <Quantity>2</Quantity>
    <Items>
      <Path>/images/*</Path>
      <Path>/css/*</Path>
    </Items>
  </Paths>
  <CallerReference>inv-ref-1</CallerReference>
</InvalidationBatch>`
	req = httptest.NewRequest(http.MethodPost, "/2020-05-31/distribution/"+distID+"/invalidation", strings.NewReader(createInvBody))
	resp, err = p.HandleRequest(context.Background(), "CreateInvalidation", req)
	require.NoError(t, err)
	assert.Equal(t, http.StatusCreated, resp.StatusCode)

	type invResp struct {
		Id string `xml:"Id"`
	}
	var ir invResp
	require.NoError(t, xml.Unmarshal(resp.Body, &ir))
	invID := ir.Id
	require.NotEmpty(t, invID)

	// GetInvalidation
	resp = doRequest(t, p, http.MethodGet, "/2020-05-31/distribution/"+distID+"/invalidation/"+invID, "")
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Contains(t, string(resp.Body), invID)

	// ListInvalidations
	resp = doRequest(t, p, http.MethodGet, "/2020-05-31/distribution/"+distID+"/invalidation", "")
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Contains(t, string(resp.Body), invID)
}

func TestTags(t *testing.T) {
	p := newTestProvider(t)

	// Create a distribution
	createDistBody := `<?xml version="1.0"?>
<DistributionConfig>
  <Comment>tag test</Comment>
  <Enabled>true</Enabled>
  <CallerReference>ref-tags</CallerReference>
</DistributionConfig>`
	req := httptest.NewRequest(http.MethodPost, "/2020-05-31/distribution", strings.NewReader(createDistBody))
	resp, err := p.HandleRequest(context.Background(), "CreateDistribution", req)
	require.NoError(t, err)
	require.Equal(t, http.StatusCreated, resp.StatusCode)
	type distResp struct {
		ARN string `xml:"ARN"`
	}
	var dr distResp
	require.NoError(t, xml.Unmarshal(resp.Body, &dr))
	arn := dr.ARN
	require.NotEmpty(t, arn)

	// TagResource
	tagBody := `<?xml version="1.0"?>
<Tags>
  <Items>
    <Tag><Key>env</Key><Value>prod</Value></Tag>
    <Tag><Key>team</Key><Value>platform</Value></Tag>
  </Items>
</Tags>`
	req = httptest.NewRequest(http.MethodPost, "/2020-05-31/tagging?Operation=Tag&Resource="+arn, strings.NewReader(tagBody))
	resp, err = p.HandleRequest(context.Background(), "TagResource", req)
	require.NoError(t, err)
	assert.Equal(t, http.StatusNoContent, resp.StatusCode)

	// ListTagsForResource
	req = httptest.NewRequest(http.MethodGet, "/2020-05-31/tagging?Resource="+arn, nil)
	resp, err = p.HandleRequest(context.Background(), "ListTagsForResource", req)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	body := string(resp.Body)
	assert.Contains(t, body, "env")
	assert.Contains(t, body, "prod")

	// UntagResource
	untagBody := `<?xml version="1.0"?>
<TagKeys>
  <Items>
    <Key>team</Key>
  </Items>
</TagKeys>`
	req = httptest.NewRequest(http.MethodPost, "/2020-05-31/tagging?Operation=Untag&Resource="+arn, strings.NewReader(untagBody))
	resp, err = p.HandleRequest(context.Background(), "UntagResource", req)
	require.NoError(t, err)
	assert.Equal(t, http.StatusNoContent, resp.StatusCode)

	// ListTagsForResource again - "team" should be gone
	req = httptest.NewRequest(http.MethodGet, "/2020-05-31/tagging?Resource="+arn, nil)
	resp, err = p.HandleRequest(context.Background(), "ListTagsForResource", req)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	body = string(resp.Body)
	assert.Contains(t, body, "env")
	assert.NotContains(t, body, "team")
}

func TestDefaultOperations(t *testing.T) {
	p := newTestProvider(t)

	// Unimplemented operations should return 200 with empty XML
	resp := doRequest(t, p, http.MethodGet, "/2020-05-31/streaming-distribution", "")
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Contains(t, string(resp.Body), "xml")
}
