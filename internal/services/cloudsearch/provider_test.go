// SPDX-License-Identifier: Apache-2.0

package cloudsearch

import (
	"context"
	"encoding/xml"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/skyoo2003/devcloud/internal/plugin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"net/url"
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

func TestCreateAndDescribeDomain(t *testing.T) {
	p := newTestProvider(t)

	// Create a domain
	resp := callQuery(t, p, "CreateDomain", map[string]string{"DomainName": "test-domain"})
	assert.Equal(t, 200, resp.StatusCode, string(resp.Body))

	// Parse response to confirm domain fields
	type createResp struct {
		DomainStatus struct {
			DomainName string `xml:"DomainName"`
			ARN        string `xml:"ARN"`
			Created    bool   `xml:"Created"`
		} `xml:"CreateDomainResult>DomainStatus"`
	}
	var cr createResp
	require.NoError(t, xml.Unmarshal(resp.Body, &cr))
	assert.Equal(t, "test-domain", cr.DomainStatus.DomainName)
	assert.Contains(t, cr.DomainStatus.ARN, "arn:aws:cloudsearch")
	assert.True(t, cr.DomainStatus.Created)

	// DescribeDomains should return the domain
	descResp := callQuery(t, p, "DescribeDomains", map[string]string{
		"DomainNames.member.1": "test-domain",
	})
	assert.Equal(t, 200, descResp.StatusCode, string(descResp.Body))

	type describeResp struct {
		DomainItems []struct {
			DomainName string `xml:"DomainName"`
		} `xml:"DescribeDomainsResult>DomainStatusList>member"`
	}
	var dr describeResp
	require.NoError(t, xml.Unmarshal(descResp.Body, &dr))
	require.Len(t, dr.DomainItems, 1)
	assert.Equal(t, "test-domain", dr.DomainItems[0].DomainName)
}

func TestListDomainNames(t *testing.T) {
	p := newTestProvider(t)

	callQuery(t, p, "CreateDomain", map[string]string{"DomainName": "alpha"})
	callQuery(t, p, "CreateDomain", map[string]string{"DomainName": "beta"})

	resp := callQuery(t, p, "ListDomainNames", nil)
	assert.Equal(t, 200, resp.StatusCode, string(resp.Body))

	type listResp struct {
		DomainNames []struct {
			Key   string `xml:"key"`
			Value string `xml:"value"`
		} `xml:"ListDomainNamesResult>DomainNames>entry"`
	}
	var lr listResp
	require.NoError(t, xml.Unmarshal(resp.Body, &lr))
	assert.Len(t, lr.DomainNames, 2)

	names := make([]string, 0, len(lr.DomainNames))
	for _, e := range lr.DomainNames {
		names = append(names, e.Key)
	}
	assert.Contains(t, names, "alpha")
	assert.Contains(t, names, "beta")
}

func TestDeleteDomain(t *testing.T) {
	p := newTestProvider(t)

	callQuery(t, p, "CreateDomain", map[string]string{"DomainName": "del-me"})

	// Delete domain
	resp := callQuery(t, p, "DeleteDomain", map[string]string{"DomainName": "del-me"})
	assert.Equal(t, 200, resp.StatusCode, string(resp.Body))

	// DescribeDomains should now return empty list for that domain
	descResp := callQuery(t, p, "DescribeDomains", map[string]string{
		"DomainNames.member.1": "del-me",
	})
	assert.Equal(t, 200, descResp.StatusCode)

	type describeResp struct {
		DomainItems []struct {
			DomainName string `xml:"DomainName"`
		} `xml:"DescribeDomainsResult>DomainStatusList>member"`
	}
	var dr describeResp
	require.NoError(t, xml.Unmarshal(descResp.Body, &dr))
	assert.Empty(t, dr.DomainItems)

	// Deleting non-existent domain returns error
	errResp := callQuery(t, p, "DeleteDomain", map[string]string{"DomainName": "del-me"})
	assert.Equal(t, 400, errResp.StatusCode)
}

func TestDefineAndDescribeIndexFields(t *testing.T) {
	p := newTestProvider(t)

	callQuery(t, p, "CreateDomain", map[string]string{"DomainName": "my-domain"})

	// Define a text field
	resp := callQuery(t, p, "DefineIndexField", map[string]string{
		"DomainName":                "my-domain",
		"IndexField.IndexFieldName": "title",
		"IndexField.IndexFieldType": "text",
	})
	assert.Equal(t, 200, resp.StatusCode, string(resp.Body))

	// Define an int field
	resp2 := callQuery(t, p, "DefineIndexField", map[string]string{
		"DomainName":                "my-domain",
		"IndexField.IndexFieldName": "year",
		"IndexField.IndexFieldType": "int",
	})
	assert.Equal(t, 200, resp2.StatusCode)

	// Describe all fields
	descResp := callQuery(t, p, "DescribeIndexFields", map[string]string{
		"DomainName": "my-domain",
	})
	assert.Equal(t, 200, descResp.StatusCode, string(descResp.Body))

	type describeResp struct {
		IndexFields []struct {
			Options struct {
				IndexFieldName string `xml:"IndexFieldName"`
				IndexFieldType string `xml:"IndexFieldType"`
			} `xml:"Options"`
		} `xml:"DescribeIndexFieldsResult>IndexFields>member"`
	}
	var dr describeResp
	require.NoError(t, xml.Unmarshal(descResp.Body, &dr))
	assert.Len(t, dr.IndexFields, 2)

	fieldNames := make([]string, 0, 2)
	for _, f := range dr.IndexFields {
		fieldNames = append(fieldNames, f.Options.IndexFieldName)
	}
	assert.Contains(t, fieldNames, "title")
	assert.Contains(t, fieldNames, "year")
}

func TestDeleteIndexField(t *testing.T) {
	p := newTestProvider(t)

	callQuery(t, p, "CreateDomain", map[string]string{"DomainName": "my-domain"})
	callQuery(t, p, "DefineIndexField", map[string]string{
		"DomainName":                "my-domain",
		"IndexField.IndexFieldName": "title",
		"IndexField.IndexFieldType": "text",
	})

	// Delete the field
	delResp := callQuery(t, p, "DeleteIndexField", map[string]string{
		"DomainName":     "my-domain",
		"IndexFieldName": "title",
	})
	assert.Equal(t, 200, delResp.StatusCode, string(delResp.Body))

	// Should be gone
	descResp := callQuery(t, p, "DescribeIndexFields", map[string]string{
		"DomainName": "my-domain",
	})
	type describeResp struct {
		IndexFields []struct{} `xml:"DescribeIndexFieldsResult>IndexFields>member"`
	}
	var dr describeResp
	require.NoError(t, xml.Unmarshal(descResp.Body, &dr))
	assert.Empty(t, dr.IndexFields)

	// Deleting non-existent field returns error
	errResp := callQuery(t, p, "DeleteIndexField", map[string]string{
		"DomainName":     "my-domain",
		"IndexFieldName": "title",
	})
	assert.Equal(t, 400, errResp.StatusCode)
}

func TestDefineAndDescribeExpression(t *testing.T) {
	p := newTestProvider(t)

	callQuery(t, p, "CreateDomain", map[string]string{"DomainName": "expr-domain"})

	resp := callQuery(t, p, "DefineExpression", map[string]string{
		"DomainName":                 "expr-domain",
		"Expression.ExpressionName":  "rank",
		"Expression.ExpressionValue": "_score*2",
	})
	assert.Equal(t, 200, resp.StatusCode, string(resp.Body))

	descResp := callQuery(t, p, "DescribeExpressions", map[string]string{
		"DomainName": "expr-domain",
	})
	assert.Equal(t, 200, descResp.StatusCode)

	type describeResp struct {
		Expressions []struct {
			Options struct {
				ExpressionName string `xml:"ExpressionName"`
			} `xml:"Options"`
		} `xml:"DescribeExpressionsResult>Expressions>member"`
	}
	var dr describeResp
	require.NoError(t, xml.Unmarshal(descResp.Body, &dr))
	require.Len(t, dr.Expressions, 1)
	assert.Equal(t, "rank", dr.Expressions[0].Options.ExpressionName)
}

func TestOptionsOperations(t *testing.T) {
	p := newTestProvider(t)
	callQuery(t, p, "CreateDomain", map[string]string{"DomainName": "opts-domain"})

	// DescribeAvailabilityOptions
	r1 := callQuery(t, p, "DescribeAvailabilityOptions", map[string]string{"DomainName": "opts-domain"})
	assert.Equal(t, 200, r1.StatusCode)

	// UpdateAvailabilityOptions
	r2 := callQuery(t, p, "UpdateAvailabilityOptions", map[string]string{
		"DomainName": "opts-domain",
		"MultiAZ":    "true",
	})
	assert.Equal(t, 200, r2.StatusCode)

	// DescribeScalingParameters
	r3 := callQuery(t, p, "DescribeScalingParameters", map[string]string{"DomainName": "opts-domain"})
	assert.Equal(t, 200, r3.StatusCode)

	// UpdateScalingParameters
	r4 := callQuery(t, p, "UpdateScalingParameters", map[string]string{
		"DomainName":                            "opts-domain",
		"ScalingParameters.DesiredInstanceType": "search.m2.xlarge",
	})
	assert.Equal(t, 200, r4.StatusCode)

	// DescribeServiceAccessPolicies
	r5 := callQuery(t, p, "DescribeServiceAccessPolicies", map[string]string{"DomainName": "opts-domain"})
	assert.Equal(t, 200, r5.StatusCode)

	// UpdateServiceAccessPolicies
	r6 := callQuery(t, p, "UpdateServiceAccessPolicies", map[string]string{
		"DomainName":     "opts-domain",
		"AccessPolicies": `{"Version":"2012-10-17","Statement":[]}`,
	})
	assert.Equal(t, 200, r6.StatusCode)
}
