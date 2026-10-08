// SPDX-License-Identifier: Apache-2.0

package cloudformation

import (
	"encoding/xml"
	"fmt"
	"net/http"
	"net/url"

	"github.com/skyoo2003/devcloud/internal/plugin"
	"github.com/skyoo2003/devcloud/internal/shared"
)

func (p *Provider) handleCancelUpdateStack(form url.Values) (*plugin.Response, error) {
	name := form.Get("StackName")
	if name == "" {
		return cfnError("ValidationError", "StackName is required", http.StatusBadRequest), nil
	}

	st, err := p.store.GetStack(name)
	if err != nil || st == nil {
		return cfnError("ValidationError", fmt.Sprintf("Stack [%s] does not exist", name), http.StatusBadRequest), nil
	}

	_ = p.store.SetStackStatus(name, "UPDATE_ROLLBACK_COMPLETE")

	type result struct {
		XMLName xml.Name `xml:"CancelUpdateStackResponse"`
		Meta    respMeta
	}
	var r result
	r.Meta = newMeta()
	return cfnXMLResponse(http.StatusOK, r)
}

func (p *Provider) handleDetectStackSetDrift(form url.Values) (*plugin.Response, error) {
	name := form.Get("StackSetName")
	if name == "" {
		return cfnError("ValidationError", "StackSetName is required", http.StatusBadRequest), nil
	}

	type result struct {
		XMLName xml.Name `xml:"DetectStackSetDriftResponse"`
		Result  struct {
			OperationID string `xml:"OperationId"`
		} `xml:"DetectStackSetDriftResult"`
		Meta respMeta
	}
	var r result
	r.Result.OperationID = shared.GenerateUUID()
	r.Meta = newMeta()
	return cfnXMLResponse(http.StatusOK, r)
}

func (p *Provider) handleExecuteStackRefactor(form url.Values) (*plugin.Response, error) {
	type result struct {
		XMLName xml.Name `xml:"ExecuteStackRefactorResponse"`
		Meta    respMeta
	}
	var r result
	r.Meta = newMeta()
	return cfnXMLResponse(http.StatusOK, r)
}

func (p *Provider) handleStartResourceScan(form url.Values) (*plugin.Response, error) {
	type result struct {
		XMLName xml.Name `xml:"StartResourceScanResponse"`
		Result  struct {
			ResourceScanID string `xml:"ResourceScanId"`
		} `xml:"StartResourceScanResult"`
		Meta respMeta
	}
	var r result
	r.Result.ResourceScanID = fmt.Sprintf("arn:aws:cloudformation:us-east-1:000000000000:resource-scan/%s", shared.GenerateUUID())
	r.Meta = newMeta()
	return cfnXMLResponse(http.StatusOK, r)
}

func (p *Provider) handleEstimateTemplateCost(form url.Values) (*plugin.Response, error) {
	type result struct {
		XMLName xml.Name `xml:"EstimateTemplateCostResponse"`
		Result  struct {
			URL string `xml:"Url"`
		} `xml:"EstimateTemplateCostResult"`
		Meta respMeta
	}
	var r result
	r.Result.URL = fmt.Sprintf("http://calculator.s3.amazonaws.com/calc5.html?key=cloudformation-%s", shared.GenerateID("", 16))
	r.Meta = newMeta()
	return cfnXMLResponse(http.StatusOK, r)
}

func (p *Provider) handleRecordHandlerProgress(form url.Values) (*plugin.Response, error) {
	type result struct {
		XMLName xml.Name `xml:"RecordHandlerProgressResponse"`
		Result  struct{} `xml:"RecordHandlerProgressResult"`
		Meta    respMeta
	}
	var r result
	r.Meta = newMeta()
	return cfnXMLResponse(http.StatusOK, r)
}

func (p *Provider) handleActivateOrganizationsAccess(form url.Values) (*plugin.Response, error) {
	type result struct {
		XMLName xml.Name `xml:"ActivateOrganizationsAccessResponse"`
		Result  struct{} `xml:"ActivateOrganizationsAccessResult"`
		Meta    respMeta
	}
	var r result
	r.Meta = newMeta()
	return cfnXMLResponse(http.StatusOK, r)
}

func (p *Provider) handleDeactivateOrganizationsAccess(form url.Values) (*plugin.Response, error) {
	type result struct {
		XMLName xml.Name `xml:"DeactivateOrganizationsAccessResponse"`
		Result  struct{} `xml:"DeactivateOrganizationsAccessResult"`
		Meta    respMeta
	}
	var r result
	r.Meta = newMeta()
	return cfnXMLResponse(http.StatusOK, r)
}
