// SPDX-License-Identifier: Apache-2.0

package iam

import (
	"context"
	"encoding/xml"
	"fmt"
	"net/http"
	"net/url"

	"github.com/skyoo2003/devcloud/internal/plugin"
)

// --- Response structs ---

type addClientIDToOpenIDConnectProviderResponse struct {
	XMLName xml.Name `xml:"AddClientIDToOpenIDConnectProviderResponse"`
}

type removeClientIDFromOpenIDConnectProviderResponse struct {
	XMLName xml.Name `xml:"RemoveClientIDFromOpenIDConnectProviderResponse"`
}

type acceptDelegationRequestResponse struct {
	XMLName xml.Name `xml:"AcceptDelegationRequestResponse"`
}

type associateDelegationRequestResponse struct {
	XMLName xml.Name `xml:"AssociateDelegationRequestResponse"`
}

type rejectDelegationRequestResponse struct {
	XMLName xml.Name `xml:"RejectDelegationRequestResponse"`
}

type sendDelegationTokenResponse struct {
	XMLName xml.Name `xml:"SendDelegationTokenResponse"`
}

type generateCredentialReportResponse struct {
	XMLName                        xml.Name                       `xml:"GenerateCredentialReportResponse"`
	GenerateCredentialReportResult generateCredentialReportResult `xml:"GenerateCredentialReportResult"`
}

type generateCredentialReportResult struct {
	State       string `xml:"State"`
	Description string `xml:"Description"`
}

type generateOrganizationsAccessReportResponse struct {
	XMLName                                 xml.Name                                `xml:"GenerateOrganizationsAccessReportResponse"`
	GenerateOrganizationsAccessReportResult generateOrganizationsAccessReportResult `xml:"GenerateOrganizationsAccessReportResult"`
}

type generateOrganizationsAccessReportResult struct {
	JobID string `xml:"JobId"`
}

type generateServiceLastAccessedDetailsResponse struct {
	XMLName                                  xml.Name                                 `xml:"GenerateServiceLastAccessedDetailsResponse"`
	GenerateServiceLastAccessedDetailsResult generateServiceLastAccessedDetailsResult `xml:"GenerateServiceLastAccessedDetailsResult"`
}

type generateServiceLastAccessedDetailsResult struct {
	JobID string `xml:"JobId"`
}

type simulateCustomPolicyResponse struct {
	XMLName              xml.Name             `xml:"SimulateCustomPolicyResponse"`
	SimulatePolicyResult simulatePolicyResult `xml:"SimulatePolicyResult"`
}

type simulatePrincipalPolicyResponse struct {
	XMLName              xml.Name             `xml:"SimulatePrincipalPolicyResponse"`
	SimulatePolicyResult simulatePolicyResult `xml:"SimulatePolicyResult"`
}

type simulatePolicyResult struct {
	EvaluationResults []evaluationResultXML `xml:"EvaluationResults>member"`
	IsTruncated       bool                  `xml:"IsTruncated"`
}

type evaluationResultXML struct {
	EvalActionName   string `xml:"EvalActionName"`
	EvalDecision     string `xml:"EvalDecision"`
	EvalResourceName string `xml:"EvalResourceName"`
}

type enableOrganizationsRootCredentialsManagementResponse struct {
	XMLName                                            xml.Name                                           `xml:"EnableOrganizationsRootCredentialsManagementResponse"`
	EnableOrganizationsRootCredentialsManagementResult enableOrganizationsRootCredentialsManagementResult `xml:"EnableOrganizationsRootCredentialsManagementResult"`
}

type enableOrganizationsRootCredentialsManagementResult struct {
	OrganizationID  string   `xml:"OrganizationId"`
	EnabledFeatures []string `xml:"EnabledFeatures>member"`
}

type disableOrganizationsRootCredentialsManagementResponse struct {
	XMLName                                             xml.Name                                            `xml:"DisableOrganizationsRootCredentialsManagementResponse"`
	DisableOrganizationsRootCredentialsManagementResult disableOrganizationsRootCredentialsManagementResult `xml:"DisableOrganizationsRootCredentialsManagementResult"`
}

type disableOrganizationsRootCredentialsManagementResult struct {
	OrganizationID  string   `xml:"OrganizationId"`
	EnabledFeatures []string `xml:"EnabledFeatures>member,omitempty"`
}

type enableOrganizationsRootSessionsResponse struct {
	XMLName                               xml.Name                              `xml:"EnableOrganizationsRootSessionsResponse"`
	EnableOrganizationsRootSessionsResult enableOrganizationsRootSessionsResult `xml:"EnableOrganizationsRootSessionsResult"`
}

type enableOrganizationsRootSessionsResult struct {
	OrganizationID  string   `xml:"OrganizationId"`
	EnabledFeatures []string `xml:"EnabledFeatures>member"`
}

type disableOrganizationsRootSessionsResponse struct {
	XMLName                                xml.Name                               `xml:"DisableOrganizationsRootSessionsResponse"`
	DisableOrganizationsRootSessionsResult disableOrganizationsRootSessionsResult `xml:"DisableOrganizationsRootSessionsResult"`
}

type disableOrganizationsRootSessionsResult struct {
	OrganizationID  string   `xml:"OrganizationId"`
	EnabledFeatures []string `xml:"EnabledFeatures>member,omitempty"`
}

type enableOutboundWebIdentityFederationResponse struct {
	XMLName                                   xml.Name                                  `xml:"EnableOutboundWebIdentityFederationResponse"`
	EnableOutboundWebIdentityFederationResult enableOutboundWebIdentityFederationResult `xml:"EnableOutboundWebIdentityFederationResult"`
}

type enableOutboundWebIdentityFederationResult struct {
	IssuerIdentifier string `xml:"IssuerIdentifier"`
}

type disableOutboundWebIdentityFederationResponse struct {
	XMLName xml.Name `xml:"DisableOutboundWebIdentityFederationResponse"`
}

// --- Handlers ---

func (p *IAMProvider) handleAddClientIDToOpenIDConnectProvider(_ context.Context, form url.Values) (*plugin.Response, error) {
	arn := form.Get("OpenIDConnectProviderArn")
	clientID := form.Get("ClientID")
	if arn == "" || clientID == "" {
		return iamXMLError("MissingParameter", "OpenIDConnectProviderArn and ClientID are required", http.StatusBadRequest), nil
	}
	if err := p.store.AddClientIDToOpenIDConnectProvider(defaultAccountID, arn, clientID); err != nil {
		return iamXMLError("InternalFailure", err.Error(), http.StatusInternalServerError), nil
	}
	return iamXMLResponse(http.StatusOK, addClientIDToOpenIDConnectProviderResponse{})
}

func (p *IAMProvider) handleRemoveClientIDFromOpenIDConnectProvider(_ context.Context, form url.Values) (*plugin.Response, error) {
	arn := form.Get("OpenIDConnectProviderArn")
	clientID := form.Get("ClientID")
	if arn == "" || clientID == "" {
		return iamXMLError("MissingParameter", "OpenIDConnectProviderArn and ClientID are required", http.StatusBadRequest), nil
	}
	if err := p.store.RemoveClientIDFromOpenIDConnectProvider(defaultAccountID, arn, clientID); err != nil {
		return iamXMLError("InternalFailure", err.Error(), http.StatusInternalServerError), nil
	}
	return iamXMLResponse(http.StatusOK, removeClientIDFromOpenIDConnectProviderResponse{})
}

func (p *IAMProvider) handleAcceptDelegationRequest(_ context.Context, form url.Values) (*plugin.Response, error) {
	reqID := form.Get("DelegationRequestId")
	if reqID == "" {
		return iamXMLError("MissingParameter", "DelegationRequestId is required", http.StatusBadRequest), nil
	}
	_ = p.store.RecordDelegationRequest(defaultAccountID, reqID, defaultAccountID, "", "Accepted")
	return iamXMLResponse(http.StatusOK, acceptDelegationRequestResponse{})
}

func (p *IAMProvider) handleAssociateDelegationRequest(_ context.Context, form url.Values) (*plugin.Response, error) {
	reqID := form.Get("DelegationRequestId")
	targetAccount := form.Get("TargetAccount")
	if reqID == "" {
		return iamXMLError("MissingParameter", "DelegationRequestId is required", http.StatusBadRequest), nil
	}
	_ = p.store.RecordDelegationRequest(defaultAccountID, reqID, targetAccount, "", "Associated")
	return iamXMLResponse(http.StatusOK, associateDelegationRequestResponse{})
}

func (p *IAMProvider) handleRejectDelegationRequest(_ context.Context, form url.Values) (*plugin.Response, error) {
	reqID := form.Get("DelegationRequestId")
	if reqID == "" {
		return iamXMLError("MissingParameter", "DelegationRequestId is required", http.StatusBadRequest), nil
	}
	_ = p.store.RecordDelegationRequest(defaultAccountID, reqID, defaultAccountID, "", "Rejected")
	return iamXMLResponse(http.StatusOK, rejectDelegationRequestResponse{})
}

func (p *IAMProvider) handleSendDelegationToken(_ context.Context, form url.Values) (*plugin.Response, error) {
	targetAccount := form.Get("TargetAccount")
	token := form.Get("Token")
	if targetAccount == "" || token == "" {
		return iamXMLError("MissingParameter", "TargetAccount and Token are required", http.StatusBadRequest), nil
	}
	reqID, _ := generateID()
	_ = p.store.RecordDelegationRequest(defaultAccountID, reqID, targetAccount, token, "Sent")
	return iamXMLResponse(http.StatusOK, sendDelegationTokenResponse{})
}

func (p *IAMProvider) handleGenerateCredentialReport(_ context.Context, _ url.Values) (*plugin.Response, error) {
	return iamXMLResponse(http.StatusOK, generateCredentialReportResponse{
		GenerateCredentialReportResult: generateCredentialReportResult{
			State:       "COMPLETE",
			Description: "Report generated successfully",
		},
	})
}

func (p *IAMProvider) handleGenerateOrganizationsAccessReport(_ context.Context, _ url.Values) (*plugin.Response, error) {
	jobID, err := generateID()
	if err != nil {
		return nil, err
	}
	return iamXMLResponse(http.StatusOK, generateOrganizationsAccessReportResponse{
		GenerateOrganizationsAccessReportResult: generateOrganizationsAccessReportResult{
			JobID: fmt.Sprintf("job-%s", jobID),
		},
	})
}

func (p *IAMProvider) handleGenerateServiceLastAccessedDetails(_ context.Context, _ url.Values) (*plugin.Response, error) {
	jobID, err := generateID()
	if err != nil {
		return nil, err
	}
	return iamXMLResponse(http.StatusOK, generateServiceLastAccessedDetailsResponse{
		GenerateServiceLastAccessedDetailsResult: generateServiceLastAccessedDetailsResult{
			JobID: fmt.Sprintf("job-%s", jobID),
		},
	})
}

func (p *IAMProvider) handleSimulateCustomPolicy(_ context.Context, form url.Values) (*plugin.Response, error) {
	var actions []string
	for i := 1; ; i++ {
		action := form.Get(fmt.Sprintf("ActionNames.member.%d", i))
		if action == "" {
			break
		}
		actions = append(actions, action)
	}
	if len(actions) == 0 {
		actions = append(actions, "sts:GetCallerIdentity")
	}

	results := make([]evaluationResultXML, 0, len(actions))
	for _, act := range actions {
		results = append(results, evaluationResultXML{
			EvalActionName:   act,
			EvalDecision:     "allowed",
			EvalResourceName: "*",
		})
	}

	return iamXMLResponse(http.StatusOK, simulateCustomPolicyResponse{
		SimulatePolicyResult: simulatePolicyResult{
			EvaluationResults: results,
			IsTruncated:       false,
		},
	})
}

func (p *IAMProvider) handleSimulatePrincipalPolicy(_ context.Context, form url.Values) (*plugin.Response, error) {
	policyArn := form.Get("PolicySourceArn")
	if policyArn == "" {
		return iamXMLError("MissingParameter", "PolicySourceArn is required", http.StatusBadRequest), nil
	}

	var actions []string
	for i := 1; ; i++ {
		action := form.Get(fmt.Sprintf("ActionNames.member.%d", i))
		if action == "" {
			break
		}
		actions = append(actions, action)
	}
	if len(actions) == 0 {
		actions = append(actions, "sts:GetCallerIdentity")
	}

	results := make([]evaluationResultXML, 0, len(actions))
	for _, act := range actions {
		results = append(results, evaluationResultXML{
			EvalActionName:   act,
			EvalDecision:     "allowed",
			EvalResourceName: "*",
		})
	}

	return iamXMLResponse(http.StatusOK, simulatePrincipalPolicyResponse{
		SimulatePolicyResult: simulatePolicyResult{
			EvaluationResults: results,
			IsTruncated:       false,
		},
	})
}

func (p *IAMProvider) handleEnableOrganizationsRootCredentialsManagement(_ context.Context, _ url.Values) (*plugin.Response, error) {
	t := true
	_ = p.store.SetOrgSettings(defaultAccountID, &t, nil, nil)
	return iamXMLResponse(http.StatusOK, enableOrganizationsRootCredentialsManagementResponse{
		EnableOrganizationsRootCredentialsManagementResult: enableOrganizationsRootCredentialsManagementResult{
			OrganizationID:  "o-local00000",
			EnabledFeatures: []string{"RootCredentialsManagement"},
		},
	})
}

func (p *IAMProvider) handleDisableOrganizationsRootCredentialsManagement(_ context.Context, _ url.Values) (*plugin.Response, error) {
	f := false
	_ = p.store.SetOrgSettings(defaultAccountID, &f, nil, nil)
	return iamXMLResponse(http.StatusOK, disableOrganizationsRootCredentialsManagementResponse{
		DisableOrganizationsRootCredentialsManagementResult: disableOrganizationsRootCredentialsManagementResult{
			OrganizationID: "o-local00000",
		},
	})
}

func (p *IAMProvider) handleEnableOrganizationsRootSessions(_ context.Context, _ url.Values) (*plugin.Response, error) {
	t := true
	_ = p.store.SetOrgSettings(defaultAccountID, nil, &t, nil)
	return iamXMLResponse(http.StatusOK, enableOrganizationsRootSessionsResponse{
		EnableOrganizationsRootSessionsResult: enableOrganizationsRootSessionsResult{
			OrganizationID:  "o-local00000",
			EnabledFeatures: []string{"RootSessions"},
		},
	})
}

func (p *IAMProvider) handleDisableOrganizationsRootSessions(_ context.Context, _ url.Values) (*plugin.Response, error) {
	f := false
	_ = p.store.SetOrgSettings(defaultAccountID, nil, &f, nil)
	return iamXMLResponse(http.StatusOK, disableOrganizationsRootSessionsResponse{
		DisableOrganizationsRootSessionsResult: disableOrganizationsRootSessionsResult{
			OrganizationID: "o-local00000",
		},
	})
}

func (p *IAMProvider) handleEnableOutboundWebIdentityFederation(_ context.Context, _ url.Values) (*plugin.Response, error) {
	t := true
	_ = p.store.SetOrgSettings(defaultAccountID, nil, nil, &t)
	return iamXMLResponse(http.StatusOK, enableOutboundWebIdentityFederationResponse{
		EnableOutboundWebIdentityFederationResult: enableOutboundWebIdentityFederationResult{
			IssuerIdentifier: "https://sts.devcloud.local",
		},
	})
}

func (p *IAMProvider) handleDisableOutboundWebIdentityFederation(_ context.Context, _ url.Values) (*plugin.Response, error) {
	f := false
	_ = p.store.SetOrgSettings(defaultAccountID, nil, nil, &f)
	return iamXMLResponse(http.StatusOK, disableOutboundWebIdentityFederationResponse{})
}
