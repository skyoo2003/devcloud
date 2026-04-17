// SPDX-License-Identifier: Apache-2.0

package cloudsearch

import (
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"

	"strings"

	"github.com/skyoo2003/devcloud/internal/plugin"
	"github.com/skyoo2003/devcloud/internal/shared"
)

// Provider implements the A9SearchCloudConfigService2013 service (Query/XML protocol).
type Provider struct {
	store *Store
}

func (p *Provider) ServiceID() string             { return "cloudsearch" }
func (p *Provider) ServiceName() string           { return "A9SearchCloudConfigService2013" }
func (p *Provider) Protocol() plugin.ProtocolType { return plugin.ProtocolQuery }

func (p *Provider) Init(cfg plugin.PluginConfig) error {
	if err := os.MkdirAll(cfg.DataDir, 0o755); err != nil {
		return fmt.Errorf("init cloudsearch: %w", err)
	}
	var err error
	p.store, err = NewStore(cfg.DataDir)
	return err
}

func (p *Provider) Shutdown(_ context.Context) error {
	if p.store != nil {
		return p.store.Close()
	}
	return nil
}

func (p *Provider) HandleRequest(ctx context.Context, op string, req *http.Request) (*plugin.Response, error) {
	body, err := io.ReadAll(req.Body)
	if err != nil {
		return csError("InvalidRequest", "failed to read request body", http.StatusBadRequest), nil
	}
	form, err := url.ParseQuery(string(body))
	if err != nil {
		return csError("InvalidRequest", "failed to parse form body", http.StatusBadRequest), nil
	}
	action := op
	if action == "" {
		action = form.Get("Action")
	}

	switch action {
	// Domain management
	case "CreateDomain":
		return p.handleCreateDomain(form)
	case "DeleteDomain":
		return p.handleDeleteDomain(form)
	case "DescribeDomains":
		return p.handleDescribeDomains(form)
	case "ListDomainNames":
		return p.handleListDomainNames(form)

	// Index fields
	case "DefineIndexField":
		return p.handleDefineIndexField(form)
	case "DescribeIndexFields":
		return p.handleDescribeIndexFields(form)
	case "DeleteIndexField":
		return p.handleDeleteIndexField(form)

	// Expressions
	case "DefineExpression":
		return p.handleDefineExpression(form)
	case "DescribeExpressions":
		return p.handleDescribeExpressions(form)
	case "DeleteExpression":
		return p.handleDeleteExpression(form)

	// Suggesters
	case "DefineSuggester":
		return p.handleDefineSuggester(form)
	case "DescribeSuggesters":
		return p.handleDescribeSuggesters(form)
	case "DeleteSuggester":
		return p.handleDeleteSuggester(form)

	// Analysis schemes
	case "DefineAnalysisScheme":
		return p.handleDefineAnalysisScheme(form)
	case "DescribeAnalysisSchemes":
		return p.handleDescribeAnalysisSchemes(form)
	case "DeleteAnalysisScheme":
		return p.handleDeleteAnalysisScheme(form)

	// Index / build
	case "IndexDocuments":
		return p.handleIndexDocuments(form)
	case "BuildSuggesters":
		return p.handleBuildSuggesters(form)

	// Options
	case "DescribeAvailabilityOptions":
		return p.handleDescribeAvailabilityOptions(form)
	case "UpdateAvailabilityOptions":
		return p.handleUpdateAvailabilityOptions(form)
	case "DescribeDomainEndpointOptions":
		return p.handleDescribeDomainEndpointOptions(form)
	case "UpdateDomainEndpointOptions":
		return p.handleUpdateDomainEndpointOptions(form)
	case "DescribeScalingParameters":
		return p.handleDescribeScalingParameters(form)
	case "UpdateScalingParameters":
		return p.handleUpdateScalingParameters(form)
	case "DescribeServiceAccessPolicies":
		return p.handleDescribeServiceAccessPolicies(form)
	case "UpdateServiceAccessPolicies":
		return p.handleUpdateServiceAccessPolicies(form)

	default:
		return csError("InvalidAction", fmt.Sprintf("unknown action: %s", action), http.StatusBadRequest), nil
	}
}

func (p *Provider) ListResources(ctx context.Context) ([]plugin.Resource, error) {
	domains, err := p.store.ListDomains(nil)
	if err != nil {
		return nil, err
	}
	out := make([]plugin.Resource, 0, len(domains))
	for _, d := range domains {
		out = append(out, plugin.Resource{Type: "domain", ID: d.Name, Name: d.Name})
	}
	return out, nil
}

func (p *Provider) GetMetrics(_ context.Context) (*plugin.ServiceMetrics, error) {
	return &plugin.ServiceMetrics{}, nil
}

// --- helpers ---

func csError(code, msg string, status int) *plugin.Response {
	return shared.QueryXMLError(code, msg, status)
}

func csXMLResponse(status int, v any) (*plugin.Response, error) {
	return shared.XMLResponse(status, v)
}

// domainStatusXML is the common domain status sub-element.
type domainStatusXML struct {
	DomainId       string `xml:"DomainId"`
	DomainName     string `xml:"DomainName"`
	ARN            string `xml:"ARN"`
	Created        bool   `xml:"Created"`
	Deleted        bool   `xml:"Deleted"`
	Processing     bool   `xml:"Processing"`
	SearchEndpoint string `xml:"SearchEndpoint,omitempty"`
	DocEndpoint    string `xml:"DocService>Endpoint,omitempty"`
}

func domainToXML(d *Domain) domainStatusXML {
	return domainStatusXML{
		DomainId:       d.DomainID,
		DomainName:     d.Name,
		ARN:            d.ARN,
		Created:        true,
		Processing:     false,
		SearchEndpoint: d.SearchEndpoint,
		DocEndpoint:    d.DocEndpoint,
	}
}

// --- Domain handlers ---

func (p *Provider) handleCreateDomain(form url.Values) (*plugin.Response, error) {
	name := form.Get("DomainName")
	if name == "" {
		return csError("MissingParameter", "DomainName is required", http.StatusBadRequest), nil
	}
	arn := shared.BuildARN("cloudsearch", "domain", name)
	domainID := shared.GenerateID("", 22)
	d, err := p.store.CreateDomain(name, arn, domainID)
	if err != nil {
		if strings.Contains(err.Error(), "UNIQUE constraint failed") {
			return csError("ResourceAlreadyExistsException", "domain already exists: "+name, http.StatusConflict), nil
		}
		return nil, err
	}

	type createDomainResponse struct {
		XMLName      xml.Name        `xml:"CreateDomainResponse"`
		DomainStatus domainStatusXML `xml:"CreateDomainResult>DomainStatus"`
	}
	return csXMLResponse(http.StatusOK, createDomainResponse{DomainStatus: domainToXML(d)})
}

func (p *Provider) handleDeleteDomain(form url.Values) (*plugin.Response, error) {
	name := form.Get("DomainName")
	if name == "" {
		return csError("MissingParameter", "DomainName is required", http.StatusBadRequest), nil
	}
	d, err := p.store.DeleteDomain(name)
	if err != nil {
		if errors.Is(err, errDomainNotFound) {
			return csError("ResourceNotFoundException", "domain not found: "+name, http.StatusBadRequest), nil
		}
		return nil, err
	}
	ds := domainToXML(d)
	ds.Deleted = true

	type deleteDomainResponse struct {
		XMLName      xml.Name        `xml:"DeleteDomainResponse"`
		DomainStatus domainStatusXML `xml:"DeleteDomainResult>DomainStatus"`
	}
	return csXMLResponse(http.StatusOK, deleteDomainResponse{DomainStatus: ds})
}

func (p *Provider) handleDescribeDomains(form url.Values) (*plugin.Response, error) {
	var names []string
	for i := 1; ; i++ {
		n := form.Get(fmt.Sprintf("DomainNames.member.%d", i))
		if n == "" {
			break
		}
		names = append(names, n)
	}
	domains, err := p.store.ListDomains(names)
	if err != nil {
		return nil, err
	}

	items := make([]domainStatusXML, 0, len(domains))
	for i := range domains {
		items = append(items, domainToXML(&domains[i]))
	}

	type describeDomainsResponse struct {
		XMLName     xml.Name          `xml:"DescribeDomainsResponse"`
		DomainItems []domainStatusXML `xml:"DescribeDomainsResult>DomainStatusList>member"`
	}
	return csXMLResponse(http.StatusOK, describeDomainsResponse{DomainItems: items})
}

func (p *Provider) handleListDomainNames(_ url.Values) (*plugin.Response, error) {
	domains, err := p.store.ListDomains(nil)
	if err != nil {
		return nil, err
	}

	type domainNameXML struct {
		Key   string `xml:"key"`
		Value string `xml:"value"`
	}
	type listDomainNamesResponse struct {
		XMLName     xml.Name        `xml:"ListDomainNamesResponse"`
		DomainNames []domainNameXML `xml:"ListDomainNamesResult>DomainNames>entry"`
	}
	entries := make([]domainNameXML, 0, len(domains))
	for _, d := range domains {
		entries = append(entries, domainNameXML{Key: d.Name, Value: d.ARN})
	}
	return csXMLResponse(http.StatusOK, listDomainNamesResponse{DomainNames: entries})
}

// --- IndexField handlers ---

type indexFieldStatusXML struct {
	Options struct {
		IndexFieldName string `xml:"IndexFieldName"`
		IndexFieldType string `xml:"IndexFieldType"`
	} `xml:"Options"`
	Status optionStatusXML `xml:"Status"`
}

type optionStatusXML struct {
	State string `xml:"State"`
}

func (p *Provider) handleDefineIndexField(form url.Values) (*plugin.Response, error) {
	domainName := form.Get("DomainName")
	fieldName := form.Get("IndexField.IndexFieldName")
	fieldType := form.Get("IndexField.IndexFieldType")
	if domainName == "" || fieldName == "" {
		return csError("MissingParameter", "DomainName and IndexField.IndexFieldName are required", http.StatusBadRequest), nil
	}
	if fieldType == "" {
		fieldType = "text"
	}
	// Verify domain exists
	if _, err := p.store.GetDomain(domainName); err != nil {
		if errors.Is(err, errDomainNotFound) {
			return csError("ResourceNotFoundException", "domain not found: "+domainName, http.StatusBadRequest), nil
		}
		return nil, err
	}
	_, err := p.store.DefineIndexField(domainName, fieldName, fieldType, "{}")
	if err != nil {
		return nil, err
	}

	item := indexFieldStatusXML{}
	item.Options.IndexFieldName = fieldName
	item.Options.IndexFieldType = fieldType
	item.Status.State = "RequiresIndexDocuments"

	type defineIndexFieldResponse struct {
		XMLName    xml.Name            `xml:"DefineIndexFieldResponse"`
		IndexField indexFieldStatusXML `xml:"DefineIndexFieldResult>IndexField"`
	}
	return csXMLResponse(http.StatusOK, defineIndexFieldResponse{IndexField: item})
}

func (p *Provider) handleDescribeIndexFields(form url.Values) (*plugin.Response, error) {
	domainName := form.Get("DomainName")
	if domainName == "" {
		return csError("MissingParameter", "DomainName is required", http.StatusBadRequest), nil
	}
	var fieldNames []string
	for i := 1; ; i++ {
		n := form.Get(fmt.Sprintf("FieldNames.member.%d", i))
		if n == "" {
			break
		}
		fieldNames = append(fieldNames, n)
	}
	fields, err := p.store.ListIndexFields(domainName, fieldNames)
	if err != nil {
		return nil, err
	}

	items := make([]indexFieldStatusXML, 0, len(fields))
	for _, f := range fields {
		item := indexFieldStatusXML{}
		item.Options.IndexFieldName = f.Name
		item.Options.IndexFieldType = f.Type
		item.Status.State = "Active"
		items = append(items, item)
	}

	type describeIndexFieldsResponse struct {
		XMLName     xml.Name              `xml:"DescribeIndexFieldsResponse"`
		IndexFields []indexFieldStatusXML `xml:"DescribeIndexFieldsResult>IndexFields>member"`
	}
	return csXMLResponse(http.StatusOK, describeIndexFieldsResponse{IndexFields: items})
}

func (p *Provider) handleDeleteIndexField(form url.Values) (*plugin.Response, error) {
	domainName := form.Get("DomainName")
	fieldName := form.Get("IndexFieldName")
	if domainName == "" || fieldName == "" {
		return csError("MissingParameter", "DomainName and IndexFieldName are required", http.StatusBadRequest), nil
	}
	if err := p.store.DeleteIndexField(domainName, fieldName); err != nil {
		if errors.Is(err, errFieldNotFound) {
			return csError("ResourceNotFoundException", "index field not found: "+fieldName, http.StatusBadRequest), nil
		}
		return nil, err
	}

	item := indexFieldStatusXML{}
	item.Options.IndexFieldName = fieldName
	item.Options.IndexFieldType = "text"
	item.Status.State = "Processing"

	type deleteIndexFieldResponse struct {
		XMLName    xml.Name            `xml:"DeleteIndexFieldResponse"`
		IndexField indexFieldStatusXML `xml:"DeleteIndexFieldResult>IndexField"`
	}
	return csXMLResponse(http.StatusOK, deleteIndexFieldResponse{IndexField: item})
}

// --- Expression handlers ---

type expressionStatusXML struct {
	Options struct {
		ExpressionName  string `xml:"ExpressionName"`
		ExpressionValue string `xml:"ExpressionValue"`
	} `xml:"Options"`
	Status optionStatusXML `xml:"Status"`
}

func (p *Provider) handleDefineExpression(form url.Values) (*plugin.Response, error) {
	domainName := form.Get("DomainName")
	exprName := form.Get("Expression.ExpressionName")
	exprValue := form.Get("Expression.ExpressionValue")
	if domainName == "" || exprName == "" {
		return csError("MissingParameter", "DomainName and Expression.ExpressionName are required", http.StatusBadRequest), nil
	}
	data := fmt.Sprintf(`{"name":%q,"value":%q}`, exprName, exprValue)
	if err := p.store.SetDomainConfig(domainName, "expression", exprName, data); err != nil {
		return nil, err
	}

	item := expressionStatusXML{}
	item.Options.ExpressionName = exprName
	item.Options.ExpressionValue = exprValue
	item.Status.State = "RequiresIndexDocuments"

	type defineExpressionResponse struct {
		XMLName    xml.Name            `xml:"DefineExpressionResponse"`
		Expression expressionStatusXML `xml:"DefineExpressionResult>Expression"`
	}
	return csXMLResponse(http.StatusOK, defineExpressionResponse{Expression: item})
}

func (p *Provider) handleDescribeExpressions(form url.Values) (*plugin.Response, error) {
	domainName := form.Get("DomainName")
	if domainName == "" {
		return csError("MissingParameter", "DomainName is required", http.StatusBadRequest), nil
	}
	cfgs, err := p.store.ListDomainConfigs(domainName, "expression")
	if err != nil {
		return nil, err
	}
	items := make([]expressionStatusXML, 0, len(cfgs))
	for _, c := range cfgs {
		item := expressionStatusXML{}
		item.Options.ExpressionName = c.Name
		item.Status.State = "Active"
		items = append(items, item)
	}

	type describeExpressionsResponse struct {
		XMLName     xml.Name              `xml:"DescribeExpressionsResponse"`
		Expressions []expressionStatusXML `xml:"DescribeExpressionsResult>Expressions>member"`
	}
	return csXMLResponse(http.StatusOK, describeExpressionsResponse{Expressions: items})
}

func (p *Provider) handleDeleteExpression(form url.Values) (*plugin.Response, error) {
	domainName := form.Get("DomainName")
	exprName := form.Get("ExpressionName")
	if domainName == "" || exprName == "" {
		return csError("MissingParameter", "DomainName and ExpressionName are required", http.StatusBadRequest), nil
	}
	if err := p.store.DeleteDomainConfig(domainName, "expression", exprName); err != nil {
		if errors.Is(err, errConfigNotFound) {
			return csError("ResourceNotFoundException", "expression not found: "+exprName, http.StatusBadRequest), nil
		}
		return nil, err
	}

	item := expressionStatusXML{}
	item.Options.ExpressionName = exprName
	item.Status.State = "Processing"

	type deleteExpressionResponse struct {
		XMLName    xml.Name            `xml:"DeleteExpressionResponse"`
		Expression expressionStatusXML `xml:"DeleteExpressionResult>Expression"`
	}
	return csXMLResponse(http.StatusOK, deleteExpressionResponse{Expression: item})
}

// --- Suggester handlers ---

type suggesterStatusXML struct {
	Options struct {
		SuggesterName string `xml:"SuggesterName"`
	} `xml:"Options"`
	Status optionStatusXML `xml:"Status"`
}

func (p *Provider) handleDefineSuggester(form url.Values) (*plugin.Response, error) {
	domainName := form.Get("DomainName")
	suggName := form.Get("Suggester.SuggesterName")
	if domainName == "" || suggName == "" {
		return csError("MissingParameter", "DomainName and Suggester.SuggesterName are required", http.StatusBadRequest), nil
	}
	data := fmt.Sprintf(`{"name":%q}`, suggName)
	if err := p.store.SetDomainConfig(domainName, "suggester", suggName, data); err != nil {
		return nil, err
	}

	item := suggesterStatusXML{}
	item.Options.SuggesterName = suggName
	item.Status.State = "RequiresIndexDocuments"

	type defineSuggesterResponse struct {
		XMLName   xml.Name           `xml:"DefineSuggesterResponse"`
		Suggester suggesterStatusXML `xml:"DefineSuggesterResult>Suggester"`
	}
	return csXMLResponse(http.StatusOK, defineSuggesterResponse{Suggester: item})
}

func (p *Provider) handleDescribeSuggesters(form url.Values) (*plugin.Response, error) {
	domainName := form.Get("DomainName")
	if domainName == "" {
		return csError("MissingParameter", "DomainName is required", http.StatusBadRequest), nil
	}
	cfgs, err := p.store.ListDomainConfigs(domainName, "suggester")
	if err != nil {
		return nil, err
	}
	items := make([]suggesterStatusXML, 0, len(cfgs))
	for _, c := range cfgs {
		item := suggesterStatusXML{}
		item.Options.SuggesterName = c.Name
		item.Status.State = "Active"
		items = append(items, item)
	}

	type describeSuggestersResponse struct {
		XMLName    xml.Name             `xml:"DescribeSuggestersResponse"`
		Suggesters []suggesterStatusXML `xml:"DescribeSuggestersResult>Suggesters>member"`
	}
	return csXMLResponse(http.StatusOK, describeSuggestersResponse{Suggesters: items})
}

func (p *Provider) handleDeleteSuggester(form url.Values) (*plugin.Response, error) {
	domainName := form.Get("DomainName")
	suggName := form.Get("SuggesterName")
	if domainName == "" || suggName == "" {
		return csError("MissingParameter", "DomainName and SuggesterName are required", http.StatusBadRequest), nil
	}
	if err := p.store.DeleteDomainConfig(domainName, "suggester", suggName); err != nil {
		if errors.Is(err, errConfigNotFound) {
			return csError("ResourceNotFoundException", "suggester not found: "+suggName, http.StatusBadRequest), nil
		}
		return nil, err
	}

	item := suggesterStatusXML{}
	item.Options.SuggesterName = suggName
	item.Status.State = "Processing"

	type deleteSuggesterResponse struct {
		XMLName   xml.Name           `xml:"DeleteSuggesterResponse"`
		Suggester suggesterStatusXML `xml:"DeleteSuggesterResult>Suggester"`
	}
	return csXMLResponse(http.StatusOK, deleteSuggesterResponse{Suggester: item})
}

// --- AnalysisScheme handlers ---

type analysisSchemeStatusXML struct {
	Options struct {
		AnalysisSchemeName     string `xml:"AnalysisSchemeName"`
		AnalysisSchemeLanguage string `xml:"AnalysisSchemeLanguage"`
	} `xml:"Options"`
	Status optionStatusXML `xml:"Status"`
}

func (p *Provider) handleDefineAnalysisScheme(form url.Values) (*plugin.Response, error) {
	domainName := form.Get("DomainName")
	schemeName := form.Get("AnalysisScheme.AnalysisSchemeName")
	language := form.Get("AnalysisScheme.AnalysisSchemeLanguage")
	if domainName == "" || schemeName == "" {
		return csError("MissingParameter", "DomainName and AnalysisScheme.AnalysisSchemeName are required", http.StatusBadRequest), nil
	}
	if language == "" {
		language = "mul"
	}
	data := fmt.Sprintf(`{"name":%q,"language":%q}`, schemeName, language)
	if err := p.store.SetDomainConfig(domainName, "analysis_scheme", schemeName, data); err != nil {
		return nil, err
	}

	item := analysisSchemeStatusXML{}
	item.Options.AnalysisSchemeName = schemeName
	item.Options.AnalysisSchemeLanguage = language
	item.Status.State = "RequiresIndexDocuments"

	type defineAnalysisSchemeResponse struct {
		XMLName        xml.Name                `xml:"DefineAnalysisSchemeResponse"`
		AnalysisScheme analysisSchemeStatusXML `xml:"DefineAnalysisSchemeResult>AnalysisScheme"`
	}
	return csXMLResponse(http.StatusOK, defineAnalysisSchemeResponse{AnalysisScheme: item})
}

func (p *Provider) handleDescribeAnalysisSchemes(form url.Values) (*plugin.Response, error) {
	domainName := form.Get("DomainName")
	if domainName == "" {
		return csError("MissingParameter", "DomainName is required", http.StatusBadRequest), nil
	}
	cfgs, err := p.store.ListDomainConfigs(domainName, "analysis_scheme")
	if err != nil {
		return nil, err
	}
	items := make([]analysisSchemeStatusXML, 0, len(cfgs))
	for _, c := range cfgs {
		item := analysisSchemeStatusXML{}
		item.Options.AnalysisSchemeName = c.Name
		item.Status.State = "Active"
		items = append(items, item)
	}

	type describeAnalysisSchemesResponse struct {
		XMLName         xml.Name                  `xml:"DescribeAnalysisSchemesResponse"`
		AnalysisSchemes []analysisSchemeStatusXML `xml:"DescribeAnalysisSchemesResult>AnalysisSchemes>member"`
	}
	return csXMLResponse(http.StatusOK, describeAnalysisSchemesResponse{AnalysisSchemes: items})
}

func (p *Provider) handleDeleteAnalysisScheme(form url.Values) (*plugin.Response, error) {
	domainName := form.Get("DomainName")
	schemeName := form.Get("AnalysisSchemeName")
	if domainName == "" || schemeName == "" {
		return csError("MissingParameter", "DomainName and AnalysisSchemeName are required", http.StatusBadRequest), nil
	}
	if err := p.store.DeleteDomainConfig(domainName, "analysis_scheme", schemeName); err != nil {
		if errors.Is(err, errConfigNotFound) {
			return csError("ResourceNotFoundException", "analysis scheme not found: "+schemeName, http.StatusBadRequest), nil
		}
		return nil, err
	}

	item := analysisSchemeStatusXML{}
	item.Options.AnalysisSchemeName = schemeName
	item.Status.State = "Processing"

	type deleteAnalysisSchemeResponse struct {
		XMLName        xml.Name                `xml:"DeleteAnalysisSchemeResponse"`
		AnalysisScheme analysisSchemeStatusXML `xml:"DeleteAnalysisSchemeResult>AnalysisScheme"`
	}
	return csXMLResponse(http.StatusOK, deleteAnalysisSchemeResponse{AnalysisScheme: item})
}

// --- IndexDocuments / BuildSuggesters ---

func (p *Provider) handleIndexDocuments(form url.Values) (*plugin.Response, error) {
	domainName := form.Get("DomainName")
	if domainName == "" {
		return csError("MissingParameter", "DomainName is required", http.StatusBadRequest), nil
	}
	if _, err := p.store.GetDomain(domainName); err != nil {
		if errors.Is(err, errDomainNotFound) {
			return csError("ResourceNotFoundException", "domain not found: "+domainName, http.StatusBadRequest), nil
		}
		return nil, err
	}
	// Return field names being indexed
	fields, err := p.store.ListIndexFields(domainName, nil)
	if err != nil {
		return nil, err
	}
	fieldNames := make([]string, 0, len(fields))
	for _, f := range fields {
		fieldNames = append(fieldNames, f.Name)
	}

	type indexDocumentsResponse struct {
		XMLName    xml.Name `xml:"IndexDocumentsResponse"`
		FieldNames []string `xml:"IndexDocumentsResult>FieldNames>member"`
	}
	return csXMLResponse(http.StatusOK, indexDocumentsResponse{FieldNames: fieldNames})
}

func (p *Provider) handleBuildSuggesters(form url.Values) (*plugin.Response, error) {
	domainName := form.Get("DomainName")
	if domainName == "" {
		return csError("MissingParameter", "DomainName is required", http.StatusBadRequest), nil
	}
	if _, err := p.store.GetDomain(domainName); err != nil {
		if errors.Is(err, errDomainNotFound) {
			return csError("ResourceNotFoundException", "domain not found: "+domainName, http.StatusBadRequest), nil
		}
		return nil, err
	}
	cfgs, err := p.store.ListDomainConfigs(domainName, "suggester")
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, len(cfgs))
	for _, c := range cfgs {
		names = append(names, c.Name)
	}

	type buildSuggestersResponse struct {
		XMLName    xml.Name `xml:"BuildSuggestersResponse"`
		FieldNames []string `xml:"BuildSuggestersResult>FieldNames>member"`
	}
	return csXMLResponse(http.StatusOK, buildSuggestersResponse{FieldNames: names})
}

// --- Options (return reasonable defaults) ---

func (p *Provider) handleDescribeAvailabilityOptions(form url.Values) (*plugin.Response, error) {
	domainName := form.Get("DomainName")
	if domainName == "" {
		return csError("MissingParameter", "DomainName is required", http.StatusBadRequest), nil
	}
	type availabilityOptionsXML struct {
		Options bool            `xml:"Options"`
		Status  optionStatusXML `xml:"Status"`
	}
	type describeAvailabilityOptionsResponse struct {
		XMLName             xml.Name               `xml:"DescribeAvailabilityOptionsResponse"`
		AvailabilityOptions availabilityOptionsXML `xml:"DescribeAvailabilityOptionsResult>AvailabilityOptions"`
	}
	return csXMLResponse(http.StatusOK, describeAvailabilityOptionsResponse{
		AvailabilityOptions: availabilityOptionsXML{Options: false, Status: optionStatusXML{State: "Active"}},
	})
}

func (p *Provider) handleUpdateAvailabilityOptions(form url.Values) (*plugin.Response, error) {
	domainName := form.Get("DomainName")
	if domainName == "" {
		return csError("MissingParameter", "DomainName is required", http.StatusBadRequest), nil
	}
	type availabilityOptionsXML struct {
		Options bool            `xml:"Options"`
		Status  optionStatusXML `xml:"Status"`
	}
	type updateAvailabilityOptionsResponse struct {
		XMLName             xml.Name               `xml:"UpdateAvailabilityOptionsResponse"`
		AvailabilityOptions availabilityOptionsXML `xml:"UpdateAvailabilityOptionsResult>AvailabilityOptions"`
	}
	multiAZ := form.Get("MultiAZ") == "true"
	return csXMLResponse(http.StatusOK, updateAvailabilityOptionsResponse{
		AvailabilityOptions: availabilityOptionsXML{Options: multiAZ, Status: optionStatusXML{State: "Processing"}},
	})
}

func (p *Provider) handleDescribeDomainEndpointOptions(form url.Values) (*plugin.Response, error) {
	domainName := form.Get("DomainName")
	if domainName == "" {
		return csError("MissingParameter", "DomainName is required", http.StatusBadRequest), nil
	}
	type endpointOptionsXML struct {
		Options struct {
			EnforceHTTPS      bool   `xml:"EnforceHTTPS"`
			TLSSecurityPolicy string `xml:"TLSSecurityPolicy"`
		} `xml:"Options"`
		Status optionStatusXML `xml:"Status"`
	}
	type describeDomainEndpointOptionsResponse struct {
		XMLName               xml.Name           `xml:"DescribeDomainEndpointOptionsResponse"`
		DomainEndpointOptions endpointOptionsXML `xml:"DescribeDomainEndpointOptionsResult>DomainEndpointOptions"`
	}
	opts := endpointOptionsXML{}
	opts.Options.EnforceHTTPS = false
	opts.Options.TLSSecurityPolicy = "Policy-Min-TLS-1-0-2019-07"
	opts.Status.State = "Active"
	return csXMLResponse(http.StatusOK, describeDomainEndpointOptionsResponse{DomainEndpointOptions: opts})
}

func (p *Provider) handleUpdateDomainEndpointOptions(form url.Values) (*plugin.Response, error) {
	domainName := form.Get("DomainName")
	if domainName == "" {
		return csError("MissingParameter", "DomainName is required", http.StatusBadRequest), nil
	}
	type endpointOptionsXML struct {
		Options struct {
			EnforceHTTPS      bool   `xml:"EnforceHTTPS"`
			TLSSecurityPolicy string `xml:"TLSSecurityPolicy"`
		} `xml:"Options"`
		Status optionStatusXML `xml:"Status"`
	}
	type updateDomainEndpointOptionsResponse struct {
		XMLName               xml.Name           `xml:"UpdateDomainEndpointOptionsResponse"`
		DomainEndpointOptions endpointOptionsXML `xml:"UpdateDomainEndpointOptionsResult>DomainEndpointOptions"`
	}
	opts := endpointOptionsXML{}
	opts.Options.EnforceHTTPS = form.Get("DomainEndpointOptions.EnforceHTTPS") == "true"
	opts.Options.TLSSecurityPolicy = form.Get("DomainEndpointOptions.TLSSecurityPolicy")
	if opts.Options.TLSSecurityPolicy == "" {
		opts.Options.TLSSecurityPolicy = "Policy-Min-TLS-1-0-2019-07"
	}
	opts.Status.State = "Processing"
	return csXMLResponse(http.StatusOK, updateDomainEndpointOptionsResponse{DomainEndpointOptions: opts})
}

func (p *Provider) handleDescribeScalingParameters(form url.Values) (*plugin.Response, error) {
	domainName := form.Get("DomainName")
	if domainName == "" {
		return csError("MissingParameter", "DomainName is required", http.StatusBadRequest), nil
	}
	type scalingParamsXML struct {
		Options struct {
			DesiredInstanceType     string `xml:"DesiredInstanceType"`
			DesiredReplicationCount int    `xml:"DesiredReplicationCount"`
			DesiredPartitionCount   int    `xml:"DesiredPartitionCount"`
		} `xml:"Options"`
		Status optionStatusXML `xml:"Status"`
	}
	type describeScalingParametersResponse struct {
		XMLName           xml.Name         `xml:"DescribeScalingParametersResponse"`
		ScalingParameters scalingParamsXML `xml:"DescribeScalingParametersResult>ScalingParameters"`
	}
	sp := scalingParamsXML{}
	sp.Options.DesiredInstanceType = "search.m1.small"
	sp.Options.DesiredReplicationCount = 1
	sp.Options.DesiredPartitionCount = 1
	sp.Status.State = "Active"
	return csXMLResponse(http.StatusOK, describeScalingParametersResponse{ScalingParameters: sp})
}

func (p *Provider) handleUpdateScalingParameters(form url.Values) (*plugin.Response, error) {
	domainName := form.Get("DomainName")
	if domainName == "" {
		return csError("MissingParameter", "DomainName is required", http.StatusBadRequest), nil
	}
	type scalingParamsXML struct {
		Options struct {
			DesiredInstanceType     string `xml:"DesiredInstanceType"`
			DesiredReplicationCount int    `xml:"DesiredReplicationCount"`
			DesiredPartitionCount   int    `xml:"DesiredPartitionCount"`
		} `xml:"Options"`
		Status optionStatusXML `xml:"Status"`
	}
	type updateScalingParametersResponse struct {
		XMLName           xml.Name         `xml:"UpdateScalingParametersResponse"`
		ScalingParameters scalingParamsXML `xml:"UpdateScalingParametersResult>ScalingParameters"`
	}
	sp := scalingParamsXML{}
	sp.Options.DesiredInstanceType = form.Get("ScalingParameters.DesiredInstanceType")
	if sp.Options.DesiredInstanceType == "" {
		sp.Options.DesiredInstanceType = "search.m1.small"
	}
	sp.Options.DesiredReplicationCount = 1
	sp.Options.DesiredPartitionCount = 1
	sp.Status.State = "Processing"
	return csXMLResponse(http.StatusOK, updateScalingParametersResponse{ScalingParameters: sp})
}

func (p *Provider) handleDescribeServiceAccessPolicies(form url.Values) (*plugin.Response, error) {
	domainName := form.Get("DomainName")
	if domainName == "" {
		return csError("MissingParameter", "DomainName is required", http.StatusBadRequest), nil
	}
	type accessPoliciesXML struct {
		Options string          `xml:"Options"`
		Status  optionStatusXML `xml:"Status"`
	}
	type describeServiceAccessPoliciesResponse struct {
		XMLName        xml.Name          `xml:"DescribeServiceAccessPoliciesResponse"`
		AccessPolicies accessPoliciesXML `xml:"DescribeServiceAccessPoliciesResult>AccessPolicies"`
	}
	ap := accessPoliciesXML{
		Options: `{"Version":"2012-10-17","Statement":[]}`,
		Status:  optionStatusXML{State: "Active"},
	}
	return csXMLResponse(http.StatusOK, describeServiceAccessPoliciesResponse{AccessPolicies: ap})
}

func (p *Provider) handleUpdateServiceAccessPolicies(form url.Values) (*plugin.Response, error) {
	domainName := form.Get("DomainName")
	if domainName == "" {
		return csError("MissingParameter", "DomainName is required", http.StatusBadRequest), nil
	}
	policy := form.Get("AccessPolicies")
	if policy == "" {
		policy = `{"Version":"2012-10-17","Statement":[]}`
	}
	type accessPoliciesXML struct {
		Options string          `xml:"Options"`
		Status  optionStatusXML `xml:"Status"`
	}
	type updateServiceAccessPoliciesResponse struct {
		XMLName        xml.Name          `xml:"UpdateServiceAccessPoliciesResponse"`
		AccessPolicies accessPoliciesXML `xml:"UpdateServiceAccessPoliciesResult>AccessPolicies"`
	}
	ap := accessPoliciesXML{
		Options: policy,
		Status:  optionStatusXML{State: "Processing"},
	}
	return csXMLResponse(http.StatusOK, updateServiceAccessPoliciesResponse{AccessPolicies: ap})
}
