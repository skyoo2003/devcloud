// SPDX-License-Identifier: Apache-2.0

// internal/services/apigatewayv2/store.go
package apigatewayv2

import (
	"database/sql"
	"encoding/json"
	"errors"
	"path/filepath"
	"time"

	"github.com/skyoo2003/devcloud/internal/shared"
	"github.com/skyoo2003/devcloud/internal/storage/sqlite"
)

var (
	errAPINotFound                 = errors.New("api not found")
	errRouteNotFound               = errors.New("route not found")
	errIntegrationNotFound         = errors.New("integration not found")
	errIntegrationResponseNotFound = errors.New("integration response not found")
	errAuthorizerNotFound          = errors.New("authorizer not found")
	errDeploymentNotFound          = errors.New("deployment not found")
	errStageNotFound               = errors.New("stage not found")
	errModelNotFound               = errors.New("model not found")
	errDomainNameNotFound          = errors.New("domain name not found")
	errAPIMappingNotFound          = errors.New("api mapping not found")
	errVpcLinkNotFound             = errors.New("vpc link not found")
	errRouteResponseNotFound       = errors.New("route response not found")
)

var migrations = []sqlite.Migration{
	{Version: 1, SQL: `
		CREATE TABLE IF NOT EXISTS apis (
			id               TEXT PRIMARY KEY,
			arn              TEXT NOT NULL UNIQUE,
			name             TEXT NOT NULL,
			protocol_type    TEXT NOT NULL DEFAULT 'HTTP',
			description      TEXT NOT NULL DEFAULT '',
			route_selection  TEXT NOT NULL DEFAULT '',
			api_endpoint     TEXT NOT NULL DEFAULT '',
			created_at       INTEGER NOT NULL
		);
		CREATE TABLE IF NOT EXISTS routes (
			id                 TEXT PRIMARY KEY,
			api_id             TEXT NOT NULL,
			route_key          TEXT NOT NULL,
			target             TEXT NOT NULL DEFAULT '',
			authorization_type TEXT NOT NULL DEFAULT 'NONE',
			authorizer_id      TEXT NOT NULL DEFAULT ''
		);
		CREATE TABLE IF NOT EXISTS integrations (
			id                 TEXT PRIMARY KEY,
			api_id             TEXT NOT NULL,
			type               TEXT NOT NULL DEFAULT 'HTTP_PROXY',
			integration_uri    TEXT NOT NULL DEFAULT '',
			integration_method TEXT NOT NULL DEFAULT '',
			payload_format     TEXT NOT NULL DEFAULT '2.0'
		);
		CREATE TABLE IF NOT EXISTS integration_responses (
			id                   TEXT PRIMARY KEY,
			api_id               TEXT NOT NULL,
			integration_id       TEXT NOT NULL,
			response_key         TEXT NOT NULL DEFAULT 'default',
			template_selection   TEXT NOT NULL DEFAULT ''
		);
		CREATE TABLE IF NOT EXISTS authorizers (
			id              TEXT PRIMARY KEY,
			api_id          TEXT NOT NULL,
			name            TEXT NOT NULL,
			type            TEXT NOT NULL DEFAULT 'JWT',
			identity_source TEXT NOT NULL DEFAULT '',
			jwt_config      TEXT NOT NULL DEFAULT '{}'
		);
		CREATE TABLE IF NOT EXISTS deployments (
			id           TEXT PRIMARY KEY,
			api_id       TEXT NOT NULL,
			description  TEXT NOT NULL DEFAULT '',
			auto_deployed INTEGER NOT NULL DEFAULT 0,
			created_at   INTEGER NOT NULL
		);
		CREATE TABLE IF NOT EXISTS stages (
			name           TEXT NOT NULL,
			api_id         TEXT NOT NULL,
			description    TEXT NOT NULL DEFAULT '',
			deployment_id  TEXT NOT NULL DEFAULT '',
			auto_deploy    INTEGER NOT NULL DEFAULT 0,
			stage_variables TEXT NOT NULL DEFAULT '{}',
			access_log     TEXT NOT NULL DEFAULT '{}',
			created_at     INTEGER NOT NULL,
			PRIMARY KEY (name, api_id)
		);
		CREATE TABLE IF NOT EXISTS models (
			id           TEXT PRIMARY KEY,
			api_id       TEXT NOT NULL,
			name         TEXT NOT NULL,
			content_type TEXT NOT NULL DEFAULT 'application/json',
			schema_def   TEXT NOT NULL DEFAULT '{}'
		);
		CREATE TABLE IF NOT EXISTS domain_names (
			name       TEXT PRIMARY KEY,
			config     TEXT NOT NULL DEFAULT '[]',
			mutual_tls TEXT NOT NULL DEFAULT '{}',
			created_at INTEGER NOT NULL
		);
		CREATE TABLE IF NOT EXISTS api_mappings (
			id              TEXT PRIMARY KEY,
			api_id          TEXT NOT NULL,
			domain_name     TEXT NOT NULL,
			stage           TEXT NOT NULL DEFAULT '',
			api_mapping_key TEXT NOT NULL DEFAULT ''
		);
		CREATE TABLE IF NOT EXISTS vpc_links (
			id              TEXT PRIMARY KEY,
			name            TEXT NOT NULL,
			security_groups TEXT NOT NULL DEFAULT '[]',
			subnet_ids      TEXT NOT NULL DEFAULT '[]',
			status          TEXT NOT NULL DEFAULT 'AVAILABLE',
			created_at      INTEGER NOT NULL
		);
		CREATE TABLE IF NOT EXISTS route_responses (
			id                   TEXT PRIMARY KEY,
			api_id               TEXT NOT NULL,
			route_id             TEXT NOT NULL,
			route_response_key   TEXT NOT NULL DEFAULT 'default',
			model_selection_expr TEXT NOT NULL DEFAULT ''
		);
	`},
}

// --- Model types ---

type API struct {
	ID             string
	ARN            string
	Name           string
	ProtocolType   string
	Description    string
	RouteSelection string
	APIEndpoint    string
	CreatedAt      time.Time
}

type Route struct {
	ID                string
	APIID             string
	RouteKey          string
	Target            string
	AuthorizationType string
	AuthorizerID      string
}

type Integration struct {
	ID                string
	APIID             string
	Type              string
	IntegrationURI    string
	IntegrationMethod string
	PayloadFormat     string
}

type IntegrationResponse struct {
	ID                string
	APIID             string
	IntegrationID     string
	ResponseKey       string
	TemplateSelection string
}

type Authorizer struct {
	ID             string
	APIID          string
	Name           string
	Type           string
	IdentitySource string
	JWTConfig      string // JSON
}

type Deployment struct {
	ID           string
	APIID        string
	Description  string
	AutoDeployed bool
	CreatedAt    time.Time
}

type Stage struct {
	Name           string
	APIID          string
	Description    string
	DeploymentID   string
	AutoDeploy     bool
	StageVariables string // JSON
	AccessLog      string // JSON
	CreatedAt      time.Time
}

type Model struct {
	ID          string
	APIID       string
	Name        string
	ContentType string
	SchemaDef   string // JSON
}

type DomainName struct {
	Name      string
	Config    string // JSON
	MutualTLS string // JSON
	CreatedAt time.Time
}

type APIMapping struct {
	ID            string
	APIID         string
	DomainName    string
	Stage         string
	APIMappingKey string
}

type VpcLink struct {
	ID             string
	Name           string
	SecurityGroups string // JSON
	SubnetIDs      string // JSON
	Status         string
	CreatedAt      time.Time
}

type RouteResponse struct {
	ID                 string
	APIID              string
	RouteID            string
	RouteResponseKey   string
	ModelSelectionExpr string
}

// --- Store ---

type Store struct {
	store *sqlite.Store
	tags  *shared.TagStore
}

func NewStore(dataDir string) (*Store, error) {
	allMigrations := append(migrations, shared.TagMigrations...)
	dbPath := filepath.Join(dataDir, "apigatewayv2.db")
	s, err := sqlite.Open(dbPath, allMigrations)
	if err != nil {
		return nil, err
	}
	return &Store{store: s, tags: shared.NewTagStore(s)}, nil
}

func (s *Store) Close() error { return s.store.Close() }

// --- APIs ---

func (s *Store) CreateAPI(a *API) error {
	now := time.Now().Unix()
	_, err := s.store.DB().Exec(
		`INSERT INTO apis (id, arn, name, protocol_type, description, route_selection, api_endpoint, created_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		a.ID, a.ARN, a.Name, a.ProtocolType, a.Description, a.RouteSelection, a.APIEndpoint, now,
	)
	return err
}

func (s *Store) GetAPI(id string) (*API, error) {
	row := s.store.DB().QueryRow(
		`SELECT id, arn, name, protocol_type, description, route_selection, api_endpoint, created_at
		 FROM apis WHERE id = ?`, id)
	return scanAPI(row)
}

func (s *Store) ListAPIs() ([]API, error) {
	rows, err := s.store.DB().Query(
		`SELECT id, arn, name, protocol_type, description, route_selection, api_endpoint, created_at
		 FROM apis ORDER BY created_at`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var apis []API
	for rows.Next() {
		a, err := scanAPI(rows)
		if err != nil {
			return nil, err
		}
		apis = append(apis, *a)
	}
	return apis, rows.Err()
}

func (s *Store) UpdateAPI(id string, fields map[string]any) error {
	a, err := s.GetAPI(id)
	if err != nil {
		return errAPINotFound
	}
	if v, ok := fields["name"].(string); ok && v != "" {
		a.Name = v
	}
	if v, ok := fields["description"].(string); ok {
		a.Description = v
	}
	if v, ok := fields["routeSelectionExpression"].(string); ok {
		a.RouteSelection = v
	}
	res, err := s.store.DB().Exec(
		`UPDATE apis SET name=?, description=?, route_selection=? WHERE id=?`,
		a.Name, a.Description, a.RouteSelection, id,
	)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errAPINotFound
	}
	return nil
}

func (s *Store) DeleteAPI(id string) (*API, error) {
	a, err := s.GetAPI(id)
	if err != nil {
		return nil, errAPINotFound
	}
	if _, err := s.store.DB().Exec(`DELETE FROM apis WHERE id = ?`, id); err != nil {
		return nil, err
	}
	return a, nil
}

// --- Routes ---

func (s *Store) CreateRoute(r *Route) error {
	_, err := s.store.DB().Exec(
		`INSERT INTO routes (id, api_id, route_key, target, authorization_type, authorizer_id)
		 VALUES (?, ?, ?, ?, ?, ?)`,
		r.ID, r.APIID, r.RouteKey, r.Target, r.AuthorizationType, r.AuthorizerID,
	)
	return err
}

func (s *Store) GetRoute(apiID, routeID string) (*Route, error) {
	row := s.store.DB().QueryRow(
		`SELECT id, api_id, route_key, target, authorization_type, authorizer_id
		 FROM routes WHERE id = ? AND api_id = ?`, routeID, apiID)
	return scanRoute(row)
}

func (s *Store) ListRoutes(apiID string) ([]Route, error) {
	rows, err := s.store.DB().Query(
		`SELECT id, api_id, route_key, target, authorization_type, authorizer_id
		 FROM routes WHERE api_id = ?`, apiID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var routes []Route
	for rows.Next() {
		r, err := scanRoute(rows)
		if err != nil {
			return nil, err
		}
		routes = append(routes, *r)
	}
	return routes, rows.Err()
}

func (s *Store) UpdateRoute(apiID, routeID string, fields map[string]any) error {
	r, err := s.GetRoute(apiID, routeID)
	if err != nil {
		return errRouteNotFound
	}
	if v, ok := fields["routeKey"].(string); ok && v != "" {
		r.RouteKey = v
	}
	if v, ok := fields["target"].(string); ok {
		r.Target = v
	}
	if v, ok := fields["authorizationType"].(string); ok {
		r.AuthorizationType = v
	}
	if v, ok := fields["authorizerId"].(string); ok {
		r.AuthorizerID = v
	}
	res, err := s.store.DB().Exec(
		`UPDATE routes SET route_key=?, target=?, authorization_type=?, authorizer_id=? WHERE id=? AND api_id=?`,
		r.RouteKey, r.Target, r.AuthorizationType, r.AuthorizerID, routeID, apiID,
	)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errRouteNotFound
	}
	return nil
}

func (s *Store) DeleteRoute(apiID, routeID string) (*Route, error) {
	r, err := s.GetRoute(apiID, routeID)
	if err != nil {
		return nil, errRouteNotFound
	}
	if _, err := s.store.DB().Exec(`DELETE FROM routes WHERE id = ? AND api_id = ?`, routeID, apiID); err != nil {
		return nil, err
	}
	return r, nil
}

// --- Integrations ---

func (s *Store) CreateIntegration(i *Integration) error {
	_, err := s.store.DB().Exec(
		`INSERT INTO integrations (id, api_id, type, integration_uri, integration_method, payload_format)
		 VALUES (?, ?, ?, ?, ?, ?)`,
		i.ID, i.APIID, i.Type, i.IntegrationURI, i.IntegrationMethod, i.PayloadFormat,
	)
	return err
}

func (s *Store) GetIntegration(apiID, integrationID string) (*Integration, error) {
	row := s.store.DB().QueryRow(
		`SELECT id, api_id, type, integration_uri, integration_method, payload_format
		 FROM integrations WHERE id = ? AND api_id = ?`, integrationID, apiID)
	return scanIntegration(row)
}

func (s *Store) ListIntegrations(apiID string) ([]Integration, error) {
	rows, err := s.store.DB().Query(
		`SELECT id, api_id, type, integration_uri, integration_method, payload_format
		 FROM integrations WHERE api_id = ?`, apiID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []Integration
	for rows.Next() {
		i, err := scanIntegration(rows)
		if err != nil {
			return nil, err
		}
		items = append(items, *i)
	}
	return items, rows.Err()
}

func (s *Store) UpdateIntegration(apiID, integrationID string, fields map[string]any) error {
	i, err := s.GetIntegration(apiID, integrationID)
	if err != nil {
		return errIntegrationNotFound
	}
	if v, ok := fields["integrationType"].(string); ok && v != "" {
		i.Type = v
	}
	if v, ok := fields["integrationUri"].(string); ok {
		i.IntegrationURI = v
	}
	if v, ok := fields["integrationMethod"].(string); ok {
		i.IntegrationMethod = v
	}
	if v, ok := fields["payloadFormatVersion"].(string); ok {
		i.PayloadFormat = v
	}
	res, err := s.store.DB().Exec(
		`UPDATE integrations SET type=?, integration_uri=?, integration_method=?, payload_format=? WHERE id=? AND api_id=?`,
		i.Type, i.IntegrationURI, i.IntegrationMethod, i.PayloadFormat, integrationID, apiID,
	)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errIntegrationNotFound
	}
	return nil
}

func (s *Store) DeleteIntegration(apiID, integrationID string) (*Integration, error) {
	i, err := s.GetIntegration(apiID, integrationID)
	if err != nil {
		return nil, errIntegrationNotFound
	}
	if _, err := s.store.DB().Exec(`DELETE FROM integrations WHERE id = ? AND api_id = ?`, integrationID, apiID); err != nil {
		return nil, err
	}
	return i, nil
}

// --- Integration Responses ---

func (s *Store) CreateIntegrationResponse(ir *IntegrationResponse) error {
	_, err := s.store.DB().Exec(
		`INSERT INTO integration_responses (id, api_id, integration_id, response_key, template_selection)
		 VALUES (?, ?, ?, ?, ?)`,
		ir.ID, ir.APIID, ir.IntegrationID, ir.ResponseKey, ir.TemplateSelection,
	)
	return err
}

func (s *Store) GetIntegrationResponse(apiID, integrationID, responseID string) (*IntegrationResponse, error) {
	row := s.store.DB().QueryRow(
		`SELECT id, api_id, integration_id, response_key, template_selection
		 FROM integration_responses WHERE id = ? AND api_id = ? AND integration_id = ?`,
		responseID, apiID, integrationID)
	return scanIntegrationResponse(row)
}

func (s *Store) ListIntegrationResponses(apiID, integrationID string) ([]IntegrationResponse, error) {
	rows, err := s.store.DB().Query(
		`SELECT id, api_id, integration_id, response_key, template_selection
		 FROM integration_responses WHERE api_id = ? AND integration_id = ?`, apiID, integrationID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []IntegrationResponse
	for rows.Next() {
		ir, err := scanIntegrationResponse(rows)
		if err != nil {
			return nil, err
		}
		items = append(items, *ir)
	}
	return items, rows.Err()
}

func (s *Store) UpdateIntegrationResponse(apiID, integrationID, responseID string, fields map[string]any) error {
	ir, err := s.GetIntegrationResponse(apiID, integrationID, responseID)
	if err != nil {
		return errIntegrationResponseNotFound
	}
	if v, ok := fields["integrationResponseKey"].(string); ok && v != "" {
		ir.ResponseKey = v
	}
	if v, ok := fields["templateSelectionExpression"].(string); ok {
		ir.TemplateSelection = v
	}
	res, err := s.store.DB().Exec(
		`UPDATE integration_responses SET response_key=?, template_selection=? WHERE id=? AND api_id=? AND integration_id=?`,
		ir.ResponseKey, ir.TemplateSelection, responseID, apiID, integrationID,
	)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errIntegrationResponseNotFound
	}
	return nil
}

func (s *Store) DeleteIntegrationResponse(apiID, integrationID, responseID string) (*IntegrationResponse, error) {
	ir, err := s.GetIntegrationResponse(apiID, integrationID, responseID)
	if err != nil {
		return nil, errIntegrationResponseNotFound
	}
	if _, err := s.store.DB().Exec(
		`DELETE FROM integration_responses WHERE id = ? AND api_id = ? AND integration_id = ?`,
		responseID, apiID, integrationID); err != nil {
		return nil, err
	}
	return ir, nil
}

// --- Authorizers ---

func (s *Store) CreateAuthorizer(a *Authorizer) error {
	_, err := s.store.DB().Exec(
		`INSERT INTO authorizers (id, api_id, name, type, identity_source, jwt_config)
		 VALUES (?, ?, ?, ?, ?, ?)`,
		a.ID, a.APIID, a.Name, a.Type, a.IdentitySource, a.JWTConfig,
	)
	return err
}

func (s *Store) GetAuthorizer(apiID, authorizerID string) (*Authorizer, error) {
	row := s.store.DB().QueryRow(
		`SELECT id, api_id, name, type, identity_source, jwt_config
		 FROM authorizers WHERE id = ? AND api_id = ?`, authorizerID, apiID)
	return scanAuthorizer(row)
}

func (s *Store) ListAuthorizers(apiID string) ([]Authorizer, error) {
	rows, err := s.store.DB().Query(
		`SELECT id, api_id, name, type, identity_source, jwt_config
		 FROM authorizers WHERE api_id = ?`, apiID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []Authorizer
	for rows.Next() {
		a, err := scanAuthorizer(rows)
		if err != nil {
			return nil, err
		}
		items = append(items, *a)
	}
	return items, rows.Err()
}

func (s *Store) UpdateAuthorizer(apiID, authorizerID string, fields map[string]any) error {
	a, err := s.GetAuthorizer(apiID, authorizerID)
	if err != nil {
		return errAuthorizerNotFound
	}
	if v, ok := fields["name"].(string); ok && v != "" {
		a.Name = v
	}
	if v, ok := fields["authorizerType"].(string); ok && v != "" {
		a.Type = v
	}
	if v, ok := fields["identitySource"].(string); ok {
		a.IdentitySource = v
	}
	res, err := s.store.DB().Exec(
		`UPDATE authorizers SET name=?, type=?, identity_source=? WHERE id=? AND api_id=?`,
		a.Name, a.Type, a.IdentitySource, authorizerID, apiID,
	)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errAuthorizerNotFound
	}
	return nil
}

func (s *Store) DeleteAuthorizer(apiID, authorizerID string) (*Authorizer, error) {
	a, err := s.GetAuthorizer(apiID, authorizerID)
	if err != nil {
		return nil, errAuthorizerNotFound
	}
	if _, err := s.store.DB().Exec(`DELETE FROM authorizers WHERE id = ? AND api_id = ?`, authorizerID, apiID); err != nil {
		return nil, err
	}
	return a, nil
}

// --- Deployments ---

func (s *Store) CreateDeployment(d *Deployment) error {
	now := time.Now().Unix()
	_, err := s.store.DB().Exec(
		`INSERT INTO deployments (id, api_id, description, auto_deployed, created_at)
		 VALUES (?, ?, ?, ?, ?)`,
		d.ID, d.APIID, d.Description, boolInt(d.AutoDeployed), now,
	)
	return err
}

func (s *Store) GetDeployment(apiID, deploymentID string) (*Deployment, error) {
	row := s.store.DB().QueryRow(
		`SELECT id, api_id, description, auto_deployed, created_at
		 FROM deployments WHERE id = ? AND api_id = ?`, deploymentID, apiID)
	return scanDeployment(row)
}

func (s *Store) ListDeployments(apiID string) ([]Deployment, error) {
	rows, err := s.store.DB().Query(
		`SELECT id, api_id, description, auto_deployed, created_at
		 FROM deployments WHERE api_id = ? ORDER BY created_at`, apiID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []Deployment
	for rows.Next() {
		d, err := scanDeployment(rows)
		if err != nil {
			return nil, err
		}
		items = append(items, *d)
	}
	return items, rows.Err()
}

func (s *Store) UpdateDeployment(apiID, deploymentID string, fields map[string]any) error {
	d, err := s.GetDeployment(apiID, deploymentID)
	if err != nil {
		return errDeploymentNotFound
	}
	if v, ok := fields["description"].(string); ok {
		d.Description = v
	}
	res, err := s.store.DB().Exec(
		`UPDATE deployments SET description=? WHERE id=? AND api_id=?`,
		d.Description, deploymentID, apiID,
	)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errDeploymentNotFound
	}
	return nil
}

func (s *Store) DeleteDeployment(apiID, deploymentID string) (*Deployment, error) {
	d, err := s.GetDeployment(apiID, deploymentID)
	if err != nil {
		return nil, errDeploymentNotFound
	}
	if _, err := s.store.DB().Exec(`DELETE FROM deployments WHERE id = ? AND api_id = ?`, deploymentID, apiID); err != nil {
		return nil, err
	}
	return d, nil
}

// --- Stages ---

func (s *Store) CreateStage(st *Stage) error {
	now := time.Now().Unix()
	_, err := s.store.DB().Exec(
		`INSERT INTO stages (name, api_id, description, deployment_id, auto_deploy, stage_variables, access_log, created_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		st.Name, st.APIID, st.Description, st.DeploymentID, boolInt(st.AutoDeploy), st.StageVariables, st.AccessLog, now,
	)
	return err
}

func (s *Store) GetStage(apiID, name string) (*Stage, error) {
	row := s.store.DB().QueryRow(
		`SELECT name, api_id, description, deployment_id, auto_deploy, stage_variables, access_log, created_at
		 FROM stages WHERE name = ? AND api_id = ?`, name, apiID)
	return scanStage(row)
}

func (s *Store) ListStages(apiID string) ([]Stage, error) {
	rows, err := s.store.DB().Query(
		`SELECT name, api_id, description, deployment_id, auto_deploy, stage_variables, access_log, created_at
		 FROM stages WHERE api_id = ? ORDER BY created_at`, apiID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []Stage
	for rows.Next() {
		st, err := scanStage(rows)
		if err != nil {
			return nil, err
		}
		items = append(items, *st)
	}
	return items, rows.Err()
}

func (s *Store) UpdateStage(apiID, name string, fields map[string]any) error {
	st, err := s.GetStage(apiID, name)
	if err != nil {
		return errStageNotFound
	}
	if v, ok := fields["description"].(string); ok {
		st.Description = v
	}
	if v, ok := fields["deploymentId"].(string); ok {
		st.DeploymentID = v
	}
	if v, ok := fields["autoDeploy"].(bool); ok {
		st.AutoDeploy = v
	}
	res, err := s.store.DB().Exec(
		`UPDATE stages SET description=?, deployment_id=?, auto_deploy=? WHERE name=? AND api_id=?`,
		st.Description, st.DeploymentID, boolInt(st.AutoDeploy), name, apiID,
	)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errStageNotFound
	}
	return nil
}

func (s *Store) DeleteStage(apiID, name string) (*Stage, error) {
	st, err := s.GetStage(apiID, name)
	if err != nil {
		return nil, errStageNotFound
	}
	if _, err := s.store.DB().Exec(`DELETE FROM stages WHERE name = ? AND api_id = ?`, name, apiID); err != nil {
		return nil, err
	}
	return st, nil
}

// --- Models ---

func (s *Store) CreateModel(m *Model) error {
	_, err := s.store.DB().Exec(
		`INSERT INTO models (id, api_id, name, content_type, schema_def)
		 VALUES (?, ?, ?, ?, ?)`,
		m.ID, m.APIID, m.Name, m.ContentType, m.SchemaDef,
	)
	return err
}

func (s *Store) GetModel(apiID, modelID string) (*Model, error) {
	row := s.store.DB().QueryRow(
		`SELECT id, api_id, name, content_type, schema_def
		 FROM models WHERE id = ? AND api_id = ?`, modelID, apiID)
	return scanModel(row)
}

func (s *Store) ListModels(apiID string) ([]Model, error) {
	rows, err := s.store.DB().Query(
		`SELECT id, api_id, name, content_type, schema_def
		 FROM models WHERE api_id = ?`, apiID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []Model
	for rows.Next() {
		m, err := scanModel(rows)
		if err != nil {
			return nil, err
		}
		items = append(items, *m)
	}
	return items, rows.Err()
}

func (s *Store) UpdateModel(apiID, modelID string, fields map[string]any) error {
	m, err := s.GetModel(apiID, modelID)
	if err != nil {
		return errModelNotFound
	}
	if v, ok := fields["name"].(string); ok && v != "" {
		m.Name = v
	}
	if v, ok := fields["contentType"].(string); ok {
		m.ContentType = v
	}
	if v, ok := fields["schema"].(string); ok {
		m.SchemaDef = v
	}
	res, err := s.store.DB().Exec(
		`UPDATE models SET name=?, content_type=?, schema_def=? WHERE id=? AND api_id=?`,
		m.Name, m.ContentType, m.SchemaDef, modelID, apiID,
	)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errModelNotFound
	}
	return nil
}

func (s *Store) DeleteModel(apiID, modelID string) (*Model, error) {
	m, err := s.GetModel(apiID, modelID)
	if err != nil {
		return nil, errModelNotFound
	}
	if _, err := s.store.DB().Exec(`DELETE FROM models WHERE id = ? AND api_id = ?`, modelID, apiID); err != nil {
		return nil, err
	}
	return m, nil
}

// --- Domain Names ---

func (s *Store) CreateDomainName(d *DomainName) error {
	now := time.Now().Unix()
	_, err := s.store.DB().Exec(
		`INSERT INTO domain_names (name, config, mutual_tls, created_at)
		 VALUES (?, ?, ?, ?)`,
		d.Name, d.Config, d.MutualTLS, now,
	)
	return err
}

func (s *Store) GetDomainName(name string) (*DomainName, error) {
	row := s.store.DB().QueryRow(
		`SELECT name, config, mutual_tls, created_at FROM domain_names WHERE name = ?`, name)
	return scanDomainName(row)
}

func (s *Store) ListDomainNames() ([]DomainName, error) {
	rows, err := s.store.DB().Query(
		`SELECT name, config, mutual_tls, created_at FROM domain_names ORDER BY created_at`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []DomainName
	for rows.Next() {
		d, err := scanDomainName(rows)
		if err != nil {
			return nil, err
		}
		items = append(items, *d)
	}
	return items, rows.Err()
}

func (s *Store) UpdateDomainName(name string, fields map[string]any) error {
	d, err := s.GetDomainName(name)
	if err != nil {
		return errDomainNameNotFound
	}
	if v, ok := fields["domainNameConfigurations"].([]any); ok {
		if b, err2 := json.Marshal(v); err2 == nil {
			d.Config = string(b)
		}
	}
	res, err := s.store.DB().Exec(
		`UPDATE domain_names SET config=? WHERE name=?`,
		d.Config, name,
	)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errDomainNameNotFound
	}
	return nil
}

func (s *Store) DeleteDomainName(name string) (*DomainName, error) {
	d, err := s.GetDomainName(name)
	if err != nil {
		return nil, errDomainNameNotFound
	}
	if _, err := s.store.DB().Exec(`DELETE FROM domain_names WHERE name = ?`, name); err != nil {
		return nil, err
	}
	return d, nil
}

// --- API Mappings ---

func (s *Store) CreateAPIMapping(m *APIMapping) error {
	_, err := s.store.DB().Exec(
		`INSERT INTO api_mappings (id, api_id, domain_name, stage, api_mapping_key)
		 VALUES (?, ?, ?, ?, ?)`,
		m.ID, m.APIID, m.DomainName, m.Stage, m.APIMappingKey,
	)
	return err
}

func (s *Store) GetAPIMapping(domainName, mappingID string) (*APIMapping, error) {
	row := s.store.DB().QueryRow(
		`SELECT id, api_id, domain_name, stage, api_mapping_key
		 FROM api_mappings WHERE id = ? AND domain_name = ?`, mappingID, domainName)
	return scanAPIMapping(row)
}

func (s *Store) ListAPIMappings(domainName string) ([]APIMapping, error) {
	rows, err := s.store.DB().Query(
		`SELECT id, api_id, domain_name, stage, api_mapping_key
		 FROM api_mappings WHERE domain_name = ?`, domainName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []APIMapping
	for rows.Next() {
		m, err := scanAPIMapping(rows)
		if err != nil {
			return nil, err
		}
		items = append(items, *m)
	}
	return items, rows.Err()
}

func (s *Store) UpdateAPIMapping(domainName, mappingID string, fields map[string]any) error {
	m, err := s.GetAPIMapping(domainName, mappingID)
	if err != nil {
		return errAPIMappingNotFound
	}
	if v, ok := fields["apiId"].(string); ok && v != "" {
		m.APIID = v
	}
	if v, ok := fields["stage"].(string); ok {
		m.Stage = v
	}
	if v, ok := fields["apiMappingKey"].(string); ok {
		m.APIMappingKey = v
	}
	res, err := s.store.DB().Exec(
		`UPDATE api_mappings SET api_id=?, stage=?, api_mapping_key=? WHERE id=? AND domain_name=?`,
		m.APIID, m.Stage, m.APIMappingKey, mappingID, domainName,
	)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errAPIMappingNotFound
	}
	return nil
}

func (s *Store) DeleteAPIMapping(domainName, mappingID string) (*APIMapping, error) {
	m, err := s.GetAPIMapping(domainName, mappingID)
	if err != nil {
		return nil, errAPIMappingNotFound
	}
	if _, err := s.store.DB().Exec(`DELETE FROM api_mappings WHERE id = ? AND domain_name = ?`, mappingID, domainName); err != nil {
		return nil, err
	}
	return m, nil
}

// --- VPC Links ---

func (s *Store) CreateVpcLink(v *VpcLink) error {
	now := time.Now().Unix()
	_, err := s.store.DB().Exec(
		`INSERT INTO vpc_links (id, name, security_groups, subnet_ids, status, created_at)
		 VALUES (?, ?, ?, ?, ?, ?)`,
		v.ID, v.Name, v.SecurityGroups, v.SubnetIDs, v.Status, now,
	)
	return err
}

func (s *Store) GetVpcLink(id string) (*VpcLink, error) {
	row := s.store.DB().QueryRow(
		`SELECT id, name, security_groups, subnet_ids, status, created_at
		 FROM vpc_links WHERE id = ?`, id)
	return scanVpcLink(row)
}

func (s *Store) ListVpcLinks() ([]VpcLink, error) {
	rows, err := s.store.DB().Query(
		`SELECT id, name, security_groups, subnet_ids, status, created_at
		 FROM vpc_links ORDER BY created_at`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []VpcLink
	for rows.Next() {
		v, err := scanVpcLink(rows)
		if err != nil {
			return nil, err
		}
		items = append(items, *v)
	}
	return items, rows.Err()
}

func (s *Store) UpdateVpcLink(id string, fields map[string]any) error {
	v, err := s.GetVpcLink(id)
	if err != nil {
		return errVpcLinkNotFound
	}
	if val, ok := fields["name"].(string); ok && val != "" {
		v.Name = val
	}
	res, err := s.store.DB().Exec(
		`UPDATE vpc_links SET name=? WHERE id=?`,
		v.Name, id,
	)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errVpcLinkNotFound
	}
	return nil
}

func (s *Store) DeleteVpcLink(id string) (*VpcLink, error) {
	v, err := s.GetVpcLink(id)
	if err != nil {
		return nil, errVpcLinkNotFound
	}
	if _, err := s.store.DB().Exec(`DELETE FROM vpc_links WHERE id = ?`, id); err != nil {
		return nil, err
	}
	return v, nil
}

// --- Route Responses ---

func (s *Store) CreateRouteResponse(rr *RouteResponse) error {
	_, err := s.store.DB().Exec(
		`INSERT INTO route_responses (id, api_id, route_id, route_response_key, model_selection_expr)
		 VALUES (?, ?, ?, ?, ?)`,
		rr.ID, rr.APIID, rr.RouteID, rr.RouteResponseKey, rr.ModelSelectionExpr,
	)
	return err
}

func (s *Store) GetRouteResponse(apiID, routeID, responseID string) (*RouteResponse, error) {
	row := s.store.DB().QueryRow(
		`SELECT id, api_id, route_id, route_response_key, model_selection_expr
		 FROM route_responses WHERE id = ? AND api_id = ? AND route_id = ?`,
		responseID, apiID, routeID)
	return scanRouteResponse(row)
}

func (s *Store) ListRouteResponses(apiID, routeID string) ([]RouteResponse, error) {
	rows, err := s.store.DB().Query(
		`SELECT id, api_id, route_id, route_response_key, model_selection_expr
		 FROM route_responses WHERE api_id = ? AND route_id = ?`, apiID, routeID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []RouteResponse
	for rows.Next() {
		rr, err := scanRouteResponse(rows)
		if err != nil {
			return nil, err
		}
		items = append(items, *rr)
	}
	return items, rows.Err()
}

func (s *Store) DeleteRouteResponse(apiID, routeID, responseID string) (*RouteResponse, error) {
	rr, err := s.GetRouteResponse(apiID, routeID, responseID)
	if err != nil {
		return nil, errRouteResponseNotFound
	}
	if _, err := s.store.DB().Exec(
		`DELETE FROM route_responses WHERE id = ? AND api_id = ? AND route_id = ?`,
		responseID, apiID, routeID); err != nil {
		return nil, err
	}
	return rr, nil
}

// --- Scan helpers ---

type scanner interface{ Scan(dest ...any) error }

func scanAPI(sc scanner) (*API, error) {
	var a API
	var createdAt int64
	err := sc.Scan(&a.ID, &a.ARN, &a.Name, &a.ProtocolType, &a.Description, &a.RouteSelection, &a.APIEndpoint, &createdAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errAPINotFound
		}
		return nil, err
	}
	a.CreatedAt = time.Unix(createdAt, 0)
	return &a, nil
}

func scanRoute(sc scanner) (*Route, error) {
	var r Route
	err := sc.Scan(&r.ID, &r.APIID, &r.RouteKey, &r.Target, &r.AuthorizationType, &r.AuthorizerID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errRouteNotFound
		}
		return nil, err
	}
	return &r, nil
}

func scanIntegration(sc scanner) (*Integration, error) {
	var i Integration
	err := sc.Scan(&i.ID, &i.APIID, &i.Type, &i.IntegrationURI, &i.IntegrationMethod, &i.PayloadFormat)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errIntegrationNotFound
		}
		return nil, err
	}
	return &i, nil
}

func scanIntegrationResponse(sc scanner) (*IntegrationResponse, error) {
	var ir IntegrationResponse
	err := sc.Scan(&ir.ID, &ir.APIID, &ir.IntegrationID, &ir.ResponseKey, &ir.TemplateSelection)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errIntegrationResponseNotFound
		}
		return nil, err
	}
	return &ir, nil
}

func scanAuthorizer(sc scanner) (*Authorizer, error) {
	var a Authorizer
	err := sc.Scan(&a.ID, &a.APIID, &a.Name, &a.Type, &a.IdentitySource, &a.JWTConfig)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errAuthorizerNotFound
		}
		return nil, err
	}
	return &a, nil
}

func scanDeployment(sc scanner) (*Deployment, error) {
	var d Deployment
	var createdAt int64
	var autoDeployed int
	err := sc.Scan(&d.ID, &d.APIID, &d.Description, &autoDeployed, &createdAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errDeploymentNotFound
		}
		return nil, err
	}
	d.AutoDeployed = autoDeployed != 0
	d.CreatedAt = time.Unix(createdAt, 0)
	return &d, nil
}

func scanStage(sc scanner) (*Stage, error) {
	var st Stage
	var createdAt int64
	var autoDeploy int
	err := sc.Scan(&st.Name, &st.APIID, &st.Description, &st.DeploymentID, &autoDeploy, &st.StageVariables, &st.AccessLog, &createdAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errStageNotFound
		}
		return nil, err
	}
	st.AutoDeploy = autoDeploy != 0
	st.CreatedAt = time.Unix(createdAt, 0)
	return &st, nil
}

func scanModel(sc scanner) (*Model, error) {
	var m Model
	err := sc.Scan(&m.ID, &m.APIID, &m.Name, &m.ContentType, &m.SchemaDef)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errModelNotFound
		}
		return nil, err
	}
	return &m, nil
}

func scanDomainName(sc scanner) (*DomainName, error) {
	var d DomainName
	var createdAt int64
	err := sc.Scan(&d.Name, &d.Config, &d.MutualTLS, &createdAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errDomainNameNotFound
		}
		return nil, err
	}
	d.CreatedAt = time.Unix(createdAt, 0)
	return &d, nil
}

func scanAPIMapping(sc scanner) (*APIMapping, error) {
	var m APIMapping
	err := sc.Scan(&m.ID, &m.APIID, &m.DomainName, &m.Stage, &m.APIMappingKey)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errAPIMappingNotFound
		}
		return nil, err
	}
	return &m, nil
}

func scanVpcLink(sc scanner) (*VpcLink, error) {
	var v VpcLink
	var createdAt int64
	err := sc.Scan(&v.ID, &v.Name, &v.SecurityGroups, &v.SubnetIDs, &v.Status, &createdAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errVpcLinkNotFound
		}
		return nil, err
	}
	v.CreatedAt = time.Unix(createdAt, 0)
	return &v, nil
}

func scanRouteResponse(sc scanner) (*RouteResponse, error) {
	var rr RouteResponse
	err := sc.Scan(&rr.ID, &rr.APIID, &rr.RouteID, &rr.RouteResponseKey, &rr.ModelSelectionExpr)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errRouteResponseNotFound
		}
		return nil, err
	}
	return &rr, nil
}

// --- Helpers ---

func boolInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

func isUniqueErr(err error) bool {
	return sqlite.IsUniqueConstraintError(err)
}
