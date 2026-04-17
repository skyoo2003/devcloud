// SPDX-License-Identifier: Apache-2.0

// internal/services/appsync/store.go
package appsync

import (
	"database/sql"
	"errors"
	"path/filepath"
	"time"

	"github.com/skyoo2003/devcloud/internal/shared"
	"github.com/skyoo2003/devcloud/internal/storage/sqlite"
)

var (
	errGraphqlApiNotFound = errors.New("graphql api not found")
	errDataSourceNotFound = errors.New("data source not found")
	errResolverNotFound   = errors.New("resolver not found")
	errFunctionNotFound   = errors.New("function not found")
	errApiKeyNotFound     = errors.New("api key not found")
	errTypeNotFound       = errors.New("type not found")
)

var migrations = []sqlite.Migration{
	{Version: 1, SQL: `
		CREATE TABLE IF NOT EXISTS graphql_apis (
			id         TEXT PRIMARY KEY,
			arn        TEXT NOT NULL UNIQUE,
			name       TEXT NOT NULL,
			auth_type  TEXT NOT NULL DEFAULT 'API_KEY',
			log_config TEXT NOT NULL DEFAULT '{}',
			uris       TEXT NOT NULL DEFAULT '{}',
			created_at INTEGER NOT NULL
		);
		CREATE TABLE IF NOT EXISTS data_sources (
			api_id       TEXT NOT NULL,
			name         TEXT NOT NULL,
			arn          TEXT NOT NULL UNIQUE,
			type         TEXT NOT NULL DEFAULT 'NONE',
			config       TEXT NOT NULL DEFAULT '{}',
			service_role TEXT NOT NULL DEFAULT '',
			PRIMARY KEY (api_id, name)
		);
		CREATE TABLE IF NOT EXISTS resolvers (
			api_id            TEXT NOT NULL,
			type_name         TEXT NOT NULL,
			field_name        TEXT NOT NULL,
			arn               TEXT NOT NULL UNIQUE,
			data_source       TEXT NOT NULL DEFAULT '',
			request_template  TEXT NOT NULL DEFAULT '',
			response_template TEXT NOT NULL DEFAULT '',
			kind              TEXT NOT NULL DEFAULT 'UNIT',
			PRIMARY KEY (api_id, type_name, field_name)
		);
		CREATE TABLE IF NOT EXISTS functions (
			api_id            TEXT NOT NULL,
			id                TEXT NOT NULL,
			arn               TEXT NOT NULL UNIQUE,
			name              TEXT NOT NULL,
			data_source       TEXT NOT NULL DEFAULT '',
			request_template  TEXT NOT NULL DEFAULT '',
			response_template TEXT NOT NULL DEFAULT '',
			PRIMARY KEY (api_id, id)
		);
		CREATE TABLE IF NOT EXISTS api_keys (
			api_id      TEXT NOT NULL,
			id          TEXT NOT NULL,
			expires     INTEGER NOT NULL DEFAULT 0,
			description TEXT NOT NULL DEFAULT '',
			PRIMARY KEY (api_id, id)
		);
		CREATE TABLE IF NOT EXISTS types (
			api_id     TEXT NOT NULL,
			name       TEXT NOT NULL,
			definition TEXT NOT NULL DEFAULT '',
			format     TEXT NOT NULL DEFAULT 'SDL',
			PRIMARY KEY (api_id, name)
		);
	`},
}

// --- Model types ---

type GraphqlApi struct {
	ID        string
	ARN       string
	Name      string
	AuthType  string
	LogConfig string
	Uris      string
	CreatedAt time.Time
}

type DataSource struct {
	ApiID       string
	Name        string
	ARN         string
	Type        string
	Config      string
	ServiceRole string
}

type Resolver struct {
	ApiID            string
	TypeName         string
	FieldName        string
	ARN              string
	DataSource       string
	RequestTemplate  string
	ResponseTemplate string
	Kind             string
}

type Function struct {
	ApiID            string
	ID               string
	ARN              string
	Name             string
	DataSource       string
	RequestTemplate  string
	ResponseTemplate string
}

type ApiKey struct {
	ApiID       string
	ID          string
	Expires     int64
	Description string
}

type Type struct {
	ApiID      string
	Name       string
	Definition string
	Format     string
}

// --- Store ---

type Store struct {
	store *sqlite.Store
	tags  *shared.TagStore
}

func NewStore(dataDir string) (*Store, error) {
	allMigrations := append(migrations, shared.TagMigrations...)
	dbPath := filepath.Join(dataDir, "appsync.db")
	s, err := sqlite.Open(dbPath, allMigrations)
	if err != nil {
		return nil, err
	}
	return &Store{store: s, tags: shared.NewTagStore(s)}, nil
}

func (s *Store) Close() error { return s.store.Close() }

// --- GraphqlApi ---

func (s *Store) CreateGraphqlApi(a *GraphqlApi) error {
	now := time.Now().Unix()
	_, err := s.store.DB().Exec(
		`INSERT INTO graphql_apis (id, arn, name, auth_type, log_config, uris, created_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?)`,
		a.ID, a.ARN, a.Name, a.AuthType, a.LogConfig, a.Uris, now,
	)
	return err
}

func (s *Store) GetGraphqlApi(id string) (*GraphqlApi, error) {
	row := s.store.DB().QueryRow(
		`SELECT id, arn, name, auth_type, log_config, uris, created_at FROM graphql_apis WHERE id = ?`, id)
	return scanGraphqlApi(row)
}

func (s *Store) ListGraphqlApis() ([]GraphqlApi, error) {
	rows, err := s.store.DB().Query(
		`SELECT id, arn, name, auth_type, log_config, uris, created_at FROM graphql_apis ORDER BY created_at`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var apis []GraphqlApi
	for rows.Next() {
		a, err := scanGraphqlApi(rows)
		if err != nil {
			return nil, err
		}
		apis = append(apis, *a)
	}
	return apis, rows.Err()
}

func (s *Store) UpdateGraphqlApi(id string, fields map[string]any) error {
	a, err := s.GetGraphqlApi(id)
	if err != nil {
		return errGraphqlApiNotFound
	}
	if v, ok := fields["name"].(string); ok && v != "" {
		a.Name = v
	}
	if v, ok := fields["authenticationType"].(string); ok && v != "" {
		a.AuthType = v
	}
	res, err := s.store.DB().Exec(
		`UPDATE graphql_apis SET name=?, auth_type=? WHERE id=?`,
		a.Name, a.AuthType, id,
	)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errGraphqlApiNotFound
	}
	return nil
}

func (s *Store) DeleteGraphqlApi(id string) (*GraphqlApi, error) {
	a, err := s.GetGraphqlApi(id)
	if err != nil {
		return nil, errGraphqlApiNotFound
	}
	if _, err := s.store.DB().Exec(`DELETE FROM graphql_apis WHERE id = ?`, id); err != nil {
		return nil, err
	}
	return a, nil
}

// --- DataSource ---

func (s *Store) CreateDataSource(ds *DataSource) error {
	_, err := s.store.DB().Exec(
		`INSERT INTO data_sources (api_id, name, arn, type, config, service_role)
		 VALUES (?, ?, ?, ?, ?, ?)`,
		ds.ApiID, ds.Name, ds.ARN, ds.Type, ds.Config, ds.ServiceRole,
	)
	return err
}

func (s *Store) GetDataSource(apiID, name string) (*DataSource, error) {
	row := s.store.DB().QueryRow(
		`SELECT api_id, name, arn, type, config, service_role FROM data_sources WHERE api_id = ? AND name = ?`,
		apiID, name)
	return scanDataSource(row)
}

func (s *Store) ListDataSources(apiID string) ([]DataSource, error) {
	rows, err := s.store.DB().Query(
		`SELECT api_id, name, arn, type, config, service_role FROM data_sources WHERE api_id = ? ORDER BY name`,
		apiID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var dss []DataSource
	for rows.Next() {
		ds, err := scanDataSource(rows)
		if err != nil {
			return nil, err
		}
		dss = append(dss, *ds)
	}
	return dss, rows.Err()
}

func (s *Store) UpdateDataSource(apiID, name string, fields map[string]any) error {
	ds, err := s.GetDataSource(apiID, name)
	if err != nil {
		return errDataSourceNotFound
	}
	if v, ok := fields["type"].(string); ok && v != "" {
		ds.Type = v
	}
	if v, ok := fields["serviceRoleArn"].(string); ok {
		ds.ServiceRole = v
	}
	res, err := s.store.DB().Exec(
		`UPDATE data_sources SET type=?, service_role=? WHERE api_id=? AND name=?`,
		ds.Type, ds.ServiceRole, apiID, name,
	)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errDataSourceNotFound
	}
	return nil
}

func (s *Store) DeleteDataSource(apiID, name string) (*DataSource, error) {
	ds, err := s.GetDataSource(apiID, name)
	if err != nil {
		return nil, errDataSourceNotFound
	}
	if _, err := s.store.DB().Exec(`DELETE FROM data_sources WHERE api_id = ? AND name = ?`, apiID, name); err != nil {
		return nil, err
	}
	return ds, nil
}

// --- Resolver ---

func (s *Store) CreateResolver(r *Resolver) error {
	_, err := s.store.DB().Exec(
		`INSERT INTO resolvers (api_id, type_name, field_name, arn, data_source, request_template, response_template, kind)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		r.ApiID, r.TypeName, r.FieldName, r.ARN, r.DataSource, r.RequestTemplate, r.ResponseTemplate, r.Kind,
	)
	return err
}

func (s *Store) GetResolver(apiID, typeName, fieldName string) (*Resolver, error) {
	row := s.store.DB().QueryRow(
		`SELECT api_id, type_name, field_name, arn, data_source, request_template, response_template, kind
		 FROM resolvers WHERE api_id = ? AND type_name = ? AND field_name = ?`,
		apiID, typeName, fieldName)
	return scanResolver(row)
}

func (s *Store) ListResolvers(apiID, typeName string) ([]Resolver, error) {
	rows, err := s.store.DB().Query(
		`SELECT api_id, type_name, field_name, arn, data_source, request_template, response_template, kind
		 FROM resolvers WHERE api_id = ? AND type_name = ? ORDER BY field_name`,
		apiID, typeName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var resolvers []Resolver
	for rows.Next() {
		r, err := scanResolver(rows)
		if err != nil {
			return nil, err
		}
		resolvers = append(resolvers, *r)
	}
	return resolvers, rows.Err()
}

func (s *Store) ListResolversByFunction(apiID, functionID string) ([]Resolver, error) {
	// Find resolvers that reference this function via pipeline config (simplified: return all for api)
	rows, err := s.store.DB().Query(
		`SELECT api_id, type_name, field_name, arn, data_source, request_template, response_template, kind
		 FROM resolvers WHERE api_id = ? ORDER BY type_name, field_name`,
		apiID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var resolvers []Resolver
	for rows.Next() {
		r, err := scanResolver(rows)
		if err != nil {
			return nil, err
		}
		resolvers = append(resolvers, *r)
	}
	return resolvers, rows.Err()
}

func (s *Store) UpdateResolver(apiID, typeName, fieldName string, fields map[string]any) error {
	r, err := s.GetResolver(apiID, typeName, fieldName)
	if err != nil {
		return errResolverNotFound
	}
	if v, ok := fields["dataSourceName"].(string); ok {
		r.DataSource = v
	}
	if v, ok := fields["requestMappingTemplate"].(string); ok {
		r.RequestTemplate = v
	}
	if v, ok := fields["responseMappingTemplate"].(string); ok {
		r.ResponseTemplate = v
	}
	if v, ok := fields["kind"].(string); ok && v != "" {
		r.Kind = v
	}
	res, err := s.store.DB().Exec(
		`UPDATE resolvers SET data_source=?, request_template=?, response_template=?, kind=?
		 WHERE api_id=? AND type_name=? AND field_name=?`,
		r.DataSource, r.RequestTemplate, r.ResponseTemplate, r.Kind, apiID, typeName, fieldName,
	)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errResolverNotFound
	}
	return nil
}

func (s *Store) DeleteResolver(apiID, typeName, fieldName string) (*Resolver, error) {
	r, err := s.GetResolver(apiID, typeName, fieldName)
	if err != nil {
		return nil, errResolverNotFound
	}
	if _, err := s.store.DB().Exec(
		`DELETE FROM resolvers WHERE api_id = ? AND type_name = ? AND field_name = ?`,
		apiID, typeName, fieldName); err != nil {
		return nil, err
	}
	return r, nil
}

// --- Function ---

func (s *Store) CreateFunction(f *Function) error {
	_, err := s.store.DB().Exec(
		`INSERT INTO functions (api_id, id, arn, name, data_source, request_template, response_template)
		 VALUES (?, ?, ?, ?, ?, ?, ?)`,
		f.ApiID, f.ID, f.ARN, f.Name, f.DataSource, f.RequestTemplate, f.ResponseTemplate,
	)
	return err
}

func (s *Store) GetFunction(apiID, id string) (*Function, error) {
	row := s.store.DB().QueryRow(
		`SELECT api_id, id, arn, name, data_source, request_template, response_template
		 FROM functions WHERE api_id = ? AND id = ?`,
		apiID, id)
	return scanFunction(row)
}

func (s *Store) ListFunctions(apiID string) ([]Function, error) {
	rows, err := s.store.DB().Query(
		`SELECT api_id, id, arn, name, data_source, request_template, response_template
		 FROM functions WHERE api_id = ? ORDER BY name`,
		apiID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var fns []Function
	for rows.Next() {
		f, err := scanFunction(rows)
		if err != nil {
			return nil, err
		}
		fns = append(fns, *f)
	}
	return fns, rows.Err()
}

func (s *Store) UpdateFunction(apiID, id string, fields map[string]any) error {
	f, err := s.GetFunction(apiID, id)
	if err != nil {
		return errFunctionNotFound
	}
	if v, ok := fields["name"].(string); ok && v != "" {
		f.Name = v
	}
	if v, ok := fields["dataSourceName"].(string); ok {
		f.DataSource = v
	}
	if v, ok := fields["requestMappingTemplate"].(string); ok {
		f.RequestTemplate = v
	}
	if v, ok := fields["responseMappingTemplate"].(string); ok {
		f.ResponseTemplate = v
	}
	res, err := s.store.DB().Exec(
		`UPDATE functions SET name=?, data_source=?, request_template=?, response_template=?
		 WHERE api_id=? AND id=?`,
		f.Name, f.DataSource, f.RequestTemplate, f.ResponseTemplate, apiID, id,
	)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errFunctionNotFound
	}
	return nil
}

func (s *Store) DeleteFunction(apiID, id string) (*Function, error) {
	f, err := s.GetFunction(apiID, id)
	if err != nil {
		return nil, errFunctionNotFound
	}
	if _, err := s.store.DB().Exec(`DELETE FROM functions WHERE api_id = ? AND id = ?`, apiID, id); err != nil {
		return nil, err
	}
	return f, nil
}

// --- ApiKey ---

func (s *Store) CreateApiKey(k *ApiKey) error {
	_, err := s.store.DB().Exec(
		`INSERT INTO api_keys (api_id, id, expires, description) VALUES (?, ?, ?, ?)`,
		k.ApiID, k.ID, k.Expires, k.Description,
	)
	return err
}

func (s *Store) GetApiKey(apiID, id string) (*ApiKey, error) {
	row := s.store.DB().QueryRow(
		`SELECT api_id, id, expires, description FROM api_keys WHERE api_id = ? AND id = ?`,
		apiID, id)
	return scanApiKey(row)
}

func (s *Store) ListApiKeys(apiID string) ([]ApiKey, error) {
	rows, err := s.store.DB().Query(
		`SELECT api_id, id, expires, description FROM api_keys WHERE api_id = ? ORDER BY id`,
		apiID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var keys []ApiKey
	for rows.Next() {
		k, err := scanApiKey(rows)
		if err != nil {
			return nil, err
		}
		keys = append(keys, *k)
	}
	return keys, rows.Err()
}

func (s *Store) UpdateApiKey(apiID, id string, fields map[string]any) error {
	k, err := s.GetApiKey(apiID, id)
	if err != nil {
		return errApiKeyNotFound
	}
	if v, ok := fields["description"].(string); ok {
		k.Description = v
	}
	if v, ok := fields["expires"].(float64); ok {
		k.Expires = int64(v)
	}
	res, err := s.store.DB().Exec(
		`UPDATE api_keys SET expires=?, description=? WHERE api_id=? AND id=?`,
		k.Expires, k.Description, apiID, id,
	)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errApiKeyNotFound
	}
	return nil
}

func (s *Store) DeleteApiKey(apiID, id string) (*ApiKey, error) {
	k, err := s.GetApiKey(apiID, id)
	if err != nil {
		return nil, errApiKeyNotFound
	}
	if _, err := s.store.DB().Exec(`DELETE FROM api_keys WHERE api_id = ? AND id = ?`, apiID, id); err != nil {
		return nil, err
	}
	return k, nil
}

// --- Type ---

func (s *Store) CreateType(tp *Type) error {
	_, err := s.store.DB().Exec(
		`INSERT INTO types (api_id, name, definition, format) VALUES (?, ?, ?, ?)`,
		tp.ApiID, tp.Name, tp.Definition, tp.Format,
	)
	return err
}

func (s *Store) GetType(apiID, name string) (*Type, error) {
	row := s.store.DB().QueryRow(
		`SELECT api_id, name, definition, format FROM types WHERE api_id = ? AND name = ?`,
		apiID, name)
	return scanType(row)
}

func (s *Store) ListTypes(apiID, format string) ([]Type, error) {
	rows, err := s.store.DB().Query(
		`SELECT api_id, name, definition, format FROM types WHERE api_id = ? ORDER BY name`,
		apiID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var types []Type
	for rows.Next() {
		tp, err := scanType(rows)
		if err != nil {
			return nil, err
		}
		types = append(types, *tp)
	}
	return types, rows.Err()
}

func (s *Store) UpdateType(apiID, name string, fields map[string]any) error {
	tp, err := s.GetType(apiID, name)
	if err != nil {
		return errTypeNotFound
	}
	if v, ok := fields["definition"].(string); ok {
		tp.Definition = v
	}
	if v, ok := fields["format"].(string); ok && v != "" {
		tp.Format = v
	}
	res, err := s.store.DB().Exec(
		`UPDATE types SET definition=?, format=? WHERE api_id=? AND name=?`,
		tp.Definition, tp.Format, apiID, name,
	)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errTypeNotFound
	}
	return nil
}

func (s *Store) DeleteType(apiID, name string) (*Type, error) {
	tp, err := s.GetType(apiID, name)
	if err != nil {
		return nil, errTypeNotFound
	}
	if _, err := s.store.DB().Exec(`DELETE FROM types WHERE api_id = ? AND name = ?`, apiID, name); err != nil {
		return nil, err
	}
	return tp, nil
}

// --- Scan helpers ---

type scanner interface{ Scan(dest ...any) error }

func scanGraphqlApi(sc scanner) (*GraphqlApi, error) {
	var a GraphqlApi
	var createdAt int64
	err := sc.Scan(&a.ID, &a.ARN, &a.Name, &a.AuthType, &a.LogConfig, &a.Uris, &createdAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errGraphqlApiNotFound
		}
		return nil, err
	}
	a.CreatedAt = time.Unix(createdAt, 0)
	return &a, nil
}

func scanDataSource(sc scanner) (*DataSource, error) {
	var ds DataSource
	err := sc.Scan(&ds.ApiID, &ds.Name, &ds.ARN, &ds.Type, &ds.Config, &ds.ServiceRole)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errDataSourceNotFound
		}
		return nil, err
	}
	return &ds, nil
}

func scanResolver(sc scanner) (*Resolver, error) {
	var r Resolver
	err := sc.Scan(&r.ApiID, &r.TypeName, &r.FieldName, &r.ARN, &r.DataSource, &r.RequestTemplate, &r.ResponseTemplate, &r.Kind)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errResolverNotFound
		}
		return nil, err
	}
	return &r, nil
}

func scanFunction(sc scanner) (*Function, error) {
	var f Function
	err := sc.Scan(&f.ApiID, &f.ID, &f.ARN, &f.Name, &f.DataSource, &f.RequestTemplate, &f.ResponseTemplate)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errFunctionNotFound
		}
		return nil, err
	}
	return &f, nil
}

func scanApiKey(sc scanner) (*ApiKey, error) {
	var k ApiKey
	err := sc.Scan(&k.ApiID, &k.ID, &k.Expires, &k.Description)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errApiKeyNotFound
		}
		return nil, err
	}
	return &k, nil
}

func scanType(sc scanner) (*Type, error) {
	var tp Type
	err := sc.Scan(&tp.ApiID, &tp.Name, &tp.Definition, &tp.Format)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errTypeNotFound
		}
		return nil, err
	}
	return &tp, nil
}

func isUniqueErr(err error) bool {
	return sqlite.IsUniqueConstraintError(err)
}
