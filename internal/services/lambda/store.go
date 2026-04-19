// SPDX-License-Identifier: Apache-2.0

package lambda

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/skyoo2003/devcloud/internal/storage/sqlite"
)

// Sentinel errors.
var (
	ErrFunctionAlreadyExists = errors.New("function already exists")
	ErrFunctionNotFound      = errors.New("function not found")
	ErrVersionNotFound       = errors.New("version not found")
	ErrAliasNotFound         = errors.New("alias not found")
	ErrAliasAlreadyExists    = errors.New("alias already exists")
	ErrPermissionNotFound    = errors.New("permission not found")
	ErrMappingNotFound       = errors.New("event source mapping not found")
)

// FunctionInfo holds metadata about a Lambda function.
type FunctionInfo struct {
	FunctionName string
	FunctionArn  string
	Runtime      string // e.g. "python3.12", "nodejs20.x"
	Handler      string // e.g. "index.handler"
	Role         string // IAM role ARN
	CodeSize     int64
	Description  string
	Timeout      int // seconds, default 3
	MemorySize   int // MB, default 128
	AccountID    string
	CodePath     string // filesystem path to code zip
	LastModified time.Time
}

var lambdaMigrations = []sqlite.Migration{
	{
		Version: 1,
		SQL: `CREATE TABLE IF NOT EXISTS functions (
			function_name TEXT NOT NULL,
			function_arn  TEXT NOT NULL,
			runtime       TEXT NOT NULL,
			handler       TEXT NOT NULL,
			role          TEXT NOT NULL DEFAULT '',
			code_size     INTEGER NOT NULL DEFAULT 0,
			description   TEXT NOT NULL DEFAULT '',
			timeout       INTEGER NOT NULL DEFAULT 3,
			memory_size   INTEGER NOT NULL DEFAULT 128,
			account_id    TEXT NOT NULL,
			code_path     TEXT NOT NULL DEFAULT '',
			last_modified DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
			PRIMARY KEY (function_name, account_id)
		);`,
	},
	{
		Version: 2,
		SQL: `CREATE TABLE IF NOT EXISTS function_versions (
			function_name TEXT NOT NULL,
			version       TEXT NOT NULL,
			account_id    TEXT NOT NULL,
			code_path     TEXT NOT NULL,
			config_json   TEXT NOT NULL,
			created_at    DATETIME NOT NULL,
			PRIMARY KEY (function_name, version, account_id)
		);`,
	},
	{
		Version: 3,
		SQL: `CREATE TABLE IF NOT EXISTS function_aliases (
			function_name    TEXT NOT NULL,
			alias_name       TEXT NOT NULL,
			function_version TEXT NOT NULL,
			account_id       TEXT NOT NULL,
			created_at       DATETIME NOT NULL,
			PRIMARY KEY (function_name, alias_name, account_id)
		);`,
	},
	{
		Version: 4,
		SQL: `CREATE TABLE IF NOT EXISTS function_permissions (
			function_name TEXT NOT NULL,
			statement_id  TEXT NOT NULL,
			action        TEXT NOT NULL,
			principal     TEXT NOT NULL,
			source_arn    TEXT,
			account_id    TEXT NOT NULL,
			PRIMARY KEY (function_name, statement_id, account_id)
		);`,
	},
	{
		Version: 5,
		SQL: `CREATE TABLE IF NOT EXISTS function_tags (
			function_arn TEXT NOT NULL,
			tag_key      TEXT NOT NULL,
			tag_value    TEXT NOT NULL,
			PRIMARY KEY (function_arn, tag_key)
		);`,
	},
	{
		Version: 6,
		SQL: `CREATE TABLE IF NOT EXISTS event_source_mappings (
			uuid             TEXT PRIMARY KEY,
			function_name    TEXT NOT NULL,
			event_source_arn TEXT NOT NULL,
			batch_size       INTEGER NOT NULL DEFAULT 10,
			enabled          INTEGER NOT NULL DEFAULT 1,
			account_id       TEXT NOT NULL,
			state            TEXT NOT NULL DEFAULT 'Enabled',
			created_at       DATETIME NOT NULL
		);`,
	},
}

// LambdaStore is a SQLite-backed store for Lambda function metadata,
// combined with a filesystem store for function code.
type LambdaStore struct {
	store   *sqlite.Store
	codeDir string
}

// NewLambdaStore opens (or creates) a SQLite database at dbPath and
// initializes the Lambda functions schema. Code zip files are stored
// under codeDir.
func NewLambdaStore(dbPath, codeDir string) (*LambdaStore, error) {
	store, err := sqlite.Open(dbPath, lambdaMigrations)
	if err != nil {
		return nil, err
	}
	return &LambdaStore{store: store, codeDir: codeDir}, nil
}

// Close closes the underlying database connection.
func (s *LambdaStore) Close() error {
	return s.store.Close()
}

// validatePathComponent ensures v is a single safe path component.
func validatePathComponent(name, v string) error {
	if v == "" {
		return fmt.Errorf("%s is required", name)
	}
	if v == "." || v == ".." {
		return fmt.Errorf("invalid %s: %q", name, v)
	}
	if strings.Contains(v, "/") || strings.Contains(v, "\\") {
		return fmt.Errorf("invalid %s: must not contain path separators", name)
	}
	return nil
}

// codePath returns the filesystem path for a function's code zip.
// It validates the result stays under codeDir to prevent path traversal.
func (s *LambdaStore) codePath(accountID, functionName string) (string, error) {
	if err := validatePathComponent("accountID", accountID); err != nil {
		return "", err
	}
	if err := validatePathComponent("functionName", functionName); err != nil {
		return "", err
	}

	joined := filepath.Join(s.codeDir, accountID, functionName, "code.zip")
	cleaned := filepath.Clean(joined)
	absBase, err := filepath.Abs(s.codeDir)
	if err != nil {
		return "", fmt.Errorf("resolve base code dir: %w", err)
	}
	absCleaned, err := filepath.Abs(cleaned)
	if err != nil {
		return "", fmt.Errorf("resolve code path: %w", err)
	}
	if !strings.HasPrefix(absCleaned, absBase+string(filepath.Separator)) {
		return "", fmt.Errorf("path traversal detected: %s", cleaned)
	}
	return cleaned, nil
}

// CreateFunction saves function metadata to SQLite and writes the code zip
// to the filesystem. It returns ErrFunctionAlreadyExists if the function
// already exists for the given account.
func (s *LambdaStore) CreateFunction(info *FunctionInfo, codeZip []byte) (*FunctionInfo, error) {
	path, err := s.codePath(info.AccountID, info.FunctionName)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, fmt.Errorf("create code directory: %w", err)
	}
	if err := os.WriteFile(path, codeZip, 0o644); err != nil {
		return nil, fmt.Errorf("write code zip: %w", err)
	}

	now := time.Now().UTC()
	codeSize := int64(len(codeZip))

	_, err = s.store.DB().Exec(
		`INSERT INTO functions
			(function_name, function_arn, runtime, handler, role, code_size,
			 description, timeout, memory_size, account_id, code_path, last_modified)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);`,
		info.FunctionName,
		info.FunctionArn,
		info.Runtime,
		info.Handler,
		info.Role,
		codeSize,
		info.Description,
		info.Timeout,
		info.MemorySize,
		info.AccountID,
		path,
		now,
	)
	if err != nil {
		if sqlite.IsUniqueConstraintError(err) {
			return nil, ErrFunctionAlreadyExists
		}
		return nil, err
	}

	result := *info
	result.CodePath = path
	result.CodeSize = codeSize
	result.LastModified = now
	return &result, nil
}

// GetFunction retrieves function metadata by accountID and functionName.
// Returns ErrFunctionNotFound if no such function exists.
func (s *LambdaStore) GetFunction(accountID, functionName string) (*FunctionInfo, error) {
	row := s.store.DB().QueryRow(
		`SELECT function_name, function_arn, runtime, handler, role, code_size,
		        description, timeout, memory_size, account_id, code_path, last_modified
		 FROM functions
		 WHERE account_id = ? AND function_name = ?;`,
		accountID, functionName,
	)

	var f FunctionInfo
	err := row.Scan(
		&f.FunctionName,
		&f.FunctionArn,
		&f.Runtime,
		&f.Handler,
		&f.Role,
		&f.CodeSize,
		&f.Description,
		&f.Timeout,
		&f.MemorySize,
		&f.AccountID,
		&f.CodePath,
		&f.LastModified,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrFunctionNotFound
		}
		return nil, err
	}
	return &f, nil
}

// ListFunctions returns all function metadata for the given account.
func (s *LambdaStore) ListFunctions(accountID string) ([]FunctionInfo, error) {
	rows, err := s.store.DB().Query(
		`SELECT function_name, function_arn, runtime, handler, role, code_size,
		        description, timeout, memory_size, account_id, code_path, last_modified
		 FROM functions
		 WHERE account_id = ?
		 ORDER BY function_name;`,
		accountID,
	)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	var functions []FunctionInfo
	for rows.Next() {
		var f FunctionInfo
		if err := rows.Scan(
			&f.FunctionName,
			&f.FunctionArn,
			&f.Runtime,
			&f.Handler,
			&f.Role,
			&f.CodeSize,
			&f.Description,
			&f.Timeout,
			&f.MemorySize,
			&f.AccountID,
			&f.CodePath,
			&f.LastModified,
		); err != nil {
			return nil, err
		}
		functions = append(functions, f)
	}
	return functions, rows.Err()
}

// DeleteFunction removes the function from SQLite and deletes its code
// directory from the filesystem. Returns ErrFunctionNotFound if the
// function does not exist.
func (s *LambdaStore) DeleteFunction(accountID, functionName string) error {
	result, err := s.store.DB().Exec(
		`DELETE FROM functions WHERE account_id = ? AND function_name = ?;`,
		accountID, functionName,
	)
	if err != nil {
		return err
	}
	n, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return ErrFunctionNotFound
	}

	// Remove the code directory (best-effort; ignore errors).
	codeDir := filepath.Join(s.codeDir, accountID, functionName)
	_ = os.RemoveAll(codeDir)

	return nil
}

// UpdateFunctionConfiguration updates the configuration fields of an existing function.
// Only non-zero values are applied. Returns ErrFunctionNotFound if the function does not exist.
func (s *LambdaStore) UpdateFunctionConfiguration(accountID, functionName, handler, runtime, role, description string, timeout, memorySize int) (*FunctionInfo, error) {
	existing, err := s.GetFunction(accountID, functionName)
	if err != nil {
		return nil, err
	}

	if handler != "" {
		existing.Handler = handler
	}
	if runtime != "" {
		existing.Runtime = runtime
	}
	if role != "" {
		existing.Role = role
	}
	if description != "" {
		existing.Description = description
	}
	if timeout > 0 {
		existing.Timeout = timeout
	}
	if memorySize > 0 {
		existing.MemorySize = memorySize
	}

	now := time.Now().UTC()
	existing.LastModified = now

	_, err = s.store.DB().Exec(
		`UPDATE functions SET handler = ?, runtime = ?, role = ?, description = ?, timeout = ?, memory_size = ?, last_modified = ?
		 WHERE account_id = ? AND function_name = ?;`,
		existing.Handler, existing.Runtime, existing.Role, existing.Description,
		existing.Timeout, existing.MemorySize, now,
		accountID, functionName,
	)
	if err != nil {
		return nil, err
	}

	return existing, nil
}

// UpdateFunctionCode replaces the code zip for an existing function,
// updating CodeSize and LastModified in SQLite. Returns ErrFunctionNotFound
// if the function does not exist.
func (s *LambdaStore) UpdateFunctionCode(accountID, functionName string, codeZip []byte) (*FunctionInfo, error) {
	// Ensure the function exists first.
	existing, err := s.GetFunction(accountID, functionName)
	if err != nil {
		return nil, err
	}

	path, err := s.codePath(accountID, functionName)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, fmt.Errorf("create code directory: %w", err)
	}
	if err := os.WriteFile(path, codeZip, 0o644); err != nil {
		return nil, fmt.Errorf("write code zip: %w", err)
	}

	now := time.Now().UTC()
	codeSize := int64(len(codeZip))

	_, err = s.store.DB().Exec(
		`UPDATE functions SET code_size = ?, code_path = ?, last_modified = ?
		 WHERE account_id = ? AND function_name = ?;`,
		codeSize, path, now, accountID, functionName,
	)
	if err != nil {
		return nil, err
	}

	existing.CodeSize = codeSize
	existing.CodePath = path
	existing.LastModified = now
	return existing, nil
}

// GetFunctionCode reads and returns the code zip bytes from the filesystem.
// Returns ErrFunctionNotFound if the function does not exist.
func (s *LambdaStore) GetFunctionCode(accountID, functionName string) ([]byte, error) {
	f, err := s.GetFunction(accountID, functionName)
	if err != nil {
		return nil, err
	}
	return os.ReadFile(f.CodePath)
}

// --- Versions ---

// FunctionVersion holds a published snapshot of a function.
type FunctionVersion struct {
	FunctionName string
	Version      string
	AccountID    string
	CodePath     string
	Config       map[string]any
	CreatedAt    time.Time
}

// PublishVersion snapshots the current function config+code as the next numbered version.
func (s *LambdaStore) PublishVersion(accountID, functionName string) (*FunctionVersion, error) {
	f, err := s.GetFunction(accountID, functionName)
	if err != nil {
		return nil, err
	}

	// Determine next version number.
	var maxVer int
	row := s.store.DB().QueryRow(
		`SELECT COALESCE(MAX(CAST(version AS INTEGER)), 0) FROM function_versions WHERE function_name = ? AND account_id = ?`,
		functionName, accountID,
	)
	if err := row.Scan(&maxVer); err != nil {
		return nil, err
	}
	nextVer := strconv.Itoa(maxVer + 1)

	configMap := map[string]any{
		"FunctionName": f.FunctionName,
		"FunctionArn":  f.FunctionArn,
		"Runtime":      f.Runtime,
		"Handler":      f.Handler,
		"Role":         f.Role,
		"CodeSize":     f.CodeSize,
		"Description":  f.Description,
		"Timeout":      f.Timeout,
		"MemorySize":   f.MemorySize,
	}
	configJSON, err := json.Marshal(configMap)
	if err != nil {
		return nil, err
	}

	now := time.Now().UTC()
	_, err = s.store.DB().Exec(
		`INSERT INTO function_versions (function_name, version, account_id, code_path, config_json, created_at) VALUES (?, ?, ?, ?, ?, ?)`,
		functionName, nextVer, accountID, f.CodePath, string(configJSON), now,
	)
	if err != nil {
		return nil, err
	}

	return &FunctionVersion{
		FunctionName: functionName,
		Version:      nextVer,
		AccountID:    accountID,
		CodePath:     f.CodePath,
		Config:       configMap,
		CreatedAt:    now,
	}, nil
}

// GetVersion retrieves a specific published version.
func (s *LambdaStore) GetVersion(accountID, functionName, version string) (*FunctionVersion, error) {
	row := s.store.DB().QueryRow(
		`SELECT function_name, version, account_id, code_path, config_json, created_at FROM function_versions WHERE function_name = ? AND version = ? AND account_id = ?`,
		functionName, version, accountID,
	)
	var v FunctionVersion
	var configJSON string
	err := row.Scan(&v.FunctionName, &v.Version, &v.AccountID, &v.CodePath, &configJSON, &v.CreatedAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrVersionNotFound
		}
		return nil, err
	}
	if err := json.Unmarshal([]byte(configJSON), &v.Config); err != nil {
		return nil, err
	}
	return &v, nil
}

// ListVersions returns all published versions for a function.
func (s *LambdaStore) ListVersions(accountID, functionName string) ([]FunctionVersion, error) {
	rows, err := s.store.DB().Query(
		`SELECT function_name, version, account_id, code_path, config_json, created_at FROM function_versions WHERE function_name = ? AND account_id = ? ORDER BY CAST(version AS INTEGER)`,
		functionName, accountID,
	)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	var versions []FunctionVersion
	for rows.Next() {
		var v FunctionVersion
		var configJSON string
		if err := rows.Scan(&v.FunctionName, &v.Version, &v.AccountID, &v.CodePath, &configJSON, &v.CreatedAt); err != nil {
			return nil, err
		}
		if err := json.Unmarshal([]byte(configJSON), &v.Config); err != nil {
			return nil, err
		}
		versions = append(versions, v)
	}
	return versions, rows.Err()
}

// --- Aliases ---

// FunctionAlias holds alias metadata.
type FunctionAlias struct {
	FunctionName    string
	AliasName       string
	FunctionVersion string
	AccountID       string
	CreatedAt       time.Time
}

// CreateAlias creates a new alias for a function.
func (s *LambdaStore) CreateAlias(accountID, functionName, aliasName, functionVersion string) (*FunctionAlias, error) {
	now := time.Now().UTC()
	_, err := s.store.DB().Exec(
		`INSERT INTO function_aliases (function_name, alias_name, function_version, account_id, created_at) VALUES (?, ?, ?, ?, ?)`,
		functionName, aliasName, functionVersion, accountID, now,
	)
	if err != nil {
		if sqlite.IsUniqueConstraintError(err) {
			return nil, ErrAliasAlreadyExists
		}
		return nil, err
	}
	return &FunctionAlias{
		FunctionName:    functionName,
		AliasName:       aliasName,
		FunctionVersion: functionVersion,
		AccountID:       accountID,
		CreatedAt:       now,
	}, nil
}

// GetAlias retrieves a specific alias.
func (s *LambdaStore) GetAlias(accountID, functionName, aliasName string) (*FunctionAlias, error) {
	row := s.store.DB().QueryRow(
		`SELECT function_name, alias_name, function_version, account_id, created_at FROM function_aliases WHERE function_name = ? AND alias_name = ? AND account_id = ?`,
		functionName, aliasName, accountID,
	)
	var a FunctionAlias
	err := row.Scan(&a.FunctionName, &a.AliasName, &a.FunctionVersion, &a.AccountID, &a.CreatedAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrAliasNotFound
		}
		return nil, err
	}
	return &a, nil
}

// UpdateAlias updates the target version of an alias.
func (s *LambdaStore) UpdateAlias(accountID, functionName, aliasName, functionVersion string) (*FunctionAlias, error) {
	result, err := s.store.DB().Exec(
		`UPDATE function_aliases SET function_version = ? WHERE function_name = ? AND alias_name = ? AND account_id = ?`,
		functionVersion, functionName, aliasName, accountID,
	)
	if err != nil {
		return nil, err
	}
	n, err := result.RowsAffected()
	if err != nil {
		return nil, err
	}
	if n == 0 {
		return nil, ErrAliasNotFound
	}
	return s.GetAlias(accountID, functionName, aliasName)
}

// DeleteAlias removes an alias.
func (s *LambdaStore) DeleteAlias(accountID, functionName, aliasName string) error {
	result, err := s.store.DB().Exec(
		`DELETE FROM function_aliases WHERE function_name = ? AND alias_name = ? AND account_id = ?`,
		functionName, aliasName, accountID,
	)
	if err != nil {
		return err
	}
	n, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return ErrAliasNotFound
	}
	return nil
}

// ListAliases returns all aliases for a function.
func (s *LambdaStore) ListAliases(accountID, functionName string) ([]FunctionAlias, error) {
	rows, err := s.store.DB().Query(
		`SELECT function_name, alias_name, function_version, account_id, created_at FROM function_aliases WHERE function_name = ? AND account_id = ? ORDER BY alias_name`,
		functionName, accountID,
	)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	var aliases []FunctionAlias
	for rows.Next() {
		var a FunctionAlias
		if err := rows.Scan(&a.FunctionName, &a.AliasName, &a.FunctionVersion, &a.AccountID, &a.CreatedAt); err != nil {
			return nil, err
		}
		aliases = append(aliases, a)
	}
	return aliases, rows.Err()
}

// --- Permissions ---

// FunctionPermission holds a resource-based policy statement.
type FunctionPermission struct {
	FunctionName string
	StatementID  string
	Action       string
	Principal    string
	SourceARN    string
	AccountID    string
}

// AddPermission adds a permission statement to a function's resource policy.
func (s *LambdaStore) AddPermission(p *FunctionPermission) error {
	_, err := s.store.DB().Exec(
		`INSERT INTO function_permissions (function_name, statement_id, action, principal, source_arn, account_id) VALUES (?, ?, ?, ?, ?, ?)`,
		p.FunctionName, p.StatementID, p.Action, p.Principal, p.SourceARN, p.AccountID,
	)
	return err
}

// RemovePermission deletes a permission statement by statementId.
func (s *LambdaStore) RemovePermission(accountID, functionName, statementID string) error {
	result, err := s.store.DB().Exec(
		`DELETE FROM function_permissions WHERE function_name = ? AND statement_id = ? AND account_id = ?`,
		functionName, statementID, accountID,
	)
	if err != nil {
		return err
	}
	n, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return ErrPermissionNotFound
	}
	return nil
}

// GetPermissions returns all permission statements for a function.
func (s *LambdaStore) GetPermissions(accountID, functionName string) ([]FunctionPermission, error) {
	rows, err := s.store.DB().Query(
		`SELECT function_name, statement_id, action, principal, COALESCE(source_arn, ''), account_id FROM function_permissions WHERE function_name = ? AND account_id = ?`,
		functionName, accountID,
	)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	var perms []FunctionPermission
	for rows.Next() {
		var p FunctionPermission
		if err := rows.Scan(&p.FunctionName, &p.StatementID, &p.Action, &p.Principal, &p.SourceARN, &p.AccountID); err != nil {
			return nil, err
		}
		perms = append(perms, p)
	}
	return perms, rows.Err()
}

// --- Tags ---

// TagResource sets tags on a function ARN.
func (s *LambdaStore) TagResource(functionARN string, tags map[string]string) error {
	for k, v := range tags {
		_, err := s.store.DB().Exec(
			`INSERT INTO function_tags (function_arn, tag_key, tag_value) VALUES (?, ?, ?) ON CONFLICT(function_arn, tag_key) DO UPDATE SET tag_value = excluded.tag_value`,
			functionARN, k, v,
		)
		if err != nil {
			return err
		}
	}
	return nil
}

// UntagResource removes specific tag keys from a function ARN.
func (s *LambdaStore) UntagResource(functionARN string, tagKeys []string) error {
	for _, k := range tagKeys {
		_, err := s.store.DB().Exec(
			`DELETE FROM function_tags WHERE function_arn = ? AND tag_key = ?`,
			functionARN, k,
		)
		if err != nil {
			return err
		}
	}
	return nil
}

// ListTags returns all tags for a function ARN.
func (s *LambdaStore) ListTags(functionARN string) (map[string]string, error) {
	rows, err := s.store.DB().Query(
		`SELECT tag_key, tag_value FROM function_tags WHERE function_arn = ?`,
		functionARN,
	)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	tags := make(map[string]string)
	for rows.Next() {
		var k, v string
		if err := rows.Scan(&k, &v); err != nil {
			return nil, err
		}
		tags[k] = v
	}
	return tags, rows.Err()
}

// --- Event Source Mappings ---

// EventSourceMapping holds a mapping between an event source and a Lambda function.
type EventSourceMapping struct {
	UUID           string
	FunctionName   string
	EventSourceARN string
	BatchSize      int
	Enabled        bool
	AccountID      string
	State          string
	CreatedAt      time.Time
}

// CreateEventSourceMapping creates a new event source mapping.
func (s *LambdaStore) CreateEventSourceMapping(m *EventSourceMapping) error {
	enabled := 1
	if !m.Enabled {
		enabled = 0
	}
	state := m.State
	if state == "" {
		state = "Enabled"
	}
	batchSize := m.BatchSize
	if batchSize <= 0 {
		batchSize = 10
	}
	now := time.Now().UTC()
	m.CreatedAt = now
	_, err := s.store.DB().Exec(
		`INSERT INTO event_source_mappings (uuid, function_name, event_source_arn, batch_size, enabled, account_id, state, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		m.UUID, m.FunctionName, m.EventSourceARN, batchSize, enabled, m.AccountID, state, now,
	)
	return err
}

// GetEventSourceMapping retrieves a mapping by UUID.
func (s *LambdaStore) GetEventSourceMapping(uuid string) (*EventSourceMapping, error) {
	row := s.store.DB().QueryRow(
		`SELECT uuid, function_name, event_source_arn, batch_size, enabled, account_id, state, created_at FROM event_source_mappings WHERE uuid = ?`,
		uuid,
	)
	var m EventSourceMapping
	var enabled int
	err := row.Scan(&m.UUID, &m.FunctionName, &m.EventSourceARN, &m.BatchSize, &enabled, &m.AccountID, &m.State, &m.CreatedAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrMappingNotFound
		}
		return nil, err
	}
	m.Enabled = enabled == 1
	return &m, nil
}

// UpdateEventSourceMapping updates batchSize and/or enabled for a mapping.
func (s *LambdaStore) UpdateEventSourceMapping(uuid string, batchSize int, enabled *bool) (*EventSourceMapping, error) {
	m, err := s.GetEventSourceMapping(uuid)
	if err != nil {
		return nil, err
	}
	if batchSize > 0 {
		m.BatchSize = batchSize
	}
	if enabled != nil {
		m.Enabled = *enabled
	}
	enabledInt := 0
	if m.Enabled {
		enabledInt = 1
	}
	_, err = s.store.DB().Exec(
		`UPDATE event_source_mappings SET batch_size = ?, enabled = ? WHERE uuid = ?`,
		m.BatchSize, enabledInt, uuid,
	)
	if err != nil {
		return nil, err
	}
	return m, nil
}

// DeleteEventSourceMapping removes a mapping by UUID.
func (s *LambdaStore) DeleteEventSourceMapping(uuid string) error {
	result, err := s.store.DB().Exec(
		`DELETE FROM event_source_mappings WHERE uuid = ?`,
		uuid,
	)
	if err != nil {
		return err
	}
	n, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return ErrMappingNotFound
	}
	return nil
}

// ListEventSourceMappings returns mappings, optionally filtered by functionName.
func (s *LambdaStore) ListEventSourceMappings(accountID, functionName string) ([]EventSourceMapping, error) {
	var rows *sql.Rows
	var err error
	if functionName != "" {
		rows, err = s.store.DB().Query(
			`SELECT uuid, function_name, event_source_arn, batch_size, enabled, account_id, state, created_at FROM event_source_mappings WHERE account_id = ? AND function_name = ? ORDER BY created_at`,
			accountID, functionName,
		)
	} else {
		rows, err = s.store.DB().Query(
			`SELECT uuid, function_name, event_source_arn, batch_size, enabled, account_id, state, created_at FROM event_source_mappings WHERE account_id = ? ORDER BY created_at`,
			accountID,
		)
	}
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	var mappings []EventSourceMapping
	for rows.Next() {
		var m EventSourceMapping
		var enabled int
		if err := rows.Scan(&m.UUID, &m.FunctionName, &m.EventSourceARN, &m.BatchSize, &enabled, &m.AccountID, &m.State, &m.CreatedAt); err != nil {
			return nil, err
		}
		m.Enabled = enabled == 1
		mappings = append(mappings, m)
	}
	return mappings, rows.Err()
}
