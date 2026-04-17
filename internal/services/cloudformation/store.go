// SPDX-License-Identifier: Apache-2.0

package cloudformation

import (
	"database/sql"
	"errors"
	"path/filepath"
	"time"

	"github.com/skyoo2003/devcloud/internal/shared"
	"github.com/skyoo2003/devcloud/internal/storage/sqlite"
)

var errStackNotFound = errors.New("stack not found")
var errChangeSetNotFound = errors.New("changeset not found")
var errStackSetNotFound = errors.New("stackset not found")

var migrations = []sqlite.Migration{
	{Version: 1, SQL: `
		CREATE TABLE IF NOT EXISTS stacks (
			name             TEXT NOT NULL,
			id               TEXT NOT NULL UNIQUE,
			arn              TEXT NOT NULL UNIQUE,
			status           TEXT NOT NULL DEFAULT 'CREATE_COMPLETE',
			template_body    TEXT NOT NULL DEFAULT '{}',
			parameters       TEXT NOT NULL DEFAULT '[]',
			outputs          TEXT NOT NULL DEFAULT '[]',
			capabilities     TEXT NOT NULL DEFAULT '[]',
			role_arn         TEXT NOT NULL DEFAULT '',
			description      TEXT NOT NULL DEFAULT '',
			disable_rollback INTEGER NOT NULL DEFAULT 0,
			created_at       INTEGER NOT NULL,
			updated_at       INTEGER NOT NULL,
			PRIMARY KEY (name)
		);
		CREATE TABLE IF NOT EXISTS stack_resources (
			stack_name  TEXT NOT NULL,
			logical_id  TEXT NOT NULL,
			physical_id TEXT NOT NULL DEFAULT '',
			type        TEXT NOT NULL DEFAULT '',
			status      TEXT NOT NULL DEFAULT 'CREATE_COMPLETE',
			PRIMARY KEY (stack_name, logical_id)
		);
		CREATE TABLE IF NOT EXISTS change_sets (
			name             TEXT NOT NULL,
			id               TEXT NOT NULL UNIQUE,
			arn              TEXT NOT NULL UNIQUE,
			stack_name       TEXT NOT NULL,
			status           TEXT NOT NULL DEFAULT 'CREATE_COMPLETE',
			execution_status TEXT NOT NULL DEFAULT 'AVAILABLE',
			template_body    TEXT NOT NULL DEFAULT '{}',
			parameters       TEXT NOT NULL DEFAULT '[]',
			changes          TEXT NOT NULL DEFAULT '[]',
			description      TEXT NOT NULL DEFAULT '',
			created_at       INTEGER NOT NULL
		);
		CREATE TABLE IF NOT EXISTS stack_sets (
			name           TEXT PRIMARY KEY,
			id             TEXT NOT NULL UNIQUE,
			arn            TEXT NOT NULL UNIQUE,
			status         TEXT NOT NULL DEFAULT 'ACTIVE',
			template_body  TEXT NOT NULL DEFAULT '{}',
			description    TEXT NOT NULL DEFAULT '',
			admin_role     TEXT NOT NULL DEFAULT '',
			execution_role TEXT NOT NULL DEFAULT '',
			created_at     INTEGER NOT NULL
		);
		CREATE TABLE IF NOT EXISTS exports (
			name       TEXT PRIMARY KEY,
			value      TEXT NOT NULL DEFAULT '',
			stack_name TEXT NOT NULL DEFAULT ''
		);
	`},
	{Version: 2, SQL: `
		ALTER TABLE stack_resources ADD COLUMN arn TEXT NOT NULL DEFAULT '';
		ALTER TABLE stack_resources ADD COLUMN attributes TEXT NOT NULL DEFAULT '{}';
	`},
}

// --- Model types ---

type Stack struct {
	Name            string
	ID              string
	ARN             string
	Status          string
	TemplateBody    string
	Parameters      string
	Outputs         string
	Capabilities    string
	RoleARN         string
	Description     string
	DisableRollback bool
	CreatedAt       time.Time
	UpdatedAt       time.Time
}

type StackResource struct {
	StackName  string
	LogicalID  string
	PhysicalID string
	Type       string
	Status     string
	ARN        string
	Attributes string // JSON map of resource attributes (e.g. Arn, DomainName, QueueUrl)
}

type ChangeSet struct {
	Name            string
	ID              string
	ARN             string
	StackName       string
	Status          string
	ExecutionStatus string
	TemplateBody    string
	Parameters      string
	Changes         string
	Description     string
	CreatedAt       time.Time
}

type StackSet struct {
	Name          string
	ID            string
	ARN           string
	Status        string
	TemplateBody  string
	Description   string
	AdminRole     string
	ExecutionRole string
	CreatedAt     time.Time
}

type Export struct {
	Name      string
	Value     string
	StackName string
}

// --- Store ---

type Store struct {
	store *sqlite.Store
	tags  *shared.TagStore
}

func NewStore(dataDir string) (*Store, error) {
	allMigrations := append(migrations, shared.TagMigrations...)
	dbPath := filepath.Join(dataDir, "cloudformation.db")
	s, err := sqlite.Open(dbPath, allMigrations)
	if err != nil {
		return nil, err
	}
	return &Store{store: s, tags: shared.NewTagStore(s)}, nil
}

func (s *Store) Close() error { return s.store.Close() }

// --- Stack CRUD ---

func (s *Store) CreateStack(name, id, arn, templateBody, parameters, outputs, capabilities, roleARN, description string, disableRollback bool) (*Stack, error) {
	now := time.Now().Unix()
	dr := 0
	if disableRollback {
		dr = 1
	}
	_, err := s.store.DB().Exec(
		`INSERT INTO stacks (name, id, arn, status, template_body, parameters, outputs, capabilities, role_arn, description, disable_rollback, created_at, updated_at)
		 VALUES (?, ?, ?, 'CREATE_COMPLETE', ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		name, id, arn, templateBody, parameters, outputs, capabilities, roleARN, description, dr, now, now,
	)
	if err != nil {
		return nil, err
	}
	return &Stack{
		Name: name, ID: id, ARN: arn, Status: "CREATE_COMPLETE",
		TemplateBody: templateBody, Parameters: parameters, Outputs: outputs,
		Capabilities: capabilities, RoleARN: roleARN, Description: description,
		DisableRollback: disableRollback,
		CreatedAt:       time.Unix(now, 0),
		UpdatedAt:       time.Unix(now, 0),
	}, nil
}

func (s *Store) GetStack(name string) (*Stack, error) {
	row := s.store.DB().QueryRow(
		`SELECT name, id, arn, status, template_body, parameters, outputs, capabilities, role_arn, description, disable_rollback, created_at, updated_at
		 FROM stacks WHERE name = ?`, name)
	return scanStack(row)
}

func (s *Store) ListStacks(statusFilter string) ([]Stack, error) {
	query := `SELECT name, id, arn, status, template_body, parameters, outputs, capabilities, role_arn, description, disable_rollback, created_at, updated_at FROM stacks`
	var args []any
	if statusFilter != "" {
		query += " WHERE status = ?"
		args = append(args, statusFilter)
	}
	query += " ORDER BY name"
	rows, err := s.store.DB().Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var stacks []Stack
	for rows.Next() {
		st, err := scanStack(rows)
		if err != nil {
			return nil, err
		}
		stacks = append(stacks, *st)
	}
	return stacks, rows.Err()
}

func (s *Store) UpdateStack(name, templateBody, parameters, outputs, capabilities, roleARN, description string) error {
	now := time.Now().Unix()
	res, err := s.store.DB().Exec(
		`UPDATE stacks SET template_body=?, parameters=?, outputs=?, capabilities=?, role_arn=?, description=?, status='UPDATE_COMPLETE', updated_at=? WHERE name=?`,
		templateBody, parameters, outputs, capabilities, roleARN, description, now, name,
	)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errStackNotFound
	}
	return nil
}

func (s *Store) DeleteStack(name string) error {
	res, err := s.store.DB().Exec(`DELETE FROM stacks WHERE name = ?`, name)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errStackNotFound
	}
	s.store.DB().Exec(`DELETE FROM stack_resources WHERE stack_name = ?`, name)
	s.store.DB().Exec(`DELETE FROM change_sets WHERE stack_name = ?`, name)
	return nil
}

// --- StackResource CRUD ---

func (s *Store) UpsertStackResource(stackName, logicalID, physicalID, resType, status string) error {
	_, err := s.store.DB().Exec(
		`INSERT INTO stack_resources (stack_name, logical_id, physical_id, type, status) VALUES (?, ?, ?, ?, ?)
		 ON CONFLICT(stack_name, logical_id) DO UPDATE SET physical_id=excluded.physical_id, type=excluded.type, status=excluded.status`,
		stackName, logicalID, physicalID, resType, status,
	)
	return err
}

// UpsertStackResourceFull persists the full resource row including ARN and a JSON attribute blob.
func (s *Store) UpsertStackResourceFull(stackName, logicalID, physicalID, resType, status, arn, attributes string) error {
	if attributes == "" {
		attributes = "{}"
	}
	_, err := s.store.DB().Exec(
		`INSERT INTO stack_resources (stack_name, logical_id, physical_id, type, status, arn, attributes)
		 VALUES (?, ?, ?, ?, ?, ?, ?)
		 ON CONFLICT(stack_name, logical_id) DO UPDATE SET
		    physical_id=excluded.physical_id,
		    type=excluded.type,
		    status=excluded.status,
		    arn=excluded.arn,
		    attributes=excluded.attributes`,
		stackName, logicalID, physicalID, resType, status, arn, attributes,
	)
	return err
}

// DeleteStackResource removes a single resource row.
func (s *Store) DeleteStackResource(stackName, logicalID string) error {
	_, err := s.store.DB().Exec(
		`DELETE FROM stack_resources WHERE stack_name = ? AND logical_id = ?`,
		stackName, logicalID,
	)
	return err
}

// SetStackStatus updates the stack's status column.
func (s *Store) SetStackStatus(stackName, status string) error {
	now := time.Now().Unix()
	_, err := s.store.DB().Exec(
		`UPDATE stacks SET status=?, updated_at=? WHERE name=?`,
		status, now, stackName,
	)
	return err
}

func (s *Store) ListStackResources(stackName string) ([]StackResource, error) {
	rows, err := s.store.DB().Query(
		`SELECT stack_name, logical_id, physical_id, type, status, arn, attributes FROM stack_resources WHERE stack_name = ? ORDER BY logical_id`,
		stackName,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []StackResource
	for rows.Next() {
		var r StackResource
		if err := rows.Scan(&r.StackName, &r.LogicalID, &r.PhysicalID, &r.Type, &r.Status, &r.ARN, &r.Attributes); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

func (s *Store) GetStackResource(stackName, logicalID string) (*StackResource, error) {
	row := s.store.DB().QueryRow(
		`SELECT stack_name, logical_id, physical_id, type, status, arn, attributes FROM stack_resources WHERE stack_name = ? AND logical_id = ?`,
		stackName, logicalID,
	)
	var r StackResource
	err := row.Scan(&r.StackName, &r.LogicalID, &r.PhysicalID, &r.Type, &r.Status, &r.ARN, &r.Attributes)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, errors.New("stack resource not found")
	}
	return &r, err
}

// --- ChangeSet CRUD ---

func (s *Store) CreateChangeSet(name, id, arn, stackName, templateBody, parameters, description string) (*ChangeSet, error) {
	now := time.Now().Unix()
	_, err := s.store.DB().Exec(
		`INSERT INTO change_sets (name, id, arn, stack_name, status, execution_status, template_body, parameters, changes, description, created_at)
		 VALUES (?, ?, ?, ?, 'CREATE_COMPLETE', 'AVAILABLE', ?, ?, '[]', ?, ?)`,
		name, id, arn, stackName, templateBody, parameters, description, now,
	)
	if err != nil {
		return nil, err
	}
	return &ChangeSet{
		Name: name, ID: id, ARN: arn, StackName: stackName,
		Status: "CREATE_COMPLETE", ExecutionStatus: "AVAILABLE",
		TemplateBody: templateBody, Parameters: parameters, Changes: "[]",
		Description: description, CreatedAt: time.Unix(now, 0),
	}, nil
}

func (s *Store) GetChangeSet(stackName, nameOrID string) (*ChangeSet, error) {
	row := s.store.DB().QueryRow(
		`SELECT name, id, arn, stack_name, status, execution_status, template_body, parameters, changes, description, created_at
		 FROM change_sets WHERE stack_name = ? AND (name = ? OR id = ? OR arn = ?)`,
		stackName, nameOrID, nameOrID, nameOrID,
	)
	return scanChangeSet(row)
}

func (s *Store) ListChangeSets(stackName string) ([]ChangeSet, error) {
	rows, err := s.store.DB().Query(
		`SELECT name, id, arn, stack_name, status, execution_status, template_body, parameters, changes, description, created_at
		 FROM change_sets WHERE stack_name = ? ORDER BY created_at DESC`,
		stackName,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ChangeSet
	for rows.Next() {
		cs, err := scanChangeSet(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *cs)
	}
	return out, rows.Err()
}

func (s *Store) DeleteChangeSet(stackName, nameOrID string) error {
	res, err := s.store.DB().Exec(
		`DELETE FROM change_sets WHERE stack_name = ? AND (name = ? OR id = ? OR arn = ?)`,
		stackName, nameOrID, nameOrID, nameOrID,
	)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errChangeSetNotFound
	}
	return nil
}

func (s *Store) ExecuteChangeSet(stackName, nameOrID string) error {
	res, err := s.store.DB().Exec(
		`UPDATE change_sets SET status='EXECUTE_COMPLETE', execution_status='EXECUTE_COMPLETE' WHERE stack_name = ? AND (name = ? OR id = ? OR arn = ?)`,
		stackName, nameOrID, nameOrID, nameOrID,
	)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errChangeSetNotFound
	}
	return nil
}

// --- StackSet CRUD ---

func (s *Store) CreateStackSet(name, id, arn, templateBody, description, adminRole, executionRole string) (*StackSet, error) {
	now := time.Now().Unix()
	_, err := s.store.DB().Exec(
		`INSERT INTO stack_sets (name, id, arn, status, template_body, description, admin_role, execution_role, created_at)
		 VALUES (?, ?, ?, 'ACTIVE', ?, ?, ?, ?, ?)`,
		name, id, arn, templateBody, description, adminRole, executionRole, now,
	)
	if err != nil {
		return nil, err
	}
	return &StackSet{
		Name: name, ID: id, ARN: arn, Status: "ACTIVE",
		TemplateBody: templateBody, Description: description,
		AdminRole: adminRole, ExecutionRole: executionRole,
		CreatedAt: time.Unix(now, 0),
	}, nil
}

func (s *Store) GetStackSet(name string) (*StackSet, error) {
	row := s.store.DB().QueryRow(
		`SELECT name, id, arn, status, template_body, description, admin_role, execution_role, created_at FROM stack_sets WHERE name = ?`, name)
	return scanStackSet(row)
}

func (s *Store) ListStackSets() ([]StackSet, error) {
	rows, err := s.store.DB().Query(
		`SELECT name, id, arn, status, template_body, description, admin_role, execution_role, created_at FROM stack_sets ORDER BY name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []StackSet
	for rows.Next() {
		ss, err := scanStackSet(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *ss)
	}
	return out, rows.Err()
}

func (s *Store) UpdateStackSet(name, templateBody, description, adminRole, executionRole string) error {
	res, err := s.store.DB().Exec(
		`UPDATE stack_sets SET template_body=?, description=?, admin_role=?, execution_role=? WHERE name=?`,
		templateBody, description, adminRole, executionRole, name,
	)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errStackSetNotFound
	}
	return nil
}

func (s *Store) DeleteStackSet(name string) error {
	res, err := s.store.DB().Exec(`DELETE FROM stack_sets WHERE name = ?`, name)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errStackSetNotFound
	}
	return nil
}

// --- Exports CRUD ---

func (s *Store) ListExports() ([]Export, error) {
	rows, err := s.store.DB().Query(`SELECT name, value, stack_name FROM exports ORDER BY name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Export
	for rows.Next() {
		var e Export
		if err := rows.Scan(&e.Name, &e.Value, &e.StackName); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// --- Tags ---

func (s *Store) AddTags(arn string, tags map[string]string) error {
	return s.tags.AddTags(arn, tags)
}

func (s *Store) ListTags(arn string) (map[string]string, error) {
	return s.tags.ListTags(arn)
}

func (s *Store) DeleteAllTags(arn string) error {
	return s.tags.DeleteAllTags(arn)
}

// --- Scan helpers ---

type scanner interface {
	Scan(dest ...any) error
}

func scanStack(row scanner) (*Stack, error) {
	var st Stack
	var dr int
	var createdAt, updatedAt int64
	err := row.Scan(
		&st.Name, &st.ID, &st.ARN, &st.Status,
		&st.TemplateBody, &st.Parameters, &st.Outputs, &st.Capabilities,
		&st.RoleARN, &st.Description, &dr, &createdAt, &updatedAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, errStackNotFound
	}
	if err != nil {
		return nil, err
	}
	st.DisableRollback = dr != 0
	st.CreatedAt = time.Unix(createdAt, 0)
	st.UpdatedAt = time.Unix(updatedAt, 0)
	return &st, nil
}

func scanChangeSet(row scanner) (*ChangeSet, error) {
	var cs ChangeSet
	var createdAt int64
	err := row.Scan(
		&cs.Name, &cs.ID, &cs.ARN, &cs.StackName, &cs.Status, &cs.ExecutionStatus,
		&cs.TemplateBody, &cs.Parameters, &cs.Changes, &cs.Description, &createdAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, errChangeSetNotFound
	}
	if err != nil {
		return nil, err
	}
	cs.CreatedAt = time.Unix(createdAt, 0)
	return &cs, nil
}

func scanStackSet(row scanner) (*StackSet, error) {
	var ss StackSet
	var createdAt int64
	err := row.Scan(
		&ss.Name, &ss.ID, &ss.ARN, &ss.Status,
		&ss.TemplateBody, &ss.Description, &ss.AdminRole, &ss.ExecutionRole, &createdAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, errStackSetNotFound
	}
	if err != nil {
		return nil, err
	}
	ss.CreatedAt = time.Unix(createdAt, 0)
	return &ss, nil
}
