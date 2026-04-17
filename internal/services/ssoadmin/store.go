// SPDX-License-Identifier: Apache-2.0

// internal/services/ssoadmin/store.go
package ssoadmin

import (
	"database/sql"
	"errors"
	"path/filepath"
	"time"

	"github.com/skyoo2003/devcloud/internal/shared"
	"github.com/skyoo2003/devcloud/internal/storage/sqlite"
)

var (
	errInstanceNotFound           = errors.New("instance not found")
	errPermissionSetNotFound      = errors.New("permission set not found")
	errAssignmentNotFound         = errors.New("account assignment not found")
	errApplicationNotFound        = errors.New("application not found")
	errTrustedTokenIssuerNotFound = errors.New("trusted token issuer not found")
)

var migrations = []sqlite.Migration{
	{Version: 1, SQL: `
		CREATE TABLE IF NOT EXISTS instances (
			arn              TEXT PRIMARY KEY,
			name             TEXT NOT NULL DEFAULT '',
			identity_store_id TEXT NOT NULL DEFAULT '',
			status           TEXT NOT NULL DEFAULT 'ACTIVE',
			created_at       INTEGER NOT NULL
		);
		CREATE TABLE IF NOT EXISTS permission_sets (
			arn                  TEXT PRIMARY KEY,
			name                 TEXT NOT NULL,
			instance_arn         TEXT NOT NULL,
			description          TEXT NOT NULL DEFAULT '',
			session_duration     TEXT NOT NULL DEFAULT 'PT1H',
			relay_state          TEXT NOT NULL DEFAULT '',
			inline_policy        TEXT NOT NULL DEFAULT '',
			permissions_boundary TEXT NOT NULL DEFAULT '{}',
			created_at           INTEGER NOT NULL
		);
		CREATE TABLE IF NOT EXISTS managed_policies (
			permission_set_arn TEXT NOT NULL,
			policy_arn         TEXT NOT NULL,
			PRIMARY KEY (permission_set_arn, policy_arn)
		);
		CREATE TABLE IF NOT EXISTS customer_managed_policies (
			permission_set_arn TEXT NOT NULL,
			name               TEXT NOT NULL,
			path               TEXT NOT NULL DEFAULT '/',
			PRIMARY KEY (permission_set_arn, name, path)
		);
		CREATE TABLE IF NOT EXISTS account_assignments (
			instance_arn       TEXT NOT NULL,
			permission_set_arn TEXT NOT NULL,
			target_id          TEXT NOT NULL,
			target_type        TEXT NOT NULL DEFAULT 'AWS_ACCOUNT',
			principal_id       TEXT NOT NULL,
			principal_type     TEXT NOT NULL DEFAULT 'USER',
			PRIMARY KEY (instance_arn, permission_set_arn, target_id, principal_id)
		);
		CREATE TABLE IF NOT EXISTS applications (
			arn              TEXT PRIMARY KEY,
			name             TEXT NOT NULL,
			instance_arn     TEXT NOT NULL,
			app_provider_arn TEXT NOT NULL DEFAULT '',
			description      TEXT NOT NULL DEFAULT '',
			status           TEXT NOT NULL DEFAULT 'ENABLED',
			created_at       INTEGER NOT NULL
		);
		CREATE TABLE IF NOT EXISTS trusted_token_issuers (
			arn          TEXT PRIMARY KEY,
			name         TEXT NOT NULL,
			instance_arn TEXT NOT NULL,
			type         TEXT NOT NULL DEFAULT 'OIDC_JWT',
			config       TEXT NOT NULL DEFAULT '{}',
			created_at   INTEGER NOT NULL
		);
	`},
}

// --- model types ---

type Instance struct {
	ARN             string
	Name            string
	IdentityStoreID string
	Status          string
	CreatedAt       time.Time
}

type PermissionSet struct {
	ARN                 string
	Name                string
	InstanceARN         string
	Description         string
	SessionDuration     string
	RelayState          string
	InlinePolicy        string
	PermissionsBoundary string
	CreatedAt           time.Time
}

type AccountAssignment struct {
	InstanceARN      string
	PermissionSetARN string
	TargetID         string
	TargetType       string
	PrincipalID      string
	PrincipalType    string
}

type Application struct {
	ARN            string
	Name           string
	InstanceARN    string
	AppProviderARN string
	Description    string
	Status         string
	CreatedAt      time.Time
}

type TrustedTokenIssuer struct {
	ARN         string
	Name        string
	InstanceARN string
	Type        string
	Config      string
	CreatedAt   time.Time
}

// --- store ---

type Store struct {
	store *sqlite.Store
	tags  *shared.TagStore
}

func NewStore(dataDir string) (*Store, error) {
	allMigrations := append(migrations, shared.TagMigrations...)
	dbPath := filepath.Join(dataDir, "ssoadmin.db")
	s, err := sqlite.Open(dbPath, allMigrations)
	if err != nil {
		return nil, err
	}
	return &Store{store: s, tags: shared.NewTagStore(s)}, nil
}

func (s *Store) Close() error { return s.store.Close() }

// --- Instance ---

func (s *Store) CreateInstance(inst *Instance) error {
	now := time.Now().Unix()
	_, err := s.store.DB().Exec(`
		INSERT INTO instances (arn, name, identity_store_id, status, created_at)
		VALUES (?, ?, ?, ?, ?)`,
		inst.ARN, inst.Name, inst.IdentityStoreID, inst.Status, now,
	)
	return err
}

func (s *Store) GetInstance(arn string) (*Instance, error) {
	row := s.store.DB().QueryRow(`
		SELECT arn, name, identity_store_id, status, created_at
		FROM instances WHERE arn = ?`, arn)
	return scanInstance(row)
}

func (s *Store) ListInstances() ([]Instance, error) {
	rows, err := s.store.DB().Query(`
		SELECT arn, name, identity_store_id, status, created_at
		FROM instances ORDER BY created_at`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var insts []Instance
	for rows.Next() {
		inst, err := scanInstance(rows)
		if err != nil {
			return nil, err
		}
		insts = append(insts, *inst)
	}
	return insts, rows.Err()
}

func (s *Store) UpdateInstance(arn, name string) error {
	res, err := s.store.DB().Exec(`UPDATE instances SET name=? WHERE arn=?`, name, arn)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errInstanceNotFound
	}
	return nil
}

func (s *Store) DeleteInstance(arn string) error {
	res, err := s.store.DB().Exec(`DELETE FROM instances WHERE arn = ?`, arn)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errInstanceNotFound
	}
	return nil
}

// --- PermissionSet ---

func (s *Store) CreatePermissionSet(ps *PermissionSet) error {
	now := time.Now().Unix()
	_, err := s.store.DB().Exec(`
		INSERT INTO permission_sets (arn, name, instance_arn, description, session_duration, relay_state, inline_policy, permissions_boundary, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		ps.ARN, ps.Name, ps.InstanceARN, ps.Description, ps.SessionDuration,
		ps.RelayState, ps.InlinePolicy, ps.PermissionsBoundary, now,
	)
	return err
}

func (s *Store) GetPermissionSet(arn string) (*PermissionSet, error) {
	row := s.store.DB().QueryRow(`
		SELECT arn, name, instance_arn, description, session_duration, relay_state, inline_policy, permissions_boundary, created_at
		FROM permission_sets WHERE arn = ?`, arn)
	return scanPermissionSet(row)
}

func (s *Store) ListPermissionSets(instanceARN string) ([]PermissionSet, error) {
	rows, err := s.store.DB().Query(`
		SELECT arn, name, instance_arn, description, session_duration, relay_state, inline_policy, permissions_boundary, created_at
		FROM permission_sets WHERE instance_arn = ? ORDER BY created_at`, instanceARN)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var sets []PermissionSet
	for rows.Next() {
		ps, err := scanPermissionSet(rows)
		if err != nil {
			return nil, err
		}
		sets = append(sets, *ps)
	}
	return sets, rows.Err()
}

func (s *Store) UpdatePermissionSet(arn string, fields map[string]any) error {
	ps, err := s.GetPermissionSet(arn)
	if err != nil {
		return errPermissionSetNotFound
	}
	if v, ok := fields["Description"].(string); ok {
		ps.Description = v
	}
	if v, ok := fields["SessionDuration"].(string); ok && v != "" {
		ps.SessionDuration = v
	}
	if v, ok := fields["RelayState"].(string); ok {
		ps.RelayState = v
	}
	res, err := s.store.DB().Exec(`
		UPDATE permission_sets SET description=?, session_duration=?, relay_state=? WHERE arn=?`,
		ps.Description, ps.SessionDuration, ps.RelayState, arn,
	)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errPermissionSetNotFound
	}
	return nil
}

func (s *Store) DeletePermissionSet(arn string) error {
	res, err := s.store.DB().Exec(`DELETE FROM permission_sets WHERE arn = ?`, arn)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errPermissionSetNotFound
	}
	return nil
}

func (s *Store) SetInlinePolicy(permissionSetARN, policy string) error {
	res, err := s.store.DB().Exec(`UPDATE permission_sets SET inline_policy=? WHERE arn=?`, policy, permissionSetARN)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errPermissionSetNotFound
	}
	return nil
}

func (s *Store) DeleteInlinePolicy(permissionSetARN string) error {
	return s.SetInlinePolicy(permissionSetARN, "")
}

func (s *Store) SetPermissionsBoundary(permissionSetARN, boundary string) error {
	res, err := s.store.DB().Exec(`UPDATE permission_sets SET permissions_boundary=? WHERE arn=?`, boundary, permissionSetARN)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errPermissionSetNotFound
	}
	return nil
}

func (s *Store) DeletePermissionsBoundary(permissionSetARN string) error {
	return s.SetPermissionsBoundary(permissionSetARN, "{}")
}

// --- Managed policies ---

func (s *Store) AttachManagedPolicy(permissionSetARN, policyARN string) error {
	_, err := s.store.DB().Exec(`
		INSERT OR IGNORE INTO managed_policies (permission_set_arn, policy_arn) VALUES (?, ?)`,
		permissionSetARN, policyARN)
	return err
}

func (s *Store) DetachManagedPolicy(permissionSetARN, policyARN string) error {
	_, err := s.store.DB().Exec(`
		DELETE FROM managed_policies WHERE permission_set_arn = ? AND policy_arn = ?`,
		permissionSetARN, policyARN)
	return err
}

func (s *Store) ListManagedPolicies(permissionSetARN string) ([]string, error) {
	rows, err := s.store.DB().Query(`
		SELECT policy_arn FROM managed_policies WHERE permission_set_arn = ? ORDER BY policy_arn`,
		permissionSetARN)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var arns []string
	for rows.Next() {
		var arn string
		if err := rows.Scan(&arn); err != nil {
			return nil, err
		}
		arns = append(arns, arn)
	}
	return arns, rows.Err()
}

// --- Customer managed policies ---

func (s *Store) AttachCustomerManagedPolicy(permissionSetARN, name, path string) error {
	if path == "" {
		path = "/"
	}
	_, err := s.store.DB().Exec(`
		INSERT OR IGNORE INTO customer_managed_policies (permission_set_arn, name, path) VALUES (?, ?, ?)`,
		permissionSetARN, name, path)
	return err
}

func (s *Store) DetachCustomerManagedPolicy(permissionSetARN, name, path string) error {
	if path == "" {
		path = "/"
	}
	_, err := s.store.DB().Exec(`
		DELETE FROM customer_managed_policies WHERE permission_set_arn = ? AND name = ? AND path = ?`,
		permissionSetARN, name, path)
	return err
}

func (s *Store) ListCustomerManagedPolicies(permissionSetARN string) ([][2]string, error) {
	rows, err := s.store.DB().Query(`
		SELECT name, path FROM customer_managed_policies WHERE permission_set_arn = ? ORDER BY name`,
		permissionSetARN)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var policies [][2]string
	for rows.Next() {
		var p [2]string
		if err := rows.Scan(&p[0], &p[1]); err != nil {
			return nil, err
		}
		policies = append(policies, p)
	}
	return policies, rows.Err()
}

// --- AccountAssignment ---

func (s *Store) CreateAccountAssignment(a *AccountAssignment) error {
	_, err := s.store.DB().Exec(`
		INSERT OR IGNORE INTO account_assignments
		(instance_arn, permission_set_arn, target_id, target_type, principal_id, principal_type)
		VALUES (?, ?, ?, ?, ?, ?)`,
		a.InstanceARN, a.PermissionSetARN, a.TargetID, a.TargetType, a.PrincipalID, a.PrincipalType,
	)
	return err
}

func (s *Store) DeleteAccountAssignment(instanceARN, permissionSetARN, targetID, principalID string) error {
	res, err := s.store.DB().Exec(`
		DELETE FROM account_assignments
		WHERE instance_arn=? AND permission_set_arn=? AND target_id=? AND principal_id=?`,
		instanceARN, permissionSetARN, targetID, principalID)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errAssignmentNotFound
	}
	return nil
}

func (s *Store) ListAccountAssignments(instanceARN, permissionSetARN, accountID string) ([]AccountAssignment, error) {
	rows, err := s.store.DB().Query(`
		SELECT instance_arn, permission_set_arn, target_id, target_type, principal_id, principal_type
		FROM account_assignments
		WHERE instance_arn=? AND permission_set_arn=? AND target_id=?`,
		instanceARN, permissionSetARN, accountID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanAssignments(rows)
}

func (s *Store) ListAccountAssignmentsForPrincipal(instanceARN, principalID, principalType string) ([]AccountAssignment, error) {
	rows, err := s.store.DB().Query(`
		SELECT instance_arn, permission_set_arn, target_id, target_type, principal_id, principal_type
		FROM account_assignments
		WHERE instance_arn=? AND principal_id=? AND principal_type=?`,
		instanceARN, principalID, principalType)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanAssignments(rows)
}

func (s *Store) ListAccountsForPermissionSet(instanceARN, permissionSetARN string) ([]string, error) {
	rows, err := s.store.DB().Query(`
		SELECT DISTINCT target_id FROM account_assignments
		WHERE instance_arn=? AND permission_set_arn=?`,
		instanceARN, permissionSetARN)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var accounts []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		accounts = append(accounts, id)
	}
	return accounts, rows.Err()
}

func (s *Store) ListPermissionSetsProvisionedToAccount(instanceARN, accountID string) ([]string, error) {
	rows, err := s.store.DB().Query(`
		SELECT DISTINCT permission_set_arn FROM account_assignments
		WHERE instance_arn=? AND target_id=?`,
		instanceARN, accountID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var arns []string
	for rows.Next() {
		var arn string
		if err := rows.Scan(&arn); err != nil {
			return nil, err
		}
		arns = append(arns, arn)
	}
	return arns, rows.Err()
}

func scanAssignments(rows *sql.Rows) ([]AccountAssignment, error) {
	var assignments []AccountAssignment
	for rows.Next() {
		var a AccountAssignment
		if err := rows.Scan(&a.InstanceARN, &a.PermissionSetARN, &a.TargetID, &a.TargetType, &a.PrincipalID, &a.PrincipalType); err != nil {
			return nil, err
		}
		assignments = append(assignments, a)
	}
	return assignments, rows.Err()
}

// --- Application ---

func (s *Store) CreateApplication(app *Application) error {
	now := time.Now().Unix()
	_, err := s.store.DB().Exec(`
		INSERT INTO applications (arn, name, instance_arn, app_provider_arn, description, status, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?)`,
		app.ARN, app.Name, app.InstanceARN, app.AppProviderARN, app.Description, app.Status, now,
	)
	return err
}

func (s *Store) GetApplication(arn string) (*Application, error) {
	row := s.store.DB().QueryRow(`
		SELECT arn, name, instance_arn, app_provider_arn, description, status, created_at
		FROM applications WHERE arn = ?`, arn)
	return scanApplication(row)
}

func (s *Store) ListApplications(instanceARN string) ([]Application, error) {
	rows, err := s.store.DB().Query(`
		SELECT arn, name, instance_arn, app_provider_arn, description, status, created_at
		FROM applications WHERE instance_arn = ? ORDER BY created_at`, instanceARN)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var apps []Application
	for rows.Next() {
		app, err := scanApplication(rows)
		if err != nil {
			return nil, err
		}
		apps = append(apps, *app)
	}
	return apps, rows.Err()
}

func (s *Store) UpdateApplication(arn string, fields map[string]any) error {
	app, err := s.GetApplication(arn)
	if err != nil {
		return errApplicationNotFound
	}
	if v, ok := fields["Name"].(string); ok && v != "" {
		app.Name = v
	}
	if v, ok := fields["Description"].(string); ok {
		app.Description = v
	}
	if v, ok := fields["Status"].(string); ok && v != "" {
		app.Status = v
	}
	res, err := s.store.DB().Exec(`
		UPDATE applications SET name=?, description=?, status=? WHERE arn=?`,
		app.Name, app.Description, app.Status, arn,
	)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errApplicationNotFound
	}
	return nil
}

func (s *Store) DeleteApplication(arn string) error {
	res, err := s.store.DB().Exec(`DELETE FROM applications WHERE arn = ?`, arn)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errApplicationNotFound
	}
	return nil
}

// --- TrustedTokenIssuer ---

func (s *Store) CreateTrustedTokenIssuer(tti *TrustedTokenIssuer) error {
	now := time.Now().Unix()
	_, err := s.store.DB().Exec(`
		INSERT INTO trusted_token_issuers (arn, name, instance_arn, type, config, created_at)
		VALUES (?, ?, ?, ?, ?, ?)`,
		tti.ARN, tti.Name, tti.InstanceARN, tti.Type, tti.Config, now,
	)
	return err
}

func (s *Store) GetTrustedTokenIssuer(arn string) (*TrustedTokenIssuer, error) {
	row := s.store.DB().QueryRow(`
		SELECT arn, name, instance_arn, type, config, created_at
		FROM trusted_token_issuers WHERE arn = ?`, arn)
	return scanTrustedTokenIssuer(row)
}

func (s *Store) ListTrustedTokenIssuers(instanceARN string) ([]TrustedTokenIssuer, error) {
	rows, err := s.store.DB().Query(`
		SELECT arn, name, instance_arn, type, config, created_at
		FROM trusted_token_issuers WHERE instance_arn = ? ORDER BY created_at`, instanceARN)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var issuers []TrustedTokenIssuer
	for rows.Next() {
		tti, err := scanTrustedTokenIssuer(rows)
		if err != nil {
			return nil, err
		}
		issuers = append(issuers, *tti)
	}
	return issuers, rows.Err()
}

func (s *Store) UpdateTrustedTokenIssuer(arn string, fields map[string]any) error {
	tti, err := s.GetTrustedTokenIssuer(arn)
	if err != nil {
		return errTrustedTokenIssuerNotFound
	}
	if v, ok := fields["Name"].(string); ok && v != "" {
		tti.Name = v
	}
	if v, ok := fields["Config"].(string); ok {
		tti.Config = v
	}
	res, err := s.store.DB().Exec(`
		UPDATE trusted_token_issuers SET name=?, config=? WHERE arn=?`,
		tti.Name, tti.Config, arn,
	)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errTrustedTokenIssuerNotFound
	}
	return nil
}

func (s *Store) DeleteTrustedTokenIssuer(arn string) error {
	res, err := s.store.DB().Exec(`DELETE FROM trusted_token_issuers WHERE arn = ?`, arn)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errTrustedTokenIssuerNotFound
	}
	return nil
}

// --- scanners ---

type rowScanner interface{ Scan(dest ...any) error }

func scanInstance(sc rowScanner) (*Instance, error) {
	var inst Instance
	var createdAt int64
	err := sc.Scan(&inst.ARN, &inst.Name, &inst.IdentityStoreID, &inst.Status, &createdAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errInstanceNotFound
		}
		return nil, err
	}
	inst.CreatedAt = time.Unix(createdAt, 0)
	return &inst, nil
}

func scanPermissionSet(sc rowScanner) (*PermissionSet, error) {
	var ps PermissionSet
	var createdAt int64
	err := sc.Scan(
		&ps.ARN, &ps.Name, &ps.InstanceARN, &ps.Description,
		&ps.SessionDuration, &ps.RelayState, &ps.InlinePolicy, &ps.PermissionsBoundary, &createdAt,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errPermissionSetNotFound
		}
		return nil, err
	}
	ps.CreatedAt = time.Unix(createdAt, 0)
	return &ps, nil
}

func scanApplication(sc rowScanner) (*Application, error) {
	var app Application
	var createdAt int64
	err := sc.Scan(&app.ARN, &app.Name, &app.InstanceARN, &app.AppProviderARN, &app.Description, &app.Status, &createdAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errApplicationNotFound
		}
		return nil, err
	}
	app.CreatedAt = time.Unix(createdAt, 0)
	return &app, nil
}

func scanTrustedTokenIssuer(sc rowScanner) (*TrustedTokenIssuer, error) {
	var tti TrustedTokenIssuer
	var createdAt int64
	err := sc.Scan(&tti.ARN, &tti.Name, &tti.InstanceARN, &tti.Type, &tti.Config, &createdAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errTrustedTokenIssuerNotFound
		}
		return nil, err
	}
	tti.CreatedAt = time.Unix(createdAt, 0)
	return &tti, nil
}
