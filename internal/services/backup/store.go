// SPDX-License-Identifier: Apache-2.0

// internal/services/backup/store.go
package backup

import (
	"database/sql"
	"errors"
	"path/filepath"
	"time"

	"github.com/skyoo2003/devcloud/internal/shared"
	"github.com/skyoo2003/devcloud/internal/storage/sqlite"
)

var (
	errNotFound = errors.New("resource not found")
)

var migrations = []sqlite.Migration{
	{Version: 1, SQL: `
		CREATE TABLE IF NOT EXISTS backup_plans (
			id         TEXT PRIMARY KEY,
			arn        TEXT NOT NULL UNIQUE,
			name       TEXT NOT NULL,
			version_id TEXT NOT NULL DEFAULT '',
			rules      TEXT NOT NULL DEFAULT '[]',
			created_at INTEGER NOT NULL
		);
		CREATE TABLE IF NOT EXISTS backup_vaults (
			name           TEXT PRIMARY KEY,
			arn            TEXT NOT NULL UNIQUE,
			encryption_key TEXT NOT NULL DEFAULT '',
			notifications  TEXT NOT NULL DEFAULT '{}',
			access_policy  TEXT NOT NULL DEFAULT '',
			lock_config    TEXT NOT NULL DEFAULT '{}',
			created_at     INTEGER NOT NULL
		);
		CREATE TABLE IF NOT EXISTS backup_selections (
			id         TEXT PRIMARY KEY,
			plan_id    TEXT NOT NULL,
			name       TEXT NOT NULL,
			iam_role   TEXT NOT NULL DEFAULT '',
			resources  TEXT NOT NULL DEFAULT '[]',
			conditions TEXT NOT NULL DEFAULT '{}',
			created_at INTEGER NOT NULL
		);
		CREATE TABLE IF NOT EXISTS backup_jobs (
			id           TEXT PRIMARY KEY,
			vault_name   TEXT NOT NULL,
			resource_arn TEXT NOT NULL DEFAULT '',
			resource_type TEXT NOT NULL DEFAULT '',
			status       TEXT NOT NULL DEFAULT 'COMPLETED',
			plan_id      TEXT NOT NULL DEFAULT '',
			created_at   INTEGER NOT NULL,
			completed_at INTEGER NOT NULL DEFAULT 0
		);
		CREATE TABLE IF NOT EXISTS recovery_points (
			arn           TEXT PRIMARY KEY,
			vault_name    TEXT NOT NULL,
			resource_arn  TEXT NOT NULL DEFAULT '',
			resource_type TEXT NOT NULL DEFAULT '',
			status        TEXT NOT NULL DEFAULT 'COMPLETED',
			created_at    INTEGER NOT NULL
		);
		CREATE TABLE IF NOT EXISTS restore_jobs (
			id              TEXT PRIMARY KEY,
			vault_name      TEXT NOT NULL DEFAULT '',
			recovery_point  TEXT NOT NULL DEFAULT '',
			resource_type   TEXT NOT NULL DEFAULT '',
			status          TEXT NOT NULL DEFAULT 'COMPLETED',
			created_at      INTEGER NOT NULL,
			completed_at    INTEGER NOT NULL DEFAULT 0
		);
		CREATE TABLE IF NOT EXISTS frameworks (
			name        TEXT PRIMARY KEY,
			arn         TEXT NOT NULL UNIQUE,
			description TEXT NOT NULL DEFAULT '',
			controls    TEXT NOT NULL DEFAULT '[]',
			status      TEXT NOT NULL DEFAULT 'ACTIVE',
			created_at  INTEGER NOT NULL
		);
		CREATE TABLE IF NOT EXISTS report_plans (
			name             TEXT PRIMARY KEY,
			arn              TEXT NOT NULL UNIQUE,
			description      TEXT NOT NULL DEFAULT '',
			delivery_channel TEXT NOT NULL DEFAULT '{}',
			report_setting   TEXT NOT NULL DEFAULT '{}',
			created_at       INTEGER NOT NULL
		);
		CREATE TABLE IF NOT EXISTS restore_testing_plans (
			name         TEXT PRIMARY KEY,
			arn          TEXT NOT NULL UNIQUE,
			schedule     TEXT NOT NULL DEFAULT '',
			start_window INTEGER NOT NULL DEFAULT 0,
			config       TEXT NOT NULL DEFAULT '{}',
			created_at   INTEGER NOT NULL
		);
		CREATE TABLE IF NOT EXISTS restore_testing_selections (
			name          TEXT NOT NULL,
			plan_name     TEXT NOT NULL,
			iam_role      TEXT NOT NULL DEFAULT '',
			resource_type TEXT NOT NULL DEFAULT '',
			config        TEXT NOT NULL DEFAULT '{}',
			created_at    INTEGER NOT NULL,
			PRIMARY KEY (name, plan_name)
		);
		CREATE TABLE IF NOT EXISTS report_jobs (
			id           TEXT PRIMARY KEY,
			plan_name    TEXT NOT NULL DEFAULT '',
			status       TEXT NOT NULL DEFAULT 'COMPLETED',
			created_at   INTEGER NOT NULL,
			completed_at INTEGER NOT NULL DEFAULT 0
		);
	`},
}

// --- model structs ---

type BackupPlan struct {
	ID        string
	ARN       string
	Name      string
	VersionID string
	Rules     string
	CreatedAt time.Time
}

type BackupVault struct {
	Name          string
	ARN           string
	EncryptionKey string
	Notifications string
	AccessPolicy  string
	LockConfig    string
	CreatedAt     time.Time
}

type BackupSelection struct {
	ID         string
	PlanID     string
	Name       string
	IAMRole    string
	Resources  string
	Conditions string
	CreatedAt  time.Time
}

type BackupJob struct {
	ID           string
	VaultName    string
	ResourceARN  string
	ResourceType string
	Status       string
	PlanID       string
	CreatedAt    time.Time
	CompletedAt  time.Time
}

type RecoveryPoint struct {
	ARN          string
	VaultName    string
	ResourceARN  string
	ResourceType string
	Status       string
	CreatedAt    time.Time
}

type RestoreJob struct {
	ID            string
	VaultName     string
	RecoveryPoint string
	ResourceType  string
	Status        string
	CreatedAt     time.Time
	CompletedAt   time.Time
}

type Framework struct {
	Name        string
	ARN         string
	Description string
	Controls    string
	Status      string
	CreatedAt   time.Time
}

type ReportPlan struct {
	Name            string
	ARN             string
	Description     string
	DeliveryChannel string
	ReportSetting   string
	CreatedAt       time.Time
}

type RestoreTestingPlan struct {
	Name        string
	ARN         string
	Schedule    string
	StartWindow int64
	Config      string
	CreatedAt   time.Time
}

type RestoreTestingSelection struct {
	Name         string
	PlanName     string
	IAMRole      string
	ResourceType string
	Config       string
	CreatedAt    time.Time
}

type ReportJob struct {
	ID          string
	PlanName    string
	Status      string
	CreatedAt   time.Time
	CompletedAt time.Time
}

// --- Store ---

type Store struct {
	store *sqlite.Store
	tags  *shared.TagStore
}

func NewStore(dataDir string) (*Store, error) {
	allMigrations := append(migrations, shared.TagMigrations...)
	dbPath := filepath.Join(dataDir, "backup.db")
	s, err := sqlite.Open(dbPath, allMigrations)
	if err != nil {
		return nil, err
	}
	return &Store{store: s, tags: shared.NewTagStore(s)}, nil
}

func (s *Store) Close() error { return s.store.Close() }

// --- BackupPlan ---

func (s *Store) CreateBackupPlan(p *BackupPlan) error {
	now := time.Now().Unix()
	_, err := s.store.DB().Exec(
		`INSERT INTO backup_plans (id, arn, name, version_id, rules, created_at) VALUES (?, ?, ?, ?, ?, ?)`,
		p.ID, p.ARN, p.Name, p.VersionID, p.Rules, now,
	)
	return err
}

func (s *Store) GetBackupPlan(id string) (*BackupPlan, error) {
	row := s.store.DB().QueryRow(
		`SELECT id, arn, name, version_id, rules, created_at FROM backup_plans WHERE id = ?`, id)
	return scanBackupPlan(row)
}

func (s *Store) ListBackupPlans() ([]BackupPlan, error) {
	rows, err := s.store.DB().Query(
		`SELECT id, arn, name, version_id, rules, created_at FROM backup_plans ORDER BY created_at`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var plans []BackupPlan
	for rows.Next() {
		p, err := scanBackupPlan(rows)
		if err != nil {
			return nil, err
		}
		plans = append(plans, *p)
	}
	return plans, rows.Err()
}

func (s *Store) UpdateBackupPlan(id string, name, rules, versionID string) error {
	res, err := s.store.DB().Exec(
		`UPDATE backup_plans SET name=?, rules=?, version_id=? WHERE id=?`,
		name, rules, versionID, id,
	)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errNotFound
	}
	return nil
}

func (s *Store) DeleteBackupPlan(id string) error {
	res, err := s.store.DB().Exec(`DELETE FROM backup_plans WHERE id = ?`, id)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errNotFound
	}
	return nil
}

type scanner interface{ Scan(dest ...any) error }

func scanBackupPlan(sc scanner) (*BackupPlan, error) {
	var p BackupPlan
	var createdAt int64
	err := sc.Scan(&p.ID, &p.ARN, &p.Name, &p.VersionID, &p.Rules, &createdAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errNotFound
		}
		return nil, err
	}
	p.CreatedAt = time.Unix(createdAt, 0)
	return &p, nil
}

// --- BackupVault ---

func (s *Store) CreateBackupVault(v *BackupVault) error {
	now := time.Now().Unix()
	_, err := s.store.DB().Exec(
		`INSERT INTO backup_vaults (name, arn, encryption_key, notifications, access_policy, lock_config, created_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?)`,
		v.Name, v.ARN, v.EncryptionKey, v.Notifications, v.AccessPolicy, v.LockConfig, now,
	)
	return err
}

func (s *Store) GetBackupVault(name string) (*BackupVault, error) {
	row := s.store.DB().QueryRow(
		`SELECT name, arn, encryption_key, notifications, access_policy, lock_config, created_at
		 FROM backup_vaults WHERE name = ?`, name)
	return scanBackupVault(row)
}

func (s *Store) ListBackupVaults() ([]BackupVault, error) {
	rows, err := s.store.DB().Query(
		`SELECT name, arn, encryption_key, notifications, access_policy, lock_config, created_at
		 FROM backup_vaults ORDER BY created_at`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var vaults []BackupVault
	for rows.Next() {
		v, err := scanBackupVault(rows)
		if err != nil {
			return nil, err
		}
		vaults = append(vaults, *v)
	}
	return vaults, rows.Err()
}

func (s *Store) DeleteBackupVault(name string) error {
	res, err := s.store.DB().Exec(`DELETE FROM backup_vaults WHERE name = ?`, name)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errNotFound
	}
	return nil
}

func (s *Store) UpdateBackupVaultAccessPolicy(name, policy string) error {
	res, err := s.store.DB().Exec(`UPDATE backup_vaults SET access_policy=? WHERE name=?`, policy, name)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errNotFound
	}
	return nil
}

func (s *Store) UpdateBackupVaultNotifications(name, notifications string) error {
	res, err := s.store.DB().Exec(`UPDATE backup_vaults SET notifications=? WHERE name=?`, notifications, name)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errNotFound
	}
	return nil
}

func (s *Store) UpdateBackupVaultLockConfig(name, lockConfig string) error {
	res, err := s.store.DB().Exec(`UPDATE backup_vaults SET lock_config=? WHERE name=?`, lockConfig, name)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errNotFound
	}
	return nil
}

func scanBackupVault(sc scanner) (*BackupVault, error) {
	var v BackupVault
	var createdAt int64
	err := sc.Scan(&v.Name, &v.ARN, &v.EncryptionKey, &v.Notifications, &v.AccessPolicy, &v.LockConfig, &createdAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errNotFound
		}
		return nil, err
	}
	v.CreatedAt = time.Unix(createdAt, 0)
	return &v, nil
}

// --- BackupSelection ---

func (s *Store) CreateBackupSelection(sel *BackupSelection) error {
	now := time.Now().Unix()
	_, err := s.store.DB().Exec(
		`INSERT INTO backup_selections (id, plan_id, name, iam_role, resources, conditions, created_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?)`,
		sel.ID, sel.PlanID, sel.Name, sel.IAMRole, sel.Resources, sel.Conditions, now,
	)
	return err
}

func (s *Store) GetBackupSelection(id string) (*BackupSelection, error) {
	row := s.store.DB().QueryRow(
		`SELECT id, plan_id, name, iam_role, resources, conditions, created_at
		 FROM backup_selections WHERE id = ?`, id)
	return scanBackupSelection(row)
}

func (s *Store) ListBackupSelections(planID string) ([]BackupSelection, error) {
	rows, err := s.store.DB().Query(
		`SELECT id, plan_id, name, iam_role, resources, conditions, created_at
		 FROM backup_selections WHERE plan_id = ? ORDER BY created_at`, planID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var sels []BackupSelection
	for rows.Next() {
		sel, err := scanBackupSelection(rows)
		if err != nil {
			return nil, err
		}
		sels = append(sels, *sel)
	}
	return sels, rows.Err()
}

func (s *Store) DeleteBackupSelection(id string) error {
	res, err := s.store.DB().Exec(`DELETE FROM backup_selections WHERE id = ?`, id)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errNotFound
	}
	return nil
}

func scanBackupSelection(sc scanner) (*BackupSelection, error) {
	var sel BackupSelection
	var createdAt int64
	err := sc.Scan(&sel.ID, &sel.PlanID, &sel.Name, &sel.IAMRole, &sel.Resources, &sel.Conditions, &createdAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errNotFound
		}
		return nil, err
	}
	sel.CreatedAt = time.Unix(createdAt, 0)
	return &sel, nil
}

// --- BackupJob ---

func (s *Store) CreateBackupJob(j *BackupJob) error {
	now := time.Now().Unix()
	_, err := s.store.DB().Exec(
		`INSERT INTO backup_jobs (id, vault_name, resource_arn, resource_type, status, plan_id, created_at, completed_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		j.ID, j.VaultName, j.ResourceARN, j.ResourceType, j.Status, j.PlanID, now, now,
	)
	return err
}

func (s *Store) GetBackupJob(id string) (*BackupJob, error) {
	row := s.store.DB().QueryRow(
		`SELECT id, vault_name, resource_arn, resource_type, status, plan_id, created_at, completed_at
		 FROM backup_jobs WHERE id = ?`, id)
	return scanBackupJob(row)
}

func (s *Store) ListBackupJobs() ([]BackupJob, error) {
	rows, err := s.store.DB().Query(
		`SELECT id, vault_name, resource_arn, resource_type, status, plan_id, created_at, completed_at
		 FROM backup_jobs ORDER BY created_at DESC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var jobs []BackupJob
	for rows.Next() {
		j, err := scanBackupJob(rows)
		if err != nil {
			return nil, err
		}
		jobs = append(jobs, *j)
	}
	return jobs, rows.Err()
}

func (s *Store) UpdateBackupJobStatus(id, status string) error {
	res, err := s.store.DB().Exec(`UPDATE backup_jobs SET status=? WHERE id=?`, status, id)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errNotFound
	}
	return nil
}

func scanBackupJob(sc scanner) (*BackupJob, error) {
	var j BackupJob
	var createdAt, completedAt int64
	err := sc.Scan(&j.ID, &j.VaultName, &j.ResourceARN, &j.ResourceType, &j.Status, &j.PlanID, &createdAt, &completedAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errNotFound
		}
		return nil, err
	}
	j.CreatedAt = time.Unix(createdAt, 0)
	j.CompletedAt = time.Unix(completedAt, 0)
	return &j, nil
}

// --- RecoveryPoint ---

func (s *Store) CreateRecoveryPoint(rp *RecoveryPoint) error {
	now := time.Now().Unix()
	_, err := s.store.DB().Exec(
		`INSERT INTO recovery_points (arn, vault_name, resource_arn, resource_type, status, created_at)
		 VALUES (?, ?, ?, ?, ?, ?)`,
		rp.ARN, rp.VaultName, rp.ResourceARN, rp.ResourceType, rp.Status, now,
	)
	return err
}

func (s *Store) GetRecoveryPoint(arn string) (*RecoveryPoint, error) {
	row := s.store.DB().QueryRow(
		`SELECT arn, vault_name, resource_arn, resource_type, status, created_at
		 FROM recovery_points WHERE arn = ?`, arn)
	return scanRecoveryPoint(row)
}

func (s *Store) ListRecoveryPointsByVault(vaultName string) ([]RecoveryPoint, error) {
	rows, err := s.store.DB().Query(
		`SELECT arn, vault_name, resource_arn, resource_type, status, created_at
		 FROM recovery_points WHERE vault_name = ? ORDER BY created_at DESC`, vaultName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var rps []RecoveryPoint
	for rows.Next() {
		rp, err := scanRecoveryPoint(rows)
		if err != nil {
			return nil, err
		}
		rps = append(rps, *rp)
	}
	return rps, rows.Err()
}

func (s *Store) ListRecoveryPointsByResource(resourceARN string) ([]RecoveryPoint, error) {
	rows, err := s.store.DB().Query(
		`SELECT arn, vault_name, resource_arn, resource_type, status, created_at
		 FROM recovery_points WHERE resource_arn = ? ORDER BY created_at DESC`, resourceARN)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var rps []RecoveryPoint
	for rows.Next() {
		rp, err := scanRecoveryPoint(rows)
		if err != nil {
			return nil, err
		}
		rps = append(rps, *rp)
	}
	return rps, rows.Err()
}

func (s *Store) DeleteRecoveryPoint(arn string) error {
	res, err := s.store.DB().Exec(`DELETE FROM recovery_points WHERE arn = ?`, arn)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errNotFound
	}
	return nil
}

func scanRecoveryPoint(sc scanner) (*RecoveryPoint, error) {
	var rp RecoveryPoint
	var createdAt int64
	err := sc.Scan(&rp.ARN, &rp.VaultName, &rp.ResourceARN, &rp.ResourceType, &rp.Status, &createdAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errNotFound
		}
		return nil, err
	}
	rp.CreatedAt = time.Unix(createdAt, 0)
	return &rp, nil
}

// --- RestoreJob ---

func (s *Store) CreateRestoreJob(j *RestoreJob) error {
	now := time.Now().Unix()
	_, err := s.store.DB().Exec(
		`INSERT INTO restore_jobs (id, vault_name, recovery_point, resource_type, status, created_at, completed_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?)`,
		j.ID, j.VaultName, j.RecoveryPoint, j.ResourceType, j.Status, now, now,
	)
	return err
}

func (s *Store) GetRestoreJob(id string) (*RestoreJob, error) {
	row := s.store.DB().QueryRow(
		`SELECT id, vault_name, recovery_point, resource_type, status, created_at, completed_at
		 FROM restore_jobs WHERE id = ?`, id)
	return scanRestoreJob(row)
}

func (s *Store) ListRestoreJobs() ([]RestoreJob, error) {
	rows, err := s.store.DB().Query(
		`SELECT id, vault_name, recovery_point, resource_type, status, created_at, completed_at
		 FROM restore_jobs ORDER BY created_at DESC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var jobs []RestoreJob
	for rows.Next() {
		j, err := scanRestoreJob(rows)
		if err != nil {
			return nil, err
		}
		jobs = append(jobs, *j)
	}
	return jobs, rows.Err()
}

func scanRestoreJob(sc scanner) (*RestoreJob, error) {
	var j RestoreJob
	var createdAt, completedAt int64
	err := sc.Scan(&j.ID, &j.VaultName, &j.RecoveryPoint, &j.ResourceType, &j.Status, &createdAt, &completedAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errNotFound
		}
		return nil, err
	}
	j.CreatedAt = time.Unix(createdAt, 0)
	j.CompletedAt = time.Unix(completedAt, 0)
	return &j, nil
}

// --- Framework ---

func (s *Store) CreateFramework(f *Framework) error {
	now := time.Now().Unix()
	_, err := s.store.DB().Exec(
		`INSERT INTO frameworks (name, arn, description, controls, status, created_at)
		 VALUES (?, ?, ?, ?, ?, ?)`,
		f.Name, f.ARN, f.Description, f.Controls, f.Status, now,
	)
	return err
}

func (s *Store) GetFramework(name string) (*Framework, error) {
	row := s.store.DB().QueryRow(
		`SELECT name, arn, description, controls, status, created_at
		 FROM frameworks WHERE name = ?`, name)
	return scanFramework(row)
}

func (s *Store) ListFrameworks() ([]Framework, error) {
	rows, err := s.store.DB().Query(
		`SELECT name, arn, description, controls, status, created_at
		 FROM frameworks ORDER BY created_at`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var fws []Framework
	for rows.Next() {
		f, err := scanFramework(rows)
		if err != nil {
			return nil, err
		}
		fws = append(fws, *f)
	}
	return fws, rows.Err()
}

func (s *Store) UpdateFramework(name, description, controls, status string) error {
	res, err := s.store.DB().Exec(
		`UPDATE frameworks SET description=?, controls=?, status=? WHERE name=?`,
		description, controls, status, name,
	)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errNotFound
	}
	return nil
}

func (s *Store) DeleteFramework(name string) error {
	res, err := s.store.DB().Exec(`DELETE FROM frameworks WHERE name = ?`, name)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errNotFound
	}
	return nil
}

func scanFramework(sc scanner) (*Framework, error) {
	var f Framework
	var createdAt int64
	err := sc.Scan(&f.Name, &f.ARN, &f.Description, &f.Controls, &f.Status, &createdAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errNotFound
		}
		return nil, err
	}
	f.CreatedAt = time.Unix(createdAt, 0)
	return &f, nil
}

// --- ReportPlan ---

func (s *Store) CreateReportPlan(rp *ReportPlan) error {
	now := time.Now().Unix()
	_, err := s.store.DB().Exec(
		`INSERT INTO report_plans (name, arn, description, delivery_channel, report_setting, created_at)
		 VALUES (?, ?, ?, ?, ?, ?)`,
		rp.Name, rp.ARN, rp.Description, rp.DeliveryChannel, rp.ReportSetting, now,
	)
	return err
}

func (s *Store) GetReportPlan(name string) (*ReportPlan, error) {
	row := s.store.DB().QueryRow(
		`SELECT name, arn, description, delivery_channel, report_setting, created_at
		 FROM report_plans WHERE name = ?`, name)
	return scanReportPlan(row)
}

func (s *Store) ListReportPlans() ([]ReportPlan, error) {
	rows, err := s.store.DB().Query(
		`SELECT name, arn, description, delivery_channel, report_setting, created_at
		 FROM report_plans ORDER BY created_at`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var rps []ReportPlan
	for rows.Next() {
		rp, err := scanReportPlan(rows)
		if err != nil {
			return nil, err
		}
		rps = append(rps, *rp)
	}
	return rps, rows.Err()
}

func (s *Store) UpdateReportPlan(name, description, deliveryChannel, reportSetting string) error {
	res, err := s.store.DB().Exec(
		`UPDATE report_plans SET description=?, delivery_channel=?, report_setting=? WHERE name=?`,
		description, deliveryChannel, reportSetting, name,
	)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errNotFound
	}
	return nil
}

func (s *Store) DeleteReportPlan(name string) error {
	res, err := s.store.DB().Exec(`DELETE FROM report_plans WHERE name = ?`, name)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errNotFound
	}
	return nil
}

func scanReportPlan(sc scanner) (*ReportPlan, error) {
	var rp ReportPlan
	var createdAt int64
	err := sc.Scan(&rp.Name, &rp.ARN, &rp.Description, &rp.DeliveryChannel, &rp.ReportSetting, &createdAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errNotFound
		}
		return nil, err
	}
	rp.CreatedAt = time.Unix(createdAt, 0)
	return &rp, nil
}

// --- ReportJob ---

func (s *Store) CreateReportJob(j *ReportJob) error {
	now := time.Now().Unix()
	_, err := s.store.DB().Exec(
		`INSERT INTO report_jobs (id, plan_name, status, created_at, completed_at) VALUES (?, ?, ?, ?, ?)`,
		j.ID, j.PlanName, j.Status, now, now,
	)
	return err
}

func (s *Store) GetReportJob(id string) (*ReportJob, error) {
	row := s.store.DB().QueryRow(
		`SELECT id, plan_name, status, created_at, completed_at FROM report_jobs WHERE id = ?`, id)
	return scanReportJob(row)
}

func (s *Store) ListReportJobs() ([]ReportJob, error) {
	rows, err := s.store.DB().Query(
		`SELECT id, plan_name, status, created_at, completed_at FROM report_jobs ORDER BY created_at DESC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var jobs []ReportJob
	for rows.Next() {
		j, err := scanReportJob(rows)
		if err != nil {
			return nil, err
		}
		jobs = append(jobs, *j)
	}
	return jobs, rows.Err()
}

func scanReportJob(sc scanner) (*ReportJob, error) {
	var j ReportJob
	var createdAt, completedAt int64
	err := sc.Scan(&j.ID, &j.PlanName, &j.Status, &createdAt, &completedAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errNotFound
		}
		return nil, err
	}
	j.CreatedAt = time.Unix(createdAt, 0)
	j.CompletedAt = time.Unix(completedAt, 0)
	return &j, nil
}

// --- RestoreTestingPlan ---

func (s *Store) CreateRestoreTestingPlan(p *RestoreTestingPlan) error {
	now := time.Now().Unix()
	_, err := s.store.DB().Exec(
		`INSERT INTO restore_testing_plans (name, arn, schedule, start_window, config, created_at)
		 VALUES (?, ?, ?, ?, ?, ?)`,
		p.Name, p.ARN, p.Schedule, p.StartWindow, p.Config, now,
	)
	return err
}

func (s *Store) GetRestoreTestingPlan(name string) (*RestoreTestingPlan, error) {
	row := s.store.DB().QueryRow(
		`SELECT name, arn, schedule, start_window, config, created_at
		 FROM restore_testing_plans WHERE name = ?`, name)
	return scanRestoreTestingPlan(row)
}

func (s *Store) ListRestoreTestingPlans() ([]RestoreTestingPlan, error) {
	rows, err := s.store.DB().Query(
		`SELECT name, arn, schedule, start_window, config, created_at
		 FROM restore_testing_plans ORDER BY created_at`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var plans []RestoreTestingPlan
	for rows.Next() {
		p, err := scanRestoreTestingPlan(rows)
		if err != nil {
			return nil, err
		}
		plans = append(plans, *p)
	}
	return plans, rows.Err()
}

func (s *Store) UpdateRestoreTestingPlan(name, schedule, config string, startWindow int64) error {
	res, err := s.store.DB().Exec(
		`UPDATE restore_testing_plans SET schedule=?, start_window=?, config=? WHERE name=?`,
		schedule, startWindow, config, name,
	)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errNotFound
	}
	return nil
}

func (s *Store) DeleteRestoreTestingPlan(name string) error {
	res, err := s.store.DB().Exec(`DELETE FROM restore_testing_plans WHERE name = ?`, name)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errNotFound
	}
	return nil
}

func scanRestoreTestingPlan(sc scanner) (*RestoreTestingPlan, error) {
	var p RestoreTestingPlan
	var createdAt int64
	err := sc.Scan(&p.Name, &p.ARN, &p.Schedule, &p.StartWindow, &p.Config, &createdAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errNotFound
		}
		return nil, err
	}
	p.CreatedAt = time.Unix(createdAt, 0)
	return &p, nil
}

// --- RestoreTestingSelection ---

func (s *Store) CreateRestoreTestingSelection(sel *RestoreTestingSelection) error {
	now := time.Now().Unix()
	_, err := s.store.DB().Exec(
		`INSERT INTO restore_testing_selections (name, plan_name, iam_role, resource_type, config, created_at)
		 VALUES (?, ?, ?, ?, ?, ?)`,
		sel.Name, sel.PlanName, sel.IAMRole, sel.ResourceType, sel.Config, now,
	)
	return err
}

func (s *Store) GetRestoreTestingSelection(planName, selName string) (*RestoreTestingSelection, error) {
	row := s.store.DB().QueryRow(
		`SELECT name, plan_name, iam_role, resource_type, config, created_at
		 FROM restore_testing_selections WHERE plan_name = ? AND name = ?`, planName, selName)
	return scanRestoreTestingSelection(row)
}

func (s *Store) ListRestoreTestingSelections(planName string) ([]RestoreTestingSelection, error) {
	rows, err := s.store.DB().Query(
		`SELECT name, plan_name, iam_role, resource_type, config, created_at
		 FROM restore_testing_selections WHERE plan_name = ? ORDER BY created_at`, planName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var sels []RestoreTestingSelection
	for rows.Next() {
		sel, err := scanRestoreTestingSelection(rows)
		if err != nil {
			return nil, err
		}
		sels = append(sels, *sel)
	}
	return sels, rows.Err()
}

func (s *Store) UpdateRestoreTestingSelection(planName, selName, iamRole, resourceType, config string) error {
	res, err := s.store.DB().Exec(
		`UPDATE restore_testing_selections SET iam_role=?, resource_type=?, config=? WHERE plan_name=? AND name=?`,
		iamRole, resourceType, config, planName, selName,
	)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errNotFound
	}
	return nil
}

func (s *Store) DeleteRestoreTestingSelection(planName, selName string) error {
	res, err := s.store.DB().Exec(
		`DELETE FROM restore_testing_selections WHERE plan_name = ? AND name = ?`, planName, selName)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errNotFound
	}
	return nil
}

func scanRestoreTestingSelection(sc scanner) (*RestoreTestingSelection, error) {
	var sel RestoreTestingSelection
	var createdAt int64
	err := sc.Scan(&sel.Name, &sel.PlanName, &sel.IAMRole, &sel.ResourceType, &sel.Config, &createdAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errNotFound
		}
		return nil, err
	}
	sel.CreatedAt = time.Unix(createdAt, 0)
	return &sel, nil
}
