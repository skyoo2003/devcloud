// SPDX-License-Identifier: Apache-2.0

// internal/services/appconfig/store.go
package appconfig

import (
	"database/sql"
	"errors"
	"path/filepath"
	"time"

	"github.com/skyoo2003/devcloud/internal/shared"
	"github.com/skyoo2003/devcloud/internal/storage/sqlite"
)

var (
	errApplicationNotFound          = errors.New("application not found")
	errEnvironmentNotFound          = errors.New("environment not found")
	errConfigProfileNotFound        = errors.New("configuration profile not found")
	errDeploymentStrategyNotFound   = errors.New("deployment strategy not found")
	errDeploymentNotFound           = errors.New("deployment not found")
	errExtensionNotFound            = errors.New("extension not found")
	errExtensionAssociationNotFound = errors.New("extension association not found")
	errHostedConfigVersionNotFound  = errors.New("hosted configuration version not found")
)

var migrations = []sqlite.Migration{
	{Version: 1, SQL: `
        CREATE TABLE IF NOT EXISTS applications (
            id          TEXT PRIMARY KEY,
            arn         TEXT NOT NULL UNIQUE,
            name        TEXT NOT NULL,
            description TEXT NOT NULL DEFAULT '',
            created_at  INTEGER NOT NULL
        );
        CREATE TABLE IF NOT EXISTS environments (
            id          TEXT PRIMARY KEY,
            app_id      TEXT NOT NULL,
            arn         TEXT NOT NULL UNIQUE,
            name        TEXT NOT NULL,
            description TEXT NOT NULL DEFAULT '',
            state       TEXT NOT NULL DEFAULT 'READY_FOR_DEPLOYMENT',
            created_at  INTEGER NOT NULL
        );
        CREATE TABLE IF NOT EXISTS config_profiles (
            id           TEXT PRIMARY KEY,
            app_id       TEXT NOT NULL,
            arn          TEXT NOT NULL UNIQUE,
            name         TEXT NOT NULL,
            location_uri TEXT NOT NULL DEFAULT 'hosted',
            type         TEXT NOT NULL DEFAULT 'AWS.Freeform',
            created_at   INTEGER NOT NULL
        );
        CREATE TABLE IF NOT EXISTS deployment_strategies (
            id                  TEXT PRIMARY KEY,
            arn                 TEXT NOT NULL UNIQUE,
            name                TEXT NOT NULL,
            growth_type         TEXT NOT NULL DEFAULT 'LINEAR',
            growth_factor       REAL NOT NULL DEFAULT 10,
            deployment_duration INTEGER NOT NULL DEFAULT 0,
            final_bake          INTEGER NOT NULL DEFAULT 0,
            replicate_to        TEXT NOT NULL DEFAULT 'NONE'
        );
        CREATE TABLE IF NOT EXISTS deployments (
            app_id          TEXT NOT NULL,
            env_id          TEXT NOT NULL,
            number          INTEGER NOT NULL,
            config_profile  TEXT NOT NULL DEFAULT '',
            config_version  TEXT NOT NULL DEFAULT '',
            strategy        TEXT NOT NULL DEFAULT '',
            state           TEXT NOT NULL DEFAULT 'COMPLETE',
            started_at      INTEGER NOT NULL,
            completed_at    INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY (app_id, env_id, number)
        );
        CREATE TABLE IF NOT EXISTS extensions (
            id          TEXT PRIMARY KEY,
            arn         TEXT NOT NULL UNIQUE,
            name        TEXT NOT NULL,
            description TEXT NOT NULL DEFAULT '',
            actions     TEXT NOT NULL DEFAULT '{}',
            parameters  TEXT NOT NULL DEFAULT '{}',
            version     INTEGER NOT NULL DEFAULT 1
        );
        CREATE TABLE IF NOT EXISTS extension_associations (
            id                      TEXT PRIMARY KEY,
            arn                     TEXT NOT NULL UNIQUE,
            extension_id            TEXT NOT NULL,
            extension_arn           TEXT NOT NULL DEFAULT '',
            extension_version       INTEGER NOT NULL DEFAULT 1,
            resource_arn            TEXT NOT NULL DEFAULT '',
            parameters              TEXT NOT NULL DEFAULT '{}'
        );
        CREATE TABLE IF NOT EXISTS hosted_config_versions (
            app_id       TEXT NOT NULL,
            profile_id   TEXT NOT NULL,
            version      INTEGER NOT NULL,
            content      TEXT NOT NULL DEFAULT '',
            content_type TEXT NOT NULL DEFAULT 'application/json',
            description  TEXT NOT NULL DEFAULT '',
            PRIMARY KEY (app_id, profile_id, version)
        );
    `},
}

// --- Model structs ---

type Application struct {
	ID          string
	ARN         string
	Name        string
	Description string
	CreatedAt   time.Time
}

type Environment struct {
	ID          string
	AppID       string
	ARN         string
	Name        string
	Description string
	State       string
	CreatedAt   time.Time
}

type ConfigProfile struct {
	ID          string
	AppID       string
	ARN         string
	Name        string
	LocationURI string
	Type        string
	CreatedAt   time.Time
}

type DeploymentStrategy struct {
	ID                 string
	ARN                string
	Name               string
	GrowthType         string
	GrowthFactor       float64
	DeploymentDuration int
	FinalBake          int
	ReplicateTo        string
}

type Deployment struct {
	AppID         string
	EnvID         string
	Number        int
	ConfigProfile string
	ConfigVersion string
	Strategy      string
	State         string
	StartedAt     time.Time
	CompletedAt   time.Time
}

type Extension struct {
	ID          string
	ARN         string
	Name        string
	Description string
	Actions     string
	Parameters  string
	Version     int
}

type ExtensionAssociation struct {
	ID               string
	ARN              string
	ExtensionID      string
	ExtensionARN     string
	ExtensionVersion int
	ResourceARN      string
	Parameters       string
}

type HostedConfigVersion struct {
	AppID       string
	ProfileID   string
	Version     int
	Content     string
	ContentType string
	Description string
}

// --- Store ---

type Store struct {
	store *sqlite.Store
	tags  *shared.TagStore
}

func NewStore(dataDir string) (*Store, error) {
	allMigrations := append(migrations, shared.TagMigrations...)
	dbPath := filepath.Join(dataDir, "appconfig.db")
	s, err := sqlite.Open(dbPath, allMigrations)
	if err != nil {
		return nil, err
	}
	return &Store{store: s, tags: shared.NewTagStore(s)}, nil
}

func (s *Store) Close() error { return s.store.Close() }

// --- Applications ---

func (s *Store) CreateApplication(a *Application) error {
	now := time.Now().Unix()
	_, err := s.store.DB().Exec(
		`INSERT INTO applications (id, arn, name, description, created_at) VALUES (?, ?, ?, ?, ?)`,
		a.ID, a.ARN, a.Name, a.Description, now,
	)
	return err
}

func (s *Store) GetApplication(id string) (*Application, error) {
	row := s.store.DB().QueryRow(
		`SELECT id, arn, name, description, created_at FROM applications WHERE id = ?`, id)
	return scanApplication(row)
}

func (s *Store) FindApplicationByName(name string) (*Application, error) {
	row := s.store.DB().QueryRow(
		`SELECT id, arn, name, description, created_at FROM applications WHERE name = ?`, name)
	return scanApplication(row)
}

func (s *Store) ListApplications() ([]Application, error) {
	rows, err := s.store.DB().Query(
		`SELECT id, arn, name, description, created_at FROM applications ORDER BY created_at`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var apps []Application
	for rows.Next() {
		a, err := scanApplication(rows)
		if err != nil {
			return nil, err
		}
		apps = append(apps, *a)
	}
	return apps, rows.Err()
}

func (s *Store) UpdateApplication(id, name, description string) error {
	res, err := s.store.DB().Exec(
		`UPDATE applications SET name=?, description=? WHERE id=?`, name, description, id)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errApplicationNotFound
	}
	return nil
}

func (s *Store) DeleteApplication(id string) error {
	res, err := s.store.DB().Exec(`DELETE FROM applications WHERE id = ?`, id)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errApplicationNotFound
	}
	return nil
}

// --- Environments ---

func (s *Store) CreateEnvironment(e *Environment) error {
	now := time.Now().Unix()
	_, err := s.store.DB().Exec(
		`INSERT INTO environments (id, app_id, arn, name, description, state, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)`,
		e.ID, e.AppID, e.ARN, e.Name, e.Description, e.State, now,
	)
	return err
}

func (s *Store) GetEnvironment(appID, envID string) (*Environment, error) {
	row := s.store.DB().QueryRow(
		`SELECT id, app_id, arn, name, description, state, created_at FROM environments WHERE app_id=? AND id=?`,
		appID, envID)
	return scanEnvironment(row)
}

func (s *Store) ListEnvironments(appID string) ([]Environment, error) {
	rows, err := s.store.DB().Query(
		`SELECT id, app_id, arn, name, description, state, created_at FROM environments WHERE app_id=? ORDER BY created_at`,
		appID)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var envs []Environment
	for rows.Next() {
		e, err := scanEnvironment(rows)
		if err != nil {
			return nil, err
		}
		envs = append(envs, *e)
	}
	return envs, rows.Err()
}

func (s *Store) UpdateEnvironment(appID, envID, name, description string) error {
	res, err := s.store.DB().Exec(
		`UPDATE environments SET name=?, description=? WHERE app_id=? AND id=?`,
		name, description, appID, envID)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errEnvironmentNotFound
	}
	return nil
}

func (s *Store) DeleteEnvironment(appID, envID string) error {
	res, err := s.store.DB().Exec(`DELETE FROM environments WHERE app_id=? AND id=?`, appID, envID)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errEnvironmentNotFound
	}
	return nil
}

// --- Config Profiles ---

func (s *Store) CreateConfigProfile(cp *ConfigProfile) error {
	now := time.Now().Unix()
	_, err := s.store.DB().Exec(
		`INSERT INTO config_profiles (id, app_id, arn, name, location_uri, type, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)`,
		cp.ID, cp.AppID, cp.ARN, cp.Name, cp.LocationURI, cp.Type, now,
	)
	return err
}

func (s *Store) GetConfigProfile(appID, profileID string) (*ConfigProfile, error) {
	row := s.store.DB().QueryRow(
		`SELECT id, app_id, arn, name, location_uri, type, created_at FROM config_profiles WHERE app_id=? AND id=?`,
		appID, profileID)
	return scanConfigProfile(row)
}

func (s *Store) ListConfigProfiles(appID string) ([]ConfigProfile, error) {
	rows, err := s.store.DB().Query(
		`SELECT id, app_id, arn, name, location_uri, type, created_at FROM config_profiles WHERE app_id=? ORDER BY created_at`,
		appID)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var profiles []ConfigProfile
	for rows.Next() {
		cp, err := scanConfigProfile(rows)
		if err != nil {
			return nil, err
		}
		profiles = append(profiles, *cp)
	}
	return profiles, rows.Err()
}

func (s *Store) UpdateConfigProfile(appID, profileID, name string) error {
	res, err := s.store.DB().Exec(
		`UPDATE config_profiles SET name=? WHERE app_id=? AND id=?`, name, appID, profileID)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errConfigProfileNotFound
	}
	return nil
}

func (s *Store) DeleteConfigProfile(appID, profileID string) error {
	res, err := s.store.DB().Exec(`DELETE FROM config_profiles WHERE app_id=? AND id=?`, appID, profileID)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errConfigProfileNotFound
	}
	return nil
}

// --- Deployment Strategies ---

func (s *Store) CreateDeploymentStrategy(ds *DeploymentStrategy) error {
	_, err := s.store.DB().Exec(
		`INSERT INTO deployment_strategies (id, arn, name, growth_type, growth_factor, deployment_duration, final_bake, replicate_to)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		ds.ID, ds.ARN, ds.Name, ds.GrowthType, ds.GrowthFactor, ds.DeploymentDuration, ds.FinalBake, ds.ReplicateTo,
	)
	return err
}

func (s *Store) GetDeploymentStrategy(id string) (*DeploymentStrategy, error) {
	row := s.store.DB().QueryRow(
		`SELECT id, arn, name, growth_type, growth_factor, deployment_duration, final_bake, replicate_to
         FROM deployment_strategies WHERE id=?`, id)
	return scanDeploymentStrategy(row)
}

func (s *Store) FindDeploymentStrategyByName(name string) (*DeploymentStrategy, error) {
	row := s.store.DB().QueryRow(
		`SELECT id, arn, name, growth_type, growth_factor, deployment_duration, final_bake, replicate_to
         FROM deployment_strategies WHERE name=?`, name)
	return scanDeploymentStrategy(row)
}

func (s *Store) ListDeploymentStrategies() ([]DeploymentStrategy, error) {
	rows, err := s.store.DB().Query(
		`SELECT id, arn, name, growth_type, growth_factor, deployment_duration, final_bake, replicate_to
         FROM deployment_strategies ORDER BY name`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var strategies []DeploymentStrategy
	for rows.Next() {
		ds, err := scanDeploymentStrategy(rows)
		if err != nil {
			return nil, err
		}
		strategies = append(strategies, *ds)
	}
	return strategies, rows.Err()
}

func (s *Store) UpdateDeploymentStrategy(id string, growthType string, growthFactor float64, deploymentDuration, finalBake int) error {
	res, err := s.store.DB().Exec(
		`UPDATE deployment_strategies SET growth_type=?, growth_factor=?, deployment_duration=?, final_bake=? WHERE id=?`,
		growthType, growthFactor, deploymentDuration, finalBake, id)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errDeploymentStrategyNotFound
	}
	return nil
}

func (s *Store) DeleteDeploymentStrategy(id string) error {
	res, err := s.store.DB().Exec(`DELETE FROM deployment_strategies WHERE id=?`, id)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errDeploymentStrategyNotFound
	}
	return nil
}

// --- Deployments ---

func (s *Store) CreateDeployment(d *Deployment) error {
	// Find next deployment number
	var maxNum sql.NullInt64
	_ = s.store.DB().QueryRow(
		`SELECT MAX(number) FROM deployments WHERE app_id=? AND env_id=?`, d.AppID, d.EnvID).Scan(&maxNum)
	if maxNum.Valid {
		d.Number = int(maxNum.Int64) + 1
	} else {
		d.Number = 1
	}
	now := time.Now().Unix()
	_, err := s.store.DB().Exec(
		`INSERT INTO deployments (app_id, env_id, number, config_profile, config_version, strategy, state, started_at, completed_at)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		d.AppID, d.EnvID, d.Number, d.ConfigProfile, d.ConfigVersion, d.Strategy, d.State, now, now,
	)
	return err
}

func (s *Store) GetDeployment(appID, envID string, number int) (*Deployment, error) {
	row := s.store.DB().QueryRow(
		`SELECT app_id, env_id, number, config_profile, config_version, strategy, state, started_at, completed_at
         FROM deployments WHERE app_id=? AND env_id=? AND number=?`, appID, envID, number)
	return scanDeployment(row)
}

func (s *Store) GetLatestDeployment(appID, envID string) (*Deployment, error) {
	row := s.store.DB().QueryRow(
		`SELECT app_id, env_id, number, config_profile, config_version, strategy, state, started_at, completed_at
         FROM deployments WHERE app_id=? AND env_id=? ORDER BY number DESC LIMIT 1`, appID, envID)
	return scanDeployment(row)
}

func (s *Store) ListDeployments(appID, envID string) ([]Deployment, error) {
	rows, err := s.store.DB().Query(
		`SELECT app_id, env_id, number, config_profile, config_version, strategy, state, started_at, completed_at
         FROM deployments WHERE app_id=? AND env_id=? ORDER BY number DESC`, appID, envID)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var deployments []Deployment
	for rows.Next() {
		d, err := scanDeployment(rows)
		if err != nil {
			return nil, err
		}
		deployments = append(deployments, *d)
	}
	return deployments, rows.Err()
}

func (s *Store) UpdateDeploymentState(appID, envID string, number int, state string) error {
	res, err := s.store.DB().Exec(
		`UPDATE deployments SET state=? WHERE app_id=? AND env_id=? AND number=?`,
		state, appID, envID, number)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errDeploymentNotFound
	}
	return nil
}

// --- Extensions ---

func (s *Store) CreateExtension(e *Extension) error {
	_, err := s.store.DB().Exec(
		`INSERT INTO extensions (id, arn, name, description, actions, parameters, version) VALUES (?, ?, ?, ?, ?, ?, ?)`,
		e.ID, e.ARN, e.Name, e.Description, e.Actions, e.Parameters, e.Version,
	)
	return err
}

func (s *Store) GetExtension(id string) (*Extension, error) {
	row := s.store.DB().QueryRow(
		`SELECT id, arn, name, description, actions, parameters, version FROM extensions WHERE id=?`, id)
	return scanExtension(row)
}

func (s *Store) FindExtensionByName(name string) (*Extension, error) {
	row := s.store.DB().QueryRow(
		`SELECT id, arn, name, description, actions, parameters, version FROM extensions WHERE name=?`, name)
	return scanExtension(row)
}

func (s *Store) ListExtensions() ([]Extension, error) {
	rows, err := s.store.DB().Query(
		`SELECT id, arn, name, description, actions, parameters, version FROM extensions ORDER BY name`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var exts []Extension
	for rows.Next() {
		e, err := scanExtension(rows)
		if err != nil {
			return nil, err
		}
		exts = append(exts, *e)
	}
	return exts, rows.Err()
}

func (s *Store) UpdateExtension(id, description, actions, parameters string) error {
	res, err := s.store.DB().Exec(
		`UPDATE extensions SET description=?, actions=?, parameters=?, version=version+1 WHERE id=?`,
		description, actions, parameters, id)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errExtensionNotFound
	}
	return nil
}

func (s *Store) DeleteExtension(id string) error {
	res, err := s.store.DB().Exec(`DELETE FROM extensions WHERE id=?`, id)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errExtensionNotFound
	}
	return nil
}

// --- Extension Associations ---

func (s *Store) CreateExtensionAssociation(ea *ExtensionAssociation) error {
	_, err := s.store.DB().Exec(
		`INSERT INTO extension_associations (id, arn, extension_id, extension_arn, extension_version, resource_arn, parameters)
         VALUES (?, ?, ?, ?, ?, ?, ?)`,
		ea.ID, ea.ARN, ea.ExtensionID, ea.ExtensionARN, ea.ExtensionVersion, ea.ResourceARN, ea.Parameters,
	)
	return err
}

func (s *Store) GetExtensionAssociation(id string) (*ExtensionAssociation, error) {
	row := s.store.DB().QueryRow(
		`SELECT id, arn, extension_id, extension_arn, extension_version, resource_arn, parameters
         FROM extension_associations WHERE id=?`, id)
	return scanExtensionAssociation(row)
}

func (s *Store) ListExtensionAssociations() ([]ExtensionAssociation, error) {
	rows, err := s.store.DB().Query(
		`SELECT id, arn, extension_id, extension_arn, extension_version, resource_arn, parameters
         FROM extension_associations ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var assocs []ExtensionAssociation
	for rows.Next() {
		ea, err := scanExtensionAssociation(rows)
		if err != nil {
			return nil, err
		}
		assocs = append(assocs, *ea)
	}
	return assocs, rows.Err()
}

func (s *Store) UpdateExtensionAssociation(id, parameters string) error {
	res, err := s.store.DB().Exec(
		`UPDATE extension_associations SET parameters=? WHERE id=?`, parameters, id)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errExtensionAssociationNotFound
	}
	return nil
}

func (s *Store) DeleteExtensionAssociation(id string) error {
	res, err := s.store.DB().Exec(`DELETE FROM extension_associations WHERE id=?`, id)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errExtensionAssociationNotFound
	}
	return nil
}

// --- Hosted Config Versions ---

func (s *Store) CreateHostedConfigVersion(hcv *HostedConfigVersion) error {
	// Find next version number
	var maxVer sql.NullInt64
	_ = s.store.DB().QueryRow(
		`SELECT MAX(version) FROM hosted_config_versions WHERE app_id=? AND profile_id=?`,
		hcv.AppID, hcv.ProfileID).Scan(&maxVer)
	if maxVer.Valid {
		hcv.Version = int(maxVer.Int64) + 1
	} else {
		hcv.Version = 1
	}
	_, err := s.store.DB().Exec(
		`INSERT INTO hosted_config_versions (app_id, profile_id, version, content, content_type, description)
         VALUES (?, ?, ?, ?, ?, ?)`,
		hcv.AppID, hcv.ProfileID, hcv.Version, hcv.Content, hcv.ContentType, hcv.Description,
	)
	return err
}

func (s *Store) GetHostedConfigVersion(appID, profileID string, version int) (*HostedConfigVersion, error) {
	row := s.store.DB().QueryRow(
		`SELECT app_id, profile_id, version, content, content_type, description
         FROM hosted_config_versions WHERE app_id=? AND profile_id=? AND version=?`,
		appID, profileID, version)
	return scanHostedConfigVersion(row)
}

func (s *Store) GetLatestHostedConfigVersion(appID, profileID string) (*HostedConfigVersion, error) {
	row := s.store.DB().QueryRow(
		`SELECT app_id, profile_id, version, content, content_type, description
         FROM hosted_config_versions WHERE app_id=? AND profile_id=? ORDER BY version DESC LIMIT 1`,
		appID, profileID)
	return scanHostedConfigVersion(row)
}

func (s *Store) ListHostedConfigVersions(appID, profileID string) ([]HostedConfigVersion, error) {
	rows, err := s.store.DB().Query(
		`SELECT app_id, profile_id, version, content, content_type, description
         FROM hosted_config_versions WHERE app_id=? AND profile_id=? ORDER BY version DESC`,
		appID, profileID)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var versions []HostedConfigVersion
	for rows.Next() {
		hcv, err := scanHostedConfigVersion(rows)
		if err != nil {
			return nil, err
		}
		versions = append(versions, *hcv)
	}
	return versions, rows.Err()
}

func (s *Store) DeleteHostedConfigVersion(appID, profileID string, version int) error {
	res, err := s.store.DB().Exec(
		`DELETE FROM hosted_config_versions WHERE app_id=? AND profile_id=? AND version=?`,
		appID, profileID, version)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errHostedConfigVersionNotFound
	}
	return nil
}

// --- Scan helpers ---

type scanner interface{ Scan(dest ...any) error }

func scanApplication(sc scanner) (*Application, error) {
	var a Application
	var createdAt int64
	err := sc.Scan(&a.ID, &a.ARN, &a.Name, &a.Description, &createdAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errApplicationNotFound
		}
		return nil, err
	}
	a.CreatedAt = time.Unix(createdAt, 0)
	return &a, nil
}

func scanEnvironment(sc scanner) (*Environment, error) {
	var e Environment
	var createdAt int64
	err := sc.Scan(&e.ID, &e.AppID, &e.ARN, &e.Name, &e.Description, &e.State, &createdAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errEnvironmentNotFound
		}
		return nil, err
	}
	e.CreatedAt = time.Unix(createdAt, 0)
	return &e, nil
}

func scanConfigProfile(sc scanner) (*ConfigProfile, error) {
	var cp ConfigProfile
	var createdAt int64
	err := sc.Scan(&cp.ID, &cp.AppID, &cp.ARN, &cp.Name, &cp.LocationURI, &cp.Type, &createdAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errConfigProfileNotFound
		}
		return nil, err
	}
	cp.CreatedAt = time.Unix(createdAt, 0)
	return &cp, nil
}

func scanDeploymentStrategy(sc scanner) (*DeploymentStrategy, error) {
	var ds DeploymentStrategy
	err := sc.Scan(&ds.ID, &ds.ARN, &ds.Name, &ds.GrowthType, &ds.GrowthFactor, &ds.DeploymentDuration, &ds.FinalBake, &ds.ReplicateTo)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errDeploymentStrategyNotFound
		}
		return nil, err
	}
	return &ds, nil
}

func scanDeployment(sc scanner) (*Deployment, error) {
	var d Deployment
	var startedAt, completedAt int64
	err := sc.Scan(&d.AppID, &d.EnvID, &d.Number, &d.ConfigProfile, &d.ConfigVersion, &d.Strategy, &d.State, &startedAt, &completedAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errDeploymentNotFound
		}
		return nil, err
	}
	d.StartedAt = time.Unix(startedAt, 0)
	d.CompletedAt = time.Unix(completedAt, 0)
	return &d, nil
}

func scanExtension(sc scanner) (*Extension, error) {
	var e Extension
	err := sc.Scan(&e.ID, &e.ARN, &e.Name, &e.Description, &e.Actions, &e.Parameters, &e.Version)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errExtensionNotFound
		}
		return nil, err
	}
	return &e, nil
}

func scanExtensionAssociation(sc scanner) (*ExtensionAssociation, error) {
	var ea ExtensionAssociation
	err := sc.Scan(&ea.ID, &ea.ARN, &ea.ExtensionID, &ea.ExtensionARN, &ea.ExtensionVersion, &ea.ResourceARN, &ea.Parameters)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errExtensionAssociationNotFound
		}
		return nil, err
	}
	return &ea, nil
}

func scanHostedConfigVersion(sc scanner) (*HostedConfigVersion, error) {
	var hcv HostedConfigVersion
	err := sc.Scan(&hcv.AppID, &hcv.ProfileID, &hcv.Version, &hcv.Content, &hcv.ContentType, &hcv.Description)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errHostedConfigVersionNotFound
		}
		return nil, err
	}
	return &hcv, nil
}
