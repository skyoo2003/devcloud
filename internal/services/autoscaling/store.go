// SPDX-License-Identifier: Apache-2.0

package autoscaling

import (
	"database/sql"
	"errors"
	"path/filepath"
	"time"

	"github.com/skyoo2003/devcloud/internal/shared"
	"github.com/skyoo2003/devcloud/internal/storage/sqlite"
)

var errASGNotFound = errors.New("auto scaling group not found")
var errLCNotFound = errors.New("launch configuration not found")
var errPolicyNotFound = errors.New("scaling policy not found")
var errScheduledNotFound = errors.New("scheduled action not found")
var errHookNotFound = errors.New("lifecycle hook not found")

var migrations = []sqlite.Migration{
	{Version: 1, SQL: `
		CREATE TABLE IF NOT EXISTS auto_scaling_groups (
			name         TEXT PRIMARY KEY,
			arn          TEXT NOT NULL UNIQUE,
			launch_config TEXT NOT NULL DEFAULT '',
			min_size     INTEGER NOT NULL DEFAULT 0,
			max_size     INTEGER NOT NULL DEFAULT 10,
			desired      INTEGER NOT NULL DEFAULT 1,
			az_list      TEXT NOT NULL DEFAULT '[]',
			vpc_zones    TEXT NOT NULL DEFAULT '',
			health_check TEXT NOT NULL DEFAULT 'EC2',
			target_groups TEXT NOT NULL DEFAULT '[]',
			status       TEXT NOT NULL DEFAULT '',
			created_at   INTEGER NOT NULL
		);
		CREATE TABLE IF NOT EXISTS launch_configs (
			name          TEXT PRIMARY KEY,
			arn           TEXT NOT NULL UNIQUE,
			image_id      TEXT NOT NULL DEFAULT '',
			instance_type TEXT NOT NULL DEFAULT 't3.micro',
			key_name      TEXT NOT NULL DEFAULT '',
			security_groups TEXT NOT NULL DEFAULT '[]',
			created_at    INTEGER NOT NULL
		);
		CREATE TABLE IF NOT EXISTS scaling_policies (
			arn              TEXT PRIMARY KEY,
			name             TEXT NOT NULL,
			asg_name         TEXT NOT NULL,
			policy_type      TEXT NOT NULL DEFAULT 'TargetTrackingScaling',
			adjustment_type  TEXT NOT NULL DEFAULT '',
			scaling_adjustment INTEGER NOT NULL DEFAULT 0,
			config           TEXT NOT NULL DEFAULT '{}'
		);
		CREATE TABLE IF NOT EXISTS scheduled_actions (
			arn        TEXT PRIMARY KEY,
			name       TEXT NOT NULL,
			asg_name   TEXT NOT NULL,
			min_size   INTEGER NOT NULL DEFAULT -1,
			max_size   INTEGER NOT NULL DEFAULT -1,
			desired    INTEGER NOT NULL DEFAULT -1,
			schedule   TEXT NOT NULL DEFAULT '',
			start_time INTEGER NOT NULL DEFAULT 0,
			end_time   INTEGER NOT NULL DEFAULT 0
		);
		CREATE TABLE IF NOT EXISTS lifecycle_hooks (
			name               TEXT NOT NULL,
			asg_name           TEXT NOT NULL,
			transition         TEXT NOT NULL DEFAULT 'autoscaling:EC2_INSTANCE_LAUNCHING',
			heartbeat_timeout  INTEGER NOT NULL DEFAULT 3600,
			default_result     TEXT NOT NULL DEFAULT 'ABANDON',
			role_arn           TEXT NOT NULL DEFAULT '',
			PRIMARY KEY (name, asg_name)
		);
	`},
}

// --- Model types ---

type AutoScalingGroup struct {
	Name         string
	ARN          string
	LaunchConfig string
	MinSize      int
	MaxSize      int
	Desired      int
	AZList       string
	VPCZones     string
	HealthCheck  string
	TargetGroups string
	Status       string
	CreatedAt    time.Time
}

type LaunchConfiguration struct {
	Name           string
	ARN            string
	ImageID        string
	InstanceType   string
	KeyName        string
	SecurityGroups string
	CreatedAt      time.Time
}

type ScalingPolicy struct {
	ARN               string
	Name              string
	ASGName           string
	PolicyType        string
	AdjustmentType    string
	ScalingAdjustment int
	Config            string
}

type ScheduledAction struct {
	ARN       string
	Name      string
	ASGName   string
	MinSize   int
	MaxSize   int
	Desired   int
	Schedule  string
	StartTime int64
	EndTime   int64
}

type LifecycleHook struct {
	Name             string
	ASGName          string
	Transition       string
	HeartbeatTimeout int
	DefaultResult    string
	RoleARN          string
}

// --- Store ---

type Store struct {
	store *sqlite.Store
	tags  *shared.TagStore
}

func NewStore(dataDir string) (*Store, error) {
	allMigrations := append(migrations, shared.TagMigrations...)
	dbPath := filepath.Join(dataDir, "autoscaling.db")
	s, err := sqlite.Open(dbPath, allMigrations)
	if err != nil {
		return nil, err
	}
	return &Store{store: s, tags: shared.NewTagStore(s)}, nil
}

func (s *Store) Close() error { return s.store.Close() }

// --- AutoScalingGroup CRUD ---

func (s *Store) CreateASG(name, arn, launchConfig string, minSize, maxSize, desired int,
	azList, vpcZones, healthCheck, targetGroups string) (*AutoScalingGroup, error) {
	now := time.Now().Unix()
	_, err := s.store.DB().Exec(
		`INSERT INTO auto_scaling_groups
		 (name, arn, launch_config, min_size, max_size, desired, az_list, vpc_zones, health_check, target_groups, status, created_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, '', ?)`,
		name, arn, launchConfig, minSize, maxSize, desired, azList, vpcZones, healthCheck, targetGroups, now,
	)
	if err != nil {
		return nil, err
	}
	return &AutoScalingGroup{
		Name: name, ARN: arn, LaunchConfig: launchConfig,
		MinSize: minSize, MaxSize: maxSize, Desired: desired,
		AZList: azList, VPCZones: vpcZones, HealthCheck: healthCheck,
		TargetGroups: targetGroups, Status: "",
		CreatedAt: time.Unix(now, 0),
	}, nil
}

func (s *Store) GetASG(name string) (*AutoScalingGroup, error) {
	row := s.store.DB().QueryRow(
		`SELECT name, arn, launch_config, min_size, max_size, desired, az_list, vpc_zones, health_check, target_groups, status, created_at
		 FROM auto_scaling_groups WHERE name = ?`, name)
	return scanASG(row)
}

func (s *Store) ListASGs(names []string) ([]AutoScalingGroup, error) {
	query := `SELECT name, arn, launch_config, min_size, max_size, desired, az_list, vpc_zones, health_check, target_groups, status, created_at FROM auto_scaling_groups`
	var args []any
	if len(names) > 0 {
		ph := buildPlaceholders(len(names))
		query += " WHERE name IN (" + ph + ")"
		for _, n := range names {
			args = append(args, n)
		}
	}
	query += " ORDER BY name"
	rows, err := s.store.DB().Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []AutoScalingGroup
	for rows.Next() {
		a, err := scanASG(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *a)
	}
	return out, rows.Err()
}

func (s *Store) UpdateASG(name, launchConfig string, minSize, maxSize, desired int, vpcZones, healthCheck string) error {
	res, err := s.store.DB().Exec(
		`UPDATE auto_scaling_groups
		 SET launch_config = CASE WHEN ? != '' THEN ? ELSE launch_config END,
		     min_size = ?,
		     max_size = ?,
		     desired = ?,
		     vpc_zones = CASE WHEN ? != '' THEN ? ELSE vpc_zones END,
		     health_check = CASE WHEN ? != '' THEN ? ELSE health_check END
		 WHERE name = ?`,
		launchConfig, launchConfig, minSize, maxSize, desired,
		vpcZones, vpcZones, healthCheck, healthCheck, name,
	)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errASGNotFound
	}
	return nil
}

func (s *Store) SetDesiredCapacity(name string, desired int) error {
	res, err := s.store.DB().Exec(`UPDATE auto_scaling_groups SET desired = ? WHERE name = ?`, desired, name)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errASGNotFound
	}
	return nil
}

func (s *Store) DeleteASG(name string) (*AutoScalingGroup, error) {
	a, err := s.GetASG(name)
	if err != nil {
		return nil, err
	}
	res, err := s.store.DB().Exec(`DELETE FROM auto_scaling_groups WHERE name = ?`, name)
	if err != nil {
		return nil, err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return nil, errASGNotFound
	}
	return a, nil
}

// --- LaunchConfiguration CRUD ---

func (s *Store) CreateLC(name, arn, imageID, instanceType, keyName, securityGroups string) (*LaunchConfiguration, error) {
	now := time.Now().Unix()
	_, err := s.store.DB().Exec(
		`INSERT INTO launch_configs (name, arn, image_id, instance_type, key_name, security_groups, created_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?)`,
		name, arn, imageID, instanceType, keyName, securityGroups, now,
	)
	if err != nil {
		return nil, err
	}
	return &LaunchConfiguration{
		Name: name, ARN: arn, ImageID: imageID, InstanceType: instanceType,
		KeyName: keyName, SecurityGroups: securityGroups,
		CreatedAt: time.Unix(now, 0),
	}, nil
}

func (s *Store) GetLC(name string) (*LaunchConfiguration, error) {
	row := s.store.DB().QueryRow(
		`SELECT name, arn, image_id, instance_type, key_name, security_groups, created_at
		 FROM launch_configs WHERE name = ?`, name)
	return scanLC(row)
}

func (s *Store) ListLCs(names []string) ([]LaunchConfiguration, error) {
	query := `SELECT name, arn, image_id, instance_type, key_name, security_groups, created_at FROM launch_configs`
	var args []any
	if len(names) > 0 {
		ph := buildPlaceholders(len(names))
		query += " WHERE name IN (" + ph + ")"
		for _, n := range names {
			args = append(args, n)
		}
	}
	query += " ORDER BY name"
	rows, err := s.store.DB().Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []LaunchConfiguration
	for rows.Next() {
		lc, err := scanLC(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *lc)
	}
	return out, rows.Err()
}

func (s *Store) DeleteLC(name string) (*LaunchConfiguration, error) {
	lc, err := s.GetLC(name)
	if err != nil {
		return nil, err
	}
	res, err := s.store.DB().Exec(`DELETE FROM launch_configs WHERE name = ?`, name)
	if err != nil {
		return nil, err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return nil, errLCNotFound
	}
	return lc, nil
}

// --- ScalingPolicy CRUD ---

func (s *Store) PutPolicy(arn, name, asgName, policyType, adjustmentType string, scalingAdj int, config string) (*ScalingPolicy, error) {
	_, err := s.store.DB().Exec(
		`INSERT INTO scaling_policies (arn, name, asg_name, policy_type, adjustment_type, scaling_adjustment, config)
		 VALUES (?, ?, ?, ?, ?, ?, ?)
		 ON CONFLICT(arn) DO UPDATE SET
		   name=excluded.name, asg_name=excluded.asg_name, policy_type=excluded.policy_type,
		   adjustment_type=excluded.adjustment_type, scaling_adjustment=excluded.scaling_adjustment,
		   config=excluded.config`,
		arn, name, asgName, policyType, adjustmentType, scalingAdj, config,
	)
	if err != nil {
		return nil, err
	}
	return &ScalingPolicy{
		ARN: arn, Name: name, ASGName: asgName, PolicyType: policyType,
		AdjustmentType: adjustmentType, ScalingAdjustment: scalingAdj, Config: config,
	}, nil
}

func (s *Store) ListPolicies(asgName, policyName string) ([]ScalingPolicy, error) {
	query := `SELECT arn, name, asg_name, policy_type, adjustment_type, scaling_adjustment, config FROM scaling_policies WHERE 1=1`
	var args []any
	if asgName != "" {
		query += " AND asg_name = ?"
		args = append(args, asgName)
	}
	if policyName != "" {
		query += " AND name = ?"
		args = append(args, policyName)
	}
	query += " ORDER BY name"
	rows, err := s.store.DB().Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []ScalingPolicy
	for rows.Next() {
		var p ScalingPolicy
		if err := rows.Scan(&p.ARN, &p.Name, &p.ASGName, &p.PolicyType, &p.AdjustmentType, &p.ScalingAdjustment, &p.Config); err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

func (s *Store) DeletePolicy(asgName, policyName string) error {
	res, err := s.store.DB().Exec(`DELETE FROM scaling_policies WHERE asg_name = ? AND name = ?`, asgName, policyName)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errPolicyNotFound
	}
	return nil
}

// --- ScheduledAction CRUD ---

func (s *Store) PutScheduledAction(arn, name, asgName string, minSize, maxSize, desired int, schedule string, startTime, endTime int64) (*ScheduledAction, error) {
	_, err := s.store.DB().Exec(
		`INSERT INTO scheduled_actions (arn, name, asg_name, min_size, max_size, desired, schedule, start_time, end_time)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		 ON CONFLICT(arn) DO UPDATE SET
		   name=excluded.name, asg_name=excluded.asg_name, min_size=excluded.min_size,
		   max_size=excluded.max_size, desired=excluded.desired, schedule=excluded.schedule,
		   start_time=excluded.start_time, end_time=excluded.end_time`,
		arn, name, asgName, minSize, maxSize, desired, schedule, startTime, endTime,
	)
	if err != nil {
		return nil, err
	}
	return &ScheduledAction{
		ARN: arn, Name: name, ASGName: asgName,
		MinSize: minSize, MaxSize: maxSize, Desired: desired,
		Schedule: schedule, StartTime: startTime, EndTime: endTime,
	}, nil
}

func (s *Store) ListScheduledActions(asgName, actionName string) ([]ScheduledAction, error) {
	query := `SELECT arn, name, asg_name, min_size, max_size, desired, schedule, start_time, end_time FROM scheduled_actions WHERE 1=1`
	var args []any
	if asgName != "" {
		query += " AND asg_name = ?"
		args = append(args, asgName)
	}
	if actionName != "" {
		query += " AND name = ?"
		args = append(args, actionName)
	}
	query += " ORDER BY name"
	rows, err := s.store.DB().Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []ScheduledAction
	for rows.Next() {
		var a ScheduledAction
		if err := rows.Scan(&a.ARN, &a.Name, &a.ASGName, &a.MinSize, &a.MaxSize, &a.Desired, &a.Schedule, &a.StartTime, &a.EndTime); err != nil {
			return nil, err
		}
		out = append(out, a)
	}
	return out, rows.Err()
}

func (s *Store) DeleteScheduledAction(asgName, actionName string) error {
	res, err := s.store.DB().Exec(`DELETE FROM scheduled_actions WHERE asg_name = ? AND name = ?`, asgName, actionName)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errScheduledNotFound
	}
	return nil
}

// --- LifecycleHook CRUD ---

func (s *Store) PutLifecycleHook(name, asgName, transition string, heartbeatTimeout int, defaultResult, roleARN string) (*LifecycleHook, error) {
	_, err := s.store.DB().Exec(
		`INSERT INTO lifecycle_hooks (name, asg_name, transition, heartbeat_timeout, default_result, role_arn)
		 VALUES (?, ?, ?, ?, ?, ?)
		 ON CONFLICT(name, asg_name) DO UPDATE SET
		   transition=excluded.transition, heartbeat_timeout=excluded.heartbeat_timeout,
		   default_result=excluded.default_result, role_arn=excluded.role_arn`,
		name, asgName, transition, heartbeatTimeout, defaultResult, roleARN,
	)
	if err != nil {
		return nil, err
	}
	return &LifecycleHook{
		Name: name, ASGName: asgName, Transition: transition,
		HeartbeatTimeout: heartbeatTimeout, DefaultResult: defaultResult, RoleARN: roleARN,
	}, nil
}

func (s *Store) ListLifecycleHooks(asgName, hookName string) ([]LifecycleHook, error) {
	query := `SELECT name, asg_name, transition, heartbeat_timeout, default_result, role_arn FROM lifecycle_hooks WHERE 1=1`
	var args []any
	if asgName != "" {
		query += " AND asg_name = ?"
		args = append(args, asgName)
	}
	if hookName != "" {
		query += " AND name = ?"
		args = append(args, hookName)
	}
	query += " ORDER BY name"
	rows, err := s.store.DB().Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []LifecycleHook
	for rows.Next() {
		var h LifecycleHook
		if err := rows.Scan(&h.Name, &h.ASGName, &h.Transition, &h.HeartbeatTimeout, &h.DefaultResult, &h.RoleARN); err != nil {
			return nil, err
		}
		out = append(out, h)
	}
	return out, rows.Err()
}

func (s *Store) DeleteLifecycleHook(asgName, hookName string) error {
	res, err := s.store.DB().Exec(`DELETE FROM lifecycle_hooks WHERE asg_name = ? AND name = ?`, asgName, hookName)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errHookNotFound
	}
	return nil
}

// --- Tag helpers ---

func (s *Store) AddTags(arn string, tags map[string]string) error {
	return s.tags.AddTags(arn, tags)
}

func (s *Store) RemoveTags(arn string, keys []string) error {
	return s.tags.RemoveTags(arn, keys)
}

func (s *Store) ListTags(arn string) (map[string]string, error) {
	return s.tags.ListTags(arn)
}

func (s *Store) ListAllTags() (map[string]map[string]string, error) {
	rows, err := s.store.DB().Query(
		`SELECT resource_arn, key, value FROM resource_tags ORDER BY resource_arn, key`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	out := make(map[string]map[string]string)
	for rows.Next() {
		var resARN, k, v string
		if err := rows.Scan(&resARN, &k, &v); err != nil {
			return nil, err
		}
		if out[resARN] == nil {
			out[resARN] = make(map[string]string)
		}
		out[resARN][k] = v
	}
	return out, rows.Err()
}

// --- helpers ---

type scanner interface{ Scan(dest ...any) error }

func scanASG(sc scanner) (*AutoScalingGroup, error) {
	var a AutoScalingGroup
	var createdAt int64
	err := sc.Scan(&a.Name, &a.ARN, &a.LaunchConfig, &a.MinSize, &a.MaxSize, &a.Desired,
		&a.AZList, &a.VPCZones, &a.HealthCheck, &a.TargetGroups, &a.Status, &createdAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errASGNotFound
		}
		return nil, err
	}
	a.CreatedAt = time.Unix(createdAt, 0)
	return &a, nil
}

func scanLC(sc scanner) (*LaunchConfiguration, error) {
	var lc LaunchConfiguration
	var createdAt int64
	err := sc.Scan(&lc.Name, &lc.ARN, &lc.ImageID, &lc.InstanceType, &lc.KeyName, &lc.SecurityGroups, &createdAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errLCNotFound
		}
		return nil, err
	}
	lc.CreatedAt = time.Unix(createdAt, 0)
	return &lc, nil
}

func buildPlaceholders(n int) string {
	if n == 0 {
		return ""
	}
	s := "?"
	for i := 1; i < n; i++ {
		s += ",?"
	}
	return s
}
