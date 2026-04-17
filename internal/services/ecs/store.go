// SPDX-License-Identifier: Apache-2.0

// internal/services/ecs/store.go
package ecs

import (
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"github.com/skyoo2003/devcloud/internal/shared"
	"github.com/skyoo2003/devcloud/internal/storage/sqlite"
)

var (
	ErrClusterNotFound           = errors.New("cluster not found")
	ErrTaskDefNotFound           = errors.New("task definition not found")
	ErrTaskNotFound              = errors.New("task not found")
	ErrServiceNotFound           = errors.New("service not found")
	ErrCapacityProviderNotFound  = errors.New("capacity provider not found")
	ErrContainerInstanceNotFound = errors.New("container instance not found")
	ErrScalableTargetNotFound    = errors.New("scalable target not found")
	ErrScalingPolicyNotFound     = errors.New("scaling policy not found")
	ErrTaskSetNotFound           = errors.New("task set not found")
)

const region = "us-east-1"

var migrations = []sqlite.Migration{
	{Version: 1, SQL: `
		CREATE TABLE IF NOT EXISTS clusters (
			arn        TEXT PRIMARY KEY,
			name       TEXT NOT NULL,
			account_id TEXT NOT NULL,
			status     TEXT NOT NULL DEFAULT 'ACTIVE'
		);
		CREATE TABLE IF NOT EXISTS task_definitions (
			arn            TEXT PRIMARY KEY,
			family         TEXT NOT NULL,
			revision       INTEGER NOT NULL,
			account_id     TEXT NOT NULL,
			container_defs TEXT NOT NULL DEFAULT '[]',
			status         TEXT NOT NULL DEFAULT 'ACTIVE'
		);
		CREATE TABLE IF NOT EXISTS tasks (
			arn           TEXT PRIMARY KEY,
			cluster_arn   TEXT NOT NULL,
			task_def_arn  TEXT NOT NULL,
			account_id    TEXT NOT NULL,
			status        TEXT NOT NULL DEFAULT 'RUNNING',
			desired_status TEXT NOT NULL DEFAULT 'RUNNING',
			started_at    DATETIME,
			stopped_at    DATETIME
		);
		CREATE TABLE IF NOT EXISTS services (
			arn           TEXT PRIMARY KEY,
			name          TEXT NOT NULL,
			cluster_arn   TEXT NOT NULL,
			task_def_arn  TEXT NOT NULL,
			desired_count INTEGER NOT NULL DEFAULT 1,
			running_count INTEGER NOT NULL DEFAULT 0,
			account_id    TEXT NOT NULL,
			status        TEXT NOT NULL DEFAULT 'ACTIVE'
		);
	`},
	{Version: 2, SQL: `
		CREATE TABLE IF NOT EXISTS capacity_providers (
			name       TEXT NOT NULL,
			arn        TEXT NOT NULL,
			status     TEXT NOT NULL DEFAULT 'ACTIVE',
			asg_arn    TEXT NOT NULL DEFAULT '',
			account_id TEXT NOT NULL,
			created_at DATETIME NOT NULL,
			PRIMARY KEY (name, account_id)
		);
		CREATE TABLE IF NOT EXISTS container_instances (
			container_instance_arn TEXT PRIMARY KEY,
			cluster_arn            TEXT NOT NULL,
			ec2_instance_id        TEXT NOT NULL DEFAULT '',
			status                 TEXT NOT NULL DEFAULT 'ACTIVE',
			agent_connected        INTEGER NOT NULL DEFAULT 1,
			account_id             TEXT NOT NULL,
			registered_at          DATETIME NOT NULL
		);
		CREATE TABLE IF NOT EXISTS ecs_attributes (
			cluster_arn     TEXT NOT NULL,
			target_type     TEXT NOT NULL DEFAULT 'container-instance',
			target_id       TEXT NOT NULL DEFAULT '',
			name            TEXT NOT NULL,
			value           TEXT NOT NULL DEFAULT '',
			account_id      TEXT NOT NULL,
			PRIMARY KEY (cluster_arn, target_type, target_id, name, account_id)
		);
	`},
	{Version: 3, SQL: `
		CREATE TABLE IF NOT EXISTS service_scalable_targets (
			service_arn        TEXT NOT NULL,
			scalable_dimension TEXT NOT NULL,
			min_capacity       INTEGER NOT NULL DEFAULT 0,
			max_capacity       INTEGER NOT NULL DEFAULT 10,
			role_arn           TEXT NOT NULL DEFAULT '',
			account_id         TEXT NOT NULL,
			created_at         DATETIME NOT NULL,
			PRIMARY KEY (service_arn, scalable_dimension, account_id)
		);
		CREATE TABLE IF NOT EXISTS service_scaling_policies (
			policy_arn         TEXT PRIMARY KEY,
			policy_name        TEXT NOT NULL,
			service_arn        TEXT NOT NULL,
			scalable_dimension TEXT NOT NULL,
			policy_type        TEXT NOT NULL DEFAULT 'TargetTrackingScaling',
			config             TEXT NOT NULL DEFAULT '{}',
			account_id         TEXT NOT NULL,
			created_at         DATETIME NOT NULL
		);
		CREATE TABLE IF NOT EXISTS task_sets (
			id                TEXT NOT NULL,
			arn               TEXT NOT NULL,
			service_arn       TEXT NOT NULL,
			cluster_arn       TEXT NOT NULL,
			task_def_arn      TEXT NOT NULL,
			external_id       TEXT NOT NULL DEFAULT '',
			launch_type       TEXT NOT NULL DEFAULT 'EC2',
			status            TEXT NOT NULL DEFAULT 'ACTIVE',
			scale_value       REAL NOT NULL DEFAULT 100.0,
			scale_unit        TEXT NOT NULL DEFAULT 'PERCENT',
			account_id        TEXT NOT NULL,
			created_at        DATETIME NOT NULL,
			PRIMARY KEY (id, account_id)
		);
		CREATE TABLE IF NOT EXISTS account_settings (
			name       TEXT NOT NULL,
			value      TEXT NOT NULL,
			principal  TEXT NOT NULL DEFAULT '',
			account_id TEXT NOT NULL,
			PRIMARY KEY (name, principal, account_id)
		);
	`},
}

type Cluster struct {
	ARN       string
	Name      string
	AccountID string
	Status    string
}

type TaskDefinition struct {
	ARN           string
	Family        string
	Revision      int
	AccountID     string
	ContainerDefs string
	Status        string
}

type Task struct {
	ARN           string
	ClusterARN    string
	TaskDefARN    string
	AccountID     string
	Status        string
	DesiredStatus string
	StartedAt     *time.Time
	StoppedAt     *time.Time
}

type Service struct {
	ARN          string
	Name         string
	ClusterARN   string
	TaskDefARN   string
	DesiredCount int
	RunningCount int
	AccountID    string
	Status       string
}

type CapacityProvider struct {
	Name      string
	ARN       string
	Status    string
	AsgARN    string
	AccountID string
	CreatedAt time.Time
}

type ContainerInstance struct {
	ARN            string
	ClusterARN     string
	Ec2InstanceID  string
	Status         string
	AgentConnected bool
	AccountID      string
	RegisteredAt   time.Time
}

type ECSAttribute struct {
	ClusterARN string
	TargetType string
	TargetID   string
	Name       string
	Value      string
	AccountID  string
}

type ServiceScalableTarget struct {
	ServiceARN        string
	ScalableDimension string
	MinCapacity       int
	MaxCapacity       int
	RoleARN           string
	AccountID         string
	CreatedAt         time.Time
}

type ServiceScalingPolicy struct {
	PolicyARN         string
	PolicyName        string
	ServiceARN        string
	ScalableDimension string
	PolicyType        string
	Config            string
	AccountID         string
	CreatedAt         time.Time
}

type TaskSet struct {
	ID         string
	ARN        string
	ServiceARN string
	ClusterARN string
	TaskDefARN string
	ExternalID string
	LaunchType string
	Status     string
	ScaleValue float64
	ScaleUnit  string
	AccountID  string
	CreatedAt  time.Time
}

type AccountSetting struct {
	Name      string
	Value     string
	Principal string
	AccountID string
}

type ECSStore struct {
	store *sqlite.Store
	tags  *shared.TagStore
}

func NewECSStore(dataDir string) (*ECSStore, error) {
	dbPath := filepath.Join(dataDir, "ecs.db")
	allMigrations := append(migrations, shared.TagMigrations...)
	s, err := sqlite.Open(dbPath, allMigrations)
	if err != nil {
		return nil, err
	}
	return &ECSStore{store: s, tags: shared.NewTagStore(s)}, nil
}

func (s *ECSStore) Close() error { return s.store.Close() }
func (s *ECSStore) db() *sql.DB  { return s.store.DB() }

func clusterARN(accountID, name string) string {
	return fmt.Sprintf("arn:aws:ecs:%s:%s:cluster/%s", region, accountID, name)
}
func taskDefARN(accountID, family string, revision int) string {
	return fmt.Sprintf("arn:aws:ecs:%s:%s:task-definition/%s:%d", region, accountID, family, revision)
}
func taskARN(accountID string) (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return fmt.Sprintf("arn:aws:ecs:%s:%s:task/%s", region, accountID, hex.EncodeToString(b)), nil
}
func serviceARN(accountID, clusterName, serviceName string) string {
	return fmt.Sprintf("arn:aws:ecs:%s:%s:service/%s/%s", region, accountID, clusterName, serviceName)
}

// Cluster CRUD

func (s *ECSStore) CreateCluster(accountID, name string) (*Cluster, error) {
	arn := clusterARN(accountID, name)
	_, err := s.db().Exec(
		`INSERT INTO clusters (arn, name, account_id, status) VALUES (?, ?, ?, 'ACTIVE')
		 ON CONFLICT(arn) DO UPDATE SET status='ACTIVE'`,
		arn, name, accountID,
	)
	if err != nil {
		return nil, err
	}
	return &Cluster{ARN: arn, Name: name, AccountID: accountID, Status: "ACTIVE"}, nil
}

func (s *ECSStore) ListClusters(accountID string) ([]string, error) {
	rows, err := s.db().Query(
		`SELECT arn FROM clusters WHERE account_id=? AND status='ACTIVE' ORDER BY name`,
		accountID,
	)
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

func (s *ECSStore) DescribeClusters(accountID string, arns []string) ([]Cluster, error) {
	query := `SELECT arn, name, account_id, status FROM clusters WHERE account_id=?`
	args := []any{accountID}
	if len(arns) > 0 {
		ph := ""
		for i, a := range arns {
			if i > 0 {
				ph += ","
			}
			ph += "?"
			args = append(args, a)
		}
		query += " AND arn IN (" + ph + ")"
	}
	rows, err := s.db().Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Cluster
	for rows.Next() {
		var c Cluster
		if err := rows.Scan(&c.ARN, &c.Name, &c.AccountID, &c.Status); err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

func (s *ECSStore) DeleteCluster(accountID, clusterARN string) error {
	res, err := s.db().Exec(
		`UPDATE clusters SET status='INACTIVE' WHERE arn=? AND account_id=?`,
		clusterARN, accountID,
	)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrClusterNotFound
	}
	return nil
}

// TaskDefinition CRUD

func (s *ECSStore) RegisterTaskDefinition(accountID, family, containerDefs string) (*TaskDefinition, error) {
	var maxRev int
	row := s.db().QueryRow(`SELECT COALESCE(MAX(revision),0) FROM task_definitions WHERE family=? AND account_id=?`, family, accountID)
	row.Scan(&maxRev)
	revision := maxRev + 1
	arn := taskDefARN(accountID, family, revision)
	_, err := s.db().Exec(
		`INSERT INTO task_definitions (arn, family, revision, account_id, container_defs, status) VALUES (?, ?, ?, ?, ?, 'ACTIVE')`,
		arn, family, revision, accountID, containerDefs,
	)
	if err != nil {
		return nil, err
	}
	return &TaskDefinition{ARN: arn, Family: family, Revision: revision, AccountID: accountID, ContainerDefs: containerDefs, Status: "ACTIVE"}, nil
}

func (s *ECSStore) DescribeTaskDefinition(accountID, arn string) (*TaskDefinition, error) {
	var td TaskDefinition
	err := s.db().QueryRow(
		`SELECT arn, family, revision, account_id, container_defs, status FROM task_definitions WHERE arn=? AND account_id=?`,
		arn, accountID,
	).Scan(&td.ARN, &td.Family, &td.Revision, &td.AccountID, &td.ContainerDefs, &td.Status)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrTaskDefNotFound
	}
	return &td, err
}

func (s *ECSStore) ListTaskDefinitions(accountID string) ([]string, error) {
	rows, err := s.db().Query(
		`SELECT arn FROM task_definitions WHERE account_id=? AND status='ACTIVE' ORDER BY family, revision`,
		accountID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var arns []string
	for rows.Next() {
		var arn string
		rows.Scan(&arn)
		arns = append(arns, arn)
	}
	return arns, rows.Err()
}

// Task CRUD

func (s *ECSStore) RunTask(accountID, clusterArn, taskDefArn string) (*Task, error) {
	arn, err := taskARN(accountID)
	if err != nil {
		return nil, err
	}
	now := time.Now().UTC()
	_, err = s.db().Exec(
		`INSERT INTO tasks (arn, cluster_arn, task_def_arn, account_id, status, desired_status, started_at) VALUES (?, ?, ?, ?, 'RUNNING', 'RUNNING', ?)`,
		arn, clusterArn, taskDefArn, accountID, now,
	)
	if err != nil {
		return nil, err
	}
	return &Task{ARN: arn, ClusterARN: clusterArn, TaskDefARN: taskDefArn, AccountID: accountID, Status: "RUNNING", DesiredStatus: "RUNNING", StartedAt: &now}, nil
}

func (s *ECSStore) StopTask(accountID, taskArn string) error {
	now := time.Now().UTC()
	res, err := s.db().Exec(
		`UPDATE tasks SET status='STOPPED', desired_status='STOPPED', stopped_at=? WHERE arn=? AND account_id=?`,
		now, taskArn, accountID,
	)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrTaskNotFound
	}
	return nil
}

func (s *ECSStore) DescribeTasks(accountID string, arns []string) ([]Task, error) {
	query := `SELECT arn, cluster_arn, task_def_arn, account_id, status, desired_status, started_at, stopped_at FROM tasks WHERE account_id=?`
	args := []any{accountID}
	if len(arns) > 0 {
		ph := ""
		for i, a := range arns {
			if i > 0 {
				ph += ","
			}
			ph += "?"
			args = append(args, a)
		}
		query += " AND arn IN (" + ph + ")"
	}
	rows, err := s.db().Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Task
	for rows.Next() {
		var t Task
		if err := rows.Scan(&t.ARN, &t.ClusterARN, &t.TaskDefARN, &t.AccountID, &t.Status, &t.DesiredStatus, &t.StartedAt, &t.StoppedAt); err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, rows.Err()
}

// Service CRUD

func (s *ECSStore) CreateService(accountID, clusterArn, clusterName, name, taskDefArn string, desiredCount int) (*Service, error) {
	arn := serviceARN(accountID, clusterName, name)
	_, err := s.db().Exec(
		`INSERT INTO services (arn, name, cluster_arn, task_def_arn, desired_count, running_count, account_id, status)
		 VALUES (?, ?, ?, ?, ?, 0, ?, 'ACTIVE')`,
		arn, name, clusterArn, taskDefArn, desiredCount, accountID,
	)
	if err != nil {
		if sqlite.IsUniqueConstraintError(err) {
			return nil, fmt.Errorf("service %q already exists", name)
		}
		return nil, err
	}
	return &Service{ARN: arn, Name: name, ClusterARN: clusterArn, TaskDefARN: taskDefArn, DesiredCount: desiredCount, AccountID: accountID, Status: "ACTIVE"}, nil
}

func (s *ECSStore) UpdateService(accountID, serviceArn string, desiredCount int, taskDefArn string) (*Service, error) {
	// Build dynamic update to only change fields that are provided.
	setClauses := []string{}
	args := []any{}
	if desiredCount >= 0 {
		setClauses = append(setClauses, "desired_count=?")
		args = append(args, desiredCount)
	}
	if taskDefArn != "" {
		setClauses = append(setClauses, "task_def_arn=?")
		args = append(args, taskDefArn)
	}
	if len(setClauses) > 0 {
		query := "UPDATE services SET " + strings.Join(setClauses, ", ") + " WHERE arn=? AND account_id=?"
		args = append(args, serviceArn, accountID)
		res, err := s.db().Exec(query, args...)
		if err != nil {
			return nil, err
		}
		n, _ := res.RowsAffected()
		if n == 0 {
			return nil, ErrServiceNotFound
		}
	}
	var svc Service
	err := s.db().QueryRow(
		`SELECT arn, name, cluster_arn, task_def_arn, desired_count, running_count, account_id, status FROM services WHERE arn=? AND account_id=?`,
		serviceArn, accountID,
	).Scan(&svc.ARN, &svc.Name, &svc.ClusterARN, &svc.TaskDefARN, &svc.DesiredCount, &svc.RunningCount, &svc.AccountID, &svc.Status)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrServiceNotFound
	}
	return &svc, err
}

func (s *ECSStore) DeleteService(accountID, serviceArn string) error {
	res, err := s.db().Exec(`UPDATE services SET status='INACTIVE' WHERE arn=? AND account_id=?`, serviceArn, accountID)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrServiceNotFound
	}
	return nil
}

func (s *ECSStore) ListServices(accountID, clusterArn string) ([]string, error) {
	rows, err := s.db().Query(
		`SELECT arn FROM services WHERE account_id=? AND cluster_arn=? AND status='ACTIVE' ORDER BY name`,
		accountID, clusterArn,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var arns []string
	for rows.Next() {
		var arn string
		rows.Scan(&arn)
		arns = append(arns, arn)
	}
	return arns, rows.Err()
}

// FindServiceARNByName finds a service ARN by name within a cluster.
func (s *ECSStore) FindServiceARNByName(accountID, clusterArn, serviceName string) (string, error) {
	query := `SELECT arn FROM services WHERE account_id=? AND name=? AND status='ACTIVE'`
	args := []any{accountID, serviceName}
	if clusterArn != "" {
		query += " AND cluster_arn=?"
		args = append(args, clusterArn)
	}
	query += " LIMIT 1"
	var arn string
	err := s.db().QueryRow(query, args...).Scan(&arn)
	if errors.Is(err, sql.ErrNoRows) {
		return "", ErrServiceNotFound
	}
	return arn, err
}

// DeregisterTaskDefinition marks a task definition as INACTIVE.
func (s *ECSStore) DeregisterTaskDefinition(accountID, arn string) (*TaskDefinition, error) {
	res, err := s.db().Exec(
		`UPDATE task_definitions SET status='INACTIVE' WHERE arn=? AND account_id=?`,
		arn, accountID,
	)
	if err != nil {
		return nil, err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return nil, ErrTaskDefNotFound
	}
	var td TaskDefinition
	err = s.db().QueryRow(
		`SELECT arn, family, revision, account_id, container_defs, status FROM task_definitions WHERE arn=? AND account_id=?`,
		arn, accountID,
	).Scan(&td.ARN, &td.Family, &td.Revision, &td.AccountID, &td.ContainerDefs, &td.Status)
	if err != nil {
		return nil, err
	}
	return &td, nil
}

func (s *ECSStore) DescribeServices(accountID string, arns []string) ([]Service, error) {
	query := `SELECT arn, name, cluster_arn, task_def_arn, desired_count, running_count, account_id, status FROM services WHERE account_id=?`
	args := []any{accountID}
	if len(arns) > 0 {
		ph := ""
		for i, a := range arns {
			if i > 0 {
				ph += ","
			}
			ph += "?"
			args = append(args, a)
		}
		query += " AND arn IN (" + ph + ")"
	}
	rows, err := s.db().Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Service
	for rows.Next() {
		var svc Service
		rows.Scan(&svc.ARN, &svc.Name, &svc.ClusterARN, &svc.TaskDefARN, &svc.DesiredCount, &svc.RunningCount, &svc.AccountID, &svc.Status)
		out = append(out, svc)
	}
	return out, rows.Err()
}

// --- Capacity Provider ---

func capacityProviderARN(accountID, name string) string {
	return fmt.Sprintf("arn:aws:ecs:%s:%s:capacity-provider/%s", region, accountID, name)
}

func (s *ECSStore) CreateCapacityProvider(accountID, name, asgARN string) (*CapacityProvider, error) {
	arn := capacityProviderARN(accountID, name)
	now := time.Now().UTC()
	_, err := s.db().Exec(
		`INSERT INTO capacity_providers (name, arn, status, asg_arn, account_id, created_at) VALUES (?, ?, 'ACTIVE', ?, ?, ?)
		 ON CONFLICT(name, account_id) DO UPDATE SET status='ACTIVE', asg_arn=excluded.asg_arn`,
		name, arn, asgARN, accountID, now,
	)
	if err != nil {
		return nil, err
	}
	return &CapacityProvider{Name: name, ARN: arn, Status: "ACTIVE", AsgARN: asgARN, AccountID: accountID, CreatedAt: now}, nil
}

func (s *ECSStore) DescribeCapacityProviders(accountID string, names []string) ([]CapacityProvider, error) {
	query := `SELECT name, arn, status, asg_arn, account_id, created_at FROM capacity_providers WHERE account_id=?`
	args := []any{accountID}
	if len(names) > 0 {
		ph := ""
		for i, n := range names {
			if i > 0 {
				ph += ","
			}
			ph += "?"
			args = append(args, n)
		}
		query += " AND name IN (" + ph + ")"
	}
	query += " ORDER BY name"
	rows, err := s.db().Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []CapacityProvider
	for rows.Next() {
		var cp CapacityProvider
		var createdAt string
		if err := rows.Scan(&cp.Name, &cp.ARN, &cp.Status, &cp.AsgARN, &cp.AccountID, &createdAt); err != nil {
			return nil, err
		}
		cp.CreatedAt, _ = time.Parse(time.RFC3339, createdAt)
		out = append(out, cp)
	}
	return out, rows.Err()
}

func (s *ECSStore) DeleteCapacityProvider(accountID, nameOrARN string) error {
	var res sql.Result
	var err error
	if strings.Contains(nameOrARN, ":") {
		res, err = s.db().Exec(`UPDATE capacity_providers SET status='INACTIVE' WHERE arn=? AND account_id=?`, nameOrARN, accountID)
	} else {
		res, err = s.db().Exec(`UPDATE capacity_providers SET status='INACTIVE' WHERE name=? AND account_id=?`, nameOrARN, accountID)
	}
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrCapacityProviderNotFound
	}
	return nil
}

// --- Container Instances ---

func containerInstanceARN(accountID, clusterName, id string) string {
	return fmt.Sprintf("arn:aws:ecs:%s:%s:container-instance/%s/%s", region, accountID, clusterName, id)
}

func (s *ECSStore) RegisterContainerInstance(accountID, clusterARN, clusterName, ec2ID string) (*ContainerInstance, error) {
	b := make([]byte, 8)
	rand.Read(b)
	id := hex.EncodeToString(b)
	arn := containerInstanceARN(accountID, clusterName, id)
	now := time.Now().UTC()
	_, err := s.db().Exec(
		`INSERT INTO container_instances (container_instance_arn, cluster_arn, ec2_instance_id, status, agent_connected, account_id, registered_at)
		 VALUES (?, ?, ?, 'ACTIVE', 1, ?, ?)`,
		arn, clusterARN, ec2ID, accountID, now,
	)
	if err != nil {
		return nil, err
	}
	return &ContainerInstance{ARN: arn, ClusterARN: clusterARN, Ec2InstanceID: ec2ID, Status: "ACTIVE", AgentConnected: true, AccountID: accountID, RegisteredAt: now}, nil
}

func (s *ECSStore) ListContainerInstances(accountID, clusterARN, status string) ([]string, error) {
	query := `SELECT container_instance_arn FROM container_instances WHERE account_id=? AND cluster_arn=?`
	args := []any{accountID, clusterARN}
	if status != "" {
		query += " AND status=?"
		args = append(args, status)
	}
	query += " ORDER BY registered_at"
	rows, err := s.db().Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var arns []string
	for rows.Next() {
		var arn string
		rows.Scan(&arn)
		arns = append(arns, arn)
	}
	return arns, rows.Err()
}

func (s *ECSStore) DescribeContainerInstances(accountID string, arns []string) ([]ContainerInstance, error) {
	query := `SELECT container_instance_arn, cluster_arn, ec2_instance_id, status, agent_connected, account_id, registered_at FROM container_instances WHERE account_id=?`
	args := []any{accountID}
	if len(arns) > 0 {
		ph := ""
		for i, a := range arns {
			if i > 0 {
				ph += ","
			}
			ph += "?"
			args = append(args, a)
		}
		query += " AND container_instance_arn IN (" + ph + ")"
	}
	rows, err := s.db().Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ContainerInstance
	for rows.Next() {
		var ci ContainerInstance
		var agentConnected int
		var registeredAt string
		if err := rows.Scan(&ci.ARN, &ci.ClusterARN, &ci.Ec2InstanceID, &ci.Status, &agentConnected, &ci.AccountID, &registeredAt); err != nil {
			return nil, err
		}
		ci.AgentConnected = agentConnected == 1
		ci.RegisteredAt, _ = time.Parse(time.RFC3339, registeredAt)
		out = append(out, ci)
	}
	return out, rows.Err()
}

func (s *ECSStore) DeregisterContainerInstance(accountID, arn string, force bool) error {
	res, err := s.db().Exec(`UPDATE container_instances SET status='INACTIVE' WHERE container_instance_arn=? AND account_id=?`, arn, accountID)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrContainerInstanceNotFound
	}
	return nil
}

func (s *ECSStore) UpdateContainerInstancesState(accountID string, arns []string, status string) error {
	for _, arn := range arns {
		s.db().Exec(`UPDATE container_instances SET status=? WHERE container_instance_arn=? AND account_id=?`, status, arn, accountID)
	}
	return nil
}

// --- Attributes ---

func (s *ECSStore) PutAttributes(accountID, clusterARN string, attrs []ECSAttribute) error {
	for _, a := range attrs {
		_, err := s.db().Exec(
			`INSERT OR REPLACE INTO ecs_attributes (cluster_arn, target_type, target_id, name, value, account_id) VALUES (?, ?, ?, ?, ?, ?)`,
			clusterARN, a.TargetType, a.TargetID, a.Name, a.Value, accountID,
		)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *ECSStore) DeleteAttributes(accountID, clusterARN string, attrs []ECSAttribute) error {
	for _, a := range attrs {
		s.db().Exec(
			`DELETE FROM ecs_attributes WHERE cluster_arn=? AND target_type=? AND target_id=? AND name=? AND account_id=?`,
			clusterARN, a.TargetType, a.TargetID, a.Name, accountID,
		)
	}
	return nil
}

func (s *ECSStore) ListAttributes(accountID, clusterARN, targetType, attrName, attrValue string) ([]ECSAttribute, error) {
	query := `SELECT cluster_arn, target_type, target_id, name, value, account_id FROM ecs_attributes WHERE account_id=? AND cluster_arn=?`
	args := []any{accountID, clusterARN}
	if targetType != "" {
		query += " AND target_type=?"
		args = append(args, targetType)
	}
	if attrName != "" {
		query += " AND name=?"
		args = append(args, attrName)
	}
	if attrValue != "" {
		query += " AND value=?"
		args = append(args, attrValue)
	}
	rows, err := s.db().Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ECSAttribute
	for rows.Next() {
		var a ECSAttribute
		rows.Scan(&a.ClusterARN, &a.TargetType, &a.TargetID, &a.Name, &a.Value, &a.AccountID)
		out = append(out, a)
	}
	return out, rows.Err()
}

// --- Service Auto-Scaling Targets ---

func (s *ECSStore) RegisterScalableTarget(accountID, serviceARN, dimension, roleARN string, minCap, maxCap int) (*ServiceScalableTarget, error) {
	now := time.Now().UTC()
	_, err := s.db().Exec(
		`INSERT INTO service_scalable_targets (service_arn, scalable_dimension, min_capacity, max_capacity, role_arn, account_id, created_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?)
		 ON CONFLICT(service_arn, scalable_dimension, account_id) DO UPDATE
		 SET min_capacity=excluded.min_capacity, max_capacity=excluded.max_capacity, role_arn=excluded.role_arn`,
		serviceARN, dimension, minCap, maxCap, roleARN, accountID, now,
	)
	if err != nil {
		return nil, err
	}
	return &ServiceScalableTarget{
		ServiceARN:        serviceARN,
		ScalableDimension: dimension,
		MinCapacity:       minCap,
		MaxCapacity:       maxCap,
		RoleARN:           roleARN,
		AccountID:         accountID,
		CreatedAt:         now,
	}, nil
}

func (s *ECSStore) DeregisterScalableTarget(accountID, serviceARN, dimension string) error {
	res, err := s.db().Exec(
		`DELETE FROM service_scalable_targets WHERE service_arn=? AND scalable_dimension=? AND account_id=?`,
		serviceARN, dimension, accountID,
	)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrScalableTargetNotFound
	}
	return nil
}

func (s *ECSStore) ListScalableTargets(accountID, serviceARN, dimension string) ([]ServiceScalableTarget, error) {
	query := `SELECT service_arn, scalable_dimension, min_capacity, max_capacity, role_arn, account_id, created_at FROM service_scalable_targets WHERE account_id=?`
	args := []any{accountID}
	if serviceARN != "" {
		query += " AND service_arn=?"
		args = append(args, serviceARN)
	}
	if dimension != "" {
		query += " AND scalable_dimension=?"
		args = append(args, dimension)
	}
	query += " ORDER BY created_at"
	rows, err := s.db().Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ServiceScalableTarget
	for rows.Next() {
		var t ServiceScalableTarget
		var createdAt string
		if err := rows.Scan(&t.ServiceARN, &t.ScalableDimension, &t.MinCapacity, &t.MaxCapacity, &t.RoleARN, &t.AccountID, &createdAt); err != nil {
			return nil, err
		}
		t.CreatedAt, _ = time.Parse(time.RFC3339, createdAt)
		out = append(out, t)
	}
	return out, rows.Err()
}

// --- Service Scaling Policies ---

func (s *ECSStore) PutServiceScalingPolicy(accountID, serviceARN, policyName, dimension, policyType, config string) (*ServiceScalingPolicy, error) {
	arn := fmt.Sprintf("arn:aws:autoscaling:%s:%s:scalingPolicy:%s:resource/ecs/%s:policyName/%s",
		region, accountID, hex.EncodeToString(randomBytes(8)), strings.ReplaceAll(strings.TrimPrefix(serviceARN, fmt.Sprintf("arn:aws:ecs:%s:%s:service/", region, accountID)), "/", "-"), policyName)
	now := time.Now().UTC()
	// Try update first
	res, err := s.db().Exec(
		`UPDATE service_scaling_policies SET policy_type=?, config=? WHERE policy_name=? AND service_arn=? AND scalable_dimension=? AND account_id=?`,
		policyType, config, policyName, serviceARN, dimension, accountID,
	)
	if err != nil {
		return nil, err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		_, err = s.db().Exec(
			`INSERT INTO service_scaling_policies (policy_arn, policy_name, service_arn, scalable_dimension, policy_type, config, account_id, created_at)
			 VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
			arn, policyName, serviceARN, dimension, policyType, config, accountID, now,
		)
		if err != nil {
			return nil, err
		}
	} else {
		row := s.db().QueryRow(
			`SELECT policy_arn, created_at FROM service_scaling_policies WHERE policy_name=? AND service_arn=? AND scalable_dimension=? AND account_id=?`,
			policyName, serviceARN, dimension, accountID,
		)
		var createdAt string
		row.Scan(&arn, &createdAt)
		now, _ = time.Parse(time.RFC3339, createdAt)
	}
	return &ServiceScalingPolicy{
		PolicyARN:         arn,
		PolicyName:        policyName,
		ServiceARN:        serviceARN,
		ScalableDimension: dimension,
		PolicyType:        policyType,
		Config:            config,
		AccountID:         accountID,
		CreatedAt:         now,
	}, nil
}

func (s *ECSStore) DeleteServiceScalingPolicy(accountID, serviceARN, policyName string) error {
	res, err := s.db().Exec(
		`DELETE FROM service_scaling_policies WHERE service_arn=? AND policy_name=? AND account_id=?`,
		serviceARN, policyName, accountID,
	)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrScalingPolicyNotFound
	}
	return nil
}

func (s *ECSStore) ListServiceScalingPolicies(accountID, serviceARN string) ([]ServiceScalingPolicy, error) {
	query := `SELECT policy_arn, policy_name, service_arn, scalable_dimension, policy_type, config, account_id, created_at FROM service_scaling_policies WHERE account_id=?`
	args := []any{accountID}
	if serviceARN != "" {
		query += " AND service_arn=?"
		args = append(args, serviceARN)
	}
	query += " ORDER BY created_at"
	rows, err := s.db().Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ServiceScalingPolicy
	for rows.Next() {
		var p ServiceScalingPolicy
		var createdAt string
		if err := rows.Scan(&p.PolicyARN, &p.PolicyName, &p.ServiceARN, &p.ScalableDimension, &p.PolicyType, &p.Config, &p.AccountID, &createdAt); err != nil {
			return nil, err
		}
		p.CreatedAt, _ = time.Parse(time.RFC3339, createdAt)
		out = append(out, p)
	}
	return out, rows.Err()
}

// --- Task Sets ---

func (s *ECSStore) CreateTaskSet(accountID, clusterARN, serviceARN, taskDefARN, externalID, launchType string) (*TaskSet, error) {
	id := "ecs-svc/" + hex.EncodeToString(randomBytes(8))
	arn := fmt.Sprintf("arn:aws:ecs:%s:%s:task-set/%s/%s/%s", region, accountID,
		strings.TrimPrefix(clusterARN, fmt.Sprintf("arn:aws:ecs:%s:%s:cluster/", region, accountID)),
		strings.TrimPrefix(serviceARN, fmt.Sprintf("arn:aws:ecs:%s:%s:service/", region, accountID)), id)
	now := time.Now().UTC()
	if launchType == "" {
		launchType = "EC2"
	}
	_, err := s.db().Exec(
		`INSERT INTO task_sets (id, arn, service_arn, cluster_arn, task_def_arn, external_id, launch_type, status, scale_value, scale_unit, account_id, created_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, 'ACTIVE', 100.0, 'PERCENT', ?, ?)`,
		id, arn, serviceARN, clusterARN, taskDefARN, externalID, launchType, accountID, now,
	)
	if err != nil {
		return nil, err
	}
	return &TaskSet{
		ID:         id,
		ARN:        arn,
		ServiceARN: serviceARN,
		ClusterARN: clusterARN,
		TaskDefARN: taskDefARN,
		ExternalID: externalID,
		LaunchType: launchType,
		Status:     "ACTIVE",
		ScaleValue: 100.0,
		ScaleUnit:  "PERCENT",
		AccountID:  accountID,
		CreatedAt:  now,
	}, nil
}

func (s *ECSStore) DeleteTaskSet(accountID, id string) error {
	res, err := s.db().Exec(
		`UPDATE task_sets SET status='INACTIVE' WHERE id=? AND account_id=?`,
		id, accountID,
	)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrTaskSetNotFound
	}
	return nil
}

func (s *ECSStore) UpdateTaskSet(accountID, id string, scale float64, unit string) (*TaskSet, error) {
	if unit == "" {
		unit = "PERCENT"
	}
	res, err := s.db().Exec(
		`UPDATE task_sets SET scale_value=?, scale_unit=? WHERE id=? AND account_id=?`,
		scale, unit, id, accountID,
	)
	if err != nil {
		return nil, err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return nil, ErrTaskSetNotFound
	}
	return s.GetTaskSet(accountID, id)
}

func (s *ECSStore) GetTaskSet(accountID, id string) (*TaskSet, error) {
	var ts TaskSet
	var createdAt string
	err := s.db().QueryRow(
		`SELECT id, arn, service_arn, cluster_arn, task_def_arn, external_id, launch_type, status, scale_value, scale_unit, account_id, created_at
		 FROM task_sets WHERE id=? AND account_id=?`,
		id, accountID,
	).Scan(&ts.ID, &ts.ARN, &ts.ServiceARN, &ts.ClusterARN, &ts.TaskDefARN, &ts.ExternalID, &ts.LaunchType, &ts.Status, &ts.ScaleValue, &ts.ScaleUnit, &ts.AccountID, &createdAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrTaskSetNotFound
	}
	if err != nil {
		return nil, err
	}
	ts.CreatedAt, _ = time.Parse(time.RFC3339, createdAt)
	return &ts, nil
}

func (s *ECSStore) ListTaskSets(accountID, serviceARN string) ([]TaskSet, error) {
	query := `SELECT id, arn, service_arn, cluster_arn, task_def_arn, external_id, launch_type, status, scale_value, scale_unit, account_id, created_at FROM task_sets WHERE account_id=?`
	args := []any{accountID}
	if serviceARN != "" {
		query += " AND service_arn=?"
		args = append(args, serviceARN)
	}
	query += " ORDER BY created_at"
	rows, err := s.db().Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []TaskSet
	for rows.Next() {
		var ts TaskSet
		var createdAt string
		if err := rows.Scan(&ts.ID, &ts.ARN, &ts.ServiceARN, &ts.ClusterARN, &ts.TaskDefARN, &ts.ExternalID, &ts.LaunchType, &ts.Status, &ts.ScaleValue, &ts.ScaleUnit, &ts.AccountID, &createdAt); err != nil {
			return nil, err
		}
		ts.CreatedAt, _ = time.Parse(time.RFC3339, createdAt)
		out = append(out, ts)
	}
	return out, rows.Err()
}

// --- Account Settings ---

func (s *ECSStore) PutAccountSetting(accountID, name, value, principal string) (*AccountSetting, error) {
	_, err := s.db().Exec(
		`INSERT INTO account_settings (name, value, principal, account_id) VALUES (?, ?, ?, ?)
		 ON CONFLICT(name, principal, account_id) DO UPDATE SET value=excluded.value`,
		name, value, principal, accountID,
	)
	if err != nil {
		return nil, err
	}
	return &AccountSetting{Name: name, Value: value, Principal: principal, AccountID: accountID}, nil
}

func (s *ECSStore) DeleteAccountSetting(accountID, name, principal string) error {
	_, err := s.db().Exec(
		`DELETE FROM account_settings WHERE name=? AND principal=? AND account_id=?`,
		name, principal, accountID,
	)
	return err
}

func (s *ECSStore) ListAccountSettings(accountID, name, principal string) ([]AccountSetting, error) {
	query := `SELECT name, value, principal, account_id FROM account_settings WHERE account_id=?`
	args := []any{accountID}
	if name != "" {
		query += " AND name=?"
		args = append(args, name)
	}
	if principal != "" {
		query += " AND principal=?"
		args = append(args, principal)
	}
	query += " ORDER BY name"
	rows, err := s.db().Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []AccountSetting
	for rows.Next() {
		var a AccountSetting
		if err := rows.Scan(&a.Name, &a.Value, &a.Principal, &a.AccountID); err != nil {
			return nil, err
		}
		out = append(out, a)
	}
	return out, rows.Err()
}

// --- helpers ---

// Tags exposes the tag store for use by the provider.
func (s *ECSStore) Tags() *shared.TagStore {
	return s.tags
}

func randomBytes(n int) []byte {
	b := make([]byte, n)
	rand.Read(b)
	return b
}
