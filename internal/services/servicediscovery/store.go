// SPDX-License-Identifier: Apache-2.0

// internal/services/servicediscovery/store.go
package servicediscovery

import (
	"database/sql"
	"errors"
	"path/filepath"
	"time"

	"github.com/skyoo2003/devcloud/internal/shared"
	"github.com/skyoo2003/devcloud/internal/storage/sqlite"
)

var (
	errNamespaceNotFound = errors.New("namespace not found")
	errServiceNotFound   = errors.New("service not found")
	errInstanceNotFound  = errors.New("instance not found")
	errOperationNotFound = errors.New("operation not found")
)

var migrations = []sqlite.Migration{
	{Version: 1, SQL: `
		CREATE TABLE IF NOT EXISTS namespaces (
			id          TEXT PRIMARY KEY,
			arn         TEXT NOT NULL UNIQUE,
			name        TEXT NOT NULL,
			type        TEXT NOT NULL DEFAULT 'HTTP',
			description TEXT NOT NULL DEFAULT '',
			config      TEXT NOT NULL DEFAULT '{}',
			created_at  INTEGER NOT NULL
		);
		CREATE TABLE IF NOT EXISTS services (
			id            TEXT PRIMARY KEY,
			arn           TEXT NOT NULL UNIQUE,
			name          TEXT NOT NULL,
			namespace_id  TEXT NOT NULL,
			description   TEXT NOT NULL DEFAULT '',
			dns_config    TEXT NOT NULL DEFAULT '{}',
			health_config TEXT NOT NULL DEFAULT '{}',
			created_at    INTEGER NOT NULL
		);
		CREATE TABLE IF NOT EXISTS instances (
			id         TEXT NOT NULL,
			service_id TEXT NOT NULL,
			attributes TEXT NOT NULL DEFAULT '{}',
			PRIMARY KEY (id, service_id)
		);
		CREATE TABLE IF NOT EXISTS operations (
			id         TEXT PRIMARY KEY,
			type       TEXT NOT NULL,
			status     TEXT NOT NULL DEFAULT 'SUCCESS',
			targets    TEXT NOT NULL DEFAULT '{}',
			created_at INTEGER NOT NULL
		);
	`},
}

type Namespace struct {
	ID          string
	ARN         string
	Name        string
	Type        string
	Description string
	Config      string
	CreatedAt   time.Time
}

type Service struct {
	ID           string
	ARN          string
	Name         string
	NamespaceID  string
	Description  string
	DnsConfig    string
	HealthConfig string
	CreatedAt    time.Time
}

type Instance struct {
	ID         string
	ServiceID  string
	Attributes string
}

type Operation struct {
	ID        string
	Type      string
	Status    string
	Targets   string
	CreatedAt time.Time
}

type Store struct {
	store *sqlite.Store
	tags  *shared.TagStore
}

func NewStore(dataDir string) (*Store, error) {
	allMigrations := append(migrations, shared.TagMigrations...)
	dbPath := filepath.Join(dataDir, "servicediscovery.db")
	s, err := sqlite.Open(dbPath, allMigrations)
	if err != nil {
		return nil, err
	}
	return &Store{store: s, tags: shared.NewTagStore(s)}, nil
}

func (s *Store) Close() error { return s.store.Close() }

// Namespace CRUD

func (s *Store) CreateNamespace(id, arn, name, nsType, description, config string) (*Namespace, error) {
	now := time.Now().Unix()
	_, err := s.store.DB().Exec(
		`INSERT INTO namespaces (id, arn, name, type, description, config, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)`,
		id, arn, name, nsType, description, config, now,
	)
	if err != nil {
		return nil, err
	}
	return &Namespace{ID: id, ARN: arn, Name: name, Type: nsType, Description: description, Config: config, CreatedAt: time.Unix(now, 0)}, nil
}

func (s *Store) GetNamespace(id string) (*Namespace, error) {
	row := s.store.DB().QueryRow(
		`SELECT id, arn, name, type, description, config, created_at FROM namespaces WHERE id = ?`, id)
	return scanNamespace(row)
}

func (s *Store) ListNamespaces() ([]Namespace, error) {
	rows, err := s.store.DB().Query(
		`SELECT id, arn, name, type, description, config, created_at FROM namespaces ORDER BY created_at`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Namespace
	for rows.Next() {
		ns, err := scanNamespace(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *ns)
	}
	return out, rows.Err()
}

func (s *Store) UpdateNamespace(id, description string) error {
	res, err := s.store.DB().Exec(`UPDATE namespaces SET description = ? WHERE id = ?`, description, id)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errNamespaceNotFound
	}
	return nil
}

func (s *Store) DeleteNamespace(id string) error {
	res, err := s.store.DB().Exec(`DELETE FROM namespaces WHERE id = ?`, id)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errNamespaceNotFound
	}
	return nil
}

// Service CRUD

func (s *Store) CreateService(id, arn, name, namespaceID, description, dnsConfig, healthConfig string) (*Service, error) {
	now := time.Now().Unix()
	_, err := s.store.DB().Exec(
		`INSERT INTO services (id, arn, name, namespace_id, description, dns_config, health_config, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		id, arn, name, namespaceID, description, dnsConfig, healthConfig, now,
	)
	if err != nil {
		return nil, err
	}
	return &Service{ID: id, ARN: arn, Name: name, NamespaceID: namespaceID, Description: description, DnsConfig: dnsConfig, HealthConfig: healthConfig, CreatedAt: time.Unix(now, 0)}, nil
}

func (s *Store) GetService(id string) (*Service, error) {
	row := s.store.DB().QueryRow(
		`SELECT id, arn, name, namespace_id, description, dns_config, health_config, created_at FROM services WHERE id = ?`, id)
	return scanService(row)
}

func (s *Store) ListServices(namespaceID string) ([]Service, error) {
	query := `SELECT id, arn, name, namespace_id, description, dns_config, health_config, created_at FROM services`
	var args []any
	if namespaceID != "" {
		query += ` WHERE namespace_id = ?`
		args = append(args, namespaceID)
	}
	query += ` ORDER BY created_at`
	rows, err := s.store.DB().Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Service
	for rows.Next() {
		svc, err := scanService(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *svc)
	}
	return out, rows.Err()
}

func (s *Store) UpdateService(id, description, dnsConfig, healthConfig string) error {
	res, err := s.store.DB().Exec(
		`UPDATE services SET description = ?, dns_config = ?, health_config = ? WHERE id = ?`,
		description, dnsConfig, healthConfig, id,
	)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errServiceNotFound
	}
	return nil
}

func (s *Store) DeleteService(id string) error {
	res, err := s.store.DB().Exec(`DELETE FROM services WHERE id = ?`, id)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errServiceNotFound
	}
	return nil
}

// Instance CRUD

func (s *Store) RegisterInstance(id, serviceID, attributes string) (*Instance, error) {
	_, err := s.store.DB().Exec(
		`INSERT INTO instances (id, service_id, attributes) VALUES (?, ?, ?)
		 ON CONFLICT(id, service_id) DO UPDATE SET attributes=excluded.attributes`,
		id, serviceID, attributes,
	)
	if err != nil {
		return nil, err
	}
	return &Instance{ID: id, ServiceID: serviceID, Attributes: attributes}, nil
}

func (s *Store) GetInstance(id, serviceID string) (*Instance, error) {
	row := s.store.DB().QueryRow(
		`SELECT id, service_id, attributes FROM instances WHERE id = ? AND service_id = ?`, id, serviceID)
	var inst Instance
	err := row.Scan(&inst.ID, &inst.ServiceID, &inst.Attributes)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errInstanceNotFound
		}
		return nil, err
	}
	return &inst, nil
}

func (s *Store) ListInstances(serviceID string) ([]Instance, error) {
	rows, err := s.store.DB().Query(
		`SELECT id, service_id, attributes FROM instances WHERE service_id = ? ORDER BY id`, serviceID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Instance
	for rows.Next() {
		var inst Instance
		if err := rows.Scan(&inst.ID, &inst.ServiceID, &inst.Attributes); err != nil {
			return nil, err
		}
		out = append(out, inst)
	}
	return out, rows.Err()
}

func (s *Store) DeregisterInstance(id, serviceID string) error {
	res, err := s.store.DB().Exec(`DELETE FROM instances WHERE id = ? AND service_id = ?`, id, serviceID)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errInstanceNotFound
	}
	return nil
}

// Operation CRUD

func (s *Store) CreateOperation(id, opType, targets string) (*Operation, error) {
	now := time.Now().Unix()
	_, err := s.store.DB().Exec(
		`INSERT INTO operations (id, type, status, targets, created_at) VALUES (?, ?, 'SUCCESS', ?, ?)`,
		id, opType, targets, now,
	)
	if err != nil {
		return nil, err
	}
	return &Operation{ID: id, Type: opType, Status: "SUCCESS", Targets: targets, CreatedAt: time.Unix(now, 0)}, nil
}

func (s *Store) GetOperation(id string) (*Operation, error) {
	row := s.store.DB().QueryRow(
		`SELECT id, type, status, targets, created_at FROM operations WHERE id = ?`, id)
	var op Operation
	var createdAt int64
	err := row.Scan(&op.ID, &op.Type, &op.Status, &op.Targets, &createdAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errOperationNotFound
		}
		return nil, err
	}
	op.CreatedAt = time.Unix(createdAt, 0)
	return &op, nil
}

func (s *Store) ListOperations() ([]Operation, error) {
	rows, err := s.store.DB().Query(
		`SELECT id, type, status, targets, created_at FROM operations ORDER BY created_at`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Operation
	for rows.Next() {
		var op Operation
		var createdAt int64
		if err := rows.Scan(&op.ID, &op.Type, &op.Status, &op.Targets, &createdAt); err != nil {
			return nil, err
		}
		op.CreatedAt = time.Unix(createdAt, 0)
		out = append(out, op)
	}
	return out, rows.Err()
}

// ServiceCount returns the number of services in a namespace.
func (s *Store) ServiceCount(namespaceID string) (int32, error) {
	var count int32
	err := s.store.DB().QueryRow(`SELECT COUNT(*) FROM services WHERE namespace_id = ?`, namespaceID).Scan(&count)
	return count, err
}

// InstanceCount returns the number of instances for a service.
func (s *Store) InstanceCount(serviceID string) (int32, error) {
	var count int32
	err := s.store.DB().QueryRow(`SELECT COUNT(*) FROM instances WHERE service_id = ?`, serviceID).Scan(&count)
	return count, err
}

type scanner interface{ Scan(dest ...any) error }

func scanNamespace(s scanner) (*Namespace, error) {
	var ns Namespace
	var createdAt int64
	err := s.Scan(&ns.ID, &ns.ARN, &ns.Name, &ns.Type, &ns.Description, &ns.Config, &createdAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errNamespaceNotFound
		}
		return nil, err
	}
	ns.CreatedAt = time.Unix(createdAt, 0)
	return &ns, nil
}

func scanService(s scanner) (*Service, error) {
	var svc Service
	var createdAt int64
	err := s.Scan(&svc.ID, &svc.ARN, &svc.Name, &svc.NamespaceID, &svc.Description, &svc.DnsConfig, &svc.HealthConfig, &createdAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errServiceNotFound
		}
		return nil, err
	}
	svc.CreatedAt = time.Unix(createdAt, 0)
	return &svc, nil
}
