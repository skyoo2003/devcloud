// SPDX-License-Identifier: Apache-2.0

// internal/services/route53/store.go
package route53

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"time"

	"github.com/skyoo2003/devcloud/internal/shared"
	"github.com/skyoo2003/devcloud/internal/storage/sqlite"
)

var (
	ErrZoneNotFound                  = errors.New("hosted zone not found")
	ErrZoneNotEmpty                  = errors.New("hosted zone contains record sets")
	ErrRecordNotFound                = errors.New("record set not found")
	ErrHealthCheckNotFound           = errors.New("health check not found")
	ErrTrafficPolicyNotFound         = errors.New("traffic policy not found")
	ErrQueryLoggingNotFound          = errors.New("query logging config not found")
	ErrTrafficPolicyInstanceNotFound = errors.New("traffic policy instance not found")
	ErrKeySigningKeyNotFound         = errors.New("key signing key not found")
	ErrCidrCollectionNotFound        = errors.New("CIDR collection not found")
	ErrReusableDelegationSetNotFound = errors.New("reusable delegation set not found")
)

var migrations = []sqlite.Migration{
	{Version: 1, SQL: `
		CREATE TABLE IF NOT EXISTS hosted_zones (
			id           TEXT PRIMARY KEY,
			name         TEXT NOT NULL,
			account_id   TEXT NOT NULL,
			caller_ref   TEXT NOT NULL,
			comment      TEXT NOT NULL DEFAULT '',
			record_count INTEGER NOT NULL DEFAULT 0,
			created_at   INTEGER NOT NULL
		);
		CREATE TABLE IF NOT EXISTS record_sets (
			zone_id      TEXT NOT NULL,
			name         TEXT NOT NULL,
			type         TEXT NOT NULL,
			ttl          INTEGER NOT NULL DEFAULT 300,
			records      TEXT NOT NULL DEFAULT '[]',
			alias_target TEXT,
			account_id   TEXT NOT NULL,
			PRIMARY KEY (zone_id, name, type, account_id)
		);
	`},
	{Version: 2, SQL: `
		CREATE TABLE IF NOT EXISTS health_checks (
			health_check_id              TEXT PRIMARY KEY,
			caller_reference             TEXT UNIQUE,
			type                         TEXT NOT NULL,
			ip_address                   TEXT,
			port                         INTEGER,
			resource_path                TEXT,
			fully_qualified_domain_name  TEXT,
			search_string                TEXT,
			request_interval             INTEGER DEFAULT 30,
			failure_threshold            INTEGER DEFAULT 3,
			status                       TEXT DEFAULT 'Healthy',
			account_id                   TEXT NOT NULL,
			created_at                   DATETIME NOT NULL
		);
		CREATE TABLE IF NOT EXISTS traffic_policies (
			id          TEXT NOT NULL,
			version     INTEGER NOT NULL,
			name        TEXT NOT NULL,
			type        TEXT,
			document    TEXT NOT NULL,
			comment     TEXT,
			account_id  TEXT NOT NULL,
			created_at  DATETIME NOT NULL,
			PRIMARY KEY (id, version)
		);
		CREATE TABLE IF NOT EXISTS traffic_policy_instances (
			id                TEXT PRIMARY KEY,
			hosted_zone_id    TEXT NOT NULL,
			name              TEXT NOT NULL,
			traffic_policy_id TEXT NOT NULL,
			version           INTEGER NOT NULL,
			ttl               INTEGER DEFAULT 300,
			state             TEXT DEFAULT 'Applied',
			account_id        TEXT NOT NULL,
			created_at        DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
		);
		CREATE TABLE IF NOT EXISTS query_logging_configs (
			id                       TEXT PRIMARY KEY,
			hosted_zone_id           TEXT NOT NULL,
			cloudwatch_log_group_arn TEXT NOT NULL,
			account_id               TEXT NOT NULL
		);
		CREATE TABLE IF NOT EXISTS hosted_zone_dnssec (
			hosted_zone_id TEXT PRIMARY KEY,
			status         TEXT NOT NULL DEFAULT 'Disabled',
			updated_at     DATETIME NOT NULL
		);
		CREATE TABLE IF NOT EXISTS key_signing_keys (
			hosted_zone_id TEXT NOT NULL,
			name           TEXT NOT NULL,
			key_id         TEXT NOT NULL,
			state          TEXT NOT NULL DEFAULT 'Pending',
			created_at     DATETIME NOT NULL,
			PRIMARY KEY (hosted_zone_id, name)
		);
		CREATE TABLE IF NOT EXISTS cidr_collections (
			cidr_collection_id TEXT PRIMARY KEY,
			name               TEXT NOT NULL,
			cidrs              TEXT NOT NULL,
			state              TEXT NOT NULL DEFAULT 'Created',
			account_id         TEXT NOT NULL,
			created_at         DATETIME NOT NULL
		);
		CREATE TABLE IF NOT EXISTS reusable_delegation_sets (
			delegation_set_id TEXT PRIMARY KEY,
			name              TEXT,
			key_ids           TEXT NOT NULL,
			state             TEXT NOT NULL DEFAULT 'Incomplete',
			account_id        TEXT NOT NULL,
			created_at        DATETIME NOT NULL
		);
	`},
}

type HostedZone struct {
	ID          string
	Name        string
	AccountID   string
	CallerRef   string
	Comment     string
	RecordCount int
	CreatedAt   time.Time
}

type RecordSet struct {
	ZoneID      string
	Name        string
	Type        string
	TTL         int64
	Records     []string
	AliasTarget *AliasTargetData
	AccountID   string
}

type AliasTargetData struct {
	DNSName              string `json:"DNSName"`
	HostedZoneId         string `json:"HostedZoneId"`
	EvaluateTargetHealth bool   `json:"EvaluateTargetHealth"`
}

type Route53Store struct {
	store *sqlite.Store
	tags  *shared.TagStore
}

func NewRoute53Store(dataDir string) (*Route53Store, error) {
	dbPath := filepath.Join(dataDir, "route53.db")
	allMigrations := append(migrations, shared.TagMigrations...)
	s, err := sqlite.Open(dbPath, allMigrations)
	if err != nil {
		return nil, err
	}
	return &Route53Store{store: s, tags: shared.NewTagStore(s)}, nil
}

func (s *Route53Store) Close() error { return s.store.Close() }

func (s *Route53Store) CreateZone(zone *HostedZone) error {
	_, err := s.store.DB().Exec(
		`INSERT INTO hosted_zones (id, name, account_id, caller_ref, comment, record_count, created_at)
		 VALUES (?, ?, ?, ?, ?, 0, ?)`,
		zone.ID, zone.Name, zone.AccountID, zone.CallerRef, zone.Comment,
		zone.CreatedAt.Unix(),
	)
	if sqlite.IsUniqueConstraintError(err) {
		return fmt.Errorf("zone %s already exists", zone.ID)
	}
	return err
}

func (s *Route53Store) GetZone(id, accountID string) (*HostedZone, error) {
	row := s.store.DB().QueryRow(
		`SELECT id, name, account_id, caller_ref, comment, record_count, created_at
		 FROM hosted_zones WHERE id = ? AND account_id = ?`,
		id, accountID,
	)
	return scanZone(row)
}

func (s *Route53Store) ListZones(accountID string) ([]HostedZone, error) {
	rows, err := s.store.DB().Query(
		`SELECT id, name, account_id, caller_ref, comment, record_count, created_at
		 FROM hosted_zones WHERE account_id = ? ORDER BY created_at`,
		accountID,
	)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var zones []HostedZone
	for rows.Next() {
		z, err := scanZone(rows)
		if err != nil {
			return nil, err
		}
		zones = append(zones, *z)
	}
	return zones, rows.Err()
}

func (s *Route53Store) DeleteZone(id, accountID string) error {
	// check record count
	var count int
	_ = s.store.DB().QueryRow(
		`SELECT record_count FROM hosted_zones WHERE id = ? AND account_id = ?`, id, accountID,
	).Scan(&count)
	if count > 0 {
		return ErrZoneNotEmpty
	}
	res, err := s.store.DB().Exec(
		`DELETE FROM hosted_zones WHERE id = ? AND account_id = ?`, id, accountID,
	)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrZoneNotFound
	}
	return nil
}

func (s *Route53Store) UpsertRecord(r *RecordSet) error {
	records, _ := json.Marshal(r.Records)
	var aliasJSON *string
	if r.AliasTarget != nil {
		b, _ := json.Marshal(r.AliasTarget)
		str := string(b)
		aliasJSON = &str
	}
	_, err := s.store.DB().Exec(
		`INSERT INTO record_sets (zone_id, name, type, ttl, records, alias_target, account_id)
		 VALUES (?, ?, ?, ?, ?, ?, ?)
		 ON CONFLICT(zone_id, name, type, account_id) DO UPDATE SET
		   ttl=excluded.ttl, records=excluded.records, alias_target=excluded.alias_target`,
		r.ZoneID, r.Name, r.Type, r.TTL, string(records), aliasJSON, r.AccountID,
	)
	if err != nil {
		return err
	}
	_, _ = s.store.DB().Exec(
		`UPDATE hosted_zones SET record_count = (
			SELECT COUNT(*) FROM record_sets WHERE zone_id = ? AND account_id = ?
		) WHERE id = ? AND account_id = ?`,
		r.ZoneID, r.AccountID, r.ZoneID, r.AccountID,
	)
	return nil
}

func (s *Route53Store) DeleteRecord(zoneID, name, recType, accountID string) error {
	res, err := s.store.DB().Exec(
		`DELETE FROM record_sets WHERE zone_id = ? AND name = ? AND type = ? AND account_id = ?`,
		zoneID, name, recType, accountID,
	)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrRecordNotFound
	}
	_, _ = s.store.DB().Exec(
		`UPDATE hosted_zones SET record_count = (
			SELECT COUNT(*) FROM record_sets WHERE zone_id = ? AND account_id = ?
		) WHERE id = ? AND account_id = ?`,
		zoneID, accountID, zoneID, accountID,
	)
	return nil
}

func (s *Route53Store) GetRecord(zoneID, name, recType, accountID string) (*RecordSet, error) {
	row := s.store.DB().QueryRow(
		`SELECT zone_id, name, type, ttl, records, alias_target, account_id
		 FROM record_sets WHERE zone_id = ? AND name = ? AND type = ? AND account_id = ?`,
		zoneID, name, recType, accountID,
	)
	return scanRecord(row)
}

func (s *Route53Store) ListRecords(zoneID, accountID string) ([]RecordSet, error) {
	rows, err := s.store.DB().Query(
		`SELECT zone_id, name, type, ttl, records, alias_target, account_id
		 FROM record_sets WHERE zone_id = ? AND account_id = ? ORDER BY name, type`,
		zoneID, accountID,
	)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var recs []RecordSet
	for rows.Next() {
		r, err := scanRecord(rows)
		if err != nil {
			return nil, err
		}
		recs = append(recs, *r)
	}
	return recs, rows.Err()
}

type scanner interface {
	Scan(dest ...any) error
}

func scanZone(s scanner) (*HostedZone, error) {
	var z HostedZone
	var createdAt int64
	err := s.Scan(&z.ID, &z.Name, &z.AccountID, &z.CallerRef, &z.Comment, &z.RecordCount, &createdAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrZoneNotFound
		}
		return nil, err
	}
	z.CreatedAt = time.Unix(createdAt, 0)
	return &z, nil
}

func scanRecord(s scanner) (*RecordSet, error) {
	var r RecordSet
	var recordsJSON string
	var aliasJSON sql.NullString
	err := s.Scan(&r.ZoneID, &r.Name, &r.Type, &r.TTL, &recordsJSON, &aliasJSON, &r.AccountID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrRecordNotFound
		}
		return nil, err
	}
	_ = json.Unmarshal([]byte(recordsJSON), &r.Records)
	if aliasJSON.Valid && aliasJSON.String != "" {
		r.AliasTarget = &AliasTargetData{}
		_ = json.Unmarshal([]byte(aliasJSON.String), r.AliasTarget)
	}
	return &r, nil
}

// --- Health Checks ---

type HealthCheck struct {
	ID                       string
	CallerReference          string
	Type                     string
	IPAddress                string
	Port                     int
	ResourcePath             string
	FullyQualifiedDomainName string
	SearchString             string
	RequestInterval          int
	FailureThreshold         int
	Status                   string
	AccountID                string
	CreatedAt                time.Time
}

func (s *Route53Store) CreateHealthCheck(hc *HealthCheck) error {
	_, err := s.store.DB().Exec(
		`INSERT INTO health_checks
		 (health_check_id, caller_reference, type, ip_address, port, resource_path,
		  fully_qualified_domain_name, search_string, request_interval, failure_threshold,
		  status, account_id, created_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		hc.ID, hc.CallerReference, hc.Type, hc.IPAddress, hc.Port, hc.ResourcePath,
		hc.FullyQualifiedDomainName, hc.SearchString, hc.RequestInterval, hc.FailureThreshold,
		hc.Status, hc.AccountID, hc.CreatedAt.UTC().Format(time.RFC3339),
	)
	if sqlite.IsUniqueConstraintError(err) {
		return fmt.Errorf("health check with caller reference already exists")
	}
	return err
}

func (s *Route53Store) GetHealthCheck(id, accountID string) (*HealthCheck, error) {
	row := s.store.DB().QueryRow(
		`SELECT health_check_id, caller_reference, type, ip_address, port, resource_path,
		        fully_qualified_domain_name, search_string, request_interval, failure_threshold,
		        status, account_id, created_at
		 FROM health_checks WHERE health_check_id = ? AND account_id = ?`, id, accountID,
	)
	return scanHealthCheck(row)
}

func (s *Route53Store) ListHealthChecks(accountID string) ([]HealthCheck, error) {
	rows, err := s.store.DB().Query(
		`SELECT health_check_id, caller_reference, type, ip_address, port, resource_path,
		        fully_qualified_domain_name, search_string, request_interval, failure_threshold,
		        status, account_id, created_at
		 FROM health_checks WHERE account_id = ? ORDER BY created_at`, accountID,
	)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var hcs []HealthCheck
	for rows.Next() {
		hc, err := scanHealthCheck(rows)
		if err != nil {
			return nil, err
		}
		hcs = append(hcs, *hc)
	}
	return hcs, rows.Err()
}

func (s *Route53Store) UpdateHealthCheck(hc *HealthCheck) error {
	res, err := s.store.DB().Exec(
		`UPDATE health_checks SET type=?, ip_address=?, port=?, resource_path=?,
		  fully_qualified_domain_name=?, search_string=?, request_interval=?, failure_threshold=?
		 WHERE health_check_id=? AND account_id=?`,
		hc.Type, hc.IPAddress, hc.Port, hc.ResourcePath, hc.FullyQualifiedDomainName,
		hc.SearchString, hc.RequestInterval, hc.FailureThreshold, hc.ID, hc.AccountID,
	)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrHealthCheckNotFound
	}
	return nil
}

func (s *Route53Store) DeleteHealthCheck(id, accountID string) error {
	res, err := s.store.DB().Exec(
		`DELETE FROM health_checks WHERE health_check_id = ? AND account_id = ?`, id, accountID,
	)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrHealthCheckNotFound
	}
	return nil
}

func scanHealthCheck(s scanner) (*HealthCheck, error) {
	var hc HealthCheck
	var ip, rp, fqdn, ss sql.NullString
	var port sql.NullInt64
	var createdAtStr string
	err := s.Scan(
		&hc.ID, &hc.CallerReference, &hc.Type, &ip, &port, &rp,
		&fqdn, &ss, &hc.RequestInterval, &hc.FailureThreshold,
		&hc.Status, &hc.AccountID, &createdAtStr,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrHealthCheckNotFound
		}
		return nil, err
	}
	if ip.Valid {
		hc.IPAddress = ip.String
	}
	if port.Valid {
		hc.Port = int(port.Int64)
	}
	if rp.Valid {
		hc.ResourcePath = rp.String
	}
	if fqdn.Valid {
		hc.FullyQualifiedDomainName = fqdn.String
	}
	if ss.Valid {
		hc.SearchString = ss.String
	}
	hc.CreatedAt, _ = time.Parse(time.RFC3339, createdAtStr)
	return &hc, nil
}

// --- Traffic Policies ---

type TrafficPolicy struct {
	ID        string
	Version   int
	Name      string
	Type      string
	Document  string
	Comment   string
	AccountID string
	CreatedAt time.Time
}

type TrafficPolicyInstance struct {
	ID              string
	HostedZoneID    string
	Name            string
	TrafficPolicyID string
	Version         int
	TTL             int
	State           string
	AccountID       string
	RoutingPolicy   string
}

func (s *Route53Store) CreateTrafficPolicy(tp *TrafficPolicy) error {
	_, err := s.store.DB().Exec(
		`INSERT INTO traffic_policies (id, version, name, type, document, comment, account_id, created_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		tp.ID, tp.Version, tp.Name, tp.Type, tp.Document, tp.Comment, tp.AccountID,
		tp.CreatedAt.UTC().Format(time.RFC3339),
	)
	return err
}

func (s *Route53Store) GetTrafficPolicy(id string, version int, accountID string) (*TrafficPolicy, error) {
	row := s.store.DB().QueryRow(
		`SELECT id, version, name, type, document, comment, account_id, created_at
		 FROM traffic_policies WHERE id = ? AND version = ? AND account_id = ?`,
		id, version, accountID,
	)
	return scanTrafficPolicy(row)
}

func (s *Route53Store) ListTrafficPolicies(accountID string) ([]TrafficPolicy, error) {
	rows, err := s.store.DB().Query(
		`SELECT id, version, name, type, document, comment, account_id, created_at
		 FROM traffic_policies WHERE account_id = ? ORDER BY created_at`, accountID,
	)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var tps []TrafficPolicy
	for rows.Next() {
		tp, err := scanTrafficPolicy(rows)
		if err != nil {
			return nil, err
		}
		tps = append(tps, *tp)
	}
	return tps, rows.Err()
}

func (s *Route53Store) DeleteTrafficPolicy(id string, version int, accountID string) error {
	res, err := s.store.DB().Exec(
		`DELETE FROM traffic_policies WHERE id = ? AND version = ? AND account_id = ?`,
		id, version, accountID,
	)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrTrafficPolicyNotFound
	}
	return nil
}

func scanTrafficPolicy(s scanner) (*TrafficPolicy, error) {
	var tp TrafficPolicy
	var tpType sql.NullString
	var comment sql.NullString
	var createdAtStr string
	err := s.Scan(&tp.ID, &tp.Version, &tp.Name, &tpType, &tp.Document, &comment, &tp.AccountID, &createdAtStr)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrTrafficPolicyNotFound
		}
		return nil, err
	}
	if tpType.Valid {
		tp.Type = tpType.String
	}
	if comment.Valid {
		tp.Comment = comment.String
	}
	tp.CreatedAt, _ = time.Parse(time.RFC3339, createdAtStr)
	return &tp, nil
}

func (s *Route53Store) CreateTrafficPolicyInstance(tpi *TrafficPolicyInstance) error {
	_, err := s.store.DB().Exec(
		`INSERT INTO traffic_policy_instances
		 (id, hosted_zone_id, name, traffic_policy_id, version, ttl, state, account_id)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		tpi.ID, tpi.HostedZoneID, tpi.Name, tpi.TrafficPolicyID, tpi.Version,
		tpi.TTL, tpi.State, tpi.AccountID,
	)
	return err
}

func (s *Route53Store) UpdateTrafficPolicyInstance(tpi *TrafficPolicyInstance) error {
	res, err := s.store.DB().Exec(
		`UPDATE traffic_policy_instances
		 SET name=?, traffic_policy_id=?, version=?, ttl=?, state=?
		 WHERE id=? AND account_id=?`,
		tpi.Name, tpi.TrafficPolicyID, tpi.Version, tpi.TTL, tpi.State, tpi.ID, tpi.AccountID,
	)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrTrafficPolicyInstanceNotFound
	}
	return nil
}

func (s *Route53Store) DeleteTrafficPolicyInstance(id, accountID string) error {
	res, err := s.store.DB().Exec(
		`DELETE FROM traffic_policy_instances WHERE id = ? AND account_id = ?`, id, accountID,
	)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrTrafficPolicyInstanceNotFound
	}
	return nil
}

func (s *Route53Store) GetTrafficPolicyInstance(id, accountID string) (*TrafficPolicyInstance, error) {
	row := s.store.DB().QueryRow(
		`SELECT id, hosted_zone_id, name, traffic_policy_id, version, ttl, state, account_id
		 FROM traffic_policy_instances WHERE id = ? AND account_id = ?`, id, accountID,
	)
	var tpi TrafficPolicyInstance
	err := row.Scan(&tpi.ID, &tpi.HostedZoneID, &tpi.Name, &tpi.TrafficPolicyID, &tpi.Version, &tpi.TTL, &tpi.State, &tpi.AccountID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrTrafficPolicyInstanceNotFound
		}
		return nil, err
	}
	return &tpi, nil
}

func (s *Route53Store) ListTrafficPolicyInstances(accountID string) ([]TrafficPolicyInstance, error) {
	rows, err := s.store.DB().Query(
		`SELECT id, hosted_zone_id, name, traffic_policy_id, version, ttl, state, account_id
		 FROM traffic_policy_instances WHERE account_id = ? ORDER BY created_at`, accountID,
	)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var tpis []TrafficPolicyInstance
	for rows.Next() {
		var tpi TrafficPolicyInstance
		if err := rows.Scan(&tpi.ID, &tpi.HostedZoneID, &tpi.Name, &tpi.TrafficPolicyID, &tpi.Version, &tpi.TTL, &tpi.State, &tpi.AccountID); err != nil {
			return nil, err
		}
		tpis = append(tpis, tpi)
	}
	return tpis, rows.Err()
}

// --- Query Logging Configs ---

type QueryLoggingConfig struct {
	ID                    string
	HostedZoneID          string
	CloudWatchLogGroupARN string
	AccountID             string
}

func (s *Route53Store) CreateQueryLoggingConfig(qlc *QueryLoggingConfig) error {
	_, err := s.store.DB().Exec(
		`INSERT INTO query_logging_configs (id, hosted_zone_id, cloudwatch_log_group_arn, account_id)
		 VALUES (?, ?, ?, ?)`,
		qlc.ID, qlc.HostedZoneID, qlc.CloudWatchLogGroupARN, qlc.AccountID,
	)
	return err
}

func (s *Route53Store) GetQueryLoggingConfig(id, accountID string) (*QueryLoggingConfig, error) {
	row := s.store.DB().QueryRow(
		`SELECT id, hosted_zone_id, cloudwatch_log_group_arn, account_id
		 FROM query_logging_configs WHERE id = ? AND account_id = ?`, id, accountID,
	)
	var qlc QueryLoggingConfig
	err := row.Scan(&qlc.ID, &qlc.HostedZoneID, &qlc.CloudWatchLogGroupARN, &qlc.AccountID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrQueryLoggingNotFound
		}
		return nil, err
	}
	return &qlc, nil
}

func (s *Route53Store) ListQueryLoggingConfigs(accountID string) ([]QueryLoggingConfig, error) {
	rows, err := s.store.DB().Query(
		`SELECT id, hosted_zone_id, cloudwatch_log_group_arn, account_id
		 FROM query_logging_configs WHERE account_id = ?`, accountID,
	)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var qlcs []QueryLoggingConfig
	for rows.Next() {
		var qlc QueryLoggingConfig
		if err := rows.Scan(&qlc.ID, &qlc.HostedZoneID, &qlc.CloudWatchLogGroupARN, &qlc.AccountID); err != nil {
			return nil, err
		}
		qlcs = append(qlcs, qlc)
	}
	return qlcs, rows.Err()
}

func (s *Route53Store) DeleteQueryLoggingConfig(id, accountID string) error {
	res, err := s.store.DB().Exec(
		`DELETE FROM query_logging_configs WHERE id = ? AND account_id = ?`, id, accountID,
	)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrQueryLoggingNotFound
	}
	return nil
}

// --- DNSSEC ---

type HostedZoneDNSSEC struct {
	HostedZoneID string
	Status       string // "Enabled" or "Disabled"
	UpdatedAt    time.Time
}

func (s *Route53Store) EnableDNSSEC(zoneID, accountID string) error {
	_, err := s.store.DB().Exec(
		`INSERT INTO hosted_zone_dnssec (hosted_zone_id, status, updated_at)
		 VALUES (?, 'Enabled', ?)
		 ON CONFLICT(hosted_zone_id) DO UPDATE SET status='Enabled', updated_at=excluded.updated_at`,
		zoneID, time.Now().UTC().Format(time.RFC3339),
	)
	return err
}

func (s *Route53Store) DisableDNSSEC(zoneID, accountID string) error {
	_, err := s.store.DB().Exec(
		`INSERT INTO hosted_zone_dnssec (hosted_zone_id, status, updated_at)
		 VALUES (?, 'Disabled', ?)
		 ON CONFLICT(hosted_zone_id) DO UPDATE SET status='Disabled', updated_at=excluded.updated_at`,
		zoneID, time.Now().UTC().Format(time.RFC3339),
	)
	return err
}

func (s *Route53Store) GetDNSSEC(zoneID, accountID string) (*HostedZoneDNSSEC, error) {
	row := s.store.DB().QueryRow(
		`SELECT hosted_zone_id, status, updated_at
		 FROM hosted_zone_dnssec WHERE hosted_zone_id = ?`, zoneID,
	)
	var dnssec HostedZoneDNSSEC
	err := row.Scan(&dnssec.HostedZoneID, &dnssec.Status, &dnssec.UpdatedAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return &HostedZoneDNSSEC{HostedZoneID: zoneID, Status: "Disabled"}, nil
		}
		return nil, err
	}
	return &dnssec, nil
}

// --- Key Signing Key ---

type KeySigningKey struct {
	HostedZoneID string
	Name         string
	KeyID        string
	State        string // "Pending", "Signing", "Deprecating", "Deleted"
	CreatedAt    time.Time
}

func (s *Route53Store) CreateKeySigningKey(kss *KeySigningKey) error {
	_, err := s.store.DB().Exec(
		`INSERT INTO key_signing_keys (hosted_zone_id, name, key_id, state, created_at)
		 VALUES (?, ?, ?, ?, ?)`,
		kss.HostedZoneID, kss.Name, kss.KeyID, kss.State, kss.CreatedAt.UTC().Format(time.RFC3339),
	)
	return err
}

func (s *Route53Store) DeleteKeySigningKey(zoneID, name, accountID string) error {
	res, err := s.store.DB().Exec(
		`DELETE FROM key_signing_keys WHERE hosted_zone_id = ? AND name = ?`, zoneID, name,
	)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrKeySigningKeyNotFound
	}
	return nil
}

func (s *Route53Store) ListKeySigningKeys(accountID string) ([]KeySigningKey, error) {
	rows, err := s.store.DB().Query(
		`SELECT hosted_zone_id, name, key_id, state, created_at
		 FROM key_signing_keys WHERE hosted_zone_id IN (
		   SELECT id FROM hosted_zones WHERE account_id = ?
		 ) ORDER BY created_at`, accountID,
	)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var kss []KeySigningKey
	for rows.Next() {
		var kssItem KeySigningKey
		var createdAtStr string
		if err := rows.Scan(&kssItem.HostedZoneID, &kssItem.Name, &kssItem.KeyID, &kssItem.State, &createdAtStr); err != nil {
			return nil, err
		}
		kssItem.CreatedAt, _ = time.Parse(time.RFC3339, createdAtStr)
		kss = append(kss, kssItem)
	}
	return kss, rows.Err()
}

// --- Cidr Collection ---

type CidrCollection struct {
	CidrCollectionID string
	Name             string
	Cidrs            []string // JSON array
	State            string   // "Created", "Deleting"
	AccountID        string
	CreatedAt        time.Time
}

func (s *Route53Store) CreateCidrCollection(cc *CidrCollection) error {
	cidrsJSON, _ := json.Marshal(cc.Cidrs)
	_, err := s.store.DB().Exec(
		`INSERT INTO cidr_collections (cidr_collection_id, name, cidrs, state, account_id, created_at)
		 VALUES (?, ?, ?, ?, ?, ?)`,
		cc.CidrCollectionID, cc.Name, string(cidrsJSON), cc.State, cc.AccountID, cc.CreatedAt.UTC().Format(time.RFC3339),
	)
	return err
}

func (s *Route53Store) DeleteCidrCollection(id, accountID string) error {
	res, err := s.store.DB().Exec(
		`DELETE FROM cidr_collections WHERE cidr_collection_id = ? AND account_id = ?`, id, accountID,
	)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrCidrCollectionNotFound
	}
	return nil
}

func (s *Route53Store) ListCidrCollections(accountID string) ([]CidrCollection, error) {
	rows, err := s.store.DB().Query(
		`SELECT cidr_collection_id, name, cidrs, state, account_id, created_at
		 FROM cidr_collections WHERE account_id = ? ORDER BY created_at`, accountID,
	)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var cc []CidrCollection
	for rows.Next() {
		var c CidrCollection
		var cidrsJSON string
		var createdAtStr string
		if err := rows.Scan(&c.CidrCollectionID, &c.Name, &cidrsJSON, &c.State, &c.AccountID, &createdAtStr); err != nil {
			return nil, err
		}
		_ = json.Unmarshal([]byte(cidrsJSON), &c.Cidrs)
		c.CreatedAt, _ = time.Parse(time.RFC3339, createdAtStr)
		cc = append(cc, c)
	}
	return cc, rows.Err()
}

// --- Reusable Delegation Set ---

type ReusableDelegationSet struct {
	DelegationSetID string
	Name            string
	KeyIds          []string // JSON array
	State           string   // "Incomplete", "Complete", "Deleting"
	AccountID       string
	CreatedAt       time.Time
}

func (s *Route53Store) CreateReusableDelegationSet(ds *ReusableDelegationSet) error {
	keyIdsJSON, _ := json.Marshal(ds.KeyIds)
	_, err := s.store.DB().Exec(
		`INSERT INTO reusable_delegation_sets (delegation_set_id, name, key_ids, state, account_id, created_at)
		 VALUES (?, ?, ?, ?, ?, ?)`,
		ds.DelegationSetID, ds.Name, string(keyIdsJSON), ds.State, ds.AccountID, ds.CreatedAt.UTC().Format(time.RFC3339),
	)
	return err
}

func (s *Route53Store) DeleteReusableDelegationSet(id, accountID string) error {
	res, err := s.store.DB().Exec(
		`DELETE FROM reusable_delegation_sets WHERE delegation_set_id = ? AND account_id = ?`, id, accountID,
	)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrReusableDelegationSetNotFound
	}
	return nil
}

func (s *Route53Store) GetReusableDelegationSet(id, accountID string) (*ReusableDelegationSet, error) {
	row := s.store.DB().QueryRow(
		`SELECT delegation_set_id, name, key_ids, state, account_id, created_at
		 FROM reusable_delegation_sets WHERE delegation_set_id = ? AND account_id = ?`, id, accountID,
	)
	var ds ReusableDelegationSet
	var keyIdsJSON string
	var createdAtStr string
	err := row.Scan(&ds.DelegationSetID, &ds.Name, &keyIdsJSON, &ds.State, &ds.AccountID, &createdAtStr)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrReusableDelegationSetNotFound
		}
		return nil, err
	}
	_ = json.Unmarshal([]byte(keyIdsJSON), &ds.KeyIds)
	ds.CreatedAt, _ = time.Parse(time.RFC3339, createdAtStr)
	return &ds, nil
}

func (s *Route53Store) ListReusableDelegationSets(accountID string) ([]ReusableDelegationSet, error) {
	rows, err := s.store.DB().Query(
		`SELECT delegation_set_id, name, key_ids, state, account_id, created_at
		 FROM reusable_delegation_sets WHERE account_id = ? ORDER BY created_at`, accountID,
	)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var ds []ReusableDelegationSet
	for rows.Next() {
		var d ReusableDelegationSet
		var keyIdsJSON string
		var createdAtStr string
		if err := rows.Scan(&d.DelegationSetID, &d.Name, &keyIdsJSON, &d.State, &d.AccountID, &createdAtStr); err != nil {
			return nil, err
		}
		_ = json.Unmarshal([]byte(keyIdsJSON), &d.KeyIds)
		d.CreatedAt, _ = time.Parse(time.RFC3339, createdAtStr)
		ds = append(ds, d)
	}
	return ds, rows.Err()
}
