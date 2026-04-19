// SPDX-License-Identifier: Apache-2.0

// internal/services/route53resolver/store.go
package route53resolver

import (
	"database/sql"
	"encoding/json"
	"errors"
	"path/filepath"
	"time"

	"github.com/skyoo2003/devcloud/internal/shared"
	"github.com/skyoo2003/devcloud/internal/storage/sqlite"
)

var (
	errNotFound = errors.New("not found")
)

var migrations = []sqlite.Migration{
	{Version: 1, SQL: `
CREATE TABLE IF NOT EXISTS resolver_endpoints (
    id              TEXT PRIMARY KEY,
    arn             TEXT NOT NULL UNIQUE,
    name            TEXT NOT NULL DEFAULT '',
    direction       TEXT NOT NULL DEFAULT 'INBOUND',
    security_groups TEXT NOT NULL DEFAULT '[]',
    ip_addresses    TEXT NOT NULL DEFAULT '[]',
    status          TEXT NOT NULL DEFAULT 'OPERATIONAL',
    created_at      INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS resolver_rules (
    id                   TEXT PRIMARY KEY,
    arn                  TEXT NOT NULL UNIQUE,
    name                 TEXT NOT NULL DEFAULT '',
    domain_name          TEXT NOT NULL DEFAULT '',
    rule_type            TEXT NOT NULL DEFAULT 'FORWARD',
    resolver_endpoint_id TEXT NOT NULL DEFAULT '',
    target_ips           TEXT NOT NULL DEFAULT '[]',
    status               TEXT NOT NULL DEFAULT 'COMPLETE',
    created_at           INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS rule_associations (
    id               TEXT PRIMARY KEY,
    resolver_rule_id TEXT NOT NULL,
    vpc_id           TEXT NOT NULL,
    status           TEXT NOT NULL DEFAULT 'COMPLETE',
    name             TEXT NOT NULL DEFAULT ''
);
CREATE TABLE IF NOT EXISTS query_log_configs (
    id          TEXT PRIMARY KEY,
    arn         TEXT NOT NULL UNIQUE,
    name        TEXT NOT NULL,
    destination TEXT NOT NULL DEFAULT '',
    status      TEXT NOT NULL DEFAULT 'ACTIVE',
    created_at  INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS query_log_associations (
    id         TEXT PRIMARY KEY,
    config_id  TEXT NOT NULL,
    resource_id TEXT NOT NULL,
    status     TEXT NOT NULL DEFAULT 'ACTIVE'
);
CREATE TABLE IF NOT EXISTS firewall_rule_groups (
    id         TEXT PRIMARY KEY,
    arn        TEXT NOT NULL UNIQUE,
    name       TEXT NOT NULL,
    status     TEXT NOT NULL DEFAULT 'COMPLETE',
    rule_count INTEGER NOT NULL DEFAULT 0,
    created_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS firewall_rules (
    group_id       TEXT NOT NULL,
    domain_list_id TEXT NOT NULL,
    name           TEXT NOT NULL DEFAULT '',
    priority       INTEGER NOT NULL DEFAULT 100,
    action         TEXT NOT NULL DEFAULT 'BLOCK',
    block_response TEXT NOT NULL DEFAULT 'NODATA',
    PRIMARY KEY (group_id, domain_list_id)
);
CREATE TABLE IF NOT EXISTS firewall_domain_lists (
    id           TEXT PRIMARY KEY,
    arn          TEXT NOT NULL UNIQUE,
    name         TEXT NOT NULL,
    status       TEXT NOT NULL DEFAULT 'COMPLETE',
    domain_count INTEGER NOT NULL DEFAULT 0,
    created_at   INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS firewall_rule_group_associations (
    id       TEXT PRIMARY KEY,
    group_id TEXT NOT NULL,
    vpc_id   TEXT NOT NULL,
    name     TEXT NOT NULL DEFAULT '',
    priority INTEGER NOT NULL DEFAULT 100,
    status   TEXT NOT NULL DEFAULT 'COMPLETE'
);
CREATE TABLE IF NOT EXISTS firewall_domains (
    list_id TEXT NOT NULL,
    domain  TEXT NOT NULL,
    PRIMARY KEY (list_id, domain)
);
`},
}

// --- models ---

type endpointRow struct {
	ID             string
	ARN            string
	Name           string
	Direction      string
	SecurityGroups string // JSON []string
	IPAddresses    string // JSON []ipAddrEntry
	Status         string
	CreatedAt      int64
}

type ipAddrEntry struct {
	SubnetID string `json:"subnetId"`
	IP       string `json:"ip"`
	IPID     string `json:"ipId"`
	Status   string `json:"status"`
}

type ruleRow struct {
	ID                 string
	ARN                string
	Name               string
	DomainName         string
	RuleType           string
	ResolverEndpointID string
	TargetIPs          string // JSON
	Status             string
	CreatedAt          int64
}

type ruleAssocRow struct {
	ID             string
	ResolverRuleID string
	VPCID          string
	Status         string
	Name           string
}

type queryLogRow struct {
	ID          string
	ARN         string
	Name        string
	Destination string
	Status      string
	CreatedAt   int64
}

type queryLogAssocRow struct {
	ID         string
	ConfigID   string
	ResourceID string
	Status     string
}

type fwRuleGroupRow struct {
	ID        string
	ARN       string
	Name      string
	Status    string
	RuleCount int
	CreatedAt int64
}

type fwRuleRow struct {
	GroupID       string
	DomainListID  string
	Name          string
	Priority      int
	Action        string
	BlockResponse string
}

type fwDomainListRow struct {
	ID          string
	ARN         string
	Name        string
	Status      string
	DomainCount int
	CreatedAt   int64
}

type fwRuleGroupAssocRow struct {
	ID       string
	GroupID  string
	VPCID    string
	Name     string
	Priority int
	Status   string
}

// --- Store ---

type Store struct {
	store *sqlite.Store
	tags  *shared.TagStore
}

func NewStore(dataDir string) (*Store, error) {
	allMigrations := append(migrations, shared.TagMigrations...)
	dbPath := filepath.Join(dataDir, "route53resolver.db")
	s, err := sqlite.Open(dbPath, allMigrations)
	if err != nil {
		return nil, err
	}
	return &Store{store: s, tags: shared.NewTagStore(s)}, nil
}

func (s *Store) Close() error { return s.store.Close() }

func (s *Store) db() *sql.DB { return s.store.DB() }

// ---- helpers ----

func marshalJSON(v any) string {
	b, _ := json.Marshal(v)
	return string(b)
}

// ===================== ResolverEndpoints =====================

func (s *Store) CreateEndpoint(id, arn, name, direction string, sgIDs []string, ipAddrs []ipAddrEntry) (*endpointRow, error) {
	now := time.Now().Unix()
	sgJSON := marshalJSON(sgIDs)
	ipJSON := marshalJSON(ipAddrs)
	_, err := s.db().Exec(
		`INSERT INTO resolver_endpoints (id,arn,name,direction,security_groups,ip_addresses,created_at) VALUES (?,?,?,?,?,?,?)`,
		id, arn, name, direction, sgJSON, ipJSON, now,
	)
	if err != nil {
		return nil, err
	}
	return &endpointRow{ID: id, ARN: arn, Name: name, Direction: direction,
		SecurityGroups: sgJSON, IPAddresses: ipJSON, Status: "OPERATIONAL", CreatedAt: now}, nil
}

func (s *Store) GetEndpoint(id string) (*endpointRow, error) {
	row := s.db().QueryRow(
		`SELECT id,arn,name,direction,security_groups,ip_addresses,status,created_at FROM resolver_endpoints WHERE id=?`, id)
	return scanEndpoint(row)
}

func (s *Store) ListEndpoints() ([]endpointRow, error) {
	rows, err := s.db().Query(
		`SELECT id,arn,name,direction,security_groups,ip_addresses,status,created_at FROM resolver_endpoints ORDER BY created_at`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var result []endpointRow
	for rows.Next() {
		r, err := scanEndpoint(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, *r)
	}
	return result, rows.Err()
}

func (s *Store) UpdateEndpoint(id, name string) error {
	res, err := s.db().Exec(`UPDATE resolver_endpoints SET name=? WHERE id=?`, name, id)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errNotFound
	}
	return nil
}

func (s *Store) DeleteEndpoint(id string) error {
	res, err := s.db().Exec(`DELETE FROM resolver_endpoints WHERE id=?`, id)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errNotFound
	}
	return nil
}

func (s *Store) AssociateEndpointIP(id string, ip ipAddrEntry) (*endpointRow, error) {
	ep, err := s.GetEndpoint(id)
	if err != nil {
		return nil, err
	}
	var ips []ipAddrEntry
	_ = json.Unmarshal([]byte(ep.IPAddresses), &ips)
	ips = append(ips, ip)
	ipJSON := marshalJSON(ips)
	if _, err := s.db().Exec(`UPDATE resolver_endpoints SET ip_addresses=? WHERE id=?`, ipJSON, id); err != nil {
		return nil, err
	}
	ep.IPAddresses = ipJSON
	return ep, nil
}

func (s *Store) DisassociateEndpointIP(id, ipID string) (*endpointRow, error) {
	ep, err := s.GetEndpoint(id)
	if err != nil {
		return nil, err
	}
	var ips []ipAddrEntry
	_ = json.Unmarshal([]byte(ep.IPAddresses), &ips)
	filtered := ips[:0]
	for _, ip := range ips {
		if ip.IPID != ipID {
			filtered = append(filtered, ip)
		}
	}
	ipJSON := marshalJSON(filtered)
	if _, err := s.db().Exec(`UPDATE resolver_endpoints SET ip_addresses=? WHERE id=?`, ipJSON, id); err != nil {
		return nil, err
	}
	ep.IPAddresses = ipJSON
	return ep, nil
}

func (s *Store) ListEndpointIPs(id string) ([]ipAddrEntry, error) {
	ep, err := s.GetEndpoint(id)
	if err != nil {
		return nil, err
	}
	var ips []ipAddrEntry
	_ = json.Unmarshal([]byte(ep.IPAddresses), &ips)
	return ips, nil
}

type scanner interface{ Scan(dest ...any) error }

func scanEndpoint(sc scanner) (*endpointRow, error) {
	var r endpointRow
	err := sc.Scan(&r.ID, &r.ARN, &r.Name, &r.Direction, &r.SecurityGroups, &r.IPAddresses, &r.Status, &r.CreatedAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errNotFound
		}
		return nil, err
	}
	return &r, nil
}

// ===================== ResolverRules =====================

func (s *Store) CreateRule(id, arn, name, domainName, ruleType, endpointID string, targetIPs []map[string]any) (*ruleRow, error) {
	now := time.Now().Unix()
	tJSON := marshalJSON(targetIPs)
	_, err := s.db().Exec(
		`INSERT INTO resolver_rules (id,arn,name,domain_name,rule_type,resolver_endpoint_id,target_ips,created_at) VALUES (?,?,?,?,?,?,?,?)`,
		id, arn, name, domainName, ruleType, endpointID, tJSON, now,
	)
	if err != nil {
		return nil, err
	}
	return &ruleRow{ID: id, ARN: arn, Name: name, DomainName: domainName, RuleType: ruleType,
		ResolverEndpointID: endpointID, TargetIPs: tJSON, Status: "COMPLETE", CreatedAt: now}, nil
}

func (s *Store) GetRule(id string) (*ruleRow, error) {
	row := s.db().QueryRow(
		`SELECT id,arn,name,domain_name,rule_type,resolver_endpoint_id,target_ips,status,created_at FROM resolver_rules WHERE id=?`, id)
	return scanRule(row)
}

func (s *Store) ListRules() ([]ruleRow, error) {
	rows, err := s.db().Query(
		`SELECT id,arn,name,domain_name,rule_type,resolver_endpoint_id,target_ips,status,created_at FROM resolver_rules ORDER BY created_at`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var result []ruleRow
	for rows.Next() {
		r, err := scanRule(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, *r)
	}
	return result, rows.Err()
}

func (s *Store) UpdateRule(id, name, endpointID string, targetIPs []map[string]any) error {
	tJSON := marshalJSON(targetIPs)
	res, err := s.db().Exec(
		`UPDATE resolver_rules SET name=?,resolver_endpoint_id=?,target_ips=? WHERE id=?`,
		name, endpointID, tJSON, id)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errNotFound
	}
	return nil
}

func (s *Store) DeleteRule(id string) error {
	res, err := s.db().Exec(`DELETE FROM resolver_rules WHERE id=?`, id)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errNotFound
	}
	return nil
}

func scanRule(sc scanner) (*ruleRow, error) {
	var r ruleRow
	err := sc.Scan(&r.ID, &r.ARN, &r.Name, &r.DomainName, &r.RuleType, &r.ResolverEndpointID, &r.TargetIPs, &r.Status, &r.CreatedAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errNotFound
		}
		return nil, err
	}
	return &r, nil
}

// ===================== RuleAssociations =====================

func (s *Store) AssociateRule(id, ruleID, vpcID, name string) (*ruleAssocRow, error) {
	_, err := s.db().Exec(
		`INSERT INTO rule_associations (id,resolver_rule_id,vpc_id,name) VALUES (?,?,?,?)`,
		id, ruleID, vpcID, name)
	if err != nil {
		return nil, err
	}
	return &ruleAssocRow{ID: id, ResolverRuleID: ruleID, VPCID: vpcID, Status: "COMPLETE", Name: name}, nil
}

func (s *Store) DisassociateRule(ruleID, vpcID string) (*ruleAssocRow, error) {
	row := s.db().QueryRow(
		`SELECT id,resolver_rule_id,vpc_id,status,name FROM rule_associations WHERE resolver_rule_id=? AND vpc_id=?`,
		ruleID, vpcID)
	assoc, err := scanRuleAssoc(row)
	if err != nil {
		return nil, err
	}
	_, _ = s.db().Exec(`DELETE FROM rule_associations WHERE id=?`, assoc.ID)
	return assoc, nil
}

func (s *Store) GetRuleAssociation(id string) (*ruleAssocRow, error) {
	row := s.db().QueryRow(
		`SELECT id,resolver_rule_id,vpc_id,status,name FROM rule_associations WHERE id=?`, id)
	return scanRuleAssoc(row)
}

func (s *Store) ListRuleAssociations() ([]ruleAssocRow, error) {
	rows, err := s.db().Query(
		`SELECT id,resolver_rule_id,vpc_id,status,name FROM rule_associations ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var result []ruleAssocRow
	for rows.Next() {
		r, err := scanRuleAssoc(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, *r)
	}
	return result, rows.Err()
}

func scanRuleAssoc(sc scanner) (*ruleAssocRow, error) {
	var r ruleAssocRow
	err := sc.Scan(&r.ID, &r.ResolverRuleID, &r.VPCID, &r.Status, &r.Name)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errNotFound
		}
		return nil, err
	}
	return &r, nil
}

// ===================== QueryLogConfigs =====================

func (s *Store) CreateQueryLogConfig(id, arn, name, dest string) (*queryLogRow, error) {
	now := time.Now().Unix()
	_, err := s.db().Exec(
		`INSERT INTO query_log_configs (id,arn,name,destination,created_at) VALUES (?,?,?,?,?)`,
		id, arn, name, dest, now)
	if err != nil {
		return nil, err
	}
	return &queryLogRow{ID: id, ARN: arn, Name: name, Destination: dest, Status: "ACTIVE", CreatedAt: now}, nil
}

func (s *Store) GetQueryLogConfig(id string) (*queryLogRow, error) {
	row := s.db().QueryRow(
		`SELECT id,arn,name,destination,status,created_at FROM query_log_configs WHERE id=?`, id)
	return scanQueryLog(row)
}

func (s *Store) ListQueryLogConfigs() ([]queryLogRow, error) {
	rows, err := s.db().Query(
		`SELECT id,arn,name,destination,status,created_at FROM query_log_configs ORDER BY created_at`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var result []queryLogRow
	for rows.Next() {
		r, err := scanQueryLog(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, *r)
	}
	return result, rows.Err()
}

func (s *Store) DeleteQueryLogConfig(id string) error {
	res, err := s.db().Exec(`DELETE FROM query_log_configs WHERE id=?`, id)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errNotFound
	}
	return nil
}

func scanQueryLog(sc scanner) (*queryLogRow, error) {
	var r queryLogRow
	err := sc.Scan(&r.ID, &r.ARN, &r.Name, &r.Destination, &r.Status, &r.CreatedAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errNotFound
		}
		return nil, err
	}
	return &r, nil
}

// ===================== QueryLogAssociations =====================

func (s *Store) AssociateQueryLogConfig(id, configID, resourceID string) (*queryLogAssocRow, error) {
	_, err := s.db().Exec(
		`INSERT INTO query_log_associations (id,config_id,resource_id) VALUES (?,?,?)`,
		id, configID, resourceID)
	if err != nil {
		return nil, err
	}
	return &queryLogAssocRow{ID: id, ConfigID: configID, ResourceID: resourceID, Status: "ACTIVE"}, nil
}

func (s *Store) DisassociateQueryLogConfig(configID, resourceID string) (*queryLogAssocRow, error) {
	row := s.db().QueryRow(
		`SELECT id,config_id,resource_id,status FROM query_log_associations WHERE config_id=? AND resource_id=?`,
		configID, resourceID)
	assoc, err := scanQueryLogAssoc(row)
	if err != nil {
		return nil, err
	}
	_, _ = s.db().Exec(`DELETE FROM query_log_associations WHERE id=?`, assoc.ID)
	return assoc, nil
}

func (s *Store) GetQueryLogConfigAssociation(id string) (*queryLogAssocRow, error) {
	row := s.db().QueryRow(
		`SELECT id,config_id,resource_id,status FROM query_log_associations WHERE id=?`, id)
	return scanQueryLogAssoc(row)
}

func (s *Store) ListQueryLogConfigAssociations() ([]queryLogAssocRow, error) {
	rows, err := s.db().Query(
		`SELECT id,config_id,resource_id,status FROM query_log_associations ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var result []queryLogAssocRow
	for rows.Next() {
		r, err := scanQueryLogAssoc(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, *r)
	}
	return result, rows.Err()
}

func scanQueryLogAssoc(sc scanner) (*queryLogAssocRow, error) {
	var r queryLogAssocRow
	err := sc.Scan(&r.ID, &r.ConfigID, &r.ResourceID, &r.Status)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errNotFound
		}
		return nil, err
	}
	return &r, nil
}

// ===================== FirewallRuleGroups =====================

func (s *Store) CreateFirewallRuleGroup(id, arn, name string) (*fwRuleGroupRow, error) {
	now := time.Now().Unix()
	_, err := s.db().Exec(
		`INSERT INTO firewall_rule_groups (id,arn,name,created_at) VALUES (?,?,?,?)`,
		id, arn, name, now)
	if err != nil {
		return nil, err
	}
	return &fwRuleGroupRow{ID: id, ARN: arn, Name: name, Status: "COMPLETE", RuleCount: 0, CreatedAt: now}, nil
}

func (s *Store) GetFirewallRuleGroup(id string) (*fwRuleGroupRow, error) {
	row := s.db().QueryRow(
		`SELECT id,arn,name,status,rule_count,created_at FROM firewall_rule_groups WHERE id=?`, id)
	return scanFwRuleGroup(row)
}

func (s *Store) ListFirewallRuleGroups() ([]fwRuleGroupRow, error) {
	rows, err := s.db().Query(
		`SELECT id,arn,name,status,rule_count,created_at FROM firewall_rule_groups ORDER BY created_at`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var result []fwRuleGroupRow
	for rows.Next() {
		r, err := scanFwRuleGroup(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, *r)
	}
	return result, rows.Err()
}

func (s *Store) DeleteFirewallRuleGroup(id string) error {
	res, err := s.db().Exec(`DELETE FROM firewall_rule_groups WHERE id=?`, id)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errNotFound
	}
	return nil
}

func (s *Store) incrFwRuleGroupCount(groupID string, delta int) {
	_, _ = s.db().Exec(
		`UPDATE firewall_rule_groups SET rule_count = rule_count + ? WHERE id=?`, delta, groupID)
}

func scanFwRuleGroup(sc scanner) (*fwRuleGroupRow, error) {
	var r fwRuleGroupRow
	err := sc.Scan(&r.ID, &r.ARN, &r.Name, &r.Status, &r.RuleCount, &r.CreatedAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errNotFound
		}
		return nil, err
	}
	return &r, nil
}

// ===================== FirewallRules =====================

func (s *Store) CreateFirewallRule(groupID, domainListID, name string, priority int, action, blockResponse string) (*fwRuleRow, error) {
	_, err := s.db().Exec(
		`INSERT INTO firewall_rules (group_id,domain_list_id,name,priority,action,block_response) VALUES (?,?,?,?,?,?)`,
		groupID, domainListID, name, priority, action, blockResponse)
	if err != nil {
		return nil, err
	}
	s.incrFwRuleGroupCount(groupID, 1)
	return &fwRuleRow{GroupID: groupID, DomainListID: domainListID, Name: name, Priority: priority, Action: action, BlockResponse: blockResponse}, nil
}

func (s *Store) ListFirewallRules(groupID string) ([]fwRuleRow, error) {
	rows, err := s.db().Query(
		`SELECT group_id,domain_list_id,name,priority,action,block_response FROM firewall_rules WHERE group_id=? ORDER BY priority`,
		groupID)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var result []fwRuleRow
	for rows.Next() {
		r, err := scanFwRule(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, *r)
	}
	return result, rows.Err()
}

func (s *Store) UpdateFirewallRule(groupID, domainListID, name string, priority int, action, blockResponse string) (*fwRuleRow, error) {
	res, err := s.db().Exec(
		`UPDATE firewall_rules SET name=?,priority=?,action=?,block_response=? WHERE group_id=? AND domain_list_id=?`,
		name, priority, action, blockResponse, groupID, domainListID)
	if err != nil {
		return nil, err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return nil, errNotFound
	}
	return &fwRuleRow{GroupID: groupID, DomainListID: domainListID, Name: name, Priority: priority, Action: action, BlockResponse: blockResponse}, nil
}

func (s *Store) DeleteFirewallRule(groupID, domainListID string) error {
	res, err := s.db().Exec(
		`DELETE FROM firewall_rules WHERE group_id=? AND domain_list_id=?`, groupID, domainListID)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errNotFound
	}
	s.incrFwRuleGroupCount(groupID, -1)
	return nil
}

func scanFwRule(sc scanner) (*fwRuleRow, error) {
	var r fwRuleRow
	err := sc.Scan(&r.GroupID, &r.DomainListID, &r.Name, &r.Priority, &r.Action, &r.BlockResponse)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errNotFound
		}
		return nil, err
	}
	return &r, nil
}

// ===================== FirewallDomainLists =====================

func (s *Store) CreateFirewallDomainList(id, arn, name string) (*fwDomainListRow, error) {
	now := time.Now().Unix()
	_, err := s.db().Exec(
		`INSERT INTO firewall_domain_lists (id,arn,name,created_at) VALUES (?,?,?,?)`,
		id, arn, name, now)
	if err != nil {
		return nil, err
	}
	return &fwDomainListRow{ID: id, ARN: arn, Name: name, Status: "COMPLETE", DomainCount: 0, CreatedAt: now}, nil
}

func (s *Store) GetFirewallDomainList(id string) (*fwDomainListRow, error) {
	row := s.db().QueryRow(
		`SELECT id,arn,name,status,domain_count,created_at FROM firewall_domain_lists WHERE id=?`, id)
	return scanFwDomainList(row)
}

func (s *Store) ListFirewallDomainLists() ([]fwDomainListRow, error) {
	rows, err := s.db().Query(
		`SELECT id,arn,name,status,domain_count,created_at FROM firewall_domain_lists ORDER BY created_at`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var result []fwDomainListRow
	for rows.Next() {
		r, err := scanFwDomainList(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, *r)
	}
	return result, rows.Err()
}

func (s *Store) DeleteFirewallDomainList(id string) error {
	res, err := s.db().Exec(`DELETE FROM firewall_domain_lists WHERE id=?`, id)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errNotFound
	}
	_, _ = s.db().Exec(`DELETE FROM firewall_domains WHERE list_id=?`, id)
	return nil
}

func (s *Store) UpdateFirewallDomains(listID, op string, domains []string) error {
	switch op {
	case "ADD":
		for _, d := range domains {
			_, _ = s.db().Exec(`INSERT OR IGNORE INTO firewall_domains (list_id,domain) VALUES (?,?)`, listID, d)
		}
	case "REMOVE":
		for _, d := range domains {
			_, _ = s.db().Exec(`DELETE FROM firewall_domains WHERE list_id=? AND domain=?`, listID, d)
		}
	case "REPLACE":
		_, _ = s.db().Exec(`DELETE FROM firewall_domains WHERE list_id=?`, listID)
		for _, d := range domains {
			_, _ = s.db().Exec(`INSERT OR IGNORE INTO firewall_domains (list_id,domain) VALUES (?,?)`, listID, d)
		}
	}
	var count int
	_ = s.db().QueryRow(`SELECT COUNT(*) FROM firewall_domains WHERE list_id=?`, listID).Scan(&count)
	_, _ = s.db().Exec(`UPDATE firewall_domain_lists SET domain_count=? WHERE id=?`, count, listID)
	return nil
}

func (s *Store) ListFirewallDomains(listID string) ([]string, error) {
	rows, err := s.db().Query(`SELECT domain FROM firewall_domains WHERE list_id=? ORDER BY domain`, listID)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var result []string
	for rows.Next() {
		var d string
		if err := rows.Scan(&d); err != nil {
			return nil, err
		}
		result = append(result, d)
	}
	return result, rows.Err()
}

func scanFwDomainList(sc scanner) (*fwDomainListRow, error) {
	var r fwDomainListRow
	err := sc.Scan(&r.ID, &r.ARN, &r.Name, &r.Status, &r.DomainCount, &r.CreatedAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errNotFound
		}
		return nil, err
	}
	return &r, nil
}

// ===================== FirewallRuleGroupAssociations =====================

func (s *Store) AssociateFirewallRuleGroup(id, groupID, vpcID, name string, priority int) (*fwRuleGroupAssocRow, error) {
	_, err := s.db().Exec(
		`INSERT INTO firewall_rule_group_associations (id,group_id,vpc_id,name,priority) VALUES (?,?,?,?,?)`,
		id, groupID, vpcID, name, priority)
	if err != nil {
		return nil, err
	}
	return &fwRuleGroupAssocRow{ID: id, GroupID: groupID, VPCID: vpcID, Name: name, Priority: priority, Status: "COMPLETE"}, nil
}

func (s *Store) GetFirewallRuleGroupAssociation(id string) (*fwRuleGroupAssocRow, error) {
	row := s.db().QueryRow(
		`SELECT id,group_id,vpc_id,name,priority,status FROM firewall_rule_group_associations WHERE id=?`, id)
	return scanFwRuleGroupAssoc(row)
}

func (s *Store) ListFirewallRuleGroupAssociations(vpcID string) ([]fwRuleGroupAssocRow, error) {
	var rows *sql.Rows
	var err error
	if vpcID != "" {
		rows, err = s.db().Query(
			`SELECT id,group_id,vpc_id,name,priority,status FROM firewall_rule_group_associations WHERE vpc_id=? ORDER BY priority`,
			vpcID)
	} else {
		rows, err = s.db().Query(
			`SELECT id,group_id,vpc_id,name,priority,status FROM firewall_rule_group_associations ORDER BY priority`)
	}
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var result []fwRuleGroupAssocRow
	for rows.Next() {
		r, err := scanFwRuleGroupAssoc(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, *r)
	}
	return result, rows.Err()
}

func (s *Store) DisassociateFirewallRuleGroup(id string) (*fwRuleGroupAssocRow, error) {
	assoc, err := s.GetFirewallRuleGroupAssociation(id)
	if err != nil {
		return nil, err
	}
	_, _ = s.db().Exec(`DELETE FROM firewall_rule_group_associations WHERE id=?`, id)
	return assoc, nil
}

func (s *Store) UpdateFirewallRuleGroupAssociation(id, name string, priority int) (*fwRuleGroupAssocRow, error) {
	res, err := s.db().Exec(
		`UPDATE firewall_rule_group_associations SET name=?,priority=? WHERE id=?`, name, priority, id)
	if err != nil {
		return nil, err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return nil, errNotFound
	}
	return s.GetFirewallRuleGroupAssociation(id)
}

func scanFwRuleGroupAssoc(sc scanner) (*fwRuleGroupAssocRow, error) {
	var r fwRuleGroupAssocRow
	err := sc.Scan(&r.ID, &r.GroupID, &r.VPCID, &r.Name, &r.Priority, &r.Status)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errNotFound
		}
		return nil, err
	}
	return &r, nil
}
