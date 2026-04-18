// SPDX-License-Identifier: Apache-2.0

package elasticloadbalancingv2

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
	errLoadBalancerNotFound = errors.New("load balancer not found")
	errTargetGroupNotFound  = errors.New("target group not found")
	errListenerNotFound     = errors.New("listener not found")
	errRuleNotFound         = errors.New("rule not found")
)

var migrations = []sqlite.Migration{
	{Version: 1, SQL: `
		CREATE TABLE IF NOT EXISTS load_balancers (
			arn            TEXT PRIMARY KEY,
			name           TEXT NOT NULL UNIQUE,
			dns_name       TEXT NOT NULL DEFAULT '',
			type           TEXT NOT NULL DEFAULT 'application',
			scheme         TEXT NOT NULL DEFAULT 'internet-facing',
			state          TEXT NOT NULL DEFAULT 'active',
			vpc_id         TEXT NOT NULL DEFAULT '',
			subnets        TEXT NOT NULL DEFAULT '[]',
			security_groups TEXT NOT NULL DEFAULT '[]',
			ip_type        TEXT NOT NULL DEFAULT 'ipv4',
			created_at     INTEGER NOT NULL
		);
		CREATE TABLE IF NOT EXISTS target_groups (
			arn          TEXT PRIMARY KEY,
			name         TEXT NOT NULL UNIQUE,
			protocol     TEXT NOT NULL DEFAULT 'HTTP',
			port         INTEGER NOT NULL DEFAULT 80,
			vpc_id       TEXT NOT NULL DEFAULT '',
			target_type  TEXT NOT NULL DEFAULT 'instance',
			health_check TEXT NOT NULL DEFAULT '{}',
			created_at   INTEGER NOT NULL
		);
		CREATE TABLE IF NOT EXISTS listeners (
			arn             TEXT PRIMARY KEY,
			lb_arn          TEXT NOT NULL,
			protocol        TEXT NOT NULL DEFAULT 'HTTP',
			port            INTEGER NOT NULL DEFAULT 80,
			default_actions TEXT NOT NULL DEFAULT '[]',
			ssl_policy      TEXT NOT NULL DEFAULT '',
			certificates    TEXT NOT NULL DEFAULT '[]',
			created_at      INTEGER NOT NULL
		);
		CREATE TABLE IF NOT EXISTS rules (
			arn          TEXT PRIMARY KEY,
			listener_arn TEXT NOT NULL,
			priority     TEXT NOT NULL DEFAULT '1',
			conditions   TEXT NOT NULL DEFAULT '[]',
			actions      TEXT NOT NULL DEFAULT '[]',
			is_default   INTEGER NOT NULL DEFAULT 0
		);
		CREATE TABLE IF NOT EXISTS targets (
			target_group_arn TEXT NOT NULL,
			target_id        TEXT NOT NULL,
			port             INTEGER NOT NULL DEFAULT 0,
			az               TEXT NOT NULL DEFAULT '',
			health           TEXT NOT NULL DEFAULT 'healthy',
			PRIMARY KEY (target_group_arn, target_id)
		);
	`},
}

// --- Model types ---

type LoadBalancer struct {
	ARN            string
	Name           string
	DNSName        string
	Type           string
	Scheme         string
	State          string
	VpcID          string
	Subnets        []string
	SecurityGroups []string
	IPType         string
	CreatedAt      time.Time
}

type TargetGroup struct {
	ARN         string
	Name        string
	Protocol    string
	Port        int
	VpcID       string
	TargetType  string
	HealthCheck map[string]string
	CreatedAt   time.Time
}

type Listener struct {
	ARN            string
	LBARN          string
	Protocol       string
	Port           int
	DefaultActions []map[string]string
	SSLPolicy      string
	Certificates   []map[string]string
	CreatedAt      time.Time
}

type Rule struct {
	ARN         string
	ListenerARN string
	Priority    string
	Conditions  []map[string]string
	Actions     []map[string]string
	IsDefault   bool
}

type Target struct {
	TargetGroupARN string
	TargetID       string
	Port           int
	AZ             string
	Health         string
}

// --- Store ---

type Store struct {
	store *sqlite.Store
	tags  *shared.TagStore
}

func NewStore(dataDir string) (*Store, error) {
	allMigrations := append(migrations, shared.TagMigrations...)
	dbPath := filepath.Join(dataDir, "elasticloadbalancingv2.db")
	s, err := sqlite.Open(dbPath, allMigrations)
	if err != nil {
		return nil, err
	}
	return &Store{store: s, tags: shared.NewTagStore(s)}, nil
}

func (s *Store) Close() error { return s.store.Close() }

// --- LoadBalancer CRUD ---

func (s *Store) CreateLoadBalancer(arn, name, lbType, scheme, vpcID string, subnets, sgs []string) (*LoadBalancer, error) {
	now := time.Now().Unix()
	subnetsJSON, _ := json.Marshal(subnets)
	sgsJSON, _ := json.Marshal(sgs)
	dnsName := name + ".elb.localhost"
	_, err := s.store.DB().Exec(
		`INSERT INTO load_balancers (arn, name, dns_name, type, scheme, state, vpc_id, subnets, security_groups, ip_type, created_at)
		 VALUES (?, ?, ?, ?, ?, 'active', ?, ?, ?, 'ipv4', ?)`,
		arn, name, dnsName, lbType, scheme, vpcID, string(subnetsJSON), string(sgsJSON), now,
	)
	if err != nil {
		return nil, err
	}
	return &LoadBalancer{
		ARN: arn, Name: name, DNSName: dnsName, Type: lbType, Scheme: scheme,
		State: "active", VpcID: vpcID, Subnets: subnets, SecurityGroups: sgs,
		IPType: "ipv4", CreatedAt: time.Unix(now, 0),
	}, nil
}

func (s *Store) GetLoadBalancer(arn string) (*LoadBalancer, error) {
	row := s.store.DB().QueryRow(
		`SELECT arn, name, dns_name, type, scheme, state, vpc_id, subnets, security_groups, ip_type, created_at
		 FROM load_balancers WHERE arn = ?`, arn)
	return scanLoadBalancer(row)
}

func (s *Store) GetLoadBalancerByName(name string) (*LoadBalancer, error) {
	row := s.store.DB().QueryRow(
		`SELECT arn, name, dns_name, type, scheme, state, vpc_id, subnets, security_groups, ip_type, created_at
		 FROM load_balancers WHERE name = ?`, name)
	return scanLoadBalancer(row)
}

func (s *Store) ListLoadBalancers(arns []string) ([]LoadBalancer, error) {
	query := `SELECT arn, name, dns_name, type, scheme, state, vpc_id, subnets, security_groups, ip_type, created_at FROM load_balancers`
	var args []any
	if len(arns) > 0 {
		query += " WHERE arn IN (" + buildPlaceholders(len(arns)) + ")"
		for _, a := range arns {
			args = append(args, a)
		}
	}
	query += " ORDER BY name"
	rows, err := s.store.DB().Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var lbs []LoadBalancer
	for rows.Next() {
		lb, err := scanLoadBalancer(rows)
		if err != nil {
			return nil, err
		}
		lbs = append(lbs, *lb)
	}
	return lbs, rows.Err()
}

func (s *Store) SetLoadBalancerSecurityGroups(arn string, sgs []string) error {
	b, _ := json.Marshal(sgs)
	res, err := s.store.DB().Exec(`UPDATE load_balancers SET security_groups = ? WHERE arn = ?`, string(b), arn)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errLoadBalancerNotFound
	}
	return nil
}

func (s *Store) SetLoadBalancerSubnets(arn string, subnets []string) error {
	b, _ := json.Marshal(subnets)
	res, err := s.store.DB().Exec(`UPDATE load_balancers SET subnets = ? WHERE arn = ?`, string(b), arn)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errLoadBalancerNotFound
	}
	return nil
}

func (s *Store) SetLoadBalancerIPAddressType(arn, ipType string) error {
	res, err := s.store.DB().Exec(`UPDATE load_balancers SET ip_type = ? WHERE arn = ?`, ipType, arn)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errLoadBalancerNotFound
	}
	return nil
}

func (s *Store) DeleteLoadBalancer(arn string) error {
	res, err := s.store.DB().Exec(`DELETE FROM load_balancers WHERE arn = ?`, arn)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errLoadBalancerNotFound
	}
	return nil
}

// --- TargetGroup CRUD ---

func (s *Store) CreateTargetGroup(arn, name, protocol string, port int, vpcID, targetType string) (*TargetGroup, error) {
	now := time.Now().Unix()
	hc := map[string]string{"path": "/", "protocol": protocol, "interval": "30", "timeout": "5", "healthyThreshold": "5", "unhealthyThreshold": "2"}
	hcJSON, _ := json.Marshal(hc)
	_, err := s.store.DB().Exec(
		`INSERT INTO target_groups (arn, name, protocol, port, vpc_id, target_type, health_check, created_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		arn, name, protocol, port, vpcID, targetType, string(hcJSON), now,
	)
	if err != nil {
		return nil, err
	}
	return &TargetGroup{
		ARN: arn, Name: name, Protocol: protocol, Port: port, VpcID: vpcID,
		TargetType: targetType, HealthCheck: hc, CreatedAt: time.Unix(now, 0),
	}, nil
}

func (s *Store) GetTargetGroup(arn string) (*TargetGroup, error) {
	row := s.store.DB().QueryRow(
		`SELECT arn, name, protocol, port, vpc_id, target_type, health_check, created_at FROM target_groups WHERE arn = ?`, arn)
	return scanTargetGroup(row)
}

func (s *Store) ListTargetGroups(arns []string) ([]TargetGroup, error) {
	query := `SELECT arn, name, protocol, port, vpc_id, target_type, health_check, created_at FROM target_groups`
	var args []any
	if len(arns) > 0 {
		query += " WHERE arn IN (" + buildPlaceholders(len(arns)) + ")"
		for _, a := range arns {
			args = append(args, a)
		}
	}
	query += " ORDER BY name"
	rows, err := s.store.DB().Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var tgs []TargetGroup
	for rows.Next() {
		tg, err := scanTargetGroup(rows)
		if err != nil {
			return nil, err
		}
		tgs = append(tgs, *tg)
	}
	return tgs, rows.Err()
}

func (s *Store) DeleteTargetGroup(arn string) error {
	res, err := s.store.DB().Exec(`DELETE FROM target_groups WHERE arn = ?`, arn)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errTargetGroupNotFound
	}
	return nil
}

// --- Listener CRUD ---

func (s *Store) CreateListener(arn, lbARN, protocol string, port int, defaultActions []map[string]string, sslPolicy string, certs []map[string]string) (*Listener, error) {
	now := time.Now().Unix()
	actionsJSON, _ := json.Marshal(defaultActions)
	certsJSON, _ := json.Marshal(certs)
	_, err := s.store.DB().Exec(
		`INSERT INTO listeners (arn, lb_arn, protocol, port, default_actions, ssl_policy, certificates, created_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		arn, lbARN, protocol, port, string(actionsJSON), sslPolicy, string(certsJSON), now,
	)
	if err != nil {
		return nil, err
	}
	return &Listener{
		ARN: arn, LBARN: lbARN, Protocol: protocol, Port: port,
		DefaultActions: defaultActions, SSLPolicy: sslPolicy, Certificates: certs,
		CreatedAt: time.Unix(now, 0),
	}, nil
}

func (s *Store) GetListener(arn string) (*Listener, error) {
	row := s.store.DB().QueryRow(
		`SELECT arn, lb_arn, protocol, port, default_actions, ssl_policy, certificates, created_at FROM listeners WHERE arn = ?`, arn)
	return scanListener(row)
}

func (s *Store) ListListeners(lbARN string) ([]Listener, error) {
	var query string
	var args []any
	if lbARN != "" {
		query = `SELECT arn, lb_arn, protocol, port, default_actions, ssl_policy, certificates, created_at FROM listeners WHERE lb_arn = ? ORDER BY port`
		args = append(args, lbARN)
	} else {
		query = `SELECT arn, lb_arn, protocol, port, default_actions, ssl_policy, certificates, created_at FROM listeners ORDER BY port`
	}
	rows, err := s.store.DB().Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var listeners []Listener
	for rows.Next() {
		l, err := scanListener(rows)
		if err != nil {
			return nil, err
		}
		listeners = append(listeners, *l)
	}
	return listeners, rows.Err()
}

func (s *Store) ListListenersByARNs(arns []string) ([]Listener, error) {
	if len(arns) == 0 {
		return s.ListListeners("")
	}
	query := `SELECT arn, lb_arn, protocol, port, default_actions, ssl_policy, certificates, created_at FROM listeners WHERE arn IN (` + buildPlaceholders(len(arns)) + `) ORDER BY port`
	var args []any
	for _, a := range arns {
		args = append(args, a)
	}
	rows, err := s.store.DB().Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var listeners []Listener
	for rows.Next() {
		l, err := scanListener(rows)
		if err != nil {
			return nil, err
		}
		listeners = append(listeners, *l)
	}
	return listeners, rows.Err()
}

func (s *Store) UpdateListener(arn, protocol string, port int, defaultActions []map[string]string, sslPolicy string, certs []map[string]string) error {
	actionsJSON, _ := json.Marshal(defaultActions)
	certsJSON, _ := json.Marshal(certs)
	res, err := s.store.DB().Exec(
		`UPDATE listeners SET protocol = ?, port = ?, default_actions = ?, ssl_policy = ?, certificates = ? WHERE arn = ?`,
		protocol, port, string(actionsJSON), sslPolicy, string(certsJSON), arn,
	)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errListenerNotFound
	}
	return nil
}

func (s *Store) AddListenerCertificates(arn string, newCerts []map[string]string) error {
	l, err := s.GetListener(arn)
	if err != nil {
		return err
	}
	existing := map[string]bool{}
	for _, c := range l.Certificates {
		existing[c["CertificateArn"]] = true
	}
	for _, c := range newCerts {
		if !existing[c["CertificateArn"]] {
			l.Certificates = append(l.Certificates, c)
		}
	}
	b, _ := json.Marshal(l.Certificates)
	_, err = s.store.DB().Exec(`UPDATE listeners SET certificates = ? WHERE arn = ?`, string(b), arn)
	return err
}

func (s *Store) RemoveListenerCertificates(arn string, removeCerts []map[string]string) error {
	l, err := s.GetListener(arn)
	if err != nil {
		return err
	}
	removeSet := map[string]bool{}
	for _, c := range removeCerts {
		removeSet[c["CertificateArn"]] = true
	}
	filtered := l.Certificates[:0]
	for _, c := range l.Certificates {
		if !removeSet[c["CertificateArn"]] {
			filtered = append(filtered, c)
		}
	}
	b, _ := json.Marshal(filtered)
	_, err = s.store.DB().Exec(`UPDATE listeners SET certificates = ? WHERE arn = ?`, string(b), arn)
	return err
}

func (s *Store) DeleteListener(arn string) error {
	res, err := s.store.DB().Exec(`DELETE FROM listeners WHERE arn = ?`, arn)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errListenerNotFound
	}
	return nil
}

// --- Rule CRUD ---

func (s *Store) CreateRule(arn, listenerARN, priority string, conditions, actions []map[string]string, isDefault bool) (*Rule, error) {
	condsJSON, _ := json.Marshal(conditions)
	actsJSON, _ := json.Marshal(actions)
	def := 0
	if isDefault {
		def = 1
	}
	_, err := s.store.DB().Exec(
		`INSERT INTO rules (arn, listener_arn, priority, conditions, actions, is_default)
		 VALUES (?, ?, ?, ?, ?, ?)`,
		arn, listenerARN, priority, string(condsJSON), string(actsJSON), def,
	)
	if err != nil {
		return nil, err
	}
	return &Rule{
		ARN: arn, ListenerARN: listenerARN, Priority: priority,
		Conditions: conditions, Actions: actions, IsDefault: isDefault,
	}, nil
}

func (s *Store) GetRule(arn string) (*Rule, error) {
	row := s.store.DB().QueryRow(
		`SELECT arn, listener_arn, priority, conditions, actions, is_default FROM rules WHERE arn = ?`, arn)
	return scanRule(row)
}

func (s *Store) ListRules(listenerARN string) ([]Rule, error) {
	var query string
	var args []any
	if listenerARN != "" {
		query = `SELECT arn, listener_arn, priority, conditions, actions, is_default FROM rules WHERE listener_arn = ? ORDER BY priority`
		args = append(args, listenerARN)
	} else {
		query = `SELECT arn, listener_arn, priority, conditions, actions, is_default FROM rules ORDER BY priority`
	}
	rows, err := s.store.DB().Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var rules []Rule
	for rows.Next() {
		r, err := scanRule(rows)
		if err != nil {
			return nil, err
		}
		rules = append(rules, *r)
	}
	return rules, rows.Err()
}

func (s *Store) ListRulesByARNs(arns []string) ([]Rule, error) {
	query := `SELECT arn, listener_arn, priority, conditions, actions, is_default FROM rules WHERE arn IN (` + buildPlaceholders(len(arns)) + `)`
	var args []any
	for _, a := range arns {
		args = append(args, a)
	}
	rows, err := s.store.DB().Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var rules []Rule
	for rows.Next() {
		r, err := scanRule(rows)
		if err != nil {
			return nil, err
		}
		rules = append(rules, *r)
	}
	return rules, rows.Err()
}

func (s *Store) UpdateRule(arn string, conditions, actions []map[string]string) error {
	condsJSON, _ := json.Marshal(conditions)
	actsJSON, _ := json.Marshal(actions)
	res, err := s.store.DB().Exec(
		`UPDATE rules SET conditions = ?, actions = ? WHERE arn = ?`,
		string(condsJSON), string(actsJSON), arn,
	)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errRuleNotFound
	}
	return nil
}

func (s *Store) UpdateRulePriority(arn, priority string) error {
	res, err := s.store.DB().Exec(`UPDATE rules SET priority = ? WHERE arn = ?`, priority, arn)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errRuleNotFound
	}
	return nil
}

func (s *Store) DeleteRule(arn string) error {
	res, err := s.store.DB().Exec(`DELETE FROM rules WHERE arn = ?`, arn)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errRuleNotFound
	}
	return nil
}

// --- Target CRUD ---

func (s *Store) RegisterTarget(tgARN, targetID string, port int, az string) error {
	_, err := s.store.DB().Exec(
		`INSERT INTO targets (target_group_arn, target_id, port, az, health)
		 VALUES (?, ?, ?, ?, 'healthy')
		 ON CONFLICT(target_group_arn, target_id) DO UPDATE SET port=excluded.port, az=excluded.az`,
		tgARN, targetID, port, az,
	)
	return err
}

func (s *Store) DeregisterTarget(tgARN, targetID string) error {
	_, err := s.store.DB().Exec(`DELETE FROM targets WHERE target_group_arn = ? AND target_id = ?`, tgARN, targetID)
	return err
}

func (s *Store) ListTargets(tgARN string) ([]Target, error) {
	rows, err := s.store.DB().Query(
		`SELECT target_group_arn, target_id, port, az, health FROM targets WHERE target_group_arn = ?`, tgARN)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var targets []Target
	for rows.Next() {
		var t Target
		if err := rows.Scan(&t.TargetGroupARN, &t.TargetID, &t.Port, &t.AZ, &t.Health); err != nil {
			return nil, err
		}
		targets = append(targets, t)
	}
	return targets, rows.Err()
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

// --- scan helpers ---

type scanner interface{ Scan(dest ...any) error }

func scanLoadBalancer(sc scanner) (*LoadBalancer, error) {
	var lb LoadBalancer
	var createdAt int64
	var subnetsJSON, sgsJSON string
	err := sc.Scan(&lb.ARN, &lb.Name, &lb.DNSName, &lb.Type, &lb.Scheme, &lb.State, &lb.VpcID, &subnetsJSON, &sgsJSON, &lb.IPType, &createdAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errLoadBalancerNotFound
		}
		return nil, err
	}
	_ = json.Unmarshal([]byte(subnetsJSON), &lb.Subnets)
	_ = json.Unmarshal([]byte(sgsJSON), &lb.SecurityGroups)
	lb.CreatedAt = time.Unix(createdAt, 0)
	return &lb, nil
}

func scanTargetGroup(sc scanner) (*TargetGroup, error) {
	var tg TargetGroup
	var createdAt int64
	var hcJSON string
	err := sc.Scan(&tg.ARN, &tg.Name, &tg.Protocol, &tg.Port, &tg.VpcID, &tg.TargetType, &hcJSON, &createdAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errTargetGroupNotFound
		}
		return nil, err
	}
	_ = json.Unmarshal([]byte(hcJSON), &tg.HealthCheck)
	tg.CreatedAt = time.Unix(createdAt, 0)
	return &tg, nil
}

func scanListener(sc scanner) (*Listener, error) {
	var l Listener
	var createdAt int64
	var actionsJSON, certsJSON string
	err := sc.Scan(&l.ARN, &l.LBARN, &l.Protocol, &l.Port, &actionsJSON, &l.SSLPolicy, &certsJSON, &createdAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errListenerNotFound
		}
		return nil, err
	}
	_ = json.Unmarshal([]byte(actionsJSON), &l.DefaultActions)
	_ = json.Unmarshal([]byte(certsJSON), &l.Certificates)
	l.CreatedAt = time.Unix(createdAt, 0)
	return &l, nil
}

func scanRule(sc scanner) (*Rule, error) {
	var r Rule
	var isDefault int
	var condsJSON, actsJSON string
	err := sc.Scan(&r.ARN, &r.ListenerARN, &r.Priority, &condsJSON, &actsJSON, &isDefault)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errRuleNotFound
		}
		return nil, err
	}
	_ = json.Unmarshal([]byte(condsJSON), &r.Conditions)
	_ = json.Unmarshal([]byte(actsJSON), &r.Actions)
	r.IsDefault = isDefault != 0
	return &r, nil
}

func buildPlaceholders(n int) string {
	if n == 0 {
		return ""
	}
	b := make([]byte, 0, n*2-1)
	for i := 0; i < n; i++ {
		if i > 0 {
			b = append(b, ',')
		}
		b = append(b, '?')
	}
	return string(b)
}
