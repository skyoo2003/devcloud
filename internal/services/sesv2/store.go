// SPDX-License-Identifier: Apache-2.0

// internal/services/sesv2/store.go
package sesv2

import (
	"database/sql"
	"errors"
	"path/filepath"
	"strings"
	"time"

	"github.com/skyoo2003/devcloud/internal/shared"
	"github.com/skyoo2003/devcloud/internal/storage/sqlite"
)

var (
	errNotFound = errors.New("not found")
)

var migrations = []sqlite.Migration{
	{Version: 1, SQL: `
		CREATE TABLE IF NOT EXISTS email_identities (
			name            TEXT PRIMARY KEY,
			type            TEXT NOT NULL DEFAULT 'EMAIL_ADDRESS',
			verified        INTEGER NOT NULL DEFAULT 1,
			dkim_status     TEXT NOT NULL DEFAULT 'SUCCESS',
			config_set      TEXT NOT NULL DEFAULT '',
			created_at      INTEGER NOT NULL
		);
		CREATE TABLE IF NOT EXISTS email_identity_policies (
			identity_name   TEXT NOT NULL,
			policy_name     TEXT NOT NULL,
			policy          TEXT NOT NULL DEFAULT '{}',
			PRIMARY KEY (identity_name, policy_name)
		);
		CREATE TABLE IF NOT EXISTS email_templates (
			name            TEXT PRIMARY KEY,
			subject         TEXT NOT NULL DEFAULT '',
			html            TEXT NOT NULL DEFAULT '',
			text_content    TEXT NOT NULL DEFAULT '',
			created_at      INTEGER NOT NULL
		);
		CREATE TABLE IF NOT EXISTS config_sets (
			name            TEXT PRIMARY KEY,
			sending_enabled INTEGER NOT NULL DEFAULT 1,
			reputation      INTEGER NOT NULL DEFAULT 0,
			suppression     TEXT NOT NULL DEFAULT '{}',
			tracking        TEXT NOT NULL DEFAULT '{}',
			delivery        TEXT NOT NULL DEFAULT '{}',
			created_at      INTEGER NOT NULL
		);
		CREATE TABLE IF NOT EXISTS config_set_event_destinations (
			config_set_name TEXT NOT NULL,
			dest_name       TEXT NOT NULL,
			enabled         INTEGER NOT NULL DEFAULT 1,
			matching_types  TEXT NOT NULL DEFAULT '[]',
			destination     TEXT NOT NULL DEFAULT '{}',
			PRIMARY KEY (config_set_name, dest_name)
		);
		CREATE TABLE IF NOT EXISTS contact_lists (
			name            TEXT PRIMARY KEY,
			description     TEXT NOT NULL DEFAULT '',
			topics          TEXT NOT NULL DEFAULT '[]',
			created_at      INTEGER NOT NULL,
			updated_at      INTEGER NOT NULL
		);
		CREATE TABLE IF NOT EXISTS contacts (
			email           TEXT NOT NULL,
			list_name       TEXT NOT NULL,
			topics          TEXT NOT NULL DEFAULT '[]',
			unsubscribe_all INTEGER NOT NULL DEFAULT 0,
			attributes      TEXT NOT NULL DEFAULT '{}',
			created_at      INTEGER NOT NULL,
			updated_at      INTEGER NOT NULL,
			PRIMARY KEY (email, list_name)
		);
		CREATE TABLE IF NOT EXISTS dedicated_ip_pools (
			name            TEXT PRIMARY KEY,
			scaling_mode    TEXT NOT NULL DEFAULT 'STANDARD',
			created_at      INTEGER NOT NULL
		);
		CREATE TABLE IF NOT EXISTS suppressed_destinations (
			email           TEXT PRIMARY KEY,
			reason          TEXT NOT NULL DEFAULT 'BOUNCE',
			created_at      INTEGER NOT NULL
		);
		CREATE TABLE IF NOT EXISTS sent_emails (
			message_id      TEXT PRIMARY KEY,
			source          TEXT NOT NULL DEFAULT '',
			destination     TEXT NOT NULL DEFAULT '{}',
			subject         TEXT NOT NULL DEFAULT '',
			body            TEXT NOT NULL DEFAULT '',
			sent_at         INTEGER NOT NULL
		);
	`},
}

// --- Models ---

type EmailIdentity struct {
	Name       string
	Type       string
	Verified   bool
	DkimStatus string
	ConfigSet  string
	CreatedAt  int64
}

type EmailTemplate struct {
	Name        string
	Subject     string
	HTML        string
	TextContent string
	CreatedAt   int64
}

type ConfigSet struct {
	Name           string
	SendingEnabled bool
	Reputation     int
	Suppression    string
	Tracking       string
	Delivery       string
	CreatedAt      int64
}

type ConfigSetEventDest struct {
	ConfigSetName string
	DestName      string
	Enabled       bool
	MatchingTypes string
	Destination   string
}

type ContactList struct {
	Name        string
	Description string
	Topics      string
	CreatedAt   int64
	UpdatedAt   int64
}

type Contact struct {
	Email          string
	ListName       string
	Topics         string
	UnsubscribeAll bool
	Attributes     string
	CreatedAt      int64
	UpdatedAt      int64
}

type DedicatedIpPool struct {
	Name        string
	ScalingMode string
	CreatedAt   int64
}

type SuppressedDestination struct {
	Email     string
	Reason    string
	CreatedAt int64
}

type SentEmail struct {
	MessageID   string
	Source      string
	Destination string
	Subject     string
	Body        string
	SentAt      int64
}

// --- Store ---

type Store struct {
	store *sqlite.Store
	tags  *shared.TagStore
}

func NewStore(dataDir string) (*Store, error) {
	allMigrations := append(migrations, shared.TagMigrations...)
	dbPath := filepath.Join(dataDir, "sesv2.db")
	s, err := sqlite.Open(dbPath, allMigrations)
	if err != nil {
		return nil, err
	}
	return &Store{store: s, tags: shared.NewTagStore(s)}, nil
}

func (s *Store) Close() error { return s.store.Close() }

func (s *Store) db() *sql.DB { return s.store.DB() }

// --- EmailIdentity ---

func (s *Store) CreateEmailIdentity(e *EmailIdentity) error {
	_, err := s.db().Exec(
		`INSERT INTO email_identities (name, type, verified, dkim_status, config_set, created_at)
		 VALUES (?, ?, ?, ?, ?, ?)`,
		e.Name, e.Type, boolInt(e.Verified), e.DkimStatus, e.ConfigSet, time.Now().Unix(),
	)
	return err
}

func (s *Store) GetEmailIdentity(name string) (*EmailIdentity, error) {
	row := s.db().QueryRow(
		`SELECT name, type, verified, dkim_status, config_set, created_at FROM email_identities WHERE name = ?`, name)
	return scanEmailIdentity(row)
}

func (s *Store) ListEmailIdentities() ([]EmailIdentity, error) {
	rows, err := s.db().Query(`SELECT name, type, verified, dkim_status, config_set, created_at FROM email_identities ORDER BY created_at`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []EmailIdentity
	for rows.Next() {
		e, err := scanEmailIdentity(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *e)
	}
	return out, rows.Err()
}

func (s *Store) UpdateEmailIdentityConfigSet(name, configSet string) error {
	res, err := s.db().Exec(`UPDATE email_identities SET config_set = ? WHERE name = ?`, configSet, name)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errNotFound
	}
	return nil
}

func (s *Store) UpdateEmailIdentityDkimStatus(name, status string) error {
	res, err := s.db().Exec(`UPDATE email_identities SET dkim_status = ? WHERE name = ?`, status, name)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errNotFound
	}
	return nil
}

func (s *Store) DeleteEmailIdentity(name string) error {
	res, err := s.db().Exec(`DELETE FROM email_identities WHERE name = ?`, name)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errNotFound
	}
	return nil
}

// --- EmailIdentityPolicy ---

func (s *Store) UpsertEmailIdentityPolicy(identityName, policyName, policy string) error {
	_, err := s.db().Exec(
		`INSERT INTO email_identity_policies (identity_name, policy_name, policy) VALUES (?, ?, ?)
		 ON CONFLICT(identity_name, policy_name) DO UPDATE SET policy = excluded.policy`,
		identityName, policyName, policy,
	)
	return err
}

func (s *Store) GetEmailIdentityPolicies(identityName string) (map[string]string, error) {
	rows, err := s.db().Query(`SELECT policy_name, policy FROM email_identity_policies WHERE identity_name = ?`, identityName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make(map[string]string)
	for rows.Next() {
		var k, v string
		if err := rows.Scan(&k, &v); err != nil {
			return nil, err
		}
		out[k] = v
	}
	return out, rows.Err()
}

func (s *Store) DeleteEmailIdentityPolicy(identityName, policyName string) error {
	_, err := s.db().Exec(`DELETE FROM email_identity_policies WHERE identity_name = ? AND policy_name = ?`, identityName, policyName)
	return err
}

// --- EmailTemplate ---

func (s *Store) CreateEmailTemplate(t *EmailTemplate) error {
	_, err := s.db().Exec(
		`INSERT INTO email_templates (name, subject, html, text_content, created_at) VALUES (?, ?, ?, ?, ?)`,
		t.Name, t.Subject, t.HTML, t.TextContent, time.Now().Unix(),
	)
	return err
}

func (s *Store) GetEmailTemplate(name string) (*EmailTemplate, error) {
	row := s.db().QueryRow(`SELECT name, subject, html, text_content, created_at FROM email_templates WHERE name = ?`, name)
	return scanEmailTemplate(row)
}

func (s *Store) ListEmailTemplates() ([]EmailTemplate, error) {
	rows, err := s.db().Query(`SELECT name, subject, html, text_content, created_at FROM email_templates ORDER BY created_at`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []EmailTemplate
	for rows.Next() {
		t, err := scanEmailTemplate(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *t)
	}
	return out, rows.Err()
}

func (s *Store) UpdateEmailTemplate(name, subject, html, textContent string) error {
	res, err := s.db().Exec(
		`UPDATE email_templates SET subject = ?, html = ?, text_content = ? WHERE name = ?`,
		subject, html, textContent, name,
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

func (s *Store) DeleteEmailTemplate(name string) error {
	res, err := s.db().Exec(`DELETE FROM email_templates WHERE name = ?`, name)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errNotFound
	}
	return nil
}

// --- ConfigSet ---

func (s *Store) CreateConfigSet(c *ConfigSet) error {
	_, err := s.db().Exec(
		`INSERT INTO config_sets (name, sending_enabled, reputation, suppression, tracking, delivery, created_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?)`,
		c.Name, boolInt(c.SendingEnabled), c.Reputation, c.Suppression, c.Tracking, c.Delivery, time.Now().Unix(),
	)
	return err
}

func (s *Store) GetConfigSet(name string) (*ConfigSet, error) {
	row := s.db().QueryRow(`SELECT name, sending_enabled, reputation, suppression, tracking, delivery, created_at FROM config_sets WHERE name = ?`, name)
	return scanConfigSet(row)
}

func (s *Store) ListConfigSets() ([]ConfigSet, error) {
	rows, err := s.db().Query(`SELECT name, sending_enabled, reputation, suppression, tracking, delivery, created_at FROM config_sets ORDER BY created_at`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ConfigSet
	for rows.Next() {
		c, err := scanConfigSet(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *c)
	}
	return out, rows.Err()
}

func (s *Store) UpdateConfigSetField(name, field, value string) error {
	// field must be validated by caller
	res, err := s.db().Exec(`UPDATE config_sets SET `+field+` = ? WHERE name = ?`, value, name)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errNotFound
	}
	return nil
}

func (s *Store) UpdateConfigSetSending(name string, enabled bool) error {
	res, err := s.db().Exec(`UPDATE config_sets SET sending_enabled = ? WHERE name = ?`, boolInt(enabled), name)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errNotFound
	}
	return nil
}

func (s *Store) DeleteConfigSet(name string) error {
	res, err := s.db().Exec(`DELETE FROM config_sets WHERE name = ?`, name)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errNotFound
	}
	return nil
}

// --- ConfigSetEventDest ---

func (s *Store) CreateConfigSetEventDest(d *ConfigSetEventDest) error {
	_, err := s.db().Exec(
		`INSERT INTO config_set_event_destinations (config_set_name, dest_name, enabled, matching_types, destination)
		 VALUES (?, ?, ?, ?, ?)`,
		d.ConfigSetName, d.DestName, boolInt(d.Enabled), d.MatchingTypes, d.Destination,
	)
	return err
}

func (s *Store) ListConfigSetEventDests(configSetName string) ([]ConfigSetEventDest, error) {
	rows, err := s.db().Query(
		`SELECT config_set_name, dest_name, enabled, matching_types, destination FROM config_set_event_destinations WHERE config_set_name = ?`,
		configSetName,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ConfigSetEventDest
	for rows.Next() {
		var d ConfigSetEventDest
		var enabled int
		if err := rows.Scan(&d.ConfigSetName, &d.DestName, &enabled, &d.MatchingTypes, &d.Destination); err != nil {
			return nil, err
		}
		d.Enabled = enabled != 0
		out = append(out, d)
	}
	return out, rows.Err()
}

func (s *Store) UpdateConfigSetEventDest(d *ConfigSetEventDest) error {
	res, err := s.db().Exec(
		`UPDATE config_set_event_destinations SET enabled = ?, matching_types = ?, destination = ? WHERE config_set_name = ? AND dest_name = ?`,
		boolInt(d.Enabled), d.MatchingTypes, d.Destination, d.ConfigSetName, d.DestName,
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

func (s *Store) DeleteConfigSetEventDest(configSetName, destName string) error {
	_, err := s.db().Exec(
		`DELETE FROM config_set_event_destinations WHERE config_set_name = ? AND dest_name = ?`,
		configSetName, destName,
	)
	return err
}

// --- ContactList ---

func (s *Store) CreateContactList(cl *ContactList) error {
	now := time.Now().Unix()
	_, err := s.db().Exec(
		`INSERT INTO contact_lists (name, description, topics, created_at, updated_at) VALUES (?, ?, ?, ?, ?)`,
		cl.Name, cl.Description, cl.Topics, now, now,
	)
	return err
}

func (s *Store) GetContactList(name string) (*ContactList, error) {
	row := s.db().QueryRow(`SELECT name, description, topics, created_at, updated_at FROM contact_lists WHERE name = ?`, name)
	return scanContactList(row)
}

func (s *Store) ListContactLists() ([]ContactList, error) {
	rows, err := s.db().Query(`SELECT name, description, topics, created_at, updated_at FROM contact_lists ORDER BY created_at`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ContactList
	for rows.Next() {
		cl, err := scanContactList(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *cl)
	}
	return out, rows.Err()
}

func (s *Store) UpdateContactList(name, description, topics string) error {
	res, err := s.db().Exec(
		`UPDATE contact_lists SET description = ?, topics = ?, updated_at = ? WHERE name = ?`,
		description, topics, time.Now().Unix(), name,
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

func (s *Store) DeleteContactList(name string) error {
	res, err := s.db().Exec(`DELETE FROM contact_lists WHERE name = ?`, name)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errNotFound
	}
	return nil
}

// --- Contact ---

func (s *Store) CreateContact(c *Contact) error {
	now := time.Now().Unix()
	_, err := s.db().Exec(
		`INSERT INTO contacts (email, list_name, topics, unsubscribe_all, attributes, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?)`,
		c.Email, c.ListName, c.Topics, boolInt(c.UnsubscribeAll), c.Attributes, now, now,
	)
	return err
}

func (s *Store) GetContact(email, listName string) (*Contact, error) {
	row := s.db().QueryRow(
		`SELECT email, list_name, topics, unsubscribe_all, attributes, created_at, updated_at FROM contacts WHERE email = ? AND list_name = ?`,
		email, listName,
	)
	return scanContact(row)
}

func (s *Store) ListContacts(listName string) ([]Contact, error) {
	rows, err := s.db().Query(
		`SELECT email, list_name, topics, unsubscribe_all, attributes, created_at, updated_at FROM contacts WHERE list_name = ? ORDER BY created_at`,
		listName,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Contact
	for rows.Next() {
		c, err := scanContact(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *c)
	}
	return out, rows.Err()
}

func (s *Store) UpdateContact(email, listName, topics string, unsubscribeAll bool, attributes string) error {
	res, err := s.db().Exec(
		`UPDATE contacts SET topics = ?, unsubscribe_all = ?, attributes = ?, updated_at = ? WHERE email = ? AND list_name = ?`,
		topics, boolInt(unsubscribeAll), attributes, time.Now().Unix(), email, listName,
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

func (s *Store) DeleteContact(email, listName string) error {
	res, err := s.db().Exec(`DELETE FROM contacts WHERE email = ? AND list_name = ?`, email, listName)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errNotFound
	}
	return nil
}

// --- DedicatedIpPool ---

func (s *Store) CreateDedicatedIpPool(p *DedicatedIpPool) error {
	_, err := s.db().Exec(
		`INSERT INTO dedicated_ip_pools (name, scaling_mode, created_at) VALUES (?, ?, ?)`,
		p.Name, p.ScalingMode, time.Now().Unix(),
	)
	return err
}

func (s *Store) GetDedicatedIpPool(name string) (*DedicatedIpPool, error) {
	row := s.db().QueryRow(`SELECT name, scaling_mode, created_at FROM dedicated_ip_pools WHERE name = ?`, name)
	return scanDedicatedIpPool(row)
}

func (s *Store) ListDedicatedIpPools() ([]DedicatedIpPool, error) {
	rows, err := s.db().Query(`SELECT name, scaling_mode, created_at FROM dedicated_ip_pools ORDER BY created_at`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []DedicatedIpPool
	for rows.Next() {
		p, err := scanDedicatedIpPool(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *p)
	}
	return out, rows.Err()
}

func (s *Store) DeleteDedicatedIpPool(name string) error {
	res, err := s.db().Exec(`DELETE FROM dedicated_ip_pools WHERE name = ?`, name)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errNotFound
	}
	return nil
}

// --- SuppressedDestination ---

func (s *Store) PutSuppressedDestination(sd *SuppressedDestination) error {
	_, err := s.db().Exec(
		`INSERT INTO suppressed_destinations (email, reason, created_at) VALUES (?, ?, ?)
		 ON CONFLICT(email) DO UPDATE SET reason = excluded.reason`,
		sd.Email, sd.Reason, time.Now().Unix(),
	)
	return err
}

func (s *Store) GetSuppressedDestination(email string) (*SuppressedDestination, error) {
	row := s.db().QueryRow(`SELECT email, reason, created_at FROM suppressed_destinations WHERE email = ?`, email)
	return scanSuppressedDestination(row)
}

func (s *Store) ListSuppressedDestinations() ([]SuppressedDestination, error) {
	rows, err := s.db().Query(`SELECT email, reason, created_at FROM suppressed_destinations ORDER BY created_at`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []SuppressedDestination
	for rows.Next() {
		sd, err := scanSuppressedDestination(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *sd)
	}
	return out, rows.Err()
}

func (s *Store) DeleteSuppressedDestination(email string) error {
	res, err := s.db().Exec(`DELETE FROM suppressed_destinations WHERE email = ?`, email)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errNotFound
	}
	return nil
}

// --- SentEmail ---

func (s *Store) CreateSentEmail(e *SentEmail) error {
	_, err := s.db().Exec(
		`INSERT INTO sent_emails (message_id, source, destination, subject, body, sent_at) VALUES (?, ?, ?, ?, ?, ?)`,
		e.MessageID, e.Source, e.Destination, e.Subject, e.Body, time.Now().Unix(),
	)
	return err
}

// --- Scan helpers ---

type scanner interface{ Scan(dest ...any) error }

func scanEmailIdentity(sc scanner) (*EmailIdentity, error) {
	var e EmailIdentity
	var verified int
	err := sc.Scan(&e.Name, &e.Type, &verified, &e.DkimStatus, &e.ConfigSet, &e.CreatedAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errNotFound
		}
		return nil, err
	}
	e.Verified = verified != 0
	return &e, nil
}

func scanEmailTemplate(sc scanner) (*EmailTemplate, error) {
	var t EmailTemplate
	err := sc.Scan(&t.Name, &t.Subject, &t.HTML, &t.TextContent, &t.CreatedAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errNotFound
		}
		return nil, err
	}
	return &t, nil
}

func scanConfigSet(sc scanner) (*ConfigSet, error) {
	var c ConfigSet
	var sending int
	err := sc.Scan(&c.Name, &sending, &c.Reputation, &c.Suppression, &c.Tracking, &c.Delivery, &c.CreatedAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errNotFound
		}
		return nil, err
	}
	c.SendingEnabled = sending != 0
	return &c, nil
}

func scanContactList(sc scanner) (*ContactList, error) {
	var cl ContactList
	err := sc.Scan(&cl.Name, &cl.Description, &cl.Topics, &cl.CreatedAt, &cl.UpdatedAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errNotFound
		}
		return nil, err
	}
	return &cl, nil
}

func scanContact(sc scanner) (*Contact, error) {
	var c Contact
	var unsub int
	err := sc.Scan(&c.Email, &c.ListName, &c.Topics, &unsub, &c.Attributes, &c.CreatedAt, &c.UpdatedAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errNotFound
		}
		return nil, err
	}
	c.UnsubscribeAll = unsub != 0
	return &c, nil
}

func scanDedicatedIpPool(sc scanner) (*DedicatedIpPool, error) {
	var p DedicatedIpPool
	err := sc.Scan(&p.Name, &p.ScalingMode, &p.CreatedAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errNotFound
		}
		return nil, err
	}
	return &p, nil
}

func scanSuppressedDestination(sc scanner) (*SuppressedDestination, error) {
	var sd SuppressedDestination
	err := sc.Scan(&sd.Email, &sd.Reason, &sd.CreatedAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errNotFound
		}
		return nil, err
	}
	return &sd, nil
}

func boolInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

func isUniqueErr(err error) bool {
	return err != nil && strings.Contains(err.Error(), "UNIQUE constraint failed")
}
