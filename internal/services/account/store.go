// SPDX-License-Identifier: Apache-2.0

package account

import (
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"

	"github.com/skyoo2003/devcloud/internal/storage/sqlite"
)

var ErrNotFound = errors.New("resource not found")

var migrations = []sqlite.Migration{
	{Version: 1, SQL: `
		CREATE TABLE IF NOT EXISTS account_contact_info (
			account_id    TEXT PRIMARY KEY,
			full_name     TEXT,
			company_name  TEXT,
			phone_number  TEXT,
			address_line1 TEXT,
			city          TEXT,
			postal_code   TEXT,
			country_code  TEXT,
			website_url   TEXT
		);
		CREATE TABLE IF NOT EXISTS account_alternate_contacts (
			account_id   TEXT NOT NULL,
			contact_type TEXT NOT NULL,
			name         TEXT,
			title        TEXT,
			email        TEXT,
			phone_number TEXT,
			PRIMARY KEY (account_id, contact_type)
		);
		CREATE TABLE IF NOT EXISTS account_regions (
			account_id  TEXT NOT NULL,
			region_name TEXT NOT NULL,
			opt_status  TEXT DEFAULT 'ENABLED',
			PRIMARY KEY (account_id, region_name)
		);
		CREATE TABLE IF NOT EXISTS account_primary_email (
			account_id    TEXT PRIMARY KEY,
			email         TEXT,
			pending_email TEXT
		);
	`},
}

type Store struct {
	s *sqlite.Store
}

func NewStore(dir string) (*Store, error) {
	s, err := sqlite.Open(filepath.Join(dir, "account.db"), migrations)
	if err != nil {
		return nil, fmt.Errorf("account: open store: %w", err)
	}
	return &Store{s: s}, nil
}

func (s *Store) Close() error { return s.s.Close() }
func (s *Store) db() *sql.DB  { return s.s.DB() }

// --- Contact Info ---

type ContactInfo struct {
	FullName     string
	CompanyName  string
	PhoneNumber  string
	AddressLine1 string
	City         string
	PostalCode   string
	CountryCode  string
	WebsiteURL   string
}

func (s *Store) GetContactInfo(accountID string) (*ContactInfo, error) {
	row := s.db().QueryRow(
		`SELECT COALESCE(full_name,''), COALESCE(company_name,''), COALESCE(phone_number,''),
		        COALESCE(address_line1,''), COALESCE(city,''), COALESCE(postal_code,''),
		        COALESCE(country_code,''), COALESCE(website_url,'')
		 FROM account_contact_info WHERE account_id=?`, accountID)
	c := &ContactInfo{}
	err := row.Scan(&c.FullName, &c.CompanyName, &c.PhoneNumber, &c.AddressLine1, &c.City, &c.PostalCode, &c.CountryCode, &c.WebsiteURL)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	return c, nil
}

func (s *Store) PutContactInfo(accountID string, c *ContactInfo) error {
	_, err := s.db().Exec(
		`INSERT INTO account_contact_info (account_id, full_name, company_name, phone_number, address_line1, city, postal_code, country_code, website_url)
		 VALUES (?,?,?,?,?,?,?,?,?)
		 ON CONFLICT(account_id) DO UPDATE SET
		   full_name=excluded.full_name, company_name=excluded.company_name, phone_number=excluded.phone_number,
		   address_line1=excluded.address_line1, city=excluded.city, postal_code=excluded.postal_code,
		   country_code=excluded.country_code, website_url=excluded.website_url`,
		accountID, c.FullName, c.CompanyName, c.PhoneNumber, c.AddressLine1, c.City, c.PostalCode, c.CountryCode, c.WebsiteURL,
	)
	return err
}

func (s *Store) DeleteContactInfo(accountID string) error {
	_, err := s.db().Exec(`DELETE FROM account_contact_info WHERE account_id=?`, accountID)
	return err
}

// --- Alternate Contacts ---

type AlternateContact struct {
	ContactType string
	Name        string
	Title       string
	Email       string
	PhoneNumber string
}

func (s *Store) GetAlternateContact(accountID, contactType string) (*AlternateContact, error) {
	row := s.db().QueryRow(
		`SELECT contact_type, COALESCE(name,''), COALESCE(title,''), COALESCE(email,''), COALESCE(phone_number,'')
		 FROM account_alternate_contacts WHERE account_id=? AND contact_type=?`, accountID, contactType)
	c := &AlternateContact{}
	err := row.Scan(&c.ContactType, &c.Name, &c.Title, &c.Email, &c.PhoneNumber)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	return c, nil
}

func (s *Store) PutAlternateContact(accountID string, c *AlternateContact) error {
	_, err := s.db().Exec(
		`INSERT INTO account_alternate_contacts (account_id, contact_type, name, title, email, phone_number)
		 VALUES (?,?,?,?,?,?)
		 ON CONFLICT(account_id, contact_type) DO UPDATE SET
		   name=excluded.name, title=excluded.title, email=excluded.email, phone_number=excluded.phone_number`,
		accountID, c.ContactType, c.Name, c.Title, c.Email, c.PhoneNumber,
	)
	return err
}

func (s *Store) DeleteAlternateContact(accountID, contactType string) error {
	_, err := s.db().Exec(`DELETE FROM account_alternate_contacts WHERE account_id=? AND contact_type=?`, accountID, contactType)
	return err
}

// --- Regions ---

var defaultRegions = []string{
	"us-east-1", "us-east-2", "us-west-1", "us-west-2",
	"eu-west-1", "eu-west-2", "eu-central-1",
	"ap-northeast-1", "ap-northeast-2", "ap-southeast-1", "ap-southeast-2",
	"sa-east-1",
}

func (s *Store) ensureRegions(accountID string) error {
	for _, r := range defaultRegions {
		_, err := s.db().Exec(
			`INSERT INTO account_regions (account_id, region_name, opt_status) VALUES (?,?,'ENABLED')
			 ON CONFLICT DO NOTHING`, accountID, r)
		if err != nil {
			return err
		}
	}
	return nil
}

type Region struct {
	RegionName string
	OptStatus  string
}

func (s *Store) ListRegions(accountID string) ([]*Region, error) {
	s.ensureRegions(accountID)
	rows, err := s.db().Query(
		`SELECT region_name, opt_status FROM account_regions WHERE account_id=? ORDER BY region_name`, accountID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*Region
	for rows.Next() {
		r := &Region{}
		rows.Scan(&r.RegionName, &r.OptStatus)
		out = append(out, r)
	}
	return out, rows.Err()
}

func (s *Store) GetRegionOptStatus(accountID, regionName string) (*Region, error) {
	s.ensureRegions(accountID)
	row := s.db().QueryRow(
		`SELECT region_name, opt_status FROM account_regions WHERE account_id=? AND region_name=?`,
		accountID, regionName)
	r := &Region{}
	err := row.Scan(&r.RegionName, &r.OptStatus)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	return r, nil
}

func (s *Store) SetRegionOptStatus(accountID, regionName, status string) error {
	s.ensureRegions(accountID)
	_, err := s.db().Exec(
		`INSERT INTO account_regions (account_id, region_name, opt_status) VALUES (?,?,?)
		 ON CONFLICT(account_id, region_name) DO UPDATE SET opt_status=excluded.opt_status`,
		accountID, regionName, status)
	return err
}

// --- Primary Email ---

func (s *Store) GetPrimaryEmail(accountID string) (email, pending string) {
	s.db().QueryRow(`SELECT COALESCE(email,''), COALESCE(pending_email,'') FROM account_primary_email WHERE account_id=?`, accountID).
		Scan(&email, &pending)
	return
}

func (s *Store) SetPrimaryEmail(accountID, email string) error {
	_, err := s.db().Exec(
		`INSERT INTO account_primary_email (account_id, email) VALUES (?,?)
		 ON CONFLICT(account_id) DO UPDATE SET email=excluded.email`,
		accountID, email)
	return err
}

func (s *Store) StartPrimaryEmailUpdate(accountID, pendingEmail string) error {
	_, err := s.db().Exec(
		`INSERT INTO account_primary_email (account_id, pending_email) VALUES (?,?)
		 ON CONFLICT(account_id) DO UPDATE SET pending_email=excluded.pending_email`,
		accountID, pendingEmail)
	return err
}

func (s *Store) AcceptPrimaryEmailUpdate(accountID string) error {
	row := s.db().QueryRow(`SELECT COALESCE(pending_email,'') FROM account_primary_email WHERE account_id=?`, accountID)
	var pending string
	row.Scan(&pending)
	if pending == "" {
		return ErrNotFound
	}
	_, err := s.db().Exec(
		`UPDATE account_primary_email SET email=pending_email, pending_email='' WHERE account_id=?`, accountID)
	return err
}
