// SPDX-License-Identifier: Apache-2.0

// internal/services/iotwireless/store.go
package iotwireless

import (
	"database/sql"
	"encoding/json"
	"errors"
	"path/filepath"

	"github.com/skyoo2003/devcloud/internal/shared"
	"github.com/skyoo2003/devcloud/internal/storage/sqlite"
)

var (
	errDestinationNotFound           = errors.New("destination not found")
	errDeviceProfileNotFound         = errors.New("device profile not found")
	errServiceProfileNotFound        = errors.New("service profile not found")
	errWirelessDeviceNotFound        = errors.New("wireless device not found")
	errWirelessGatewayNotFound       = errors.New("wireless gateway not found")
	errFuotaTaskNotFound             = errors.New("fuota task not found")
	errMulticastGroupNotFound        = errors.New("multicast group not found")
	errNetworkAnalyzerConfigNotFound = errors.New("network analyzer configuration not found")
)

var migrations = []sqlite.Migration{
	{Version: 1, SQL: `
		CREATE TABLE IF NOT EXISTS destinations (
			name            TEXT PRIMARY KEY,
			arn             TEXT NOT NULL UNIQUE,
			expression      TEXT NOT NULL DEFAULT '',
			expression_type TEXT NOT NULL DEFAULT 'RuleName',
			role_arn        TEXT NOT NULL DEFAULT '',
			description     TEXT NOT NULL DEFAULT ''
		);
		CREATE TABLE IF NOT EXISTS device_profiles (
			id     TEXT PRIMARY KEY,
			arn    TEXT NOT NULL UNIQUE,
			name   TEXT NOT NULL DEFAULT '',
			config TEXT NOT NULL DEFAULT '{}'
		);
		CREATE TABLE IF NOT EXISTS service_profiles (
			id     TEXT PRIMARY KEY,
			arn    TEXT NOT NULL UNIQUE,
			name   TEXT NOT NULL DEFAULT '',
			config TEXT NOT NULL DEFAULT '{}'
		);
		CREATE TABLE IF NOT EXISTS wireless_devices (
			id          TEXT PRIMARY KEY,
			arn         TEXT NOT NULL UNIQUE,
			name        TEXT NOT NULL DEFAULT '',
			type        TEXT NOT NULL DEFAULT 'LoRaWAN',
			destination TEXT NOT NULL DEFAULT '',
			thing_arn   TEXT NOT NULL DEFAULT '',
			description TEXT NOT NULL DEFAULT '',
			config      TEXT NOT NULL DEFAULT '{}'
		);
		CREATE TABLE IF NOT EXISTS wireless_gateways (
			id          TEXT PRIMARY KEY,
			arn         TEXT NOT NULL UNIQUE,
			name        TEXT NOT NULL DEFAULT '',
			description TEXT NOT NULL DEFAULT '',
			config      TEXT NOT NULL DEFAULT '{}',
			thing_arn   TEXT NOT NULL DEFAULT ''
		);
		CREATE TABLE IF NOT EXISTS fuota_tasks (
			id     TEXT PRIMARY KEY,
			arn    TEXT NOT NULL UNIQUE,
			name   TEXT NOT NULL DEFAULT '',
			status TEXT NOT NULL DEFAULT 'Pending',
			config TEXT NOT NULL DEFAULT '{}'
		);
		CREATE TABLE IF NOT EXISTS multicast_groups (
			id     TEXT PRIMARY KEY,
			arn    TEXT NOT NULL UNIQUE,
			name   TEXT NOT NULL DEFAULT '',
			status TEXT NOT NULL DEFAULT 'Active',
			config TEXT NOT NULL DEFAULT '{}'
		);
		CREATE TABLE IF NOT EXISTS network_analyzer_configs (
			name   TEXT PRIMARY KEY,
			arn    TEXT NOT NULL UNIQUE,
			config TEXT NOT NULL DEFAULT '{}'
		);
	`},
}

// --- Model types ---

type Destination struct {
	Name           string
	ARN            string
	Expression     string
	ExpressionType string
	RoleARN        string
	Description    string
}

type DeviceProfile struct {
	ID     string
	ARN    string
	Name   string
	Config string // JSON
}

type ServiceProfile struct {
	ID     string
	ARN    string
	Name   string
	Config string // JSON
}

type WirelessDevice struct {
	ID          string
	ARN         string
	Name        string
	Type        string
	Destination string
	ThingARN    string
	Description string
	Config      string // JSON
}

type WirelessGateway struct {
	ID          string
	ARN         string
	Name        string
	Description string
	Config      string // JSON
	ThingARN    string
}

type FuotaTask struct {
	ID     string
	ARN    string
	Name   string
	Status string
	Config string // JSON
}

type MulticastGroup struct {
	ID     string
	ARN    string
	Name   string
	Status string
	Config string // JSON
}

type NetworkAnalyzerConfig struct {
	Name   string
	ARN    string
	Config string // JSON
}

// --- Store ---

type Store struct {
	store *sqlite.Store
	tags  *shared.TagStore
}

func NewStore(dataDir string) (*Store, error) {
	allMigrations := append(migrations, shared.TagMigrations...)
	dbPath := filepath.Join(dataDir, "iotwireless.db")
	s, err := sqlite.Open(dbPath, allMigrations)
	if err != nil {
		return nil, err
	}
	return &Store{store: s, tags: shared.NewTagStore(s)}, nil
}

func (s *Store) Close() error { return s.store.Close() }

// --- Destinations ---

func (s *Store) CreateDestination(d *Destination) error {
	_, err := s.store.DB().Exec(
		`INSERT INTO destinations (name, arn, expression, expression_type, role_arn, description) VALUES (?, ?, ?, ?, ?, ?)`,
		d.Name, d.ARN, d.Expression, d.ExpressionType, d.RoleARN, d.Description,
	)
	return err
}

func (s *Store) GetDestination(name string) (*Destination, error) {
	row := s.store.DB().QueryRow(
		`SELECT name, arn, expression, expression_type, role_arn, description FROM destinations WHERE name = ?`, name)
	return scanDestination(row)
}

func (s *Store) ListDestinations() ([]Destination, error) {
	rows, err := s.store.DB().Query(
		`SELECT name, arn, expression, expression_type, role_arn, description FROM destinations ORDER BY name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []Destination
	for rows.Next() {
		d, err := scanDestination(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, *d)
	}
	return result, rows.Err()
}

func (s *Store) UpdateDestination(name string, fields map[string]any) error {
	d, err := s.GetDestination(name)
	if err != nil {
		return errDestinationNotFound
	}
	if v, ok := fields["Expression"].(string); ok {
		d.Expression = v
	}
	if v, ok := fields["ExpressionType"].(string); ok {
		d.ExpressionType = v
	}
	if v, ok := fields["RoleArn"].(string); ok {
		d.RoleARN = v
	}
	if v, ok := fields["Description"].(string); ok {
		d.Description = v
	}
	res, err := s.store.DB().Exec(
		`UPDATE destinations SET expression=?, expression_type=?, role_arn=?, description=? WHERE name=?`,
		d.Expression, d.ExpressionType, d.RoleARN, d.Description, name,
	)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errDestinationNotFound
	}
	return nil
}

func (s *Store) DeleteDestination(name string) (*Destination, error) {
	d, err := s.GetDestination(name)
	if err != nil {
		return nil, errDestinationNotFound
	}
	if _, err := s.store.DB().Exec(`DELETE FROM destinations WHERE name = ?`, name); err != nil {
		return nil, err
	}
	return d, nil
}

// --- Device Profiles ---

func (s *Store) CreateDeviceProfile(dp *DeviceProfile) error {
	_, err := s.store.DB().Exec(
		`INSERT INTO device_profiles (id, arn, name, config) VALUES (?, ?, ?, ?)`,
		dp.ID, dp.ARN, dp.Name, dp.Config,
	)
	return err
}

func (s *Store) GetDeviceProfile(id string) (*DeviceProfile, error) {
	row := s.store.DB().QueryRow(
		`SELECT id, arn, name, config FROM device_profiles WHERE id = ?`, id)
	return scanDeviceProfile(row)
}

func (s *Store) ListDeviceProfiles() ([]DeviceProfile, error) {
	rows, err := s.store.DB().Query(
		`SELECT id, arn, name, config FROM device_profiles ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []DeviceProfile
	for rows.Next() {
		dp, err := scanDeviceProfile(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, *dp)
	}
	return result, rows.Err()
}

func (s *Store) DeleteDeviceProfile(id string) (*DeviceProfile, error) {
	dp, err := s.GetDeviceProfile(id)
	if err != nil {
		return nil, errDeviceProfileNotFound
	}
	if _, err := s.store.DB().Exec(`DELETE FROM device_profiles WHERE id = ?`, id); err != nil {
		return nil, err
	}
	return dp, nil
}

// --- Service Profiles ---

func (s *Store) CreateServiceProfile(sp *ServiceProfile) error {
	_, err := s.store.DB().Exec(
		`INSERT INTO service_profiles (id, arn, name, config) VALUES (?, ?, ?, ?)`,
		sp.ID, sp.ARN, sp.Name, sp.Config,
	)
	return err
}

func (s *Store) GetServiceProfile(id string) (*ServiceProfile, error) {
	row := s.store.DB().QueryRow(
		`SELECT id, arn, name, config FROM service_profiles WHERE id = ?`, id)
	return scanServiceProfile(row)
}

func (s *Store) ListServiceProfiles() ([]ServiceProfile, error) {
	rows, err := s.store.DB().Query(
		`SELECT id, arn, name, config FROM service_profiles ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []ServiceProfile
	for rows.Next() {
		sp, err := scanServiceProfile(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, *sp)
	}
	return result, rows.Err()
}

func (s *Store) DeleteServiceProfile(id string) (*ServiceProfile, error) {
	sp, err := s.GetServiceProfile(id)
	if err != nil {
		return nil, errServiceProfileNotFound
	}
	if _, err := s.store.DB().Exec(`DELETE FROM service_profiles WHERE id = ?`, id); err != nil {
		return nil, err
	}
	return sp, nil
}

// --- Wireless Devices ---

func (s *Store) CreateWirelessDevice(wd *WirelessDevice) error {
	_, err := s.store.DB().Exec(
		`INSERT INTO wireless_devices (id, arn, name, type, destination, thing_arn, description, config) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		wd.ID, wd.ARN, wd.Name, wd.Type, wd.Destination, wd.ThingARN, wd.Description, wd.Config,
	)
	return err
}

func (s *Store) GetWirelessDevice(id string) (*WirelessDevice, error) {
	row := s.store.DB().QueryRow(
		`SELECT id, arn, name, type, destination, thing_arn, description, config FROM wireless_devices WHERE id = ?`, id)
	return scanWirelessDevice(row)
}

func (s *Store) ListWirelessDevices() ([]WirelessDevice, error) {
	rows, err := s.store.DB().Query(
		`SELECT id, arn, name, type, destination, thing_arn, description, config FROM wireless_devices ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []WirelessDevice
	for rows.Next() {
		wd, err := scanWirelessDevice(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, *wd)
	}
	return result, rows.Err()
}

func (s *Store) UpdateWirelessDevice(id string, fields map[string]any) error {
	wd, err := s.GetWirelessDevice(id)
	if err != nil {
		return errWirelessDeviceNotFound
	}
	if v, ok := fields["Name"].(string); ok {
		wd.Name = v
	}
	if v, ok := fields["DestinationName"].(string); ok {
		wd.Destination = v
	}
	if v, ok := fields["Description"].(string); ok {
		wd.Description = v
	}
	res, err := s.store.DB().Exec(
		`UPDATE wireless_devices SET name=?, destination=?, description=? WHERE id=?`,
		wd.Name, wd.Destination, wd.Description, id,
	)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errWirelessDeviceNotFound
	}
	return nil
}

func (s *Store) DeleteWirelessDevice(id string) (*WirelessDevice, error) {
	wd, err := s.GetWirelessDevice(id)
	if err != nil {
		return nil, errWirelessDeviceNotFound
	}
	if _, err := s.store.DB().Exec(`DELETE FROM wireless_devices WHERE id = ?`, id); err != nil {
		return nil, err
	}
	return wd, nil
}

// --- Wireless Gateways ---

func (s *Store) CreateWirelessGateway(wg *WirelessGateway) error {
	_, err := s.store.DB().Exec(
		`INSERT INTO wireless_gateways (id, arn, name, description, config, thing_arn) VALUES (?, ?, ?, ?, ?, ?)`,
		wg.ID, wg.ARN, wg.Name, wg.Description, wg.Config, wg.ThingARN,
	)
	return err
}

func (s *Store) GetWirelessGateway(id string) (*WirelessGateway, error) {
	row := s.store.DB().QueryRow(
		`SELECT id, arn, name, description, config, thing_arn FROM wireless_gateways WHERE id = ?`, id)
	return scanWirelessGateway(row)
}

func (s *Store) ListWirelessGateways() ([]WirelessGateway, error) {
	rows, err := s.store.DB().Query(
		`SELECT id, arn, name, description, config, thing_arn FROM wireless_gateways ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []WirelessGateway
	for rows.Next() {
		wg, err := scanWirelessGateway(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, *wg)
	}
	return result, rows.Err()
}

func (s *Store) UpdateWirelessGateway(id string, fields map[string]any) error {
	wg, err := s.GetWirelessGateway(id)
	if err != nil {
		return errWirelessGatewayNotFound
	}
	if v, ok := fields["Name"].(string); ok {
		wg.Name = v
	}
	if v, ok := fields["Description"].(string); ok {
		wg.Description = v
	}
	res, err := s.store.DB().Exec(
		`UPDATE wireless_gateways SET name=?, description=? WHERE id=?`,
		wg.Name, wg.Description, id,
	)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errWirelessGatewayNotFound
	}
	return nil
}

func (s *Store) DeleteWirelessGateway(id string) (*WirelessGateway, error) {
	wg, err := s.GetWirelessGateway(id)
	if err != nil {
		return nil, errWirelessGatewayNotFound
	}
	if _, err := s.store.DB().Exec(`DELETE FROM wireless_gateways WHERE id = ?`, id); err != nil {
		return nil, err
	}
	return wg, nil
}

// --- Fuota Tasks ---

func (s *Store) CreateFuotaTask(ft *FuotaTask) error {
	_, err := s.store.DB().Exec(
		`INSERT INTO fuota_tasks (id, arn, name, status, config) VALUES (?, ?, ?, ?, ?)`,
		ft.ID, ft.ARN, ft.Name, ft.Status, ft.Config,
	)
	return err
}

func (s *Store) GetFuotaTask(id string) (*FuotaTask, error) {
	row := s.store.DB().QueryRow(
		`SELECT id, arn, name, status, config FROM fuota_tasks WHERE id = ?`, id)
	return scanFuotaTask(row)
}

func (s *Store) ListFuotaTasks() ([]FuotaTask, error) {
	rows, err := s.store.DB().Query(
		`SELECT id, arn, name, status, config FROM fuota_tasks ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []FuotaTask
	for rows.Next() {
		ft, err := scanFuotaTask(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, *ft)
	}
	return result, rows.Err()
}

func (s *Store) UpdateFuotaTask(id string, fields map[string]any) error {
	ft, err := s.GetFuotaTask(id)
	if err != nil {
		return errFuotaTaskNotFound
	}
	if v, ok := fields["Name"].(string); ok {
		ft.Name = v
	}
	if v, ok := fields["Status"].(string); ok {
		ft.Status = v
	}
	res, err := s.store.DB().Exec(
		`UPDATE fuota_tasks SET name=?, status=? WHERE id=?`,
		ft.Name, ft.Status, id,
	)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errFuotaTaskNotFound
	}
	return nil
}

func (s *Store) DeleteFuotaTask(id string) (*FuotaTask, error) {
	ft, err := s.GetFuotaTask(id)
	if err != nil {
		return nil, errFuotaTaskNotFound
	}
	if _, err := s.store.DB().Exec(`DELETE FROM fuota_tasks WHERE id = ?`, id); err != nil {
		return nil, err
	}
	return ft, nil
}

// --- Multicast Groups ---

func (s *Store) CreateMulticastGroup(mg *MulticastGroup) error {
	_, err := s.store.DB().Exec(
		`INSERT INTO multicast_groups (id, arn, name, status, config) VALUES (?, ?, ?, ?, ?)`,
		mg.ID, mg.ARN, mg.Name, mg.Status, mg.Config,
	)
	return err
}

func (s *Store) GetMulticastGroup(id string) (*MulticastGroup, error) {
	row := s.store.DB().QueryRow(
		`SELECT id, arn, name, status, config FROM multicast_groups WHERE id = ?`, id)
	return scanMulticastGroup(row)
}

func (s *Store) ListMulticastGroups() ([]MulticastGroup, error) {
	rows, err := s.store.DB().Query(
		`SELECT id, arn, name, status, config FROM multicast_groups ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []MulticastGroup
	for rows.Next() {
		mg, err := scanMulticastGroup(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, *mg)
	}
	return result, rows.Err()
}

func (s *Store) UpdateMulticastGroup(id string, fields map[string]any) error {
	mg, err := s.GetMulticastGroup(id)
	if err != nil {
		return errMulticastGroupNotFound
	}
	if v, ok := fields["Name"].(string); ok {
		mg.Name = v
	}
	res, err := s.store.DB().Exec(
		`UPDATE multicast_groups SET name=? WHERE id=?`,
		mg.Name, id,
	)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errMulticastGroupNotFound
	}
	return nil
}

func (s *Store) DeleteMulticastGroup(id string) (*MulticastGroup, error) {
	mg, err := s.GetMulticastGroup(id)
	if err != nil {
		return nil, errMulticastGroupNotFound
	}
	if _, err := s.store.DB().Exec(`DELETE FROM multicast_groups WHERE id = ?`, id); err != nil {
		return nil, err
	}
	return mg, nil
}

// --- Network Analyzer Configurations ---

func (s *Store) CreateNetworkAnalyzerConfig(nac *NetworkAnalyzerConfig) error {
	_, err := s.store.DB().Exec(
		`INSERT INTO network_analyzer_configs (name, arn, config) VALUES (?, ?, ?)`,
		nac.Name, nac.ARN, nac.Config,
	)
	return err
}

func (s *Store) GetNetworkAnalyzerConfig(name string) (*NetworkAnalyzerConfig, error) {
	row := s.store.DB().QueryRow(
		`SELECT name, arn, config FROM network_analyzer_configs WHERE name = ?`, name)
	return scanNetworkAnalyzerConfig(row)
}

func (s *Store) ListNetworkAnalyzerConfigs() ([]NetworkAnalyzerConfig, error) {
	rows, err := s.store.DB().Query(
		`SELECT name, arn, config FROM network_analyzer_configs ORDER BY name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []NetworkAnalyzerConfig
	for rows.Next() {
		nac, err := scanNetworkAnalyzerConfig(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, *nac)
	}
	return result, rows.Err()
}

func (s *Store) UpdateNetworkAnalyzerConfig(name string, fields map[string]any) error {
	nac, err := s.GetNetworkAnalyzerConfig(name)
	if err != nil {
		return errNetworkAnalyzerConfigNotFound
	}
	if v, ok := fields["Config"]; ok {
		b, _ := json.Marshal(v)
		nac.Config = string(b)
	}
	res, err := s.store.DB().Exec(
		`UPDATE network_analyzer_configs SET config=? WHERE name=?`,
		nac.Config, name,
	)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errNetworkAnalyzerConfigNotFound
	}
	return nil
}

func (s *Store) DeleteNetworkAnalyzerConfig(name string) (*NetworkAnalyzerConfig, error) {
	nac, err := s.GetNetworkAnalyzerConfig(name)
	if err != nil {
		return nil, errNetworkAnalyzerConfigNotFound
	}
	if _, err := s.store.DB().Exec(`DELETE FROM network_analyzer_configs WHERE name = ?`, name); err != nil {
		return nil, err
	}
	return nac, nil
}

// --- Scanner helpers ---

type scanner interface {
	Scan(dest ...any) error
}

func scanDestination(s scanner) (*Destination, error) {
	d := &Destination{}
	err := s.Scan(&d.Name, &d.ARN, &d.Expression, &d.ExpressionType, &d.RoleARN, &d.Description)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, errDestinationNotFound
	}
	return d, err
}

func scanDeviceProfile(s scanner) (*DeviceProfile, error) {
	dp := &DeviceProfile{}
	err := s.Scan(&dp.ID, &dp.ARN, &dp.Name, &dp.Config)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, errDeviceProfileNotFound
	}
	return dp, err
}

func scanServiceProfile(s scanner) (*ServiceProfile, error) {
	sp := &ServiceProfile{}
	err := s.Scan(&sp.ID, &sp.ARN, &sp.Name, &sp.Config)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, errServiceProfileNotFound
	}
	return sp, err
}

func scanWirelessDevice(s scanner) (*WirelessDevice, error) {
	wd := &WirelessDevice{}
	err := s.Scan(&wd.ID, &wd.ARN, &wd.Name, &wd.Type, &wd.Destination, &wd.ThingARN, &wd.Description, &wd.Config)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, errWirelessDeviceNotFound
	}
	return wd, err
}

func scanWirelessGateway(s scanner) (*WirelessGateway, error) {
	wg := &WirelessGateway{}
	err := s.Scan(&wg.ID, &wg.ARN, &wg.Name, &wg.Description, &wg.Config, &wg.ThingARN)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, errWirelessGatewayNotFound
	}
	return wg, err
}

func scanFuotaTask(s scanner) (*FuotaTask, error) {
	ft := &FuotaTask{}
	err := s.Scan(&ft.ID, &ft.ARN, &ft.Name, &ft.Status, &ft.Config)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, errFuotaTaskNotFound
	}
	return ft, err
}

func scanMulticastGroup(s scanner) (*MulticastGroup, error) {
	mg := &MulticastGroup{}
	err := s.Scan(&mg.ID, &mg.ARN, &mg.Name, &mg.Status, &mg.Config)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, errMulticastGroupNotFound
	}
	return mg, err
}

func scanNetworkAnalyzerConfig(s scanner) (*NetworkAnalyzerConfig, error) {
	nac := &NetworkAnalyzerConfig{}
	err := s.Scan(&nac.Name, &nac.ARN, &nac.Config)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, errNetworkAnalyzerConfigNotFound
	}
	return nac, err
}
