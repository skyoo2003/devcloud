// SPDX-License-Identifier: Apache-2.0

package lambda

import (
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/skyoo2003/devcloud/internal/plugin"
)

// LayerVersionInfo holds metadata for a Lambda layer version.
type LayerVersionInfo struct {
	LayerName          string
	Version            int
	Description        string
	CodePath           string
	CodeSize           int64
	LicenseInfo        string
	CompatibleRuntimes []string
	AccountID          string
	CreatedAt          time.Time
}

// PublishLayerVersion publishes a new version of a layer.
func (s *LambdaStore) PublishLayerVersion(name, accountID, desc, license string, runtimes []string, zipBytes []byte) (*LayerVersionInfo, error) {
	s.codeMu.Lock()
	defer s.codeMu.Unlock()

	var maxVer sql.NullInt64
	err := s.store.DB().QueryRow(
		`SELECT MAX(version) FROM layer_versions WHERE layer_name = ? AND account_id = ?;`,
		name, accountID,
	).Scan(&maxVer)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return nil, err
	}

	newVersion := 1
	if maxVer.Valid {
		newVersion = int(maxVer.Int64) + 1
	}

	layerDir := filepath.Join(s.codeDir, "layers", name, fmt.Sprintf("%d", newVersion))
	if err := os.MkdirAll(layerDir, 0o755); err != nil {
		return nil, err
	}
	codePath := filepath.Join(layerDir, "layer.zip")
	if err := os.WriteFile(codePath, zipBytes, 0o600); err != nil {
		return nil, err
	}

	runtimesJSON, _ := json.Marshal(runtimes)
	now := time.Now().UTC()

	_, err = s.store.DB().Exec(
		`INSERT INTO layer_versions (layer_name, version, description, code_path, code_size, license_info, compatible_runtimes, account_id, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);`,
		name, newVersion, desc, codePath, len(zipBytes), license, string(runtimesJSON), accountID, now,
	)
	if err != nil {
		return nil, err
	}

	return &LayerVersionInfo{
		LayerName:          name,
		Version:            newVersion,
		Description:        desc,
		CodePath:           codePath,
		CodeSize:           int64(len(zipBytes)),
		LicenseInfo:        license,
		CompatibleRuntimes: runtimes,
		AccountID:          accountID,
		CreatedAt:          now,
	}, nil
}

// AddLayerVersionPermission adds a permission statement to a layer version.
func (s *LambdaStore) AddLayerVersionPermission(name string, version int, accountID, statementID, action, principal, orgID string) error {
	_, err := s.store.DB().Exec(
		`INSERT INTO layer_permissions (layer_name, version, statement_id, action, principal, organization_id, account_id)
		VALUES (?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(layer_name, version, statement_id, account_id) DO UPDATE SET
			action = excluded.action,
			principal = excluded.principal,
			organization_id = excluded.organization_id;`,
		name, version, statementID, action, principal, orgID, accountID,
	)
	return err
}

// RemoveLayerVersionPermission removes a permission statement from a layer version.
func (s *LambdaStore) RemoveLayerVersionPermission(name string, version int, accountID, statementID string) error {
	res, err := s.store.DB().Exec(
		`DELETE FROM layer_permissions WHERE layer_name = ? AND version = ? AND statement_id = ? AND account_id = ?;`,
		name, version, statementID, accountID,
	)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrPermissionNotFound
	}
	return nil
}

// Layer HTTP handlers on LambdaProvider

type publishLayerVersionInput struct {
	Content struct {
		ZipFile  string `json:"ZipFile"`
		S3Bucket string `json:"S3Bucket"`
		S3Key    string `json:"S3Key"`
	} `json:"Content"`
	Description        string   `json:"Description"`
	CompatibleRuntimes []string `json:"CompatibleRuntimes"`
	LicenseInfo        string   `json:"LicenseInfo"`
}

func (p *LambdaProvider) publishLayerVersion(name string, req *http.Request) (*plugin.Response, error) {
	var in publishLayerVersionInput
	if req.Body != nil {
		body, _ := io.ReadAll(req.Body)
		_ = json.Unmarshal(body, &in)
	}

	var zipBytes []byte
	if in.Content.ZipFile != "" {
		decoded, err := base64.StdEncoding.DecodeString(in.Content.ZipFile)
		if err == nil {
			zipBytes = decoded
		}
	}

	info, err := p.store.PublishLayerVersion(name, defaultAccountID, in.Description, in.LicenseInfo, in.CompatibleRuntimes, zipBytes)
	if err != nil {
		return nil, err
	}

	layerArn := fmt.Sprintf("arn:aws:lambda:%s:%s:layer:%s", defaultRegion, defaultAccountID, name)
	versionArn := fmt.Sprintf("%s:%d", layerArn, info.Version)

	out := map[string]any{
		"LayerVersionArn":    versionArn,
		"LayerArn":           layerArn,
		"Description":        info.Description,
		"CreatedDate":        info.CreatedAt.Format("2006-01-02T15:04:05.000+0000"),
		"Version":            info.Version,
		"CompatibleRuntimes": info.CompatibleRuntimes,
		"LicenseInfo":        info.LicenseInfo,
		"Content": map[string]any{
			"CodeSize": info.CodeSize,
		},
	}
	bytes, _ := json.Marshal(out)
	return &plugin.Response{
		StatusCode:  http.StatusCreated,
		ContentType: "application/json",
		Body:        bytes,
	}, nil
}

type addLayerPermissionInput struct {
	StatementId    string `json:"StatementId"`
	Action         string `json:"Action"`
	Principal      string `json:"Principal"`
	OrganizationId string `json:"OrganizationId"`
}

func (p *LambdaProvider) addLayerVersionPermission(name string, version int, req *http.Request) (*plugin.Response, error) {
	var in addLayerPermissionInput
	if req.Body != nil {
		body, _ := io.ReadAll(req.Body)
		_ = json.Unmarshal(body, &in)
	}

	if in.StatementId == "" || in.Action == "" || in.Principal == "" {
		return lambdaError("InvalidParameterValueException", "StatementId, Action, and Principal are required", http.StatusBadRequest), nil
	}

	if err := p.store.AddLayerVersionPermission(name, version, defaultAccountID, in.StatementId, in.Action, in.Principal, in.OrganizationId); err != nil {
		return nil, err
	}

	stmt := fmt.Sprintf(`{"Sid":%q,"Effect":"Allow","Principal":%q,"Action":%q}`, in.StatementId, in.Principal, in.Action)
	out := map[string]any{
		"Statement":  stmt,
		"RevisionId": "devcloud-rev-1",
	}
	bytes, _ := json.Marshal(out)
	return &plugin.Response{
		StatusCode:  http.StatusCreated,
		ContentType: "application/json",
		Body:        bytes,
	}, nil
}

func (p *LambdaProvider) removeLayerVersionPermission(name string, version int, statementID string) (*plugin.Response, error) {
	err := p.store.RemoveLayerVersionPermission(name, version, defaultAccountID, statementID)
	if err != nil {
		if errors.Is(err, ErrPermissionNotFound) {
			return lambdaError("ResourceNotFoundException", fmt.Sprintf("Statement %s not found", statementID), http.StatusNotFound), nil
		}
		return nil, err
	}
	return &plugin.Response{
		StatusCode: http.StatusNoContent,
	}, nil
}
