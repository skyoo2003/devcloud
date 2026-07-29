// SPDX-License-Identifier: Apache-2.0

package config

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestExpandTiers_All(t *testing.T) {
	result, warnings := expandTiers("all")
	assert.Nil(t, result, "all should return nil (no filtering)")
	assert.Empty(t, warnings)
}

func TestExpandTiers_Tier1(t *testing.T) {
	result, _ := expandTiers("tier1")
	assert.Contains(t, result, "s3")
	assert.Contains(t, result, "sns")
	assert.Contains(t, result, "kms")
	assert.Contains(t, result, "ec2")
	assert.NotContains(t, result, "cognito")
}

func TestExpandTiers_Mixed(t *testing.T) {
	result, _ := expandTiers("tier1,cognito,rds")
	assert.Contains(t, result, "s3")
	assert.Contains(t, result, "cognito")
	assert.Contains(t, result, "rds")
}

// TestExpandTiers_UnknownToken_TreatedAsService documents the current
// behavior: unknown tokens are passed through as service names, allowing
// users to mix tier shortcuts with individual service names. A token that
// looks like a mistyped tier is reported as a warning.
func TestExpandTiers_UnknownToken_TreatedAsService(t *testing.T) {
	result, warnings := expandTiers("tier1,kinesis,some-custom-service,tier9")
	assert.Contains(t, result, "s3", "tier1 expanded")
	assert.Contains(t, result, "kinesis", "explicit service included")
	assert.Contains(t, result, "some-custom-service",
		"unknown tokens pass through as service names (current contract)")
	require.Len(t, warnings, 1, "only the tier-looking typo warns")
	assert.Contains(t, warnings[0], "tier9")
}

func TestService_EnvServiceFilter(t *testing.T) {
	cfg := &Config{
		Services: map[string]ServiceConfig{
			"s3":  {Enabled: true},
			"sqs": {Enabled: true},
		},
	}
	t.Setenv("DEVCLOUD_SERVICES", "s3")
	applyEnvOverrides(cfg)
	assert.True(t, cfg.Service("s3").Enabled)
	assert.False(t, cfg.Service("sqs").Enabled)
}

// TestService_NoServicesBlock_EnablesEverything covers the zero-config
// contract: with no services block every service is enabled and lands under
// ./data/<id>.
func TestService_NoServicesBlock_EnablesEverything(t *testing.T) {
	cfg, _, err := parse([]byte("server:\n  port: 4747\n"))
	require.NoError(t, err)

	svc := cfg.Service("s3")
	assert.True(t, svc.Enabled)
	assert.Equal(t, filepath.Join("./data", "s3"), svc.DataDir)
	assert.True(t, cfg.Service("some-new-service").Enabled,
		"a service absent from the config still runs when there is no services block")
}

// TestService_ExplicitBlockIsAuthoritative verifies that listing any service
// restricts startup to that list — otherwise a minimal config would silently
// start all 100+ services.
func TestService_ExplicitBlockIsAuthoritative(t *testing.T) {
	cfg, _, err := parse([]byte("services:\n  s3:\n    enabled: true\n"))
	require.NoError(t, err)
	assert.True(t, cfg.Service("s3").Enabled)
	assert.False(t, cfg.Service("sqs").Enabled, "unlisted service must not start")
}

func TestService_DataDirOverride(t *testing.T) {
	yaml := []byte("services:\n  s3:\n    enabled: true\n    data_dir: ./custom/s3\n")

	cfg, _, err := parse(yaml)
	require.NoError(t, err)
	assert.Equal(t, "./custom/s3", cfg.Service("s3").DataDir)

	t.Setenv("DEVCLOUD_DATA_DIR", "/tmp/dc")
	cfg, _, err = parse(yaml)
	require.NoError(t, err)
	assert.Equal(t, filepath.Join("/tmp/dc", "s3"), cfg.Service("s3").DataDir,
		"DEVCLOUD_DATA_DIR overrides data_dir")
}

func TestLoadConfig_DefaultFile(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "devcloud.yaml")
	err := os.WriteFile(cfgPath, []byte(`
server:
  port: 4747
services:
  s3:
    enabled: true
    data_dir: ./data/s3
admin:
  enabled: false
logging:
  level: info
  format: text
`), 0644)
	require.NoError(t, err)

	cfg, warnings, err := Load(cfgPath)
	require.NoError(t, err)
	assert.Empty(t, warnings)
	assert.Equal(t, 4747, cfg.Server.Port)
	assert.True(t, cfg.Service("s3").Enabled)
	assert.Equal(t, "./data/s3", cfg.Service("s3").DataDir)
	assert.False(t, cfg.Admin.Enabled)
	assert.Equal(t, "info", cfg.Logging.Level)
}

// TestLoadOrDefault_FileMissing_UsesEmbedded verifies zero-config startup:
// when the fallback path doesn't exist, LoadOrDefault returns the embedded
// default configuration (port 4747, every service enabled).
func TestLoadOrDefault_FileMissing_UsesEmbedded(t *testing.T) {
	cfg, _, err := LoadOrDefault(filepath.Join(t.TempDir(), "nonexistent.yaml"))
	require.NoError(t, err)
	require.NotNil(t, cfg)
	assert.Equal(t, 4747, cfg.Server.Port, "embedded default port should be 4747")
	assert.Empty(t, cfg.Services, "embedded default carries no services block")
	assert.True(t, cfg.Service("s3").Enabled, "s3 should be enabled in embedded default")
}

// TestLoadOrDefault_FileExists_UsesFile verifies that when the fallback path
// exists, its contents are loaded (not the embedded default).
func TestLoadOrDefault_FileExists_UsesFile(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "devcloud.yaml")
	require.NoError(t, os.WriteFile(cfgPath, []byte(`
server:
  port: 5858
services:
  s3:
    enabled: true
    data_dir: ./custom/s3
`), 0644))

	cfg, _, err := LoadOrDefault(cfgPath)
	require.NoError(t, err)
	assert.Equal(t, 5858, cfg.Server.Port, "should use file's port, not embedded default")
	assert.Equal(t, "./custom/s3", cfg.Service("s3").DataDir)
}

// TestLoadOrDefault_EmptyPath_UsesEmbedded verifies that an empty fallback
// path skips the file check entirely and uses embedded defaults.
func TestLoadOrDefault_EmptyPath_UsesEmbedded(t *testing.T) {
	cfg, _, err := LoadOrDefault("")
	require.NoError(t, err)
	assert.Equal(t, 4747, cfg.Server.Port)
	assert.True(t, cfg.Service("dynamodb").Enabled)
}

// TestLoad_ExplicitPathMissing_ReturnsError verifies that Load (strict)
// surfaces an error when the explicit path does not exist — unlike
// LoadOrDefault, which falls back to embedded defaults.
func TestLoad_ExplicitPathMissing_ReturnsError(t *testing.T) {
	_, _, err := Load(filepath.Join(t.TempDir(), "nope.yaml"))
	require.Error(t, err, "Load should error on missing file; use LoadOrDefault for graceful fallback")
}

// TestParse_DeprecatedDashboardKey verifies the legacy 'dashboard' config key
// still enables the admin API after the rename (with a deprecation warning),
// so existing deployments don't silently lose it.
func TestParse_DeprecatedDashboardKey(t *testing.T) {
	cfg, warnings, err := parse([]byte("dashboard:\n  enabled: true\n"))
	require.NoError(t, err)
	assert.True(t, cfg.Admin.Enabled, "deprecated dashboard.enabled=true should enable admin")
	assert.Nil(t, cfg.Dashboard, "deprecated key should be cleared after folding into admin")
	require.Len(t, warnings, 1)
	assert.Contains(t, warnings[0], "deprecated")
}

// TestParse_AdminKeyWinsOverDeprecated verifies an explicit 'admin' block takes
// precedence over the deprecated 'dashboard' key when both are present.
func TestParse_AdminKeyWinsOverDeprecated(t *testing.T) {
	cfg, _, err := parse([]byte("admin:\n  enabled: true\ndashboard:\n  enabled: false\n"))
	require.NoError(t, err)
	assert.True(t, cfg.Admin.Enabled, "explicit admin.enabled=true should win")

	// The reverse precedence must also hold: an explicit admin.enabled=false
	// must not be re-enabled by a leftover deprecated dashboard.enabled=true.
	cfg, _, err = parse([]byte("admin:\n  enabled: false\ndashboard:\n  enabled: true\n"))
	require.NoError(t, err)
	assert.False(t, cfg.Admin.Enabled, "explicit admin.enabled=false should win over deprecated dashboard.enabled=true")
}

// TestParse_EmptyData_FillsDefaults verifies that parsing an empty YAML
// payload yields a Config with at least the default server port populated,
// so downstream code sees a usable config rather than a zero-value one.
func TestParse_EmptyData_FillsDefaults(t *testing.T) {
	cfg, _, err := parse([]byte(""))
	require.NoError(t, err)
	assert.Equal(t, 4747, cfg.Server.Port, "empty data should still yield default port 4747")
	require.NotNil(t, cfg.Admin)
	assert.False(t, cfg.Admin.Enabled)
}
