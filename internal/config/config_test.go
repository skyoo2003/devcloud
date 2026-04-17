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
	result := expandTiers("all")
	assert.Nil(t, result, "all should return nil (no filtering)")
}

func TestExpandTiers_Tier1(t *testing.T) {
	result := expandTiers("tier1")
	assert.Contains(t, result, "s3")
	assert.Contains(t, result, "sns")
	assert.Contains(t, result, "kms")
	assert.Contains(t, result, "ec2")
	assert.NotContains(t, result, "cognito")
}

func TestExpandTiers_Mixed(t *testing.T) {
	result := expandTiers("tier1,cognito,rds")
	assert.Contains(t, result, "s3")
	assert.Contains(t, result, "cognito")
	assert.Contains(t, result, "rds")
}

func TestApplyEnvOverrides_TierBased(t *testing.T) {
	cfg := &Config{
		Services: map[string]ServiceConfig{
			"s3":  {Enabled: true},
			"sqs": {Enabled: true},
		},
	}
	t.Setenv("DEVCLOUD_SERVICES", "s3")
	applyEnvOverrides(cfg)
	assert.True(t, cfg.Services["s3"].Enabled)
	assert.False(t, cfg.Services["sqs"].Enabled)
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
auth:
  enabled: false
dashboard:
  enabled: false
logging:
  level: info
  format: text
`), 0644)
	require.NoError(t, err)

	cfg, err := Load(cfgPath)
	require.NoError(t, err)
	assert.Equal(t, 4747, cfg.Server.Port)
	assert.True(t, cfg.Services["s3"].Enabled)
	assert.Equal(t, "./data/s3", cfg.Services["s3"].DataDir)
	assert.False(t, cfg.Auth.Enabled)
	assert.Equal(t, "info", cfg.Logging.Level)
}

func TestLoadConfig_EnvOverride(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "devcloud.yaml")
	err := os.WriteFile(cfgPath, []byte(`
server:
  port: 4747
services:
  s3:
    enabled: true
  sqs:
    enabled: true
`), 0644)
	require.NoError(t, err)

	t.Setenv("DEVCLOUD_SERVICES", "s3")
	cfg, err := Load(cfgPath)
	require.NoError(t, err)
	assert.True(t, cfg.Services["s3"].Enabled)
	assert.False(t, cfg.Services["sqs"].Enabled)
}

// TestLoadOrDefault_FileMissing_UsesEmbedded verifies zero-config startup:
// when the fallback path doesn't exist, LoadOrDefault returns the embedded
// default configuration (which has all services enabled and port 4747).
func TestLoadOrDefault_FileMissing_UsesEmbedded(t *testing.T) {
	cfg, err := LoadOrDefault(filepath.Join(t.TempDir(), "nonexistent.yaml"))
	require.NoError(t, err)
	require.NotNil(t, cfg)
	assert.Equal(t, 4747, cfg.Server.Port, "embedded default port should be 4747")
	assert.NotEmpty(t, cfg.Services, "embedded default should have services")
	assert.True(t, cfg.Services["s3"].Enabled, "s3 should be enabled in embedded default")
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

	cfg, err := LoadOrDefault(cfgPath)
	require.NoError(t, err)
	assert.Equal(t, 5858, cfg.Server.Port, "should use file's port, not embedded default")
	assert.Equal(t, "./custom/s3", cfg.Services["s3"].DataDir)
}

// TestLoadOrDefault_EmptyPath_UsesEmbedded verifies that an empty fallback
// path skips the file check entirely and uses embedded defaults.
func TestLoadOrDefault_EmptyPath_UsesEmbedded(t *testing.T) {
	cfg, err := LoadOrDefault("")
	require.NoError(t, err)
	assert.Equal(t, 4747, cfg.Server.Port)
	assert.NotEmpty(t, cfg.Services)
}

// TestLoad_ExplicitPathMissing_ReturnsError verifies that Load (strict)
// surfaces an error when the explicit path does not exist — unlike
// LoadOrDefault, which falls back to embedded defaults.
func TestLoad_ExplicitPathMissing_ReturnsError(t *testing.T) {
	_, err := Load(filepath.Join(t.TempDir(), "nope.yaml"))
	require.Error(t, err, "Load should error on missing file; use LoadOrDefault for graceful fallback")
}

// TestParse_EmptyData_FillsDefaults verifies that parsing an empty YAML
// payload yields a Config with at least the default server port populated,
// so downstream code sees a usable config rather than a zero-value one.
func TestParse_EmptyData_FillsDefaults(t *testing.T) {
	cfg, err := parse([]byte(""))
	require.NoError(t, err)
	assert.Equal(t, 4747, cfg.Server.Port, "empty data should still yield default port 4747")
}

// TestExpandTiers_UnknownToken_TreatedAsService documents the current
// behavior: unknown tokens are passed through as service names, allowing
// users to mix tier shortcuts with individual service names.
func TestExpandTiers_UnknownToken_TreatedAsService(t *testing.T) {
	result := expandTiers("tier1,kinesis,some-custom-service")
	assert.Contains(t, result, "s3", "tier1 expanded")
	assert.Contains(t, result, "kinesis", "explicit service included")
	assert.Contains(t, result, "some-custom-service",
		"unknown tokens pass through as service names (current contract)")
}
