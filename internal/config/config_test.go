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

// isolateEnv clears the env overrides parse() reads, so a developer or CI runner
// that exports DEVCLOUD_DATA_DIR / DEVCLOUD_SERVICES / DEVCLOUD_PORT does not
// fail every assertion about ports, data dirs, and enabled services.
// t.Setenv restores the previous value when the test ends.
func isolateEnv(t *testing.T) {
	t.Helper()
	for _, k := range []string{"DEVCLOUD_DATA_DIR", "DEVCLOUD_SERVICES", "DEVCLOUD_PORT"} {
		t.Setenv(k, "")
	}
}

func TestService_EnvServiceFilter(t *testing.T) {
	isolateEnv(t)
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
	isolateEnv(t)
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
	isolateEnv(t)
	cfg, _, err := parse([]byte("services:\n  s3:\n    enabled: true\n"))
	require.NoError(t, err)
	assert.True(t, cfg.Service("s3").Enabled)
	assert.False(t, cfg.Service("sqs").Enabled, "unlisted service must not start")
}

// TestService_EmptyServicesBlock_RunsNothing covers "services: {}". YAML makes
// that a non-nil empty map, so it is a block that lists nothing — and a block
// is authoritative. Treating it like an absent block would start all 104
// services, which is the opposite of what the operator wrote.
func TestService_EmptyServicesBlock_RunsNothing(t *testing.T) {
	isolateEnv(t)
	cfg, _, err := parse([]byte("services: {}\n"))
	require.NoError(t, err)
	require.NotNil(t, cfg.Services, "an empty block must survive parsing as a non-nil map")
	assert.False(t, cfg.Service("s3").Enabled, "an empty services block lists nothing, so nothing runs")
}

// TestService_EnvSelectionOverridesBlock covers the precedence
// docs/configuration.md states: DEVCLOUD_SERVICES names the running set
// outright. It must be able to add a service the block omits — not merely
// intersect with the block, which would leave "env names sqs, YAML lists s3"
// running nothing at all.
func TestService_EnvSelectionOverridesBlock(t *testing.T) {
	isolateEnv(t)
	t.Setenv("DEVCLOUD_SERVICES", "sqs,dynamodb")

	cfg, _, err := parse([]byte(`
services:
  s3:
    enabled: true
  dynamodb:
    enabled: false
    data_dir: ./custom/ddb
`))
	require.NoError(t, err)

	assert.True(t, cfg.Service("sqs").Enabled, "env must enable a service the block omits")
	assert.True(t, cfg.Service("dynamodb").Enabled, "env must override the block's enabled: false")
	assert.False(t, cfg.Service("s3").Enabled, "env must disable a service it does not name")
	assert.Equal(t, "./custom/ddb", cfg.Service("dynamodb").DataDir,
		"the block still supplies data_dir for a service the env enables")
}

func TestService_DataDirOverride(t *testing.T) {
	isolateEnv(t)
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
	isolateEnv(t)
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
	isolateEnv(t)
	cfg, _, err := LoadOrDefault(filepath.Join(t.TempDir(), "nonexistent.yaml"))
	require.NoError(t, err)
	require.NotNil(t, cfg)
	assert.Equal(t, 4747, cfg.Server.Port, "embedded default port should be 4747")
	assert.Nil(t, cfg.Services, "embedded default carries no services block — nil, not an empty one, which would run nothing")
	assert.True(t, cfg.Service("s3").Enabled, "s3 should be enabled in embedded default")
}

// TestLoadOrDefault_FileExists_UsesFile verifies that when the fallback path
// exists, its contents are loaded (not the embedded default).
func TestLoadOrDefault_FileExists_UsesFile(t *testing.T) {
	isolateEnv(t)
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
	isolateEnv(t)
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

// TestParse_RemovedAuthKeyWarns verifies the removed 'auth' key is reported
// rather than silently ignored: yaml.Unmarshal drops unknown keys, so an
// operator who set auth.enabled: true to require SigV4 would otherwise get no
// hint that nothing validates signatures.
func TestParse_RemovedAuthKeyWarns(t *testing.T) {
	_, warnings, err := parse([]byte("auth:\n  enabled: true\n"))
	require.NoError(t, err)
	require.Len(t, warnings, 1)
	assert.Contains(t, warnings[0], "auth.enabled")
	assert.Contains(t, warnings[0], "not implemented")

	// An explicit auth.enabled: false is still stale config worth flagging, but
	// it must not claim credentials are being accepted contrary to intent.
	_, warnings, err = parse([]byte("auth:\n  enabled: false\n"))
	require.NoError(t, err)
	require.Len(t, warnings, 1)
	assert.Contains(t, warnings[0], "removed")
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

// TestGuaranteedConfigSurface locks the config keys docs/compatibility-policy.md
// promises will keep their name and meaning across 1.x. Adding a key is fine;
// removing or repurposing one of these is a major-version event, and this test
// is what makes that visible instead of silent. The deprecated 'dashboard' and
// removed 'auth' keys are covered by TestParse_DeprecatedDashboardKey and
// TestParse_RemovedAuthKeyWarns.
func TestGuaranteedConfigSurface(t *testing.T) {
	isolateEnv(t)

	cfg, warnings, err := parse([]byte(`
server:
  port: 5555
services:
  s3:
    enabled: true
    data_dir: ./custom/s3
admin:
  enabled: true
logging:
  level: debug
  format: json
`))
	require.NoError(t, err)
	assert.Empty(t, warnings, "the guaranteed surface must parse without warnings")

	// t.Errorf per key, not require: one removed field should not hide the rest.
	for _, tc := range []struct {
		key  string
		got  any
		want any
	}{
		{"server.port", cfg.Server.Port, 5555},
		{"services.<id>.enabled", cfg.Service("s3").Enabled, true},
		{"services.<id>.data_dir", cfg.Service("s3").DataDir, "./custom/s3"},
		{"services (block is authoritative)", cfg.Service("sqs").Enabled, false},
		{"admin.enabled", cfg.Admin.Enabled, true},
		{"logging.level", cfg.Logging.Level, "debug"},
		{"logging.format", cfg.Logging.Format, "json"},
	} {
		if tc.got != tc.want {
			t.Errorf("%s: got %v, want %v — guaranteed by docs/compatibility-policy.md", tc.key, tc.got, tc.want)
		}
	}
}

// TestGuaranteedEnvSurface locks the three environment overrides the policy
// guarantees, including their precedence over the config file.
func TestGuaranteedEnvSurface(t *testing.T) {
	isolateEnv(t)
	t.Setenv("DEVCLOUD_PORT", "6060")
	// s3 is listed in the YAML below, lambda deliberately is not: naming only a
	// listed service would let a merely-intersecting filter pass this test.
	t.Setenv("DEVCLOUD_SERVICES", "s3,lambda")
	t.Setenv("DEVCLOUD_DATA_DIR", "/tmp/dc")

	cfg, _, err := parse([]byte(`
server:
  port: 4747
services:
  s3:
    enabled: true
    data_dir: ./custom/s3
  sqs:
    enabled: true
`))
	require.NoError(t, err)

	assert.Equal(t, 6060, cfg.Server.Port, "DEVCLOUD_PORT must override server.port")
	assert.True(t, cfg.Service("s3").Enabled, "DEVCLOUD_SERVICES must keep the service it names")
	assert.True(t, cfg.Service("lambda").Enabled, "DEVCLOUD_SERVICES must enable a service the file omits")
	assert.False(t, cfg.Service("sqs").Enabled, "DEVCLOUD_SERVICES must filter out services it does not name")
	assert.Equal(t, filepath.Join("/tmp/dc", "s3"), cfg.Service("s3").DataDir,
		"DEVCLOUD_DATA_DIR must rebase data dirs, overriding data_dir")
}

// TestParse_EmptyData_FillsDefaults verifies that parsing an empty YAML
// payload yields a Config with at least the default server port populated,
// so downstream code sees a usable config rather than a zero-value one.
func TestParse_EmptyData_FillsDefaults(t *testing.T) {
	isolateEnv(t)
	cfg, _, err := parse([]byte(""))
	require.NoError(t, err)
	assert.Equal(t, 4747, cfg.Server.Port, "empty data should still yield default port 4747")
	require.NotNil(t, cfg.Admin)
	assert.False(t, cfg.Admin.Enabled)
}
