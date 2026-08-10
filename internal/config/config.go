// SPDX-License-Identifier: Apache-2.0

package config

import (
	_ "embed"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"
)

// defaultConfigYAML is the built-in configuration used when no YAML file is
// provided. It carries no services block, which means "every registered
// service, enabled, under ./data/<id>" — see Config.Service.
//
//go:embed default.yaml
var defaultConfigYAML []byte

// defaultDataDir is the base directory services store data under when neither
// the YAML nor DEVCLOUD_DATA_DIR names one.
const defaultDataDir = "./data"

type Config struct {
	Server   ServerConfig             `yaml:"server"`
	Services map[string]ServiceConfig `yaml:"services"`
	Admin    *AdminConfig             `yaml:"admin"`
	Logging  LoggingConfig            `yaml:"logging"`

	// Dashboard is the deprecated pre-rename name for Admin. It is honoured for
	// one release (see parse) so existing configs keep working; use Admin.
	// Both Admin and Dashboard are pointers so parse can tell an explicit
	// block from an absent one and apply the correct precedence.
	Dashboard *AdminConfig `yaml:"dashboard"`

	// Auth is the removed pre-1.0 auth block. SigV4 enforcement was never
	// implemented, so the key is gone — but yaml.Unmarshal ignores unknown keys
	// silently, and an operator who wrote auth.enabled: true to require
	// signature validation must not be left believing it took effect. Parsed
	// only to warn (see parse); remove with the dashboard key.
	Auth *AuthConfig `yaml:"auth"`

	// allowed is the DEVCLOUD_SERVICES filter; nil means no filtering.
	allowed map[string]bool
	// baseDir is the DEVCLOUD_DATA_DIR override; "" means honour data_dir.
	baseDir string
}

type ServerConfig struct {
	Port int `yaml:"port"`
}

type ServiceConfig struct {
	Enabled bool   `yaml:"enabled"`
	DataDir string `yaml:"data_dir"`
}

type AdminConfig struct {
	Enabled bool `yaml:"enabled"`
}

// AuthConfig is the shape of the removed auth block, kept only so parse can
// warn about it instead of ignoring it. See Config.Auth.
type AuthConfig struct {
	Enabled bool `yaml:"enabled"`
}

type LoggingConfig struct {
	Level  string `yaml:"level"`
	Format string `yaml:"format"`
}

// Service returns the effective configuration for serviceID.
//
// A YAML services block is authoritative: only the services it lists can run.
// An empty block therefore runs nothing — it is a block, so it decides. Absent
// entirely (the embedded default), every service is enabled with data_dir
// <base>/<id>. DEVCLOUD_SERVICES and DEVCLOUD_DATA_DIR are applied here so both
// paths agree.
func (c *Config) Service(serviceID string) ServiceConfig {
	svc, listed := c.Services[serviceID]
	switch {
	case c.allowed != nil:
		// DEVCLOUD_SERVICES names the running set outright, so it decides
		// membership on its own: it can enable a service the YAML block omits
		// as well as one the block lists with enabled: false. The block still
		// supplies that service's data_dir. Environment beats file.
		svc.Enabled = c.allowed[serviceID]
	case !listed:
		// nil means no block at all; a non-nil empty map means "services: {}",
		// which lists nothing and therefore runs nothing.
		if c.Services != nil {
			return ServiceConfig{}
		}
		svc.Enabled = true
	}
	if c.baseDir == "" && svc.DataDir != "" {
		return svc
	}
	base := c.baseDir
	if base == "" {
		base = defaultDataDir
	}
	svc.DataDir = filepath.Join(base, serviceID)
	return svc
}

// Load reads and parses a YAML config file from the given path. It returns any
// configuration warnings alongside the config so the caller can log them
// through the operator-configured handler (see Logging).
func Load(path string) (*Config, []string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, nil, err
	}
	return parse(data)
}

// LoadOrDefault tries to load a config from fallbackPath; if the file is
// missing, it returns the embedded default configuration instead. Any other
// read or parse error is returned as-is. This is the recommended entry point
// for CLI usage so the server runs with zero setup.
func LoadOrDefault(fallbackPath string) (*Config, []string, error) {
	if fallbackPath != "" {
		if _, err := os.Stat(fallbackPath); err == nil {
			return Load(fallbackPath)
		} else if !os.IsNotExist(err) {
			return nil, nil, err
		}
	}
	return parse(defaultConfigYAML)
}

func parse(data []byte) (*Config, []string, error) {
	cfg := &Config{}
	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, nil, err
	}

	var warnings []string

	// Back-compat: 'dashboard' was renamed to 'admin'. Honour the old key for
	// one release so existing configs don't silently lose the admin API. An
	// explicit 'admin' block always wins; the deprecated key is used only when
	// no 'admin' block is present (so an explicit admin.enabled: false is not
	// overridden by a leftover dashboard.enabled: true).
	if cfg.Dashboard != nil {
		warnings = append(warnings,
			"config: 'dashboard' key is deprecated and will be removed; rename it to 'admin'")
		if cfg.Admin == nil {
			cfg.Admin = cfg.Dashboard
		}
		cfg.Dashboard = nil
	}

	// The auth block is gone, and an ignored auth.enabled: true is a security
	// surprise: the operator thinks signatures are checked when nothing is.
	if cfg.Auth != nil {
		if cfg.Auth.Enabled {
			warnings = append(warnings,
				"config: 'auth.enabled: true' has no effect — SigV4 enforcement is not implemented and the 'auth' key was removed; any credentials are accepted")
		} else {
			warnings = append(warnings,
				"config: the 'auth' key was removed and is ignored; delete it")
		}
		cfg.Auth = nil
	}

	if cfg.Admin == nil {
		cfg.Admin = &AdminConfig{}
	}

	if cfg.Server.Port == 0 {
		cfg.Server.Port = 4747
	}

	return cfg, append(warnings, applyEnvOverrides(cfg)...), nil
}

var serviceTiers = map[string][]string{
	"tier1": {
		"s3", "sqs", "dynamodb", "iam", "sts", "lambda",
		"sns", "kms", "secretsmanager", "ssm", "cloudwatchlogs", "cloudwatch",
		"eventbridge", "ec2", "ecs", "ecr", "route53", "acm",
	},
	"tier2": {
		"cognito", "elasticloadbalancingv2", "ebs", "efs",
		"states", "apigateway", "apigatewayv2", "kinesis",
		"firehose", "ses", "sesv2", "rds", "cloudformation",
	},
	"tier3": {
		"elasticache", "cloudfront", "wafv2", "glue", "athena",
		"organizations", "cloudtrail", "eks", "autoscaling",
		"appsync", "emr", "batch",
	},
}

// expandTiers resolves a DEVCLOUD_SERVICES value into the set of allowed
// service names, returning nil for "all" (no filtering). Tokens that look like
// a mistyped tier shortcut are reported as warnings.
func expandTiers(value string) (map[string]bool, []string) {
	if value == "all" {
		return nil, nil
	}
	var warnings []string
	allowed := make(map[string]bool)
	for _, token := range strings.Split(value, ",") {
		token = strings.TrimSpace(token)
		if token == "" {
			continue
		}
		if tierServices, ok := serviceTiers[token]; ok {
			for _, s := range tierServices {
				allowed[s] = true
			}
			continue
		}
		// Token was not a tier shortcut — treat it as an explicit service
		// name. If it looks like it was *meant* to be a tier ("tierXXX"),
		// warn the operator so a typo surfaces in logs.
		if token != "all" && strings.HasPrefix(token, "tier") {
			warnings = append(warnings, fmt.Sprintf(
				"DEVCLOUD_SERVICES: unknown tier shortcut %q; treating as a literal service name", token))
		}
		allowed[token] = true
	}
	return allowed, warnings
}

func applyEnvOverrides(cfg *Config) []string {
	if p := os.Getenv("DEVCLOUD_PORT"); p != "" {
		if v, err := strconv.Atoi(p); err == nil {
			cfg.Server.Port = v
		}
	}

	var warnings []string
	if envServices := os.Getenv("DEVCLOUD_SERVICES"); envServices != "" {
		cfg.allowed, warnings = expandTiers(envServices)
	}
	cfg.baseDir = os.Getenv("DEVCLOUD_DATA_DIR")
	return warnings
}
