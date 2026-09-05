// SPDX-License-Identifier: Apache-2.0

package config

import (
	_ "embed"
	"fmt"
	"os"
	"path/filepath"
	"sort"
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

// DefaultProvider is the CSP a service belongs to when nothing says otherwise.
// It mirrors plugin.DefaultProvider; config does not import plugin, so the
// constant is repeated rather than shared.
const DefaultProvider = "aws"

// knownProviders are the providers this build can actually serve. A block for
// anything else still parses — a config written for a later DevCloud loads here
// without erroring — but it is reported, because nothing in this binary will
// read it. Adding a provider means adding its services and this entry in the
// same change.
var knownProviders = map[string]bool{DefaultProvider: true}

type Config struct {
	Server   ServerConfig             `yaml:"server"`
	Services map[string]ServiceConfig `yaml:"services"`
	Admin    *AdminConfig             `yaml:"admin"`
	Logging  LoggingConfig            `yaml:"logging"`

	// Providers namespaces service configuration by CSP:
	//
	//   providers:
	//     aws:
	//       services:
	//         s3: {enabled: true}
	//
	// The top-level Services block is the AWS provider's block under its
	// historical name and keeps working unchanged; providers.aws.services is
	// the same block spelled forward-compatibly, and wins when both are set
	// (see parse). Blocks for other providers are carried through so a config
	// can be written before the services it configures exist.
	Providers map[string]ProviderConfig `yaml:"providers"`

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

// ProviderConfig is one CSP's slice of the config. It holds only a services
// block today; per-provider settings (endpoints, credentials, regions) belong
// here when they arrive.
type ProviderConfig struct {
	Services map[string]ServiceConfig `yaml:"services"`
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

// Service returns the effective configuration for an AWS service. It is
// ProviderService for DefaultProvider — the spelling every caller wanted before
// there was more than one provider.
func (c *Config) Service(serviceID string) ServiceConfig {
	return c.ProviderService(DefaultProvider, serviceID)
}

// ProviderService returns the effective configuration for serviceID under
// provider.
//
// A YAML services block is authoritative: only the services it lists can run.
// An empty block therefore runs nothing — it is a block, so it decides. Absent
// entirely (the embedded default), every service is enabled with data_dir
// <base>/<id>. DEVCLOUD_SERVICES and DEVCLOUD_DATA_DIR are applied here so both
// paths agree.
//
// Data directories stay flat for AWS (<base>/<id>) because that layout is a 1.x
// guarantee. Any other provider nests under its own name (<base>/<provider>/<id>)
// so two CSPs offering a same-named service cannot land on the same directory.
func (c *Config) ProviderService(provider, serviceID string) ServiceConfig {
	block, hasBlock := c.serviceBlock(provider)
	svc, listed := block[serviceID]
	switch {
	case provider == DefaultProvider && c.allowed != nil:
		// DEVCLOUD_SERVICES names the running set outright, so it decides
		// membership on its own: it can enable a service the YAML block omits
		// as well as one the block lists with enabled: false. The block still
		// supplies that service's data_dir. Environment beats file. It is
		// AWS-scoped: there is no syntax for naming another provider's
		// services, so it does not silently disable them.
		svc.Enabled = c.allowed[serviceID]
	case !listed:
		// No block at all means every service runs; a block that exists but
		// does not list this service means it does not.
		if hasBlock {
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
	if provider != DefaultProvider {
		base = filepath.Join(base, provider)
	}
	svc.DataDir = filepath.Join(base, serviceID)
	return svc
}

// serviceBlock returns the services block that governs provider, and whether
// one was written at all. The distinction matters: a block that exists is
// authoritative even when empty, while no block means "run everything".
func (c *Config) serviceBlock(provider string) (map[string]ServiceConfig, bool) {
	if pc, ok := c.Providers[provider]; ok && pc.Services != nil {
		return pc.Services, true
	}
	if provider == DefaultProvider && c.Services != nil {
		return c.Services, true
	}
	return nil, false
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

	warnings = append(warnings, checkProviders(cfg)...)

	if cfg.Admin == nil {
		cfg.Admin = &AdminConfig{}
	}

	if cfg.Server.Port == 0 {
		cfg.Server.Port = 4747
	}

	return cfg, append(warnings, applyEnvOverrides(cfg)...), nil
}

// checkProviders reports the two ways a providers block can be written and do
// nothing the operator expected: naming a provider this build cannot serve, and
// duplicating the AWS services block that already exists at the top level.
// Neither is an error — both parse and resolve deterministically — but a
// silently ignored services block is exactly the surprise this config package
// warns about elsewhere.
func checkProviders(cfg *Config) []string {
	var warnings []string
	for _, name := range sortedProviders(cfg.Providers) {
		if !knownProviders[name] {
			warnings = append(warnings, fmt.Sprintf(
				"config: providers.%s is not served by this build and is ignored; check the spelling, or see docs/roadmap.md for which providers exist", name))
			continue
		}
		if name == DefaultProvider && cfg.Providers[name].Services != nil && cfg.Services != nil {
			warnings = append(warnings, fmt.Sprintf(
				"config: both 'services' and 'providers.%s.services' are set; the top-level 'services' block is ignored", name))
		}
	}
	return warnings
}

// sortedProviders keeps warning order stable across runs — map iteration order
// would otherwise reshuffle the log on every start.
func sortedProviders(providers map[string]ProviderConfig) []string {
	names := make([]string, 0, len(providers))
	for name := range providers {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
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
