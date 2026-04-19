// SPDX-License-Identifier: Apache-2.0

package config

import (
	_ "embed"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"
)

// defaultConfigYAML is the built-in configuration used when no YAML file is
// provided. It enables all services with standard data directories so DevCloud
// runs out of the box with zero setup.
//
//go:embed default.yaml
var defaultConfigYAML []byte

type Config struct {
	Server    ServerConfig             `yaml:"server"`
	Services  map[string]ServiceConfig `yaml:"services"`
	Auth      AuthConfig               `yaml:"auth"`
	Dashboard DashboardConfig          `yaml:"dashboard"`
	Logging   LoggingConfig            `yaml:"logging"`
}

type ServerConfig struct {
	Port int `yaml:"port"`
}

type ServiceConfig struct {
	Enabled         bool   `yaml:"enabled"`
	DataDir         string `yaml:"data_dir"`
	Runtime         string `yaml:"runtime"`
	WarmContainers  int    `yaml:"warm_containers"`
	EnforcePolicies bool   `yaml:"enforce_policies"`
}

type AuthConfig struct {
	Enabled bool `yaml:"enabled"`
}

type DashboardConfig struct {
	Enabled bool `yaml:"enabled"`
}

type LoggingConfig struct {
	Level  string `yaml:"level"`
	Format string `yaml:"format"`
}

// Load reads and parses a YAML config file from the given path.
// Returns an error if the file cannot be read or parsed.
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return parse(data)
}

// LoadOrDefault tries to load a config from fallbackPath; if the file is
// missing, it returns the embedded default configuration instead. Any other
// read or parse error is returned as-is. This is the recommended entry point
// for CLI usage so the server runs with zero setup.
func LoadOrDefault(fallbackPath string) (*Config, error) {
	if fallbackPath != "" {
		if _, err := os.Stat(fallbackPath); err == nil {
			return Load(fallbackPath)
		} else if !os.IsNotExist(err) {
			return nil, err
		}
		slog.Info("config file not found, using embedded defaults", "tried", fallbackPath)
	}
	return parse(defaultConfigYAML)
}

func parse(data []byte) (*Config, error) {
	cfg := &Config{}
	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, err
	}

	if cfg.Server.Port == 0 {
		cfg.Server.Port = 4747
	}

	applyEnvOverrides(cfg)
	return cfg, nil
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

// knownTierTokens returns the set of recognized tier shortcut names.
func knownTierTokens() map[string]struct{} {
	out := make(map[string]struct{}, len(serviceTiers)+1)
	for k := range serviceTiers {
		out[k] = struct{}{}
	}
	out["all"] = struct{}{}
	return out
}

func expandTiers(value string) map[string]bool {
	if value == "all" {
		return nil
	}
	tiers := knownTierTokens()
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
		if _, isTier := tiers[token]; !isTier && strings.HasPrefix(token, "tier") {
			slog.Warn("DEVCLOUD_SERVICES: unknown tier shortcut; treating as literal service name",
				"token", token)
		}
		allowed[token] = true
	}
	return allowed
}

func applyEnvOverrides(cfg *Config) {
	if p := os.Getenv("DEVCLOUD_PORT"); p != "" {
		if v, err := strconv.Atoi(p); err == nil {
			cfg.Server.Port = v
		}
	}

	if envServices := os.Getenv("DEVCLOUD_SERVICES"); envServices != "" {
		allowed := expandTiers(envServices)
		if allowed == nil {
			return
		}
		for name, svc := range cfg.Services {
			if !allowed[name] {
				svc.Enabled = false
				cfg.Services[name] = svc
			}
		}
	}

	// DEVCLOUD_DATA_DIR overrides the base data directory for all services.
	// Each service's data_dir is rewritten to <base>/<service_name> using
	// the host's path separator.
	if baseDir := os.Getenv("DEVCLOUD_DATA_DIR"); baseDir != "" {
		for name, svc := range cfg.Services {
			svc.DataDir = filepath.Join(baseDir, name)
			cfg.Services[name] = svc
		}
	}
}
