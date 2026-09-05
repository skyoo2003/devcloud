// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/skyoo2003/devcloud/internal/admin"
	"github.com/skyoo2003/devcloud/internal/config"
	"github.com/skyoo2003/devcloud/internal/gateway"
	"github.com/skyoo2003/devcloud/internal/plugin"
	iamsvc "github.com/skyoo2003/devcloud/internal/services/iam"
)

// initOrder lists the services that must come up for DevCloud to be useful, in
// dependency order — sts borrows iam's store, so iam precedes it. A failure
// here is fatal; every other registered service only warns.
var initOrder = []string{
	"s3", "sqs", "dynamodb", "iam", "sts", "lambda",
	"kms", "sns", "secretsmanager", "ssm", "cloudwatchlogs", "cloudwatch",
	"eventbridge", "ec2", "ecs", "ecr", "route53", "acm",
}

func main() {
	cfgPath := flag.String("config", "", "Path to config file (optional; uses ./devcloud.yaml if present, else embedded defaults)")
	flag.Parse()

	var (
		cfg      *config.Config
		warnings []string
		err      error
	)
	if *cfgPath != "" {
		// Explicit --config flag: the file must exist.
		cfg, warnings, err = config.Load(*cfgPath)
	} else {
		// No flag: prefer ./devcloud.yaml in the working directory; fall back to embedded defaults.
		cfg, warnings, err = config.LoadOrDefault("devcloud.yaml")
	}
	if err != nil {
		// Load failed, so there is no logging config to honor; fall back to the
		// default handler and surface the error.
		setupLogging(config.LoggingConfig{})
		slog.Error("failed to load config", "error", err)
		os.Exit(1)
	}

	// Config warnings are returned rather than logged by the config package so
	// they honor logging.format / logging.level.
	setupLogging(cfg.Logging)
	for _, w := range warnings {
		slog.Warn(w)
	}

	registry := plugin.DefaultRegistry

	// A service's provider comes from the plugin itself, so config resolution is
	// provider-aware without the config package needing to know which services
	// exist. Construct builds an instance without Init, which is all the
	// metadata methods need.
	providerOf := func(name string) string {
		p, ok := registry.Construct(name)
		if !ok {
			return plugin.DefaultProvider
		}
		return plugin.ProviderOf(p)
	}

	initService := func(name string, fatal bool) {
		svcCfg := cfg.ProviderService(providerOf(name), name)
		if !svcCfg.Enabled {
			return
		}
		pluginCfg := plugin.PluginConfig{
			DataDir: svcCfg.DataDir,
			Options: buildOptions(name, cfg, registry),
		}
		if _, err := registry.Init(name, pluginCfg); err != nil {
			if fatal {
				slog.Error("failed to init service", "service", name, "error", err)
				os.Exit(1)
			}
			slog.Warn("service init failed", "service", name, "error", err)
			return
		}
		slog.Info("service initialized", "service", name)
	}

	for _, name := range initOrder {
		initService(name, true)
	}
	// RegisteredServices() is sorted, so the long tail starts in a reproducible
	// order. Services already brought up above are skipped.
	for _, name := range registry.RegisteredServices() {
		if _, ok := registry.Get(name); ok {
			continue
		}
		initService(name, false)
	}

	// A services block is authoritative and `enabled` defaults to Go's false,
	// so `services:\n  s3:\n` (or a typo'd DEVCLOUD_SERVICES) silently brings up
	// nothing. Serving zero services is never what an operator wanted.
	if len(registry.ActiveServices()) == 0 {
		slog.Warn("no services enabled; a 'services' block only starts what it lists, and each entry still needs 'enabled: true' — also check DEVCLOUD_SERVICES")
	}

	// Admin API: build the REST handler only when the operator opted in via
	// admin.enabled. Otherwise expose a 404 handler so the admin routes don't
	// leak service internals. This binary serves no web UI; the dashboard
	// frontend lives in a separate repository and talks to this API.
	// The log collector is only built when admin is enabled; otherwise no
	// consumer can ever read it, so a nil collector keeps Add off the request
	// hot path (see gateway.New).
	var logCollector *admin.LogCollector
	adminHandler := http.NotFoundHandler()
	if cfg.Admin.Enabled {
		logCollector = admin.NewLogCollector(1000)
		adminHandler = admin.NewAPI(registry, logCollector).Handler()
		slog.Info("admin API enabled")
	}
	gw := gateway.New(cfg.Server.Port, registry, adminHandler, logCollector)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		slog.Info("shutting down (timeout 15s, send again to force)...")
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()

		go func() {
			<-sigCh
			slog.Error("forced shutdown")
			os.Exit(1)
		}()

		if err := registry.ShutdownAll(ctx); err != nil {
			slog.Error("service shutdown error", "error", err)
		}
		if err := gw.Shutdown(ctx); err != nil {
			slog.Error("gateway shutdown error", "error", err)
		}
	}()

	slog.Info("DevCloud ready", "port", cfg.Server.Port, "services", registry.ActiveServices())
	if err := gw.Start(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		slog.Error("server error", "error", err)
		os.Exit(1)
	}
}

func buildOptions(serviceID string, cfg *config.Config, registry *plugin.Registry) map[string]any {
	// server_port is passed to every service so URL-building providers
	// (SQS, ECR, CloudFormation, S3, Lambda, etc.) can construct
	// endpoint-accurate response URLs without hardcoding 4747. Services
	// that don't need it simply ignore the key.
	opts := map[string]any{
		"server_port": cfg.Server.Port,
	}
	if serviceID == "sts" {
		if iamPlugin, ok := registry.Get("iam"); ok {
			if iamProv, ok := iamPlugin.(*iamsvc.IAMProvider); ok {
				opts["iam_store"] = iamProv.Store().SQLiteStore()
			} else {
				slog.Warn("iam plugin cast failed; sts will start without shared iam_store",
					"iam_type", "unexpected")
			}
		}
	}
	return opts
}

func setupLogging(cfg config.LoggingConfig) {
	level := slog.LevelInfo
	switch cfg.Level {
	case "debug":
		level = slog.LevelDebug
	case "warn":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	}

	var handler slog.Handler
	if cfg.Format == "json" {
		handler = slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level})
	} else {
		handler = slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: level})
	}
	slog.SetDefault(slog.New(handler))
}
