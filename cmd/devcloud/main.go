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
	"sort"
	"syscall"
	"time"

	"github.com/skyoo2003/devcloud/internal/admin"
	"github.com/skyoo2003/devcloud/internal/config"
	"github.com/skyoo2003/devcloud/internal/eventbus"
	"github.com/skyoo2003/devcloud/internal/gateway"
	"github.com/skyoo2003/devcloud/internal/plugin"
	iamsvc "github.com/skyoo2003/devcloud/internal/services/iam"
)

var initOrder = []string{
	"s3", "sqs", "dynamodb", "iam", "sts", "lambda",
	"kms", "sns", "secretsmanager", "ssm", "cloudwatchlogs", "cloudwatch",
	"eventbridge", "ec2", "ecs", "ecr", "route53", "acm",
}

func main() {
	cfgPath := flag.String("config", "", "Path to config file (optional; uses ./devcloud.yaml if present, else embedded defaults)")
	flag.Parse()

	// Buffer log records emitted while loading config (parse() deprecation and
	// unknown-tier warnings, etc.) so they can be replayed through the
	// operator-configured handler below and honor logging.format / level.
	buf := &bufferHandler{}
	slog.SetDefault(slog.New(buf))

	var (
		cfg *config.Config
		err error
	)
	if *cfgPath != "" {
		// Explicit --config flag: the file must exist.
		cfg, err = config.Load(*cfgPath)
	} else {
		// No flag: prefer ./devcloud.yaml in the working directory; fall back to embedded defaults.
		cfg, err = config.LoadOrDefault("devcloud.yaml")
	}
	if err != nil {
		// Load failed, so there is no logging config to honor; fall back to the
		// default handler, flush any buffered warnings, then surface the error.
		setupLogging(config.LoggingConfig{})
		buf.flushTo(slog.Default().Handler())
		slog.Error("failed to load config", "error", err)
		os.Exit(1)
	}

	setupLogging(cfg.Logging)
	buf.flushTo(slog.Default().Handler())

	registry := plugin.DefaultRegistry

	// Initialize services in dependency order.
	for _, name := range initOrder {
		svcCfg, exists := cfg.Services[name]
		if !exists || !svcCfg.Enabled {
			continue
		}
		pluginCfg := plugin.PluginConfig{
			DataDir: svcCfg.DataDir,
			Options: buildOptions(name, cfg, registry),
		}
		if _, err := registry.Init(name, pluginCfg); err != nil {
			slog.Error("failed to init service", "service", name, "error", err)
			os.Exit(1)
		}
		slog.Info("service initialized", "service", name)
	}

	// Initialize any remaining enabled services not in initOrder, in
	// deterministic alphabetical order so startup logs and init sequencing
	// are reproducible across runs.
	remaining := make([]string, 0, len(cfg.Services))
	for name := range cfg.Services {
		remaining = append(remaining, name)
	}
	sort.Strings(remaining)
	for _, name := range remaining {
		svcCfg := cfg.Services[name]
		if !svcCfg.Enabled {
			continue
		}
		if _, ok := registry.Get(name); ok {
			continue
		}
		pluginCfg := plugin.PluginConfig{
			DataDir: svcCfg.DataDir,
			Options: buildOptions(name, cfg, registry),
		}
		if _, err := registry.Init(name, pluginCfg); err != nil {
			slog.Warn("service init failed", "service", name, "error", err)
			continue
		}
		slog.Info("service initialized", "service", name)
	}

	if cfg.Auth.Enabled {
		slog.Warn("auth.enabled=true but SigV4 enforcement is not yet implemented; requests are accepted regardless of signature validity")
	}

	// Admin API: build the REST + WebSocket handler only when the operator
	// opted in via admin.enabled. Otherwise expose a 404 handler so the admin
	// routes don't leak service internals. This binary serves no web UI; the
	// dashboard frontend lives in a separate repository and talks to this API.
	logCollector := admin.NewLogCollector(1000)
	adminHandler := http.NotFoundHandler()
	if cfg.Admin.Enabled {
		adminAPI := admin.NewAPI(registry, logCollector)
		hub := admin.NewHub(eventbus.New())
		go hub.Start()

		adminMux := http.NewServeMux()
		adminMux.Handle("/devcloud/api/", adminAPI.Handler())
		adminMux.HandleFunc("/devcloud/api/ws", hub.ServeWS)
		adminHandler = adminMux
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
