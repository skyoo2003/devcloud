// SPDX-License-Identifier: Apache-2.0

package workload

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
)

type OCIDriver struct {
	cli string
}

func NewOCIDriver(explicitCLI string) *OCIDriver {
	return &OCIDriver{cli: explicitCLI}
}

func (d *OCIDriver) resolveCLI() string {
	if d.cli != "" {
		return d.cli
	}
	if envCLI := os.Getenv("CONTAINER_RUNTIME"); envCLI != "" {
		return envCLI
	}
	// Docker first: it is what Lambda has always used, and a host with both
	// installed keeps its images and DEVCLOUD_LAMBDA_NETWORK in Docker.
	for _, candidate := range []string{"docker", "podman", "nerdctl"} {
		if path, err := exec.LookPath(candidate); err == nil && path != "" {
			return candidate
		}
	}
	// Default fallback to "docker"
	return "docker"
}

func (d *OCIDriver) Name() string {
	return d.resolveCLI()
}

func (d *OCIDriver) Available(ctx context.Context) bool {
	cli := d.resolveCLI()
	cmd := exec.CommandContext(ctx, cli, "info")
	return cmd.Run() == nil
}

func (d *OCIDriver) Run(ctx context.Context, input []byte, args ...string) ([]byte, error) {
	cli := d.resolveCLI()
	cmd := exec.CommandContext(ctx, cli, args...)
	if len(input) > 0 {
		cmd.Stdin = bytes.NewReader(input)
	}

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		// Only the subcommand: the full argv carries `--env K=V` for every
		// function variable, and this error reaches the Lambda API response.
		sub := ""
		if len(args) > 0 {
			sub = args[0]
		}
		return nil, fmt.Errorf("%s %s failed: %w: %s", cli, sub, err, stderr.String())
	}

	if len(args) > 0 && args[0] == "logs" {
		// Container runtimes forward container stderr separately from stdout.
		stdout.Write(stderr.Bytes())
	}

	return stdout.Bytes(), nil
}
