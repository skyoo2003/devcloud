// SPDX-License-Identifier: Apache-2.0

package workload

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
)

type ProcessDriver struct{}

func NewProcessDriver() *ProcessDriver {
	return &ProcessDriver{}
}

func (d *ProcessDriver) Name() string {
	return "process"
}

func (d *ProcessDriver) Available(ctx context.Context) bool {
	return true
}

func (d *ProcessDriver) RunProcess(ctx context.Context, bin string, args []string, input []byte, env map[string]string) ([]byte, error) {
	cmd := exec.CommandContext(ctx, bin, args...)
	if len(input) > 0 {
		cmd.Stdin = bytes.NewReader(input)
	}

	cmd.Env = os.Environ()
	for k, v := range env {
		cmd.Env = append(cmd.Env, fmt.Sprintf("%s=%s", k, v))
	}

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("%s %v failed: %w: %s", bin, args, err, stderr.String())
	}

	return stdout.Bytes(), nil
}
