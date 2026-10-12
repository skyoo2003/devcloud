// SPDX-License-Identifier: Apache-2.0

package workload

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
)

type WASMDriver struct {
	cli string
}

func NewWASMDriver(explicitCLI string) *WASMDriver {
	return &WASMDriver{cli: explicitCLI}
}

func (d *WASMDriver) resolveCLI() string {
	if d.cli != "" {
		return d.cli
	}
	if envCLI := os.Getenv("WASM_RUNTIME"); envCLI != "" {
		return envCLI
	}
	for _, candidate := range []string{"wasmtime", "wasmer", "wasmedge", "wazero"} {
		if path, err := exec.LookPath(candidate); err == nil && path != "" {
			return candidate
		}
	}
	return ""
}

func (d *WASMDriver) Name() string {
	cli := d.resolveCLI()
	if cli != "" {
		return cli
	}
	return "wasm-embedded"
}

func (d *WASMDriver) Available(ctx context.Context) bool {
	return d.resolveCLI() != ""
}

func (d *WASMDriver) RunWASM(ctx context.Context, module []byte, input []byte) ([]byte, error) {
	cli := d.resolveCLI()
	if cli == "" {
		return nil, fmt.Errorf("%w: install wasmtime, wasmer, or set WASM_RUNTIME", ErrNoRuntimeAvailable)
	}

	tmpFile, err := os.CreateTemp("", "workload-*.wasm")
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = os.Remove(tmpFile.Name())
	}()

	if _, err := tmpFile.Write(module); err != nil {
		_ = tmpFile.Close()
		return nil, err
	}
	_ = tmpFile.Close()

	cmd := exec.CommandContext(ctx, cli, "run", tmpFile.Name())
	if len(input) > 0 {
		cmd.Stdin = bytes.NewReader(input)
	}

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("%s wasm run failed: %w: %s", cli, err, stderr.String())
	}

	return stdout.Bytes(), nil
}
