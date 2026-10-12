// SPDX-License-Identifier: Apache-2.0

package workload

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestOCIDriver_Resolution(t *testing.T) {
	d := NewOCIDriver("")
	t.Setenv("CONTAINER_RUNTIME", "podman")
	if d.Name() != "podman" {
		t.Fatalf("expected podman, got %s", d.Name())
	}

	dExplicit := NewOCIDriver("my-custom-oci")
	if dExplicit.Name() != "my-custom-oci" {
		t.Fatalf("expected my-custom-oci, got %s", dExplicit.Name())
	}
}

func TestOCIDriver_RunMock(t *testing.T) {
	dir := t.TempDir()
	mockBin := filepath.Join(dir, "mock-container-cli")
	script := "#!/bin/sh\nprintf 'standard-output'; printf 'standard-error' >&2\n"
	if err := os.WriteFile(mockBin, []byte(script), 0755); err != nil {
		t.Fatalf("failed to write mock script: %v", err)
	}

	driver := NewOCIDriver(mockBin)
	out, err := driver.Run(context.Background(), nil, "logs", "test-id")
	if err != nil {
		t.Fatalf("Run failed: %v", err)
	}
	if string(out) != "standard-outputstandard-error" {
		t.Fatalf("expected combined logs, got: %s", string(out))
	}
}

func TestWASMDriver_Resolution(t *testing.T) {
	d := NewWASMDriver("")
	t.Setenv("WASM_RUNTIME", "wasmtime")
	if d.Name() != "wasmtime" {
		t.Fatalf("expected wasmtime, got %s", d.Name())
	}
}

func TestProcessDriver_Execution(t *testing.T) {
	d := NewProcessDriver()
	if !d.Available(context.Background()) {
		t.Fatalf("process driver should be available")
	}

	out, err := d.RunProcess(context.Background(), "echo", []string{"hello-workload"}, nil, map[string]string{"FOO": "BAR"})
	if err != nil {
		t.Fatalf("RunProcess failed: %v", err)
	}
	if string(out) != "hello-workload\n" {
		t.Fatalf("unexpected output: %s", string(out))
	}
}

func TestManager_Registry(t *testing.T) {
	mgr := DefaultManager()
	origRunner := mgr.ContainerRunner()
	defer mgr.SetContainerRunner(origRunner)

	mockRunner := NewOCIDriver("test-oci")
	mgr.SetContainerRunner(mockRunner)

	if mgr.ContainerRunner().Name() != "test-oci" {
		t.Fatalf("expected test-oci, got %s", mgr.ContainerRunner().Name())
	}

	runnerFn := DefaultContainerRunner()
	if runnerFn == nil {
		t.Fatalf("DefaultContainerRunner returned nil")
	}
}
