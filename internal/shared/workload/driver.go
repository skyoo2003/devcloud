// SPDX-License-Identifier: Apache-2.0

package workload

import (
	"context"
	"errors"
	"sync"
)

var (
	ErrNoRuntimeAvailable = errors.New("no container or workload runtime available")
	ErrUnsupportedDriver  = errors.New("unsupported workload driver")
)

type DriverType string

const (
	DriverTypeOCI     DriverType = "oci"
	DriverTypeWASM    DriverType = "wasm"
	DriverTypeProcess DriverType = "process"
)

// ContainerRunner abstracts running commands against a container daemon (Docker, Podman, nerdctl).
type ContainerRunner interface {
	Name() string
	Available(ctx context.Context) bool
	Run(ctx context.Context, input []byte, args ...string) ([]byte, error)
}

// WASMRunner abstracts WebAssembly workload execution.
type WASMRunner interface {
	Name() string
	Available(ctx context.Context) bool
	RunWASM(ctx context.Context, module []byte, input []byte) ([]byte, error)
}

// ProcessRunner abstracts direct host process sandbox execution.
type ProcessRunner interface {
	Name() string
	Available(ctx context.Context) bool
	RunProcess(ctx context.Context, bin string, args []string, input []byte, env map[string]string) ([]byte, error)
}

type Manager struct {
	mu              sync.RWMutex
	containerRunner ContainerRunner
	wasmRunner      WASMRunner
	processRunner   ProcessRunner
}

var (
	defaultManager     *Manager
	defaultManagerOnce sync.Once
)

// DefaultManager returns the global workload manager.
func DefaultManager() *Manager {
	defaultManagerOnce.Do(func() {
		defaultManager = &Manager{
			containerRunner: NewOCIDriver(""),
			wasmRunner:      NewWASMDriver(""),
			processRunner:   NewProcessDriver(),
		}
	})
	return defaultManager
}

func (m *Manager) SetContainerRunner(r ContainerRunner) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.containerRunner = r
}

func (m *Manager) ContainerRunner() ContainerRunner {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.containerRunner
}

func (m *Manager) SetWASMRunner(r WASMRunner) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.wasmRunner = r
}

func (m *Manager) WASMRunner() WASMRunner {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.wasmRunner
}

func (m *Manager) SetProcessRunner(r ProcessRunner) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.processRunner = r
}

func (m *Manager) ProcessRunner() ProcessRunner {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.processRunner
}

// DefaultContainerRunner returns a function compatible with lambda's docker signature.
func DefaultContainerRunner() func(context.Context, []byte, ...string) ([]byte, error) {
	return func(ctx context.Context, input []byte, args ...string) ([]byte, error) {
		runner := DefaultManager().ContainerRunner()
		return runner.Run(ctx, input, args...)
	}
}
