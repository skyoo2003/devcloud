// SPDX-License-Identifier: Apache-2.0

package main

import (
	"go/parser"
	"go/token"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// repoRoot resolves the repository root from this package's directory so the
// tests can reach scripts/ and internal/services/ without a hardcoded path.
func repoRoot(t *testing.T) string {
	t.Helper()
	root, err := filepath.Abs("../..")
	require.NoError(t, err)
	return root
}

// registeringServiceDirs returns the internal/services package names that
// register a service. Registration lives in an init() inside provider.go rather
// than a dedicated file, so scanning the directory's sources is the only
// reliable signal for which packages must be imported.
func registeringServiceDirs(t *testing.T) []string {
	t.Helper()
	servicesDir := filepath.Join(repoRoot(t), "internal", "services")
	entries, err := os.ReadDir(servicesDir)
	require.NoError(t, err)

	var pkgs []string
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		files, err := os.ReadDir(filepath.Join(servicesDir, entry.Name()))
		require.NoError(t, err)
		for _, f := range files {
			name := f.Name()
			if f.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
				continue
			}
			src, err := os.ReadFile(filepath.Join(servicesDir, entry.Name(), name))
			require.NoError(t, err)
			if strings.Contains(string(src), "DefaultRegistry.Register(") {
				pkgs = append(pkgs, entry.Name())
				break
			}
		}
	}
	sort.Strings(pkgs)
	return pkgs
}

// importedServicePackages reads the blank service imports out of imports.go.
func importedServicePackages(t *testing.T) []string {
	t.Helper()
	const prefix = "github.com/skyoo2003/devcloud/internal/services/"

	path := filepath.Join(repoRoot(t), "cmd", "devcloud", "imports.go")
	file, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.ImportsOnly)
	require.NoError(t, err)

	var pkgs []string
	for _, spec := range file.Imports {
		imported, err := strconv.Unquote(spec.Path.Value)
		require.NoError(t, err)
		if after, found := strings.CutPrefix(imported, prefix); found {
			pkgs = append(pkgs, after)
		}
	}
	sort.Strings(pkgs)
	return pkgs
}

// TestGenerateImportsReproducesCommittedFile is the guard on the generator
// itself: regenerating imports.go must produce exactly what is committed. The
// weekly Smithy sync runs this generator before its test step, so a generator
// that silently writes a shorter file takes every service out of the binary.
//
// It runs against a scratch tree rather than the repository, so a broken
// generator cannot overwrite the committed file while proving that it is broken.
func TestGenerateImportsReproducesCommittedFile(t *testing.T) {
	root := repoRoot(t)

	work := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(work, "cmd", "devcloud"), 0o755))
	require.NoError(t, os.MkdirAll(filepath.Join(work, "internal"), 0o755))
	require.NoError(t, os.Symlink(
		filepath.Join(root, "internal", "services"),
		filepath.Join(work, "internal", "services"),
	))

	cmd := exec.Command("bash", filepath.Join(root, "scripts", "generate-imports.sh"))
	cmd.Dir = work
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, "generator exited non-zero:\n%s", out)

	got, err := os.ReadFile(filepath.Join(work, "cmd", "devcloud", "imports.go"))
	require.NoError(t, err)
	want, err := os.ReadFile(filepath.Join(root, "cmd", "devcloud", "imports.go"))
	require.NoError(t, err)

	assert.Equal(t, string(want), string(got),
		"regenerating imports.go must reproduce the committed file byte for byte")
}

// TestImportsCoverEveryRegisteredService locks imports.go against drift in
// either direction. A service package that is not imported never runs its
// init(), so it is registered nowhere and unreachable at runtime — silently, and
// only in the built binary.
func TestImportsCoverEveryRegisteredService(t *testing.T) {
	registered := registeringServiceDirs(t)
	imported := importedServicePackages(t)

	// Conservative floor, mirroring conformance_test.go: a collapse in the
	// service surface should fail here loudly rather than pass an empty
	// comparison against an equally empty imports.go.
	const minServices = 50
	require.GreaterOrEqual(t, len(registered), minServices,
		"found %d registering service packages, want >= %d", len(registered), minServices)

	assert.Equal(t, registered, imported,
		"every service package that registers must be blank-imported in imports.go, and nothing else")
}
