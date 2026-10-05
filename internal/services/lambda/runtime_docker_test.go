// SPDX-License-Identifier: Apache-2.0
package lambda

import (
	"context"
	"github.com/stretchr/testify/require"
	"os"
	"path/filepath"
	"testing"
)

func TestContainerLogTailIncludesStderr(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "docker"), []byte("#!/bin/sh\nprintf 'stdout'; printf 'stderr' >&2\n"), 0755))
	t.Setenv("PATH", dir)
	logs, err := runDocker(context.Background(), nil, "logs", "container")
	require.NoError(t, err)
	require.Equal(t, "stdoutstderr", string(logs))
}
