// SPDX-License-Identifier: Apache-2.0
package lambda

import (
	"archive/zip"
	"bytes"
	"github.com/stretchr/testify/require"
	"os"
	"path/filepath"
	"testing"
)

func TestArchiveRejectsTraversal(t *testing.T) {
	for _, name := range []string{"../escape", "nested/../../escape", "/absolute", "a\\b", "__devcloud_handler__.py", "nested/../__devcloud_handler__.py", "C:/absolute", ".", "nested/.."} {
		var b bytes.Buffer
		w := zip.NewWriter(&b)
		f, err := w.Create(name)
		require.NoError(t, err)
		_, err = f.Write([]byte("bad"))
		require.NoError(t, err)
		require.NoError(t, w.Close())
		path := filepath.Join(t.TempDir(), "code.zip")
		require.NoError(t, os.WriteFile(path, b.Bytes(), 0600))
		require.ErrorIs(t, extractFunctionArchive(path, t.TempDir()), ErrInvalidFunctionCode)
	}
}
func TestArchiveRejectsSymlink(t *testing.T) {
	var b bytes.Buffer
	w := zip.NewWriter(&b)
	h := &zip.FileHeader{Name: "link"}
	h.SetMode(os.ModeSymlink | 0777)
	f, err := w.CreateHeader(h)
	require.NoError(t, err)
	_, err = f.Write([]byte("/etc/passwd"))
	require.NoError(t, err)
	require.NoError(t, w.Close())
	path := filepath.Join(t.TempDir(), "code.zip")
	require.NoError(t, os.WriteFile(path, b.Bytes(), 0600))
	require.ErrorIs(t, extractFunctionArchive(path, t.TempDir()), ErrInvalidFunctionCode)
}
func TestArchiveLimits(t *testing.T) {
	var b bytes.Buffer
	w := zip.NewWriter(&b)
	for i := 0; i < 10001; i++ {
		_, err := w.Create("empty")
		require.NoError(t, err)
	}
	require.NoError(t, w.Close())
	path := filepath.Join(t.TempDir(), "code.zip")
	require.NoError(t, os.WriteFile(path, b.Bytes(), 0600))
	require.ErrorIs(t, extractFunctionArchive(path, t.TempDir()), ErrInvalidFunctionCode)
}

func TestArchiveKeepsExecutableHelper(t *testing.T) {
	var buffer bytes.Buffer
	writer := zip.NewWriter(&buffer)
	header := &zip.FileHeader{Name: "bin/helper"}
	header.SetMode(0755)
	entry, err := writer.CreateHeader(header)
	require.NoError(t, err)
	_, err = entry.Write([]byte("#!/bin/sh\necho hello\n"))
	require.NoError(t, err)
	require.NoError(t, writer.Close())
	archive := filepath.Join(t.TempDir(), "code.zip")
	require.NoError(t, os.WriteFile(archive, buffer.Bytes(), 0600))
	destination := t.TempDir()
	require.NoError(t, extractFunctionArchive(archive, destination))
	extracted, err := os.Stat(filepath.Join(destination, "bin/helper"))
	require.NoError(t, err)
	require.NotZero(t, extracted.Mode().Perm()&0111, "Python/Node subprocess helpers must remain executable")
}

func TestArchiveKeepsSafeNormalizedPaths(t *testing.T) {
	for _, name := range []string{"nested/index.py", "nested/../index.py", "module..py"} {
		t.Run(name, func(t *testing.T) {
			var buffer bytes.Buffer
			writer := zip.NewWriter(&buffer)
			entry, err := writer.Create(name)
			require.NoError(t, err)
			_, err = entry.Write([]byte("handler contents"))
			require.NoError(t, err)
			require.NoError(t, writer.Close())
			archive := filepath.Join(t.TempDir(), "code.zip")
			require.NoError(t, os.WriteFile(archive, buffer.Bytes(), 0600))
			destination := t.TempDir()
			require.NoError(t, extractFunctionArchive(archive, destination))
			contents, err := os.ReadFile(filepath.Join(destination, filepath.Clean(name)))
			require.NoError(t, err)
			require.Equal(t, "handler contents", string(contents))
		})
	}
}
