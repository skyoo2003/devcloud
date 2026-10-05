// SPDX-License-Identifier: Apache-2.0
package lambda

import (
	"archive/zip"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

func extractFunctionArchive(zipPath, destination string) error {
	z, err := zip.OpenReader(zipPath)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidFunctionCode, err)
	}
	defer func() { _ = z.Close() }()
	if len(z.File) > 10000 {
		return fmt.Errorf("%w: too many ZIP entries", ErrInvalidFunctionCode)
	}
	remaining := int64(250 * 1024 * 1024)
	for _, f := range z.File {
		name := f.Name
		if !filepath.IsLocal(name) {
			return fmt.Errorf("%w: unsafe ZIP entry %s", ErrInvalidFunctionCode, name)
		}
		clean := filepath.Clean(name)
		if strings.ContainsAny(name, "\\:") || clean == "." || strings.HasPrefix(clean, "../") || strings.HasPrefix(clean, "__devcloud_") || (!f.Mode().IsRegular() && !f.Mode().IsDir()) {
			return fmt.Errorf("%w: unsafe ZIP entry %s", ErrInvalidFunctionCode, name)
		}
		path := filepath.Join(destination, clean)
		if f.FileInfo().IsDir() {
			if err := os.MkdirAll(path, 0755); err != nil {
				return err
			}
			continue
		}
		if f.UncompressedSize64 > uint64(remaining) {
			return fmt.Errorf("%w: ZIP exceeds 250 MiB", ErrInvalidFunctionCode)
		}
		if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
			return err
		}
		src, err := f.Open()
		if err != nil {
			return fmt.Errorf("%w: %v", ErrInvalidFunctionCode, err)
		}
		dst, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0644|f.Mode().Perm()&0111)
		if err != nil {
			_ = src.Close()
			return fmt.Errorf("%w: %v", ErrInvalidFunctionCode, err)
		}
		n, copyErr := io.Copy(dst, io.LimitReader(src, remaining+1))
		_ = src.Close()
		closeErr := dst.Close()
		remaining -= n
		if copyErr != nil || remaining < 0 {
			return fmt.Errorf("%w: invalid or oversized ZIP entry", ErrInvalidFunctionCode)
		}
		if closeErr != nil {
			return closeErr
		}
	}
	return nil
}
