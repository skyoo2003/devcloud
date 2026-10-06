// SPDX-License-Identifier: Apache-2.0
package s3

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

type renameFileMove struct {
	SourcePath, DestinationPath, BackupPath string
	Moved, BackedUp                         bool
}

// Check every existing component without following a user-created symlink.
func (fs *FileStore) checkRenamePath(path string) (os.FileInfo, error) {
	rel, err := filepath.Rel(fs.baseDir, path)
	if err != nil {
		return nil, err
	}
	if rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) || filepath.IsAbs(rel) {
		return nil, s3Error("InvalidArgument", 400, "path escapes file store")
	}
	parts := strings.Split(rel, string(filepath.Separator))
	current := fs.baseDir
	for i, part := range parts {
		current = filepath.Join(current, part)
		info, err := os.Lstat(current)
		if err != nil {
			return nil, err
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return nil, s3Error("InvalidArgument", 400, "rename cannot follow symlinks")
		}
		if i < len(parts)-1 && !info.IsDir() {
			return nil, fmt.Errorf("rename parent is not a directory")
		}
		if i == len(parts)-1 {
			return info, nil
		}
	}
	return nil, s3Error("InvalidArgument", 400, "invalid rename path")
}

func (fs *FileStore) prepareRename(accountID, bucket, sourceKey, destinationKey string) (*renameFileMove, error) {
	source, err := fs.objectPath(accountID, bucket, sourceKey)
	if err != nil {
		return nil, err
	}
	destination, err := fs.objectPath(accountID, bucket, destinationKey)
	if err != nil {
		return nil, err
	}
	sourceInfo, err := fs.checkRenamePath(source)
	if errors.Is(err, os.ErrNotExist) {
		return nil, s3Error("NoSuchKey", 404, "source file missing")
	}
	if err != nil {
		return nil, err
	}
	if !sourceInfo.Mode().IsRegular() {
		return nil, s3Error("InvalidArgument", 400, "rename source must be a regular file")
	}
	move := &renameFileMove{SourcePath: source, DestinationPath: destination}
	if source == destination {
		return move, nil
	}
	destInfo, err := fs.checkRenamePath(destination)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	if err == nil && (!destInfo.Mode().IsRegular() || os.SameFile(sourceInfo, destInfo)) {
		return nil, s3Error("InvalidArgument", 400, "invalid or aliased destination")
	}
	if err = os.MkdirAll(filepath.Dir(destination), 0755); err != nil {
		return nil, err
	}
	if destInfo != nil {
		staging := filepath.Join(fs.baseDir, ".rename-staging")
		if err = os.Mkdir(staging, 0700); err != nil && !errors.Is(err, os.ErrExist) {
			return nil, err
		}
		stageInfo, err := os.Lstat(staging)
		if err != nil {
			return nil, err
		}
		if !stageInfo.IsDir() || stageInfo.Mode()&os.ModeSymlink != 0 {
			return nil, s3Error("InvalidArgument", 400, "invalid rename staging directory")
		}
		backup, err := os.CreateTemp(staging, "move-")
		if err != nil {
			return nil, err
		}
		move.BackupPath = backup.Name()
		if err = backup.Close(); err != nil {
			return move, err
		}
		if err = os.Rename(destination, move.BackupPath); err != nil {
			return move, err
		}
		move.BackedUp = true
	}
	if err = os.Rename(source, destination); err != nil {
		return move, err
	}
	move.Moved = true
	return move, nil
}
func (m *renameFileMove) rollback() error {
	if m == nil {
		return nil
	}
	if m.Moved {
		if err := os.Rename(m.DestinationPath, m.SourcePath); err != nil {
			return err
		}
		m.Moved = false
	}
	if m.BackedUp {
		if err := os.Rename(m.BackupPath, m.DestinationPath); err != nil {
			return err
		}
		m.BackedUp = false
	}
	return m.cleanup()
}
func (m *renameFileMove) cleanup() error {
	if m == nil || m.BackupPath == "" {
		return nil
	}
	err := os.Remove(m.BackupPath)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return err
}
