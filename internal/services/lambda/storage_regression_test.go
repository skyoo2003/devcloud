// SPDX-License-Identifier: Apache-2.0
package lambda

import (
	"archive/zip"
	"bytes"
	"github.com/skyoo2003/devcloud/internal/storage/sqlite"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func handlerZIP(t *testing.T, text string) []byte {
	t.Helper()
	var b bytes.Buffer
	w := zip.NewWriter(&b)
	f, err := w.Create("index.py")
	require.NoError(t, err)
	_, err = f.Write([]byte(text))
	require.NoError(t, err)
	require.NoError(t, w.Close())
	return b.Bytes()
}
func regressionFunction() *FunctionInfo {
	return &FunctionInfo{FunctionName: "immutable", FunctionArn: "arn:aws:lambda:us-east-1:000000000000:function:immutable", AccountID: "000000000000", Runtime: "python3.12", Handler: "index.handler", Timeout: 3, MemorySize: 128}
}
func TestDuplicateCreatePreservesCode(t *testing.T) {
	s := newTestLambdaStore(t)
	first := handlerZIP(t, "def handler(e,c): return 1")
	_, err := s.CreateFunction(regressionFunction(), first)
	require.NoError(t, err)
	_, err = s.CreateFunction(regressionFunction(), handlerZIP(t, "def handler(e,c): return 2"))
	require.ErrorIs(t, err, ErrFunctionAlreadyExists)
	code, err := s.GetFunctionCode("000000000000", "immutable")
	require.NoError(t, err)
	require.Equal(t, first, code)
}
func TestPublishedVersionCodeIsImmutable(t *testing.T) {
	s := newTestLambdaStore(t)
	first := handlerZIP(t, "def handler(e,c): return 1")
	second := handlerZIP(t, "def handler(e,c): return 2")
	_, err := s.CreateFunction(regressionFunction(), first)
	require.NoError(t, err)
	v, err := s.PublishVersion("000000000000", "immutable")
	require.NoError(t, err)
	_, err = s.UpdateFunctionCode("000000000000", "immutable", second)
	require.NoError(t, err)
	code, err := os.ReadFile(v.CodePath)
	require.NoError(t, err)
	require.Equal(t, first, code)
	latest, err := s.GetFunctionCode("000000000000", "immutable")
	require.NoError(t, err)
	require.Equal(t, second, latest)
}
func TestDeleteFunctionRemovesVersionCode(t *testing.T) {
	s := newTestLambdaStore(t)
	_, err := s.CreateFunction(regressionFunction(), handlerZIP(t, "pass"))
	require.NoError(t, err)
	v, err := s.PublishVersion("000000000000", "immutable")
	require.NoError(t, err)
	require.NoError(t, s.DeleteFunction("000000000000", "immutable"))
	_, err = os.Stat(v.CodePath)
	require.True(t, os.IsNotExist(err))
	_, err = s.GetVersion("000000000000", "immutable", v.Version)
	require.ErrorIs(t, err, ErrVersionNotFound)
}

func TestFunctionEnvironmentPersists(t *testing.T) {
	dir := t.TempDir()
	db := filepath.Join(dir, "lambda.db")
	codes := filepath.Join(dir, "code")
	s, err := NewLambdaStore(db, codes)
	require.NoError(t, err)
	info := regressionFunction()
	info.Environment = map[string]string{"MODE": "v1"}
	_, err = s.CreateFunction(info, handlerZIP(t, "pass"))
	require.NoError(t, err)
	_, err = s.PublishVersion(info.AccountID, info.FunctionName)
	require.NoError(t, err)
	_, err = s.UpdateFunctionEnvironment(info.AccountID, info.FunctionName, map[string]string{})
	require.NoError(t, err)
	require.NoError(t, s.Close())
	s, err = NewLambdaStore(db, codes)
	require.NoError(t, err)
	defer func() { require.NoError(t, s.Close()) }()
	restored, err := s.GetFunction(info.AccountID, info.FunctionName)
	require.NoError(t, err)
	require.Empty(t, restored.Environment)
	old, err := s.ResolveFunction(info.AccountID, info.FunctionName, "1")
	require.NoError(t, err)
	require.Equal(t, map[string]string{"MODE": "v1"}, old.Environment)
}
func TestResolveFunctionVersionAndAlias(t *testing.T) {
	s := newTestLambdaStore(t)
	info := regressionFunction()
	_, err := s.CreateFunction(info, handlerZIP(t, "pass"))
	require.NoError(t, err)
	_, err = s.PublishVersion(info.AccountID, info.FunctionName)
	require.NoError(t, err)
	_, err = s.CreateAlias(info.AccountID, info.FunctionName, "live", "1")
	require.NoError(t, err)
	for _, ref := range []string{"immutable:1", "immutable:live", info.FunctionArn + ":live"} {
		f, err := s.ResolveFunction(info.AccountID, ref, "")
		require.NoError(t, err)
		require.Contains(t, f.CodePath, "versions/1/code.zip")
	}
	_, err = s.ResolveFunction(info.AccountID, "immutable:1", "live")
	require.Error(t, err)
	_, err = s.ResolveFunction(info.AccountID, "immutable", "99")
	require.ErrorIs(t, err, ErrVersionNotFound)
}
func TestLegacyVersionPathMigration(t *testing.T) {
	dir := t.TempDir()
	db := filepath.Join(dir, "lambda.db")
	codes := filepath.Join(dir, "code")
	path := filepath.Join(codes, "000000000000", "immutable", "code.zip")
	require.NoError(t, os.MkdirAll(filepath.Dir(path), 0755))
	code := handlerZIP(t, "pass")
	require.NoError(t, os.WriteFile(path, code, 0644))
	old, err := sqlite.Open(db, lambdaMigrations[:6])
	require.NoError(t, err)
	_, err = old.DB().Exec(`INSERT INTO functions(function_name,function_arn,runtime,handler,account_id,code_path) VALUES('immutable','arn:aws:lambda:us-east-1:000000000000:function:immutable','python3.12','index.handler','000000000000',?)`, path)
	require.NoError(t, err)
	_, err = old.DB().Exec(`INSERT INTO function_versions VALUES('immutable','1','000000000000',?,'{}',CURRENT_TIMESTAMP)`, path)
	require.NoError(t, err)
	require.NoError(t, old.Close())
	s, err := NewLambdaStore(db, codes)
	require.NoError(t, err)
	defer func() { require.NoError(t, s.Close()) }()
	v, err := s.GetVersion("000000000000", "immutable", "1")
	require.NoError(t, err)
	require.NotEqual(t, path, v.CodePath)
	restored, err := os.ReadFile(v.CodePath)
	require.NoError(t, err)
	require.Equal(t, code, restored)
	f, err := s.GetFunction("000000000000", "immutable")
	require.NoError(t, err)
	require.Empty(t, f.Environment)
	reader, err := sqlite.Open(db, lambdaMigrations[:6])
	require.NoError(t, err)
	defer func() { require.NoError(t, reader.Close()) }()
	var legacyPath string
	require.NoError(t, reader.DB().QueryRow(`SELECT code_path FROM function_versions`).Scan(&legacyPath))
	require.Equal(t, v.CodePath, legacyPath)
}

func TestUpgradeAfterLegacyDelete(t *testing.T) {
	dir := t.TempDir()
	db := filepath.Join(dir, "lambda.db")
	codes := filepath.Join(dir, "code")
	path := filepath.Join(codes, "000000000000", "deleted", "code.zip")
	old, err := sqlite.Open(db, lambdaMigrations[:6])
	require.NoError(t, err)
	// State left by the old PublishVersion then DeleteFunction: version row remains, code removed.
	_, err = old.DB().Exec(`INSERT INTO function_versions VALUES('deleted','1','000000000000',?,'{}',CURRENT_TIMESTAMP)`, path)
	require.NoError(t, err)
	require.NoError(t, old.Close())
	current, err := NewLambdaStore(db, codes)
	if current != nil {
		defer func() { require.NoError(t, current.Close()) }()
	}
	require.NoError(t, err, "old valid delete history must not prevent startup")
}
