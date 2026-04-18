// SPDX-License-Identifier: Apache-2.0

package lambda

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestLambdaStore(t *testing.T) *LambdaStore {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "lambda.db")
	codeDir := t.TempDir()
	store, err := NewLambdaStore(dbPath, codeDir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })
	return store
}

func TestLambdaStore_CreateAndGetFunction(t *testing.T) {
	store := newTestLambdaStore(t)

	codeZip := []byte("fake-zip-bytes")
	info := &FunctionInfo{
		FunctionName: "my-function",
		FunctionArn:  "arn:aws:lambda:us-east-1:123456789012:function:my-function",
		Runtime:      "python3.12",
		Handler:      "index.handler",
		Role:         "arn:aws:iam::123456789012:role/my-role",
		Description:  "A test function",
		Timeout:      30,
		MemorySize:   256,
		AccountID:    "123456789012",
	}

	created, err := store.CreateFunction(info, codeZip)
	require.NoError(t, err)
	assert.Equal(t, "my-function", created.FunctionName)
	assert.Equal(t, "python3.12", created.Runtime)
	assert.Equal(t, "index.handler", created.Handler)
	assert.Equal(t, int64(len(codeZip)), created.CodeSize)
	assert.NotEmpty(t, created.CodePath)
	assert.False(t, created.LastModified.IsZero())

	got, err := store.GetFunction("123456789012", "my-function")
	require.NoError(t, err)
	assert.Equal(t, "my-function", got.FunctionName)
	assert.Equal(t, "python3.12", got.Runtime)
	assert.Equal(t, "index.handler", got.Handler)
	assert.Equal(t, "arn:aws:iam::123456789012:role/my-role", got.Role)
	assert.Equal(t, int64(len(codeZip)), got.CodeSize)
	assert.Equal(t, "A test function", got.Description)
	assert.Equal(t, 30, got.Timeout)
	assert.Equal(t, 256, got.MemorySize)
	assert.Equal(t, "123456789012", got.AccountID)
	assert.NotEmpty(t, got.CodePath)
}

func TestLambdaStore_CreateDuplicate(t *testing.T) {
	store := newTestLambdaStore(t)

	info := &FunctionInfo{
		FunctionName: "my-function",
		FunctionArn:  "arn:aws:lambda:us-east-1:123456789012:function:my-function",
		Runtime:      "nodejs20.x",
		Handler:      "index.handler",
		AccountID:    "123456789012",
	}

	_, err := store.CreateFunction(info, []byte("zip1"))
	require.NoError(t, err)

	_, err = store.CreateFunction(info, []byte("zip2"))
	assert.ErrorIs(t, err, ErrFunctionAlreadyExists)
}

func TestLambdaStore_ListFunctions(t *testing.T) {
	store := newTestLambdaStore(t)

	for _, name := range []string{"function-a", "function-b"} {
		info := &FunctionInfo{
			FunctionName: name,
			FunctionArn:  "arn:aws:lambda:us-east-1:123456789012:function:" + name,
			Runtime:      "python3.12",
			Handler:      "index.handler",
			AccountID:    "123456789012",
		}
		_, err := store.CreateFunction(info, []byte("zip"))
		require.NoError(t, err)
	}

	functions, err := store.ListFunctions("123456789012")
	require.NoError(t, err)
	assert.Len(t, functions, 2)
}

func TestLambdaStore_DeleteFunction(t *testing.T) {
	store := newTestLambdaStore(t)

	info := &FunctionInfo{
		FunctionName: "to-delete",
		FunctionArn:  "arn:aws:lambda:us-east-1:123456789012:function:to-delete",
		Runtime:      "python3.12",
		Handler:      "index.handler",
		AccountID:    "123456789012",
	}

	_, err := store.CreateFunction(info, []byte("zip"))
	require.NoError(t, err)

	err = store.DeleteFunction("123456789012", "to-delete")
	require.NoError(t, err)

	_, err = store.GetFunction("123456789012", "to-delete")
	assert.ErrorIs(t, err, ErrFunctionNotFound)
}

func TestLambdaStore_UpdateFunctionCode(t *testing.T) {
	store := newTestLambdaStore(t)

	info := &FunctionInfo{
		FunctionName: "update-me",
		FunctionArn:  "arn:aws:lambda:us-east-1:123456789012:function:update-me",
		Runtime:      "python3.12",
		Handler:      "index.handler",
		AccountID:    "123456789012",
	}

	originalZip := []byte("small")
	_, err := store.CreateFunction(info, originalZip)
	require.NoError(t, err)

	newZip := []byte("this-is-a-much-larger-zip-file")
	updated, err := store.UpdateFunctionCode("123456789012", "update-me", newZip)
	require.NoError(t, err)
	assert.Equal(t, int64(len(newZip)), updated.CodeSize)
	assert.NotEqual(t, int64(len(originalZip)), updated.CodeSize)
}

func TestLambdaStore_GetFunctionCode(t *testing.T) {
	store := newTestLambdaStore(t)

	info := &FunctionInfo{
		FunctionName: "code-function",
		FunctionArn:  "arn:aws:lambda:us-east-1:123456789012:function:code-function",
		Runtime:      "python3.12",
		Handler:      "index.handler",
		AccountID:    "123456789012",
	}

	originalZip := []byte("zip-file-contents-here")
	_, err := store.CreateFunction(info, originalZip)
	require.NoError(t, err)

	code, err := store.GetFunctionCode("123456789012", "code-function")
	require.NoError(t, err)
	assert.Equal(t, originalZip, code)
}
