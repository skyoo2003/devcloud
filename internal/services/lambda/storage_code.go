// SPDX-License-Identifier: Apache-2.0
package lambda

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

func writeCodeAtomic(path string, code []byte) error {
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}
	f, err := os.CreateTemp(filepath.Dir(path), ".code-*")
	if err != nil {
		return err
	}
	defer func() { _ = os.Remove(f.Name()) }()
	if _, err = f.Write(code); err != nil {
		_ = f.Close()
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	return os.Rename(f.Name(), path)
}

func environmentJSON(variables map[string]string) string {
	if variables == nil {
		return "{}"
	}
	b, _ := json.Marshal(variables)
	return string(b)
}

func (s *LambdaStore) UpdateFunctionEnvironment(accountID, name string, variables map[string]string) (*FunctionInfo, error) {
	s.codeMu.Lock()
	defer s.codeMu.Unlock()
	if _, err := s.GetFunction(accountID, name); err != nil {
		return nil, err
	}
	_, err := s.store.DB().Exec(`UPDATE functions SET environment_json = ? WHERE account_id = ? AND function_name = ?`, environmentJSON(variables), accountID, name)
	if err != nil {
		return nil, err
	}
	return s.GetFunction(accountID, name)
}

func (s *LambdaStore) ResolveFunction(accountID, reference, qualifier string) (*FunctionInfo, error) {
	name, q, err := parseFunctionReference(reference, qualifier)
	if err != nil {
		return nil, err
	}
	f, err := s.GetFunction(accountID, name)
	if err != nil {
		return nil, err
	}
	if q == "" || q == "$LATEST" {
		return f, nil
	}
	version := q
	if q[0] < '0' || q[0] > '9' {
		a, err := s.GetAlias(accountID, name, q)
		if err != nil {
			return nil, err
		}
		version = a.FunctionVersion
	}
	v, err := s.GetVersion(accountID, name, version)
	if err != nil {
		return nil, err
	}
	config := make(map[string]any, len(v.Config))
	for k, val := range v.Config {
		if k != "Environment" {
			config[k] = val
		}
	}
	b, err := json.Marshal(config)
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(b, f); err != nil {
		return nil, err
	}
	f.Environment = map[string]string{}
	if e, ok := v.Config["Environment"]; ok {
		b, err = json.Marshal(e)
		if err != nil {
			return nil, err
		}
		var env struct{ Variables map[string]string }
		if err = json.Unmarshal(b, &env); err != nil {
			return nil, err
		}
		f.Environment = env.Variables
	}
	f.CodePath = v.CodePath
	f.FunctionArn = f.FunctionArn + ":" + v.Version
	return f, nil
}

func (s *LambdaStore) migrateVersionPaths() error {
	rows, err := s.store.DB().Query(`SELECT v.function_name,v.account_id,v.version,v.code_path
		FROM function_versions v JOIN functions f
		ON f.function_name = v.function_name AND f.account_id = v.account_id`)
	if err != nil {
		return err
	}
	type legacy struct{ name, account, version, path string }
	var versions []legacy
	for rows.Next() {
		var v legacy
		if err := rows.Scan(&v.name, &v.account, &v.version, &v.path); err != nil {
			_ = rows.Close()
			return err
		}
		versions = append(versions, v)
	}
	err = rows.Err()
	_ = rows.Close()
	if err != nil {
		return err
	}
	for _, v := range versions {
		base, err := s.codePath(v.account, v.name)
		if err != nil {
			return err
		}
		if !isSafePathComponent(v.version) {
			return fmt.Errorf("invalid legacy version")
		}
		path := filepath.Join(filepath.Dir(base), "versions", v.version, "code.zip")
		if filepath.Clean(v.path) == filepath.Clean(path) {
			continue
		}
		code, err := os.ReadFile(v.path)
		if err != nil {
			return fmt.Errorf("read legacy Lambda version: %w", err)
		}
		if err := writeCodeAtomic(path, code); err != nil {
			return err
		}
		if _, err := s.store.DB().Exec(`UPDATE function_versions SET code_path=? WHERE function_name=? AND account_id=? AND version=?`, path, v.name, v.account, v.version); err != nil {
			return err
		}
	}
	return nil
}
