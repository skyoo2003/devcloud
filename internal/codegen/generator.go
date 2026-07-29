// SPDX-License-Identifier: Apache-2.0

// internal/codegen/generator.go
package codegen

import (
	"fmt"
	"go/format"
	"os"
	"path/filepath"
)

// WriteGo gofmts content and writes it to path. Templates emit close-enough Go;
// formatting here keeps a fresh `make codegen` byte-identical to what is
// committed, so a regeneration diff shows real model changes and nothing else.
func WriteGo(path, content string) error {
	formatted, err := format.Source([]byte(content))
	if err != nil {
		return fmt.Errorf("gofmt %s: %w", path, err)
	}
	return os.WriteFile(path, formatted, 0644)
}

func (g *Generator) GenerateAll(model *SmithyModel, outputDir string, scaffoldDir string) error {
	pkgName := model.ServiceID
	serviceDir := filepath.Join(outputDir, pkgName)
	if err := os.MkdirAll(serviceDir, 0755); err != nil {
		return fmt.Errorf("mkdir %s: %w", serviceDir, err)
	}

	generators := []struct {
		fileName string
		generate func(string, *SmithyModel) (string, error)
	}{
		{"types.go", g.GenerateTypes},
		{"base_provider.go", g.GenerateBaseProvider},
		{"router.go", g.GenerateRouter},
		{"errors.go", g.GenerateErrors},
	}

	for _, gen := range generators {
		content, err := gen.generate(pkgName, model)
		if err != nil {
			return fmt.Errorf("generate %s: %w", gen.fileName, err)
		}
		outPath := filepath.Join(serviceDir, gen.fileName)
		if err := WriteGo(outPath, content); err != nil {
			return fmt.Errorf("write %s: %w", outPath, err)
		}
	}

	if scaffoldDir == "" {
		return nil
	}

	// Scaffold provider — only written if it doesn't already exist, so a
	// hand-written provider is never clobbered by a regeneration.
	scaffoldServiceDir := filepath.Join(scaffoldDir, pkgName)
	if err := os.MkdirAll(scaffoldServiceDir, 0755); err != nil {
		return fmt.Errorf("mkdir scaffold %s: %w", scaffoldServiceDir, err)
	}
	outPath := filepath.Join(scaffoldServiceDir, "provider.go")
	if _, err := os.Stat(outPath); err == nil {
		return nil
	}
	content, err := g.GenerateScaffold(pkgName, model)
	if err != nil {
		return fmt.Errorf("generate scaffold %s: %w", outPath, err)
	}
	if err := WriteGo(outPath, content); err != nil {
		return fmt.Errorf("write scaffold %s: %w", outPath, err)
	}
	return nil
}
