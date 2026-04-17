// SPDX-License-Identifier: Apache-2.0

// internal/codegen/generator.go
package codegen

import (
	"fmt"
	"os"
	"path/filepath"
)

func (g *Generator) GenerateAll(model *SmithyModel, outputDir string, scaffoldDir string) error {
	pkgName := model.ServiceID
	serviceDir := filepath.Join(outputDir, pkgName)
	if err := os.MkdirAll(serviceDir, 0755); err != nil {
		return fmt.Errorf("mkdir %s: %w", serviceDir, err)
	}

	generators := map[string]func(string, *SmithyModel) (string, error){
		"types.go":         g.GenerateTypes,
		"interface.go":     g.GenerateInterface,
		"base_provider.go": g.GenerateBaseProvider,
		"serializer.go":    g.GenerateSerializer,
		"deserializer.go":  g.GenerateDeserializer,
		"router.go":        g.GenerateRouter,
		"errors.go":        g.GenerateErrors,
	}

	for fileName, genFunc := range generators {
		content, err := genFunc(pkgName, model)
		if err != nil {
			return fmt.Errorf("generate %s: %w", fileName, err)
		}
		outPath := filepath.Join(serviceDir, fileName)
		if err := os.WriteFile(outPath, []byte(content), 0644); err != nil {
			return fmt.Errorf("write %s: %w", outPath, err)
		}
	}

	// Scaffold files — only written if they don't already exist.
	if scaffoldDir != "" {
		scaffoldServiceDir := filepath.Join(scaffoldDir, pkgName)
		if err := os.MkdirAll(scaffoldServiceDir, 0755); err != nil {
			return fmt.Errorf("mkdir scaffold %s: %w", scaffoldServiceDir, err)
		}

		scaffolds := map[string]func(string, *SmithyModel) (string, error){
			"provider.go": g.GenerateScaffold,
			"register.go": g.GenerateRegister,
		}

		for fileName, genFunc := range scaffolds {
			outPath := filepath.Join(scaffoldServiceDir, fileName)
			if _, err := os.Stat(outPath); err == nil {
				continue // file exists, skip
			}
			content, err := genFunc(pkgName, model)
			if err != nil {
				return fmt.Errorf("generate scaffold %s: %w", fileName, err)
			}
			if err := os.WriteFile(outPath, []byte(content), 0644); err != nil {
				return fmt.Errorf("write scaffold %s: %w", outPath, err)
			}
		}
	}

	return nil
}
