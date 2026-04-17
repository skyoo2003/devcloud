// SPDX-License-Identifier: Apache-2.0

package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/skyoo2003/devcloud/internal/codegen"
)

func main() {
	modelsDir := flag.String("models", "./smithy-models", "Directory containing Smithy JSON model files")
	outputDir := flag.String("output", "./internal/generated", "Output directory for generated code")
	services := flag.String("services", "", "Comma-separated list of services to generate (empty = all)")
	templateDir := flag.String("templates", "./internal/codegen/templates", "Directory containing Go templates")
	scaffoldDir := flag.String("scaffold-output", "", "Output directory for scaffold files (provider.go, register.go)")
	flag.Parse()

	entries, err := os.ReadDir(*modelsDir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error reading models dir: %v\n", err)
		os.Exit(1)
	}

	allowedServices := make(map[string]bool)
	if *services != "" {
		for _, s := range strings.Split(*services, ",") {
			allowedServices[strings.TrimSpace(s)] = true
		}
	}

	gen := codegen.NewGenerator(*templateDir)

	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}

		data, err := os.ReadFile(filepath.Join(*modelsDir, entry.Name()))
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error reading %s: %v\n", entry.Name(), err)
			continue
		}

		model, err := codegen.ParseSmithyJSON(data)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error parsing %s: %v\n", entry.Name(), err)
			continue
		}

		if len(allowedServices) > 0 && !allowedServices[model.ServiceID] {
			continue
		}

		fmt.Printf("Generating %s (%s)...\n", model.ServiceName, model.ServiceID)
		if err := gen.GenerateAll(model, *outputDir, *scaffoldDir); err != nil {
			fmt.Fprintf(os.Stderr, "Error generating %s: %v\n", model.ServiceID, err)
			os.Exit(1)
		}
	}

	fmt.Println("Code generation complete.")
}
