// SPDX-License-Identifier: Apache-2.0

package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/skyoo2003/devcloud/internal/codegen"
)

// handWritten services get no generated package: their provider is implemented
// by hand elsewhere (sts lives in internal/services/iam alongside its store).
// Their generated stubs were deleted in #91 and #96; without this skip every
// `make codegen` run resurrects them as untracked files that the sync
// workflow's diff check cannot see. sts is Query-protocol, so it contributes
// nothing to the JSON-only CRUD registry either.
var handWritten = map[string]bool{"sts": true}

func main() {
	modelsDir := flag.String("models", "./smithy-models", "Directory containing Smithy JSON model files")
	outputDir := flag.String("output", "./internal/generated", "Output directory for generated code")
	services := flag.String("services", "", "Comma-separated list of services to generate (empty = all)")
	templateDir := flag.String("templates", "./internal/codegen/templates", "Directory containing Go templates")
	scaffoldDir := flag.String("scaffold-output", "", "Output directory for scaffold files (provider.go, register.go)")
	servicesDir := flag.String("services-dir", "./internal/services", "Directory containing hand-written providers, scanned for the fidelity manifest")
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

	var crudServices []codegen.CRUDServiceData
	modelOps := make(map[string][]string)

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

		// Recorded before the filters below: the fidelity manifest needs the
		// operation universe of every modelled service, including the ones whose
		// provider is hand-written and generates no package.
		names := make([]string, 0, len(model.Operations))
		for _, op := range model.Operations {
			names = append(names, op.Name)
		}
		modelOps[model.ServiceID] = names

		if len(allowedServices) > 0 && !allowedServices[model.ServiceID] {
			continue
		}

		if handWritten[model.ServiceID] {
			fmt.Printf("Skipping %s (%s): hand-written provider\n", model.ServiceName, model.ServiceID)
			continue
		}

		fmt.Printf("Generating %s (%s)...\n", model.ServiceName, model.ServiceID)
		if err := gen.GenerateAll(model, *outputDir, *scaffoldDir); err != nil {
			fmt.Fprintf(os.Stderr, "Error generating %s: %v\n", model.ServiceID, err)
			os.Exit(1)
		}

		if data, ok := codegen.ServiceCRUDData(model); ok {
			crudServices = append(crudServices, data)
		}
	}

	// Write the aggregate CRUD registry only when generating the full fleet
	// (a filtered run would otherwise clobber it with a partial registry).
	if len(allowedServices) == 0 {
		sort.Slice(crudServices, func(i, j int) bool {
			return crudServices[i].ServiceID < crudServices[j].ServiceID
		})
		content, err := gen.GenerateCRUDRegistry(crudServices)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error generating CRUD registry: %v\n", err)
			os.Exit(1)
		}
		regDir := filepath.Join(*outputDir, "crudregistry")
		if err := os.MkdirAll(regDir, 0755); err != nil {
			fmt.Fprintf(os.Stderr, "Error creating crudregistry dir: %v\n", err)
			os.Exit(1)
		}
		if err := codegen.WriteGo(filepath.Join(regDir, "registry_gen.go"), content); err != nil {
			fmt.Fprintf(os.Stderr, "Error writing CRUD registry: %v\n", err)
			os.Exit(1)
		}

		if err := writeFidelityManifest(gen, *outputDir, *servicesDir, modelOps, crudServices); err != nil {
			fmt.Fprintf(os.Stderr, "Error writing fidelity manifest: %v\n", err)
			os.Exit(1)
		}
	}

	fmt.Println("Code generation complete.")
}

// writeFidelityManifest scans the hand-written providers and emits the
// per-operation fidelity manifest. Like the CRUD registry it is only written on
// a full-fleet run, since a filtered run would produce a partial manifest.
func writeFidelityManifest(
	gen *codegen.Generator,
	outputDir, servicesDir string,
	modelOps map[string][]string,
	crudServices []codegen.CRUDServiceData,
) error {
	handVerified, err := codegen.ScanHandVerified(servicesDir, modelOps)
	if err != nil {
		return err
	}

	content, err := gen.GenerateFidelityManifest(
		codegen.BuildFidelityData(modelOps, handVerified, crudServices),
	)
	if err != nil {
		return err
	}

	dir := filepath.Join(outputDir, "fidelity")
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}
	return codegen.WriteGo(filepath.Join(dir, "manifest_gen.go"), content)
}
