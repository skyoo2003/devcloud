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
	"github.com/skyoo2003/devcloud/internal/codegen/ir"
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
	var allModels []*ir.Model
	modelOps := make(map[string][]string)
	protocols := make(map[string]string)
	// A model that cannot be read or parsed is skipped, which used to leave the
	// exit status at 0 — so a drift check downstream saw no changed files and
	// called incomplete generation clean.
	skipped := false

	// The driver knows nothing about model formats: it hands every file to
	// codegen.SourceFor and generates from whatever IR comes back. Adding
	// OpenAPI or Protobuf support is a new ModelSource, not a change here.
	// A file no source claims is an error rather than a quiet skip — the
	// models directory holds models, and a silently ignored one produces
	// generated output that is wrong without saying so.
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		data, err := os.ReadFile(filepath.Join(*modelsDir, entry.Name()))
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error reading %s: %v\n", entry.Name(), err)
			skipped = true
			continue
		}

		source, err := codegen.SourceFor(entry.Name(), data)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			skipped = true
			continue
		}

		model, err := source.Parse(data)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error parsing %s as %s: %v\n", entry.Name(), source.Name(), err)
			skipped = true
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
		protocols[model.ServiceID] = model.Protocol

		// Also recorded before the filters: a hand-written provider still has to
		// be routed to, so its model still contributes routing aliases even
		// though it generates no package.
		allModels = append(allModels, model)

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

	// Bail before the aggregate artefacts: the CRUD registry and the fidelity
	// manifest describe the whole fleet, and writing them from a set that is
	// missing a service would state, in generated code, that its operations do
	// not exist.
	if skipped {
		fmt.Fprintln(os.Stderr, "Error: one or more models were skipped; generated output is incomplete")
		os.Exit(1)
	}

	// Write the aggregate CRUD registry only when generating the full fleet
	// (a filtered run would otherwise clobber it with a partial registry).
	if len(allowedServices) == 0 {
		providers, err := codegen.ScanProviders(*servicesDir)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error scanning providers: %v\n", err)
			os.Exit(1)
		}

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

		if err := writeFidelityManifest(gen, *outputDir, modelOps, protocols, providers, crudServices); err != nil {
			fmt.Fprintf(os.Stderr, "Error writing fidelity manifest: %v\n", err)
			os.Exit(1)
		}

		if err := writeAliases(gen, *outputDir, allModels); err != nil {
			fmt.Fprintf(os.Stderr, "Error writing alias table: %v\n", err)
			os.Exit(1)
		}
	}

	fmt.Println("Code generation complete.")
}

// writeAliases emits the routing alias table. Contested aliases are reported on
// stderr rather than being resolved here: the generator cannot see a request, so
// it cannot tell an opensearch call from an elasticsearchservice one. They are
// named in the generated Collisions list, and a gateway test fails until each has
// a resolution — loud, but not fatal to generation, because a collision blocks
// one alias rather than making the whole fleet's generated output wrong.
func writeAliases(gen *codegen.Generator, outputDir string, models []*ir.Model) error {
	table, collisions := codegen.BuildAliases(models)
	for _, alias := range collisions {
		fmt.Fprintf(os.Stderr, "WARN: alias %q is claimed by more than one service; "+
			"it is omitted from the table and needs a resolution in the gateway\n", alias)
	}

	content, err := gen.GenerateAliases(table, collisions)
	if err != nil {
		return err
	}

	dir := filepath.Join(outputDir, "aliases")
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}
	return codegen.WriteGo(filepath.Join(dir, "aliases_gen.go"), content)
}

// writeFidelityManifest emits the per-operation fidelity manifest. Like the CRUD
// registry it is only written on a full-fleet run, since a filtered run would
// produce a partial manifest.
func writeFidelityManifest(
	gen *codegen.Generator,
	outputDir string,
	modelOps map[string][]string,
	protocols map[string]string,
	providers map[string]codegen.ProviderScan,
	crudServices []codegen.CRUDServiceData,
) error {
	content, err := gen.GenerateFidelityManifest(
		codegen.BuildFidelityData(modelOps, protocols, providers, crudServices),
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
