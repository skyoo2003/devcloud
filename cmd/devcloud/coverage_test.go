// SPDX-License-Identifier: Apache-2.0

package main

import (
	"fmt"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/skyoo2003/devcloud/internal/generated/fidelity"
	"github.com/skyoo2003/devcloud/internal/plugin"
)

// coveragePath is the published claim this file gates. It is read rather than
// duplicated: a constant here would be a third place the number lives, and the
// doc would still be free to drift from it.
const coveragePath = "../../docs/coverage.md"

// coverageRow matches one row of the summary table at the top of
// docs/coverage.md:
//
//	| **Registered** | The gateway routes the service. … | **205** |
//
// The label is anchored to the row start so a number quoted in prose elsewhere
// on the page cannot be mistaken for the published figure.
func coverageRow(t *testing.T, doc, label string) int {
	t.Helper()

	pattern := regexp.MustCompile(`(?m)^\|\s*\*\*` + regexp.QuoteMeta(label) + `\*\*\s*\|[^|]*\|\s*\*\*(\d+)\*\*\s*\|`)
	matches := pattern.FindAllStringSubmatch(doc, -1)
	if len(matches) != 1 {
		t.Fatalf("docs/coverage.md: found %d rows for %q, want exactly 1. "+
			"The summary table was restructured; this gate reads it, so update the "+
			"pattern deliberately rather than letting the numbers stop being checked.",
			len(matches), label)
	}

	n, err := strconv.Atoi(matches[0][1])
	if err != nil {
		t.Fatalf("docs/coverage.md: %q row has an unreadable number %q", label, matches[0][1])
	}
	return n
}

// tierRow matches one row of the per-operation table in docs/coverage.md:
//
//	| `hand-verified` | 4,496 |
//
// Thousands separators are stripped: the doc is written for a reader, and the
// gate reads what the reader sees rather than asking the doc to be machine-shaped.
func tierRow(t *testing.T, doc, label string) int {
	t.Helper()

	pattern := regexp.MustCompile("(?m)^\\|\\s*`" + regexp.QuoteMeta(label) + "`\\s*\\|\\s*([\\d,]+)\\s*\\|")
	matches := pattern.FindAllStringSubmatch(doc, -1)
	if len(matches) != 1 {
		t.Fatalf("docs/coverage.md: found %d rows for tier %q, want exactly 1", len(matches), label)
	}

	n, err := strconv.Atoi(strings.ReplaceAll(matches[0][1], ",", ""))
	if err != nil {
		t.Fatalf("docs/coverage.md: tier %q has an unreadable number %q", label, matches[0][1])
	}
	return n
}

// servedCounts splits the manifest the way docs/coverage.md publishes it.
func servedCounts() (serving int, registeredOnly []string) {
	for id, svc := range fidelity.Services {
		served := 0
		for _, tier := range svc.Operations {
			if tier != fidelity.TierUnimplemented {
				served++
			}
		}
		if served > 0 {
			serving++
			continue
		}
		registeredOnly = append(registeredOnly, id)
	}
	sort.Strings(registeredOnly)
	return serving, registeredOnly
}

// TestPublishedCoverageMatchesTheBinary is Milestone 6's gate: CI fails if a
// registered service drops below the floor or the count regresses.
//
// It asserts in both directions at once, which is the property that makes it
// useful. Removing a service without editing the doc fails, because the registry
// no longer matches the published figure. Editing the doc without changing the
// code fails for the same reason. There is no way to move one without the other.
//
// The floors this replaces (minServices = 100, minOperations = 6000) were
// deliberately conservative, and right for what they were written to catch — a
// mangled generator or broken registration wiring. They cannot notice 205
// becoming 150, which is what the published claim actually rests on.
func TestPublishedCoverageMatchesTheBinary(t *testing.T) {
	raw, err := os.ReadFile(coveragePath)
	if err != nil {
		t.Fatalf("read the published coverage claim: %v", err)
	}
	doc := string(raw)

	registered := plugin.DefaultRegistry.RegisteredServices()
	serving, registeredOnly := servedCounts()

	if got, want := len(registered), len(fidelity.Services); got != want {
		t.Errorf("the registry holds %d services and the fidelity manifest %d; "+
			"run `make codegen`", got, want)
	}

	if got, want := coverageRow(t, doc, "Registered"), len(registered); got != want {
		t.Errorf("docs/coverage.md publishes %d registered services, the binary registers %d. "+
			"If a service was added or removed on purpose, the published figure moves in the "+
			"same commit — that is what this gate is for.", got, want)
	}

	if got, want := coverageRow(t, doc, "Serving ≥1 operation"), serving; got != want {
		t.Errorf("docs/coverage.md publishes %d services serving at least one operation, "+
			"the manifest reports %d", got, want)
	}

	if got, want := coverageRow(t, doc, "Registered-only"), len(registeredOnly); got != want {
		t.Errorf("docs/coverage.md publishes %d registered-only services, the manifest reports "+
			"%d: %v", got, want, registeredOnly)
	}
}

// TestPublishedOperationTiersMatchTheManifest gates the depth half of the claim.
//
// The service count alone is the number docs/coverage.md exists to stop anyone
// quoting on its own, so the per-tier split is gated with the same strictness.
// This is what replaces the minOperations = 6000 floor: a scan that silently
// dropped a provider used to stay far above 6,000 and is caught here.
func TestPublishedOperationTiersMatchTheManifest(t *testing.T) {
	raw, err := os.ReadFile(coveragePath)
	if err != nil {
		t.Fatalf("read the published coverage claim: %v", err)
	}
	doc := string(raw)

	counts := map[fidelity.Tier]int{}
	total := 0
	for _, svc := range fidelity.Services {
		for _, tier := range svc.Operations {
			counts[tier]++
			total++
		}
	}

	for _, tier := range []fidelity.Tier{
		fidelity.TierHandVerified,
		fidelity.TierAutoCRUD,
		fidelity.TierUnimplemented,
	} {
		if got, want := tierRow(t, doc, string(tier)), counts[tier]; got != want {
			t.Errorf("docs/coverage.md publishes %d %s operations, the manifest holds %d",
				got, tier, want)
		}
	}

	published := regexp.MustCompile(`(?m)^\|\s*\*\*total known\*\*\s*\|\s*\*\*([\d,]+)\*\*\s*\|`).
		FindStringSubmatch(doc)
	if published == nil {
		t.Fatal("docs/coverage.md: the 'total known' row did not parse")
	}
	want, _ := strconv.Atoi(strings.ReplaceAll(published[1], ",", ""))
	if want != total {
		t.Errorf("docs/coverage.md publishes %d known operations, the manifest holds %d", want, total)
	}
}

// TestRegisteredOnlyServicesAreNamedInTheDocs keeps the depth claim honest, not
// only the count. docs/coverage.md states that the services serving nothing are
// exactly the ones with no CRUD-shaped operation, and names them. A service
// that silently joins that set would leave the prose true-looking and wrong.
func TestRegisteredOnlyServicesAreNamedInTheDocs(t *testing.T) {
	raw, err := os.ReadFile(coveragePath)
	if err != nil {
		t.Fatalf("read the published coverage claim: %v", err)
	}
	doc := string(raw)

	_, registeredOnly := servedCounts()
	for _, id := range registeredOnly {
		if !strings.Contains(doc, id) && !strings.Contains(doc, hyphenate(id)) {
			t.Errorf("%s serves nothing but docs/coverage.md never names it. "+
				"The page states which services serve nothing and why; a service that "+
				"joins them silently turns a true sentence into a false one.", id)
		}
	}
}

// TestOtherDocsQuoteTheSameFigure catches the drift that actually happened.
//
// README.md and docs/README.md both quoted "148 registered / 117 serving" three
// milestones after it stopped being true, because coverage.md was the only page
// anyone thought to update and nothing checked the others. A front page is where
// the number is read most and verified least.
//
// The pattern is deliberately loose about wording and strict about the pair of
// numbers: these are prose, and pinning their phrasing would make every edit a
// test failure.
func TestOtherDocsQuoteTheSameFigure(t *testing.T) {
	registered := len(plugin.DefaultRegistry.RegisteredServices())
	serving, _ := servedCounts()

	// Loose enough for both phrasings in use — "205 AWS services registered, 201
	// serving at least one operation" and "205 registered / 201 serving".
	quoted := regexp.MustCompile(`(?m)(\d+)[^.\n]{0,24}?registered\b[^.\n]*?(\d+)[^.\n]{0,4}?serving`)

	for _, path := range []string{"../../README.md", "../../docs/README.md"} {
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Errorf("read %s: %v", path, err)
			continue
		}

		matches := quoted.FindAllStringSubmatch(string(raw), -1)
		if len(matches) == 0 {
			// Zero matches is a failure, not a pass. A rewording that stops
			// matching would otherwise disable this check in silence, which is
			// the exact way the figure went stale in the first place.
			t.Errorf("%s no longer states the coverage figure in a form this gate "+
				"can read. Restore the phrasing or update the pattern deliberately.", path)
			continue
		}
		for _, m := range matches {
			gotRegistered, _ := strconv.Atoi(m[1])
			gotServing, _ := strconv.Atoi(m[2])
			if gotRegistered != registered || gotServing != serving {
				t.Errorf("%s quotes %d registered / %d serving; the binary has %d / %d",
					path, gotRegistered, gotServing, registered, serving)
			}
		}
	}
}

// demandPath is the evidence behind the published target. See docs/demand.md.
const demandPath = "../../docs/demand.md"

// demandRow matches one row of the ranking table in docs/demand.md:
//
//	| `emr-serverless` | yes | yes | yes | 3 |
var demandRow = regexp.MustCompile("(?m)^\\|\\s*`([a-z0-9-]+)`\\s*\\|[^|]*\\|[^|]*\\|[^|]*\\|\\s*(\\d+)\\s*\\|")

// TestDemandSetIsRegistered gates the target itself, not only the count.
//
// The published target is "148 registered + the 57 services with demonstrated
// demand", and Milestone 4 met it. Nothing checked that it stays met: a service
// could be dropped from the registry and the total held constant by adding
// something else, leaving the count honest and the target quietly missed.
//
// The survey is not re-run here. It samples three external projects and its
// sample date is part of the evidence — re-sampling in CI would move the target
// silently, which is the opposite of a gate.
func TestDemandSetIsRegistered(t *testing.T) {
	raw, err := os.ReadFile(demandPath)
	if err != nil {
		t.Fatalf("read the demand evidence: %v", err)
	}

	registered := make(map[string]bool)
	for _, id := range plugin.DefaultRegistry.RegisteredServices() {
		registered[id] = true
	}

	var demandSet, missing []string
	for _, row := range demandRow.FindAllStringSubmatch(string(raw), -1) {
		support, err := strconv.Atoi(row[2])
		if err != nil || support < 2 {
			continue
		}
		name := row[1]
		demandSet = append(demandSet, name)
		// Demand names carry the SDK's punctuation; DevCloud service IDs have
		// none. That is the only difference between the two vocabularies here.
		if !registered[strings.ReplaceAll(name, "-", "")] {
			missing = append(missing, name)
		}
	}

	const demandSetSize = 57
	if len(demandSet) != demandSetSize {
		t.Errorf("docs/demand.md lists %d services with support >= 2, want %d. "+
			"The demand set defines the published target; changing it is a decision, "+
			"not a side effect.", len(demandSet), demandSetSize)
	}
	if len(missing) > 0 {
		t.Errorf("%d services with demonstrated demand are not registered: %v. "+
			"docs/coverage.md publishes the target as met.", len(missing), missing)
	}
}

// hyphenate renders a service ID the way the docs write it: DevCloud IDs have no
// punctuation, prose uses the SDK's spelling ("rds-data" for rdsdata).
func hyphenate(id string) string {
	for _, suffix := range []string{"data", "runtime", "query", "control"} {
		if strings.HasSuffix(id, suffix) && len(id) > len(suffix) {
			return fmt.Sprintf("%s-%s", strings.TrimSuffix(id, suffix), suffix)
		}
	}
	return id
}
