// SPDX-License-Identifier: Apache-2.0

package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
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

// coverageRowPattern matches one row of the summary table at the top of
// docs/coverage.md:
//
//	| **Registered** | The gateway routes the service. … | **205** |
//
// The label is anchored to the row start so a number quoted in prose elsewhere
// on the page cannot be mistaken for the published figure. Capture group 1 is
// the published figure, and is the only span figures_update_test.go rewrites —
// the gate that reads a row and the tool that writes it share one pattern, so a
// restructured page cannot leave one of them silently reading the wrong cell.
func coverageRowPattern(label string) *regexp.Regexp {
	return regexp.MustCompile(`(?m)^\|\s*\*\*` + regexp.QuoteMeta(label) + `\*\*\s*\|[^|]*\|\s*\*\*(\d+)\*\*\s*\|`)
}

// coverageRow returns the figure the summary table publishes for label.
func coverageRow(t *testing.T, doc, label string) int {
	t.Helper()

	matches := coverageRowPattern(label).FindAllStringSubmatch(doc, -1)
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

// tierRowPattern matches one row of the per-operation table in docs/coverage.md:
//
//	| `hand-verified` | 4,497 | 4,528 |
//
// Two capture groups, because the table publishes two denominators: the serving
// target first, then the whole registered fleet. Reading only the first column
// would silently repoint this gate at the narrower claim.
//
// Thousands separators are inside the capture groups: the doc is written for a
// reader, and the gate reads what the reader sees rather than asking the doc to
// be machine-shaped. tierRow strips them, and the updater writes them back.
func tierRowPattern(label string) *regexp.Regexp {
	return regexp.MustCompile("(?m)^\\|\\s*`" + regexp.QuoteMeta(label) +
		"`\\s*\\|\\s*([\\d,]+)\\s*\\|\\s*([\\d,]+)\\s*\\|")
}

// totalKnownPattern matches the summed row of that same table, in both columns.
var totalKnownPattern = regexp.MustCompile(
	`(?m)^\|\s*\*\*total known\*\*\s*\|\s*\*\*([\d,]+)\*\*\s*\|\s*\*\*([\d,]+)\*\*\s*\|`)

// handVerifiedSharePattern matches the share row of that same table:
//
//	| **hand-verified share** | **36.2%** | **23.6%** |
//
// The label carries no backticks, so tierRowPattern("hand-verified") cannot
// reach it — the two rows name the same tier and only their shape tells them
// apart. Capture groups hold the number without its percent sign, so the
// updater rewrites 36.2 and leaves the % where it is.
var handVerifiedSharePattern = regexp.MustCompile(
	`(?m)^\|\s*\*\*hand-verified share\*\*\s*\|\s*\*\*([\d.]+)%\*\*\s*\|\s*\*\*([\d.]+)%\*\*\s*\|`)

// tierRow returns the operation counts the manifest table publishes for a tier:
// the serving target first, then all registered services.
func tierRow(t *testing.T, doc, label string) (target, all int) {
	t.Helper()

	matches := tierRowPattern(label).FindAllStringSubmatch(doc, -1)
	if len(matches) != 1 {
		t.Fatalf("docs/coverage.md: found %d rows for tier %q, want exactly 1", len(matches), label)
	}

	parse := func(column int) int {
		n, err := strconv.Atoi(strings.ReplaceAll(matches[0][column], ",", ""))
		if err != nil {
			t.Fatalf("docs/coverage.md: tier %q column %d has an unreadable number %q",
				label, column, matches[0][column])
		}
		return n
	}
	return parse(1), parse(2)
}

// shareTenths is the hand-verified share of a surface, in tenths of a percent.
//
// Integer tenths rather than a float because the gate and the updater compare
// the rendered string: a float tolerance would let the page and the binary
// disagree in the digit the page actually shows. Rounded half up, which is what
// a reader assumes "36.2%" means.
func shareTenths(hand, total int) int {
	if total == 0 {
		return 0
	}
	return (2000*hand + total) / (2 * total)
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

// figures is every published number that is derived rather than decided.
//
// The gates below and the updater in figures_update_test.go both read this, so
// a figure has exactly one derivation. A number that lives in two places is the
// defect docs/coverage.md exists to prevent, and a second copy inside the tool
// that maintains it would be the worst place to keep one.
type figures struct {
	registered     int      // gateway-routed services
	serving        int      // services with >= 1 non-unimplemented operation
	registeredOnly []string // the rest, sorted
	compatTested   int      // registered minus the pinned boto3 exclusions
	tiers          map[fidelity.Tier]int
	totalKnown     int

	// The same two counts over the serving target alone. docs/coverage.md
	// publishes both denominators because reading one as the other is the
	// mistake the page exists to prevent: the long tail was registered so a
	// call cannot leave for a billable account, not because DevCloud promises
	// to serve it well, and folding its operations into the total turns
	// "we routed 226 more services" into what reads as a fidelity regression.
	targetTiers      map[fidelity.Tier]int
	targetTotalKnown int
}

// derivedFigures reads every derivable published number out of the binary.
//
// It reads the registry and the fidelity manifest rather than docs/coverage.md
// because the page is the claim under test: deriving from it would make every
// gate below agree with whatever the page happened to say.
func derivedFigures(t *testing.T) figures {
	t.Helper()

	serving, registeredOnly := servedCounts()
	f := figures{
		registered:     len(plugin.DefaultRegistry.RegisteredServices()),
		serving:        serving,
		registeredOnly: registeredOnly,
		tiers:          map[fidelity.Tier]int{},
		targetTiers:    map[fidelity.Tier]int{},
	}
	target := servingTargetServices(t)
	for id, svc := range fidelity.Services {
		inTarget := target[id]
		for _, tier := range svc.Operations {
			f.tiers[tier]++
			f.totalKnown++
			if inTarget {
				f.targetTiers[tier]++
				f.targetTotalKnown++
			}
		}
	}
	f.compatTested = f.registered - len(loadCompatExclusions(t))
	return f
}

// loadCompatExclusions returns the registered services no boto3 test can reach.
//
// docs/coverage.md's fourth number is the fleet minus these. The set is a fact
// about botocore, so tests/compatibility owns it — this reads that file rather
// than keeping a Go copy, because a Go copy would drift the week botocore
// publishes a client and only the Python side noticed.
func loadCompatExclusions(t *testing.T) []string {
	t.Helper()

	path := filepath.Join(repoRoot(t), "tests", "compatibility", "exclusions.json")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read the compatibility exclusions: %v", err)
	}

	var doc struct {
		NoBoto3Client        map[string]string `json:"noBoto3Client"`
		UnreachableFromBoto3 map[string]string `json:"unreachableFromBoto3"`
	}
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("tests/compatibility/exclusions.json is not valid JSON: %v. "+
			"Both the compatibility suite and the published Compatibility-tested "+
			"figure are derived from it, so an unreadable file stops both rather "+
			"than quietly excluding nothing.", err)
	}

	var ids []string
	for id := range doc.NoBoto3Client {
		ids = append(ids, id)
	}
	for id := range doc.UnreachableFromBoto3 {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
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

	f := derivedFigures(t)
	serving, registeredOnly := f.serving, f.registeredOnly

	// Deliberately asserted here and not inside derivedFigures: this is a
	// guarantee about codegen being current, and a helper that checked it would
	// make the updater depend on it silently rather than fail on it.
	if got, want := f.registered, len(fidelity.Services); got != want {
		t.Errorf("the registry holds %d services and the fidelity manifest %d; "+
			"run `make codegen`", got, want)
	}

	if got, want := coverageRow(t, doc, "Registered"), f.registered; got != want {
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

	// The fourth number was the one figures_update_test.go wrote and nothing read
	// back. A figure the updater maintains and no gate asserts is maintained only
	// on the weeks the sync happens to run: an exclusion added on the Python side
	// would move the fleet, leave this page stating the old number, and every
	// check would stay green. It is asserted here so the round trip closes.
	if got, want := coverageRow(t, doc, "Compatibility-tested"), f.compatTested; got != want {
		t.Errorf("docs/coverage.md publishes %d compatibility-tested services; %d registered "+
			"minus the %d pinned in tests/compatibility/exclusions.json is %d. Adding an "+
			"exclusion lowers the published figure in the same commit — that is what this "+
			"gate is for.", got, f.registered, f.registered-f.compatTested, want)
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

	f := derivedFigures(t)
	counts, total := f.tiers, f.totalKnown

	for _, tier := range []fidelity.Tier{
		fidelity.TierHandVerified,
		fidelity.TierAutoCRUD,
		fidelity.TierUnimplemented,
	} {
		gotTarget, gotAll := tierRow(t, doc, string(tier))
		if want := f.targetTiers[tier]; gotTarget != want {
			t.Errorf("docs/coverage.md publishes %d %s operations inside the serving target, "+
				"the manifest holds %d", gotTarget, tier, want)
		}
		if want := counts[tier]; gotAll != want {
			t.Errorf("docs/coverage.md publishes %d %s operations across all registered "+
				"services, the manifest holds %d", gotAll, tier, want)
		}
	}

	published := totalKnownPattern.FindStringSubmatch(doc)
	if published == nil {
		t.Fatal("docs/coverage.md: the 'total known' row did not parse")
	}
	gotTarget, _ := strconv.Atoi(strings.ReplaceAll(published[1], ",", ""))
	gotAll, _ := strconv.Atoi(strings.ReplaceAll(published[2], ",", ""))
	if gotTarget != f.targetTotalKnown {
		t.Errorf("docs/coverage.md publishes %d known operations inside the serving target, "+
			"the manifest holds %d", gotTarget, f.targetTotalKnown)
	}
	if gotAll != total {
		t.Errorf("docs/coverage.md publishes %d known operations, the manifest holds %d", gotAll, total)
	}
}

// TestPublishedFidelityShareMatchesTheManifest gates the number the PRD called a
// regression and the measurement called a denominator.
//
// The hand-verified share over all 431 registered services is 23.6%, and over
// the 205-service serving target it is 36.2% — the same share it was before the
// long tail was registered. Publishing only the first invites reading a routing
// decision as a depth regression, and "offsetting" it costs roughly 2,450
// hand-written operations that nobody asked for. So both are published, and both
// are gated: a page that states one without the other, or states either wrongly,
// fails here.
//
// Asserted as rendered strings rather than as floats. The page shows one decimal
// place, so that is the precision the claim is made at, and comparing anything
// finer would fail on a digit no reader can see.
func TestPublishedFidelityShareMatchesTheManifest(t *testing.T) {
	raw, err := os.ReadFile(coveragePath)
	if err != nil {
		t.Fatalf("read the published coverage claim: %v", err)
	}

	matches := handVerifiedSharePattern.FindAllStringSubmatch(string(raw), -1)
	if len(matches) != 1 {
		t.Fatalf("docs/coverage.md: found %d hand-verified share rows, want exactly 1. "+
			"The two-denominator table is what this gate reads; if it was restructured, "+
			"move the pattern deliberately rather than letting the share stop being checked.",
			len(matches))
	}

	f := derivedFigures(t)
	for _, c := range []struct {
		column  int
		surface string
		hand    int
		total   int
	}{
		{1, "the serving target", f.targetTiers[fidelity.TierHandVerified], f.targetTotalKnown},
		{2, "all registered services", f.tiers[fidelity.TierHandVerified], f.totalKnown},
	} {
		want := formatTenths(shareTenths(c.hand, c.total))
		if got := matches[0][c.column]; got != want {
			t.Errorf("docs/coverage.md publishes a hand-verified share of %s%% over %s; "+
				"%d of %d operations is %s%%", got, c.surface, c.hand, c.total, want)
		}
	}
}

// longTailPattern and longTailHandVerifiedPattern match the two figures the
// paragraph under the tier table states and the table itself does not: how many
// operations the long tail brought with it, and how many of those are
// hand-written.
//
// Both are the difference between the table's two columns, so both move the week
// an operation is promoted anywhere outside the serving target. proseRequired
// exists for figures a tool cannot write; these two it can, so they are gated
// and rewritten like the cells above them rather than left as prose that is
// correct on the day it is typed.
//
// Anchored on a distinctive clause rather than pinned to the whole sentence: the
// wording stays free to change, and a reword that drops the anchor matches
// nothing — which fails here, rather than disabling the check in silence.
var (
	longTailPattern             = regexp.MustCompile(`with them ([\d,]+) operations`)
	longTailHandVerifiedPattern = regexp.MustCompile("([\\d,]+) are `hand-verified`")
)

// TestPublishedLongTailProseMatchesTheManifest gates the figures that live in
// the paragraph under the tier table rather than in it.
//
// The table's own cells have been gated since the two denominators were split.
// The paragraph explaining them was not, and it states the same arithmetic in
// words: "6,794 operations, of those 31 are hand-verified". A promotion inside
// the long tail moves both, the table follows the binary, and the sentence
// underneath keeps the old numbers while every gate stays green — which is the
// drift TestOtherDocsQuoteTheSameFigure was written for, one paragraph lower.
func TestPublishedLongTailProseMatchesTheManifest(t *testing.T) {
	raw, err := os.ReadFile(coveragePath)
	if err != nil {
		t.Fatalf("read the published coverage claim: %v", err)
	}
	doc := string(raw)

	f := derivedFigures(t)
	for _, c := range []struct {
		what    string
		pattern *regexp.Regexp
		want    int
	}{
		{"operations outside the serving target", longTailPattern,
			f.totalKnown - f.targetTotalKnown},
		{"of those that are hand-verified", longTailHandVerifiedPattern,
			f.tiers[fidelity.TierHandVerified] - f.targetTiers[fidelity.TierHandVerified]},
	} {
		matches := c.pattern.FindAllStringSubmatch(doc, -1)
		if len(matches) != 1 {
			t.Fatalf("docs/coverage.md: found %d statements of %q, want exactly 1. "+
				"The paragraph under the tier table is what this gate reads; if it was "+
				"reworded, move the pattern deliberately rather than letting the figure "+
				"stop being checked.", len(matches), c.what)
		}

		got, err := strconv.Atoi(strings.ReplaceAll(matches[0][1], ",", ""))
		if err != nil {
			t.Fatalf("docs/coverage.md: %q has an unreadable number %q", c.what, matches[0][1])
		}
		if got != c.want {
			t.Errorf("docs/coverage.md states %d %s; the two columns differ by %d",
				got, c.what, c.want)
		}
	}
}

// manifestPath is the second page that states the fidelity share. See
// docs/fidelity-manifest.md.
const manifestPath = "../../docs/fidelity-manifest.md"

// manifestSharePattern matches the share where docs/fidelity-manifest.md states
// it in a sentence:
//
//	`hand-verified` is 36.2% of the operations inside the [serving target](…)
//	and 23.6% of every operation DevCloud knows about
//
// The gap between the two figures spans a line break and a Markdown link, so it
// admits `.` where quotedFigurePattern does not — "coverage.md#the-target" is
// inside it. It still excludes digits and is still bounded, which is what stops
// a loose reader from becoming a loose writer: no other number can be captured,
// and a reword long enough to break the bound fails rather than matching wrongly.
var manifestSharePattern = regexp.MustCompile(
	"`hand-verified` is ([\\d.]+)% of the[^\\d]{0,80}?and ([\\d.]+)% of")

// TestFidelityManifestQuotesTheSameShare is TestOtherDocsQuoteTheSameFigure for
// the figure this PRD added.
//
// The share is gated on docs/coverage.md and restated here, and a restatement no
// gate reads is the exact shape of the drift that put "148 registered / 117
// serving" on both front pages for three milestones. One promotion in the long
// tail moves the share, the weekly sync rewrites coverage.md, and this page goes
// on publishing the old percentage unless something compares them.
func TestFidelityManifestQuotesTheSameShare(t *testing.T) {
	raw, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatalf("read the fidelity manifest page: %v", err)
	}

	matches := manifestSharePattern.FindAllStringSubmatch(string(raw), -1)
	if len(matches) != 1 {
		t.Fatalf("docs/fidelity-manifest.md: found %d statements of the hand-verified "+
			"share, want exactly 1. The page restates a gated figure; restore the "+
			"phrasing or move the pattern deliberately.", len(matches))
	}

	f := derivedFigures(t)
	for _, c := range []struct {
		column  int
		surface string
		hand    int
		total   int
	}{
		{1, "the serving target", f.targetTiers[fidelity.TierHandVerified], f.targetTotalKnown},
		{2, "all registered services", f.tiers[fidelity.TierHandVerified], f.totalKnown},
	} {
		want := formatTenths(shareTenths(c.hand, c.total))
		if got := matches[0][c.column]; got != want {
			t.Errorf("docs/fidelity-manifest.md states a hand-verified share of %s%% over %s; "+
				"docs/coverage.md publishes %s%%, and %d of %d operations is %s%%",
				got, c.surface, want, c.hand, c.total, want)
		}
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

	for _, id := range derivedFigures(t).registeredOnly {
		if !strings.Contains(doc, id) && !strings.Contains(doc, hyphenate(id)) {
			t.Errorf("%s serves nothing but docs/coverage.md never names it. "+
				"The page states which services serve nothing and why; a service that "+
				"joins them silently turns a true sentence into a false one.", id)
		}
	}
}

// quotedFigurePattern matches the registered/serving pair where the front pages
// state it in prose. Loose enough for both phrasings in use — "205 AWS services
// registered, 201 serving at least one operation" and "205 registered / 201
// serving" — because these are sentences, and pinning their wording would make
// every edit a test failure. Capture groups 1 and 2 are the two figures, and
// they are the only spans the updater moves.
//
// Every gap excludes digits and is bounded, which is the difference between a
// loose reader and a loose writer. A gate that mismatches fails a test; the
// updater writes through this same pattern unattended every Monday, and an
// unbounded gap let "3 of the 431 registered services need 2 serving tiers"
// match with the 3 and the 2 as its figures. Bounding the gaps costs nothing
// real — both live phrasings separate the two numbers by ", " or " / " — and a
// rewording that stops matching fails loudly, because
// TestOtherDocsQuoteTheSameFigure treats zero matches as a failure.
var quotedFigurePattern = regexp.MustCompile(`(?m)(\d+)[^.\n\d]{0,24}?registered\b[^.\n\d]{0,4}?(\d+)[^.\n\d]{0,4}?serving`)

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
	f := derivedFigures(t)
	registered, serving := f.registered, f.serving

	for _, path := range []string{"../../README.md", "../../docs/README.md"} {
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Errorf("read %s: %v", path, err)
			continue
		}

		matches := quotedFigurePattern.FindAllStringSubmatch(string(raw), -1)
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

// targetRowPattern matches one row of the two-axis table in
// docs/coverage.md#the-target:
//
//	| **Routing target** | **431 / 431 — met** | leak-zero; … |
//
// Only the first number in the value cell is captured. "431 / 431 — met" and
// "205 — met" are written for a reader; the gate reads the figure the reader
// sees rather than asking the table to be machine-shaped, which is the same
// trade tierRow makes. The rest of the cell is prose, so the updater rewrites
// group 1 and reports the remainder as a sentence a person owes.
func targetRowPattern(label string) *regexp.Regexp {
	return regexp.MustCompile(`(?m)^\|\s*\*?\*?` + regexp.QuoteMeta(label) +
		`\*?\*?\s*\|\s*\*?\*?([\d,]+)`)
}

// targetTableRow returns the figure the two-axis table publishes for label.
func targetTableRow(t *testing.T, doc, label string) int {
	t.Helper()

	matches := targetRowPattern(label).FindAllStringSubmatch(doc, -1)
	if len(matches) != 1 {
		t.Fatalf("docs/coverage.md: found %d target rows for %q, want exactly 1. "+
			"The two-axis table is what this gate reads; if it was restructured, "+
			"move the pattern deliberately rather than letting the target stop "+
			"being checked.", len(matches), label)
	}

	n, err := strconv.Atoi(strings.ReplaceAll(matches[0][1], ",", ""))
	if err != nil {
		t.Fatalf("docs/coverage.md: target row %q has an unreadable number %q", label, matches[0][1])
	}
	return n
}

// TestPublishedTargetTableMatchesTheBinary gates the target itself, which is the
// half of this page nothing read until now.
//
// The summary table at the top has been gated since Milestone 6, and the target
// table under #the-target has not — so the page once reached a state where it
// published 431 registered and, further down, called the target "205 services,
// not 431" and described those 431 as not targeted. Every number there was wrong
// and every gate was green.
//
// Two of the three numbers are derivable from the binary and are checked against
// it. The serving target is a decision, not a measurement — it is read from the
// page and used as the arithmetic the third number must satisfy, so the table
// cannot be internally inconsistent either.
func TestPublishedTargetTableMatchesTheBinary(t *testing.T) {
	raw, err := os.ReadFile(coveragePath)
	if err != nil {
		t.Fatalf("read the published coverage claim: %v", err)
	}
	doc := string(raw)

	registered := derivedFigures(t).registered

	if got := targetTableRow(t, doc, "Routing target"); got != registered {
		t.Errorf("docs/coverage.md publishes a routing target of %d services, the binary "+
			"registers %d. The routing target is every service AWS publishes, so these "+
			"move together or the leak-zero claim is no longer true.", got, registered)
	}

	serving := targetTableRow(t, doc, "Serving target")
	outside := targetTableRow(t, doc, "Registered and engine-served, outside the serving target")
	if got, want := outside, registered-serving; got != want {
		t.Errorf("docs/coverage.md publishes %d services outside the serving target; "+
			"%d registered minus a serving target of %d is %d. One of the three "+
			"numbers moved without the others.", got, registered, serving, want)
	}

	// The three numbers above are internally consistent whatever they say, which
	// is the half of the table this gate checked. The other half is that the
	// target is the same set the tier table divides by: the "Serving target"
	// column is counted over servingTargetServices, and nothing tied its size to
	// the number published here. AWS shipping a service the demand study never
	// sampled grows that set to 206 — deliberately, see servingTargetServices —
	// while this row keeps promising depth on 205, and every gate stays green.
	if got, want := len(servingTargetServices(t)), serving; got != want {
		t.Errorf("docs/coverage.md promises depth on %d services, but the tier table's "+
			"serving-target column is counted over %d. The registry moved against "+
			"docs/demand.md: either the target is a new number and this row moves with "+
			"it, or a service left the registry and the depth promise is no longer met.",
			want, got)
	}
}

// demandPath is the evidence behind the published target. See docs/demand.md.
const demandPath = "../../docs/demand.md"

// demandRow matches one row of the ranking table in docs/demand.md:
//
//	| `emr-serverless` | yes | yes | yes | 3 |
var demandRow = regexp.MustCompile("(?m)^\\|\\s*`([a-z0-9-]+)`\\s*\\|[^|]*\\|[^|]*\\|[^|]*\\|\\s*(\\d+)\\s*\\|")

// demandSupport reads the support column of one matched row.
//
// Both readers of docs/demand.md split on this number, in opposite directions —
// one takes support >= 2, the other takes the rest — so the two must read the
// cell the same way or a service ends up on neither side, or on both. One parse,
// used twice, is how they stay in step.
//
// The error is fatal rather than skipped. demandRow already requires digits, so
// a cell reading "n/a" drops the whole row before this is reached and only an
// overflowing number gets here; both call sites used to fold that into their
// `continue`, which read as though a malformed cell were routine. It is not: a
// row that goes missing moves a service across the published target rather than
// out of the table, which is why the two gates that notice — the pinned demand
// set and the serving-target denominator — are assertions and not filters.
func demandSupport(t *testing.T, row []string) int {
	t.Helper()

	support, err := strconv.Atoi(row[2])
	if err != nil {
		t.Fatalf("docs/demand.md: the support column for %q reads %q, which is not a "+
			"number this can use: %v", row[1], row[2], err)
	}
	return support
}

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
		if demandSupport(t, row) < 2 {
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

// servingTargetServices is the set the depth target promises: every registered
// service except the long tail the demand study found nobody building.
//
// The set is derived rather than pinned because pinning it would be a fourth
// place the 205 lives, and docs/demand.md already enumerates all 283 services
// that were missing when the study was sampled. The 226 with support < 2 are
// exactly the ones this PRD registered for routing alone; subtracting them from
// the registry leaves the 148 registered at sample time plus the 57-service
// demand set.
//
// A service AWS publishes after the study has no row here, so it joins the
// target set. That is deliberate: nobody has measured its demand either way,
// and the alternative — failing the weekly sync whenever AWS ships a service —
// buys nothing. See docs/coverage.md, which states the denominator literally.
func servingTargetServices(t *testing.T) map[string]bool {
	t.Helper()

	raw, err := os.ReadFile(demandPath)
	if err != nil {
		t.Fatalf("read the demand evidence: %v", err)
	}

	target := make(map[string]bool)
	for _, id := range plugin.DefaultRegistry.RegisteredServices() {
		target[id] = true
	}

	var unregistered []string
	for _, row := range demandRow.FindAllStringSubmatch(string(raw), -1) {
		if demandSupport(t, row) >= 2 {
			continue
		}
		// The same two vocabularies TestDemandSetIsRegistered joins: demand
		// names carry the SDK's punctuation, DevCloud service IDs have none.
		id := strings.ReplaceAll(row[1], "-", "")
		if !target[id] {
			unregistered = append(unregistered, row[1])
			continue
		}
		delete(target, id)
	}

	// Every one of the 226 is registered — that is this PRD's whole claim. A
	// name that no longer joins means either the leak-zero guarantee broke or
	// the two vocabularies drifted, and both make the published share wrong
	// rather than merely stale.
	if len(unregistered) > 0 {
		t.Fatalf("%d services the demand study found nobody building are not registered: %v. "+
			"The serving-target denominator is the registry minus exactly those, so it "+
			"cannot be derived while they are missing.", len(unregistered), unregistered)
	}
	return target
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
