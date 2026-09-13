// SPDX-License-Identifier: Apache-2.0

package main

// This file is a tool, not a gate. The gates in coverage_test.go fail when
// docs/coverage.md disagrees with the binary; this writes the agreement they
// demand, so the weekly Smithy sync arrives with its arithmetic already done and
// a reviewer spends the review on whether the operations that moved should have.
//
// It lives beside the gates, in the same package, for one reason: the figures
// have one derivation (derivedFigures) and rewriting them from a separate
// binary would need its own copy of cmd/devcloud/imports.go's 431 blank imports
// to see the registry at all. A second copy of that list is a worse defect than
// anything this saves.
//
// It is switched by an environment variable rather than a test flag because
// `go test ./... -update-docs` fails in every other package with "flag provided
// but not defined". DEVCLOUD_STARTUP_BUDGET in budget_test.go sets the
// precedent.

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/skyoo2003/devcloud/internal/generated/fidelity"
)

// figureValue is one number this tool will rewrite, and what the PR body calls it.
type figureValue struct {
	label string
	want  int
}

// figureEdit is one published span this tool will rewrite, and where.
//
// pattern is always the same pattern the corresponding gate reads with, so a
// page restructure cannot leave the gate checking one cell and the tool writing
// another. figures maps one-to-one onto the pattern's capture groups, in order.
type figureEdit struct {
	path    string // repo-relative
	pattern *regexp.Regexp
	figures []figureValue

	// render writes the figure the way the target cell shows it. docs/coverage.md
	// thousands-separates operation counts, leaves service counts plain, and
	// shows the fidelity share to one decimal place — three renderings, and the
	// cell decides which, so the edit that owns the cell carries it.
	render func(int) string

	// everyMatch rewrites all matches rather than insisting on exactly one. The
	// front pages state the figure in a sentence, and a page is free to state it
	// twice; a table row that appears twice is a restructured table, and writing
	// into an ambiguous match is worse than failing.
	everyMatch bool
}

// figureChange is one span's before and after, for the table the PR body carries.
type figureChange struct {
	label  string
	before string
	after  string
}

// servingTarget reads the decided depth target off docs/coverage.md.
//
// It is the one row in the two-axis table the updater must not write:
// TestPublishedTargetTableMatchesTheBinary calls it "a decision, not a
// measurement" and uses it as the arithmetic the third number must satisfy, so
// writing it would make that gate tautological. It is read here for the same
// subtraction the gate performs, and for no other reason.
func servingTarget(t *testing.T, root string) int {
	t.Helper()

	raw, err := os.ReadFile(filepath.Join(root, "docs", "coverage.md"))
	if err != nil {
		t.Fatalf("read the published coverage claim: %v", err)
	}
	return targetTableRow(t, string(raw), "Serving target")
}

// publishedFigureEdits is the whole published surface this tool owns.
//
// What is absent is as deliberate as what is present. The serving target is a
// decision rather than a measurement, the protocol and runtime-cost tables are
// not derivable from the manifest at all, and docs/demand.md answers a different
// question. This writes numbers the binary already knows and nothing else.
func publishedFigureEdits(f figures, servingTarget int) []figureEdit {
	const (
		coverage  = "docs/coverage.md"
		manifest  = "docs/fidelity-manifest.md"
		readme    = "README.md"
		docsIndex = "docs/README.md"
	)

	// The front pages restate the same two figures, so their rows carry the file
	// name: three rows all labelled "Registered" would read as a table that had
	// repeated itself rather than as three pages that have to agree.
	pair := func(path string) []figureValue {
		return []figureValue{
			{fmt.Sprintf("Registered (%s)", path), f.registered},
			{fmt.Sprintf("Serving ≥1 operation (%s)", path), f.serving},
		}
	}

	// The share is stated on two pages now, so its rows carry the file name for
	// the same reason pair() does: two rows both labelled "hand-verified share"
	// read as a table that repeated itself rather than as two pages that have to
	// agree. The values are tenths of a percent — see shareTenths.
	shares := func(path string) []figureValue {
		return []figureValue{
			{fmt.Sprintf("hand-verified share, serving target (%s)", path),
				shareTenths(f.targetTiers[fidelity.TierHandVerified], f.targetTotalKnown)},
			{fmt.Sprintf("hand-verified share, all registered (%s)", path),
				shareTenths(f.tiers[fidelity.TierHandVerified], f.totalKnown)},
		}
	}

	tierEdit := func(tier fidelity.Tier) figureEdit {
		return figureEdit{path: coverage, pattern: tierRowPattern(string(tier)), render: separatedFigure,
			figures: []figureValue{
				{fmt.Sprintf("`%s` operations (serving target)", tier), f.targetTiers[tier]},
				{fmt.Sprintf("`%s` operations (all registered)", tier), f.tiers[tier]},
			}}
	}

	return []figureEdit{
		{path: coverage, pattern: coverageRowPattern("Registered"), render: plainFigure,
			figures: []figureValue{{"Registered", f.registered}}},
		{path: coverage, pattern: coverageRowPattern("Serving ≥1 operation"), render: plainFigure,
			figures: []figureValue{{"Serving ≥1 operation", f.serving}}},
		{path: coverage, pattern: coverageRowPattern("Registered-only"), render: plainFigure,
			figures: []figureValue{{"Registered-only", len(f.registeredOnly)}}},
		{path: coverage, pattern: coverageRowPattern("Compatibility-tested"), render: plainFigure,
			figures: []figureValue{{"Compatibility-tested", f.compatTested}}},

		tierEdit(fidelity.TierHandVerified),
		tierEdit(fidelity.TierAutoCRUD),
		tierEdit(fidelity.TierUnimplemented),
		{path: coverage, pattern: totalKnownPattern, render: separatedFigure,
			figures: []figureValue{
				{"total known operations (serving target)", f.targetTotalKnown},
				{"total known operations (all registered)", f.totalKnown},
			}},
		{path: coverage, pattern: handVerifiedSharePattern, render: formatTenths,
			figures: shares(coverage)},

		// The two figures the paragraph under that table states in words. They are
		// the difference between its columns, so they move with it — and prose the
		// sync leaves behind is the drift this whole tool exists to stop.
		{path: coverage, pattern: longTailPattern, render: separatedFigure,
			figures: []figureValue{
				{"operations outside the serving target", f.totalKnown - f.targetTotalKnown}}},
		{path: coverage, pattern: longTailHandVerifiedPattern, render: separatedFigure,
			figures: []figureValue{
				{"`hand-verified` operations outside the serving target",
					f.tiers[fidelity.TierHandVerified] - f.targetTiers[fidelity.TierHandVerified]}}},

		{path: coverage, pattern: targetRowPattern("Routing target"), render: separatedFigure,
			figures: []figureValue{{"Routing target", f.registered}}},
		{path: coverage, pattern: targetRowPattern("Registered and engine-served, outside the serving target"), render: separatedFigure,
			figures: []figureValue{{"Outside the serving target", f.registered - servingTarget}}},

		{path: manifest, pattern: manifestSharePattern, render: formatTenths,
			figures: shares(manifest)},

		{path: readme, pattern: quotedFigurePattern, everyMatch: true, render: plainFigure, figures: pair(readme)},
		{path: docsIndex, pattern: quotedFigurePattern, everyMatch: true, render: plainFigure, figures: pair(docsIndex)},
	}
}

// applyFigureEdits returns doc with every edit's figures rewritten, and what
// moved. File I/O stays at the edges so the round-trip self-check below can run
// without writing to the tree it is checking.
//
// Only the captured spans move. The surrounding `|`, `**` and prose are left
// exactly as they are, because the gates are deliberately loose about wording
// and rewriting a whole match would pin phrasing nothing asked to pin.
func applyFigureEdits(doc string, edits []figureEdit) (string, []figureChange, error) {
	var changes []figureChange

	for _, e := range edits {
		locs := e.pattern.FindAllStringSubmatchIndex(doc, -1)
		switch {
		case len(locs) == 0:
			return "", nil, fmt.Errorf("%s: nothing states %q in a form this tool can read. "+
				"The page was restructured; the gate that reads it uses the same pattern, so "+
				"move both deliberately rather than letting the figure stop being maintained",
				e.path, e.figures[0].label)
		case !e.everyMatch && len(locs) != 1:
			return "", nil, fmt.Errorf("%s: found %d rows for %q, want exactly 1. "+
				"An ambiguous match is not written into: writing the wrong cell is worse "+
				"than failing, so restructure the table deliberately or fix the pattern",
				e.path, len(locs), e.figures[0].label)
		}

		for _, m := range locs {
			if got, want := len(m)/2-1, len(e.figures); got != want {
				return "", nil, fmt.Errorf("%s: the pattern for %q captures %d spans and %d "+
					"figures were supplied. They are declared together and must stay in step",
					e.path, e.figures[0].label, got, want)
			}
			for i, fv := range e.figures {
				changes = append(changes, figureChange{
					label:  fv.label,
					before: doc[m[2*(i+1)]:m[2*(i+1)+1]],
					after:  e.render(fv.want),
				})
			}
		}

		// Splice the last span first: every index above came from the document as
		// it is now, and replacing from the front would invalidate the rest.
		for i := len(locs) - 1; i >= 0; i-- {
			m := locs[i]
			for g := len(e.figures) - 1; g >= 0; g-- {
				start, end := m[2*(g+1)], m[2*(g+1)+1]
				doc = doc[:start] + e.render(e.figures[g].want) + doc[end:]
			}
		}
	}

	return doc, changes, nil
}

// formatFigure renders a figure the way docs/coverage.md writes it.
//
// Operation counts are thousands-separated because the page is written for a
// reader, and tierRow strips the separators back out — so the two agree by
// construction. Service counts are not separated, because the summary table's
// own gate pattern accepts no comma.
func formatFigure(n int, separated bool) string {
	s := strconv.Itoa(n)
	if !separated {
		return s
	}
	// A depth target may be set above the fleet, and "outside the serving target"
	// is then negative. The sign is held aside rather than grouped with the
	// digits, which would otherwise render -123 as "-,123".
	sign := ""
	if strings.HasPrefix(s, "-") {
		sign, s = "-", s[1:]
	}
	for i := len(s) - 3; i > 0; i -= 3 {
		s = s[:i] + "," + s[i:]
	}
	return sign + s
}

// plainFigure and separatedFigure name the two renderings docs/coverage.md
// already used, so publishedFigureEdits reads as a list of cells rather than a
// list of booleans.
func plainFigure(n int) string     { return formatFigure(n, false) }
func separatedFigure(n int) string { return formatFigure(n, true) }

// formatTenths renders tenths of a percent as the page shows them: 362 -> "36.2".
//
// The share is carried as an integer so the gate and this tool compare the
// rendered digits rather than two floats that agree to a precision the page
// never displays. See shareTenths in coverage_test.go.
func formatTenths(n int) string {
	sign := ""
	if n < 0 {
		sign, n = "-", -n
	}
	return fmt.Sprintf("%s%d.%d", sign, n/10, n%10)
}

// renderFigureTable is what a reviewer reads instead of re-deriving.
//
// Every figure is listed, not only the ones that moved: "these four did not
// move" is half the answer a sync PR raises, and a table that silently omits
// them cannot give it. Shaped like scripts/model_churn.py's render — lead
// sentence in bold, blank line, table, em-dash for an empty cell — so the two
// halves of the body read as one.
func renderFigureTable(changes []figureChange) string {
	var b strings.Builder

	moved := 0
	for _, c := range changes {
		if c.before != c.after {
			moved++
		}
	}

	fmt.Fprintf(&b, "**%d of %d published figures moved.**\n\n", moved, len(changes))
	b.WriteString("| Figure | Before | After |\n")
	b.WriteString("|---|---|---|\n")
	for _, c := range changes {
		after := c.after
		if c.before == c.after {
			after = "—"
		}
		fmt.Fprintf(&b, "| %s | %s | %s |\n", c.label, c.before, after)
	}
	b.WriteString("\n")
	return b.String()
}

// proseRequired returns the changes a number cannot carry, one block each.
//
// The tool writes counts. It will not invent a sentence, and the two cases below
// are sentences: a service that has newly stopped serving anything, which
// docs/coverage.md must name and explain, and a registered count restated in
// prose no gate reads. Reporting them and exiting non-zero is the whole
// difference between automating the arithmetic and automating the judgement.
func proseRequired(f figures, doc string, changes []figureChange) []string {
	var blocks []string

	var unnamed []string
	for _, id := range f.registeredOnly {
		if !strings.Contains(doc, id) && !strings.Contains(doc, hyphenate(id)) {
			unnamed = append(unnamed, id)
		}
	}
	if len(unnamed) > 0 {
		var b strings.Builder
		b.WriteString("PROSE REQUIRED — the updater wrote the counts but cannot write the sentence:\n\n")
		for _, id := range unnamed {
			fmt.Fprintf(&b, "  `%s` now serves no operation. docs/coverage.md states which\n"+
				"  services serve nothing and why, and names each one. Add it under\n"+
				"  \"Why a registered service can serve nothing\" before merging.\n", id)
		}
		blocks = append(blocks, b.String())
	}

	for _, c := range changes {
		if c.label != "Registered" || c.before == c.after {
			continue
		}
		blocks = append(blocks, fmt.Sprintf(
			"PROSE REQUIRED — the registered count moved from %s to %s, and docs/coverage.md\n"+
				"restates it in two places no gate reads and this tool will not rewrite:\n\n"+
				"  - the blockquote under the summary table (\"Two targets, not one: routing is\n"+
				"    %s of %s, depth is …\")\n"+
				"  - the denominator in the Routing target cell, which now reads \"%s / %s\"\n\n"+
				"  Both are sentences about what the number means. Move them before merging.\n",
			c.before, c.after, c.before, c.before, c.after, c.before))
		break
	}

	return blocks
}

// TestUpdatePublishedFigures rewrites every derivable figure the docs publish.
//
// Inert unless DEVCLOUD_UPDATE_DOCS=1. A test suite that rewrites tracked files
// by default is a trap: `go test ./...` on any branch must read the docs and
// never write them, or a green run stops meaning the docs were right and starts
// meaning they were overwritten.
func TestUpdatePublishedFigures(t *testing.T) {
	if os.Getenv("DEVCLOUD_UPDATE_DOCS") != "1" {
		t.Skip("set DEVCLOUD_UPDATE_DOCS=1 to rewrite the published figures")
	}

	root := repoRoot(t)
	f := derivedFigures(t)

	var order []string
	byPath := map[string][]figureEdit{}
	for _, e := range publishedFigureEdits(f, servingTarget(t, root)) {
		if _, seen := byPath[e.path]; !seen {
			order = append(order, e.path)
		}
		byPath[e.path] = append(byPath[e.path], e)
	}

	var all []figureChange
	docs := map[string]string{}
	for _, rel := range order {
		full := filepath.Join(root, rel)
		raw, err := os.ReadFile(full)
		if err != nil {
			t.Fatalf("read %s: %v", rel, err)
		}

		out, changes, err := applyFigureEdits(string(raw), byPath[rel])
		if err != nil {
			t.Fatalf("%v", err)
		}
		all = append(all, changes...)
		docs[rel] = out

		// Written only when it differs: an unchanged sync must leave a clean
		// `git status`, or create-pull-request opens an empty PR every week.
		if out != string(raw) {
			if err := os.WriteFile(full, []byte(out), 0o644); err != nil {
				t.Fatalf("write %s: %v", rel, err)
			}
		}
	}

	// Printed rather than logged: the sync lifts this out of the step's stdout
	// into the PR body, and t.Logf would indent every line out of Markdown.
	fmt.Print(renderFigureTable(all))

	blocks := proseRequired(f, docs["docs/coverage.md"], all)
	for _, b := range blocks {
		fmt.Println(b)
	}
	if len(blocks) > 0 {
		t.Fatalf("%d published change(s) need a sentence this tool will not invent. "+
			"The counts are written; the prose above is not, and docs/coverage.md is "+
			"wrong until someone writes it. Do not relax the gate to merge past this.",
			len(blocks))
	}
}

// mangleFigure overwrites a pattern's captures with values, for the round trip below.
//
// It rewrites the whole match with strings.Replace rather than splicing by
// submatch index, so it shares no code with applyFigureEdits — a bug in the
// splice cannot cancel itself out across the round trip.
func mangleFigure(t *testing.T, doc string, pattern *regexp.Regexp, values ...string) string {
	t.Helper()

	m := pattern.FindStringSubmatch(doc)
	if m == nil {
		t.Fatalf("the pattern under test matches nothing in the live document")
	}

	mangled := m[0]
	for i, v := range values {
		mangled = strings.Replace(mangled, m[i+1], v, 1)
	}
	return strings.Replace(doc, m[0], mangled, 1)
}

// TestUpdaterRestoresAMangledFigure is the updater's own self-check, and it is
// always on.
//
// The published documents are the golden file, and they update themselves: a
// corrupted figure restored to what the binary says must reproduce the committed
// page byte for byte. Asserting on the whole file rather than on the three cells
// is the point — a rewrite that corrupts an unrelated byte is exactly the
// failure a cell-level assertion misses.
func TestUpdaterRestoresAMangledFigure(t *testing.T) {
	root := repoRoot(t)
	edits := publishedFigureEdits(derivedFigures(t), servingTarget(t, root))

	editsFor := func(rel string) []figureEdit {
		var out []figureEdit
		for _, e := range edits {
			if e.path == rel {
				out = append(out, e)
			}
		}
		return out
	}
	read := func(rel string) string {
		raw, err := os.ReadFile(filepath.Join(root, rel))
		if err != nil {
			t.Fatalf("read %s: %v", rel, err)
		}
		return string(raw)
	}
	restore := func(rel, doc string) string {
		out, _, err := applyFigureEdits(doc, editsFor(rel))
		if err != nil {
			t.Fatalf("%v", err)
		}
		return out
	}

	// The baseline is the page as the binary says it should read, not the page as
	// committed. They are the same on a clean tree and differ on exactly the sync
	// this tool exists for — and what is under test here is the round trip, not
	// whether the figures are current. TestPublishedCoverageMatchesTheBinary owns
	// that question; owning it twice made a stale page report "the thousands
	// separator does not survive the rewrite" about a row nothing had touched.
	coverage := restore("docs/coverage.md", read("docs/coverage.md"))

	// 1. A summary-table cell: the plain-integer shape, with prose either side.
	if got := restore("docs/coverage.md",
		mangleFigure(t, coverage, coverageRowPattern("Registered"), "999")); got != coverage {
		t.Error("a mangled **Registered** row was not restored to the committed page")
	}

	// 2. A tier row: the thousands-separated shape, both columns. Restoring
	//    "7"/"8" to "5,193"/"10,871" proves the separator survives the round trip
	//    and that the two denominators are not written into each other's cells.
	if got := restore("docs/coverage.md",
		mangleFigure(t, coverage, tierRowPattern(string(fidelity.TierAutoCRUD)), "7", "8")); got != coverage {
		t.Error("a mangled `auto-crud` tier row was not restored, so the thousands " +
			"separator does not survive the rewrite")
	}

	// 3. A target row: a partial-cell rewrite. Only the first number is captured,
	//    so "/ 431 — met" must come back untouched rather than be swallowed.
	if got := restore("docs/coverage.md",
		mangleFigure(t, coverage, targetRowPattern("Routing target"), "12")); got != coverage {
		t.Error("a mangled Routing target row was not restored, or the rewrite consumed " +
			"the rest of the cell")
	}

	// 4. Idempotence: a page the updater has already written must come back
	//    untouched and report no movement. An updater that rewrites a tree it just
	//    wrote opens an empty PR every week.
	out, changes, err := applyFigureEdits(coverage, editsFor("docs/coverage.md"))
	if err != nil {
		t.Fatalf("%v", err)
	}
	if out != coverage {
		t.Error("applyFigureEdits is not idempotent: a second pass moved a figure the " +
			"first pass had just written, so an unchanged sync would not leave a clean " +
			"git status")
	}
	for _, c := range changes {
		if c.before != c.after {
			t.Errorf("%s reported as moving from %s to %s on a page already holding "+
				"the derived figures", c.label, c.before, c.after)
		}
	}

	// 5. The README pair: two captures in one loose prose match. Both spans move
	//    and the sentence around them does not.
	for _, rel := range []string{"README.md", "docs/README.md"} {
		doc := restore(rel, read(rel))
		if got := restore(rel,
			mangleFigure(t, doc, quotedFigurePattern, "1", "2")); got != doc {
			t.Errorf("%s: a mangled figure pair was not restored, or the wording around "+
				"it moved with the numbers", rel)
		}
	}

	// 6. The share row: one decimal place, and a percent sign the updater must
	//    leave where it is. Only the digits are captured, so "36.2%" coming back
	//    as "36.2" would mean the rewrite swallowed the unit.
	if got := restore("docs/coverage.md",
		mangleFigure(t, coverage, handVerifiedSharePattern, "9.9", "8.8")); got != coverage {
		t.Error("a mangled hand-verified share row was not restored, or the rewrite " +
			"consumed the percent sign")
	}

	// 7. The total row, both columns: the same shape as case 2 but bold, which is
	//    a different pattern and so a different splice.
	if got := restore("docs/coverage.md",
		mangleFigure(t, coverage, totalKnownPattern, "1", "2")); got != coverage {
		t.Error("a mangled **total known** row was not restored in both denominators")
	}

	// 8 and 9. The two figures the paragraph under the tier table states in a
	//    sentence. Splicing into prose is where a rewrite is most likely to eat a
	//    neighbouring word, and the whole-file comparison is what catches it.
	for _, p := range []*regexp.Regexp{longTailPattern, longTailHandVerifiedPattern} {
		if got := restore("docs/coverage.md", mangleFigure(t, coverage, p, "3")); got != coverage {
			t.Error("a mangled long-tail figure was not restored, so the sentence around " +
				"it does not survive the rewrite")
		}
	}

	// 10. The other page. docs/fidelity-manifest.md restates the share in prose,
	//     across a line break and a Markdown link — the loosest span this tool
	//     writes into, and the one where an over-wide match would be invisible to
	//     a cell-level assertion.
	manifest := restore("docs/fidelity-manifest.md", read("docs/fidelity-manifest.md"))
	if got := restore("docs/fidelity-manifest.md",
		mangleFigure(t, manifest, manifestSharePattern, "9.9", "8.8")); got != manifest {
		t.Error("a mangled share in docs/fidelity-manifest.md was not restored, or the " +
			"rewrite reached past the two figures it owns")
	}
}

// TestQuotedFigurePatternReadsThePhrasingsInUse pins what the loose pattern is
// allowed to be loose about.
//
// It reads sentences, so it must survive rewording — and it is also written
// through, so it must not reach into a sentence that merely mentions the words.
// Both halves are asserted here rather than only on the live pages: the live
// pages happen to be unambiguous today, and the guarantee is about tomorrow's.
func TestQuotedFigurePatternReadsThePhrasingsInUse(t *testing.T) {
	for _, tc := range []struct {
		name           string
		text           string
		want           []string // nil means "must not match"
		whyItMustMatch string
	}{
		{
			name:           "README prose",
			text:           "- **431 AWS services registered, 426 serving at least one operation** — every",
			want:           []string{"431", "426"},
			whyItMustMatch: "this is the sentence README.md states today",
		},
		{
			name:           "docs index table cell",
			text:           "| [Coverage](coverage.md) | 431 registered / 426 serving — the routing and depth targets |",
			want:           []string{"431", "426"},
			whyItMustMatch: "this is the cell docs/README.md states today",
		},
		{
			name: "a sentence that only mentions the figures",
			text: "Only 3 of the 431 registered services need 2 serving tiers.",
			want: nil,
		},
		{
			name: "a sentence whose second number is unrelated",
			text: "All 431 services are registered, and the 3 named below serve nothing, so 12 serving tiers exist.",
			want: nil,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := quotedFigurePattern.FindStringSubmatch(tc.text)
			if tc.want == nil {
				if m != nil {
					t.Errorf("matched %q in a sentence the updater must not write into. "+
						"A gate may mismatch and only fail a test; the updater writes through "+
						"this pattern every Monday, unattended.", m[0])
				}
				return
			}
			if m == nil {
				t.Fatalf("matched nothing, but %s. Zero matches disables "+
					"TestOtherDocsQuoteTheSameFigure in silence.", tc.whyItMustMatch)
			}
			if got := []string{m[1], m[2]}; got[0] != tc.want[0] || got[1] != tc.want[1] {
				t.Errorf("captured %v, want %v", got, tc.want)
			}
		})
	}
}

// TestUpdaterLeavesUnrelatedSentencesAlone is the same guarantee one layer up.
//
// Refusing to match and refusing to write are both acceptable answers; producing
// a document whose prose moved is not.
func TestUpdaterLeavesUnrelatedSentencesAlone(t *testing.T) {
	const doc = "Only 3 of the 431 registered services need 2 serving tiers.\n"

	out, _, err := applyFigureEdits(doc, []figureEdit{{
		path:       "README.md",
		pattern:    quotedFigurePattern,
		everyMatch: true,
		render:     plainFigure,
		figures:    []figureValue{{"Registered", 431}, {"Serving ≥1 operation", 426}},
	}})
	if err != nil {
		return // refusing an unreadable page is the documented, correct outcome
	}
	if out != doc {
		t.Errorf("the updater rewrote a sentence that only mentions the figures:\n"+
			"  before: %s   after: %s", doc, out)
	}
}

func TestFormatFigureWritesTheFigureTheDocsShow(t *testing.T) {
	for _, tc := range []struct {
		n         int
		separated bool
		want      string
	}{
		{431, false, "431"},
		{10871, true, "10,871"},
		{19201, true, "19,201"},
		{999, true, "999"},
		{1000, true, "1,000"},
		{0, true, "0"},
		// A depth target may be set above the fleet, and "outside the serving
		// target" is then negative. The separator must not splice into the sign.
		{-123, true, "-123"},
		{-1234, true, "-1,234"},
	} {
		if got := formatFigure(tc.n, tc.separated); got != tc.want {
			t.Errorf("formatFigure(%d, %v) = %q, want %q", tc.n, tc.separated, got, tc.want)
		}
	}
}

// TestShareRendersTheFigureThePageShows pins the arithmetic behind the one
// figure on the page that is a ratio rather than a count.
//
// The two live cases are asserted by name: they are the numbers the PRD called a
// regression and a target, and truncating instead of rounding turns 23.6% into
// 23.5% — a digit the reader sees and no float tolerance would catch.
func TestShareRendersTheFigureThePageShows(t *testing.T) {
	for _, tc := range []struct {
		name        string
		hand, total int
		want        string
	}{
		{"the serving target, as published", 4497, 12407, "36.2"},
		{"all registered services, as published", 4528, 19201, "23.6"},
		{"rounds half up rather than truncating", 1, 8, "12.5"},
		{"a surface with nothing hand-verified", 0, 100, "0.0"},
		{"a fully hand-verified surface", 100, 100, "100.0"},
		{"an empty surface does not divide by zero", 0, 0, "0.0"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := formatTenths(shareTenths(tc.hand, tc.total)); got != tc.want {
				t.Errorf("share of %d/%d = %q, want %q", tc.hand, tc.total, got, tc.want)
			}
		})
	}
}

// TestProseRequiredNamesWhatANumberCannotCarry covers the branch that is the
// whole difference between automating the arithmetic and automating the
// judgement — and that TestUpdatePublishedFigures, being env-gated, never runs
// under `go test ./...`.
func TestProseRequiredNamesWhatANumberCannotCarry(t *testing.T) {
	t.Run("a newly silent service the page does not name", func(t *testing.T) {
		f := figures{registeredOnly: []string{"examplesvc"}}
		blocks := proseRequired(f, "a page that names nothing", nil)
		if len(blocks) != 1 {
			t.Fatalf("got %d blocks, want 1: a service that serves nothing and is not "+
				"named is exactly the sentence the tool refuses to invent", len(blocks))
		}
		if !strings.Contains(blocks[0], "examplesvc") {
			t.Errorf("the block never names the service:\n%s", blocks[0])
		}
	})

	t.Run("a service the page already names", func(t *testing.T) {
		f := figures{registeredOnly: []string{"examplesvc"}}
		if blocks := proseRequired(f, "…the examplesvc service serves nothing because…", nil); len(blocks) != 0 {
			t.Errorf("got %d blocks, want 0", len(blocks))
		}
	})

	t.Run("a service named in the page's hyphenated spelling", func(t *testing.T) {
		f := figures{registeredOnly: []string{"cloudfrontkeyvaluestore"}}
		doc := "…`" + hyphenate("cloudfrontkeyvaluestore") + "` serves nothing because…"
		if blocks := proseRequired(f, doc, nil); len(blocks) != 0 {
			t.Errorf("got %d blocks, want 0; the docs write IDs hyphenated:\n%s", len(blocks), doc)
		}
	})

	t.Run("a registered count that moved", func(t *testing.T) {
		changes := []figureChange{{label: "Registered", before: "431", after: "432"}}
		blocks := proseRequired(figures{}, "", changes)
		if len(blocks) != 1 {
			t.Fatalf("got %d blocks, want 1", len(blocks))
		}
		for _, want := range []string{"431", "432", "blockquote", "Routing target"} {
			if !strings.Contains(blocks[0], want) {
				t.Errorf("the block never mentions %q:\n%s", want, blocks[0])
			}
		}
	})

	t.Run("a registered count that did not move", func(t *testing.T) {
		changes := []figureChange{{label: "Registered", before: "431", after: "431"}}
		if blocks := proseRequired(figures{}, "", changes); len(blocks) != 0 {
			t.Errorf("got %d blocks, want 0: an unchanged figure owes no sentence", len(blocks))
		}
	})
}

// TestRenderFigureTableListsEveryFigure pins the half of the answer a table that
// only showed movement could not give: "these four did not move".
func TestRenderFigureTableListsEveryFigure(t *testing.T) {
	out := renderFigureTable([]figureChange{
		{label: "Registered", before: "431", after: "432"},
		{label: "Serving ≥1 operation", before: "426", after: "426"},
	})

	if !strings.Contains(out, "**1 of 2 published figures moved.**") {
		t.Errorf("the lead sentence does not count the movement:\n%s", out)
	}
	if !strings.Contains(out, "| Registered | 431 | 432 |") {
		t.Errorf("a figure that moved is not shown moving:\n%s", out)
	}
	if !strings.Contains(out, "| Serving ≥1 operation | 426 | — |") {
		t.Errorf("a figure that held is not shown holding with an em-dash:\n%s", out)
	}
}
