// SPDX-License-Identifier: Apache-2.0

package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

// The weekly Smithy sync is the only mechanism that surfaces upstream model
// churn, and its output is a pull request. These tests gate the shape of that
// workflow for the same reason coverage_test.go gates docs/coverage.md: the
// asset is not Go, but a silent regression in it is invisible until the week it
// matters.

// syncStep is the subset of a GitHub Actions step these tests read. `with`
// values are not all strings — `delete-branch: true` is a bool — so the map is
// typed loosely and read as text only where it is read at all.
type syncStep struct {
	Name            string         `yaml:"name"`
	ID              string         `yaml:"id"`
	Uses            string         `yaml:"uses"`
	Run             string         `yaml:"run"`
	If              string         `yaml:"if"`
	ContinueOnError bool           `yaml:"continue-on-error"`
	With            map[string]any `yaml:"with"`
}

type syncWorkflow struct {
	Jobs map[string]struct {
		Steps []syncStep `yaml:"steps"`
	} `yaml:"jobs"`
}

// syncSteps returns the steps of the sync job in file order.
func syncSteps(t *testing.T) []syncStep {
	t.Helper()

	path := filepath.Join(repoRoot(t), ".github", "workflows", "smithy-sync.yml")
	raw, err := os.ReadFile(path)
	require.NoError(t, err)

	var wf syncWorkflow
	require.NoError(t, yaml.Unmarshal(raw, &wf))
	require.Len(t, wf.Jobs, 1, "smithy-sync.yml is expected to hold exactly one job")

	for _, job := range wf.Jobs {
		require.NotEmpty(t, job.Steps, "the sync job has no steps")
		return job.Steps
	}
	return nil
}

// findStep returns the index of the first step matching pred, or -1.
func findStep(steps []syncStep, pred func(syncStep) bool) int {
	for i, s := range steps {
		if pred(s) {
			return i
		}
	}
	return -1
}

func runsGoTest(s syncStep) bool { return strings.Contains(s.Run, "go test") }

func opensPullRequest(s syncStep) bool { return strings.Contains(s.Uses, "create-pull-request") }

// prBodyText returns the text that becomes the pull request body.
//
// create-pull-request takes the body either inline as `body` or from a file as
// `body-path`. Both are legitimate, and which one the workflow uses is not a
// guarantee worth pinning — what the body *says* is. So a `body-path` resolves
// to the shell of every step that writes to that path, which is where the
// content actually comes from.
func prBodyText(t *testing.T, steps []syncStep) string {
	t.Helper()

	prIdx := findStep(steps, opensPullRequest)
	require.NotEqual(t, -1, prIdx, "no step opens a pull request")
	with := steps[prIdx].With

	if body, ok := with["body"].(string); ok {
		return body
	}

	path, ok := with["body-path"].(string)
	require.True(t, ok, "the create-pull-request step supplies neither body nor body-path")

	var b strings.Builder
	for _, s := range steps[:prIdx] {
		if strings.Contains(s.Run, path) {
			b.WriteString(s.Run)
		}
	}
	require.NotEmpty(t, b.String(),
		"body-path is %q but no earlier step writes to it, so the PR body is empty", path)
	return b.String()
}

// TestSyncOpensAPullRequestEvenWhenTestsFail is the reproducer for the defect
// that made the sustaining cost unmeasurable.
//
// A step that fails ends the job, and every step after it is skipped, unless
// either the failing step is marked continue-on-error or the later step's `if`
// re-enables it with always(). The sync runs the full Go suite — which includes
// the published-figure gate in coverage_test.go — before it opens the PR. An
// upstream model that gains a single operation moves the manifest, fails that
// gate, and takes the PR with it. The refreshed models are then discarded with
// the runner, so the one change worth reviewing is the one nobody ever sees.
//
// The rule asserted here is GitHub Actions' own: the PR step must be reachable
// from a failed test step. How that is arranged — continue-on-error on the test
// or always() on the PR — is left to the workflow.
func TestSyncOpensAPullRequestEvenWhenTestsFail(t *testing.T) {
	steps := syncSteps(t)

	testIdx := findStep(steps, runsGoTest)
	require.NotEqual(t, -1, testIdx,
		"no step runs 'go test'; if the sync stopped testing, this gate is reading the wrong thing")

	prIdx := findStep(steps, opensPullRequest)
	require.NotEqual(t, -1, prIdx, "no step opens a pull request")
	require.Less(t, testIdx, prIdx,
		"the test step is expected to run before the PR step; reordering them changes what this gate means")

	reachable := steps[testIdx].ContinueOnError || strings.Contains(steps[prIdx].If, "always()")
	assert.True(t, reachable,
		"a failing test step ends the job and the PR is never opened, so the refreshed models "+
			"are discarded with the runner. Mark the test step continue-on-error, or gate the "+
			"PR step with always(), so a red sync still leaves a reviewable PR.")
}

// TestSyncStillFailsWhenTestsFail is the other half of the fix, and the reason
// continue-on-error is not sufficient on its own.
//
// Marking the test step continue-on-error makes the PR reachable and makes the
// job green — a weekly cron that reports success no matter what upstream did.
// That is the same silent no-op download-smithy-models.sh was fixed to stop
// doing, traded for the one this milestone removes. So a workflow that swallows
// the test failure must re-raise it after the PR exists.
func TestSyncStillFailsWhenTestsFail(t *testing.T) {
	steps := syncSteps(t)

	testIdx := findStep(steps, runsGoTest)
	require.NotEqual(t, -1, testIdx)

	if !steps[testIdx].ContinueOnError {
		t.Skip("the test step is not continue-on-error, so its failure already fails the job")
	}
	require.NotEmpty(t, steps[testIdx].ID,
		"a swallowed failure cannot be re-raised without an id to read it from")

	prIdx := findStep(steps, opensPullRequest)
	require.NotEqual(t, -1, prIdx, "no step opens a pull request")

	outcome := "steps." + steps[testIdx].ID + ".outcome"
	reraised := findStep(steps[prIdx+1:], func(s syncStep) bool {
		return strings.Contains(s.If, outcome) && strings.Contains(s.Run, "exit 1")
	})
	assert.NotEqual(t, -1, reraised,
		"the test failure is swallowed by continue-on-error and never re-raised, so the weekly "+
			"cron reports success regardless of what upstream changed. Add a step after the PR "+
			"that reads "+outcome+" and exits non-zero.")
}

// TestSyncPullRequestBodyReportsTheTestResult keeps the previous guarantee
// honest. Opening a PR whose tests failed is only an improvement if the body
// says so; a red PR that reads like a green one invites a merge rather than a
// review.
func TestSyncPullRequestBodyReportsTheTestResult(t *testing.T) {
	steps := syncSteps(t)

	testIdx := findStep(steps, runsGoTest)
	require.NotEqual(t, -1, testIdx)
	require.NotEmpty(t, steps[testIdx].ID,
		"the test step needs an id before its result can be quoted in the PR body")

	assert.Contains(t, prBodyText(t, steps), "steps."+steps[testIdx].ID,
		"the PR body must state the test result, so a reviewer sees a red sync as red")
}

// TestSyncPullRequestBodySummarisesTheChurn is the guarantee that makes the PR
// reviewable rather than merely present.
//
// The sync regenerates from all 194 models at once, so its diff is whole-tree
// whether upstream moved one model or ninety — measured on 2026-09-06, a real
// refresh changed 93 models and 134 generated files. "Please review" over that
// is not an action anyone performs. scripts/model_churn.py reduces it to the
// question a reviewer has, which operations moved, and the body must carry that
// answer or the reviewer is back to reading the regeneration.
func TestSyncPullRequestBodySummarisesTheChurn(t *testing.T) {
	steps := syncSteps(t)

	churnIdx := findStep(steps, func(s syncStep) bool {
		return strings.Contains(s.Run, "model_churn.py")
	})
	require.NotEqual(t, -1, churnIdx,
		"no step runs scripts/model_churn.py, so the PR cannot say which operations moved")

	prIdx := findStep(steps, opensPullRequest)
	require.Less(t, churnIdx, prIdx, "the churn summary must be produced before the PR is opened")

	assert.Contains(t, prBodyText(t, steps), "model_churn",
		"the churn summary is produced but never reaches the PR body")
}
