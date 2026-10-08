# EventBridge support verification

Historical verification record: branch, index and authorization statements below
describe the implementation-time state. The user subsequently authorized a
commit and pull request; specifications, plans and local tracking CSVs remain
outside the published patch.

Date: 2026-10-06. Branch: `feat/aws-operation-support` in the current checkout.
Base and HEAD: `a6923c975d540143347b6b524af64094fdc55d04`.
Changes are uncommitted; the index is empty. Specs and plans remain unstaged.

## Implemented slice

Four frozen targets: ActivateEventSource, DeactivateEventSource,
DeauthorizeConnection and RemovePermission.

Fourteen promoted companions: CreatePartnerEventSource, DeletePartnerEventSource,
DescribePartnerEventSource, ListPartnerEventSources, ListPartnerEventSourceAccounts,
DescribeEventSource, ListEventSources, PutPartnerEvents, CreateConnection,
DescribeConnection, ListConnections, UpdateConnection, DeleteConnection,
PutPermission. Existing CreateEventBus, DeleteEventBus and DescribeEventBus use
canonical source and policy state.

The fixed completion CSV contains **4 verified / 3,081 pending / 3,085 total**.
Companions do not increase the fixed completion numerator.

## Final gates

| Command | Result |
|---|---|
| `CGO_ENABLED=0 go test ./... -count=1` | exit 0 |
| `CGO_ENABLED=0` and `CGO_ENABLED=1`, `go test -race ./internal/shared/crud ./internal/services/eventbridge -count=1` | both exit 0 |
| `CGO_ENABLED=0 go build -o dist/devcloud ./cmd/devcloud` | exit 0 |
| Pinned Python, `DEVCLOUD_BIN=dist/devcloud`, EventBridge compatibility file | 25 passed |
| Pinned Python, `DEVCLOUD_COMPAT_TOOLS=/tmp/devcloud-phase1-tools`, full compatibility suite | 1,543 passed, 15 existing skips, 1 existing xfail |
| Pinned Python, `DEVCLOUD_LAMBDA_RUNTIME_TESTS=1`, `-m lambda_runtime` | 13 passed, no runtime skip |
| Full-fleet generator output to a temporary directory, `diff -qr` against `internal/generated` | exit 0; no drift |
| `git diff --check` | exit 0 |

Pinned Python: `/tmp/devcloud-phase1-venv/bin/python`, using repository requirement
versions. Full test output lives in the ignored EventBridge execution workspace.
Runtime tests execute actual Docker handlers and local delivery paths.

Measured registered tiers: hand-verified 4,546, auto-crud 11,574,
unimplemented 3,081, total 19,201. Registration remains 431 services.

## Review and fixes

One fresh, read-only `gpt-6-astra` review found three Important issues and no
Critical or Minor issues. All three have observed failing regressions and fixes:

1. Interrupted initialization after migration 1001 could skip tag migration 1000.
   `TestMigrationInterruptedBeforeTagsRetry` reproduced missing `resource_tags`;
   combined migration ordering now uses a detached sorted list and retry succeeds.
2. Complete legacy statement-form permissions and default-bus Policy documents
   were not imported. `TestLegacyImportStatementPermissionsAndDefaultBus`,
   `TestLegacyImportConflictingPermissionsRollback` and
   `TestLegacyImportCanonicalPolicyWins` failed before normalization and merge
   were corrected. Reopening and RemovePermission preserve deletion markers.
3. Model-valid partial credential and HTTP parameter updates were rejected.
   `TestConnectionOperationPartialAuthUpdates` and the SDK
   `test_connection_partial_auth_update` failed before detached structured merging
   was added. Omitted fields remain, secret values stay redacted, and cleared
   credentials cannot be reauthorized with an incomplete credential patch.

The planned concurrency evidence also includes
`TestPartnerSourceConcurrentLifecycle`, covering source activation versus bus
deletion and source deletion versus bus binding. Final whole-suite and race gates
passed after these changes.

## Remaining scope

AWS-wide support is incomplete: 3,081 original targets remain pending. The next
SNS six-target spec is prepared for review. EventBridge local limits are documented
in `docs/services/eventbridge.md`; external OAuth, IAM evaluation, partner expiry,
reverse synchronization for downgraded binaries and external API destinations
remain outside this approved slice.

Current-directory branch execution, uncommitted patch review, preserved local
ledger and deferred release-note integration follow the user's scope and are
recorded in the ignored execution ledger. No release, push, merge or PR was made.
