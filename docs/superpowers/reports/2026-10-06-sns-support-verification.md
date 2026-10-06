# SNS support verification — 2026-10-06

Historical verification record: branch, index and authorization statements below
describe the implementation-time state. The user subsequently authorized a
commit and pull request; specifications, plans and local tracking CSVs remain
outside the published patch.

Approved spec and plan: `2026-10-06-sns-missing-operations-design.md` and
`2026-10-06-sns-missing-operations.md`. Native execution on current checkout
`feat/aws-operation-support`, HEAD a6923c975d540143347b6b524af64094fdc55d04.
All product/spec/plan changes remain unstaged and uncommitted.

## Scope and observed behavior

The six fixed SNS pairs are OptInPhoneNumber, PublishBatch, SetEndpointAttributes,
SetPlatformApplicationAttributes, SetSMSAttributes and VerifySMSSandboxPhoneNumber.
Thirteen companion CRUD paths now use the same canonical records. Publish,
CreateTopic and opt-out check/list share real persisted state and local delivery.

Migration 4 is additive/idempotent. Legacy JSON, identities and timestamps are
preserved with deletion markers; conflicting imports roll back together.
Actual generic Query's flattened attribute keys and PlatformEndpointArn are
normalized without discarding credentials or disabled endpoint state. Conflicting
attribute representations or ARN aliases roll back the whole import.
Missing-parent records remain retained. Imported VERIFIED does not invent OTP
authorization. Existing unknown metadata survives valid native partial setters.

Go tests observed behavioral RED before each implementation, followed by GREEN.
SDK tests inspect real SQS messages and the actual SQLite outbox OTP, including
invalid/reused codes, partial batch results, binary/text attributes and FIFO IDs.
SDK boundary failures for Unit wrappers and Name/Value attribute serialization
were reproduced and corrected. A raw-payload budget regression prevents delivery
when malformed/duplicate attributes previously hid an oversized batch.

## Verification evidence

Full logs are retained in ignored `.superpowers/sdd/2026-10-06-sns-missing-operations/`.

| Gate | Result | Log |
|---|---|---|
| `CGO_ENABLED=0 go test ./... -count=1` | exit 0, 119 tested packages | final-go-full.log |
| `CGO_ENABLED=1 go test -race ./internal/shared/crud ./internal/services/sns ./internal/services/sqs -count=1` | all pass | final-race.log |
| Pinned boto3 SNS + data fixture tests | 25 passed | task-6-sns-sdk.log |
| SQS/EventBridge/integration SDK files | 57 passed | task-6-linked-sdk.log |
| Full pinned compatibility suite and client tools | 1549 passed, 15 existing skips, 1 existing xfail | final-sdk-full.log |
| Required Lambda runtime marker with Docker, serial post-fix run | 13 passed, zero runtime skips | final-runtime-serial.log |
| Whole-fleet generation and fresh-output comparison | zero drift | task-7-codegen-drift.log |
| Published figure updater | pass | task-7-figures.log |
| Final frozen-set/log/branch/index evidence reconciliation | pass; Task 7 closed | task-7-tests.log |

Registered operation tiers are 4565 hand-verified / 11561 auto-crud / 3075
unimplemented, total 19201. Serving-target tiers are 4534 / 5883 / 1990, total
12407. These counts include six target promotions and thirteen companions.

The frozen 3085-pair completion set now has **10 verified / 3075 pending**,
including the four completed EventBridge pairs. Overall AWS support is incomplete.

## Final review

A fresh gpt-6-astra reviewer inspected the immutable SNS-only patch against the
starting working tree, preserving the preceding EventBridge work. BASE..HEAD is
empty because committing is not authorized. No Critical findings; two Important
findings were verified and fixed in one pass:

- Generic Query stored flat Attributes.entry.N.key/value, which the importer
  ignored. TestSNSLegacyGenericQueryAttributesSurviveSetterAndReopen observed
  credential loss before the fix and preserved getters/setters, unknown metadata,
  timestamps, exact raw JSON and reopening after it.
- Generic CRUD generated PlatformEndpointArn, which the importer did not select.
  TestSNSLegacyGenericEndpointARNRemainsAddressable observed NotFound before the
  fix and preserved stored identity, parent, account, Enabled=false, partial
  setter and reopened listing after it.

TestSNSLegacyQueryRepresentationConflictsRollBack observed both attribute and ARN
conflict failures before the fix and now proves zero partial imports. Behavioral
RED and GREEN are retained as final-import-red.log and final-import-green.log.
Full Go, race, pinned SDK and required runtime gates passed after the fixes.
The runtime run in parallel passed 12/13, but test_sqs_executes_handler observed an
invisible message still present at its fixed 0.2-second acknowledgement check.
The existing poller deletes only after the Lambda invocation response. Timing
contention remains an inference; rerunning the complete runtime group alone
passed 13/13 without changing code, tests, skips or expectations. The failed run
remains in final-runtime.log. No second reviewer or additional fix pass was used.

Deferred minor: SQS JSON receive returns all available FIFO identifiers when any
system-attribute list is supplied, even an empty list. A later focused change
should honor explicit requested names and All. Requested delivery data is present;
this finding affects selective response shape, not message delivery.

## Local limits and rulings

- Retain the ignored evidence workspace: there are no authorized commits from
  which to recover its record; cost is retained local scratch.
- Consolidate small store/provider tests in focused files while retaining all
  named behavioral cases; cost is that tests may later be split by ownership.
- Publish reports InternalError after attempting remaining subscriptions when
  one fails; callers that relied on always-success behavior must handle failures.
- SQS JSON receive includes binary attributes and requested FIFO identifiers,
  because preservation at send alone did not prove SDK-visible delivery; cost is
  additive receive fields.
- External push/carrier, IAM/CloudWatch, billing and cloud opt-in throttling stay
  excluded; users get local delivery and metadata. Cost if wrong: integrations
  requiring cloud side effects need further implementation.
- Durable SQS, background retry and rollback of completed sink writes stay
  excluded; existing queue lifecycle and documented fanout semantics stand. Cost
  if wrong: restart loss or duplicates require a separate delivery subsystem.
- Single local account/region stands as approved. Cost if wrong: multi-tenant
  isolation is not provided.
- Arbitrary legacy endpoint ARN paths are preserved; explicit parent and matching
  account establish linkage. Cost if wrong: consumers enforcing AWS ARN paths may
  reject preserved identifiers.
- Existing subscription filtering and SNS envelope behavior stand; this slice
  guarantees raw local SQS delivery. Cost if wrong: cloud filtering/envelope
  consumers require additional delivery work.

Delivery is local SQS; mobile/SMS credentials and settings are metadata. No
external push, carrier delivery, IAM/CloudWatch execution or billing is performed.
Sink writes cannot roll back if a later fanout/commit fails; standard retries can
duplicate deliveries, and there is no background retry worker. SQS queues retain
their existing in-memory lifecycle. OTPs are random, expire after ten minutes,
consume once and are retrieved from the actual local outbox.
