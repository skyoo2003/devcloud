# S3 UploadPartCopy verification — 2026-10-06

Historical verification record: branch, index and authorization statements below
describe the implementation-time state. The user subsequently authorized a
commit and pull request; specifications, plans and local tracking CSVs remain
outside the published patch.

Short in-chat design approved 2026-10-06; bounded Native execution in current
feat/aws-operation-support checkout. HEAD remains
a6923c975d540143347b6b524af64094fdc55d04; no stage/commit/push, no spec/plan document.
Earlier EventBridge/SNS changes are preserved.

## Behavior and evidence

UploadPartCopy copies actual local object bytes into the existing multipart
storage, with CopyPartResult, quoted MD5 ETag and LastModified. Full/range copying,
once-decoded Unicode/plus/percent/question/hash keys, condition precedence,
quoted/unquoted source ETags, part range 1–10000, exact upload destination/account,
expected owners, missing sources and unsupported source versions/encryption are
exercised through real Go/provider and boto3/gateway paths. ListParts and completed
objects read the same copied bytes. Source objects remain unchanged.

Go RED reproduced the previous empty-body upload and false successful validation;
GREEN uses a dedicated handler. Metadata failure restores previous part bytes;
reopening and completion verify durable data. A sparse source larger than 5 GiB
proves small-range reads do not read the whole source. Full/copied part writes
share serialized atomic file replacement and metadata-error rollback.

No new subsystem/schema/configuration/dependency was introduced. Version selectors,
access points and encryption headers explicitly return NotImplemented. Existing
single-account storage, metadata-only versioning/policies and filesystem key
mapping apply. Selected parts are buffered; filesystem and SQLite updates are not
a crash-atomic transaction, and persistent filesystem failure may prevent rollback.

## Gates

Logs are retained in ignored .superpowers/sdd/2026-10-06-s3-upload-part-copy/.

| Gate | Result | Log |
|---|---|---|
| Focused S3 Go after review fixes | pass | final-go-green.log |
| S3 race after review fixes | pass | final-race.log |
| New and existing S3 SDK files | 26 passed | sdk-green.log |
| Full Go after review fixes | exit 0, 119 tested packages | final-go-full.log |
| Whole-fleet generation / post-fix fresh comparison | zero drift | codegen.log / final-codegen-drift.log |
| Published figure update | pass | figures.log |
| Full pinned SDK/client tools after review fixes | 1552 passed, 15 existing skips, 1 existing xfail | final-sdk-full.log |
| Required Docker runtime, serial post-fix run | 13 passed, zero runtime skips | final-runtime.log |

One source operation is promoted: registered hand 4566 / auto 11561 /
unimplemented 3074 (19201 total), serving 4535 / 5883 / 1989 (12407 total).
The frozen completion CSV has **11 verified / 3074 pending**, exactly 3085 unique
pairs matching its original inventory. Only s3/UploadPartCopy was newly promoted;
the preceding ten verified EventBridge/SNS rows are preserved. Overall AWS support
remains incomplete. Index and HEAD remain unchanged.

## Final review

A fresh read-only gpt-6-astra reviewer inspected the immutable six-file S3-only
delta against its recorded starting tree: Critical 0, Important 3, Minor 0.
All three Important findings were confirmed and fixed in one grouped pass:

1. Completion could publish a replacement part whose metadata insert failed.
   TestUploadPartCopyFailureCannotLeakIntoConcurrentCompletion used real SQLite
   write contention and an aborting trigger: RED completed 2345 instead of keep;
   GREEN lifecycle locking consumes only the restored committed bytes.
   TestUploadPartCopyCannotCreateOrphanPartAfterAbort observed an orphan insertion
   before locked revalidation and no new part afterward.
2. Explicit x-id=UploadPartCopy could fall through to CopyObject, UploadPart or
   CreateBucket when required fields were missing.
   TestUploadPartCopyRejectsExplicitMalformedRequestsWithoutMutation observed
   wrong successful responses before early dispatch/required-field checks and
   now verifies original object and part bytes remain unchanged.
3. Source conditions could authorize old metadata with new source bytes.
   TestUploadPartCopyConditionUsesCommittedSourceSnapshot used a real concurrent
   PutObject and SQLite writer: RED success under the old ETag; GREEN 412 with the
   original part preserved. TestUploadPartCopyConditionAfterRejectedSourceOverwrite
   observed rejected source writes leaking cdef; GREEN object-write rollback
   preserves committed 2345. Relevant object writers and conditional copies share
   an object lock; lock order is object then multipart.

RED logs: final-concurrency-red.log, final-source-route-red.log,
final-source-failure-red.log and final-empty-key-red.log. Full S3 GREEN is retained
as final-go-green.log. Post-fix full Go/race/SDK/runtime gates all passed. No second reviewer or
second fix pass is used. No integration or release is requested.

The reviewer explicitly set aside these behaviors; each was retained as a
documented scope decision rather than silently omitted:

- Versioned/access-point/encrypted copying: explicit rejection stands; support
  requires a separate integration.
- IAM/policy enforcement and multiple accounts: inherited local model stands;
  cloud authorization/isolation is not provided.
- General key aliasing/symlinks: existing filesystem mapping stands; its limits
  remain applicable.
- Legacy completion ordering, requested ETag validation, minimum nonfinal sizes,
  multipart ETag construction and destination checks: existing semantics stand;
  this slice fixes their interaction with copy rollback.
- Plain UploadPart destination/upper-number checks: existing validation stands;
  copy validation does not extend those legacy guarantees.
- Crash consistency/permanent rollback failure: excluded and documented;
  process/disk failure may need recovery.
- Selected-part buffering: accepted existing strategy; memory scales with part
  and previous-object sizes.
- Persistent ListParts timestamps: no new timestamp schema; LastModified is
  returned in CopyPartResult as approved.
- Wildcard/multiple-value ETag extensions: not implemented; single tag conditions
  from the reviewed contract are supported.
- Previous SNS/EventBridge changes: outside this slice review; their preceding
  review and verification evidence remain intact.

No deferred Minor findings were reported for this slice.
