# S3 RenameObject verification — 2026-10-07

Historical verification record: branch, index and authorization statements below
describe the implementation-time state. The user subsequently authorized a
commit and pull request; specifications, plans and local tracking CSVs remain
outside the published patch.

Final status: RenameObject native implementation is verified. The one fresh
review found two Important issues; both are fixed through observed RED→GREEN and
all corrected-candidate gates pass. The frozen completion CSV has
**12 verified / 3073 pending**, including this one newly verified target.

## Scope and implementation

The fixed target is s3/RenameObject. CreateSession is a required companion
promotion from auto-crud; it does not increase the frozen completion count.
The work runs inline on feat/aws-operation-support in the current checkout.
No files are staged or committed. The initial HEAD is
`a6923c975d540143347b6b524af64094fdc55d04`.

Migration 12 adds canonical directory buckets, five-minute sessions and successful
rename receipts. Migration tests reopen a database containing actual legacy rows
and verify those rows are unchanged. Directory creation validates the supplied
XML settings and zone suffix, and never promotes an ordinary bucket by name.
Nonempty directory deletion rejects objects, orphan files and multipart uploads;
successful deletion invalidates sessions and receipts for the old incarnation.

Sessions use random credentials and hashed tokens. Admission checks the actual
issued access key, bucket incarnation, token, expiry and ReadOnly/ReadWrite mode.
SigV4/IAM verification remains outside the inherited local provider contract.
Gateway classifies s3express signing as S3 REST-XML and keeps canonical directory
errors terminal, including unsupported encryption and LocalZone.

Rename parses encoded keys exactly once, validates all eight ETag/date conditions,
and fingerprints canonical keys/conditions. A successful token replay precedes
existence checks and cannot mutate recreated source or edited/deleted destination.
A differing request returns modeled IdempotencyParameterMismatch. Failed work
leaves no receipt. Same-key rename is a validated no-op. Actual file moves retain
stored metadata and tags, support a sparse 5 GiB+1 source without full payload
copy, and restore overwritten destination and source on precommit SQL failures.

All native S3 request admission, reads and mutations share one object lock;
ListResources locks independently. Multipart locking remains beneath it, and
notification delivery remains asynchronous. Real SQLite trigger failures verify
rollback of object/tag/receipt writes. A real paused SQLite writer after file
moves demonstrates that GET/HEAD/list/tag/put/delete/batch-delete/copy/completion/
part-copy cannot expose the intermediate filesystem state.

## Evidence

Plan-owned logs and immutable review artifacts are retained in the git-ignored
`.superpowers/sdd/2026-10-07-s3-rename-object/` workspace because no commit exists
that could replace them. Every task's behavioral RED and passing task-done run is
recorded in progress.md. An initial unused test import and an invalid HTTP-date
fixture were corrected before the applicable behavioral test gate; failures and
attempts remain available.

- Baseline: CGO_ENABLED=0 go test ./... -count=1 passed before new code.
- Tasks 1–5: actual directory creation/lifecycle/session/parser/condition/move/
  retry/rollback/concurrency tests observed RED and then GREEN. S3/gateway suite
  and CGO_ENABLED=1 race run pass.
- Task 6: rebuilt dist/devcloud before tests; four new boto3 contracts plus S3,
  UploadPartCopy and integration regressions: **18 passed**. Automatic data
  session authentication stays enabled. Explicit mode clients send their actual
  issued directory token rather than replacing it with an automatic session.
- Codegen: make codegen and TestUpdatePublishedFigures pass. Fresh generation
  into an owned temporary directory produces **zero diff**. Existing ambiguous
  alias warnings are unchanged.
- Measured registered tiers: hand 4568 / auto 11560 / unimplemented 3073,
  total 19201. Serving: 4537 / 5882 / 1988, total 12407. S3 hand count is 40.
- Original candidate: Go119 tested packages, S3/gateway race, SDK1556 passed/15 existing skips/1 existing xfailed and runtime13 passed/zero skips.
  Fresh review identified two Important admission gaps. After the grouped fixes,
  corrected candidate: **Go119 tested packages**, **S3/gateway race pass**,
  **SDK1556 passed/15 existing skips/1 existing xfailed**, **runtime13 passed/zero skips**,
  rebuilt binary and **zero generation drift**. Logs are final-go.log,
  final-race.log, final-sdk.log, final-runtime.log and final-drift.log.

## Local limits

See [S3 service documentation](../../services/s3.md) for the runnable SDK contract.
Directory SDK clients use numeric loopback plus path addressing on the same local
port; virtual-host DNS, cloud zone lookup, encryption and CloudTrail are absent.
LocalZone, versions and access points are unsupported. Directory token admission
does not verify the secret/signature or IAM policies. The local account is fixed.

Filesystem/SQLite are not a crash-atomic transaction. Multiple servers must not
share a data directory. Persistent filesystem errors can prevent rollback;
joined errors return InternalError. Backup cleanup after a committed rename logs
an error but keeps success. Receipts have no TTL and persist until successful
bucket deletion. Conditions retain existing unquoted local ETag compatibility.

## Final review, rulings and completion reconciliation

The fresh gpt-6-astra/high reviewer inspected the immutable33file slice relative
to its saved starting tree, including the existing UploadPartCopy lock change.
The review confirmed patch hashes and independently passed focused tests. There
were zero Critical, two Important and one Minor findings.

1. Admission classified exempt copies differently from protected operations
   actually dispatched. Tests reproduced GET/DELETE with x-id=UploadPartCopy,
   empty-copy-header PUT and object-path batch deletion without credentials or
   with ReadOnly credentials. The corrected classifier follows actual dispatch
   priority, and explicit copy-part method conflicts return405.
   TestDirectoryAdmissionMatchesDispatchedOperation and
   TestDirectoryAdmissionRespectsMultipartAndHeadDispatch observed RED→GREEN.
2. Plain multipart operations trusted an upload ID independently of the admitted
   bucket/key/account. TestMultipartUploadBoundToAdmittedBucketAndKey observed
   RED for all16 combinations of part write/read/abort/complete with general
   bucket paths, another directory session, wrong key and wrong account. The
   shared request-identity helper now rejects mismatches before reading or
   mutation. Tests inspect actual saved part bytes and all legacy SQL rows and
   confirm correct-path access after rejection.

Both Important findings were handled in one grouped fix pass. Its initial test
helper signature compile errors were corrected before behavioral RED; all logs
are retained. No second review was dispatched.

Deferred minor: CreateSession accepts conflicting explicit selectors such as
GET ?session&x-id=DeleteBucket and can issue unused local five-minute credentials.
This alone does not grant protected object access. The final fix pass excludes
Minor findings, which remain recorded here and in the ledger.

Rulings, in order (each states the cost if wrong):

- retain ignored workspace and evidence rather than delete — no commits authorized, so git history cannot replace evidence — cost if wrong: local scratch disk usage.
- use current checkout branch rather than worktree — explicit user instruction takes precedence — cost if wrong: changes share this checkout.
- Task 1 directory object fixture now sends an actually issued ReadWrite session — directory admission is the approved new behavior — cost if wrong: test fixture needs adjustment.
- pause the real SQLite writer using a test-only INSERT trigger and scalar function after file moves — a separately held deferred writer cannot deterministically pause after moves before WAL upgrade failure — cost if wrong: test-only callback registration complexity.
- crash atomicity, shared-server storage and external filesystem races remain excluded — the approved contract guarantees ordinary rollback and single-provider request consistency — cost if wrong: crash or shared storage can require manual recovery.
- full SigV4/IAM, cloud zones, encryption and virtual-host DNS remain excluded — local token admission is enforced and cloud infrastructure is explicitly absent — cost if wrong: callers expecting cloud authentication or endpoints must adapt.
- new user-metadata/checksum persistence remains excluded — rename preserves every field already stored plus object tags — cost if wrong: unstored metadata cannot be recovered by rename.
- unchanged unsupported object-subresource dispatch including GetObjectAttributes remains outside this slice — no such operation is newly promoted; its existing fidelity status is unchanged — cost if wrong: that legacy path can return an inaccurate response until its own implementation.

Python csv reconciliation confirms3085 unique identical frozen/completion pairs,
12 verified/3073 pending and unchanged contents of every other row. Only
s3/RenameObject was newly verified. CreateSession is a companion promotion,
not an additional fixed target. The original immutable review patch and snapshot,
post-fix patch, source hashes and all failures/retries are retained. Current branch
is feat/aws-operation-support, HEAD is unchanged, index is empty and all51
previous EventBridge/SNS/SQS source hashes remain unchanged. git diff --check passes.
Previously completed EventBridge/SNS/SQS files must retain their saved hashes.
No broad AWS completion is claimed: RestoreObject, SelectObjectContent and
WriteGetObjectResponse remain pending, as do the other frozen inventory rows.


## Publication verification (2026-10-07)

The user subsequently authorized committing the implementation and opening a PR.
Specifications, implementation plans, local tracking CSVs and agent workspace
files remain excluded. The feature branch was advanced to current main
`a87a3dfda67851b8969f2e5d11288242a759ceae` before publication, including the
SQLite 1.60.1 dependency update. Publication lint cleanup changed some historical
source hashes; those implementation-time hash statements above are historical.

The final runtime source was verified using Go 1.26.7:

- `CGO_ENABLED=0 go test ./... -count=1`: 119 tested packages passed.
- Race tests across shared CRUD, EventBridge, SNS, SQS, S3 and gateway: all six packages passed.
- Uncapped `golangci-lint run --timeout=5m`: zero issues; formatting and diff whitespace checks passed.
- Fresh Smithy code generation: zero drift.
- Rebuilt binary and pinned compatibility clients: 1,556 passed, 15 existing skips, one existing xfail.
- Required Docker Lambda runtime tests: 13 passed, zero skips, 1,559 unrelated tests deselected.

The original 3,085-operation target remains 12 verified and 3,073 pending.
This publication does not claim completion of the full AWS operation inventory.


Publication commit hooks subsequently added SPDX comments to two Go files and
formatted five Python files. Exact comment-only Go comparisons and identical
Python ASTs confirm that the verified execution logic was preserved.
