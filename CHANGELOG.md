## [v1.1.0](https://github.com/skyoo2003/devcloud/releases/tag/v1.1.0) - 2026-09-06
### Added
* Provider-namespaced configuration: service blocks can be written as `providers.aws.services.*`, forward-compatible with `providers.azure.*`. The top-level `services` block is the same block under its historical name and keeps working ([#135](https://github.com/skyoo2003/devcloud/issues/135))
* Per-provider auth adapters (`internal/auth`) parse SigV4 credentials once and expose the caller's claimed identity to every plugin via `auth.FromContext`, adding a `region` field to `GET /devcloud/api/logs`. Nothing is verified — DevCloud still accepts any credentials ([#135](https://github.com/skyoo2003/devcloud/issues/135))
* Amazon Comprehend, served by the generic CRUD engine (49 operations auto-crud, 36 unimplemented), where an unimplemented operation returns a clean AWS error rather than a fabricated success ([#136](https://github.com/skyoo2003/devcloud/issues/136))
* All 48 upstream AI/Machine Learning service models are registered, taking DevCloud from 105 to 148 services. 17 are served and the remaining 31 decline with a clean AWS error instead of falling through to real AWS ([#137](https://github.com/skyoo2003/devcloud/issues/137))
* docs/coverage.md, which publishes the registered, serving and registered-only service counts together so the service count is never stated on its own ([#137](https://github.com/skyoo2003/devcloud/issues/137))
* Admin API endpoint `GET /devcloud/api/unrouted` reports which AWS services this DevCloud was asked for and does not register, with per-service counts and first/last seen; it requires `admin.enabled: true`. The figure is a floor — an unclassifiable request is routed to S3 rather than counted, and a non-zero `droppedServiceIds` means the collector hit its key cap ([#138](https://github.com/skyoo2003/devcloud/issues/138))
* "Service Not Supported" issue form for requesting an AWS service DevCloud does not register yet, which asks which operations you actually call and accepts `/devcloud/api/unrouted` output ([#138](https://github.com/skyoo2003/devcloud/issues/138))
* The generic CRUD engine now serves `rest-json` services, recovering the operation from the request method and path against the route table the model already declares (shared in `internal/shared/httproute`). 28 registered-only services started serving: services with at least one operation went 117 to 145, and `auto-crud` operations 1,415 to 2,200 ([#139](https://github.com/skyoo2003/devcloud/issues/139))
* The 57 AWS services with demonstrated demand are registered, taking DevCloud to 205 services — 199 of which serve at least one operation, for 4,858 engine-served operations. The three that serve nothing are named with their reasons in docs/coverage.md ([#139](https://github.com/skyoo2003/devcloud/issues/139))
* The generic CRUD engine now serves every protocol DevCloud registers: `rest-xml` recovers its operation from method and path, and `query` reads it from the form body's `Action` field. 112 operations move from `unimplemented` to `auto-crud`, so protocol is no longer a reason any registered service serves nothing ([#143](https://github.com/skyoo2003/devcloud/issues/143))
* CI gates the published coverage figure: `go test ./cmd/devcloud/` compares `docs/coverage.md`, `README.md` and `docs/README.md` against the live registry in both directions, so neither the code nor the number can move without the other. The demand set in `docs/demand.md` is checked to still be registered, so the count cannot be held steady by swapping one service for another ([#144](https://github.com/skyoo2003/devcloud/issues/144))
* `scripts/model_churn.py` summarises what a Smithy model sync actually changed, so the weekly sync PR reads as a change rather than a whole-tree regeneration of 194 models ([#147](https://github.com/skyoo2003/devcloud/issues/147))
### Changed
* Codegen now runs through a provider-neutral intermediate representation (`internal/codegen/ir`) behind a `ModelSource` interface, so a second API description format is an added file rather than a rewrite. Smithy is the first source and generated output is byte-identical ([#135](https://github.com/skyoo2003/devcloud/issues/135))
* Service routing aliases are derived from the Smithy models instead of a hand-maintained switch in the gateway, so onboarding a service no longer needs a hand-written routing entry. All 93 previously-routed aliases resolve unchanged, and an alias claimed by more than one service is reported rather than guessed ([#137](https://github.com/skyoo2003/devcloud/issues/137))
* The fidelity manifest records each service's wire protocol, so a registered service that the CRUD engine cannot reach is distinguishable from one whose wiring is broken ([#137](https://github.com/skyoo2003/devcloud/issues/137))
* The AWS coverage target is 205 services, not 431: of the 283 services DevCloud does not register, only 57 have been built by two or more of moto, LocalStack and terraform-provider-aws, and DevCloud has received zero service requests to date. The remaining 226 can still be onboarded on request but are no longer promised — evidence in docs/demand.md, decision rule in docs/coverage.md ([#138](https://github.com/skyoo2003/devcloud/issues/138))
* `rest-xml` request bodies are still not buffered by the gateway, now by an explicit rule rather than as a side effect of the protocol being unservable: the engine serves `rest-xml` from the path and query alone, so S3's large binary uploads keep streaming. `crud.NeedsBody` is the predicate, and a test fails if the body is read ([#143](https://github.com/skyoo2003/devcloud/issues/143))
* The boto3 compatibility suite exercises every registered service, parametrising over a generated service list (`internal/generated/compat/services.json`) instead of two hand-written name lists. It found 31 of 205 registered services with no boto3 test at all, and grew from 854 to 993 tests ([#144](https://github.com/skyoo2003/devcloud/issues/144))
### Fixed
* Service registration is no longer silently dropped: `scripts/generate-imports.sh` gated on a `register.go` file that no service has, so every run wrote an empty import block and the built binary registered nothing. It now gates on the package actually calling `Register`, and refuses to write a gutted file instead of reporting success ([#136](https://github.com/skyoo2003/devcloud/issues/136))
* A newly scaffolded service now serves its CRUD-shaped operations instead of answering HTTP 500 for everything, because the generated provider declined with an error the gateway never routes to the CRUD engine ([#136](https://github.com/skyoo2003/devcloud/issues/136))
* The fidelity manifest no longer reports `auto-crud` for operations the binary refuses; a tier assigned from CRUD-registry membership alone says an operation is classifiable rather than reachable, and now requires the owning provider to actually hand unimplemented operations to the engine ([#136](https://github.com/skyoo2003/devcloud/issues/136))
* Smithy's non-boxed primitives (PrimitiveLong, PrimitiveBoolean and the other five) are mapped to Go types; models using them previously generated code that did not compile ([#137](https://github.com/skyoo2003/devcloud/issues/137))
* `make codegen` is now byte-identical across runs, where a model bundling two namespaces with colliding shape names produced different output every time because shapes are keyed by short name and the generator iterated a Go map ([#137](https://github.com/skyoo2003/devcloud/issues/137))
* Amazon Rekognition can be onboarded; its `X-Amz-Target` prefix `RekognitionService` was covered by no routing entry, so every call returned `UnknownService` ([#137](https://github.com/skyoo2003/devcloud/issues/137))
* Stale service counts across the docs: README.md advertised 104 services and docs/faq.md 101 when 148 are registered, and docs/fidelity-manifest.md carried outdated per-tier operation totals. Every count now appears with its depth split (148 registered / 117 serving at least one operation), so the number is never read as a capability claim on its own ([#138](https://github.com/skyoo2003/devcloud/issues/138))
* S3 Control requests were served by S3, which signs with the same name and parsed them as a bucket and key — `CreateAccessPoint` returned 200 and left an object in a bucket named `v20180820`. S3 Control is now split off by its `/v20180820/` path prefix, and its unserved operations return a clean AWS error instead of a fabricated success ([#142](https://github.com/skyoo2003/devcloud/issues/142))
* Eight query-protocol providers (`rds`, `neptune`, `docdb`, `redshift`, `elasticache`, `autoscaling`, `cloudformation`, `elasticloadbalancingv2`) answered any unimplemented operation with HTTP 200 and an empty `<ActionResponse/>`, so a caller was told an operation succeeded when nothing ran — and botocore raised `KeyError` on the missing `<ActionResult>` wrapper. They now hand the operation to the generic CRUD engine, which serves the 223 CRUD-shaped ones from the real store and refuses the rest with a clean `InvalidAction` ([#145](https://github.com/skyoo2003/devcloud/issues/145))
* The four Amazon Lex services are reachable from boto3, where all four sign as `lex` and no service is called `lex`, so the alias stayed contested and every Lex call died as `UnknownService`. The shared-signing-name split now recognises a signing name that is a group key rather than a member service ID, and answers each request from the sibling whose route table models its method and path ([#146](https://github.com/skyoo2003/devcloud/issues/146))
* `appsync.ListApis`, `eks.ListAccessPolicies` and `opensearch.ListApplications` are reachable: each was implemented, but the provider's hand-written path resolver did not know the route boto3 sends, so the operation arrived empty and the call answered `NotImplemented`. Each provider now falls back to its own model's route table when its resolver recognises nothing ([#146](https://github.com/skyoo2003/devcloud/issues/146))
* The fidelity manifest no longer drops operations with short names, where the hand-verified scan required four characters or more and reported `resourcegroups.Tag` as unimplemented while the code beside it was real. Short dispatch literals are now collected separately and promoted only where the service's own model declares them ([#146](https://github.com/skyoo2003/devcloud/issues/146))
* S3 no longer answers an unimplemented bucket sub-resource with a bucket listing, where `GET /{Bucket}?analytics` and seven siblings fell through to `ListObjects` and botocore read the resulting 200 as a successful call. A bucket-level GET whose query parameter is neither a served sub-resource nor a listing parameter now returns a clean `NotImplemented` ([#146](https://github.com/skyoo2003/devcloud/issues/146))
* The weekly Smithy model sync discarded the changes worth reviewing: its test step ended the job before the pull request was opened, and an upstream model that gains an operation is designed to fail that step. The test result is now recorded rather than gating, the PR is always opened when models moved, and the failure is re-raised afterwards so the cron does not silently go green ([#147](https://github.com/skyoo2003/devcloud/issues/147))
* A release dry run keeps its artifacts; the `Upload assets` step ran when `dry_run` was not set, so it uploaded on a real release — where GoReleaser has already attached the same files — and skipped the one path that publishes nothing else ([#150](https://github.com/skyoo2003/devcloud/issues/150))
### Documentation
* Coverage docs state what the compatibility figure excludes and why: two registered services have no boto3 client at all (`sagemakerruntimehttp2`, `transcribestreaming`), and the four Lex services are registered but reachable by no boto3 caller because every Lex client signs as the contested alias `lex` ([#144](https://github.com/skyoo2003/devcloud/issues/144))
* docs/coverage.md now publishes the measured cost of keeping up with upstream — refreshing all 194 vendored models changed 93 of them, and 32 moved an operation. The reading states its own ceiling: those 93 had been vendored 141 days earlier, so it is an accumulated backlog and not a weekly rate ([#147](https://github.com/skyoo2003/devcloud/issues/147))
* The release procedure moved from `docs/release.md` to `RELEASE.md` at the repo root, and now states three things it never did: the Homebrew formula is published to a separate tap repository through a GitHub App token scoped to that repo alone, a tagged build pushes four `*-alpine` image tags, and `make changelog VERSION=` is the batch-and-merge pair ([#148](https://github.com/skyoo2003/devcloud/issues/148))
* The contributor guide no longer asks for SQLite3 development headers, CGO, or a separately running DevCloud instance — the driver has been pure Go since v1.0.0, and the compatibility suite starts its own server. docs/getting-started.md also no longer advertises a Next.js frontend on port 3000 that lives in a separate repository ([#149](https://github.com/skyoo2003/devcloud/issues/149))
* The compatibility policy states current figures, having gone stale in five places because nothing gates it: `cmd/devcloud/coverage_test.go` covers docs/coverage.md, README.md and docs/README.md, and docs/compatibility-policy.md is not in that set. It now reads 1,144 tests, 5,193 `auto-crud`, 4,497 hand-verified, 205 registered with 4 serving nothing, and 193/12 model-backed to hand-written ([#150](https://github.com/skyoo2003/devcloud/issues/150))
## [v1.0.0](https://github.com/skyoo2003/devcloud/releases/tag/v1.0.0) - 2026-08-11
### Added
* Add the four SQS dead-letter redrive operations (ListDeadLetterSourceQueues, Start/List/CancelMessageMoveTask), completing SQS at 23 operations ([#94](https://github.com/skyoo2003/devcloud/issues/94))
* Document the `ServicePlugin` API and its v1.x stability policy in docs/plugin-api.md, enforced by a conformance test over every registered service ([#96](https://github.com/skyoo2003/devcloud/issues/96))
* Enable boto3 compatibility tests for CodeConnections, DMS, and Verified Permissions ([#96](https://github.com/skyoo2003/devcloud/issues/96))
* Add 24 core-service operations: EC2 instance lifecycle, region/AZ and tag queries, security group rules, Elastic IP association, and IAM group policy management ([#97](https://github.com/skyoo2003/devcloud/issues/97))
* Serve ~2,200 standard CRUD operations across the 46 JSON-protocol services from a Smithy-driven fallback engine (`internal/shared/crud`); hand-written operations take precedence and unclassifiable ones still return an error. See docs/crud-engine.md ([#98](https://github.com/skyoo2003/devcloud/issues/98))
* Homebrew installation via `brew install skyoo2003/tap/devcloud`, published to the tap on each release ([#121](https://github.com/skyoo2003/devcloud/issues/121))
* Per-operation fidelity manifest declaring every operation as hand-verified, auto-crud or unimplemented, exposed at `GET /devcloud/api/fidelity` and enforced by a build-failing coverage test ([#126](https://github.com/skyoo2003/devcloud/issues/126))
* Real tag support for KMS, CloudWatch and EventBridge — TagResource, UntagResource and ListTagsForResource/ListResourceTags now persist tags per resource ARN instead of being echoed by the generic CRUD engine ([#126](https://github.com/skyoo2003/devcloud/issues/126))
* A published compatibility policy (docs/compatibility-policy.md) stating what v1.0 guarantees across the 1.x line — config keys, environment variables, the CLI, admin API response keys, fidelity tier names, and the response shape of hand-verified operations covered by the boto3 compatibility suite — what it explicitly does not guarantee, and the deprecation procedure that precedes any removal ([#129](https://github.com/skyoo2003/devcloud/issues/129))
### Changed
* Rename `internal/dashboard` to `internal/admin` and the `dashboard.enabled` config key to `admin.enabled`. The old key is honoured for one release with a deprecation warning. ([#111](https://github.com/skyoo2003/devcloud/issues/111))
* Build the request log collector only when `admin.enabled` is set, dropping a per-request mutex lock and ring-buffer write from the AWS request hot path ([#112](https://github.com/skyoo2003/devcloud/issues/112))
* The `services` config block is now optional: omit it and every registered service starts with `data_dir ./data/<service>`; list services and only those start. Replaces 325 lines of identical boilerplate. ([#120](https://github.com/skyoo2003/devcloud/issues/120))
* Release archives now carry the `docs/` tree alongside the binary, so the documentation you unpack — including the fidelity manifest and the release's compatibility promises — describes exactly the version you downloaded ([#127](https://github.com/skyoo2003/devcloud/issues/127))
### Removed
* Remove the dead, unregistered STS provider stub (`internal/services/sts`); STS is served by the IAM package ([#96](https://github.com/skyoo2003/devcloud/issues/96))
* Extract the web dashboard to a separate repository. The binary builds and serves no UI, only the opt-in admin API at `/devcloud/api/*`. ([#111](https://github.com/skyoo2003/devcloud/issues/111))
* Remove dead surface found by a repo-wide over-engineering audit: the event bus and admin WebSocket, the `GetMetrics` plugin API and `/devcloud/api/metrics` (resource counts remain on `/devcloud/api/services`), `shared.ResourceStore`, `gateway.ExtractAccountID`, and the `auth.enabled` key — still read for one release so enabling it warns that credentials are accepted regardless. Drops the `gorilla/websocket` dependency. ([#120](https://github.com/skyoo2003/devcloud/issues/120))
### Fixed
* Route `X-Amz-Target` prefixes that contain dots (CodeConnections, DMS) to the right service; they misrouted to CloudTrail ([#96](https://github.com/skyoo2003/devcloud/issues/96))
* Unimplemented operations return `NotImplemented` (HTTP 501) instead of an empty `200`, so SDKs can tell a not-yet-emulated operation from an invalid one ([#96](https://github.com/skyoo2003/devcloud/issues/96))
* EC2: Describe{SecurityGroups,Tags,Addresses} honor their selectors and Filters instead of returning every resource, unknown IDs raise NotFound, batch Start/Stop is atomic, and duplicate or absent security group rules are rejected ([#97](https://github.com/skyoo2003/devcloud/issues/97))
* DynamoDB: numeric sort keys order by true value over the full 38 digits via a NUMTEXT collation, instead of a float CAST that lost precision past 2^53 ([#97](https://github.com/skyoo2003/devcloud/issues/97))
* Config-time warnings (deprecated `dashboard` key, unknown `DEVCLOUD_SERVICES` tier) now honor `logging.format`/`logging.level` instead of always printing as plain text before the logger is configured ([#113](https://github.com/skyoo2003/devcloud/issues/113))
* The weekly Smithy model sync could never report an update: it skipped every model already in the tree (all of them), and 14 entries of its hand-maintained service list 404'd upstream. The list is now derived from the tree, the workflow re-downloads with `--refresh`, and downloads are atomic so a failed fetch cannot delete a committed model. ([#120](https://github.com/skyoo2003/devcloud/issues/120))
* Codegen now parses operations bound to Smithy resource shapes, recovering 285 operations that were invisible to the generator (bedrock 0 of 101, lambda 19 of 85, ecs 12 of 76, transfer 29 of 71, sso-admin 67 of 79) ([#126](https://github.com/skyoo2003/devcloud/issues/126))
* The fidelity manifest now reads each provider's actual dispatch instead of intersecting with the Smithy model, recovering 226 served operations it had hidden (dynamodbstreams listed 4 of its 22, acm's UpdateCertificate, bedrock's InvokeModelWithResponseStream) and dropping 5 non-operations it had invented (identitystore Description/DisplayName/Emails, pipes DELETE/POST) ([#126](https://github.com/skyoo2003/devcloud/issues/126))
* EventBridge now drops a bus's or rule's tags when it is deleted. ARNs are derived from the name, so recreating a deleted resource reused its ARN and inherited the previous tags ([#126](https://github.com/skyoo2003/devcloud/issues/126))
* Restored CloudWatch's 17 CRUD-engine operations. The gateway picks the protocol from the request, not from the provider, so CloudWatch reaches the engine whenever a client speaks JSON — filtering the registry by the provider's declared protocol had removed that coverage outright ([#126](https://github.com/skyoo2003/devcloud/issues/126))
* EventBridge rule ARNs now name their event bus, as AWS does. A rule name is unique per bus, so same-named rules on two custom buses previously shared one ARN — and with it, one tag set, where tagging one rule changed the other's and deleting one wiped the survivor's ([#126](https://github.com/skyoo2003/devcloud/issues/126))
* CloudWatch now drops an alarm's tags when the alarm is deleted, so recreating an alarm under the same name no longer inherits the old one's tags ([#126](https://github.com/skyoo2003/devcloud/issues/126))
* Downloadable binaries now start, where releases built with `CGO_ENABLED=0` against a SQLite driver that required cgo exited at startup — every tar.gz/zip binary, the Homebrew formula and the versioned `*-alpine` images. The driver is now pure Go, so no build needs a C toolchain or SQLite headers ([#128](https://github.com/skyoo2003/devcloud/issues/128))
* Service selection now matches what the docs promise: an empty `services` block starts nothing rather than every registered service, and `DEVCLOUD_SERVICES` names the running set outright instead of intersecting with the block. Previously `DEVCLOUD_SERVICES=sqs` alongside a block listing only `s3` started nothing at all ([#129](https://github.com/skyoo2003/devcloud/issues/129))
* CloudFront no longer fabricates a success for the 122 operations the fidelity manifest classifies as `unimplemented`, where its dispatch fallback answered HTTP 200 with an empty XML document that boto3 parses as a successful empty result. It now returns `NotImplemented` (HTTP 501), matching the other 32 providers that decline from their own dispatch default ([#129](https://github.com/skyoo2003/devcloud/issues/129))
* Generated routers now honour route patterns that constrain the query string, so operations distinguished only by a query parameter are reachable — `matchURI` split the whole pattern into path segments, leaving CloudFront `TagResource` and `UntagResource` implemented but unroutable. Query-constrained routes are tried ahead of unconstrained ones, so a bare path cannot shadow a more specific route ([#129](https://github.com/skyoo2003/devcloud/issues/129))
* Released binaries now register their services, where GoReleaser built `cmd/devcloud/main.go` rather than the package and dropped the `imports.go` that blank-imports all 104 services — every archive, the Homebrew formula and the container images exited at startup with `unknown service: s3`. The release gate now builds with GoReleaser instead of compiling the package, which is why the boto3 suite missed it ([#131](https://github.com/skyoo2003/devcloud/issues/131))
### Documentation
* Corrected what the fidelity manifest says an `unimplemented` operation returns: it claimed JSON and Query services answer `InvalidAction` (HTTP 400), where only the 46 providers falling through to the CRUD engine do and 32 answer `NotImplemented` (HTTP 501) from their own dispatch default. Only the failure itself is stable — the specific code and status are documented, not guaranteed ([#129](https://github.com/skyoo2003/devcloud/issues/129))
* The roadmap, architecture and governance docs no longer describe DevCloud as pre-1.0. All three ship inside the release archive, so a v1.0 download would have carried a roadmap listing its own release as pending, an architecture overview naming Phase 1 as current, and a governance doc calling the API unstable — contradicting the compatibility policy packaged beside them ([#130](https://github.com/skyoo2003/devcloud/issues/130))
## [v0.2.0](https://github.com/skyoo2003/devcloud/releases/tag/v0.2.0) - 2026-04-21
### Added
* Add GoReleaser + Changie release pipeline ([#26](https://github.com/skyoo2003/devcloud/issues/26))
### Changed
* Bump dependencies: golangci-lint-action, go-sqlite3, GitHub Actions (setup-python, stale, checkout, release-drafter), web packages (lucide-react, shadcn, react, typescript, base-ui) ([#27](https://github.com/skyoo2003/devcloud/issues/27))
### Fixed
* Fix slice memory allocation with excessive size value (code scanning alerts #19-#22) ([#6](https://github.com/skyoo2003/devcloud/issues/6))
* Remove reference to non-existent devcloud.yaml in Dockerfile ([#25](https://github.com/skyoo2003/devcloud/issues/25))
### Security
* Fix uncontrolled data used in path expression (code scanning alerts #1-#15) ([#23](https://github.com/skyoo2003/devcloud/issues/23))
## [v0.1.0](https://github.com/skyoo2003/devcloud/releases/tag/v0.1.0) - 2026-04-18

First public release of DevCloud — a local development companion for cloud-native apps. Positioned as an on-ramp to the cloud, not a replacement: iterate locally without cloud bills, then land cleanly on your target CSP.

### Added

**Core runtime**

- Single-binary HTTP gateway on port **4747** with multi-protocol support (REST-XML, JSON 1.0, JSON 1.1, REST-JSON, Query) and middleware chain (error recovery, body limit, CORS, request ID, structured logging).
- Plugin registry with deterministic service initialization order and graceful shutdown.
- Zero-config startup: server runs with embedded default configuration if `devcloud.yaml` is absent.
- Config loader with `Load()` (strict) and `LoadOrDefault()` (graceful fallback to embedded).
- Environment-variable overrides: `DEVCLOUD_SERVICES` (with `tier1` / `tier2` / `tier3` / `all` shortcuts) and `DEVCLOUD_DATA_DIR`.

**AWS service coverage**

- 96 AWS services scaffolded from official Smithy models via an in-tree code generator.
- Big 6 fully implemented: **S3, SQS, DynamoDB, Lambda, IAM, STS**.
- Integration services: SNS, CloudWatch, CloudWatch Logs, KMS, SecretsManager, SSM, EventBridge, ECR, EC2, ECS, Route53, ACM, CloudFormation, and more.
- Cross-service integrations: CloudFormation provisioning, DynamoDB Streams → Lambda, SQS → Lambda, S3 → Lambda, EventBridge → SQS/SNS/Lambda, SNS → SQS subscriptions.
- **96% boto3 compatibility** (671/699 tests passing against the official AWS SDK test surface).
- Port-aware URL construction in SQS, ECR, and CloudFormation response paths (opts-based, not hardcoded).

**Code generation pipeline**

- Smithy JSON model parser and template-driven Go generator.
- Weekly auto-sync workflow keeps models current with AWS upstream.
- Generated files (`internal/generated/**`) include SPDX license headers and are marked `DO NOT EDIT`.

**Web dashboard** (optional, gated on `dashboard.enabled`)

- Next.js 16 / React 19 / Tailwind UI served statically by the Go server.
- Service status, resource browser, WebSocket-based live API log stream.

**Docker packaging**

- Multi-stage production Dockerfile (Alpine runtime).
- Dockerfile.dev for hot-reload frontend development.
- `docker-compose.yml` wiring backend (port 4747) and Next.js dev server (port 3000).

**Testing**

- Go unit tests for every service package (108 packages, all green).
- Table-driven port-propagation regression tests in SQS, ECR, CloudFormation.
- Python/boto3 compatibility suite under `tests/compatibility/`.

### Known Issues

- `auth.enabled: true` is accepted but SigV4 enforcement is not yet implemented; the server emits a warning at startup to make this visible.
- Lambda function invocation is a stub (accepts registration but does not execute handler code).
- Windows is not in the CI matrix; WSL2 is expected to work.
