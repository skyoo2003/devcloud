# AWS Service Coverage 100%

## Problem

DevCloud registers 104 AWS services. Upstream `aws-sdk-go-v2/aws-models` publishes
431. Roughly three quarters of the AWS surface is not routed at all — an SDK call
to an unregistered service does not reach a provider, it is simply not a service
DevCloud knows. Developers hit that wall silently and fall back to real AWS, which
is the exact cost and iteration-speed problem DevCloud exists to remove.

The cost of leaving this unsolved is credibility at the evaluation step: "how many
services does it support" is the first filter a team applies when choosing a local
cloud runtime, and 104 loses that filter before depth is ever examined. It also
blocks the multi-CSP roadmap — Azure and GCP expansion is only defensible once AWS,
the reference provider, is complete.

## Evidence

- **Assumption — needs validation via user research.** No issue backlog, support
  ticket, or usage telemetry currently substantiates demand for specific missing
  services. The 104 vs 431 gap is a measured fact; the claim that closing it
  changes adoption is not yet evidenced.
- Measured today: 104 registered services, 93 with an in-tree model, 7,481 known
  operations (4,498 hand-verified / 950 auto-crud / 2,033 unimplemented).
- Measured today: 431 model files exist upstream. ~71% of in-tree models use a
  JSON-family protocol; ~29% use Query, EC2 Query, or REST-XML.

**Validation owed before Milestone 3 commits budget**: which of the ~330 missing
services are actually reached for. Recommended methods — issue-template telemetry
on "service not supported", a public poll, or instrumenting unrouted-service
requests in the gateway.

## Users

- **Primary — boto3 application developers** running integration tests locally.
  Trigger: a test touches a service DevCloud does not register, so the test is
  skipped, mocked, or pointed at real AWS.
- **Primary — platform engineers using Terraform/CDK.** Trigger: a stack contains
  one resource type DevCloud cannot serve, and the whole plan/apply fails, not
  just that resource.
- **Primary — CI pipeline operators.** Trigger: a portion of the integration suite
  must run against a billed AWS account because DevCloud does not cover it,
  splitting the pipeline and its credentials.
- **Not for**: teams needing production-grade behavioural parity. This work
  increases breadth, not depth. Anyone depending on validation semantics,
  cross-resource integrity, or business logic in a newly added service is
  explicitly not served by it.

## Hypothesis

We believe **registering every AWS service published upstream, each meeting a
stated minimum quality floor**, will **remove "that service isn't supported" as a
reason to fall back to real AWS** for **boto3 developers, IaC engineers, and CI
operators**.

We'll know we're right when **the count of registered services equals the upstream
model count, every registered service passes at least one boto3 compatibility test,
and the rate of unrouted-service requests observed at the gateway trends to zero**.

## Success Metrics

| Metric | Target | How measured |
|---|---|---|
| Registered AWS services | 431 (100% of upstream models) | registered plugin count vs. upstream model count, asserted in CI |
| Services meeting the quality floor | 100% of registered | fidelity manifest reports ≥1 served operation **and** a passing compatibility test exists for the service |
| Per-service onboarding cost | TBD — baseline set in Milestone 1, target set from it | wall-clock time from service name to merged, measured across Milestone 2 |
| Unrouted-service request rate | trends to zero | TBD — requires gateway instrumentation that does not exist yet |
| Compatibility suite runtime | stays within an agreed CI budget | CI job duration; budget is an open question below |

## Scope

**MVP** — A repeatable, measured service-onboarding capability, proven by taking
one complete AWS service category from zero to the quality floor. The MVP answers
one question: *what does adding a single service actually cost once the repetitive
work is automated?* If that number is small, the remaining ~310 services are an
arithmetic problem rather than a research problem. If it is not small, the 100%
target is re-scoped before any bulk investment.

**Quality floor** — a service counts as "covered" only when both hold:
1. CRUD-shaped operations are served from a real store; everything else returns a
   clean AWS error rather than a fabricated success.
2. At least one boto3 compatibility test exercises the service and passes in CI.

**Out of scope**
- **Azure / GCP expansion** — deferred to the multi-CSP phase. AWS completion is
  its precondition, not its companion.
- **Depth on newly added services** — no business logic, validation, or
  cross-resource integrity. The floor is the ceiling for this effort.

**Deliberately left in scope but unresolved** — filling the 2,033 existing
`unimplemented` operations, non-JSON-protocol services, and cross-service
integration for new services were each offered as exclusions and not taken. They
are not out of scope, but none is funded by the MVP. See Open Questions.

## Delivery Milestones

<!-- Business outcomes, not engineering tasks. /plan turns each into a plan. -->
<!-- Status: pending | in-progress | complete -->

| # | Milestone | Outcome | Status | Plan |
|---|---|---|---|---|
| 1 | Onboarding cost is known | Adding one AWS service is a single bounded operation with a measured wall-clock cost, not a bespoke effort. The baseline number exists. | in-progress | `.claude/plans/aws-service-coverage-100.plan.md` |
| 2 | One category proven end to end | Every service in one AWS category is registered and meets the quality floor. Per-service cost is confirmed against the Milestone 1 estimate. | pending | — |
| 3 | Demand validated, or target re-scoped | The assumption in Evidence is tested. Either the remaining services are confirmed as wanted, or the 100% target is publicly narrowed before bulk investment. | pending | — |
| 4 | JSON-protocol surface complete | Every upstream JSON-protocol AWS service is registered and meets the floor. | pending | — |
| 5 | Non-JSON surface resolved | Query / EC2 Query / REST-XML services either meet a stated floor or are excluded from the "100%" claim in published docs. No silent gap. | pending | — |
| 6 | Coverage claim published and gated | Public docs state the coverage figure and its real depth. CI fails if a registered service drops below the floor or the count regresses. | pending | — |

## Open Questions

- [ ] **Does the "100%" claim include non-JSON-protocol services?** The chosen
      quality floor depends on generic CRUD serving, which today applies only to
      JSON-family protocols — roughly 29% of models are Query, EC2 Query, or
      REST-XML. Either the floor gains a second definition for those, or "100%"
      means "100% of JSON-protocol services". This changes the denominator and
      must be settled before Milestone 4.
- [ ] **Is 431 the right denominator?** The upstream directory includes runtime
      split-outs and variants that are not separate services from a user's point
      of view. The target number needs a defensible definition.
- [ ] **What is the CI budget ceiling?** +~330 compatibility tests and ~330
      registered plugins will lengthen every CI run. If the suite outgrows the
      budget, the per-service test requirement needs a different execution model.
- [ ] **What does 431 registered services cost at runtime?** Binary size, image
      size, startup time, and idle memory are currently sized for 104. The
      zero-config, single-binary promise is a stated product property and could
      break before the coverage target is reached.
- [ ] **Does the fidelity manifest remain usable at ~30,000 entries?** It is
      already noted as too large to return unfiltered at 7,481.
- [ ] **What is the sustaining cost?** The weekly upstream sync currently reviews
      93 models. At 431, routine model churn becomes an ongoing maintenance load
      that no one has budgeted.
- [ ] **Who owns the ongoing work?** Scope at this size is explicitly noted in the
      project's own guiding principles as beyond a single maintainer.

## Risks

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| Breadth-without-depth backlash — "431 services" is read as a capability claim, then a user finds most operations are store-backed stubs with no logic, and trusts nothing | High | High | Publish the depth split next to the count everywhere the count appears; never state the service number without the fidelity breakdown; keep the "clean error, never a false success" guarantee absolute |
| Building ~330 services nobody asked for | High | High | Milestone 3 gates bulk investment on demand validation; the MVP deliberately costs one category, not 330 services |
| Non-JSON services cannot meet the stated floor, leaving a silent gap inside a "100%" claim | High | Medium | Milestone 5 forces an explicit, published resolution; no partial claim ships |
| CI runtime or cost becomes the binding constraint | Medium | Medium | Set the budget ceiling as an explicit target before Milestone 4; treat suite runtime as a tracked metric, not a side effect |
| Single-binary / zero-config property breaks under 431 services | Medium | High | Measure binary size, startup, and idle memory as part of Milestone 2, while the sample is still 20 services |
| Upstream model churn across 431 models exceeds review capacity | Medium | High | Size the sustaining load from Milestone 2's category before committing to Milestone 4 |
| Multi-CSP roadmap stalls behind an AWS target that keeps expanding | Medium | Medium | Milestone 3 provides a sanctioned off-ramp to narrow the target rather than defer the roadmap indefinitely |

---
*Status: DRAFT — requirements only. Implementation planning pending via /plan.*
