# Releasing DevCloud

Releases go out through **[Changie](https://changie.dev)** (changelog) and
**[GoReleaser](https://goreleaser.com)** (build + publish). Release notes are
authored as Changie fragments during development and batched at release time —
commits and PR labels are **not** used to generate them.

Versions follow [Semantic Versioning](https://semver.org): `vMAJOR.MINOR.PATCH`.

## Two pipelines

| Pipeline | Trigger | Produces |
|----------|---------|----------|
| **Release** ([`release.yml`](.github/workflows/release.yml)) | pushing a `v*` tag, or manual dispatch | GitHub Release with binaries and checksums, versioned `*-alpine` images, a Homebrew formula |
| **CD** ([`cd.yml`](.github/workflows/cd.yml)) | every successful CI run on a push | rolling multi-arch `latest` / branch images to GHCR |

CD keeps `ghcr.io/skyoo2003/devcloud:latest` current with `main`. This document
covers the Release pipeline.

## During development: add a fragment

Any user-facing change should carry one:

```sh
changie new
```

You are prompted for a **kind** (`Added`, `Changed`, `Deprecated`, `Removed`,
`Fixed`, `Security`, `Documentation`), a **body**, and the **issue number**. It
writes a small YAML file under `changes/unreleased/` — commit it alongside your
code. Prefer `changie new` over hand-writing the YAML: it enforces the issue
number, and a fragment without one renders as a dead link.

### One sentence. Two at most.

The changelog is a scan surface, not a design document — the reader is deciding
whether this release affects them. Root cause, mechanism, measurements and
rejected alternatives belong in the linked issue and PR, which every entry
already points at.

- **One sentence** — what changed, and what it means for someone using DevCloud.
- **A second only if a reader must not miss it** — a limit, a behaviour change
  they have to act on, or the one figure that makes the entry meaningful.
- **Never a third.** If it needs one, it is either two changes (write two
  fragments) or a story that belongs in the issue.

```yaml
# too long — the root cause, the mechanism and the evidence all belong in #142
body: 'S3 Control requests were served by S3. `s3control` signs with S3''s own
  signing name, so every call fell through to the REST-XML default and the S3
  provider parsed it as a bucket and key — `CreateAccessPoint` returned 200 and
  left an object in a bucket named `v20180820`. S3 Control is now split off by its
  `/v20180820/` path prefix, and its unserved operations return a clean AWS error
  instead of a fabricated success'

# right length
body: 'S3 Control requests were served by S3, which answered `CreateAccessPoint`
  with a fabricated 200. It is now split off by its `/v20180820/` path prefix, and
  its unserved operations return a clean AWS error'
```

Config: [`.changie.yaml`](.changie.yaml).

## Pre-flight checklist

The first three are re-run by the Release workflow against the tagged commit,
which refuses to publish if any fails — tick them to find out on your machine
rather than from a failed tag. The rest are only caught here.

- [ ] **Generated code is current** —
      `rm -rf internal/generated && make codegen && git status --porcelain internal/generated`
      prints nothing. Clear the tree first, as CI does: the generator overwrites
      the outputs it still emits but never removes one it has stopped emitting, so
      regenerating in place leaves a retired file looking current. A stale fidelity
      manifest misreports what the release can be trusted to do.
- [ ] **boto3 compatibility passes** — `make test-compat`.
- [ ] **Go tests pass** — `CGO_ENABLED=0 go test ./...`.
- [ ] **`main` is green**, including lint and CodeQL.
- [ ] **Every unreleased fragment carries an issue number** —
      `grep -L 'Issue: "[0-9]' changes/unreleased/*.yaml` prints nothing.
- [ ] **`changes/unreleased/` is not empty.** No fragments means either nothing
      shipped or someone forgot one.
- [ ] **Every fragment body is one sentence, two at most.** A third sentence is
      either a second change that needs its own fragment or detail that belongs in
      the issue.
- [ ] **Deprecation review.** If this release *removes* anything previously
      deprecated — a config key, an env var, an admin route — confirm it shipped
      for at least one release with a warning first. Removing without that overlap
      is a major-version change. Full procedure:
      [docs/compatibility-policy.md](docs/compatibility-policy.md#deprecation-procedure).
- [ ] **Compatibility review.** Changing anything on the guaranteed list in
      [docs/compatibility-policy.md](docs/compatibility-policy.md) is a major bump
      — or it is a bug. Additive change (a new config key, response field, or
      service) is a minor bump.

## Cutting a release

1. **Confirm `main` is green** and holds everything you want in the release.

2. **Batch the fragments**, picking the next version per SemVer:

   ```sh
   make changelog VERSION=v1.1.0
   ```

   That is `changie batch v1.1.0 && changie merge`: it consumes
   `changes/unreleased/`, writes `changes/v1.1.0.md`, and regenerates
   [`CHANGELOG.md`](CHANGELOG.md). Run the two commands separately if you want to
   inspect the batched file first.

3. **Commit** the generated files:

   ```sh
   git add changes/ CHANGELOG.md
   git commit -m "chore(release): v1.1.0"
   git push origin main
   ```

4. **Tag and push.** The tag **must** match the batched version — the workflow
   fails if `changes/<tag>.md` does not exist.

   ```sh
   git tag v1.1.0
   git push origin v1.1.0
   ```

## What the tag triggers

- **Pins the commit once.** The tag is resolved to a SHA up front and that same
  SHA is checked out in every following job, so moving the tag mid-run cannot make
  the guardrails vouch for one commit while GoReleaser publishes another.
- **Re-runs the guardrails** against that commit and stops before publishing
  anything if any fails: the Go suite on amd64 and arm64, the boto3 suite (against
  a GoReleaser-built binary, not `go build`), and the codegen drift check. CI is
  not relied on — it races the tag, and `compat.yml` does not trigger on tags at
  all.
- **Validates the notes.** `changes/v1.1.0.md` must exist, carry exactly one
  version heading naming *this* tag (a file copied from an earlier release is
  rejected), contain only what `changie batch` renders, and end every entry in a
  valid issue link.
- **Builds and publishes** — binaries for darwin/linux/windows × amd64/arm64 as
  `tar.gz` (`zip` on Windows) with a SHA-256 `CHECKSUMS` file; container images
  tagged `v1.1.0-alpine`, `v1.1-alpine`, `v1-alpine`, `latest-alpine`; the
  Homebrew formula; and a GitHub Release whose notes come from
  `changes/v1.1.0.md`.

Config: [`.goreleaser.yaml`](.goreleaser.yaml).

Archives carry the `docs/` tree and the top-level files it and `README.md` link
to, so docs are versioned by tag — the `docs/` inside
`devcloud_v1.1.0_linux_amd64.tar.gz` describes exactly the binary beside it. There
is no separate docs site to version.

## Homebrew tap

GoReleaser's `brews` section publishes `Formula/devcloud.rb` to a separate tap
repository — `homebrew-tap` under the same owner, named by `HOMEBREW_TAP_OWNER` /
`HOMEBREW_TAP_REPO` in [`release.yml`](.github/workflows/release.yml).

The job's own `GITHUB_TOKEN` cannot write to another repository, so the workflow
mints a short-lived token from a GitHub App installed **only** on the tap repo,
using the `TAP_APP_ID` and `TAP_APP_PRIVATE_KEY` secrets. If a release fails at
the formula step, check that the App is still installed and neither secret has
expired. Token minting is skipped on a dry run.

The formula's `test` block only asserts the `-h` usage text: `devcloud` is a
long-running server with no subcommands, so actually starting it would hang.

## Dry run

Run the workflow manually from the Actions tab (**Release → Run workflow**) with a
tag and `dry_run: true` (the default for manual dispatch). GoReleaser runs in
`--snapshot` mode: artifacts are built and uploaded to the run, but nothing is
published to GHCR, the tap, or GitHub Releases.

## Requirements recap

- The tag (`v1.1.0`) and the fragment file (`changes/v1.1.0.md`) must match exactly.
- `changie batch` + `changie merge` must be committed **before** the tag is pushed.
- The tagged commit must pass the Go suite; the workflow will not publish otherwise.
- Every entry in the batched notes needs an issue number.
- No manual GitHub Release editing — the notes are owned by Changie fragments.
