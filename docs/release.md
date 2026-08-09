# Releasing DevCloud

DevCloud ships versioned releases through **[Changie](https://changie.dev)** (changelog
management) and **[GoReleaser](https://goreleaser.com)** (build + publish). Release notes
are authored as Changie fragments during development, batched into a version file, and
handed to GoReleaser at tag time — commits and PR labels are **not** used to generate
release notes.

Versions follow [Semantic Versioning](https://semver.org): `vMAJOR.MINOR.PATCH`.

## Two pipelines

| Pipeline | Trigger | What it produces | Workflow |
|----------|---------|------------------|----------|
| **Release** | pushing a `v*` tag (or manual dispatch) | GitHub Release with binaries, checksums, and versioned `*-alpine` container images | [`.github/workflows/release.yml`](../.github/workflows/release.yml) |
| **CD** | every successful CI run on a push | rolling multi-arch `latest` / branch container images to GHCR | [`.github/workflows/cd.yml`](../.github/workflows/cd.yml) |

CD keeps `ghcr.io/skyoo2003/devcloud:latest` current with `main`. The Release pipeline is
what produces an actual tagged, downloadable release. This document covers the Release pipeline.

## During development: add a changelog fragment

Any user-facing change should carry a Changie fragment. From the repo root:

```sh
changie new
```

You'll be prompted for a **kind** (`Added`, `Changed`, `Deprecated`, `Removed`, `Fixed`,
`Security`, `Documentation`), a one-line **body**, and the **issue number**. This writes a
small YAML file under `changes/unreleased/`. Commit it alongside your code change.

Config lives in [`.changie.yaml`](../.changie.yaml).

Prefer `changie new` over writing the YAML by hand: it enforces the issue number, and a
fragment without one renders as a dead link in the release notes.

## Pre-flight checklist

Run through this before batching. CI enforces most of it — the list is for catching problems
before the tag, not instead of CI.

- [ ] **`main` is green.** CI, lint, CodeQL and compat.
- [ ] **Generated code is current** — `make codegen && git diff --exit-code internal/generated`.
      `internal/generated` is committed but derived; a stale fidelity manifest misreports what
      the release can be trusted to do. Enforced by CI's `codegen-drift` job.
- [ ] **boto3 compatibility passes** — `make test-compat`.
- [ ] **Go tests pass** — `CGO_ENABLED=1 go test ./...`. The Release workflow re-runs this and
      refuses to publish if it fails.
- [ ] **Every unreleased fragment carries an issue number** —
      `grep -L 'Issue: "[0-9]' changes/unreleased/*.yaml` prints nothing.
- [ ] **`changes/unreleased/` is not empty.** No fragments means either nothing shipped or
      someone forgot one.
- [ ] **Deprecation review.** If this release *removes* anything previously deprecated — a
      config key, an env var, an admin route — confirm it shipped for at least one release
      with a warning first. The precedent is the `dashboard` → `admin` rename in
      [`internal/config/config.go`](../internal/config/config.go): the old key kept working,
      emitted a warning, and only then became removable. Removing without that overlap is a
      major-version change.

## Cutting a release

1. **Make sure `main` is green** and holds all changes you want in the release.

2. **Batch the unreleased fragments** into a version file. Pick the next version per SemVer:

   ```sh
   changie batch v0.3.0
   ```

   This consumes everything in `changes/unreleased/` and writes `changes/v0.3.0.md`.

3. **Merge into the changelog:**

   ```sh
   changie merge
   ```

   This regenerates [`CHANGELOG.md`](../CHANGELOG.md) from all version files.

4. **Commit** the generated files:

   ```sh
   git add changes/ CHANGELOG.md
   git commit -m "chore(release): v0.3.0"
   git push origin main
   ```

5. **Tag and push.** The tag name **must** match the batched version — the Release workflow
   fails if `changes/<tag>.md` does not exist.

   ```sh
   git tag v0.3.0
   git push origin v0.3.0
   ```

Pushing the tag triggers the Release workflow. It will:

- run the Go test suite and stop before publishing anything if it fails,
- verify `changes/v0.3.0.md` exists (guard against tagging without release notes) and that no
  entry in it is missing its issue number,
- run GoReleaser, which builds binaries for **darwin/linux/windows × amd64/arm64**, packages
  them as `tar.gz` (`zip` on Windows) with `LICENSE`/`README.md`/`CHANGELOG.md` and the `docs/`
  tree, and generates a SHA-256 `CHECKSUMS` file,
- build and push `*-alpine` container images to `ghcr.io/skyoo2003/devcloud`,
- publish a **GitHub Release** whose notes come from `changes/v0.3.0.md`
  (`--release-notes`, `mode: replace`).

GoReleaser config: [`.goreleaser.yaml`](../.goreleaser.yaml).

## Dry run

To validate the build without publishing, run the workflow manually from the Actions tab
(**Release → Run workflow**) with a tag and `dry_run: true`. This runs GoReleaser in
`--snapshot` mode: it builds artifacts and uploads them to the run, but publishes nothing to
GHCR or GitHub Releases.

## Requirements recap

- The tag (`v0.3.0`) and the fragment file (`changes/v0.3.0.md`) must match exactly.
- `changie batch` + `changie merge` must be committed **before** the tag is pushed.
- The tagged commit must pass the Go test suite; the workflow will not publish otherwise.
- Every entry in the batched notes needs an issue number.
- No manual GitHub Release editing — release notes are owned by Changie fragments.

Docs ship inside the release archive, so they are versioned by tag: the `docs/` tree in
`devcloud_v0.3.0_linux_amd64.tar.gz` describes exactly the binary beside it. There is no
separate docs site to version.
