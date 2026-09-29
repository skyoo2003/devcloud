# Contributing to DevCloud

Thanks for your interest in contributing! This file is a short pointer — the full contributor guide lives at [docs/contributing.md](docs/contributing.md).

## Quick links

- **Development setup, testing, codegen, and adding new services**: [docs/contributing.md](docs/contributing.md)
- **Architecture overview**: [docs/architecture.md](docs/architecture.md)
- **Code of Conduct**: [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md)
- **Security policy**: [SECURITY.md](SECURITY.md)

## Project layout

DevCloud follows the community [Standard Go Project Layout](https://github.com/golang-standards/project-layout). Treat the directory's purpose as part of the contribution contract: place new files in the matching location rather than creating a parallel top-level directory.

| Location | Use it for |
| --- | --- |
| `cmd/devcloud` | The production server executable; keep `main` packages thin. |
| `internal` | Private application code, including services and generated Go output. Do not expose it as a public API. |
| `tools/codegen` | Repository support tools, including the Smithy code generator. |
| `api/smithy` | Versioned Smithy API model inputs for code generation. |
| `test/compatibility` | External boto3/Python compatibility tests and their fixtures. |
| `build/package` | Container build and release packaging files. |
| `deployments` | Deployment definitions such as Docker Compose. |
| `scripts` | Build, generation, validation, and maintenance scripts. |
| `docs` | Contributor, architecture, and user documentation. |

Do not add a `src/` directory. Add code to `pkg/` only when it is intentionally supported for import by external Go modules; application-private code belongs in `internal/`. Keep runtime data in the ignored root `data/` directory and build output in `dist/`.

## Reporting issues

- **Bug**: use the [Bug Report](https://github.com/skyoo2003/devcloud/issues/new?template=bug_report.yml) template
- **Feature request**: use the [Feature Request](https://github.com/skyoo2003/devcloud/issues/new?template=feature_request.yml) template
- **Question**: use the [Question](https://github.com/skyoo2003/devcloud/issues/new?template=question.yml) template
- **Security vulnerability**: do not open a public issue — see [SECURITY.md](SECURITY.md)

## Pull request workflow

1. Fork the repo and create a feature branch from `main`
2. Make your changes (follow the guidelines in [docs/contributing.md](docs/contributing.md))
3. Run `make test` and ensure lint passes (`golangci-lint run`)
4. Open a PR against `main` — the PR template will guide you
5. CI must be green before review

## Commit message style

Conventional prefixes are preferred: `feat:`, `fix:`, `chore:`, `docs:`, `test:`, `refactor:`.

Release notes are **not** generated from commits — they come from [Changie](https://changie.dev) fragments. Add one for any user-facing change with `changie new` — one sentence, two at most (see [RELEASE.md](RELEASE.md#one-sentence-two-at-most)).

## License of Contributions

DevCloud is licensed under the [Apache License, Version 2.0](LICENSE). By submitting a pull request, you agree that your contribution will be licensed under the same terms.

New Go files must include the SPDX header on the first line:

```go
// SPDX-License-Identifier: Apache-2.0
```

The `go-spdx-header` pre-commit hook auto-adds this header if missing.
