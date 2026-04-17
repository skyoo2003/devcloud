# Contributing to DevCloud

Thanks for your interest in contributing! This file is a short pointer — the full contributor guide lives at [docs/contributing.md](docs/contributing.md).

## Quick links

- **Development setup, testing, codegen, and adding new services**: [docs/contributing.md](docs/contributing.md)
- **Architecture overview**: [docs/architecture.md](docs/architecture.md)
- **Code of Conduct**: [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md)
- **Security policy**: [SECURITY.md](SECURITY.md)

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

Conventional prefixes are preferred: `feat:`, `fix:`, `chore:`, `docs:`, `test:`, `refactor:`. This helps release-drafter categorize changes automatically.

## License of Contributions

DevCloud is licensed under the [Apache License, Version 2.0](LICENSE). By submitting a pull request, you agree that your contribution will be licensed under the same terms.

New Go files must include the SPDX header on the first line:

```go
// SPDX-License-Identifier: Apache-2.0
```

The `go-spdx-header` pre-commit hook auto-adds this header if missing.
