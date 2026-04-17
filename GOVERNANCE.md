# Governance

DevCloud is a small, volunteer-driven open-source project. This document describes how decisions are made and how the roles evolve as the project grows.

## Current state (0.x)

- **Benevolent maintainer model.** One maintainer ([@skyoo2003](https://github.com/skyoo2003)) has final say on roadmap, releases, and merge decisions.
- **Apache License 2.0.** All contributions are accepted under this license; see [CONTRIBUTING.md](CONTRIBUTING.md).
- **Best-effort response.** No SLA; see [SUPPORT.md](SUPPORT.md) for expectations.

This is appropriate while the project is pre-1.0 and scope is still being proven.

## How decisions are made

| Decision type | Who decides | How |
|---|---|---|
| Small bug fixes | Maintainer (self-merge) | Reviewer approval + green CI |
| Features, new services | Maintainer | After PR review; contributor discussion welcomed in issues |
| Architecture changes | Maintainer | Open design issue first; gather input before implementation |
| Roadmap priorities | Maintainer | Guided by [roadmap.md](docs/roadmap.md) and issue reactions |
| Security fixes | Maintainer + reporter | Follow [SECURITY.md](SECURITY.md); patches issued before public disclosure |
| Releases | Maintainer | Following semver; see [CHANGELOG.md](CHANGELOG.md) |

## How to influence direction

- **Open issues.** Features, bugs, questions — all go through [Issues](https://github.com/skyoo2003/devcloud/issues) or [Discussions](https://github.com/skyoo2003/devcloud/discussions).
- **React to existing issues** to surface priority.
- **Submit PRs.** Working code speaks loudest. See [CONTRIBUTING.md](CONTRIBUTING.md).
- **Discuss architecture early.** For non-trivial changes, open a design issue before writing code.

## Future governance

As DevCloud grows, the governance model will evolve. Likely milestones:

- **2+ active maintainers** — introduce a lightweight maintainer charter (who can merge what, conflict resolution).
- **First external contributor with sustained commits** — document maintainer eligibility criteria.
- **Foundation consideration** — if the project reaches the scale where vendor-neutral governance matters, a formal foundation model will be evaluated.

Until then, this one-page document is the full governance.

## Maintainers

- **Current:** [@skyoo2003](https://github.com/skyoo2003) (lead, all areas)

See [.github/CODEOWNERS](.github/CODEOWNERS) for review ownership.

## Changes to this document

Meta-changes (governance model, maintainer list) are themselves governed by this process: open an issue, discuss, PR. Substantial governance changes (e.g., moving to a multi-maintainer model) will be publicized in [CHANGELOG.md](CHANGELOG.md).
