# Getting Help

Pick the right channel below to get a fast, accurate response.

## Where to ask

| Your situation | Channel |
|---|---|
| **Bug** — something is broken or behaves unexpectedly | [Open a Bug Report](https://github.com/skyoo2003/devcloud/issues/new?template=bug_report.yml) |
| **Feature request** — you want new functionality | [Open a Feature Request](https://github.com/skyoo2003/devcloud/issues/new?template=feature_request.yml) |
| **Question** — "How do I …?" / "Why does …?" / "Is X possible?" | [Open a Question](https://github.com/skyoo2003/devcloud/issues/new?template=question.yml) or start a [GitHub Discussion](https://github.com/skyoo2003/devcloud/discussions) |
| **Security vulnerability** — potential CVE, auth bypass, data leak | **Do not open a public issue.** Follow [SECURITY.md](SECURITY.md) |
| **Code of Conduct concern** | Contact the maintainer per [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md) |

## Before you ask

Most questions already have answers:

1. **Search existing issues** — [open](https://github.com/skyoo2003/devcloud/issues) + [closed](https://github.com/skyoo2003/devcloud/issues?q=is%3Aissue+is%3Aclosed)
2. **Check [Troubleshooting](docs/troubleshooting.md)** and the [FAQ](docs/faq.md)
3. **Confirm the operation is served** — `curl -s 'localhost:4747/devcloud/api/fidelity?service=<id>'` with `admin.enabled: true`; see [fidelity-manifest.md](docs/fidelity-manifest.md)
4. **Check the [roadmap](docs/roadmap.md)** — the request may already be planned

## How to write a good question

- **What you tried** — the exact command, snippet, or SDK call
- **What you expected**, and **what actually happened** — full error message
- **Environment** — OS, language version, DevCloud version (git SHA or image tag)
- **Minimal reproduction** — the smallest example that shows the issue

## Response expectations

DevCloud is maintained by volunteers on a best-effort basis. There is no commercial SLA.

- **Bugs and security reports** — triaged within a few days
- **Feature requests** — reviewed against the [roadmap](docs/roadmap.md)
- **Questions** — answered when a maintainer or community member has time

If you need guaranteed response times, consider sponsoring the project or contributing the fix yourself — see [CONTRIBUTING.md](CONTRIBUTING.md).
