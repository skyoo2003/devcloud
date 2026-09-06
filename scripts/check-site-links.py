#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""scripts/check-site-links.py — verify every internal link in the built site
resolves to a page that exists.

docs/*.md deliberately carry no front matter and no Hugo ref shortcodes: the
same files are read on github.com, where `{{< ref >}}` is not markdown and a
metadata table above every page is noise. They link each other with plain
relative paths instead, and layouts/_markup/render-link.html turns those into
site URLs at build time.

That hook cannot fail loudly. Links leaving docs/ — `../internal/gateway/router.go`,
`../CONTRIBUTING.md` — are legitimate and resolve to GitHub blob URLs, so the
hook treats "no page found" as that case. A page renamed without its inbound
links updated therefore does not break the build; it silently becomes a link to
a file path on GitHub that may not exist either. Hugo's own refLinksErrorLevel
does not apply, because nothing here uses refs.

So the check runs after the build, against the output: every /devcloud/ URL the
site emits must correspond to a file under public/. Exits non-zero with the
offending link and the page it came from.
"""

from __future__ import annotations

import pathlib
import re
import sys
from urllib.parse import unquote, urlsplit

PUBLIC = pathlib.Path(__file__).resolve().parent.parent / "public"

# Minified output drops the quotes, so both forms have to match.
HREF = re.compile(r"""(?:href|src)=(?:"([^"]*)"|'([^']*)'|([^\s>]+))""")


def targets(html: str):
    for match in HREF.finditer(html):
        yield match.group(1) or match.group(2) or match.group(3)


def main() -> int:
    if not PUBLIC.is_dir():
        print(f"{PUBLIC} not found — run `hugo` first", file=sys.stderr)
        return 2

    pages = sorted(PUBLIC.rglob("*.html"))
    if not pages:
        print(f"no HTML under {PUBLIC} — the build produced nothing", file=sys.stderr)
        return 2

    broken: list[tuple[str, str]] = []
    checked = 0

    for page in pages:
        source = page.relative_to(PUBLIC)
        for target in targets(page.read_text(encoding="utf-8")):
            path = urlsplit(target).path
            # Only site-internal links are ours to guarantee. Absolute URLs are
            # the GitHub fallbacks and outbound references; fragments and
            # mailto: have no file behind them.
            if not path.startswith("/devcloud/"):
                continue
            checked += 1
            relative = unquote(path[len("/devcloud/") :]).strip("/")
            candidates = [PUBLIC / relative, PUBLIC / relative / "index.html"]
            if not any(candidate.exists() for candidate in candidates):
                broken.append((target, str(source)))

    if broken:
        print(f"{len(broken)} internal link(s) point at nothing:", file=sys.stderr)
        for target, source in sorted(set(broken)):
            print(f"  {target}  (from {source})", file=sys.stderr)
        print(
            "\nA page was probably renamed or removed without updating the "
            "docs/*.md that link to it, or hugo.toml's [menu.before] still "
            "lists it.",
            file=sys.stderr,
        )
        return 1

    print(f"{checked} internal links across {len(pages)} pages all resolve")
    return 0


if __name__ == "__main__":
    sys.exit(main())
