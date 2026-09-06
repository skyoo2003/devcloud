#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""scripts/model_churn.py — summarise what changed in the vendored Smithy models.

The weekly sync regenerates from all 194 committed models at once, so its pull
request is a whole-tree diff whether upstream changed one model or ninety. A
reviewer reading tens of thousands of lines of regenerated Go cannot tell which
service moved, let alone whether an operation appeared or only a doc-string was
reworded. This script answers the one question a reviewer actually has: *which
operations moved?*

What it does not do: it does not re-implement codegen, and it does not decide
whether a change is safe. It reads operation shape names and documentation
traits — nothing about protocols, bindings, or types. If it ever disagrees with
codegen, codegen is right.

Run from the workflow after `download-smithy-models.sh --refresh`, or by hand.
Like demand_rank.py, a source that reads wrong exits non-zero rather than
emitting a summary that under-reports.

usage: model_churn.py [--models-dir smithy-models] [--base-ref HEAD]
                      [--upstream] [--self-check] [--out -]
"""

from __future__ import annotations

import argparse
import json
import os
import pathlib
import re
import subprocess
import sys
import urllib.error
import urllib.request

REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent

DOC_TRAIT = "smithy.api#documentation"

UPSTREAM_MODELS_API = (
    "https://api.github.com/repos/aws/aws-sdk-go-v2/contents/"
    "codegen/sdk-codegen/aws-models?ref=main"
)


def die(msg: str) -> None:
    print(f"ERROR: {msg}", file=sys.stderr)
    sys.exit(1)


def service_operations(model: dict) -> set[str]:
    """The operation names a Smithy model declares.

    Every operation is a top-level shape keyed `<namespace>#<Name>` with
    `"type": "operation"`. Reading the shape map rather than the service shape's
    `operations` list catches an operation upstream defined but has not yet
    wired into the service — a real upstream state, and one a reviewer should
    see coming.
    """
    raise NotImplementedError


def strip_documentation(node: object) -> object:
    """Return node with every `smithy.api#documentation` trait removed.

    Documentation traits appear on shapes, on members, and on nested structures,
    so the removal is recursive rather than a pass over the top-level map.
    """
    raise NotImplementedError


def is_documentation_only(old: dict, new: dict) -> bool:
    """True when the two models differ only in documentation text.

    This is the reading that decides whether a sync needs a review or a glance.
    It is deliberately strict: anything that is not a documentation trait — a
    trait value, a member, an enum entry — makes the change non-cosmetic.
    """
    raise NotImplementedError


def _self_check() -> None:
    """Assert the three readings above on synthetic models.

    Synthetic rather than fixture files: the properties under test are about
    shape maps and trait nesting, and a real multi-megabyte model would bury
    them.
    """

    def op(doc: str | None = None) -> dict:
        shape: dict = {"type": "operation"}
        if doc is not None:
            shape["traits"] = {DOC_TRAIT: doc}
        return shape

    ns = "com.amazonaws.things"

    # 1. Operation names come out of the shape map; non-operations do not.
    model = {
        "shapes": {
            f"{ns}#ListThings": op(),
            f"{ns}#GetThing": op(),
            f"{ns}#Thing": {"type": "structure"},
            f"{ns}#Things": {"type": "service"},
        }
    }
    assert service_operations(model) == {"ListThings", "GetThing"}, service_operations(
        model
    )

    # 2. A model with no shapes has no operations rather than raising.
    assert service_operations({}) == set()

    # 3. A reworded doc-string is documentation-only.
    old = {"shapes": {f"{ns}#ListThings": op("Lists things.")}}
    new = {"shapes": {f"{ns}#ListThings": op("Lists all the things.")}}
    assert is_documentation_only(old, new)

    # 4. An added operation is not, even though its doc-string is the only new text.
    new_with_op = {
        "shapes": {
            f"{ns}#ListThings": op("Lists things."),
            f"{ns}#DeleteThing": op("Deletes a thing."),
        }
    }
    assert not is_documentation_only(old, new_with_op)

    # 5. Documentation nested on a member is stripped too — the common case, and
    #    the one a top-level-only strip would misreport as semantic churn.
    deep_old = {
        "shapes": {
            f"{ns}#Thing": {
                "type": "structure",
                "members": {
                    "Name": {"target": "smithy.api#String", "traits": {DOC_TRAIT: "A."}}
                },
            }
        }
    }
    deep_new = json.loads(json.dumps(deep_old))
    deep_new["shapes"][f"{ns}#Thing"]["members"]["Name"]["traits"][DOC_TRAIT] = "B."
    assert is_documentation_only(deep_old, deep_new)

    # 6. A changed member target is semantic, not cosmetic.
    deep_new["shapes"][f"{ns}#Thing"]["members"]["Name"]["target"] = (
        "smithy.api#Integer"
    )
    assert not is_documentation_only(deep_old, deep_new)

    print("self-check OK")


def git(args: list[str]) -> str:
    proc = subprocess.run(
        ["git", *args], cwd=REPO_ROOT, capture_output=True, text=True, check=False
    )
    if proc.returncode != 0:
        die(f"git {' '.join(args)} failed: {proc.stderr.strip()}")
    return proc.stdout


def changed_models(models_dir: str, base_ref: str) -> list[str]:
    """Model filenames (without .json) that differ from base_ref."""
    out = git(["diff", "--name-only", base_ref, "--", models_dir])
    return sorted(
        pathlib.PurePosixPath(line).stem
        for line in out.splitlines()
        if line.endswith(".json")
    )


def model_at_ref(models_dir: str, name: str, base_ref: str) -> dict:
    raw = git(["show", f"{base_ref}:{models_dir}/{name}.json"])
    try:
        return json.loads(raw)
    except json.JSONDecodeError as exc:
        die(f"{name}.json at {base_ref} is not valid JSON: {exc}")
    raise AssertionError("unreachable")


def model_on_disk(models_dir: str, name: str) -> dict:
    path = REPO_ROOT / models_dir / f"{name}.json"
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        die(f"{path} could not be read: {exc}")
    raise AssertionError("unreachable")


def upstream_not_in_tree(models_dir: str) -> tuple[int, int]:
    """(upstream model count, count not vendored here).

    Uses the one endpoint the sync's own download script already depends on. The
    three emulator sources demand_rank.py reads stay hand-run: they are
    third-party, they change under us, and they belong in a deliberate
    re-derivation rather than on a cron.
    """
    req = urllib.request.Request(
        UPSTREAM_MODELS_API, headers={"User-Agent": "devcloud-model-churn"}
    )
    token = os.environ.get("GITHUB_TOKEN")
    if token:
        req.add_header("Authorization", f"Bearer {token}")
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            entries = json.loads(resp.read())
    except urllib.error.HTTPError as exc:
        hint = (
            " — GitHub rate limit; set GITHUB_TOKEN and retry"
            if exc.code in (403, 429)
            else ""
        )
        die(f"{UPSTREAM_MODELS_API} returned HTTP {exc.code}{hint}")
    except urllib.error.URLError as exc:
        die(f"{UPSTREAM_MODELS_API} unreachable: {exc.reason}")

    names = {e["name"][:-5] for e in entries if e["name"].endswith(".json")}
    if not names:
        die("upstream model listing returned no .json files")
    local = {p.stem for p in (REPO_ROOT / models_dir).glob("*.json")}
    return len(names), len(names - local)


def render(
    rows: list[dict],
    doc_only: list[str],
    total: int,
    upstream: tuple[int, int] | None,
) -> str:
    lines: list[str] = []
    semantic = [r for r in rows if r["added"] or r["removed"]]

    lines.append(
        f"**{len(rows)} of {total} vendored models changed.** "
        f"{len(semantic)} moved an operation; {len(doc_only)} are documentation-only."
    )
    lines.append("")

    if semantic:
        lines.append("| Service | Operations added | Operations removed |")
        lines.append("|---|---|---|")
        for r in semantic:
            added = ", ".join(f"`{o}`" for o in r["added"]) or "—"
            removed = ", ".join(f"`{o}`" for o in r["removed"]) or "—"
            lines.append(f"| `{r['name']}` | {added} | {removed} |")
        lines.append("")
        lines.append(
            "An operation added or removed moves the fidelity manifest, so the "
            "published-figure gate over `docs/coverage.md` is expected to fail. "
            "Re-derive the figures in this PR rather than silencing the gate."
        )
    else:
        lines.append("No operation was added or removed by any changed model.")
    lines.append("")

    other = [
        r["name"]
        for r in rows
        if not (r["added"] or r["removed"]) and r["name"] not in doc_only
    ]
    if other:
        lines.append(
            "Changed with the same operation set but not documentation-only "
            "(shapes, traits or members moved): " + ", ".join(f"`{n}`" for n in other)
        )
        lines.append("")

    if doc_only:
        lines.append(
            f"<details><summary>Documentation-only ({len(doc_only)})</summary>"
        )
        lines.append("")
        lines.append(", ".join(f"`{n}`" for n in doc_only))
        lines.append("")
        lines.append("</details>")
        lines.append("")

    if upstream is not None:
        total_up, missing = upstream
        lines.append(
            f"Upstream publishes {total_up} models; {missing} are not vendored here. "
            "Ranked demand for those lives in `docs/demand.md`, re-derived by hand "
            "with `python3 scripts/demand_rank.py`."
        )
        lines.append("")

    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--models-dir", default="smithy-models")
    parser.add_argument(
        "--base-ref",
        default="HEAD",
        help="git ref to compare the working tree against",
    )
    parser.add_argument(
        "--upstream",
        action="store_true",
        help="also report upstream models not vendored here",
    )
    parser.add_argument(
        "--self-check",
        action="store_true",
        help="run the built-in assertions and exit",
    )
    parser.add_argument("--out", default="-", help="output path, or - for stdout")
    args = parser.parse_args()

    if args.self_check:
        _self_check()
        return

    if not re.fullmatch(r"[\w./-]+", args.models_dir):
        die(f"suspicious --models-dir {args.models_dir!r}")

    total = len(list((REPO_ROOT / args.models_dir).glob("*.json")))
    if not total:
        die(f"no models under {args.models_dir}")

    rows: list[dict] = []
    doc_only: list[str] = []
    for name in changed_models(args.models_dir, args.base_ref):
        old = model_at_ref(args.models_dir, name, args.base_ref)
        new = model_on_disk(args.models_dir, name)
        before, after = service_operations(old), service_operations(new)
        rows.append(
            {
                "name": name,
                "added": sorted(after - before),
                "removed": sorted(before - after),
            }
        )
        if is_documentation_only(old, new):
            doc_only.append(name)

    upstream = upstream_not_in_tree(args.models_dir) if args.upstream else None
    text = render(rows, doc_only, total, upstream)

    if args.out == "-":
        print(text)
    else:
        pathlib.Path(args.out).write_text(text, encoding="utf-8")
        print(f"wrote {args.out}", file=sys.stderr)


if __name__ == "__main__":
    main()
