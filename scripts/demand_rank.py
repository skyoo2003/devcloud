#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""scripts/demand_rank.py — rank the AWS services DevCloud does not register by
how much demand three independent emulator/provider projects have already met.

DevCloud has no usage telemetry, so it cannot measure which missing services its
own users reach for. What it can read is revealed demand: `moto`, LocalStack and
`terraform-provider-aws` each serve the populations DevCloud names as its users
(boto3 developers, CI operators, IaC engineers), and each only builds a service
when somebody asks. A service all three have built is one all three were asked
for. That is a proxy, not a measurement, and docs/demand.md says so.

Run by hand, like scripts/download-smithy-models.sh — never in CI. The three
sources are third-party and change under us; the committed docs/demand.md is the
evidence, and this script is how it is re-derived. A source that returns nothing
exits non-zero rather than writing a file that silently under-counts, which is
the same rule download-smithy-models.sh follows.

usage: demand_rank.py [--out docs/demand.md] [--date YYYY-MM-DD]
"""

from __future__ import annotations

import argparse
import datetime
import json
import os
import pathlib
import re
import sys
import urllib.error
import urllib.request

REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent

UPSTREAM_MODELS_API = (
    "https://api.github.com/repos/aws/aws-sdk-go-v2/contents/"
    "codegen/sdk-codegen/aws-models?ref=main"
)
MOTO_COVERAGE_URL = (
    "https://raw.githubusercontent.com/getmoto/moto/master/IMPLEMENTATION_COVERAGE.md"
)
LOCALSTACK_DOCS_TREE = (
    "https://api.github.com/repos/localstack/docs/git/trees/main?recursive=1"
)
TERRAFORM_SERVICE_DIRS = (
    "https://api.github.com/repos/hashicorp/terraform-provider-aws/contents/"
    "internal/service?ref=main"
)

# Upstream model filenames whose DevCloud service ID is neither the parsed
# Smithy namespace (the model is not in tree) nor the squashed filename. Both
# are services DevCloud implements by hand under a shorter name.
FILENAME_TO_SERVICE_ID = {
    "database-migration-service": "dms",
    "serverlessapplicationrepository": "serverlessrepo",
}

# Short names the sources use that no amount of separator-stripping resolves.
# Keys and values are already normalized. Two kinds live here, and both must:
# an abbreviation for a service DevCloud registers (`ce`, `logs`) would
# otherwise show as an unmatched name and make the join look broken, and an
# abbreviation for a service in the missing set (`ds`, `cur`) would otherwise
# lose a real support hit and undercount demand.
#
# Names absent from this map are absent on purpose. `elastictranscoder`, `qldb`,
# `iotanalytics` and `evidently` are services these projects support that AWS
# publishes no model for, so they cannot be in the missing set and must stay
# unmatched — silently mapping them somewhere would invent demand for a service
# that is not in the denominator.
SOURCE_NAME_ALIASES = {
    # → services DevCloud already registers
    "appautoscaling": "applicationautoscaling",
    "ce": "costexplorer",
    "cognitoidp": "cognitoidentityprovider",
    "config": "configservice",
    "deploy": "codedeploy",
    "elasticsearch": "elasticsearchservice",
    "elbv2": "elasticloadbalancingv2",
    "es": "elasticsearchservice",
    "events": "eventbridge",
    "iotdata": "iotdataplane",
    "lexmodels": "lexmodelbuildingservice",
    "lexv2models": "lexmodelsv2",
    "logs": "cloudwatchlogs",
    "stepfunctions": "sfn",
    # → services in the missing set
    "cur": "costandusagereportservice",
    "ds": "directoryservice",
    "elb": "elasticloadbalancing",
    "meteringmarketplace": "marketplacemetering",
    "sdb": "simpledbv2",
}

SOURCE_LABELS = ["moto", "LocalStack", "terraform-provider-aws"]
SOURCE_SLUGS = ["moto", "localstack", "terraform"]


def die(msg: str) -> None:
    print(f"ERROR: {msg}", file=sys.stderr)
    sys.exit(1)


def fetch(url: str) -> bytes:
    """GET url, honouring GITHUB_TOKEN when set.

    Unauthenticated GitHub API calls are capped at 60/hour per IP, and this
    script makes three of them. A rate-limited run must fail loudly: a caller
    who gets an empty source and a written file has evidence that under-counts
    and no way to tell.
    """
    req = urllib.request.Request(url, headers={"User-Agent": "devcloud-demand-rank"})
    token = os.environ.get("GITHUB_TOKEN")
    if token and "api.github.com" in url:
        req.add_header("Authorization", f"Bearer {token}")
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            return resp.read()
    except urllib.error.HTTPError as exc:
        hint = ""
        if exc.code in (403, 429):
            hint = " — GitHub rate limit; set GITHUB_TOKEN and retry"
        die(f"{url} returned HTTP {exc.code}{hint}")
    except urllib.error.URLError as exc:
        die(f"{url} unreachable: {exc.reason}")
    raise AssertionError("unreachable")


def normalize(name: str) -> str:
    """Collapse a service name to a join key.

    The four sources spell the same service several ways: AWS models and moto
    use `application-autoscaling`, terraform-provider-aws uses
    `applicationautoscaling`, DevCloud registers `applicationautoscaling`.
    Stripping separators makes them one key. It is a lossy join, and the
    unmatched counts below are what make a lossy join visible instead of
    quietly deflating the support numbers.
    """
    key = re.sub(r"[^a-z0-9]", "", name.lower())
    return SOURCE_NAME_ALIASES.get(key, key)


def registered_service_ids() -> set[str]:
    """The service IDs the binary actually registers.

    Read from the Register() calls rather than from directory names, for the
    same reason `make stats` does: a directory that exists but registers nothing
    is not a registered service.
    """
    ids: set[str] = set()
    pattern = re.compile(r'DefaultRegistry\.Register\("([^"]+)"')
    for path in (REPO_ROOT / "internal" / "services").rglob("*.go"):
        ids.update(pattern.findall(path.read_text(encoding="utf-8")))
    if not ids:
        die("found no DefaultRegistry.Register calls under internal/services")
    return ids


def intree_model_service_ids() -> dict[str, str]:
    """Map each in-tree model filename to the service ID codegen derives from it.

    codegen.detectServiceID takes the last dotted segment of the Smithy
    namespace, so `com.amazonaws.acmpca#...` becomes `acmpca` — which is neither
    the filename (`acm-pca`) nor anything derivable from it.
    """
    out: dict[str, str] = {}
    models_dir = REPO_ROOT / "smithy-models"
    for path in sorted(models_dir.glob("*.json")):
        model = json.loads(path.read_text(encoding="utf-8"))
        for shape_id, shape in model.get("shapes", {}).items():
            if shape.get("type") == "service":
                out[path.stem] = shape_id.split("#")[0].split(".")[-1]
                break
    if not out:
        die(f"no service shapes found under {models_dir}")
    return out


def upstream_model_names() -> list[str]:
    entries = json.loads(fetch(UPSTREAM_MODELS_API))
    names = sorted(e["name"][:-5] for e in entries if e["name"].endswith(".json"))
    if not names:
        die("upstream model listing returned no .json files")
    return names


def moto_services() -> set[str]:
    """moto publishes one `## <service>` heading per implemented service.

    The file also carries `## Unimplemented:` headings, which are section
    markers rather than services. Requiring a service-shaped name drops them.
    """
    text = fetch(MOTO_COVERAGE_URL).decode("utf-8")
    names = {
        m.group(1) for m in re.finditer(r"^## ([a-z0-9][a-z0-9._-]*)\s*$", text, re.M)
    }
    if not names:
        die("moto IMPLEMENTATION_COVERAGE.md yielded no service headings")
    return names


def localstack_services() -> set[str]:
    """LocalStack's docs carry one coverage page per service.

    The open-source repo only registers the community providers, so it
    under-reports by more than half. The docs cover both tiers, which is the
    list a user comparing emulators actually sees.
    """
    tree = json.loads(fetch(LOCALSTACK_DOCS_TREE))
    if tree.get("truncated"):
        die("LocalStack docs tree came back truncated; the service list is partial")
    names = set()
    pattern = re.compile(r"^content/en/references/coverage/coverage_(.+)/index\.md$")
    for node in tree.get("tree", []):
        m = pattern.match(node.get("path", ""))
        if m:
            names.add(m.group(1))
    if not names:
        die("LocalStack docs tree yielded no coverage pages")
    return names


def terraform_services() -> set[str]:
    """terraform-provider-aws has one Go package per AWS service."""
    entries = json.loads(fetch(TERRAFORM_SERVICE_DIRS))
    names = {e["name"] for e in entries if e["type"] == "dir"}
    if not names:
        die("terraform-provider-aws internal/service listing returned no directories")
    return names


def missing_services(
    upstream: list[str], registered: set[str], intree: dict[str, str]
) -> list[str]:
    """Upstream model files whose service DevCloud does not register.

    docs/coverage.md settled the denominator: one model file is one service,
    because that is what an SDK client selects.
    """
    missing = []
    for name in upstream:
        service_id = (
            intree.get(name) or FILENAME_TO_SERVICE_ID.get(name) or normalize(name)
        )
        if service_id not in registered:
            missing.append(name)
    return missing


def render(
    missing: list[str],
    support: dict[str, list[bool]],
    counts: dict[int, int],
    stats: dict[str, float],
    date: str,
) -> str:
    lines = [
        "# Demand for unregistered services",
        "",
        f"Sampled {date}. Re-derive with `python3 scripts/demand_rank.py`.",
        "",
        "## What this measures, and what it does not",
        "",
        "DevCloud has no usage telemetry, so this page does **not** measure what",
        "DevCloud's users ask for. It measures *revealed* demand: which of the AWS",
        "services DevCloud does not register have already been built by three",
        "independent projects serving the same populations DevCloud names as its",
        "users.",
        "",
        "| Source | What it is | Services |",
        "|---|---|---|",
        "| [moto](https://github.com/getmoto/moto) | Python AWS mocking library; "
        "the same population as DevCloud's primary user, boto3 developers running "
        f"tests | {stats['moto_total']:.0f} |",
        "| [LocalStack](https://docs.localstack.cloud/references/coverage/) | Local "
        "AWS emulator; community and pro tiers combined | "
        f"{stats['localstack_total']:.0f} |",
        "| [terraform-provider-aws](https://github.com/hashicorp/terraform-provider-aws) "
        f"| One Go package per AWS service; the IaC population | {stats['terraform_total']:.0f} |",
        "",
        "Each of those projects adds a service because somebody asked for it. None",
        "of them measured DevCloud's users. Read a high support count as *this",
        "service is worth emulating to someone*, not as *our users want this*.",
        "",
        "The live measurement of DevCloud's own traffic is",
        "`GET /devcloud/api/unrouted`, which accrues behind this page. Its ceiling",
        "is documented in [coverage.md](coverage.md).",
        "",
        "## Readings",
        "",
        "| Reading | Value |",
        "|---|---|",
        f"| Upstream model files | {stats['upstream_total']:.0f} |",
        f"| Registered by DevCloud | {stats['registered_total']:.0f} |",
        f"| **R1 — missing set `M`** | **{stats['missing_total']:.0f}** |",
        f"| R2 — `M` with support 3 | {counts[3]} |",
        f"| R2 — `M` with support 2 | {counts[2]} |",
        f"| R2 — `M` with support 1 | {counts[1]} |",
        f"| R2 — `M` with support 0 | {counts[0]} |",
        f"| **R2 — `M` with support ≥ 2** | **{stats['support_ge2']:.0f} "
        f"({stats['support_ge2_pct']:.1f}% of `M`)** |",
        "",
        "Join diagnostics — a name a source publishes that matches neither `M` nor a",
        "registered DevCloud service. A large number here means the name",
        "normalisation is dropping matches and the support counts are too low:",
        "",
        "| Source | Unmatched names |",
        "|---|---|",
        f"| moto | {stats['moto_unmatched']:.0f} |",
        f"| LocalStack | {stats['localstack_unmatched']:.0f} |",
        f"| terraform-provider-aws | {stats['terraform_unmatched']:.0f} |",
        "",
        "## Ranking",
        "",
        "Ordered by support count, then name. This is the order Milestone 4 works",
        "in, so stopping at any point stops on the best surface available.",
        "",
        "| Service | moto | LocalStack | terraform-provider-aws | Support |",
        "|---|---|---|---|---|",
    ]
    for name in sorted(missing, key=lambda n: (-sum(support[n]), n)):
        cells = ["yes" if hit else "—" for hit in support[name]]
        lines.append(
            f"| `{name}` | {cells[0]} | {cells[1]} | {cells[2]} | {sum(support[name])} |"
        )
    lines.append("")
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", default="docs/demand.md", help="output markdown path")
    parser.add_argument(
        "--date",
        default=datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d"),
        help="sample date recorded in the output; pass the original date to "
        "reproduce a committed file byte-for-byte",
    )
    args = parser.parse_args()

    registered = registered_service_ids()
    intree = intree_model_service_ids()
    upstream = upstream_model_names()
    missing = missing_services(upstream, registered, intree)

    sources = [moto_services(), localstack_services(), terraform_services()]

    missing_keys = {normalize(n): n for n in missing}
    registered_keys = {normalize(r) for r in registered}

    support = {n: [False, False, False] for n in missing}
    stats: dict[str, float] = {}
    for idx, (slug, names) in enumerate(zip(SOURCE_SLUGS, sources)):
        unmatched = 0
        for raw in names:
            key = normalize(raw)
            if key in missing_keys:
                support[missing_keys[key]][idx] = True
            elif key not in registered_keys:
                unmatched += 1
        stats[f"{slug}_total"] = len(names)
        stats[f"{slug}_unmatched"] = unmatched

    counts = {n: 0 for n in range(4)}
    for hits in support.values():
        counts[sum(hits)] += 1

    stats["upstream_total"] = len(upstream)
    stats["registered_total"] = len(registered)
    stats["missing_total"] = len(missing)
    stats["support_ge2"] = counts[2] + counts[3]
    stats["support_ge2_pct"] = (
        100.0 * stats["support_ge2"] / len(missing) if missing else 0.0
    )

    out_path = pathlib.Path(args.out)
    if not out_path.is_absolute():
        out_path = REPO_ROOT / out_path
    out_path.write_text(
        render(missing, support, counts, stats, args.date), encoding="utf-8"
    )

    # Print every total the decision rule reads, so an obviously-broken fetch is
    # visible without opening the file.
    print(f"upstream models      {stats['upstream_total']:.0f}")
    print(f"registered           {stats['registered_total']:.0f}")
    print(f"R1 missing set |M|   {stats['missing_total']:.0f}")
    for label, slug in zip(SOURCE_LABELS, SOURCE_SLUGS):
        print(
            f"  {label:<24} {stats[f'{slug}_total']:>4.0f} services"
            f"  ({stats[f'{slug}_unmatched']:.0f} names unmatched)"
        )
    for n in (3, 2, 1, 0):
        print(f"R2 support {n}         {counts[n]:>4}")
    print(
        f"R2 support >= 2      {stats['support_ge2']:.0f} "
        f"({stats['support_ge2_pct']:.1f}% of M)"
    )
    print(f"wrote {out_path.relative_to(REPO_ROOT)}")


if __name__ == "__main__":
    main()
