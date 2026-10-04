"""Pinned external-client helpers for DevCloud compatibility tests."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from collections.abc import Mapping, Sequence
import subprocess


AWS_CLI_VERSION = "2.37.6"
TERRAFORM_VERSION = "1.16.4"


@dataclass(frozen=True)
class ToolPaths:
    aws: Path
    terraform: Path


def run_command(
    argv: Sequence[str],
    *,
    cwd: Path | None = None,
    env: Mapping[str, str] | None = None,
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        argv,
        cwd=cwd,
        env=dict(env) if env is not None else None,
        capture_output=True,
        text=True,
        check=False,
    )


def _require_version(path: Path, expected: str, label: str) -> None:
    if not path.is_file() or not path.stat().st_mode & 0o111:
        version = expected.removeprefix("aws-cli/").removeprefix("Terraform v")
        raise RuntimeError(f"missing {label} {version} binary at {path}")
    result = run_command([str(path), "--version"])
    if result.returncode != 0 or expected not in result.stdout:
        actual = result.stdout.strip() or result.stderr.strip() or "no version output"
        raise RuntimeError(f"{label} expected {expected}, got {actual}")


def resolve_tools(tool_root: Path) -> ToolPaths:
    tools = ToolPaths(
        aws=tool_root / "aws" / "aws",
        terraform=tool_root / "terraform" / "terraform",
    )
    _require_version(tools.aws, f"aws-cli/{AWS_CLI_VERSION}", "aws CLI")
    _require_version(tools.terraform, f"Terraform v{TERRAFORM_VERSION}", "Terraform")
    return tools
