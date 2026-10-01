from pathlib import Path

import pytest

from client_tools import resolve_tools


def _executable(path: Path, output: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(f"#!/bin/sh\nprintf '%s\\n' '{output}'\n")
    path.chmod(0o755)


def test_resolve_tools_rejects_missing_aws_cli(tmp_path):
    with pytest.raises(RuntimeError, match="aws CLI 2.37.6"):
        resolve_tools(tmp_path)


def test_resolve_tools_rejects_an_unpinned_version(tmp_path):
    _executable(tmp_path / "aws" / "aws", "aws-cli/2.36.43 Python/3.14")
    _executable(tmp_path / "terraform" / "terraform", "Terraform v1.16.4")

    with pytest.raises(RuntimeError, match="expected.*2.37.6"):
        resolve_tools(tmp_path)


def test_resolve_tools_returns_exact_pinned_binaries(tmp_path):
    _executable(tmp_path / "aws" / "aws", "aws-cli/2.37.6 Python/3.14")
    _executable(tmp_path / "terraform" / "terraform", "Terraform v1.16.4")

    tools = resolve_tools(tmp_path)

    assert tools.aws == tmp_path / "aws" / "aws"
    assert tools.terraform == tmp_path / "terraform" / "terraform"


def test_installer_pins_the_contract_versions():
    installer = Path(__file__).parents[2] / "scripts" / "install-compat-tools.sh"

    content = installer.read_text()

    assert 'AWS_CLI_VERSION="2.37.6"' in content
    assert 'TERRAFORM_VERSION="1.16.4"' in content
    assert "awscli-exe-linux-x86_64-${AWS_CLI_VERSION}.zip.sig" in content
    assert "terraform_${TERRAFORM_VERSION}_SHA256SUMS" in content
