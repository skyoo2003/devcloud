import json
from pathlib import Path


def test_terraform_configuration_pins_aws_provider():
    content = (Path(__file__).parent / "terraform" / "versions.tf").read_text()
    assert 'source  = "hashicorp/aws"' in content
    assert 'version = "6.66.0"' in content


def test_terraform_workspace_writes_local_endpoint(terraform_workspace):
    workspace = terraform_workspace("s3")

    config = json.loads((workspace / "devcloud.auto.tfvars.json").read_text())
    assert config["devcloud_endpoint"].startswith("http://localhost:")
    assert (workspace / "providers.tf").is_file()
