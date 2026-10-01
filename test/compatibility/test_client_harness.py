import json


def test_aws_cli_uses_devcloud_endpoint(aws_cli):
    result = aws_cli("sts get-caller-identity")

    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout)["Account"]


def test_terraform_workspace_is_isolated(terraform_workspace):
    workspace = terraform_workspace("s3")

    assert (workspace / "devcloud.auto.tfvars.json").is_file()
    assert 'source = "./modules/s3"' in (workspace / "main.tf").read_text()
