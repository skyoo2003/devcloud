"""Unimplemented operations must never return a fabricated success.

- Services NOT opted into the CRUD engine return an AWS error for any
  unimplemented op.
- Engine-wired services serve CRUD-shaped ops (see test_crud_engine.py) but still
  return an honest error for non-CRUD, unclassifiable ops."""

import pytest
from botocore.exceptions import ClientError

# (client_fixture_name, boto3 method, kwargs) — each op is unimplemented in its
# provider and must hit the default branch that returns an AWS error. kwargs only
# carry required params so botocore actually sends the request (no client-side
# ParamValidationError) rather than being empty for its own sake.
UNIMPLEMENTED_OPS = [
    ("apigatewayv2_client", "list_portals", {}),
    ("backup_client", "list_legal_holds", {}),
    ("codebuild_client", "list_build_batches", {}),
    ("codedeploy_client", "list_on_premises_instances", {}),
    ("codepipeline_client", "list_rule_types", {}),
    ("cognitoidp_client", "get_csv_header", {"UserPoolId": "x"}),
    ("configservice_client", "get_discovered_resource_counts", {}),
    ("glue_client", "list_workflows", {}),
    ("iot_client", "list_audit_findings", {}),
    ("iotwireless_client", "get_service_endpoint", {}),
    ("sagemaker_client", "list_auto_ml_jobs", {}),
]

def test_unwired_service_errors_on_unimplemented(codebuild_client):
    # codebuild is not opted into the CRUD engine.
    with pytest.raises(ClientError):
        codebuild_client.list_build_batches()


def test_wired_service_errors_on_unclassifiable_op(glue_client):
    # glue IS engine-wired, but StartBlueprintRun is neither hand-implemented nor
    # CRUD-classifiable, so the engine declines it and an honest error is returned.
    with pytest.raises(ClientError):
        glue_client.start_blueprint_run(
            BlueprintName="bp", RoleArn="arn:aws:iam::000000000000:role/r"
        )
