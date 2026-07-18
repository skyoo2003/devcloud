"""Regression tests: unimplemented operations must return an AWS error, not a
silent 200 empty-body success. Guards the fix in the services that previously
returned a false 200 for unknown ops (apigatewayv2, backup, codebuild,
codedeploy, codepipeline, cognito-idp, config, glue, iot, iotwireless,
sagemaker)."""

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


@pytest.mark.parametrize(
    "fixture,method,kwargs",
    UNIMPLEMENTED_OPS,
    ids=[f"{f.removesuffix('_client')}.{m}" for f, m, _ in UNIMPLEMENTED_OPS],
)
def test_unimplemented_op_errors(request, fixture, method, kwargs):
    client = request.getfixturevalue(fixture)
    with pytest.raises(ClientError):
        getattr(client, method)(**kwargs)
