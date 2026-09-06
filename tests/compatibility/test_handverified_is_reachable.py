"""An operation the manifest calls hand-verified must be reachable.

`hand-verified` is the strongest label DevCloud publishes: someone wrote code for
this operation. Codegen derives it by reading the `case` clauses in a provider's
dispatch switch — which proves the code exists, not that any request arrives at
it.

For a REST-JSON provider those are two different questions. The gateway passes no
operation name (there is no `X-Amz-Target` to read), so the provider recovers it
from the method and path with a hand-written resolver. Where that resolver does
not know a path, the operation is unreachable however carefully it was
implemented, and the manifest says `hand-verified` about code nothing can call.

Three were found this way by Milestone 6's per-service floor test, which had
tolerated them because it needs only *one* served operation per service to pass.
They are asserted individually here, because that is the only granularity at
which the defect is visible.
"""

import pytest
from botocore import xform_name
from botocore.exceptions import ClientError

import _coverage

MANIFEST = _coverage.load_manifest()

# service id, boto3 client name, operation, and the route boto3 actually sends.
# The route is quoted from the service's own model (the generated CRUD registry
# holds the same URI), because it is what the provider's resolver has to know.
CASES = [
    ("appsync", "appsync", "ListApis", "GET /v2/apis"),
    ("eks", "eks", "ListAccessPolicies", "GET /access-policies"),
    (
        "opensearch",
        "opensearch",
        "ListApplications",
        "GET /2021-01-01/opensearch/list-applications",
    ),
]


@pytest.mark.parametrize(
    "service,client_name,operation,route",
    CASES,
    ids=[f"{s}.{op}" for s, _, op, _ in CASES],
)
def test_handverified_operation_is_reachable(
    service_client, service, client_name, operation, route
):
    """The operation answers, rather than reporting itself unimplemented.

    What is asserted is deliberately weak: any answer other than an error. The
    manifest claims the operation is implemented, so the failure being caught is
    the provider not finding its own implementation — not the shape of what it
    returns.
    """
    assert operation in MANIFEST[service]["servedOps"], (
        f"{service}.{operation} is no longer listed as served, so this test "
        "asserts a claim the manifest is not making. Delete the case, or fix "
        "whatever stopped the manifest from making it."
    )

    client = service_client(client_name)
    params = _coverage.stub_params(client_name, operation)

    try:
        response = getattr(client, xform_name(operation))(**params)
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code")
        pytest.fail(
            f"{service}.{operation} ({route}) answered {code}. The manifest calls "
            "it hand-verified, so a provider case clause implements it — but the "
            "provider's own path resolver never reaches that clause, so the "
            "operation arrives empty and the dispatch falls through to default."
        )

    assert response["ResponseMetadata"]["HTTPStatusCode"] == 200
