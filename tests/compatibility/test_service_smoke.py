"""Smoke coverage for services served by the generic CRUD engine.

A scaffolded service is registered and routed the moment it is generated, which
is not the same as being served: it answers an operation only if its provider
hands the operation to the CRUD engine. This test is the difference between the
two. It fails for a service that is routed but returns an error for every
operation — the state every scaffolded service was in before the provider
template opted into the engine.

Adding a service means adding its name below, not writing a fixture.
"""

import pytest
from botocore import xform_name
from botocore.exceptions import ClientError

# Services whose coverage comes from the generic CRUD engine rather than a
# hand-written provider. The engine only serves the awsJson1_0 / awsJson1_1
# protocols, which carry the operation name in X-Amz-Target; a rest-json,
# query, or rest-xml service cannot be listed here and be served.
ENGINE_SERVED_SERVICES = [
    "comprehend",
    # rekognition could not be onboarded at all before the alias table was
    # derived from the models: its X-Amz-Target prefix is RekognitionService,
    # which no hand-written gateway clause covered, so every call came back
    # UnknownService. It is here to prove that onboarding now costs zero
    # hand-written routing lines, not only zero provider lines.
    "rekognition",
]


def _parameterless_read_op(client):
    """Return the first List*/Describe* operation that needs no input.

    Picking the operation from the service model rather than hardcoding one per
    service is what keeps this test from growing a branch per service.
    """
    model = client.meta.service_model
    for name in sorted(model.operation_names):
        if not name.startswith(("List", "Describe")):
            continue
        shape = model.operation_model(name).input_shape
        if shape is None or not shape.required_members:
            return name
    return None


@pytest.mark.parametrize("service", ENGINE_SERVED_SERVICES)
def test_service_serves_at_least_one_operation(service_client, service):
    client = service_client(service)

    op = _parameterless_read_op(client)
    assert op is not None, (
        f"{service} declares no parameterless List*/Describe* operation, "
        "so this test cannot establish that it serves anything"
    )

    try:
        response = getattr(client, xform_name(op))()
    except ClientError as exc:
        pytest.fail(
            f"{service}.{op} is routed but not served: {exc.response['Error']}. "
            "The provider is most likely not returning plugin.ErrUnhandledOp, "
            "so the request never reaches the CRUD engine."
        )

    assert response["ResponseMetadata"]["HTTPStatusCode"] == 200
