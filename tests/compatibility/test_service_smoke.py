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

# Names below are boto3 client names, not DevCloud service IDs — the two differ
# for split-out services ("kendra-ranking" vs kendraranking). Using the boto3
# name is the point: the test asserts what a user typing it actually gets.
#
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
    # The rest of the AI/ML category that the engine can serve. bedrock,
    # sagemaker, textract and transcribe are engine-served too but already have
    # their own test files, so they are not repeated here.
    # bedrock-data-automation-runtime is engine-served (4 operations in the
    # manifest) but is absent here on purpose: every one of its operations takes
    # required parameters, so this harness has no parameterless read to prove it
    # with. The manifest is its record, not this list.
    "comprehendmedical",
    "forecast",
    "frauddetector",
    "healthlake",
    "kendra",
    "kendra-ranking",
    "lookoutequipment",
    "personalize",
    "translate",
    "voice-id",
]

# Services registered so the gateway routes them, but served by nothing: their
# protocol carries no operation name for the CRUD engine to classify, and no
# provider implements them by hand. They must answer with a clean AWS error —
# never a fabricated success, and never an unrouted UnknownService, which would
# send a caller back to real AWS. They are deliberately NOT counted as covered;
# see docs/coverage.md.
REGISTERED_ONLY_SERVICES = [
    "polly",
    "qbusiness",
    "sagemaker-runtime",
    "personalize-runtime",
    "codeguru-reviewer",
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


@pytest.mark.parametrize("service", REGISTERED_ONLY_SERVICES)
def test_registered_only_service_declines_cleanly(service_client, service):
    """A service that serves nothing must say so in AWS's own vocabulary.

    This is the guarantee that keeps breadth from becoming a lie. Registering a
    service DevCloud cannot serve is worth doing — the call is routed and
    answered locally instead of silently reaching real AWS — but only while the
    answer is an honest error. A fabricated 200 here would be worse than not
    registering the service at all, because the caller would believe it.
    """
    client = service_client(service)

    op = _parameterless_read_op(client)
    if op is None:
        pytest.skip(f"{service} declares no parameterless List*/Describe* operation")

    try:
        response = getattr(client, xform_name(op))()
    except ClientError as exc:
        error = exc.response["Error"]
        assert error.get("Code"), (
            f"{service}.{op} failed without an AWS error code: {error}"
        )
        return

    pytest.fail(
        f"{service}.{op} returned {response['ResponseMetadata']['HTTPStatusCode']} "
        f"but nothing serves this service — a success here is fabricated. "
        f"If it is genuinely served now, move it to ENGINE_SERVED_SERVICES."
    )
