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
# hand-written provider. The engine reads every protocol DevCloud registers,
# each naming its operation somewhere different: awsJson1_0 / awsJson1_1 in the
# X-Amz-Target header, rest-json and rest-xml in the request method and path via
# the model's route table, query in the Action field of the form body.
ENGINE_SERVED_SERVICES = [
    "comprehend",
    # rest-json, and the proof the engine reaches that protocol at all. All
    # three sat in REGISTERED_ONLY_SERVICES until the engine learned to resolve
    # an operation from the method and path; nothing about these three services
    # changed, only what the engine can read.
    "polly",
    "qbusiness",
    "codeguru-reviewer",
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
    # --- the demand set (Milestone 4) ---
    # Every unregistered AWS service that two or more of moto, LocalStack and
    # terraform-provider-aws had already built (docs/demand.md). 54 of the 57
    # serve at least one operation; these 51 are the ones this harness can
    # prove, because they declare a List*/Describe* that needs no input.
    "amp",
    "appmesh",
    "cleanrooms",
    "cloudhsmv2",
    "codestar-connections",
    "connect",
    "databrew",
    "datapipeline",
    "datasync",
    "dax",
    "devops-agent",
    "directconnect",
    "ds",
    "dsql",
    "emr-containers",
    "emr-serverless",
    "fsx",
    "greengrass",
    "guardduty",
    "inspector2",
    "ivs",
    "kinesisanalytics",
    "kinesisvideo",
    "macie2",
    "mediaconnect",
    "medialive",
    "mediapackage",
    "mediapackagev2",
    "mediastore",
    "mediastore-data",
    "network-firewall",
    "networkmanager",
    "opensearchserverless",
    "osis",
    "payment-cryptography",
    "quicksight",
    "redshift-data",
    "resiliencehub",
    "route53domains",
    "s3vectors",
    "securityhub",
    "service-quotas",
    "servicecatalog",
    "servicecatalog-appregistry",
    "signer",
    "synthetics",
    "timestream-influxdb",
    "timestream-query",
    "vpc-lattice",
    "workspaces",
    "workspaces-web",
    # --- the non-JSON surface (Milestone 5) ---
    # The last two of the 57, and the reason they were last. Neither protocol
    # names its operation the way the engine originally required: elb is query,
    # so the operation is the Action field of a form body, and s3control is
    # rest-xml, so it comes from the method and path. Both are read now, and
    # this entry is what proves it against a real botocore parser rather than
    # against Go's idea of the wire format.
    #
    # s3control is served too — 94 auto-crud operations in the manifest — but
    # is absent here on purpose: every one of its 97 operations takes a required
    # AccountId, so this harness has no parameterless read to prove it with. The
    # budgets precedent, and tests/compatibility/test_s3control.py is where it
    # is proved instead.
    "elb",
    # Served, but not provable here: apigateway and apigatewaymanagementapi
    # read with Get* rather than List*/Describe*, and budgets requires an
    # account id on every operation. The fidelity manifest is their record,
    # as for bedrock-data-automation-runtime.
]

# Services registered so the gateway routes them, but served by nothing: no
# operation in their whole API is CRUD-shaped, and no provider implements them
# by hand. They must answer with a clean AWS error — never a fabricated success, and never an unrouted UnknownService, which would
# send a caller back to real AWS. They are deliberately NOT counted as covered;
# see docs/coverage.md.
#
# The protocol reason for being on this list is gone. The engine reads all five
# protocols now — X-Amz-Target for the JSON family, method and path for the two
# REST ones, the Action form field for query — so what is left here is only the
# services whose operations are not CRUD-shaped at all.
#
# personalize-runtime left this list when the engine gained rest-json:
# GetRecommendations classifies as a Get, so it is served now. It is not in
# ENGINE_SERVED_SERVICES either, because both its operations take required
# parameters and this harness has no parameterless read to prove it with — the
# fidelity manifest is its record, as for bedrock-data-automation-runtime.
REGISTERED_ONLY_SERVICES = [
    # rest-json, so the engine can read its requests — and still served by
    # nothing, because it has no CRUD-shaped operation. InvokeEndpoint,
    # InvokeEndpointAsync and InvokeEndpointWithResponseStream are not
    # Create/Get/List/Delete/Update, so codegen registers nothing and there is
    # no route to match. Same case as forecastquery.
    "sagemaker-runtime",
    # --- the demand set (Milestone 4) ---
    # One of the 57 does not meet the floor. elb and s3control were here too,
    # for the protocol reason Milestone 5 removed: the engine now reads query
    # from the Action form field and rest-xml from the method and path.
    #   rds-data  rest-json and readable, but its whole API is
    #             ExecuteStatement / BeginTransaction / Commit / Rollback,
    #             none of which is CRUD-shaped. No protocol change helps it,
    #             which is why it is the one that stays.
    "rds-data",
]


def _parameterless_read_op(client):
    """Return a List*/Describe* operation that needs no input.

    Picking the operation from the service model rather than hardcoding one per
    service is what keeps this test from growing a branch per service.

    List* is tried before Describe* because the two answer differently on an
    empty store. A List returns an empty collection; a Describe of a single
    resource correctly returns ResourceNotFoundException, which is the engine
    working, not the engine missing. Preferring List keeps the common case
    unambiguous — see _served_or_missing for the Describe-only services.
    """
    model = client.meta.service_model
    for prefix in ("List", "Describe"):
        for name in sorted(model.operation_names):
            if not name.startswith(prefix):
                continue
            shape = model.operation_model(name).input_shape
            if shape is None or not shape.required_members:
                return name
    return None


# A resource that does not exist in an empty store is a served answer: the
# engine looked in the store and reported honestly. A refusal is different —
# InvalidAction and NotImplemented mean nothing handled the request at all.
SERVED_ERROR_CODES = {
    "ResourceNotFoundException",
    "NotFoundException",
    "ResourceNotFound",
    "NoSuchEntity",
}


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
        code = exc.response.get("Error", {}).get("Code")
        if code in SERVED_ERROR_CODES:
            # Served: the engine consulted an empty store and said so.
            return
        pytest.fail(
            f"{service}.{op} is routed but not served: {exc.response.get('Error')}. "
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
