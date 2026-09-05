"""An operation DevCloud does not serve must never answer 200.

This is the one guarantee docs/coverage.md calls absolute:

    What it must never do is fabricate a success.

Registering a service DevCloud cannot fully serve is worth doing — the call is
routed and answered locally instead of silently reaching a billed AWS account —
but only while the answer is an honest error. A fabricated 200 is worse than not
registering the service at all, because the caller believes it.

test_service_smoke.py already asserts this for services that serve *nothing*.
The gap this file closes is the other case, and the one the defect lived in: a
service that serves plenty and answers an operation it does not implement with
an empty success anyway. The fidelity manifest says which operations are served;
every operation it does not list must decline.
"""

import pytest
from botocore import xform_name
from botocore.exceptions import ClientError, ParamValidationError

import _coverage

MANIFEST = _coverage.load_manifest()

# Operations to avoid probing with. A refusal is observable without writing
# anything, so nothing that could create or overwrite state is used. Delete and
# Modify of a resource that does not exist stay in scope — an empty store has
# nothing to lose.
_MUTATING_PREFIXES = ("Create", "Put", "Register", "Add", "Import", "Start", "Run")


def _unserved_probe(service_id, entry):
    """Return an operation the manifest says is not served, or None.

    Reads first, so a failure reads unambiguously.
    """
    name = _coverage.boto3_name(service_id)
    if name is None:
        return None, None

    model = _coverage.service_model(name)
    unserved = sorted(set(model.operation_names) - set(entry["servedOps"]))

    for prefix in ("Describe", "List", "Get"):
        for op in unserved:
            if op.startswith(prefix):
                return name, op

    for op in unserved:
        if not op.startswith(_MUTATING_PREFIXES):
            return name, op
    return name, None


# Two fabricated successes that this file's fix does not reach, marked strict so
# they fail the moment they start passing rather than rotting into an accepted
# state. Only what was observed is recorded — the mechanism behind each is not
# the provider `default` branch that was fixed here, and has not been identified:
#
#   s3.ListBucketAnalyticsConfigurations
#       GET /{Bucket}?analytics answers 200 with an empty body. S3's provider
#       default returns MethodNotAllowed, so this is not that branch.
#
#   resourcegroups.Tag
#       PUT /resources/{Arn}/tags answers 200 echoing the request's Arn and
#       Tags. The manifest lists the operation as unimplemented and the CRUD
#       registry does not hold it; it holds GetTags at the same path under GET,
#       and httproute.Match does compare methods (match.go:42), so the obvious
#       explanation is ruled out rather than confirmed.
#
# Both are real violations of the guarantee and neither is fixed here: they are
# different defects that deserve their own diagnosis and their own change.
KNOWN_UNFIXED = {
    (
        "s3",
        "ListBucketAnalyticsConfigurations",
    ): "empty 200 from S3's sub-resource routing",
    (
        "resourcegroups",
        "Tag",
    ): "engine-shaped echo for an operation the manifest calls unimplemented",
}

# Every service that is addressable, routable, and has an unserved operation to
# ask for. Built at collection time so the parametrisation names the operation
# rather than an index.
_PROBES = []
for _service in sorted(MANIFEST):
    if (
        _service in _coverage.NO_BOTO3_CLIENT
        or _service in _coverage.UNREACHABLE_FROM_BOTO3
    ):
        continue
    _name, _op = _unserved_probe(_service, MANIFEST[_service])
    if _op is not None:
        _PROBES.append((_service, _name, _op))


def _case(service, client_name, operation):
    reason = KNOWN_UNFIXED.get((service, operation))
    marks = [pytest.mark.xfail(strict=True, reason=reason)] if reason else []
    return pytest.param(service, client_name, operation, marks=marks)


@pytest.mark.parametrize(
    "service,client_name,operation",
    [_case(*probe) for probe in _PROBES],
    ids=[f"{s}.{op}" for s, _, op in _PROBES],
)
def test_unserved_operation_declines(service_client, service, client_name, operation):
    client = service_client(client_name)
    params = _coverage.stub_params(client_name, operation)

    try:
        response = getattr(client, xform_name(operation))(**params)
    except ParamValidationError:
        pytest.skip(
            f"{client_name}.{operation}: the stub builder cannot satisfy this input "
            "shape, so the request never reaches DevCloud"
        )
    except ClientError as exc:
        assert exc.response.get("Error", {}).get("Code"), (
            f"{service} ({client_name}.{operation}) failed without an AWS error "
            f"code: {exc.response.get('Error')}"
        )
        return
    except Exception as exc:  # noqa: BLE001 - reported, not swallowed
        pytest.fail(
            f"{service} ({client_name}.{operation}, {MANIFEST[service]['protocol']}) "
            f"answered something botocore could not parse ({type(exc).__name__}: "
            f"{exc}). The manifest does not list this operation as served, so the "
            "answer should have been a clean AWS error."
        )

    pytest.fail(
        f"{service} ({client_name}.{operation}, {MANIFEST[service]['protocol']}) "
        f"returned {response['ResponseMetadata']['HTTPStatusCode']} for an "
        "operation the fidelity manifest does not list as served. That is a "
        "fabricated success — the guarantee docs/coverage.md calls absolute. "
        "The provider's default branch most likely answers unknown operations "
        "itself instead of returning plugin.ErrUnhandledOp."
    )
