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


# Fabricated successes this file's own fix does not reach, marked strict so they
# fail the moment they start passing rather than rotting into an accepted state.
#
# Empty. Both entries it carried are fixed, and each turned out to be a different
# defect from the one the entry guessed at:
#
#   s3.ListBucketAnalyticsConfigurations was the bucket-listing fall-through.
#   GET /{Bucket}?analytics matched no sub-resource branch and landed on
#   listObjects, so an unimplemented sub-resource was answered with
#   <ListBucketResult>. Seven others took the same path. The provider now
#   declines any bucket-level query parameter that is neither a sub-resource it
#   serves nor a parameter a listing carries.
#
#   resourcegroups.Tag was never a fabricated success. The scan that decides
#   which operations are hand-verified required four characters, so `Untag` was
#   recorded and `Tag` — implemented beside it in the same switch — was dropped.
#   The 200 was real code answering; the manifest was wrong, this probe believed
#   the manifest, and the report inherited the error.
#
# Both were recorded as observations with the mechanism explicitly unidentified,
# which is why neither had to be un-guessed before it could be fixed.
KNOWN_UNFIXED: dict[tuple[str, str], str] = {
    # Surfaced in Phase 2, and not by Phase 2's own services. The stub builder
    # now pads a string to the minimum length botocore insists on, so this probe
    # leaves the process for the first time; the defect it lands on predates it.
    #
    # Half of the mechanism is now fixed and this entry survives the other half.
    #
    # The registry used to hold only operations the engine can classify, so the
    # more specific route could not outrank a broader sibling's. Phase 3 changed
    # that: codegen.classifyOps now records every REST-bound operation, the
    # unclassifiable ones with an empty Verb, and crud.Handle declines them on
    # the Verb check it already makes. That fixed the query-discriminated cases
    # — chime's AssociatePhoneNumberWithUser, apigateway's ImportRestApi — where
    # the specific route differs only by a "?operation=" constraint.
    #
    # It does not reach workspaces-web, because that route cannot match at all.
    # AssociateBrowserSettings models PUT /portals/{portalArn+}/browserSettings,
    # and httproute.MatchURI requires a greedy label to be the last pattern
    # segment (match.go: "Greedy must be the last pattern segment"), so it
    # rejects the pattern outright and UpdatePortal at PUT /portals/{portalArn+}
    # still swallows the path.
    #
    # The remaining fix is in MatchURI, not in the registry: a greedy label may
    # carry segments after it, so the prefix matches from the front, the suffix
    # from the back, and the label takes the middle. Ordering has to move with
    # it — Match takes the first route that fits within a pass, and routes are
    # sorted by operation name, so DeletePortal would still outrank
    # DisassociateBrowserSettings on the alphabet alone.
    ("workspacesweb", "AssociateBrowserSettings"): (
        "greedy {portalArn+} in UpdatePortal swallows the more specific "
        "AssociateBrowserSettings path; the CRUD registry models no "
        "unclassifiable route to outrank it (Phase 3)"
    ),
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


def _skip_if_pinned_unsendable(service, client_name, operation, exc, sent):
    """Skip a probe botocore refused to send, but only if it is pinned.

    Never returns: it either skips or fails.

    A probe that stops inside botocore says nothing about what DevCloud would
    answer, so failing it would report a defect that is not there. Skipping it
    unconditionally is the opposite mistake — the probe silently stops
    existing, and the suite shrinks without any assertion moving.
    test_service_smoke.py already resolves this tension by pinning
    (UNREACHABLE_FROM_BOTO3); this is the same resolution per operation.
    """
    reason = _coverage.UNSENDABLE_PROBES.get((service, operation))
    if reason is None:
        pytest.fail(
            f"{service} ({client_name}.{operation}, {MANIFEST[service]['protocol']}) "
            f"put no request on the wire: botocore raised {type(exc).__name__} "
            f"before sending ({exc}). DevCloud was never asked, so this is not a "
            "fidelity gap — but it is one probe fewer than the suite claims to "
            f'run. Pin it in _coverage.UNSENDABLE_PROBES as ("{service}", '
            f'"{operation}") with the reason, or fix the stub builder so the '
            "request can be built."
        )
    assert sent == 0, (
        f"{service} ({client_name}.{operation}) is pinned in UNSENDABLE_PROBES "
        f"as unsendable, but it put {sent} request(s) on the wire. The pin is "
        "stale — remove it so the probe is asserted again."
    )
    pytest.skip(f"{client_name}.{operation}: {reason}")


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
    sends = _coverage.count_sends(client)
    params = _coverage.stub_params(client_name, operation)

    try:
        response = getattr(client, xform_name(operation))(**params)
    except ParamValidationError as exc:
        _skip_if_pinned_unsendable(service, client_name, operation, exc, sends())
    except ClientError as exc:
        assert exc.response.get("Error", {}).get("Code"), (
            f"{service} ({client_name}.{operation}) failed without an AWS error "
            f"code: {exc.response.get('Error')}"
        )
        return
    except Exception as exc:  # noqa: BLE001 - reported, not swallowed
        if sends() == 0:
            _skip_if_pinned_unsendable(service, client_name, operation, exc, 0)
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
