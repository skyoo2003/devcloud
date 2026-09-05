"""Every registered service is asked something, and answers as AWS would.

This file used to carry two hand-written lists of service names, and adding a
service to ``internal/services`` did not require adding it here. So 31 of 205
registered services were exercised by no boto3 test at all — 29 of them counted
in the coverage figure ``docs/coverage.md`` publishes. The lists are gone: the
fleet is now ``internal/generated/compat/services.json``, which codegen derives
from the same fidelity manifest that figure comes from.

The floor this proves is the PRD's, and it has two halves:

1. A service the manifest says serves something must serve it — from a real
   store, in AWS's vocabulary.
2. A service the manifest says serves nothing must decline with a clean AWS
   error. Never a fabricated success, which would be worse than not registering
   the service at all, because the caller would believe it.

Which of the two applies is read from the manifest, not decided here. That is
what makes this file unable to drift from the published numbers.
"""

import pathlib
import re

import pytest
from botocore import xform_name
from botocore.exceptions import ClientError, ParamValidationError

import _coverage

MANIFEST = _coverage.load_manifest()

# Sorted so a failure reads the same way on every machine and in every CI run.
ALL_SERVICES = sorted(MANIFEST)
ADDRESSABLE_SERVICES = [s for s in ALL_SERVICES if s not in _coverage.NO_BOTO3_CLIENT]
TESTABLE_SERVICES = [
    s for s in ADDRESSABLE_SERVICES if s not in _coverage.UNREACHABLE_FROM_BOTO3
]


def _call(client, operation, params):
    return getattr(client, xform_name(operation))(**params)


def test_every_registered_service_is_addressable():
    """Every registered service resolves to a boto3 client, or is a named case.

    This is the gate that keeps the parametrization below honest. Without it a
    service whose name stopped resolving would silently drop out of the suite —
    the exact failure mode the hand-written lists had, reintroduced by accident.
    """
    unresolved = [
        service
        for service in ADDRESSABLE_SERVICES
        if _coverage.boto3_name(service) is None
    ]
    assert not unresolved, (
        f"{len(unresolved)} registered services resolve to no boto3 client name: "
        f"{unresolved}. Either botocore renamed the client — add the resolution to "
        "_coverage.BOTO3_NAME_OVERRIDES — or no client exists, which belongs in "
        "_coverage.NO_BOTO3_CLIENT with its reason."
    )


def test_published_compatibility_tested_figure_matches_this_suite():
    """docs/coverage.md's fourth number is this suite's size, and is gated as such.

    The Go gate in cmd/devcloud/coverage_test.go checks the three numbers it can
    see from the binary. This one is only visible from here, because it is a
    fact about what botocore can address, so it is asserted here — same rule,
    same both-directions failure.
    """
    doc = (
        pathlib.Path(__file__).resolve().parents[2] / "docs" / "coverage.md"
    ).read_text()

    rows = re.findall(
        r"^\|\s*\*\*Compatibility-tested\*\*\s*\|[^|]*\|\s*\*\*(\d+)\*\*\s*\|",
        doc,
        re.M,
    )
    assert len(rows) == 1, (
        f"docs/coverage.md: found {len(rows)} Compatibility-tested rows, want 1. "
        "The summary table was restructured; this gate reads it."
    )
    assert int(rows[0]) == len(TESTABLE_SERVICES), (
        f"docs/coverage.md publishes {rows[0]} compatibility-tested services, this "
        f"suite exercises {len(TESTABLE_SERVICES)}. Excluded: "
        f"{sorted(_coverage.NO_BOTO3_CLIENT)} (no boto3 client) and "
        f"{sorted(_coverage.UNREACHABLE_FROM_BOTO3)} (unroutable)."
    )


def test_services_without_a_boto3_client_are_exactly_the_known_two():
    """The compatibility-tested ceiling is a fact about the SDK, not a backlog.

    docs/coverage.md publishes this ceiling. If the set changes, the published
    number changes with it, so it is pinned here rather than counted at runtime.
    """
    missing = {s for s in ALL_SERVICES if _coverage.boto3_name(s) is None}
    assert missing == set(_coverage.NO_BOTO3_CLIENT), (
        f"services with no boto3 client changed: {sorted(missing)}. "
        "docs/coverage.md publishes this set and its size."
    )


def test_no_registered_service_is_unreachable_from_boto3():
    """The exclusion category stays empty, or the published figure moves with it.

    This replaces test_lex_services_are_unreachable_from_boto3, which pinned the
    four Lex services as unroutable and told whoever fixed the routing to delete
    it. The routing was fixed — "lex" is now recognised as a signing-name group
    key, so each Lex request reaches the sibling that models its path — and the
    four joined the floor parametrization below.

    What survives is the direction of the check rather than the pin: a service
    that becomes unreachable has to be named in UNREACHABLE_FROM_BOTO3, and
    naming it lowers the compatibility-tested figure gated above.
    """
    assert _coverage.UNREACHABLE_FROM_BOTO3 == {}, (
        "a registered service is unreachable from boto3: "
        f"{sorted(_coverage.UNREACHABLE_FROM_BOTO3)}. It drops out of the "
        "compatibility-tested figure in docs/coverage.md, which is gated by "
        "test_published_compatibility_tested_figure_matches_this_suite."
    )


@pytest.mark.parametrize("service", TESTABLE_SERVICES)
def test_registered_service_meets_the_floor(service_client, service):
    """The floor, for every registered service, in one assertion per service.

    The manifest decides which half applies. A service that serves something is
    asked for one of the operations it serves; a service that serves nothing is
    asked for anything at all and must refuse in AWS's vocabulary.
    """
    entry = MANIFEST[service]
    name = _coverage.boto3_name(service)
    client = service_client(name)

    candidates = _coverage.probe_operations(name, entry["servedOps"])

    if not candidates:
        _assert_declines_cleanly(client, name, service, entry)
        return

    # The floor is a property of the service, not of one operation: at least one
    # operation the manifest calls served has to answer. Trying several is what
    # keeps a single overstated operation from reading as a dead service — the
    # manifest labels an operation hand-verified when a provider has a case
    # clause for it, which is not the same as the gateway being able to route to
    # it. See docs/coverage.md.
    refusals = []
    for operation, needs_input in candidates:
        params = _coverage.stub_params(name, operation) if needs_input else {}

        try:
            response = _call(client, operation, params)
        except ParamValidationError as exc:
            refusals.append(f"{operation}: request never left botocore ({exc})")
            continue
        except ClientError as exc:
            code = exc.response.get("Error", {}).get("Code")
            if not code or code in _coverage.UNSERVED_ERROR_CODES:
                refusals.append(f"{operation}: {code or exc.response.get('Error')}")
                continue
            # Either a store that reported an absent resource honestly, or a
            # validation refusal of the stub values. Both mean the request was
            # routed and its operation classified, which is what this proves.
            return
        except Exception as exc:  # noqa: BLE001 - the type is reported, not swallowed
            # botocore could not parse the answer. That is a real defect, but an
            # operation-level one: the service can still meet the floor on
            # another operation, and calling it dead here would report the wrong
            # thing. The type is kept so an all-candidates failure names it.
            refusals.append(
                f"{operation}: unparseable response ({type(exc).__name__}: {exc})"
            )
            continue

        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200
        return

    pytest.fail(
        f"{service} ({name}, {entry['protocol']}) serves nothing that answers. "
        f"The fidelity manifest lists {len(entry['servedOps'])} served operations "
        f"for it, and all {len(candidates)} tried were refused:\n  "
        + "\n  ".join(refusals)
        + "\nThe provider is most likely not returning plugin.ErrUnhandledOp, so "
        "the request never reaches the CRUD engine."
    )


def _assert_declines_cleanly(client, name, service, entry):
    """A service that serves nothing must say so in AWS's own vocabulary.

    This is the guarantee that keeps breadth from becoming a lie. Registering a
    service DevCloud cannot serve is worth doing — the call is routed and
    answered locally instead of silently reaching real AWS — but only while the
    answer is an honest error.
    """
    model = _coverage.service_model(name)
    operation = sorted(model.operation_names)[0]
    params = _coverage.stub_params(name, operation)

    try:
        response = _call(client, operation, params)
    except ParamValidationError as exc:
        pytest.fail(
            f"{service} ({name}.{operation}): the request never left botocore: {exc}. "
            "This is a harness gap, not a service defect."
        )
    except ClientError as exc:
        error = exc.response["Error"]
        assert error.get("Code"), (
            f"{service} ({name}.{operation}) failed without an AWS error code: {error}"
        )
        return

    pytest.fail(
        f"{service} ({name}.{operation}, {entry['protocol']}) returned "
        f"{response['ResponseMetadata']['HTTPStatusCode']} but the fidelity "
        "manifest says nothing serves this service — a success here is "
        "fabricated. If it is genuinely served now, run `make codegen`: the "
        "manifest is the source, and this test follows it."
    )
