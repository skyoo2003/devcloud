"""Resolves the generated service manifest into things boto3 can be asked.

The compatibility suite used to carry two hand-written lists of service names.
A service could therefore be registered, counted in the published coverage
figure, and exercised by no test at all — which is what 31 of 205 registered
services were. This module removes the hand-written step: the fleet comes from
``internal/generated/compat/services.json``, which codegen derives from the same
fidelity manifest ``docs/coverage.md`` publishes its numbers from.

Three things have to be resolved per service, and each is resolved from data
rather than from a list someone remembered to update:

1. **Which boto3 client name reaches it.** DevCloud service IDs and boto3 client
   names are different vocabularies (``cloudwatchlogs`` vs ``logs``,
   ``costexplorer`` vs ``ce``). botocore's own model metadata is the bridge.
2. **Which operation to ask for.** Taken from the manifest's served set, so the
   suite never asks a service for an operation the manifest already calls
   unimplemented and then reports the honest failure as a broken service.
3. **What to pass it.** Most services declare a read that needs no input; 27 do
   not, and for those a stub is built from the operation's own input shape.
"""

import datetime
import json
import pathlib

import botocore.session

MANIFEST_PATH = (
    pathlib.Path(__file__).resolve().parents[2]
    / "internal"
    / "generated"
    / "compat"
    / "services.json"
)

# Service IDs whose boto3 client name cannot be derived, and why.
#
# The rule below refuses a contested name rather than guessing, mirroring
# codegen.BuildAliases: picking silently would test one service through another
# service's client and report the result under the wrong name.
#
# elasticloadbalancing is the only contested case in the fleet. ELB v1 and v2
# share the "elasticloadbalancing" endpoint prefix and signing name, and botocore
# splits them into two clients, so the prefix names both. v1 is "elb".
BOTO3_NAME_OVERRIDES = {
    "elasticloadbalancing": "elb",
}

# Registered services no boto3 caller can reach, so no boto3 test can exist for
# them. botocore publishes 431 clients and neither of these is among them —
# "sagemaker-runtime" and "transcribe" are different clients with different APIs.
#
# This is the ceiling on the compatibility-tested figure in docs/coverage.md, and
# it is a property of the AWS SDK rather than of DevCloud. Both services stay
# registered: a registered service answers locally in AWS's error vocabulary,
# which is the whole reason registering something DevCloud cannot serve beats
# leaving the call to reach a billed AWS account.
NO_BOTO3_CLIENT = {
    "sagemakerruntimehttp2": "botocore has no HTTP/2 SageMaker Runtime client",
    "transcribestreaming": "botocore has no streaming Transcribe client",
}

# Registered, counted as serving operations, and reachable by no boto3 caller.
#
# All four Lex clients sign as "lex" and none of the four services is called
# "lex", so codegen.BuildAliases leaves the name contested and unrouted — by
# design, because guessing would send one service's traffic to another. The
# consequence docs/coverage.md did not state until this was measured: the
# "reached by its own unambiguous name" escape does not exist for a boto3
# caller, because boto3 signs with the contested name and nothing else.
#
# Splitting them on the URL is what resolved opensearch/elasticsearchservice and
# apigateway v1/v2, and it does not work here: /bots is the first segment for
# lex-model-building-service, lex-models-v2 AND lex-runtime-v2. A correct split
# needs full path matching across four services, which is routing work rather
# than coverage work.
#
# Pinned by test_lex_services_are_unreachable_from_boto3 so that fixing the
# routing fails this test and forces the published figure up with it.
UNREACHABLE_FROM_BOTO3 = {
    "lexmodelbuildingservice": "signs as the contested alias 'lex'",
    "lexmodelsv2": "signs as the contested alias 'lex'",
    "lexruntimeservice": "signs as the contested alias 'lex'",
    "lexruntimev2": "signs as the contested alias 'lex'",
}

# A resource that does not exist in an empty store is a served answer: the engine
# looked in the store and reported honestly.
SERVED_ERROR_CODES = {
    "ResourceNotFoundException",
    "NotFoundException",
    "ResourceNotFound",
    "NoSuchEntity",
}

# Codes that mean nothing handled the request. UnknownService is the registry
# miss in internal/gateway/router.go; InvalidAction is a routed service refusing
# an operation the engine could not classify. Either one under a service the
# manifest says serves something is a wiring defect.
UNSERVED_ERROR_CODES = {
    "UnknownService",
    "InvalidAction",
    "NotImplemented",
}


def load_manifest():
    """Return the generated service manifest, keyed by DevCloud service ID."""
    with MANIFEST_PATH.open() as fh:
        return json.load(fh)


def _botocore_index(session):
    """Index botocore's clients by every name a model publishes for them.

    A name claimed by more than one client is dropped rather than resolved. The
    only one that matters today is "elasticloadbalancing", claimed by elb and
    elbv2; BOTO3_NAME_OVERRIDES carries its resolution.
    """
    available = set(session.get_available_services())

    bare = {}
    for name in available:
        bare.setdefault(name.replace("-", ""), set()).add(name)

    claims = {}
    for name in available:
        try:
            metadata = session.get_service_model(name).metadata
        except (
            Exception
        ):  # a model botocore ships but cannot load is not a client we can test
            continue
        for key in (
            metadata.get("endpointPrefix"),
            metadata.get("signingName"),
            metadata.get("serviceId"),
            metadata.get("targetPrefix"),
        ):
            if key:
                normalized = key.lower().replace(" ", "").replace("-", "")
                claims.setdefault(normalized, set()).add(name)

    return available, bare, claims


_SESSION = botocore.session.get_session()
_AVAILABLE, _BARE, _CLAIMS = _botocore_index(_SESSION)


def boto3_name(service_id):
    """Return the boto3 client name for a DevCloud service ID, or None.

    None means no boto3 caller can address the service. Every such case is in
    NO_BOTO3_CLIENT with a reason; anything else returning None is a resolution
    gap and test_every_registered_service_is_addressable fails on it.
    """
    if service_id in BOTO3_NAME_OVERRIDES:
        return BOTO3_NAME_OVERRIDES[service_id]
    if service_id in _AVAILABLE:
        return service_id

    bare = service_id.replace("-", "")
    for index in (_BARE, _CLAIMS):
        candidates = index.get(bare, set())
        if len(candidates) == 1:
            return next(iter(candidates))
    return None


def service_model(name):
    """Return botocore's model for a client name."""
    return _SESSION.get_service_model(name)


# How many operations to try before calling a service unserved. The floor is
# "at least one served operation answers", so one success is enough and one
# failure is not a verdict — the manifest can be right about a service and wrong
# about a particular operation, which is exactly what appsync/ListApis,
# eks/ListAccessPolicies, opensearch/ListApplications and
# neptune/DescribeDBClusterEndpoints turned out to be.
PROBE_LIMIT = 6


def probe_operations(boto3_client_name, served_ops):
    """Return operations to try, best first, as ``(operation, needs_input)``.

    Empty when the service serves nothing at all, which is the decline case.

    Reads are preferred over writes, and within reads a List is preferred over a
    Describe: a List of an empty store returns an empty collection, while a
    Describe of one absent resource correctly raises ResourceNotFoundException.
    Both are served answers, but the List case is unambiguous. An operation
    needing no input outranks all of them, because nothing about the request can
    then be blamed on the values this module invented.

    Only operations the manifest reports as served are considered. Asking for an
    unimplemented one would produce a correct refusal that reads as a defect.
    """
    if not served_ops:
        return []

    model = service_model(boto3_client_name)
    served = sorted(set(served_ops) & set(model.operation_names))
    if not served:
        return []

    def _required(op):
        shape = model.operation_model(op).input_shape
        return bool(shape is not None and shape.required_members)

    parameterless, with_input = [], []
    for prefix in ("List", "Describe", "Get"):
        for op in served:
            if not op.startswith(prefix):
                continue
            (with_input if _required(op) else parameterless).append((op, _required(op)))

    ordered = parameterless + with_input
    if not ordered:
        ordered = [(op, _required(op)) for op in served]
    return ordered[:PROBE_LIMIT]


def stub_params(boto3_client_name, operation):
    """Build the smallest input botocore will accept for an operation.

    Only required members, typed from the shape. The values are meaningless on
    purpose: the assertion these feed proves a request was routed and its
    operation classified, never that the answer is correct. A service that
    rejects the values with an AWS error code has already proved both.
    """
    shape = service_model(boto3_client_name).operation_model(operation).input_shape
    if shape is None:
        return {}
    return _stub_structure(shape, depth=0)


def _stub_structure(shape, depth):
    return {
        name: _stub_value(shape.members[name], depth + 1)
        for name in shape.required_members
    }


def _stub_value(shape, depth):
    kind = shape.type_name
    if kind == "structure":
        # Depth-limited: a self-referential shape would otherwise recurse
        # forever, and botocore does not validate members below what it was
        # given anyway.
        if depth > 4:
            return {}
        return _stub_structure(shape, depth)
    if kind == "list":
        # botocore validates minimum collection length client-side, so an empty
        # list never leaves the process for a shape that requires members —
        # which reads as a service defect when it is a harness one.
        minimum = shape.metadata.get("min", 0)
        if minimum:
            return [_stub_value(shape.member, depth + 1) for _ in range(minimum)]
        return []
    if kind == "map":
        minimum = shape.metadata.get("min", 0)
        if minimum:
            return {
                f"devcloud-test-{i}": _stub_value(shape.value, depth + 1)
                for i in range(minimum)
            }
        return {}
    if kind == "blob":
        return b""
    if kind == "boolean":
        return False
    if kind in ("integer", "long"):
        return 1
    if kind in ("float", "double"):
        return 1.0
    if kind == "timestamp":
        return datetime.datetime(2020, 1, 1)
    # Strings, including enums: botocore does not validate enum members
    # client-side, so an arbitrary value reaches the gateway, which is the point.
    return "devcloud-test"
