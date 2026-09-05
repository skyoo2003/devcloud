"""Elastic Load Balancing v1, served by the generic CRUD engine over Query.

query was the last protocol the engine could not read. Its operation is neither
an X-Amz-Target header nor a modelled path but the Action field of a form body,
and its responses are XML in a dialect of their own: botocore's query parser
looks for <OperationResult> nested inside <OperationResponse> and, given
anything else, returns a result with nothing in it rather than an error.

That failure mode is why these tests assert on parsed members instead of on
status codes. A 200 whose body botocore silently discards looks identical to a
working service from Go's side of the wire.
"""

import pytest
from botocore.exceptions import ClientError

from conftest import _make_client


@pytest.fixture
def elb_client(devcloud_server):
    return _make_client("elb")


def test_describe_on_empty_store(elb_client):
    """The parameterless read, and the first thing that would break if the
    query envelope were wrong: an empty collection must parse as an empty list,
    not as a missing key."""
    got = elb_client.describe_load_balancers()
    assert got["LoadBalancerDescriptions"] == []


def test_create_then_describe_round_trips(elb_client):
    """The name the caller sent in a form field comes back through the XML.

    This is the whole query path end to end: Action picks the operation,
    LoadBalancerName is stored as the identifier, and the response is re-parsed
    by botocore into a modelled member.
    """
    elb_client.create_load_balancer(
        LoadBalancerName="my-lb",
        Listeners=[{"Protocol": "HTTP", "LoadBalancerPort": 80, "InstancePort": 8080}],
    )

    got = elb_client.describe_load_balancers()
    names = [lb["LoadBalancerName"] for lb in got["LoadBalancerDescriptions"]]
    assert "my-lb" in names


def test_delete_removes_it(elb_client):
    elb_client.create_load_balancer(LoadBalancerName="doomed-lb", Listeners=[])
    elb_client.delete_load_balancer(LoadBalancerName="doomed-lb")

    got = elb_client.describe_load_balancers()
    names = [lb["LoadBalancerName"] for lb in got["LoadBalancerDescriptions"]]
    assert "doomed-lb" not in names


def test_unclassifiable_operation_declines(elb_client):
    """The floor is a floor. Eleven of ELB's 29 operations are not CRUD-shaped,
    so codegen registers nothing for them and they must return a clean AWS error
    rather than an empty success.

    ConfigureHealthCheck is the one worth naming: a Terraform aws_elb resource
    calls it, so this is the honest edge of what "elb is covered" means.
    """
    with pytest.raises(ClientError) as exc:
        elb_client.configure_health_check(
            LoadBalancerName="my-lb",
            HealthCheck={
                "Target": "HTTP:80/",
                "Interval": 30,
                "Timeout": 5,
                "UnhealthyThreshold": 2,
                "HealthyThreshold": 2,
            },
        )
    # The code has to survive botocore's query parser. A bare <Error> envelope
    # here would produce a ClientError with no Code at all, which tells the
    # caller nothing about why the call failed.
    assert exc.value.response["Error"]["Code"], "botocore parsed no error code"


def test_elbv2_is_unaffected(devcloud_server):
    """ELB v1 and v2 share the signing name "elasticloadbalancing" and both
    speak query, so only the API version in the request body separates them.
    v2 has a hand-written provider and must keep reaching it."""
    v2 = _make_client("elbv2")
    got = v2.describe_load_balancers()
    assert "LoadBalancers" in got
