"""S3 Control routing.

S3 Control signs with S3's own signing name, so before the /v20180820/ split it
fell through DetectProtocol to the REST-XML default and the S3 provider parsed
every request as a bucket and a key. CreateAccessPoint returned 200 and left an
object behind — a fabricated success for a service that served nothing, which is
the one guarantee docs/coverage.md calls absolute.

These tests pin the routing, not the depth. They are written to survive the
engine learning rest-xml: none of them asserts that s3control fails, only that
whatever answers is s3control rather than S3.
"""

import pytest
from botocore.exceptions import ClientError

from conftest import _make_client

ACCOUNT_ID = "000000000000"

# Error codes only the S3 provider produces, by parsing a path as a bucket and
# a key. Seeing one of these from an S3 Control call means the request was
# misrouted, whatever the status code says.
S3_ERROR_CODES = {"NoSuchKey", "NoSuchBucket", "MethodNotAllowed", "InvalidRequest"}


@pytest.fixture
def s3control_client(devcloud_server):
    return _make_client("s3control")


def _error_code(call, **kwargs):
    """Run an operation and return its AWS error code, or None if it succeeded."""
    try:
        call(**kwargs)
        return None
    except ClientError as exc:
        return exc.response["Error"]["Code"]


def test_s3control_call_does_not_become_an_s3_object(s3control_client, s3_client):
    """The defect itself: a CreateAccessPoint must not create S3 state.

    Whether it succeeds or declines is a coverage question that Milestone 5's
    engine work answers. Whether it silently becomes an object is a correctness
    question, and the answer must always be no.
    """
    before = {b["Name"] for b in s3_client.list_buckets()["Buckets"]}

    _error_code(
        s3control_client.create_access_point,
        AccountId=ACCOUNT_ID,
        Name="my-access-point",
        Bucket="some-bucket",
    )

    after = {b["Name"] for b in s3_client.list_buckets()["Buckets"]}
    assert after == before, (
        "an S3 Control request created S3 state; it was routed to the S3 provider"
    )
    assert "v20180820" not in after, "the S3 Control URI prefix was stored as a bucket"


def test_s3control_answers_as_itself_not_as_s3(s3control_client):
    """Whatever s3control returns, it must not be S3's answer about a path S3
    invented. Before the split this was NoSuchKey for a bucket "v20180820"."""
    code = _error_code(s3control_client.list_access_points, AccountId=ACCOUNT_ID)
    assert code not in S3_ERROR_CODES, (
        f"got S3's {code} — the request reached the S3 provider, not s3control"
    )


def test_s3control_is_engine_served(s3control_client):
    """s3control's floor, proved here rather than in test_service_smoke.py.

    It cannot go in that harness's ENGINE_SERVED_SERVICES list: the list picks a
    parameterless List*/Describe* from the model, and all 97 of s3control's
    operations take a required AccountId. This is the budgets precedent — the
    service is served, and the proof needs one fixture instead of none.

    rest-xml is why it was the last protocol but one: botocore's RestXMLParser
    maps the response root's children onto the output shape, so this asserts the
    engine's generic XML is something a real parser accepts, not merely that Go
    produced a 200.
    """
    got = s3control_client.list_access_points(AccountId=ACCOUNT_ID)
    assert "AccessPointList" in got, f"no modelled member came back: {got}"


def test_s3control_round_trips_a_resource(s3control_client):
    """Create then read, through botocore both ways.

    The identifier arrives only as a path label — PUT /v20180820/accesspoint/
    {Name} — with no request body at all, because the gateway deliberately does
    not buffer rest-xml bodies. If the label were not read, the resource would
    be stored under a generated id and this read would come back empty.
    """
    name = "round-trip-ap"
    s3control_client.create_access_point(
        AccountId=ACCOUNT_ID, Name=name, Bucket="some-bucket"
    )

    listed = s3control_client.list_access_points(AccountId=ACCOUNT_ID)
    names = [ap.get("Name") for ap in listed.get("AccessPointList", [])]
    assert name in names, f"created access point is not in the list: {listed}"


def test_s3control_unclassifiable_operation_declines(s3control_client):
    """The floor is a floor. AssociateAccessGrantsIdentityCenter is one of the
    three s3control operations that is not CRUD-shaped, so codegen registers
    nothing for it and it must decline rather than answer from the store."""
    code = _error_code(
        s3control_client.associate_access_grants_identity_center,
        AccountId=ACCOUNT_ID,
        IdentityCenterArn="arn:aws:sso:::instance/ssoins-000000000000",
    )
    assert code is not None, "an unclassifiable operation returned a success"


def test_s3_is_unaffected_by_the_split(s3_client):
    """The split must cost S3 nothing for any normal bucket or key."""
    s3_client.create_bucket(Bucket="split-guard-bucket")
    names = {b["Name"] for b in s3_client.list_buckets()["Buckets"]}
    assert "split-guard-bucket" in names

    # A key that merely contains the prefix is still S3's: only a path that
    # *starts* with /v20180820/ is claimed.
    s3_client.put_object(Bucket="split-guard-bucket", Key="v20180820/nested", Body=b"x")
    got = s3_client.get_object(Bucket="split-guard-bucket", Key="v20180820/nested")
    assert got["Body"].read() == b"x"


def test_bucket_named_like_the_prefix_is_shadowed_cleanly(s3_client):
    """The one case the split gets wrong, asserted rather than left to be found.

    A bucket named exactly "v20180820" produces object paths under
    /v20180820/..., which is indistinguishable from an S3 Control request. The
    prefix wins, so those objects are unreachable.

    This is the deliberate trade. The alternative — gating on the
    x-amz-account-id header, which 96 of S3 Control's 97 operations send and no
    S3 operation does — would route Outposts `CreateBucket PUT
    /v20180820/bucket/{Bucket}` to S3, where it becomes a stored object and a
    200. A wrong-but-honest error beats a fabricated success, so the shadowing
    stands and is tested.
    """
    # Creating it is fine: PUT /v20180820 has no trailing slash, so it is S3's.
    s3_client.create_bucket(Bucket="v20180820")
    assert "v20180820" in {b["Name"] for b in s3_client.list_buckets()["Buckets"]}

    # Putting an object into it is not: the path becomes /v20180820/<key>.
    code = _error_code(
        s3_client.put_object, Bucket="v20180820", Key="shadowed", Body=b"x"
    )
    assert code is not None, (
        "an object write into the shadowed bucket must not silently succeed"
    )
    assert code not in S3_ERROR_CODES or code == "InvalidRequest", (
        f"expected s3control's clean refusal, got {code}"
    )
