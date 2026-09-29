"""Smoke tests for the generic CRUD fallback engine.

These exercise operations the hand-written provider does NOT implement, proving
the router → engine fallback serves plausible, store-backed responses end-to-end
via boto3. Fidelity is intentionally "plausible, not faithful" (see
internal/shared/crud), so assertions stay at round-trip level."""

import pytest
from botocore.exceptions import ClientError


def test_glue_engine_blueprint_crud(glue_client):
    # glue does not hand-implement Blueprint ops; they are served by the engine.
    glue_client.create_blueprint(Name="bp1", BlueprintLocation="s3://bucket/bp.zip")

    got = glue_client.get_blueprint(Name="bp1")
    assert got["ResponseMetadata"]["HTTPStatusCode"] == 200
    assert got["Blueprint"]["Name"] == "bp1"

    glue_client.delete_blueprint(Name="bp1")
    with pytest.raises(ClientError):
        glue_client.get_blueprint(Name="bp1")


def test_glue_hand_written_still_wins(glue_client):
    # Databases ARE hand-implemented; the engine must not shadow them.
    glue_client.create_database(DatabaseInput={"Name": "db_real"})
    resp = glue_client.get_database(Name="db_real")
    assert resp["Database"]["Name"] == "db_real"
