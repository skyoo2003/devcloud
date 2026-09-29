import pytest
from botocore.exceptions import ClientError


def test_create_and_describe_db_cluster(neptune_client):
    resp = neptune_client.create_db_cluster(
        DBClusterIdentifier="compat-neptune",
        Engine="neptune",
    )
    assert resp["DBCluster"]["DBClusterIdentifier"] == "compat-neptune"

    desc = neptune_client.describe_db_clusters(DBClusterIdentifier="compat-neptune")
    assert len(desc["DBClusters"]) == 1


def test_delete_db_cluster(neptune_client):
    neptune_client.create_db_cluster(
        DBClusterIdentifier="del-neptune",
        Engine="neptune",
    )
    neptune_client.delete_db_cluster(
        DBClusterIdentifier="del-neptune", SkipFinalSnapshot=True
    )


def test_create_db_instance(neptune_client):
    neptune_client.create_db_cluster(
        DBClusterIdentifier="ninst-neptune",
        Engine="neptune",
    )
    resp = neptune_client.create_db_instance(
        DBInstanceIdentifier="ninst-1",
        DBClusterIdentifier="ninst-neptune",
        DBInstanceClass="db.r5.large",
        Engine="neptune",
    )
    assert resp["DBInstance"]["DBInstanceIdentifier"] == "ninst-1"


def test_describe_nonexistent_cluster(neptune_client):
    with pytest.raises(ClientError) as exc:
        neptune_client.describe_db_clusters(DBClusterIdentifier="no-such-neptune")
    assert exc.value.response["Error"]["Code"] == "DBClusterNotFoundFault"
