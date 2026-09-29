import pytest
from botocore.exceptions import ClientError


def test_create_and_describe_db_cluster(docdb_client):
    resp = docdb_client.create_db_cluster(
        DBClusterIdentifier="compat-docdb",
        Engine="docdb",
        MasterUsername="admin",
        MasterUserPassword="password123",
    )
    assert resp["DBCluster"]["DBClusterIdentifier"] == "compat-docdb"

    desc = docdb_client.describe_db_clusters(DBClusterIdentifier="compat-docdb")
    assert len(desc["DBClusters"]) == 1


def test_delete_db_cluster(docdb_client):
    docdb_client.create_db_cluster(
        DBClusterIdentifier="del-docdb",
        Engine="docdb",
        MasterUsername="admin",
        MasterUserPassword="password123",
    )
    docdb_client.delete_db_cluster(
        DBClusterIdentifier="del-docdb", SkipFinalSnapshot=True
    )


def test_create_db_instance(docdb_client):
    docdb_client.create_db_cluster(
        DBClusterIdentifier="inst-docdb",
        Engine="docdb",
        MasterUsername="admin",
        MasterUserPassword="password123",
    )
    resp = docdb_client.create_db_instance(
        DBInstanceIdentifier="inst-docdb-1",
        DBClusterIdentifier="inst-docdb",
        DBInstanceClass="db.r5.large",
        Engine="docdb",
    )
    assert resp["DBInstance"]["DBInstanceIdentifier"] == "inst-docdb-1"


def test_describe_nonexistent_cluster(docdb_client):
    with pytest.raises(ClientError) as exc:
        docdb_client.describe_db_clusters(DBClusterIdentifier="no-such-docdb")
    assert exc.value.response["Error"]["Code"] == "DBClusterNotFoundFault"
