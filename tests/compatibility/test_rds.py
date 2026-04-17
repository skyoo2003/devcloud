import pytest
from botocore.exceptions import ClientError


def test_create_and_describe_db_instance(rds_client):
    resp = rds_client.create_db_instance(
        DBInstanceIdentifier="compat-db-1",
        DBInstanceClass="db.t3.micro",
        Engine="mysql",
        MasterUsername="admin",
        MasterUserPassword="password123",
    )
    assert resp["DBInstance"]["DBInstanceIdentifier"] == "compat-db-1"
    assert resp["DBInstance"]["DBInstanceStatus"] == "available"

    desc = rds_client.describe_db_instances(DBInstanceIdentifier="compat-db-1")
    assert len(desc["DBInstances"]) == 1
    assert desc["DBInstances"][0]["Engine"] == "mysql"


def test_delete_db_instance(rds_client):
    rds_client.create_db_instance(
        DBInstanceIdentifier="del-db",
        DBInstanceClass="db.t3.micro",
        Engine="postgres",
        MasterUsername="admin",
        MasterUserPassword="password123",
    )
    rds_client.delete_db_instance(DBInstanceIdentifier="del-db", SkipFinalSnapshot=True)
    with pytest.raises(ClientError) as exc:
        rds_client.describe_db_instances(DBInstanceIdentifier="del-db")
    assert exc.value.response["Error"]["Code"] == "DBInstanceNotFound"


def test_create_and_describe_db_cluster(rds_client):
    resp = rds_client.create_db_cluster(
        DBClusterIdentifier="compat-cluster-1",
        Engine="aurora-mysql",
        MasterUsername="admin",
        MasterUserPassword="password123",
    )
    assert resp["DBCluster"]["DBClusterIdentifier"] == "compat-cluster-1"

    desc = rds_client.describe_db_clusters(DBClusterIdentifier="compat-cluster-1")
    assert len(desc["DBClusters"]) == 1


def test_delete_db_cluster(rds_client):
    rds_client.create_db_cluster(
        DBClusterIdentifier="del-cluster",
        Engine="aurora-mysql",
        MasterUsername="admin",
        MasterUserPassword="password123",
    )
    rds_client.delete_db_cluster(
        DBClusterIdentifier="del-cluster", SkipFinalSnapshot=True
    )


def test_create_and_delete_db_snapshot(rds_client):
    rds_client.create_db_instance(
        DBInstanceIdentifier="snap-db",
        DBInstanceClass="db.t3.micro",
        Engine="mysql",
        MasterUsername="admin",
        MasterUserPassword="password123",
    )
    snap = rds_client.create_db_snapshot(
        DBSnapshotIdentifier="snap-1", DBInstanceIdentifier="snap-db"
    )
    assert snap["DBSnapshot"]["DBSnapshotIdentifier"] == "snap-1"

    desc = rds_client.describe_db_snapshots(DBSnapshotIdentifier="snap-1")
    assert len(desc["DBSnapshots"]) == 1

    rds_client.delete_db_snapshot(DBSnapshotIdentifier="snap-1")


def test_describe_nonexistent_instance(rds_client):
    with pytest.raises(ClientError) as exc:
        rds_client.describe_db_instances(DBInstanceIdentifier="no-such-db")
    assert exc.value.response["Error"]["Code"] == "DBInstanceNotFound"
