import pytest
from botocore.exceptions import ClientError


def test_create_and_describe_cluster(redshift_client):
    resp = redshift_client.create_cluster(
        ClusterIdentifier="compat-rs",
        NodeType="dc2.large",
        MasterUsername="admin",
        MasterUserPassword="Password123!",
        ClusterType="single-node",
    )
    assert resp["Cluster"]["ClusterIdentifier"] == "compat-rs"

    desc = redshift_client.describe_clusters(ClusterIdentifier="compat-rs")
    assert len(desc["Clusters"]) == 1


def test_delete_cluster(redshift_client):
    redshift_client.create_cluster(
        ClusterIdentifier="del-rs",
        NodeType="dc2.large",
        MasterUsername="admin",
        MasterUserPassword="Password123!",
        ClusterType="single-node",
    )
    redshift_client.delete_cluster(
        ClusterIdentifier="del-rs", SkipFinalClusterSnapshot=True
    )


def test_create_cluster_snapshot(redshift_client):
    redshift_client.create_cluster(
        ClusterIdentifier="snap-rs",
        NodeType="dc2.large",
        MasterUsername="admin",
        MasterUserPassword="Password123!",
        ClusterType="single-node",
    )
    snap = redshift_client.create_cluster_snapshot(
        SnapshotIdentifier="snap-rs-1", ClusterIdentifier="snap-rs"
    )
    assert snap["Snapshot"]["SnapshotIdentifier"] == "snap-rs-1"

    desc = redshift_client.describe_cluster_snapshots(SnapshotIdentifier="snap-rs-1")
    assert len(desc["Snapshots"]) == 1

    redshift_client.delete_cluster_snapshot(SnapshotIdentifier="snap-rs-1")


def test_describe_nonexistent_cluster(redshift_client):
    with pytest.raises(ClientError) as exc:
        redshift_client.describe_clusters(ClusterIdentifier="no-such-rs")
    assert exc.value.response["Error"]["Code"] == "ClusterNotFound"
