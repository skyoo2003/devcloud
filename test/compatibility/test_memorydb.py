def test_create_and_describe_cluster(memorydb_client):
    resp = memorydb_client.create_cluster(
        ClusterName="compat-memdb",
        NodeType="db.r6g.large",
        ACLName="open-access",
    )
    assert resp["Cluster"]["Name"] == "compat-memdb"

    desc = memorydb_client.describe_clusters(ClusterName="compat-memdb")
    assert len(desc["Clusters"]) == 1
    assert desc["Clusters"][0]["Name"] == "compat-memdb"


def test_describe_clusters_list_all(memorydb_client):
    memorydb_client.create_cluster(
        ClusterName="list-memdb-1",
        NodeType="db.r6g.large",
        ACLName="open-access",
    )
    resp = memorydb_client.describe_clusters()
    assert "Clusters" in resp
    assert len(resp["Clusters"]) >= 1


def test_delete_cluster(memorydb_client):
    memorydb_client.create_cluster(
        ClusterName="del-memdb",
        NodeType="db.r6g.large",
        ACLName="open-access",
    )
    resp = memorydb_client.delete_cluster(ClusterName="del-memdb")
    assert resp["Cluster"]["Name"] == "del-memdb"
