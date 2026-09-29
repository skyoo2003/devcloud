def test_create_replication_instance(dms_client):
    resp = dms_client.create_replication_instance(
        ReplicationInstanceIdentifier="compat-ri",
        ReplicationInstanceClass="dms.t2.micro",
    )
    inst = resp["ReplicationInstance"]
    assert inst["ReplicationInstanceIdentifier"] == "compat-ri"
    assert inst["ReplicationInstanceClass"] == "dms.t2.micro"


def test_describe_replication_instances(dms_client):
    dms_client.create_replication_instance(
        ReplicationInstanceIdentifier="compat-ri2",
        ReplicationInstanceClass="dms.t2.micro",
    )
    resp = dms_client.describe_replication_instances()
    ids = [i["ReplicationInstanceIdentifier"] for i in resp["ReplicationInstances"]]
    assert "compat-ri2" in ids


def test_create_endpoint(dms_client):
    resp = dms_client.create_endpoint(
        EndpointIdentifier="compat-ep",
        EndpointType="source",
        EngineName="mysql",
    )
    assert resp["Endpoint"]["EndpointIdentifier"] == "compat-ep"


def test_describe_account_attributes(dms_client):
    resp = dms_client.describe_account_attributes()
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
