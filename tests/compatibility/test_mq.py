import pytest
from botocore.exceptions import ClientError


def test_create_and_describe_broker(mq_client):
    resp = mq_client.create_broker(
        BrokerName="compat-broker",
        EngineType="ACTIVEMQ",
        EngineVersion="5.17.6",
        HostInstanceType="mq.m5.large",
        DeploymentMode="SINGLE_INSTANCE",
        Users=[{"Username": "admin", "Password": "adminPassword1"}],
        PubliclyAccessible=False,
    )
    broker_id = resp["BrokerId"]
    assert broker_id

    desc = mq_client.describe_broker(BrokerId=broker_id)
    assert desc["BrokerName"] == "compat-broker"
    assert desc["BrokerState"] == "RUNNING"


def test_list_brokers(mq_client):
    mq_client.create_broker(
        BrokerName="list-broker-1",
        EngineType="ACTIVEMQ",
        EngineVersion="5.17.6",
        HostInstanceType="mq.m5.large",
        DeploymentMode="SINGLE_INSTANCE",
        Users=[{"Username": "admin", "Password": "adminPassword1"}],
        PubliclyAccessible=False,
    )
    resp = mq_client.list_brokers()
    assert "BrokerSummaries" in resp
    names = [b["BrokerName"] for b in resp["BrokerSummaries"]]
    assert "list-broker-1" in names


def test_delete_broker(mq_client):
    resp = mq_client.create_broker(
        BrokerName="del-broker",
        EngineType="ACTIVEMQ",
        EngineVersion="5.17.6",
        HostInstanceType="mq.m5.large",
        DeploymentMode="SINGLE_INSTANCE",
        Users=[{"Username": "admin", "Password": "adminPassword1"}],
        PubliclyAccessible=False,
    )
    broker_id = resp["BrokerId"]
    mq_client.delete_broker(BrokerId=broker_id)

    with pytest.raises(ClientError):
        mq_client.describe_broker(BrokerId=broker_id)
