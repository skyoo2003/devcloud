def test_create_and_describe_cluster(kafka_client):
    resp = kafka_client.create_cluster(
        ClusterName="compat-kafka",
        KafkaVersion="3.5.1",
        NumberOfBrokerNodes=3,
        BrokerNodeGroupInfo={
            "InstanceType": "kafka.m5.large",
            "ClientSubnets": ["subnet-1"],
        },
    )
    cluster_arn = resp["ClusterArn"]
    assert cluster_arn

    desc = kafka_client.describe_cluster(ClusterArn=cluster_arn)
    assert desc["ClusterInfo"]["ClusterName"] == "compat-kafka"


def test_list_clusters(kafka_client):
    kafka_client.create_cluster(
        ClusterName="list-kafka-1",
        KafkaVersion="3.5.1",
        NumberOfBrokerNodes=3,
        BrokerNodeGroupInfo={
            "InstanceType": "kafka.m5.large",
            "ClientSubnets": ["subnet-1"],
        },
    )
    resp = kafka_client.list_clusters()
    assert "ClusterInfoList" in resp
    assert len(resp["ClusterInfoList"]) >= 1


def test_delete_cluster(kafka_client):
    resp = kafka_client.create_cluster(
        ClusterName="del-kafka",
        KafkaVersion="3.5.1",
        NumberOfBrokerNodes=3,
        BrokerNodeGroupInfo={
            "InstanceType": "kafka.m5.large",
            "ClientSubnets": ["subnet-1"],
        },
    )
    cluster_arn = resp["ClusterArn"]
    kafka_client.delete_cluster(ClusterArn=cluster_arn)
