import pytest
from botocore.exceptions import ClientError


def test_create_and_describe_cluster(eks_client):
    resp = eks_client.create_cluster(
        name="compat-eks",
        roleArn="arn:aws:iam::123456789012:role/eks-role",
        resourcesVpcConfig={
            "subnetIds": ["subnet-1", "subnet-2"],
            "securityGroupIds": ["sg-1"],
        },
    )
    assert resp["cluster"]["name"] == "compat-eks"

    desc = eks_client.describe_cluster(name="compat-eks")
    assert desc["cluster"]["name"] == "compat-eks"
    assert "status" in desc["cluster"]


def test_list_clusters(eks_client):
    eks_client.create_cluster(
        name="list-eks-1",
        roleArn="arn:aws:iam::123456789012:role/eks-role",
        resourcesVpcConfig={
            "subnetIds": ["subnet-1"],
            "securityGroupIds": ["sg-1"],
        },
    )
    resp = eks_client.list_clusters()
    assert "clusters" in resp
    assert "list-eks-1" in resp["clusters"]


def test_delete_cluster(eks_client):
    eks_client.create_cluster(
        name="del-eks",
        roleArn="arn:aws:iam::123456789012:role/eks-role",
        resourcesVpcConfig={
            "subnetIds": ["subnet-1"],
            "securityGroupIds": ["sg-1"],
        },
    )
    eks_client.delete_cluster(name="del-eks")

    with pytest.raises(ClientError):
        eks_client.describe_cluster(name="del-eks")
