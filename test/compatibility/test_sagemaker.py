import pytest
from botocore.exceptions import ClientError


def test_create_and_describe_notebook_instance(sagemaker_client):
    resp = sagemaker_client.create_notebook_instance(
        NotebookInstanceName="compat-nb",
        InstanceType="ml.t3.medium",
        RoleArn="arn:aws:iam::000000000000:role/sagemaker-role",
    )
    assert "NotebookInstanceArn" in resp

    desc = sagemaker_client.describe_notebook_instance(NotebookInstanceName="compat-nb")
    assert desc["NotebookInstanceName"] == "compat-nb"


def test_list_notebook_instances(sagemaker_client):
    sagemaker_client.create_notebook_instance(
        NotebookInstanceName="list-nb",
        InstanceType="ml.t3.medium",
        RoleArn="arn:aws:iam::000000000000:role/sagemaker-role",
    )
    resp = sagemaker_client.list_notebook_instances()
    names = [n["NotebookInstanceName"] for n in resp["NotebookInstances"]]
    assert "list-nb" in names


def test_delete_notebook_instance(sagemaker_client):
    sagemaker_client.create_notebook_instance(
        NotebookInstanceName="del-nb",
        InstanceType="ml.t3.medium",
        RoleArn="arn:aws:iam::000000000000:role/sagemaker-role",
    )
    sagemaker_client.stop_notebook_instance(NotebookInstanceName="del-nb")
    sagemaker_client.delete_notebook_instance(NotebookInstanceName="del-nb")


def test_create_and_describe_endpoint(sagemaker_client):
    sagemaker_client.create_model(
        ModelName="compat-model",
        PrimaryContainer={
            "Image": "000000000000.dkr.ecr.us-east-1.amazonaws.com/model:latest"
        },
        ExecutionRoleArn="arn:aws:iam::000000000000:role/sagemaker-role",
    )
    sagemaker_client.create_endpoint_config(
        EndpointConfigName="compat-epc",
        ProductionVariants=[
            {
                "VariantName": "v1",
                "ModelName": "compat-model",
                "InitialInstanceCount": 1,
                "InstanceType": "ml.m5.large",
            }
        ],
    )
    resp = sagemaker_client.create_endpoint(
        EndpointName="compat-ep", EndpointConfigName="compat-epc"
    )
    assert "EndpointArn" in resp

    desc = sagemaker_client.describe_endpoint(EndpointName="compat-ep")
    assert desc["EndpointName"] == "compat-ep"


def test_describe_nonexistent_notebook(sagemaker_client):
    with pytest.raises(ClientError):
        sagemaker_client.describe_notebook_instance(NotebookInstanceName="no-such-nb")
