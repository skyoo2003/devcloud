import pytest
from botocore.exceptions import ClientError


def test_create_and_get_work_group(athena_client):
    athena_client.create_work_group(
        Name="compat-wg",
        Description="test work group",
    )

    resp = athena_client.get_work_group(WorkGroup="compat-wg")
    assert resp["WorkGroup"]["Name"] == "compat-wg"


def test_list_work_groups(athena_client):
    athena_client.create_work_group(Name="list-wg-1")
    resp = athena_client.list_work_groups()
    names = [wg["Name"] for wg in resp["WorkGroups"]]
    assert "list-wg-1" in names


def test_delete_work_group(athena_client):
    athena_client.create_work_group(Name="del-wg")
    athena_client.delete_work_group(WorkGroup="del-wg")

    with pytest.raises(ClientError):
        athena_client.get_work_group(WorkGroup="del-wg")


def test_start_query_execution(athena_client):
    athena_client.create_work_group(Name="query-wg")
    resp = athena_client.start_query_execution(
        QueryString="SELECT 1",
        WorkGroup="query-wg",
    )
    assert "QueryExecutionId" in resp

    exec_resp = athena_client.get_query_execution(
        QueryExecutionId=resp["QueryExecutionId"]
    )
    assert exec_resp["QueryExecution"]["QueryExecutionId"] == resp["QueryExecutionId"]
