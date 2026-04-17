import json
import pytest
from botocore.exceptions import ClientError


SIMPLE_DEFINITION = json.dumps(
    {
        "StartAt": "Pass",
        "States": {"Pass": {"Type": "Pass", "End": True}},
    }
)


def test_create_and_describe_state_machine(sfn_client):
    resp = sfn_client.create_state_machine(
        name="compat-sm",
        definition=SIMPLE_DEFINITION,
        roleArn="arn:aws:iam::123456789012:role/test",
    )
    sm_arn = resp["stateMachineArn"]
    assert sm_arn

    desc = sfn_client.describe_state_machine(stateMachineArn=sm_arn)
    assert desc["name"] == "compat-sm"


def test_list_state_machines(sfn_client):
    sfn_client.create_state_machine(
        name="list-sm-1",
        definition=SIMPLE_DEFINITION,
        roleArn="arn:aws:iam::123456789012:role/test",
    )
    resp = sfn_client.list_state_machines()
    names = [sm["name"] for sm in resp["stateMachines"]]
    assert "list-sm-1" in names


def test_delete_state_machine(sfn_client):
    resp = sfn_client.create_state_machine(
        name="del-sm",
        definition=SIMPLE_DEFINITION,
        roleArn="arn:aws:iam::123456789012:role/test",
    )
    sm_arn = resp["stateMachineArn"]
    sfn_client.delete_state_machine(stateMachineArn=sm_arn)

    with pytest.raises(ClientError):
        sfn_client.describe_state_machine(stateMachineArn=sm_arn)


def test_start_execution(sfn_client):
    resp = sfn_client.create_state_machine(
        name="exec-sm",
        definition=SIMPLE_DEFINITION,
        roleArn="arn:aws:iam::123456789012:role/test",
    )
    sm_arn = resp["stateMachineArn"]

    exec_resp = sfn_client.start_execution(stateMachineArn=sm_arn)
    assert "executionArn" in exec_resp
