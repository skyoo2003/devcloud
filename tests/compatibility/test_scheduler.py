import pytest
from botocore.exceptions import ClientError


def test_create_and_get_schedule(scheduler_client):
    resp = scheduler_client.create_schedule(
        Name="compat-schedule",
        ScheduleExpression="rate(5 minutes)",
        Target={
            "Arn": "arn:aws:lambda:us-east-1:123456789012:function:my-func",
            "RoleArn": "arn:aws:iam::123456789012:role/test",
        },
        FlexibleTimeWindow={"Mode": "OFF"},
    )
    assert "ScheduleArn" in resp
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    get_resp = scheduler_client.get_schedule(Name="compat-schedule")
    assert get_resp["Name"] == "compat-schedule"
    assert get_resp["ScheduleExpression"] == "rate(5 minutes)"


def test_update_schedule(scheduler_client):
    scheduler_client.create_schedule(
        Name="update-schedule",
        ScheduleExpression="rate(1 minute)",
        Target={
            "Arn": "arn:aws:lambda:us-east-1:123456789012:function:fn",
            "RoleArn": "arn:aws:iam::123456789012:role/test",
        },
        FlexibleTimeWindow={"Mode": "OFF"},
    )
    scheduler_client.update_schedule(
        Name="update-schedule",
        ScheduleExpression="rate(30 minutes)",
        Target={
            "Arn": "arn:aws:lambda:us-east-1:123456789012:function:fn",
            "RoleArn": "arn:aws:iam::123456789012:role/test",
        },
        FlexibleTimeWindow={"Mode": "OFF"},
    )
    get_resp = scheduler_client.get_schedule(Name="update-schedule")
    assert get_resp["ScheduleExpression"] == "rate(30 minutes)"


def test_delete_schedule(scheduler_client):
    scheduler_client.create_schedule(
        Name="del-schedule",
        ScheduleExpression="rate(1 minute)",
        Target={
            "Arn": "arn:aws:lambda:us-east-1:123456789012:function:fn",
            "RoleArn": "arn:aws:iam::123456789012:role/test",
        },
        FlexibleTimeWindow={"Mode": "OFF"},
    )
    scheduler_client.delete_schedule(Name="del-schedule")

    with pytest.raises(ClientError):
        scheduler_client.get_schedule(Name="del-schedule")


def test_list_schedules(scheduler_client):
    scheduler_client.create_schedule(
        Name="list-schedule",
        ScheduleExpression="rate(5 minutes)",
        Target={
            "Arn": "arn:aws:lambda:us-east-1:123456789012:function:fn",
            "RoleArn": "arn:aws:iam::123456789012:role/test",
        },
        FlexibleTimeWindow={"Mode": "OFF"},
    )
    resp = scheduler_client.list_schedules()
    assert "Schedules" in resp
    names = [s["Name"] for s in resp["Schedules"]]
    assert "list-schedule" in names


def test_create_and_get_schedule_group(scheduler_client):
    resp = scheduler_client.create_schedule_group(Name="compat-group")
    assert "ScheduleGroupArn" in resp
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    get_resp = scheduler_client.get_schedule_group(Name="compat-group")
    assert get_resp["Name"] == "compat-group"
    assert get_resp["State"] == "ACTIVE"


def test_list_schedule_groups(scheduler_client):
    scheduler_client.create_schedule_group(Name="list-group")
    resp = scheduler_client.list_schedule_groups()
    assert "ScheduleGroups" in resp
    names = [g["Name"] for g in resp["ScheduleGroups"]]
    assert "list-group" in names


def test_delete_schedule_group(scheduler_client):
    scheduler_client.create_schedule_group(Name="del-group")
    scheduler_client.delete_schedule_group(Name="del-group")

    with pytest.raises(ClientError):
        scheduler_client.get_schedule_group(Name="del-group")


def test_tag_schedule_group(scheduler_client):
    resp = scheduler_client.create_schedule_group(Name="tag-group")
    arn = resp["ScheduleGroupArn"]

    scheduler_client.tag_resource(
        ResourceArn=arn,
        Tags=[{"Key": "env", "Value": "prod"}],
    )
    listed = scheduler_client.list_tags_for_resource(ResourceArn=arn)
    assert "Tags" in listed


def test_untag_schedule_group(scheduler_client):
    resp = scheduler_client.create_schedule_group(Name="untag-group")
    arn = resp["ScheduleGroupArn"]

    scheduler_client.tag_resource(
        ResourceArn=arn,
        Tags=[{"Key": "env", "Value": "prod"}],
    )
    scheduler_client.untag_resource(
        ResourceArn=arn,
        TagKeys=["env"],
    )


def test_schedule_in_custom_group(scheduler_client):
    scheduler_client.create_schedule_group(Name="my-group")
    scheduler_client.create_schedule(
        Name="grouped-schedule",
        GroupName="my-group",
        ScheduleExpression="rate(10 minutes)",
        Target={
            "Arn": "arn:aws:lambda:us-east-1:123:function:fn",
            "RoleArn": "arn:aws:iam::123:role/test",
        },
        FlexibleTimeWindow={"Mode": "OFF"},
    )
    get_resp = scheduler_client.get_schedule(
        Name="grouped-schedule",
        GroupName="my-group",
    )
    assert get_resp["Name"] == "grouped-schedule"
    assert get_resp["GroupName"] == "my-group"
