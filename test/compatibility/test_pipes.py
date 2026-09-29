import pytest
import botocore.exceptions


def test_create_pipe(pipes_client):
    resp = pipes_client.create_pipe(
        Name="test-pipe-1",
        Source="arn:aws:sqs:us-east-1:000000000000:source-queue",
        Target="arn:aws:lambda:us-east-1:000000000000:function:target-fn",
        RoleArn="arn:aws:iam::000000000000:role/PipeRole",
    )
    assert (
        resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        or resp["ResponseMetadata"]["HTTPStatusCode"] == 201
    )
    assert resp["Name"] == "test-pipe-1"
    assert "Arn" in resp


def test_describe_pipe(pipes_client):
    pipes_client.create_pipe(
        Name="desc-pipe",
        Source="arn:aws:sqs:us-east-1:000000000000:q",
        Target="arn:aws:lambda:us-east-1:000000000000:function:fn",
        RoleArn="arn:aws:iam::000000000000:role/R",
    )
    resp = pipes_client.describe_pipe(Name="desc-pipe")
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
    assert resp["Name"] == "desc-pipe"
    assert "Arn" in resp
    assert "CurrentState" in resp


def test_list_pipes(pipes_client):
    for i in range(3):
        pipes_client.create_pipe(
            Name=f"list-pipe-{i}",
            Source="arn:aws:sqs:us-east-1:000000000000:q",
            Target="arn:aws:lambda:us-east-1:000000000000:function:fn",
            RoleArn="arn:aws:iam::000000000000:role/R",
        )

    resp = pipes_client.list_pipes()
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
    assert "Pipes" in resp
    names = [p["Name"] for p in resp["Pipes"]]
    assert "list-pipe-0" in names
    assert "list-pipe-1" in names
    assert "list-pipe-2" in names


def test_update_pipe(pipes_client):
    pipes_client.create_pipe(
        Name="upd-pipe",
        Source="arn:aws:sqs:us-east-1:000000000000:q",
        Target="arn:aws:lambda:us-east-1:000000000000:function:fn",
        RoleArn="arn:aws:iam::000000000000:role/R",
    )
    resp = pipes_client.update_pipe(
        Name="upd-pipe",
        Description="updated description",
        RoleArn="arn:aws:iam::000000000000:role/NewRole",
    )
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
    assert resp["Name"] == "upd-pipe"


def test_delete_pipe(pipes_client):
    pipes_client.create_pipe(
        Name="del-pipe",
        Source="arn:aws:sqs:us-east-1:000000000000:q",
        Target="arn:aws:lambda:us-east-1:000000000000:function:fn",
        RoleArn="arn:aws:iam::000000000000:role/R",
    )
    resp = pipes_client.delete_pipe(Name="del-pipe")
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    with pytest.raises(botocore.exceptions.ClientError) as exc:
        pipes_client.describe_pipe(Name="del-pipe")
    assert exc.value.response["Error"]["Code"] in ("NotFoundException", "404")


def test_start_stop_pipe(pipes_client):
    pipes_client.create_pipe(
        Name="ss-pipe",
        Source="arn:aws:sqs:us-east-1:000000000000:q",
        Target="arn:aws:lambda:us-east-1:000000000000:function:fn",
        RoleArn="arn:aws:iam::000000000000:role/R",
        DesiredState="STOPPED",
    )

    start_resp = pipes_client.start_pipe(Name="ss-pipe")
    assert start_resp["ResponseMetadata"]["HTTPStatusCode"] == 200
    assert start_resp["CurrentState"] == "RUNNING"

    stop_resp = pipes_client.stop_pipe(Name="ss-pipe")
    assert stop_resp["ResponseMetadata"]["HTTPStatusCode"] == 200
    assert stop_resp["CurrentState"] == "STOPPED"


def test_tag_resource(pipes_client):
    create_resp = pipes_client.create_pipe(
        Name="tag-pipe",
        Source="arn:aws:sqs:us-east-1:000000000000:q",
        Target="arn:aws:lambda:us-east-1:000000000000:function:fn",
        RoleArn="arn:aws:iam::000000000000:role/R",
    )
    arn = create_resp["Arn"]

    pipes_client.tag_resource(resourceArn=arn, tags={"env": "prod", "team": "platform"})

    resp = pipes_client.list_tags_for_resource(resourceArn=arn)
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
    assert resp["tags"]["env"] == "prod"
    assert resp["tags"]["team"] == "platform"

    pipes_client.untag_resource(resourceArn=arn, tagKeys=["env"])
    resp2 = pipes_client.list_tags_for_resource(resourceArn=arn)
    assert "env" not in resp2["tags"]
    assert resp2["tags"]["team"] == "platform"
