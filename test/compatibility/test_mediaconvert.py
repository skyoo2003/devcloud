import pytest
from botocore.exceptions import ClientError


def test_create_and_get_job_template(mediaconvert_client):
    resp = mediaconvert_client.create_job_template(
        Name="my-template",
        Description="Test job template",
        Settings={
            "OutputGroups": [],
            "Inputs": [],
        },
    )
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 201
    tmpl = resp["JobTemplate"]
    assert tmpl["Name"] == "my-template"
    assert tmpl["Description"] == "Test job template"

    get_resp = mediaconvert_client.get_job_template(Name="my-template")
    assert get_resp["JobTemplate"]["Name"] == "my-template"


def test_list_job_templates(mediaconvert_client):
    mediaconvert_client.create_job_template(
        Name="list-template-1",
        Settings={"OutputGroups": [], "Inputs": []},
    )
    mediaconvert_client.create_job_template(
        Name="list-template-2",
        Settings={"OutputGroups": [], "Inputs": []},
    )
    resp = mediaconvert_client.list_job_templates()
    names = [t["Name"] for t in resp["JobTemplates"]]
    assert "list-template-1" in names
    assert "list-template-2" in names


def test_delete_job_template(mediaconvert_client):
    mediaconvert_client.create_job_template(
        Name="del-template",
        Settings={"OutputGroups": [], "Inputs": []},
    )
    mediaconvert_client.delete_job_template(Name="del-template")

    with pytest.raises(ClientError) as exc_info:
        mediaconvert_client.get_job_template(Name="del-template")
    assert exc_info.value.response["ResponseMetadata"]["HTTPStatusCode"] == 404


def test_create_and_get_queue(mediaconvert_client):
    resp = mediaconvert_client.create_queue(
        Name="my-queue",
        Description="Test queue",
        PricingPlan="ON_DEMAND",
    )
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 201
    q = resp["Queue"]
    assert q["Name"] == "my-queue"
    assert q["Status"] == "ACTIVE"
    assert q["PricingPlan"] == "ON_DEMAND"

    get_resp = mediaconvert_client.get_queue(Name="my-queue")
    assert get_resp["Queue"]["Name"] == "my-queue"


def test_list_queues(mediaconvert_client):
    mediaconvert_client.create_queue(Name="list-q-1")
    mediaconvert_client.create_queue(Name="list-q-2")
    resp = mediaconvert_client.list_queues()
    names = [q["Name"] for q in resp["Queues"]]
    assert "list-q-1" in names
    assert "list-q-2" in names


def test_delete_queue(mediaconvert_client):
    mediaconvert_client.create_queue(Name="del-queue")
    mediaconvert_client.delete_queue(Name="del-queue")

    with pytest.raises(ClientError) as exc_info:
        mediaconvert_client.get_queue(Name="del-queue")
    assert exc_info.value.response["ResponseMetadata"]["HTTPStatusCode"] == 404


def test_create_job(mediaconvert_client):
    resp = mediaconvert_client.create_job(
        Role="arn:aws:iam::000000000000:role/MediaConvert",
        Settings={
            "OutputGroups": [],
            "Inputs": [],
        },
    )
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 201
    job = resp["Job"]
    assert "Id" in job
    assert job["Status"] == "SUBMITTED"


def test_list_jobs(mediaconvert_client):
    mediaconvert_client.create_job(
        Role="arn:aws:iam::000000000000:role/MediaConvert",
        Settings={"OutputGroups": [], "Inputs": []},
    )
    resp = mediaconvert_client.list_jobs()
    assert "Jobs" in resp
    assert len(resp["Jobs"]) >= 1


def test_list_presets(mediaconvert_client):
    resp = mediaconvert_client.list_presets()
    assert "Presets" in resp
    assert isinstance(resp["Presets"], list)


def test_preset_lifecycle(mediaconvert_client):
    resp = mediaconvert_client.create_preset(
        Name="hd-preset",
        Description="Test preset",
        Settings={"VideoDescription": {}},
    )
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 201
    pr = resp["Preset"]
    assert pr["Name"] == "hd-preset"

    got = mediaconvert_client.get_preset(Name="hd-preset")
    assert got["Preset"]["Name"] == "hd-preset"

    mediaconvert_client.delete_preset(Name="hd-preset")

    with pytest.raises(ClientError):
        mediaconvert_client.get_preset(Name="hd-preset")


def test_describe_endpoints(mediaconvert_client):
    resp = mediaconvert_client.describe_endpoints()
    assert "Endpoints" in resp
    assert len(resp["Endpoints"]) >= 1


def test_cancel_job(mediaconvert_client):
    job = mediaconvert_client.create_job(
        Role="arn:aws:iam::000000000000:role/MediaConvert",
        Settings={"OutputGroups": [], "Inputs": []},
    )["Job"]
    mediaconvert_client.cancel_job(Id=job["Id"])


def test_update_queue(mediaconvert_client):
    mediaconvert_client.create_queue(Name="upd-queue", Description="original")
    resp = mediaconvert_client.update_queue(
        Name="upd-queue",
        Description="updated",
    )
    assert resp["Queue"]["Description"] == "updated"


def test_put_and_get_policy(mediaconvert_client):
    mediaconvert_client.put_policy(Policy={"HttpInputs": "ALLOWED"})
    resp = mediaconvert_client.get_policy()
    assert "Policy" in resp
    mediaconvert_client.delete_policy()
