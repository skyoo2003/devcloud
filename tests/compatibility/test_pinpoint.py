import pytest
from botocore.exceptions import ClientError


def test_create_and_get_app(pinpoint_client):
    resp = pinpoint_client.create_app(CreateApplicationRequest={"Name": "compat-app"})
    app_id = resp["ApplicationResponse"]["Id"]
    assert resp["ApplicationResponse"]["Name"] == "compat-app"

    get = pinpoint_client.get_app(ApplicationId=app_id)
    assert get["ApplicationResponse"]["Name"] == "compat-app"


def test_get_apps(pinpoint_client):
    pinpoint_client.create_app(CreateApplicationRequest={"Name": "list-app"})
    resp = pinpoint_client.get_apps()
    names = [a["Name"] for a in resp["ApplicationsResponse"]["Item"]]
    assert "list-app" in names


def test_delete_app(pinpoint_client):
    resp = pinpoint_client.create_app(CreateApplicationRequest={"Name": "del-app"})
    app_id = resp["ApplicationResponse"]["Id"]
    pinpoint_client.delete_app(ApplicationId=app_id)


def test_create_and_get_segment(pinpoint_client):
    app = pinpoint_client.create_app(CreateApplicationRequest={"Name": "seg-app"})
    app_id = app["ApplicationResponse"]["Id"]
    resp = pinpoint_client.create_segment(
        ApplicationId=app_id,
        WriteSegmentRequest={
            "Name": "compat-segment",
            "SegmentGroups": {"Groups": [], "Include": "ALL"},
        },
    )
    seg_id = resp["SegmentResponse"]["Id"]
    assert resp["SegmentResponse"]["Name"] == "compat-segment"

    get = pinpoint_client.get_segment(ApplicationId=app_id, SegmentId=seg_id)
    assert get["SegmentResponse"]["Name"] == "compat-segment"


def test_create_and_get_campaign(pinpoint_client):
    app = pinpoint_client.create_app(CreateApplicationRequest={"Name": "camp-app"})
    app_id = app["ApplicationResponse"]["Id"]
    seg = pinpoint_client.create_segment(
        ApplicationId=app_id,
        WriteSegmentRequest={
            "Name": "camp-segment",
            "SegmentGroups": {"Groups": [], "Include": "ALL"},
        },
    )
    seg_id = seg["SegmentResponse"]["Id"]

    resp = pinpoint_client.create_campaign(
        ApplicationId=app_id,
        WriteCampaignRequest={
            "Name": "compat-campaign",
            "SegmentId": seg_id,
            "MessageConfiguration": {"DefaultMessage": {"Body": "Hello!"}},
            "Schedule": {"StartTime": "IMMEDIATE"},
        },
    )
    campaign_id = resp["CampaignResponse"]["Id"]
    assert resp["CampaignResponse"]["Name"] == "compat-campaign"

    get = pinpoint_client.get_campaign(ApplicationId=app_id, CampaignId=campaign_id)
    assert get["CampaignResponse"]["Name"] == "compat-campaign"


def test_get_nonexistent_app(pinpoint_client):
    with pytest.raises(ClientError) as exc:
        pinpoint_client.get_app(ApplicationId="nonexistent-app-id")
    assert exc.value.response["Error"]["Code"] == "NotFoundException"
