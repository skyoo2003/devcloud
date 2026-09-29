import pytest
from botocore.exceptions import ClientError


def test_create_and_describe_application(kinesisanalyticsv2_client):
    resp = kinesisanalyticsv2_client.create_application(
        ApplicationName="compat-kda-app",
        RuntimeEnvironment="FLINK-1_18",
        ServiceExecutionRole="arn:aws:iam::123456789012:role/test",
    )
    assert resp["ApplicationDetail"]["ApplicationName"] == "compat-kda-app"

    desc = kinesisanalyticsv2_client.describe_application(
        ApplicationName="compat-kda-app"
    )
    assert desc["ApplicationDetail"]["ApplicationName"] == "compat-kda-app"


def test_list_applications(kinesisanalyticsv2_client):
    kinesisanalyticsv2_client.create_application(
        ApplicationName="list-kda-app",
        RuntimeEnvironment="FLINK-1_18",
        ServiceExecutionRole="arn:aws:iam::123456789012:role/test",
    )
    resp = kinesisanalyticsv2_client.list_applications()
    names = [a["ApplicationName"] for a in resp["ApplicationSummaries"]]
    assert "list-kda-app" in names


def test_delete_application(kinesisanalyticsv2_client):
    kinesisanalyticsv2_client.create_application(
        ApplicationName="del-kda-app",
        RuntimeEnvironment="FLINK-1_18",
        ServiceExecutionRole="arn:aws:iam::123456789012:role/test",
    )
    desc = kinesisanalyticsv2_client.describe_application(ApplicationName="del-kda-app")
    create_ts = desc["ApplicationDetail"]["CreateTimestamp"]

    kinesisanalyticsv2_client.delete_application(
        ApplicationName="del-kda-app",
        CreateTimestamp=create_ts,
    )

    with pytest.raises(ClientError):
        kinesisanalyticsv2_client.describe_application(ApplicationName="del-kda-app")


def test_start_and_stop_application(kinesisanalyticsv2_client):
    kinesisanalyticsv2_client.create_application(
        ApplicationName="startstop-kda-app",
        RuntimeEnvironment="FLINK-1_18",
        ServiceExecutionRole="arn:aws:iam::123456789012:role/test",
    )
    kinesisanalyticsv2_client.start_application(ApplicationName="startstop-kda-app")

    desc = kinesisanalyticsv2_client.describe_application(
        ApplicationName="startstop-kda-app"
    )
    assert desc["ApplicationDetail"]["ApplicationStatus"] == "RUNNING"

    kinesisanalyticsv2_client.stop_application(ApplicationName="startstop-kda-app")
    desc = kinesisanalyticsv2_client.describe_application(
        ApplicationName="startstop-kda-app"
    )
    assert desc["ApplicationDetail"]["ApplicationStatus"] == "READY"
