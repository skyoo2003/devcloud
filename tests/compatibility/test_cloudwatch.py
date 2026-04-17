import json


def test_put_metric_data(cloudwatch_client):
    cloudwatch_client.put_metric_data(
        Namespace="MyApp",
        MetricData=[{"MetricName": "Requests", "Value": 10.0, "Unit": "Count"}],
    )


def test_list_metrics(cloudwatch_client):
    cloudwatch_client.put_metric_data(
        Namespace="ListNS",
        MetricData=[{"MetricName": "Errors", "Value": 1.0, "Unit": "Count"}],
    )
    resp = cloudwatch_client.list_metrics(Namespace="ListNS")
    names = [m["MetricName"] for m in resp["Metrics"]]
    assert "Errors" in names


def test_put_and_describe_alarm(cloudwatch_client):
    cloudwatch_client.put_metric_alarm(
        AlarmName="CompatAlarm",
        Namespace="MyApp",
        MetricName="CPUUtilization",
        Statistic="Average",
        Period=60,
        EvaluationPeriods=1,
        Threshold=80.0,
        ComparisonOperator="GreaterThanThreshold",
    )
    resp = cloudwatch_client.describe_alarms(AlarmNames=["CompatAlarm"])
    assert len(resp["MetricAlarms"]) == 1
    assert resp["MetricAlarms"][0]["AlarmName"] == "CompatAlarm"


def test_set_alarm_state(cloudwatch_client):
    cloudwatch_client.put_metric_alarm(
        AlarmName="StateCompatAlarm",
        Namespace="MyApp",
        MetricName="CPU",
        Statistic="Average",
        Period=60,
        EvaluationPeriods=1,
        Threshold=50.0,
        ComparisonOperator="GreaterThanThreshold",
    )
    cloudwatch_client.set_alarm_state(
        AlarmName="StateCompatAlarm",
        StateValue="ALARM",
        StateReason="Testing",
    )
    resp = cloudwatch_client.describe_alarms(AlarmNames=["StateCompatAlarm"])
    assert resp["MetricAlarms"][0]["StateValue"] == "ALARM"


def test_delete_alarms(cloudwatch_client):
    cloudwatch_client.put_metric_alarm(
        AlarmName="DelAlarm",
        Namespace="MyApp",
        MetricName="CPU",
        Statistic="Average",
        Period=60,
        EvaluationPeriods=1,
        Threshold=90.0,
        ComparisonOperator="GreaterThanThreshold",
    )
    cloudwatch_client.delete_alarms(AlarmNames=["DelAlarm"])
    resp = cloudwatch_client.describe_alarms(AlarmNames=["DelAlarm"])
    assert len(resp["MetricAlarms"]) == 0


def test_disable_alarm_actions(cloudwatch_client):
    cloudwatch_client.put_metric_alarm(
        AlarmName="DisableActionsAlarm",
        Namespace="MyApp",
        MetricName="CPU",
        Statistic="Average",
        Period=60,
        EvaluationPeriods=1,
        Threshold=50.0,
        ComparisonOperator="GreaterThanThreshold",
    )
    cloudwatch_client.disable_alarm_actions(AlarmNames=["DisableActionsAlarm"])
    resp = cloudwatch_client.describe_alarms(AlarmNames=["DisableActionsAlarm"])
    assert resp["MetricAlarms"][0]["ActionsEnabled"] is False


def test_get_metric_statistics(cloudwatch_client):
    from datetime import datetime, timedelta, timezone

    cloudwatch_client.put_metric_data(
        Namespace="StatsNS",
        MetricData=[{"MetricName": "Latency", "Value": 100.0, "Unit": "Milliseconds"}],
    )
    now = datetime.now(timezone.utc)
    resp = cloudwatch_client.get_metric_statistics(
        Namespace="StatsNS",
        MetricName="Latency",
        StartTime=now - timedelta(hours=1),
        EndTime=now + timedelta(hours=1),
        Period=3600,
        Statistics=["Average"],
    )
    assert "Datapoints" in resp


def test_delete_nonexistent_alarm(cloudwatch_client):
    # delete_alarms does not raise for non-existent alarms in AWS
    cloudwatch_client.delete_alarms(AlarmNames=["nonexistent-alarm-xyz"])


def test_dashboard_lifecycle(cloudwatch_client):
    body = json.dumps({"widgets": []})
    cloudwatch_client.put_dashboard(DashboardName="my-dash", DashboardBody=body)
    resp = cloudwatch_client.get_dashboard(DashboardName="my-dash")
    assert resp["DashboardBody"]
    listing = cloudwatch_client.list_dashboards()
    assert any(d["DashboardName"] == "my-dash" for d in listing["DashboardEntries"])
    cloudwatch_client.delete_dashboards(DashboardNames=["my-dash"])


def test_composite_alarm(cloudwatch_client):
    cloudwatch_client.put_metric_alarm(
        AlarmName="child-alarm",
        MetricName="CPU",
        Namespace="AWS/EC2",
        Statistic="Average",
        Period=60,
        EvaluationPeriods=1,
        Threshold=80,
        ComparisonOperator="GreaterThanThreshold",
    )
    cloudwatch_client.put_composite_alarm(
        AlarmName="comp-alarm",
        AlarmRule="ALARM(child-alarm)",
        ActionsEnabled=True,
    )
    resp = cloudwatch_client.describe_alarms(AlarmTypes=["CompositeAlarm"])
    assert any(a["AlarmName"] == "comp-alarm" for a in resp.get("CompositeAlarms", []))


def test_alarm_history(cloudwatch_client):
    cloudwatch_client.put_metric_alarm(
        AlarmName="hist-alarm",
        MetricName="M",
        Namespace="N",
        Statistic="Sum",
        Period=60,
        EvaluationPeriods=1,
        Threshold=1,
        ComparisonOperator="GreaterThanThreshold",
    )
    cloudwatch_client.set_alarm_state(
        AlarmName="hist-alarm",
        StateValue="ALARM",
        StateReason="test",
    )
    resp = cloudwatch_client.describe_alarm_history(AlarmName="hist-alarm")
    assert "AlarmHistoryItems" in resp


def test_describe_alarms_for_metric(cloudwatch_client):
    cloudwatch_client.put_metric_alarm(
        AlarmName="m-alarm",
        MetricName="SpecialMetric",
        Namespace="MyApp",
        Statistic="Sum",
        Period=60,
        EvaluationPeriods=1,
        Threshold=1,
        ComparisonOperator="GreaterThanThreshold",
    )
    resp = cloudwatch_client.describe_alarms_for_metric(
        MetricName="SpecialMetric",
        Namespace="MyApp",
    )
    assert any(a["AlarmName"] == "m-alarm" for a in resp["MetricAlarms"])


def test_anomaly_detector(cloudwatch_client):
    cloudwatch_client.put_anomaly_detector(
        Namespace="MyApp",
        MetricName="Req",
        Stat="Average",
    )
    resp = cloudwatch_client.describe_anomaly_detectors(Namespace="MyApp")
    assert len(resp["AnomalyDetectors"]) >= 1
    cloudwatch_client.delete_anomaly_detector(
        Namespace="MyApp",
        MetricName="Req",
        Stat="Average",
    )
