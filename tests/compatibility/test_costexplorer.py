def test_get_cost_and_usage(costexplorer_client):
    resp = costexplorer_client.get_cost_and_usage(
        TimePeriod={"Start": "2024-01-01", "End": "2024-02-01"},
        Granularity="MONTHLY",
        Metrics=["BlendedCost"],
    )
    assert "ResultsByTime" in resp


def test_get_cost_forecast(costexplorer_client):
    resp = costexplorer_client.get_cost_forecast(
        TimePeriod={"Start": "2024-06-01", "End": "2024-07-01"},
        Metric="BLENDED_COST",
        Granularity="MONTHLY",
    )
    assert "Total" in resp
    assert "ForecastResultsByTime" in resp


def test_get_dimension_values(costexplorer_client):
    resp = costexplorer_client.get_dimension_values(
        TimePeriod={"Start": "2024-01-01", "End": "2024-02-01"},
        Dimension="SERVICE",
    )
    assert "DimensionValues" in resp
    assert "TotalSize" in resp
