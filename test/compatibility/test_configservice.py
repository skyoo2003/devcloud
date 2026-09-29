def test_put_config_rule(configservice_client):
    resp = configservice_client.put_config_rule(
        ConfigRule={
            "ConfigRuleName": "compat-rule",
            "Source": {
                "Owner": "AWS",
                "SourceIdentifier": "S3_BUCKET_VERSIONING_ENABLED",
            },
        }
    )
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


def test_describe_config_rules(configservice_client):
    configservice_client.put_config_rule(
        ConfigRule={
            "ConfigRuleName": "desc-rule",
            "Source": {
                "Owner": "AWS",
                "SourceIdentifier": "S3_BUCKET_VERSIONING_ENABLED",
            },
        }
    )
    resp = configservice_client.describe_config_rules(ConfigRuleNames=["desc-rule"])
    names = [r["ConfigRuleName"] for r in resp["ConfigRules"]]
    assert "desc-rule" in names
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
