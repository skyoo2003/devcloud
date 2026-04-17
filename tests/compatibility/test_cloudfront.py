def _distribution_config(caller_ref="test-ref"):
    return {
        "CallerReference": caller_ref,
        "Comment": "test distribution",
        "DefaultCacheBehavior": {
            "TargetOriginId": "myS3Origin",
            "ViewerProtocolPolicy": "allow-all",
            "ForwardedValues": {
                "QueryString": False,
                "Cookies": {"Forward": "none"},
            },
            "MinTTL": 0,
        },
        "Origins": {
            "Quantity": 1,
            "Items": [
                {
                    "Id": "myS3Origin",
                    "DomainName": "mybucket.s3.amazonaws.com",
                    "S3OriginConfig": {
                        "OriginAccessIdentity": "",
                    },
                }
            ],
        },
        "Enabled": True,
    }


def test_create_and_get_distribution(cloudfront_client):
    resp = cloudfront_client.create_distribution(
        DistributionConfig=_distribution_config("create-ref"),
    )
    assert resp["ResponseMetadata"]["HTTPStatusCode"] in (200, 201)
    dist = resp["Distribution"]
    dist_id = dist["Id"]
    assert dist_id

    get_resp = cloudfront_client.get_distribution(Id=dist_id)
    assert get_resp["ResponseMetadata"]["HTTPStatusCode"] == 200
    assert get_resp["Distribution"]["Id"] == dist_id


def test_list_distributions(cloudfront_client):
    resp = cloudfront_client.create_distribution(
        DistributionConfig=_distribution_config("list-ref"),
    )
    dist_id = resp["Distribution"]["Id"]

    list_resp = cloudfront_client.list_distributions()
    assert list_resp["ResponseMetadata"]["HTTPStatusCode"] == 200
    items = list_resp.get("DistributionList", {}).get("Items", [])
    ids = [d["Id"] for d in items] if items else []
    assert dist_id in ids
