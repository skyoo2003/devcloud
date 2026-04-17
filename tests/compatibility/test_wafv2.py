def test_create_and_get_web_acl(wafv2_client):
    resp = wafv2_client.create_web_acl(
        Name="test-acl",
        Scope="REGIONAL",
        DefaultAction={"Allow": {}},
        VisibilityConfig={
            "SampledRequestsEnabled": True,
            "CloudWatchMetricsEnabled": True,
            "MetricName": "test-acl-metric",
        },
    )
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
    summary = resp["Summary"]
    acl_id = summary["Id"]
    lock_token = summary["LockToken"]
    assert acl_id

    get_resp = wafv2_client.get_web_acl(Name="test-acl", Scope="REGIONAL", Id=acl_id)
    assert get_resp["ResponseMetadata"]["HTTPStatusCode"] == 200
    assert get_resp["WebACL"]["Name"] == "test-acl"

    wafv2_client.delete_web_acl(
        Name="test-acl", Scope="REGIONAL", Id=acl_id, LockToken=lock_token
    )


def test_list_web_acls(wafv2_client):
    resp = wafv2_client.create_web_acl(
        Name="list-acl",
        Scope="REGIONAL",
        DefaultAction={"Block": {}},
        VisibilityConfig={
            "SampledRequestsEnabled": False,
            "CloudWatchMetricsEnabled": False,
            "MetricName": "list-metric",
        },
    )
    acl_id = resp["Summary"]["Id"]
    lock_token = resp["Summary"]["LockToken"]

    acls = wafv2_client.list_web_acls(Scope="REGIONAL")
    assert acls["ResponseMetadata"]["HTTPStatusCode"] == 200
    ids = [a["Id"] for a in acls["WebACLs"]]
    assert acl_id in ids

    wafv2_client.delete_web_acl(
        Name="list-acl", Scope="REGIONAL", Id=acl_id, LockToken=lock_token
    )


def test_delete_web_acl(wafv2_client):
    resp = wafv2_client.create_web_acl(
        Name="del-acl",
        Scope="REGIONAL",
        DefaultAction={"Allow": {}},
        VisibilityConfig={
            "SampledRequestsEnabled": True,
            "CloudWatchMetricsEnabled": True,
            "MetricName": "del-metric",
        },
    )
    acl_id = resp["Summary"]["Id"]
    lock_token = resp["Summary"]["LockToken"]

    del_resp = wafv2_client.delete_web_acl(
        Name="del-acl", Scope="REGIONAL", Id=acl_id, LockToken=lock_token
    )
    assert del_resp["ResponseMetadata"]["HTTPStatusCode"] == 200


def test_create_and_get_ip_set(wafv2_client):
    resp = wafv2_client.create_ip_set(
        Name="test-ipset",
        Scope="REGIONAL",
        IPAddressVersion="IPV4",
        Addresses=["192.168.1.0/24"],
    )
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
    summary = resp["Summary"]
    ipset_id = summary["Id"]
    lock_token = summary["LockToken"]

    get_resp = wafv2_client.get_ip_set(Name="test-ipset", Scope="REGIONAL", Id=ipset_id)
    assert get_resp["ResponseMetadata"]["HTTPStatusCode"] == 200
    assert get_resp["IPSet"]["Name"] == "test-ipset"

    wafv2_client.delete_ip_set(
        Name="test-ipset", Scope="REGIONAL", Id=ipset_id, LockToken=lock_token
    )
