def _get_change_token(waf_client):
    return waf_client.get_change_token()["ChangeToken"]


def test_create_and_get_web_acl(waf_client):
    token = _get_change_token(waf_client)
    resp = waf_client.create_web_acl(
        Name="compat-acl",
        MetricName="compatAcl",
        DefaultAction={"Type": "ALLOW"},
        ChangeToken=token,
    )
    acl_id = resp["WebACL"]["WebACLId"]
    assert resp["WebACL"]["Name"] == "compat-acl"

    get = waf_client.get_web_acl(WebACLId=acl_id)
    assert get["WebACL"]["Name"] == "compat-acl"


def test_list_web_acls(waf_client):
    token = _get_change_token(waf_client)
    waf_client.create_web_acl(
        Name="list-acl",
        MetricName="listAcl",
        DefaultAction={"Type": "ALLOW"},
        ChangeToken=token,
    )
    resp = waf_client.list_web_acls(Limit=100)
    names = [a["Name"] for a in resp["WebACLs"]]
    assert "list-acl" in names


def test_create_and_get_ip_set(waf_client):
    token = _get_change_token(waf_client)
    resp = waf_client.create_ip_set(Name="compat-ipset", ChangeToken=token)
    ip_set_id = resp["IPSet"]["IPSetId"]
    assert resp["IPSet"]["Name"] == "compat-ipset"

    get = waf_client.get_ip_set(IPSetId=ip_set_id)
    assert get["IPSet"]["Name"] == "compat-ipset"


def test_create_and_get_rule(waf_client):
    token = _get_change_token(waf_client)
    resp = waf_client.create_rule(
        Name="compat-rule", MetricName="compatRule", ChangeToken=token
    )
    rule_id = resp["Rule"]["RuleId"]
    assert resp["Rule"]["Name"] == "compat-rule"

    get = waf_client.get_rule(RuleId=rule_id)
    assert get["Rule"]["Name"] == "compat-rule"


def test_delete_web_acl(waf_client):
    token = _get_change_token(waf_client)
    resp = waf_client.create_web_acl(
        Name="del-acl",
        MetricName="delAcl",
        DefaultAction={"Type": "BLOCK"},
        ChangeToken=token,
    )
    acl_id = resp["WebACL"]["WebACLId"]
    token2 = _get_change_token(waf_client)
    waf_client.delete_web_acl(WebACLId=acl_id, ChangeToken=token2)
