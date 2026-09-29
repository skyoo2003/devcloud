def test_register_and_describe_domain(swf_client):
    swf_client.register_domain(
        name="compat-domain",
        workflowExecutionRetentionPeriodInDays="30",
    )

    resp = swf_client.describe_domain(name="compat-domain")
    assert resp["domainInfo"]["name"] == "compat-domain"
    assert resp["domainInfo"]["status"] == "REGISTERED"


def test_list_domains(swf_client):
    swf_client.register_domain(
        name="list-domain-1",
        workflowExecutionRetentionPeriodInDays="30",
    )
    resp = swf_client.list_domains(registrationStatus="REGISTERED")
    names = [d["name"] for d in resp["domainInfos"]]
    assert "list-domain-1" in names


def test_deprecate_domain(swf_client):
    swf_client.register_domain(
        name="dep-domain",
        workflowExecutionRetentionPeriodInDays="30",
    )
    swf_client.deprecate_domain(name="dep-domain")

    resp = swf_client.describe_domain(name="dep-domain")
    assert resp["domainInfo"]["status"] == "DEPRECATED"
