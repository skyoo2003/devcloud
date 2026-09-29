def test_create_and_describe_domain(cloudsearch_client):
    resp = cloudsearch_client.create_domain(DomainName="compat-search")
    assert resp["DomainStatus"]["DomainName"] == "compat-search"

    desc = cloudsearch_client.describe_domains(DomainNames=["compat-search"])
    assert len(desc["DomainStatusList"]) == 1


def test_list_domain_names(cloudsearch_client):
    cloudsearch_client.create_domain(DomainName="list-search")
    resp = cloudsearch_client.list_domain_names()
    assert "list-search" in resp["DomainNames"]


def test_delete_domain(cloudsearch_client):
    cloudsearch_client.create_domain(DomainName="del-search")
    cloudsearch_client.delete_domain(DomainName="del-search")
    resp = cloudsearch_client.list_domain_names()
    assert "del-search" not in resp.get("DomainNames", {})


def test_define_index_field(cloudsearch_client):
    cloudsearch_client.create_domain(DomainName="idx-search")
    resp = cloudsearch_client.define_index_field(
        DomainName="idx-search",
        IndexField={"IndexFieldName": "title", "IndexFieldType": "text"},
    )
    assert resp["IndexField"]["Options"]["IndexFieldName"] == "title"
