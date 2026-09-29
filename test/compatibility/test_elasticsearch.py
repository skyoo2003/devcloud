def test_create_and_describe_domain(elasticsearch_client):
    resp = elasticsearch_client.create_elasticsearch_domain(
        DomainName="test-domain",
        ElasticsearchVersion="7.10",
    )
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
    domain = resp["DomainStatus"]
    assert domain["DomainName"] == "test-domain"
    assert "ARN" in domain

    desc = elasticsearch_client.describe_elasticsearch_domain(DomainName="test-domain")
    assert desc["ResponseMetadata"]["HTTPStatusCode"] == 200
    assert desc["DomainStatus"]["DomainName"] == "test-domain"

    elasticsearch_client.delete_elasticsearch_domain(DomainName="test-domain")


def test_list_domain_names(elasticsearch_client):
    elasticsearch_client.create_elasticsearch_domain(
        DomainName="list-domain",
        ElasticsearchVersion="7.10",
    )

    names = elasticsearch_client.list_domain_names()
    assert names["ResponseMetadata"]["HTTPStatusCode"] == 200
    domain_names = [d["DomainName"] for d in names["DomainNames"]]
    assert "list-domain" in domain_names

    elasticsearch_client.delete_elasticsearch_domain(DomainName="list-domain")


def test_delete_domain(elasticsearch_client):
    elasticsearch_client.create_elasticsearch_domain(
        DomainName="del-domain",
        ElasticsearchVersion="7.10",
    )

    del_resp = elasticsearch_client.delete_elasticsearch_domain(DomainName="del-domain")
    assert del_resp["ResponseMetadata"]["HTTPStatusCode"] == 200


def test_describe_domains_batch(elasticsearch_client):
    elasticsearch_client.create_elasticsearch_domain(
        DomainName="batch-domain",
        ElasticsearchVersion="7.10",
    )

    resp = elasticsearch_client.describe_elasticsearch_domains(
        DomainNames=["batch-domain"]
    )
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
    assert len(resp["DomainStatusList"]) >= 1
    assert resp["DomainStatusList"][0]["DomainName"] == "batch-domain"

    elasticsearch_client.delete_elasticsearch_domain(DomainName="batch-domain")
