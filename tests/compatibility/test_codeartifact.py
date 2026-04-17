def test_create_domain(codeartifact_client):
    resp = codeartifact_client.create_domain(domain="compat-domain")
    assert resp["domain"]["name"] == "compat-domain"
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


def test_describe_domain(codeartifact_client):
    codeartifact_client.create_domain(domain="desc-domain")
    resp = codeartifact_client.describe_domain(domain="desc-domain")
    assert resp["domain"]["name"] == "desc-domain"
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


def test_list_domains(codeartifact_client):
    codeartifact_client.create_domain(domain="list-domain")
    resp = codeartifact_client.list_domains()
    names = [d["name"] for d in resp["domains"]]
    assert "list-domain" in names
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


def test_delete_domain(codeartifact_client):
    codeartifact_client.create_domain(domain="del-domain")
    resp = codeartifact_client.delete_domain(domain="del-domain")
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
    list_resp = codeartifact_client.list_domains()
    names = [d["name"] for d in list_resp["domains"]]
    assert "del-domain" not in names
