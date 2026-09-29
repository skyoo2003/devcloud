import pytest
from botocore.exceptions import ClientError


def test_create_and_describe_domain(opensearch_client):
    resp = opensearch_client.create_domain(
        DomainName="compat-os", EngineVersion="OpenSearch_2.11"
    )
    assert resp["DomainStatus"]["DomainName"] == "compat-os"

    desc = opensearch_client.describe_domain(DomainName="compat-os")
    assert desc["DomainStatus"]["DomainName"] == "compat-os"


def test_list_domain_names(opensearch_client):
    opensearch_client.create_domain(DomainName="list-os")
    resp = opensearch_client.list_domain_names()
    names = [d["DomainName"] for d in resp["DomainNames"]]
    assert "list-os" in names


def test_delete_domain(opensearch_client):
    opensearch_client.create_domain(DomainName="del-os")
    opensearch_client.delete_domain(DomainName="del-os")
    resp = opensearch_client.list_domain_names()
    names = [d["DomainName"] for d in resp["DomainNames"]]
    assert "del-os" not in names


def test_add_and_list_tags(opensearch_client):
    resp = opensearch_client.create_domain(DomainName="tag-os")
    arn = resp["DomainStatus"]["ARN"]
    opensearch_client.add_tags(ARN=arn, TagList=[{"Key": "env", "Value": "test"}])
    tags = opensearch_client.list_tags(ARN=arn)
    tag_map = {t["Key"]: t["Value"] for t in tags["TagList"]}
    assert tag_map.get("env") == "test"


def test_describe_nonexistent_domain(opensearch_client):
    with pytest.raises(ClientError) as exc:
        opensearch_client.describe_domain(DomainName="no-such-os-domain")
    assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"
