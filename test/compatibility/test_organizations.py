from botocore.exceptions import ClientError


def test_create_organization(organizations_client):
    resp = organizations_client.create_organization(FeatureSet="ALL")
    assert resp["Organization"]
    assert resp["Organization"]["Id"]
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


def test_list_accounts(organizations_client):
    # Ensure an organization exists first
    try:
        organizations_client.create_organization(FeatureSet="ALL")
    except ClientError:
        pass  # may already exist
    resp = organizations_client.list_accounts()
    assert "Accounts" in resp
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
