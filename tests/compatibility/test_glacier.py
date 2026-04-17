def test_create_and_describe_vault(glacier_client):
    resp = glacier_client.create_vault(vaultName="test-vault")
    assert resp["ResponseMetadata"]["HTTPStatusCode"] in (200, 201)

    desc = glacier_client.describe_vault(vaultName="test-vault")
    assert desc["ResponseMetadata"]["HTTPStatusCode"] == 200
    assert desc["VaultName"] == "test-vault"
    assert "VaultARN" in desc

    glacier_client.delete_vault(vaultName="test-vault")


def test_list_vaults(glacier_client):
    glacier_client.create_vault(vaultName="list-vault")

    vaults = glacier_client.list_vaults()
    assert vaults["ResponseMetadata"]["HTTPStatusCode"] == 200
    names = [v["VaultName"] for v in vaults["VaultList"]]
    assert "list-vault" in names

    glacier_client.delete_vault(vaultName="list-vault")


def test_delete_vault(glacier_client):
    glacier_client.create_vault(vaultName="del-vault")

    del_resp = glacier_client.delete_vault(vaultName="del-vault")
    assert del_resp["ResponseMetadata"]["HTTPStatusCode"] in (200, 204)


def test_vault_tags(glacier_client):
    glacier_client.create_vault(vaultName="tag-vault")

    glacier_client.add_tags_to_vault(
        vaultName="tag-vault",
        Tags={"env": "test"},
    )

    tags_resp = glacier_client.list_tags_for_vault(vaultName="tag-vault")
    assert tags_resp["ResponseMetadata"]["HTTPStatusCode"] == 200
    assert tags_resp["Tags"]["env"] == "test"

    glacier_client.remove_tags_from_vault(
        vaultName="tag-vault",
        TagKeys=["env"],
    )

    glacier_client.delete_vault(vaultName="tag-vault")
