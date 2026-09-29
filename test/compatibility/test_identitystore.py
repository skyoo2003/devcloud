import pytest
from botocore.exceptions import ClientError

IDENTITY_STORE_ID = "d-1234567890"


def test_create_and_describe_user(identitystore_client):
    resp = identitystore_client.create_user(
        IdentityStoreId=IDENTITY_STORE_ID,
        UserName="alice",
        DisplayName="Alice Smith",
        Emails=[{"Value": "alice@example.com", "Type": "work", "Primary": True}],
    )
    assert "UserId" in resp
    user_id = resp["UserId"]

    desc = identitystore_client.describe_user(
        IdentityStoreId=IDENTITY_STORE_ID,
        UserId=user_id,
    )
    assert desc["UserName"] == "alice"
    assert desc["DisplayName"] == "Alice Smith"


def test_list_users(identitystore_client):
    identitystore_client.create_user(
        IdentityStoreId=IDENTITY_STORE_ID,
        UserName="list-user",
        DisplayName="List User",
    )
    resp = identitystore_client.list_users(IdentityStoreId=IDENTITY_STORE_ID)
    assert "Users" in resp
    usernames = [u["UserName"] for u in resp["Users"]]
    assert "list-user" in usernames


def test_delete_user(identitystore_client):
    resp = identitystore_client.create_user(
        IdentityStoreId=IDENTITY_STORE_ID,
        UserName="del-user",
        DisplayName="Del User",
    )
    user_id = resp["UserId"]

    identitystore_client.delete_user(
        IdentityStoreId=IDENTITY_STORE_ID,
        UserId=user_id,
    )

    with pytest.raises(ClientError):
        identitystore_client.describe_user(
            IdentityStoreId=IDENTITY_STORE_ID,
            UserId=user_id,
        )


def test_create_and_describe_group(identitystore_client):
    resp = identitystore_client.create_group(
        IdentityStoreId=IDENTITY_STORE_ID,
        DisplayName="Admins",
        Description="Administrator group",
    )
    assert "GroupId" in resp
    group_id = resp["GroupId"]

    desc = identitystore_client.describe_group(
        IdentityStoreId=IDENTITY_STORE_ID,
        GroupId=group_id,
    )
    assert desc["DisplayName"] == "Admins"
    assert desc["Description"] == "Administrator group"


def test_group_membership(identitystore_client):
    user_resp = identitystore_client.create_user(
        IdentityStoreId=IDENTITY_STORE_ID,
        UserName="member-user",
    )
    user_id = user_resp["UserId"]

    group_resp = identitystore_client.create_group(
        IdentityStoreId=IDENTITY_STORE_ID,
        DisplayName="MemberGroup",
    )
    group_id = group_resp["GroupId"]

    mem_resp = identitystore_client.create_group_membership(
        IdentityStoreId=IDENTITY_STORE_ID,
        GroupId=group_id,
        MemberId={"UserId": user_id},
    )
    assert "MembershipId" in mem_resp

    list_resp = identitystore_client.list_group_memberships(
        IdentityStoreId=IDENTITY_STORE_ID,
        GroupId=group_id,
    )
    assert len(list_resp["GroupMemberships"]) >= 1
    member_ids = [m["MemberId"]["UserId"] for m in list_resp["GroupMemberships"]]
    assert user_id in member_ids


def test_get_user_id_by_username(identitystore_client):
    resp = identitystore_client.create_user(
        IdentityStoreId=IDENTITY_STORE_ID,
        UserName="lookup-user",
    )
    user_id = resp["UserId"]
    got = identitystore_client.get_user_id(
        IdentityStoreId=IDENTITY_STORE_ID,
        AlternateIdentifier={
            "UniqueAttribute": {
                "AttributePath": "UserName",
                "AttributeValue": "lookup-user",
            }
        },
    )
    assert got["UserId"] == user_id


def test_update_user_display_name(identitystore_client):
    resp = identitystore_client.create_user(
        IdentityStoreId=IDENTITY_STORE_ID,
        UserName="update-user",
        DisplayName="Old",
    )
    user_id = resp["UserId"]
    identitystore_client.update_user(
        IdentityStoreId=IDENTITY_STORE_ID,
        UserId=user_id,
        Operations=[{"AttributePath": "DisplayName", "AttributeValue": "New"}],
    )
    desc = identitystore_client.describe_user(
        IdentityStoreId=IDENTITY_STORE_ID,
        UserId=user_id,
    )
    assert desc["DisplayName"] == "New"


def test_is_member_in_groups(identitystore_client):
    u = identitystore_client.create_user(
        IdentityStoreId=IDENTITY_STORE_ID, UserName="memcheck-user"
    )
    g = identitystore_client.create_group(
        IdentityStoreId=IDENTITY_STORE_ID, DisplayName="MemCheckGroup"
    )
    identitystore_client.create_group_membership(
        IdentityStoreId=IDENTITY_STORE_ID,
        GroupId=g["GroupId"],
        MemberId={"UserId": u["UserId"]},
    )
    resp = identitystore_client.is_member_in_groups(
        IdentityStoreId=IDENTITY_STORE_ID,
        MemberId={"UserId": u["UserId"]},
        GroupIds=[g["GroupId"]],
    )
    assert resp["Results"][0]["MembershipExists"] is True


def test_list_group_memberships_for_member(identitystore_client):
    u = identitystore_client.create_user(
        IdentityStoreId=IDENTITY_STORE_ID, UserName="member-lookup"
    )
    g = identitystore_client.create_group(
        IdentityStoreId=IDENTITY_STORE_ID, DisplayName="LookupGroup"
    )
    identitystore_client.create_group_membership(
        IdentityStoreId=IDENTITY_STORE_ID,
        GroupId=g["GroupId"],
        MemberId={"UserId": u["UserId"]},
    )
    resp = identitystore_client.list_group_memberships_for_member(
        IdentityStoreId=IDENTITY_STORE_ID,
        MemberId={"UserId": u["UserId"]},
    )
    assert len(resp["GroupMemberships"]) >= 1
