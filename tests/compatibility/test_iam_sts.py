import pytest
from botocore.exceptions import ClientError


def test_create_user(iam_client):
    response = iam_client.create_user(UserName="testuser")
    assert response["User"]["UserName"] == "testuser"
    assert "Arn" in response["User"]


def test_list_users(iam_client):
    iam_client.create_user(UserName="listuser1")
    iam_client.create_user(UserName="listuser2")
    response = iam_client.list_users()
    names = [u["UserName"] for u in response["Users"]]
    assert "listuser1" in names
    assert "listuser2" in names


def test_create_role(iam_client):
    response = iam_client.create_role(
        RoleName="testrole",
        AssumeRolePolicyDocument="{}",
    )
    assert response["Role"]["RoleName"] == "testrole"


def test_create_access_key(iam_client):
    iam_client.create_user(UserName="keyuser")
    response = iam_client.create_access_key(UserName="keyuser")
    assert response["AccessKey"]["AccessKeyId"].startswith("AKIA")
    assert "SecretAccessKey" in response["AccessKey"]


def test_get_caller_identity(sts_client):
    response = sts_client.get_caller_identity()
    assert "Account" in response
    assert "Arn" in response


def test_get_access_key_info(sts_client):
    response = sts_client.get_access_key_info(AccessKeyId="AKIAIOSFODNN7EXAMPLE")
    assert "Account" in response


def test_create_duplicate_user(iam_client):
    iam_client.create_user(UserName="dup-user")
    with pytest.raises(ClientError) as exc:
        iam_client.create_user(UserName="dup-user")
    assert exc.value.response["Error"]["Code"] == "EntityAlreadyExists"


def test_delete_nonexistent_user(iam_client):
    with pytest.raises(ClientError) as exc:
        iam_client.delete_user(UserName="ghost-user-xyz")
    assert exc.value.response["Error"]["Code"] == "NoSuchEntity"


def test_create_policy(iam_client):
    resp = iam_client.create_policy(
        PolicyName="test-policy",
        PolicyDocument='{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:*","Resource":"*"}]}',
    )
    assert resp["Policy"]["PolicyName"] == "test-policy"
    assert resp["Policy"]["Arn"].startswith("arn:aws:iam:")


def test_attach_role_policy(iam_client):
    iam_client.create_role(RoleName="attach-role", AssumeRolePolicyDocument="{}")
    policy = iam_client.create_policy(
        PolicyName="attach-policy",
        PolicyDocument='{"Version":"2012-10-17","Statement":[]}',
    )
    arn = policy["Policy"]["Arn"]
    iam_client.attach_role_policy(RoleName="attach-role", PolicyArn=arn)
    resp = iam_client.list_attached_role_policies(RoleName="attach-role")
    arns = [p["PolicyArn"] for p in resp["AttachedPolicies"]]
    assert arn in arns


def test_get_user(iam_client):
    iam_client.create_user(UserName="getuser")
    resp = iam_client.get_user(UserName="getuser")
    assert resp["User"]["UserName"] == "getuser"
    assert "Arn" in resp["User"]


def test_get_user_not_found(iam_client):
    with pytest.raises(ClientError) as exc:
        iam_client.get_user(UserName="nonexistent")
    assert exc.value.response["Error"]["Code"] == "NoSuchEntity"


def test_get_role(iam_client):
    iam_client.create_role(RoleName="getrole", AssumeRolePolicyDocument="{}")
    resp = iam_client.get_role(RoleName="getrole")
    assert resp["Role"]["RoleName"] == "getrole"


def test_delete_role(iam_client):
    iam_client.create_role(RoleName="delrole", AssumeRolePolicyDocument="{}")
    iam_client.delete_role(RoleName="delrole")
    with pytest.raises(ClientError) as exc:
        iam_client.get_role(RoleName="delrole")
    assert exc.value.response["Error"]["Code"] == "NoSuchEntity"


def test_update_assume_role_policy(iam_client):
    iam_client.create_role(RoleName="updpolicy", AssumeRolePolicyDocument="{}")
    iam_client.update_assume_role_policy(
        RoleName="updpolicy",
        PolicyDocument='{"Version":"2012-10-17","Statement":[]}',
    )


def test_get_policy(iam_client):
    resp = iam_client.create_policy(
        PolicyName="getpol", PolicyDocument='{"Version":"2012-10-17","Statement":[]}'
    )
    arn = resp["Policy"]["Arn"]
    resp2 = iam_client.get_policy(PolicyArn=arn)
    assert resp2["Policy"]["PolicyName"] == "getpol"


def test_delete_policy(iam_client):
    resp = iam_client.create_policy(
        PolicyName="delpol", PolicyDocument='{"Version":"2012-10-17","Statement":[]}'
    )
    arn = resp["Policy"]["Arn"]
    iam_client.delete_policy(PolicyArn=arn)
    with pytest.raises(ClientError) as exc:
        iam_client.get_policy(PolicyArn=arn)
    assert exc.value.response["Error"]["Code"] == "NoSuchEntity"


def test_attach_detach_user_policy(iam_client):
    iam_client.create_user(UserName="poluser")
    pol = iam_client.create_policy(
        PolicyName="userpol", PolicyDocument='{"Version":"2012-10-17","Statement":[]}'
    )
    arn = pol["Policy"]["Arn"]
    iam_client.attach_user_policy(UserName="poluser", PolicyArn=arn)
    resp = iam_client.list_attached_user_policies(UserName="poluser")
    assert any(p["PolicyArn"] == arn for p in resp["AttachedPolicies"])
    iam_client.detach_user_policy(UserName="poluser", PolicyArn=arn)


def test_detach_role_policy(iam_client):
    iam_client.create_role(RoleName="detrole", AssumeRolePolicyDocument="{}")
    pol = iam_client.create_policy(
        PolicyName="detpol", PolicyDocument='{"Version":"2012-10-17","Statement":[]}'
    )
    arn = pol["Policy"]["Arn"]
    iam_client.attach_role_policy(RoleName="detrole", PolicyArn=arn)
    iam_client.detach_role_policy(RoleName="detrole", PolicyArn=arn)
    resp = iam_client.list_attached_role_policies(RoleName="detrole")
    assert len(resp["AttachedPolicies"]) == 0


# Task 4: Inline policies


def test_inline_user_policy(iam_client):
    iam_client.create_user(UserName="inlineuser")
    iam_client.put_user_policy(
        UserName="inlineuser", PolicyName="mypol", PolicyDocument="{}"
    )
    resp = iam_client.get_user_policy(UserName="inlineuser", PolicyName="mypol")
    assert resp["PolicyName"] == "mypol"
    names = iam_client.list_user_policies(UserName="inlineuser")["PolicyNames"]
    assert "mypol" in names
    iam_client.delete_user_policy(UserName="inlineuser", PolicyName="mypol")
    names2 = iam_client.list_user_policies(UserName="inlineuser")["PolicyNames"]
    assert "mypol" not in names2


def test_inline_role_policy(iam_client):
    iam_client.create_role(RoleName="inlinerole", AssumeRolePolicyDocument="{}")
    iam_client.put_role_policy(
        RoleName="inlinerole", PolicyName="rolepol", PolicyDocument="{}"
    )
    resp = iam_client.get_role_policy(RoleName="inlinerole", PolicyName="rolepol")
    assert resp["PolicyName"] == "rolepol"
    names = iam_client.list_role_policies(RoleName="inlinerole")["PolicyNames"]
    assert "rolepol" in names
    iam_client.delete_role_policy(RoleName="inlinerole", PolicyName="rolepol")
    names2 = iam_client.list_role_policies(RoleName="inlinerole")["PolicyNames"]
    assert "rolepol" not in names2


# Task 5: Groups


def test_groups(iam_client):
    iam_client.create_group(GroupName="mygroup")
    iam_client.create_user(UserName="grpuser")
    iam_client.add_user_to_group(GroupName="mygroup", UserName="grpuser")
    resp = iam_client.get_group(GroupName="mygroup")
    assert resp["Group"]["GroupName"] == "mygroup"
    assert any(u["UserName"] == "grpuser" for u in resp["Users"])
    iam_client.remove_user_from_group(GroupName="mygroup", UserName="grpuser")
    resp2 = iam_client.get_group(GroupName="mygroup")
    assert not any(u["UserName"] == "grpuser" for u in resp2["Users"])
    iam_client.delete_group(GroupName="mygroup")


def test_list_groups(iam_client):
    iam_client.create_group(GroupName="list-grp-a")
    iam_client.create_group(GroupName="list-grp-b")
    resp = iam_client.list_groups()
    names = [g["GroupName"] for g in resp["Groups"]]
    assert "list-grp-a" in names
    assert "list-grp-b" in names


# Task 6: Instance profiles


def test_instance_profiles(iam_client):
    iam_client.create_instance_profile(InstanceProfileName="myip")
    iam_client.create_role(RoleName="iprole", AssumeRolePolicyDocument="{}")
    iam_client.add_role_to_instance_profile(
        InstanceProfileName="myip", RoleName="iprole"
    )
    resp = iam_client.get_instance_profile(InstanceProfileName="myip")
    assert resp["InstanceProfile"]["InstanceProfileName"] == "myip"
    assert any(r["RoleName"] == "iprole" for r in resp["InstanceProfile"]["Roles"])
    iam_client.remove_role_from_instance_profile(
        InstanceProfileName="myip", RoleName="iprole"
    )
    resp2 = iam_client.get_instance_profile(InstanceProfileName="myip")
    assert not any(r["RoleName"] == "iprole" for r in resp2["InstanceProfile"]["Roles"])
    iam_client.delete_instance_profile(InstanceProfileName="myip")


def test_list_instance_profiles(iam_client):
    iam_client.create_instance_profile(InstanceProfileName="list-ip-a")
    iam_client.create_instance_profile(InstanceProfileName="list-ip-b")
    resp = iam_client.list_instance_profiles()
    names = [ip["InstanceProfileName"] for ip in resp["InstanceProfiles"]]
    assert "list-ip-a" in names
    assert "list-ip-b" in names


# Task 7: Access keys & tagging


def test_list_access_keys(iam_client):
    iam_client.create_user(UserName="akuser")
    iam_client.create_access_key(UserName="akuser")
    resp = iam_client.list_access_keys(UserName="akuser")
    assert len(resp["AccessKeyMetadata"]) >= 1
    key_id = resp["AccessKeyMetadata"][0]["AccessKeyId"]
    iam_client.update_access_key(
        UserName="akuser", AccessKeyId=key_id, Status="Inactive"
    )
    resp2 = iam_client.list_access_keys(UserName="akuser")
    statuses = [k["Status"] for k in resp2["AccessKeyMetadata"]]
    assert "Inactive" in statuses
    iam_client.delete_access_key(UserName="akuser", AccessKeyId=key_id)
    resp3 = iam_client.list_access_keys(UserName="akuser")
    assert not any(k["AccessKeyId"] == key_id for k in resp3["AccessKeyMetadata"])


def test_user_tags(iam_client):
    iam_client.create_user(UserName="taguser")
    iam_client.tag_user(UserName="taguser", Tags=[{"Key": "env", "Value": "prod"}])
    resp = iam_client.list_user_tags(UserName="taguser")
    assert any(t["Key"] == "env" and t["Value"] == "prod" for t in resp["Tags"])
    iam_client.untag_user(UserName="taguser", TagKeys=["env"])
    resp2 = iam_client.list_user_tags(UserName="taguser")
    assert not any(t["Key"] == "env" for t in resp2["Tags"])


def test_role_tags(iam_client):
    iam_client.create_role(RoleName="tagrole", AssumeRolePolicyDocument="{}")
    iam_client.tag_role(
        RoleName="tagrole", Tags=[{"Key": "purpose", "Value": "worker"}]
    )
    resp = iam_client.list_role_tags(RoleName="tagrole")
    assert any(t["Key"] == "purpose" and t["Value"] == "worker" for t in resp["Tags"])
    iam_client.untag_role(RoleName="tagrole", TagKeys=["purpose"])
    resp2 = iam_client.list_role_tags(RoleName="tagrole")
    assert not any(t["Key"] == "purpose" for t in resp2["Tags"])
