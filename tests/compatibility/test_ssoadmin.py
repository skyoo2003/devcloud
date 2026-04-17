def test_list_instances(ssoadmin_client):
    resp = ssoadmin_client.list_instances()
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
    assert "Instances" in resp


def test_create_and_describe_instance(ssoadmin_client):
    resp = ssoadmin_client.create_instance(Name="test-instance")
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
    instance_arn = resp["InstanceArn"]
    assert instance_arn

    desc = ssoadmin_client.describe_instance(InstanceArn=instance_arn)
    assert desc["ResponseMetadata"]["HTTPStatusCode"] == 200
    assert desc["Name"] == "test-instance"

    ssoadmin_client.delete_instance(InstanceArn=instance_arn)


def test_create_permission_set(ssoadmin_client):
    inst_resp = ssoadmin_client.create_instance(Name="ps-instance")
    instance_arn = inst_resp["InstanceArn"]

    ps_resp = ssoadmin_client.create_permission_set(
        Name="test-ps",
        InstanceArn=instance_arn,
    )
    assert ps_resp["ResponseMetadata"]["HTTPStatusCode"] == 200
    ps = ps_resp["PermissionSet"]
    assert ps["Name"] == "test-ps"
    ps_arn = ps["PermissionSetArn"]

    ps_list = ssoadmin_client.list_permission_sets(InstanceArn=instance_arn)
    assert ps_list["ResponseMetadata"]["HTTPStatusCode"] == 200
    assert ps_arn in ps_list["PermissionSets"]

    ssoadmin_client.delete_permission_set(
        InstanceArn=instance_arn, PermissionSetArn=ps_arn
    )
    ssoadmin_client.delete_instance(InstanceArn=instance_arn)
