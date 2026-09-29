# ── CreateFileSystem / DescribeFileSystems / DeleteFileSystem ──────────────


def test_create_describe_delete_file_system(efs_client):
    resp = efs_client.create_file_system(CreationToken="test-token-1")
    assert resp["ResponseMetadata"]["HTTPStatusCode"] in (200, 201)
    fs_id = resp["FileSystemId"]
    assert fs_id.startswith("fs-")
    assert resp["LifeCycleState"] == "available"

    # Idempotent: same token returns same FS
    resp2 = efs_client.create_file_system(CreationToken="test-token-1")
    assert resp2["FileSystemId"] == fs_id

    # Describe all
    resp3 = efs_client.describe_file_systems()
    assert resp3["ResponseMetadata"]["HTTPStatusCode"] == 200
    found = any(f["FileSystemId"] == fs_id for f in resp3["FileSystems"])
    assert found

    # Delete
    del_resp = efs_client.delete_file_system(FileSystemId=fs_id)
    assert del_resp["ResponseMetadata"]["HTTPStatusCode"] == 204


def test_describe_file_systems_empty(efs_client):
    resp = efs_client.describe_file_systems()
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
    assert "FileSystems" in resp


# ── Backup Policy ─────────────────────────────────────────────────────────


def test_backup_policy(efs_client):
    fs = efs_client.create_file_system(CreationToken="bp-test-1")
    fs_id = fs["FileSystemId"]

    efs_client.put_backup_policy(
        FileSystemId=fs_id,
        BackupPolicy={"Status": "ENABLED"},
    )

    resp = efs_client.describe_backup_policy(FileSystemId=fs_id)
    assert resp["BackupPolicy"]["Status"] == "ENABLED"

    efs_client.delete_file_system(FileSystemId=fs_id)


# ── Lifecycle Configuration ───────────────────────────────────────────────


def test_lifecycle_configuration(efs_client):
    fs = efs_client.create_file_system(CreationToken="lc-test-1")
    fs_id = fs["FileSystemId"]

    efs_client.put_lifecycle_configuration(
        FileSystemId=fs_id,
        LifecyclePolicies=[{"TransitionToIA": "AFTER_30_DAYS"}],
    )

    resp = efs_client.describe_lifecycle_configuration(FileSystemId=fs_id)
    assert len(resp["LifecyclePolicies"]) == 1

    efs_client.delete_file_system(FileSystemId=fs_id)


# ── File System Policy ────────────────────────────────────────────────────


def test_file_system_policy(efs_client):
    import json

    fs = efs_client.create_file_system(CreationToken="pol-test-1")
    fs_id = fs["FileSystemId"]

    policy = json.dumps({"Version": "2012-10-17", "Statement": []})
    efs_client.put_file_system_policy(FileSystemId=fs_id, Policy=policy)

    resp = efs_client.describe_file_system_policy(FileSystemId=fs_id)
    assert resp["FileSystemId"] == fs_id
    assert resp["Policy"] != ""

    efs_client.delete_file_system_policy(FileSystemId=fs_id)

    efs_client.delete_file_system(FileSystemId=fs_id)


# ── Mount Targets ─────────────────────────────────────────────────────────


def test_mount_target_crud(efs_client):
    fs = efs_client.create_file_system(CreationToken="mt-test-1")
    fs_id = fs["FileSystemId"]

    mt_resp = efs_client.create_mount_target(
        FileSystemId=fs_id,
        SubnetId="subnet-0123456789abcdef",
        SecurityGroups=["sg-0123456789abcdef"],
    )
    assert mt_resp["ResponseMetadata"]["HTTPStatusCode"] == 200
    mt_id = mt_resp["MountTargetId"]
    assert mt_id.startswith("fsmt-")

    # Describe by FS
    list_resp = efs_client.describe_mount_targets(FileSystemId=fs_id)
    assert len(list_resp["MountTargets"]) == 1
    assert list_resp["MountTargets"][0]["FileSystemId"] == fs_id

    # Security groups
    sg_resp = efs_client.describe_mount_target_security_groups(MountTargetId=mt_id)
    assert "SecurityGroups" in sg_resp

    efs_client.modify_mount_target_security_groups(
        MountTargetId=mt_id,
        SecurityGroups=["sg-bbbbbbbb"],
    )

    efs_client.delete_mount_target(MountTargetId=mt_id)
    efs_client.delete_file_system(FileSystemId=fs_id)


# ── Access Points ─────────────────────────────────────────────────────────


def test_access_point_crud(efs_client):
    fs = efs_client.create_file_system(CreationToken="ap-test-1")
    fs_id = fs["FileSystemId"]

    ap_resp = efs_client.create_access_point(
        FileSystemId=fs_id,
        ClientToken="ap-client-token-1",
        Tags=[{"Key": "Name", "Value": "my-ap"}],
    )
    assert ap_resp["ResponseMetadata"]["HTTPStatusCode"] == 200
    ap_id = ap_resp["AccessPointId"]
    assert ap_id.startswith("fsap-")
    assert ap_resp["LifeCycleState"] == "available"

    # Describe
    list_resp = efs_client.describe_access_points(FileSystemId=fs_id)
    assert len(list_resp["AccessPoints"]) == 1

    # Delete
    efs_client.delete_access_point(AccessPointId=ap_id)
    efs_client.delete_file_system(FileSystemId=fs_id)


# ── Tags ──────────────────────────────────────────────────────────────────


def test_tagging(efs_client):
    fs = efs_client.create_file_system(
        CreationToken="tag-test-1",
        Tags=[{"Key": "env", "Value": "test"}],
    )
    fs_id = fs["FileSystemId"]

    resp = efs_client.list_tags_for_resource(ResourceId=fs_id)
    tags = {t["Key"]: t["Value"] for t in resp["Tags"]}
    assert tags["env"] == "test"

    efs_client.tag_resource(
        ResourceId=fs_id,
        Tags=[{"Key": "tier", "Value": "prod"}],
    )

    resp2 = efs_client.list_tags_for_resource(ResourceId=fs_id)
    tags2 = {t["Key"]: t["Value"] for t in resp2["Tags"]}
    assert "tier" in tags2

    efs_client.untag_resource(ResourceId=fs_id, TagKeys=["env"])

    resp3 = efs_client.list_tags_for_resource(ResourceId=fs_id)
    tags3 = {t["Key"]: t["Value"] for t in resp3["Tags"]}
    assert "env" not in tags3

    efs_client.delete_file_system(FileSystemId=fs_id)


# ── Replication (stub) ────────────────────────────────────────────────────


def test_describe_replication_configurations(efs_client):
    resp = efs_client.describe_replication_configurations()
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
    assert "Replications" in resp
