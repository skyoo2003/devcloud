def test_start_snapshot(ebs_client):
    resp = ebs_client.start_snapshot(VolumeSize=1)
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
    snapshot_id = resp["SnapshotId"]
    assert snapshot_id.startswith("snap-")
    assert resp["VolumeSize"] == 1
    assert resp["Status"] == "pending"


def test_complete_snapshot(ebs_client):
    start_resp = ebs_client.start_snapshot(VolumeSize=1)
    snapshot_id = start_resp["SnapshotId"]

    complete_resp = ebs_client.complete_snapshot(
        SnapshotId=snapshot_id,
        ChangedBlocksCount=0,
    )
    assert complete_resp["ResponseMetadata"]["HTTPStatusCode"] == 200
    assert complete_resp["Status"] == "completed"


def test_list_snapshot_blocks(ebs_client):
    start_resp = ebs_client.start_snapshot(VolumeSize=1)
    snapshot_id = start_resp["SnapshotId"]

    blocks_resp = ebs_client.list_snapshot_blocks(SnapshotId=snapshot_id)
    assert blocks_resp["ResponseMetadata"]["HTTPStatusCode"] == 200
    assert "Blocks" in blocks_resp
    assert blocks_resp["VolumeSize"] == 1


def test_put_and_get_snapshot_block(ebs_client):
    start_resp = ebs_client.start_snapshot(VolumeSize=1)
    snapshot_id = start_resp["SnapshotId"]

    block_data = b"X" * 524288  # 512 KB
    import hashlib
    import base64

    checksum = base64.b64encode(hashlib.sha256(block_data).digest()).decode()

    put_resp = ebs_client.put_snapshot_block(
        SnapshotId=snapshot_id,
        BlockIndex=0,
        BlockData=block_data,
        DataLength=len(block_data),
        Checksum=checksum,
        ChecksumAlgorithm="SHA256",
    )
    assert put_resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    get_resp = ebs_client.get_snapshot_block(
        SnapshotId=snapshot_id,
        BlockIndex=0,
        BlockToken=put_resp.get("Checksum", checksum),
    )
    assert get_resp["ResponseMetadata"]["HTTPStatusCode"] == 200
    assert get_resp["DataLength"] > 0


def test_list_changed_blocks(ebs_client):
    snap1 = ebs_client.start_snapshot(VolumeSize=1)["SnapshotId"]
    snap2 = ebs_client.start_snapshot(VolumeSize=1, ParentSnapshotId=snap1)[
        "SnapshotId"
    ]

    resp = ebs_client.list_changed_blocks(
        FirstSnapshotId=snap1,
        SecondSnapshotId=snap2,
    )
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
    assert "ChangedBlocks" in resp


def test_complete_snapshot_not_found(ebs_client):
    import botocore.exceptions

    try:
        ebs_client.complete_snapshot(
            SnapshotId="snap-nonexistent123456",
            ChangedBlocksCount=0,
        )
        assert False, "should raise"
    except (
        ebs_client.exceptions.ResourceNotFoundException,
        botocore.exceptions.ClientError,
    ):
        pass
