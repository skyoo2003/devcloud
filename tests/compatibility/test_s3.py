def test_create_and_list_buckets(s3_client):
    s3_client.create_bucket(Bucket="test-bucket-list")
    resp = s3_client.list_buckets()
    names = [b["Name"] for b in resp["Buckets"]]
    assert "test-bucket-list" in names


def test_put_get_delete_object(s3_client):
    s3_client.create_bucket(Bucket="obj-bucket")
    s3_client.put_object(Bucket="obj-bucket", Key="hello.txt", Body=b"world")
    resp = s3_client.get_object(Bucket="obj-bucket", Key="hello.txt")
    assert resp["Body"].read() == b"world"
    s3_client.delete_object(Bucket="obj-bucket", Key="hello.txt")


def test_list_objects_v2(s3_client):
    s3_client.create_bucket(Bucket="v2-bucket")
    for i in range(5):
        s3_client.put_object(Bucket="v2-bucket", Key=f"key{i}", Body=b"x")
    resp = s3_client.list_objects_v2(Bucket="v2-bucket", MaxKeys=2)
    assert resp["KeyCount"] == 2
    assert resp["IsTruncated"] is True
    resp2 = s3_client.list_objects_v2(
        Bucket="v2-bucket",
        MaxKeys=2,
        ContinuationToken=resp["NextContinuationToken"],
    )
    assert resp2["KeyCount"] >= 1


def test_multipart_upload(s3_client):
    s3_client.create_bucket(Bucket="mp-bucket")
    resp = s3_client.create_multipart_upload(Bucket="mp-bucket", Key="big.bin")
    upload_id = resp["UploadId"]
    part1 = s3_client.upload_part(
        Bucket="mp-bucket",
        Key="big.bin",
        UploadId=upload_id,
        PartNumber=1,
        Body=b"a" * 5 * 1024 * 1024,
    )
    part2 = s3_client.upload_part(
        Bucket="mp-bucket",
        Key="big.bin",
        UploadId=upload_id,
        PartNumber=2,
        Body=b"b" * 1024,
    )
    s3_client.complete_multipart_upload(
        Bucket="mp-bucket",
        Key="big.bin",
        UploadId=upload_id,
        MultipartUpload={
            "Parts": [
                {"PartNumber": 1, "ETag": part1["ETag"]},
                {"PartNumber": 2, "ETag": part2["ETag"]},
            ]
        },
    )
    resp = s3_client.get_object(Bucket="mp-bucket", Key="big.bin")
    data = resp["Body"].read()
    assert len(data) == 5 * 1024 * 1024 + 1024


def test_delete_objects(s3_client):
    s3_client.create_bucket(Bucket="del-bucket")
    s3_client.put_object(Bucket="del-bucket", Key="a", Body=b"1")
    s3_client.put_object(Bucket="del-bucket", Key="b", Body=b"2")
    resp = s3_client.delete_objects(
        Bucket="del-bucket",
        Delete={"Objects": [{"Key": "a"}, {"Key": "b"}]},
    )
    assert len(resp.get("Deleted", [])) == 2


def test_bucket_policy(s3_client):
    s3_client.create_bucket(Bucket="pol-bucket")
    policy = '{"Version":"2012-10-17","Statement":[]}'
    s3_client.put_bucket_policy(Bucket="pol-bucket", Policy=policy)
    resp = s3_client.get_bucket_policy(Bucket="pol-bucket")
    assert "Version" in resp["Policy"]
    s3_client.delete_bucket_policy(Bucket="pol-bucket")


def test_bucket_tagging(s3_client):
    s3_client.create_bucket(Bucket="tag-bucket")
    s3_client.put_bucket_tagging(
        Bucket="tag-bucket",
        Tagging={"TagSet": [{"Key": "env", "Value": "test"}]},
    )
    resp = s3_client.get_bucket_tagging(Bucket="tag-bucket")
    assert any(t["Key"] == "env" for t in resp["TagSet"])
    s3_client.delete_bucket_tagging(Bucket="tag-bucket")


def test_object_tagging(s3_client):
    s3_client.create_bucket(Bucket="otag-bucket")
    s3_client.put_object(Bucket="otag-bucket", Key="f.txt", Body=b"data")
    s3_client.put_object_tagging(
        Bucket="otag-bucket",
        Key="f.txt",
        Tagging={"TagSet": [{"Key": "status", "Value": "active"}]},
    )
    resp = s3_client.get_object_tagging(Bucket="otag-bucket", Key="f.txt")
    assert any(t["Key"] == "status" for t in resp["TagSet"])


def test_bucket_cors(s3_client):
    s3_client.create_bucket(Bucket="cors-bucket")
    s3_client.put_bucket_cors(
        Bucket="cors-bucket",
        CORSConfiguration={
            "CORSRules": [{"AllowedMethods": ["GET"], "AllowedOrigins": ["*"]}]
        },
    )
    resp = s3_client.get_bucket_cors(Bucket="cors-bucket")
    assert len(resp["CORSRules"]) >= 1
    s3_client.delete_bucket_cors(Bucket="cors-bucket")
