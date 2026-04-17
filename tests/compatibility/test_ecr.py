import pytest
from botocore.exceptions import ClientError


def test_repository_crud(ecr_client):
    resp = ecr_client.create_repository(repositoryName="test-repo")
    repo = resp["repository"]
    assert repo["repositoryName"] == "test-repo"
    assert "test-repo" in repo["repositoryUri"]
    assert repo["repositoryArn"].startswith("arn:aws:ecr:")

    desc = ecr_client.describe_repositories(repositoryNames=["test-repo"])
    assert len(desc["repositories"]) == 1

    ecr_client.delete_repository(repositoryName="test-repo")
    desc2 = ecr_client.describe_repositories()
    remaining = [r for r in desc2["repositories"] if r["repositoryName"] == "test-repo"]
    assert len(remaining) == 0


def test_put_and_describe_images(ecr_client):
    ecr_client.create_repository(repositoryName="img-repo")
    manifest = '{"schemaVersion":2,"mediaType":"application/vnd.docker.distribution.manifest.v2+json"}'
    resp = ecr_client.put_image(
        repositoryName="img-repo",
        imageManifest=manifest,
        imageTag="v1.0",
    )
    assert resp["image"]["imageId"]["imageTag"] == "v1.0"
    digest = resp["image"]["imageId"]["imageDigest"]
    assert digest.startswith("sha256:")

    desc = ecr_client.describe_images(repositoryName="img-repo")
    assert len(desc["imageDetails"]) == 1

    imgs = ecr_client.list_images(repositoryName="img-repo")
    assert len(imgs["imageIds"]) == 1


def test_get_authorization_token(ecr_client):
    resp = ecr_client.get_authorization_token()
    data = resp["authorizationData"]
    assert len(data) == 1
    assert data[0]["authorizationToken"]
    assert "expiresAt" in data[0]


def test_describe_nonexistent_repository(ecr_client):
    with pytest.raises(ClientError) as exc:
        ecr_client.describe_repositories(repositoryNames=["no-such-repo-xyz"])
    assert exc.value.response["Error"]["Code"] == "RepositoryNotFoundException"


def test_batch_delete_image(ecr_client):
    ecr_client.create_repository(repositoryName="batch-del-repo")
    manifest = '{"schemaVersion":2,"mediaType":"application/vnd.docker.distribution.manifest.v2+json"}'
    put_resp = ecr_client.put_image(
        repositoryName="batch-del-repo", imageManifest=manifest, imageTag="v1"
    )
    digest = put_resp["image"]["imageId"]["imageDigest"]
    resp = ecr_client.batch_delete_image(
        repositoryName="batch-del-repo", imageIds=[{"imageDigest": digest}]
    )
    assert len(resp.get("failures", [])) == 0


def test_set_and_get_repository_policy(ecr_client):
    ecr_client.create_repository(repositoryName="policy-repo")
    policy = '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"ecr:GetDownloadUrlForLayer"}]}'
    ecr_client.set_repository_policy(repositoryName="policy-repo", policyText=policy)
    resp = ecr_client.get_repository_policy(repositoryName="policy-repo")
    assert "policyText" in resp


def test_layer_upload(ecr_client):
    ecr_client.create_repository(repositoryName="layer-repo")
    init = ecr_client.initiate_layer_upload(repositoryName="layer-repo")
    upload_id = init["uploadId"]

    blob = b"fake layer data"
    ecr_client.upload_layer_part(
        repositoryName="layer-repo",
        uploadId=upload_id,
        partFirstByte=0,
        partLastByte=len(blob) - 1,
        layerPartBlob=blob,
    )

    import hashlib

    digest = "sha256:" + hashlib.sha256(blob).hexdigest()
    ecr_client.complete_layer_upload(
        repositoryName="layer-repo",
        uploadId=upload_id,
        layerDigests=[digest],
    )

    check = ecr_client.batch_check_layer_availability(
        repositoryName="layer-repo",
        layerDigests=[digest],
    )
    assert any(layer["layerAvailability"] == "AVAILABLE" for layer in check["layers"])

    dl = ecr_client.get_download_url_for_layer(
        repositoryName="layer-repo",
        layerDigest=digest,
    )
    assert "downloadUrl" in dl


def test_lifecycle_policy(ecr_client):
    ecr_client.create_repository(repositoryName="lc-repo")
    policy = '{"rules":[{"rulePriority":1,"description":"expire","selection":{"tagStatus":"untagged","countType":"imageCountMoreThan","countNumber":10},"action":{"type":"expire"}}]}'
    ecr_client.put_lifecycle_policy(
        repositoryName="lc-repo",
        lifecyclePolicyText=policy,
    )
    resp = ecr_client.get_lifecycle_policy(repositoryName="lc-repo")
    assert "rules" in resp["lifecyclePolicyText"]
    ecr_client.delete_lifecycle_policy(repositoryName="lc-repo")


def test_repo_tags(ecr_client):
    resp = ecr_client.create_repository(repositoryName="tag-repo")
    arn = resp["repository"]["repositoryArn"]
    ecr_client.tag_resource(
        resourceArn=arn,
        tags=[{"Key": "env", "Value": "test"}],
    )
    tags = ecr_client.list_tags_for_resource(resourceArn=arn)
    assert any(t["Key"] == "env" for t in tags["tags"])
    ecr_client.untag_resource(resourceArn=arn, tagKeys=["env"])


def test_image_scan(ecr_client):
    ecr_client.create_repository(repositoryName="scan-repo")
    ecr_client.put_image(
        repositoryName="scan-repo",
        imageManifest='{"schemaVersion":2}',
        imageTag="v1",
    )
    ecr_client.start_image_scan(
        repositoryName="scan-repo",
        imageId={"imageTag": "v1"},
    )
    resp = ecr_client.describe_image_scan_findings(
        repositoryName="scan-repo",
        imageId={"imageTag": "v1"},
    )
    assert resp["imageScanStatus"]["status"] == "COMPLETE"


def test_image_scanning_configuration(ecr_client):
    ecr_client.create_repository(repositoryName="cfg-repo")
    ecr_client.put_image_scanning_configuration(
        repositoryName="cfg-repo",
        imageScanningConfiguration={"scanOnPush": True},
    )
