def test_create_project(codebuild_client):
    resp = codebuild_client.create_project(
        name="compat-project",
        source={"type": "NO_SOURCE", "buildspec": "version: 0.2"},
        artifacts={"type": "NO_ARTIFACTS"},
        environment={
            "type": "LINUX_CONTAINER",
            "image": "aws/codebuild/standard:5.0",
            "computeType": "BUILD_GENERAL1_SMALL",
        },
        serviceRole="arn:aws:iam::000000000000:role/codebuild-role",
    )
    assert resp["project"]["name"] == "compat-project"
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


def test_batch_get_projects(codebuild_client):
    codebuild_client.create_project(
        name="batch-proj",
        source={"type": "NO_SOURCE", "buildspec": "version: 0.2"},
        artifacts={"type": "NO_ARTIFACTS"},
        environment={
            "type": "LINUX_CONTAINER",
            "image": "aws/codebuild/standard:5.0",
            "computeType": "BUILD_GENERAL1_SMALL",
        },
        serviceRole="arn:aws:iam::000000000000:role/codebuild-role",
    )
    resp = codebuild_client.batch_get_projects(names=["batch-proj"])
    assert len(resp["projects"]) == 1
    assert resp["projects"][0]["name"] == "batch-proj"
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


def test_list_projects(codebuild_client):
    codebuild_client.create_project(
        name="list-proj",
        source={"type": "NO_SOURCE", "buildspec": "version: 0.2"},
        artifacts={"type": "NO_ARTIFACTS"},
        environment={
            "type": "LINUX_CONTAINER",
            "image": "aws/codebuild/standard:5.0",
            "computeType": "BUILD_GENERAL1_SMALL",
        },
        serviceRole="arn:aws:iam::000000000000:role/codebuild-role",
    )
    resp = codebuild_client.list_projects()
    assert "list-proj" in resp["projects"]
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
