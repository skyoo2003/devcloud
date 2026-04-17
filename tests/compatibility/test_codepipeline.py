def test_create_pipeline(codepipeline_client):
    resp = codepipeline_client.create_pipeline(
        pipeline={
            "name": "compat-pipeline",
            "roleArn": "arn:aws:iam::000000000000:role/test-role",
            "stages": [
                {
                    "name": "Source",
                    "actions": [
                        {
                            "name": "SourceAction",
                            "actionTypeId": {
                                "category": "Source",
                                "owner": "AWS",
                                "provider": "S3",
                                "version": "1",
                            },
                            "configuration": {
                                "S3Bucket": "my-bucket",
                                "S3ObjectKey": "my-key",
                            },
                            "outputArtifacts": [{"name": "SourceOutput"}],
                        }
                    ],
                },
            ],
        }
    )
    assert resp["pipeline"]["name"] == "compat-pipeline"
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


def test_get_pipeline(codepipeline_client):
    codepipeline_client.create_pipeline(
        pipeline={
            "name": "get-pipeline",
            "roleArn": "arn:aws:iam::000000000000:role/test-role",
            "stages": [],
        }
    )
    resp = codepipeline_client.get_pipeline(name="get-pipeline")
    assert resp["pipeline"]["name"] == "get-pipeline"
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


def test_list_pipelines(codepipeline_client):
    codepipeline_client.create_pipeline(
        pipeline={
            "name": "list-pipeline",
            "roleArn": "arn:aws:iam::000000000000:role/test-role",
            "stages": [],
        }
    )
    resp = codepipeline_client.list_pipelines()
    names = [p["name"] for p in resp["pipelines"]]
    assert "list-pipeline" in names
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


def test_delete_pipeline(codepipeline_client):
    codepipeline_client.create_pipeline(
        pipeline={
            "name": "del-pipeline",
            "roleArn": "arn:aws:iam::000000000000:role/test-role",
            "stages": [],
        }
    )
    resp = codepipeline_client.delete_pipeline(name="del-pipeline")
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
    list_resp = codepipeline_client.list_pipelines()
    names = [p["name"] for p in list_resp["pipelines"]]
    assert "del-pipeline" not in names
