from botocore.exceptions import ClientError


# ── Foundation Models ─────────────────────────────────────────────────────


def test_list_foundation_models(bedrock_client):
    resp = bedrock_client.list_foundation_models()
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
    models = resp["modelSummaries"]
    assert len(models) >= 4
    model_ids = [m["modelId"] for m in models]
    assert "anthropic.claude-3-opus-20240229-v1:0" in model_ids
    assert "anthropic.claude-3-sonnet-20240229-v1:0" in model_ids


def test_get_foundation_model(bedrock_client):
    resp = bedrock_client.get_foundation_model(
        modelIdentifier="anthropic.claude-3-opus-20240229-v1:0"
    )
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
    assert "modelDetails" in resp


def test_get_foundation_model_not_found(bedrock_client):
    try:
        bedrock_client.get_foundation_model(modelIdentifier="nonexistent-model")
        assert False, "expected exception"
    except ClientError as e:
        assert e.response["ResponseMetadata"]["HTTPStatusCode"] == 404


# ── Customization Jobs ────────────────────────────────────────────────────


def test_model_customization_job_crud(bedrock_client):
    resp = bedrock_client.create_model_customization_job(
        jobName="test-job-1",
        customModelName="my-custom-model-1",
        roleArn="arn:aws:iam::000000000000:role/BedrockRole",
        baseModelIdentifier="amazon.titan-text-express-v1",
        trainingDataConfig={"s3Uri": "s3://my-bucket/training"},
        outputDataConfig={"s3Uri": "s3://my-bucket/output"},
    )
    assert resp["ResponseMetadata"]["HTTPStatusCode"] in (200, 201)
    job_arn = resp["jobArn"]
    assert "model-customization-job" in job_arn

    # Extract job ID from ARN
    job_id = job_arn.split("/")[-1]

    # Get
    get_resp = bedrock_client.get_model_customization_job(jobIdentifier=job_id)
    assert get_resp["ResponseMetadata"]["HTTPStatusCode"] == 200
    assert get_resp["jobName"] == "test-job-1"
    assert get_resp["status"] == "InProgress"

    # List
    list_resp = bedrock_client.list_model_customization_jobs()
    assert list_resp["ResponseMetadata"]["HTTPStatusCode"] == 200
    jobs = list_resp["modelCustomizationJobSummaries"]
    assert len(jobs) >= 1

    # Stop
    stop_resp = bedrock_client.stop_model_customization_job(jobIdentifier=job_id)
    assert stop_resp["ResponseMetadata"]["HTTPStatusCode"] in (200, 204)


# ── Custom Models ─────────────────────────────────────────────────────────


def test_custom_models_list(bedrock_client):
    # Create a customization job first (creates a custom model)
    bedrock_client.create_model_customization_job(
        jobName="model-list-test-job",
        customModelName="custom-list-model",
        roleArn="arn:aws:iam::000000000000:role/BedrockRole",
        baseModelIdentifier="amazon.titan-text-express-v1",
        trainingDataConfig={"s3Uri": "s3://my-bucket/training"},
        outputDataConfig={"s3Uri": "s3://my-bucket/output"},
    )

    resp = bedrock_client.list_custom_models()
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
    assert "modelSummaries" in resp


# ── Guardrails ────────────────────────────────────────────────────────────


def test_guardrail_crud(bedrock_client):
    # Create
    resp = bedrock_client.create_guardrail(
        name="test-guardrail-1",
        description="A test guardrail",
        blockedInputMessaging="blocked",
        blockedOutputsMessaging="blocked",
    )
    assert resp["ResponseMetadata"]["HTTPStatusCode"] in (200, 201)
    guardrail_id = resp["guardrailId"]
    assert guardrail_id != ""
    guardrail_arn = resp["guardrailArn"]
    assert "guardrail" in guardrail_arn

    # Get
    get_resp = bedrock_client.get_guardrail(guardrailIdentifier=guardrail_id)
    assert get_resp["ResponseMetadata"]["HTTPStatusCode"] == 200
    assert get_resp["name"] == "test-guardrail-1"

    # List
    list_resp = bedrock_client.list_guardrails()
    assert list_resp["ResponseMetadata"]["HTTPStatusCode"] == 200
    guardrails = list_resp["guardrails"]
    found = any(g.get("id", g.get("guardrailId")) == guardrail_id for g in guardrails)
    assert found

    # Update
    update_resp = bedrock_client.update_guardrail(
        guardrailIdentifier=guardrail_id,
        name="updated-guardrail",
        blockedInputMessaging="blocked",
        blockedOutputsMessaging="blocked",
    )
    assert update_resp["ResponseMetadata"]["HTTPStatusCode"] in (200, 202)

    # Delete
    del_resp = bedrock_client.delete_guardrail(guardrailIdentifier=guardrail_id)
    assert del_resp["ResponseMetadata"]["HTTPStatusCode"] in (200, 202, 204)


def test_guardrail_not_found(bedrock_client):
    try:
        bedrock_client.get_guardrail(guardrailIdentifier="nonexistent-id")
        assert False, "expected exception"
    except ClientError as e:
        assert e.response["ResponseMetadata"]["HTTPStatusCode"] == 404


# ── Tags ──────────────────────────────────────────────────────────────────


def test_bedrock_tagging(bedrock_client):
    # Create a guardrail to tag
    resp = bedrock_client.create_guardrail(
        name="tag-test-guardrail",
        description="tagging test",
        blockedInputMessaging="blocked",
        blockedOutputsMessaging="blocked",
    )
    arn = resp["guardrailArn"]

    # Tag (tags must be a list of {key, value} dicts per bedrock API)
    bedrock_client.tag_resource(
        resourceARN=arn,
        tags=[{"key": "env", "value": "test"}, {"key": "tier", "value": "free"}],
    )

    # List tags
    tag_resp = bedrock_client.list_tags_for_resource(resourceARN=arn)
    assert tag_resp["ResponseMetadata"]["HTTPStatusCode"] == 200
    tags = tag_resp["tags"]
    tag_dict = {t["key"]: t["value"] for t in tags}
    assert tag_dict.get("env") == "test"

    # Untag
    bedrock_client.untag_resource(resourceARN=arn, tagKeys=["env"])

    tag_resp2 = bedrock_client.list_tags_for_resource(resourceARN=arn)
    tags2 = tag_resp2["tags"]
    tag_dict2 = {t["key"]: t["value"] for t in tags2}
    assert "env" not in tag_dict2
