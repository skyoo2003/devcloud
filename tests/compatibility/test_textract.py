import pytest
from botocore.exceptions import ClientError


def test_list_adapters(textract_client):
    """Minimal test to verify the Textract service responds."""
    resp = textract_client.list_adapters()
    assert "Adapters" in resp
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


def test_analyze_document(textract_client):
    resp = textract_client.analyze_document(
        Document={"Bytes": b"fake-pdf-content"},
        FeatureTypes=["TABLES", "FORMS"],
    )
    assert "Blocks" in resp
    assert "DocumentMetadata" in resp
    assert resp["DocumentMetadata"]["Pages"] == 1


def test_analyze_expense(textract_client):
    resp = textract_client.analyze_expense(
        Document={"Bytes": b"fake-receipt"},
    )
    assert "ExpenseDocuments" in resp
    assert "DocumentMetadata" in resp


def test_analyze_id(textract_client):
    resp = textract_client.analyze_id(
        DocumentPages=[{"Bytes": b"fake-id-doc"}],
    )
    assert "IdentityDocuments" in resp
    assert "DocumentMetadata" in resp


def test_detect_document_text(textract_client):
    resp = textract_client.detect_document_text(
        Document={"Bytes": b"fake-document"},
    )
    assert "Blocks" in resp
    assert "DocumentMetadata" in resp


def test_start_and_get_document_analysis(textract_client):
    start_resp = textract_client.start_document_analysis(
        DocumentLocation={"S3Object": {"Bucket": "test-bucket", "Name": "test.pdf"}},
        FeatureTypes=["TABLES"],
    )
    job_id = start_resp["JobId"]
    assert job_id

    get_resp = textract_client.get_document_analysis(JobId=job_id)
    assert get_resp["JobStatus"] == "SUCCEEDED"
    assert "Blocks" in get_resp


def test_start_and_get_document_text_detection(textract_client):
    start_resp = textract_client.start_document_text_detection(
        DocumentLocation={"S3Object": {"Bucket": "test-bucket", "Name": "test.pdf"}},
    )
    job_id = start_resp["JobId"]
    assert job_id

    get_resp = textract_client.get_document_text_detection(JobId=job_id)
    assert get_resp["JobStatus"] == "SUCCEEDED"


def test_start_and_get_expense_analysis(textract_client):
    start_resp = textract_client.start_expense_analysis(
        DocumentLocation={"S3Object": {"Bucket": "test-bucket", "Name": "receipt.pdf"}},
    )
    job_id = start_resp["JobId"]
    assert job_id

    get_resp = textract_client.get_expense_analysis(JobId=job_id)
    assert get_resp["JobStatus"] == "SUCCEEDED"


def test_start_and_get_lending_analysis(textract_client):
    start_resp = textract_client.start_lending_analysis(
        DocumentLocation={"S3Object": {"Bucket": "test-bucket", "Name": "loan.pdf"}},
    )
    job_id = start_resp["JobId"]
    assert job_id

    get_resp = textract_client.get_lending_analysis(JobId=job_id)
    assert get_resp["JobStatus"] == "SUCCEEDED"


def test_adapter_crud(textract_client):
    # Create
    resp = textract_client.create_adapter(
        AdapterName="my-adapter",
        FeatureTypes=["TABLES"],
        AutoUpdate="ENABLED",
    )
    adapter_id = resp["AdapterId"]
    assert adapter_id

    # Get
    get_resp = textract_client.get_adapter(AdapterId=adapter_id)
    assert get_resp["AdapterId"] == adapter_id
    assert get_resp["AdapterName"] == "my-adapter"

    # List
    list_resp = textract_client.list_adapters()
    assert any(a["AdapterId"] == adapter_id for a in list_resp["Adapters"])

    # Update
    upd_resp = textract_client.update_adapter(
        AdapterId=adapter_id, AutoUpdate="DISABLED"
    )
    assert upd_resp["AutoUpdate"] == "DISABLED"

    # Delete
    textract_client.delete_adapter(AdapterId=adapter_id)

    # Verify deleted
    with pytest.raises(ClientError):
        textract_client.get_adapter(AdapterId=adapter_id)


def test_get_lending_analysis_summary(textract_client):
    start = textract_client.start_lending_analysis(
        DocumentLocation={"S3Object": {"Bucket": "tst", "Name": "l.pdf"}},
    )
    resp = textract_client.get_lending_analysis_summary(JobId=start["JobId"])
    assert "Summary" in resp


def test_list_tags_for_resource(textract_client):
    res = textract_client.create_adapter(
        AdapterName="tag-adapter", FeatureTypes=["TABLES"]
    )
    get_resp = textract_client.get_adapter(AdapterId=res["AdapterId"])
    arn = get_resp["AdapterArn"]
    textract_client.tag_resource(ResourceARN=arn, Tags={"Env": "dev"})
    tags = textract_client.list_tags_for_resource(ResourceARN=arn)
    assert "Tags" in tags
