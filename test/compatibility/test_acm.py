import pytest
from botocore.exceptions import ClientError


def test_request_and_describe_certificate(acm_client):
    resp = acm_client.request_certificate(DomainName="boto3.example.com")
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
    arn = resp["CertificateArn"]
    assert arn.startswith("arn:aws:acm:")

    desc = acm_client.describe_certificate(CertificateArn=arn)
    cert = desc["Certificate"]
    assert cert["DomainName"] == "boto3.example.com"
    assert cert["Status"] == "ISSUED"

    get = acm_client.get_certificate(CertificateArn=arn)
    assert "BEGIN CERTIFICATE" in get["Certificate"]

    certs = acm_client.list_certificates()
    arns = [c["CertificateArn"] for c in certs["CertificateSummaryList"]]
    assert arn in arns

    acm_client.delete_certificate(CertificateArn=arn)


def test_import_certificate(acm_client):
    from cryptography.hazmat.primitives.asymmetric import ec
    from cryptography.hazmat.primitives import serialization, hashes
    from cryptography import x509
    from cryptography.x509.oid import NameOID
    import datetime

    key = ec.generate_private_key(ec.SECP256R1())
    key_pem = key.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.TraditionalOpenSSL,
        serialization.NoEncryption(),
    )
    subject = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "import.boto3.com")])
    cert = (
        x509.CertificateBuilder()
        .subject_name(subject)
        .issuer_name(subject)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(datetime.datetime.now(datetime.timezone.utc))
        .not_valid_after(
            datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(days=365)
        )
        .add_extension(
            x509.SubjectAlternativeName([x509.DNSName("import.boto3.com")]),
            critical=False,
        )
        .sign(key, hashes.SHA256())
    )
    cert_pem = cert.public_bytes(serialization.Encoding.PEM)

    resp = acm_client.import_certificate(Certificate=cert_pem, PrivateKey=key_pem)
    arn = resp["CertificateArn"]
    assert arn.startswith("arn:aws:acm:")

    desc = acm_client.describe_certificate(CertificateArn=arn)
    assert desc["Certificate"]["Type"] == "IMPORTED"

    acm_client.delete_certificate(CertificateArn=arn)


def test_certificate_tags(acm_client):
    resp = acm_client.request_certificate(DomainName="tags.boto3.com")
    arn = resp["CertificateArn"]

    acm_client.add_tags_to_certificate(
        CertificateArn=arn,
        Tags=[{"Key": "env", "Value": "staging"}, {"Key": "owner", "Value": "bob"}],
    )
    tags_resp = acm_client.list_tags_for_certificate(CertificateArn=arn)
    tag_map = {t["Key"]: t["Value"] for t in tags_resp["Tags"]}
    assert tag_map.get("env") == "staging"
    assert tag_map.get("owner") == "bob"

    acm_client.remove_tags_from_certificate(
        CertificateArn=arn,
        Tags=[{"Key": "owner", "Value": "bob"}],
    )
    tags_resp2 = acm_client.list_tags_for_certificate(CertificateArn=arn)
    keys2 = [t["Key"] for t in tags_resp2["Tags"]]
    assert "owner" not in keys2
    assert "env" in keys2

    acm_client.delete_certificate(CertificateArn=arn)


def test_describe_nonexistent_certificate(acm_client):
    with pytest.raises(ClientError) as exc:
        acm_client.describe_certificate(
            CertificateArn="arn:aws:acm:us-east-1:000000000000:certificate/00000000-0000-0000-0000-000000000000"
        )
    assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


def test_list_tags_for_certificate(acm_client):
    resp = acm_client.request_certificate(DomainName="listtags.acm.com")
    arn = resp["CertificateArn"]
    acm_client.add_tags_to_certificate(
        CertificateArn=arn, Tags=[{"Key": "team", "Value": "platform"}]
    )
    tags = acm_client.list_tags_for_certificate(CertificateArn=arn)
    tag_map = {t["Key"]: t["Value"] for t in tags["Tags"]}
    assert tag_map.get("team") == "platform"
    acm_client.delete_certificate(CertificateArn=arn)


def test_renew_certificate(acm_client):
    resp = acm_client.request_certificate(DomainName="renew.acm.com")
    arn = resp["CertificateArn"]
    acm_client.renew_certificate(CertificateArn=arn)
    acm_client.delete_certificate(CertificateArn=arn)


def test_account_configuration(acm_client):
    resp = acm_client.get_account_configuration()
    assert "ExpiryEvents" in resp


def test_export_certificate(acm_client):
    req = acm_client.request_certificate(DomainName="export.example.com")
    resp = acm_client.export_certificate(
        CertificateArn=req["CertificateArn"],
        Passphrase=b"password",
    )
    assert "Certificate" in resp
    assert "PrivateKey" in resp


def test_certificate_options(acm_client):
    req = acm_client.request_certificate(DomainName="options.example.com")
    acm_client.update_certificate_options(
        CertificateArn=req["CertificateArn"],
        Options={"CertificateTransparencyLoggingPreference": "DISABLED"},
    )


def test_resend_validation_email(acm_client):
    req = acm_client.request_certificate(DomainName="validate.example.com")
    acm_client.resend_validation_email(
        CertificateArn=req["CertificateArn"],
        Domain="validate.example.com",
        ValidationDomain="validate.example.com",
    )
