def test_create_and_describe_certificate_authority(acmpca_client):
    resp = acmpca_client.create_certificate_authority(
        CertificateAuthorityType="ROOT",
        CertificateAuthorityConfiguration={
            "KeyAlgorithm": "RSA_2048",
            "SigningAlgorithm": "SHA256WITHRSA",
            "Subject": {"CommonName": "Test Root CA"},
        },
    )
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
    ca_arn = resp["CertificateAuthorityArn"]
    assert ca_arn.startswith("arn:aws:acm-pca:")

    desc = acmpca_client.describe_certificate_authority(CertificateAuthorityArn=ca_arn)
    assert desc["ResponseMetadata"]["HTTPStatusCode"] == 200
    ca = desc["CertificateAuthority"]
    assert ca["Arn"] == ca_arn
    assert ca["Status"] == "ACTIVE"

    acmpca_client.delete_certificate_authority(CertificateAuthorityArn=ca_arn)


def test_list_certificate_authorities(acmpca_client):
    resp = acmpca_client.create_certificate_authority(
        CertificateAuthorityType="ROOT",
        CertificateAuthorityConfiguration={
            "KeyAlgorithm": "RSA_2048",
            "SigningAlgorithm": "SHA256WITHRSA",
            "Subject": {"CommonName": "List CA"},
        },
    )
    ca_arn = resp["CertificateAuthorityArn"]

    cas = acmpca_client.list_certificate_authorities()
    assert cas["ResponseMetadata"]["HTTPStatusCode"] == 200
    arns = [ca["Arn"] for ca in cas["CertificateAuthorities"]]
    assert ca_arn in arns

    acmpca_client.delete_certificate_authority(CertificateAuthorityArn=ca_arn)


def test_delete_certificate_authority(acmpca_client):
    resp = acmpca_client.create_certificate_authority(
        CertificateAuthorityType="ROOT",
        CertificateAuthorityConfiguration={
            "KeyAlgorithm": "RSA_2048",
            "SigningAlgorithm": "SHA256WITHRSA",
            "Subject": {"CommonName": "Delete CA"},
        },
    )
    ca_arn = resp["CertificateAuthorityArn"]

    delete_resp = acmpca_client.delete_certificate_authority(
        CertificateAuthorityArn=ca_arn
    )
    assert delete_resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    # Verify status is DELETED
    desc = acmpca_client.describe_certificate_authority(CertificateAuthorityArn=ca_arn)
    assert desc["CertificateAuthority"]["Status"] == "DELETED"


def test_get_csr(acmpca_client):
    resp = acmpca_client.create_certificate_authority(
        CertificateAuthorityType="ROOT",
        CertificateAuthorityConfiguration={
            "KeyAlgorithm": "RSA_2048",
            "SigningAlgorithm": "SHA256WITHRSA",
            "Subject": {"CommonName": "CSR CA"},
        },
    )
    ca_arn = resp["CertificateAuthorityArn"]

    csr_resp = acmpca_client.get_certificate_authority_csr(
        CertificateAuthorityArn=ca_arn
    )
    assert csr_resp["ResponseMetadata"]["HTTPStatusCode"] == 200
    assert "CERTIFICATE REQUEST" in csr_resp["Csr"]

    acmpca_client.delete_certificate_authority(CertificateAuthorityArn=ca_arn)
