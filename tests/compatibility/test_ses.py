def test_verify_email_identity(ses_client):
    ses_client.verify_email_identity(EmailAddress="test@example.com")
    resp = ses_client.list_identities(IdentityType="EmailAddress")
    assert "test@example.com" in resp["Identities"]


def test_send_email(ses_client):
    ses_client.verify_email_identity(EmailAddress="sender@example.com")
    resp = ses_client.send_email(
        Source="sender@example.com",
        Destination={"ToAddresses": ["to@example.com"]},
        Message={
            "Subject": {"Data": "Test Subject"},
            "Body": {"Text": {"Data": "Test body"}},
        },
    )
    assert "MessageId" in resp


def test_delete_identity(ses_client):
    ses_client.verify_email_identity(EmailAddress="del@example.com")
    ses_client.delete_identity(Identity="del@example.com")
    resp = ses_client.list_identities(IdentityType="EmailAddress")
    assert "del@example.com" not in resp["Identities"]


def test_get_send_quota(ses_client):
    resp = ses_client.get_send_quota()
    assert "Max24HourSend" in resp
    assert "SentLast24Hours" in resp
