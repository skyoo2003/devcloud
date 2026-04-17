import pytest
from botocore.exceptions import ClientError


def test_create_and_get_email_identity(sesv2_client):
    resp = sesv2_client.create_email_identity(EmailIdentity="test@example.com")
    assert "IdentityType" in resp

    get = sesv2_client.get_email_identity(EmailIdentity="test@example.com")
    assert get["IdentityType"] == "EMAIL_ADDRESS"


def test_list_email_identities(sesv2_client):
    sesv2_client.create_email_identity(EmailIdentity="list@example.com")
    resp = sesv2_client.list_email_identities()
    identities = [i["IdentityName"] for i in resp["EmailIdentities"]]
    assert "list@example.com" in identities


def test_delete_email_identity(sesv2_client):
    sesv2_client.create_email_identity(EmailIdentity="del@example.com")
    sesv2_client.delete_email_identity(EmailIdentity="del@example.com")


def test_create_and_get_contact_list(sesv2_client):
    sesv2_client.create_contact_list(ContactListName="compat-contacts")
    resp = sesv2_client.get_contact_list(ContactListName="compat-contacts")
    assert resp["ContactListName"] == "compat-contacts"


def test_list_contact_lists(sesv2_client):
    sesv2_client.create_contact_list(ContactListName="list-contacts")
    resp = sesv2_client.list_contact_lists()
    names = [c["ContactListName"] for c in resp["ContactLists"]]
    assert "list-contacts" in names


def test_delete_contact_list(sesv2_client):
    sesv2_client.create_contact_list(ContactListName="del-contacts")
    sesv2_client.delete_contact_list(ContactListName="del-contacts")


def test_send_email(sesv2_client):
    sesv2_client.create_email_identity(EmailIdentity="sender@example.com")
    resp = sesv2_client.send_email(
        FromEmailAddress="sender@example.com",
        Destination={"ToAddresses": ["to@example.com"]},
        Content={
            "Simple": {
                "Subject": {"Data": "Test Subject"},
                "Body": {"Text": {"Data": "Test body"}},
            }
        },
    )
    assert "MessageId" in resp


def test_get_nonexistent_identity(sesv2_client):
    with pytest.raises(ClientError) as exc:
        sesv2_client.get_email_identity(EmailIdentity="no-such@identity.com")
    assert exc.value.response["Error"]["Code"] == "NotFoundException"
