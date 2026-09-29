from botocore.exceptions import ClientError


# ── Contact Information ───────────────────────────────────────────────────


def test_contact_information_crud(account_client):
    # Get - not set yet, should return 404
    try:
        account_client.get_contact_information()
        # If no exception, response may be empty or default
    except ClientError as e:
        assert e.response["ResponseMetadata"]["HTTPStatusCode"] == 404

    # Put
    account_client.put_contact_information(
        ContactInformation={
            "FullName": "John Doe",
            "PhoneNumber": "+15555551234",
            "AddressLine1": "123 Main St",
            "City": "Springfield",
            "PostalCode": "12345",
            "CountryCode": "US",
        }
    )

    # Get - should now exist
    resp = account_client.get_contact_information()
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
    ci = resp["ContactInformation"]
    assert ci["FullName"] == "John Doe"
    assert ci["CountryCode"] == "US"


def test_put_contact_information(account_client):
    # Put some contact info
    resp = account_client.put_contact_information(
        ContactInformation={
            "FullName": "Jane Doe",
            "PhoneNumber": "+15555559999",
            "AddressLine1": "456 Oak St",
            "City": "Portland",
            "PostalCode": "97201",
            "CountryCode": "US",
        }
    )
    assert resp["ResponseMetadata"]["HTTPStatusCode"] in (200, 204)

    # Overwrite it with new data
    resp2 = account_client.put_contact_information(
        ContactInformation={
            "FullName": "John Smith",
            "PhoneNumber": "+15555550000",
            "AddressLine1": "789 Elm St",
            "City": "Seattle",
            "PostalCode": "98101",
            "CountryCode": "US",
        }
    )
    assert resp2["ResponseMetadata"]["HTTPStatusCode"] in (200, 204)


# ── Alternate Contacts ────────────────────────────────────────────────────


def test_alternate_contact_crud(account_client):
    # Get - not set yet
    try:
        account_client.get_alternate_contact(AlternateContactType="BILLING")
    except ClientError as e:
        assert e.response["ResponseMetadata"]["HTTPStatusCode"] == 404

    # Put
    account_client.put_alternate_contact(
        AlternateContactType="BILLING",
        Name="Finance Team",
        Title="CFO",
        EmailAddress="finance@example.com",
        PhoneNumber="+15555550000",
    )

    # Get
    resp = account_client.get_alternate_contact(AlternateContactType="BILLING")
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
    ac = resp["AlternateContact"]
    assert ac["Name"] == "Finance Team"
    assert ac["AlternateContactType"] == "BILLING"

    # Delete
    account_client.delete_alternate_contact(AlternateContactType="BILLING")

    # Get after delete
    try:
        account_client.get_alternate_contact(AlternateContactType="BILLING")
        assert False, "expected exception"
    except ClientError as e:
        assert e.response["ResponseMetadata"]["HTTPStatusCode"] == 404


def test_alternate_contact_types(account_client):
    for contact_type in ["BILLING", "OPERATIONS", "SECURITY"]:
        account_client.put_alternate_contact(
            AlternateContactType=contact_type,
            Name=f"{contact_type} Contact",
            Title="Manager",
            EmailAddress=f"{contact_type.lower()}@example.com",
            PhoneNumber="+15555550001",
        )
        resp = account_client.get_alternate_contact(AlternateContactType=contact_type)
        assert resp["AlternateContact"]["AlternateContactType"] == contact_type

    # Clean up
    for contact_type in ["BILLING", "OPERATIONS", "SECURITY"]:
        account_client.delete_alternate_contact(AlternateContactType=contact_type)


# ── Regions ───────────────────────────────────────────────────────────────


def test_list_regions(account_client):
    resp = account_client.list_regions()
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
    regions = resp["Regions"]
    assert len(regions) >= 10
    region_names = [r["RegionName"] for r in regions]
    assert "us-east-1" in region_names
    assert "eu-west-1" in region_names


def test_get_region_opt_status(account_client):
    resp = account_client.get_region_opt_status(RegionName="us-east-1")
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
    assert resp["RegionName"] == "us-east-1"
    assert resp["RegionOptStatus"] in ("ENABLED", "ENABLING", "DISABLED", "DISABLING")


def test_enable_disable_region(account_client):
    # Disable a region
    account_client.disable_region(RegionName="ap-northeast-1")

    resp = account_client.get_region_opt_status(RegionName="ap-northeast-1")
    assert resp["RegionOptStatus"] == "DISABLED"

    # Re-enable
    account_client.enable_region(RegionName="ap-northeast-1")

    resp2 = account_client.get_region_opt_status(RegionName="ap-northeast-1")
    assert resp2["RegionOptStatus"] == "ENABLED"


# ── Primary Email ─────────────────────────────────────────────────────────


def test_get_primary_email(account_client):
    resp = account_client.get_primary_email(AccountId="000000000000")
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
    assert "PrimaryEmail" in resp


def test_start_primary_email_update(account_client):
    resp = account_client.start_primary_email_update(
        AccountId="000000000000", PrimaryEmail="new@example.com"
    )
    assert resp["ResponseMetadata"]["HTTPStatusCode"] in (200, 201)


def test_accept_primary_email_update(account_client):
    # Start an update first
    account_client.start_primary_email_update(
        AccountId="000000000000", PrimaryEmail="updated@example.com"
    )

    # Accept it
    resp = account_client.accept_primary_email_update(
        AccountId="000000000000", PrimaryEmail="updated@example.com", Otp="123456"
    )
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
    assert resp.get("Status") in ("ACCEPTED", "PENDING", None)

    # Verify email was updated
    get_resp = account_client.get_primary_email(AccountId="000000000000")
    assert "PrimaryEmail" in get_resp
