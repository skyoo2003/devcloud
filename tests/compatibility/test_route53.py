import pytest
import botocore
from botocore.exceptions import ClientError


def _tpi_create_kwargs(zone_id, tp_id):
    """Return kwargs matching the installed botocore model for CreateTrafficPolicyInstance."""
    model = (
        botocore.session.get_session()
        .create_client("route53", region_name="us-east-1")
        ._service_model.operation_model("CreateTrafficPolicyInstance")
    )
    members = list(model.input_shape.members.keys())
    base = {"TrafficPolicyId": tp_id, "TrafficPolicyVersion": 1, "TTL": 300}
    if "HostedZoneId" in members:
        base["HostedZoneId"] = zone_id
    else:
        base["Id"] = zone_id
    return base


def _tpi_update_kwargs(tpi_id, tp_id):
    """Return kwargs matching the installed botocore model for UpdateTrafficPolicyInstance."""
    model = (
        botocore.session.get_session()
        .create_client("route53", region_name="us-east-1")
        ._service_model.operation_model("UpdateTrafficPolicyInstance")
    )
    members = list(model.input_shape.members.keys())
    base = {
        "Id": tpi_id,
        "TrafficPolicyId": tp_id,
        "TrafficPolicyVersion": 1,
        "TTL": 600,
    }
    if "Name" in members:
        base["Name"] = "updated-instance"
    return base


def test_create_list_delete_hosted_zone(route53_client):
    resp = route53_client.create_hosted_zone(
        Name="boto3test.com",
        CallerReference="ref-boto3-1",
        HostedZoneConfig={"Comment": "boto3 test", "PrivateZone": False},
    )
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 201
    zone_id = resp["HostedZone"]["Id"].split("/")[-1]

    zones = route53_client.list_hosted_zones()
    ids = [z["Id"].split("/")[-1] for z in zones["HostedZones"]]
    assert zone_id in ids

    get_resp = route53_client.get_hosted_zone(Id=zone_id)
    assert get_resp["HostedZone"]["Name"] in ("boto3test.com", "boto3test.com.")

    route53_client.delete_hosted_zone(Id=zone_id)

    zones2 = route53_client.list_hosted_zones()
    ids2 = [z["Id"].split("/")[-1] for z in zones2["HostedZones"]]
    assert zone_id not in ids2


def test_change_and_list_record_sets(route53_client):
    resp = route53_client.create_hosted_zone(
        Name="records.example.com",
        CallerReference="ref-boto3-2",
    )
    zone_id = resp["HostedZone"]["Id"].split("/")[-1]

    route53_client.change_resource_record_sets(
        HostedZoneId=zone_id,
        ChangeBatch={
            "Changes": [
                {
                    "Action": "UPSERT",
                    "ResourceRecordSet": {
                        "Name": "www.records.example.com",
                        "Type": "A",
                        "TTL": 300,
                        "ResourceRecords": [{"Value": "1.2.3.4"}],
                    },
                }
            ]
        },
    )

    rrsets = route53_client.list_resource_record_sets(HostedZoneId=zone_id)
    names = [r["Name"] for r in rrsets["ResourceRecordSets"]]
    assert any("www" in n for n in names)

    # cleanup
    route53_client.change_resource_record_sets(
        HostedZoneId=zone_id,
        ChangeBatch={
            "Changes": [
                {
                    "Action": "DELETE",
                    "ResourceRecordSet": {
                        "Name": "www.records.example.com.",
                        "Type": "A",
                        "TTL": 300,
                        "ResourceRecords": [{"Value": "1.2.3.4"}],
                    },
                }
            ]
        },
    )
    route53_client.delete_hosted_zone(Id=zone_id)


def test_get_nonexistent_hosted_zone(route53_client):
    with pytest.raises(ClientError) as exc:
        route53_client.get_hosted_zone(Id="ZNONEXISTENT")
    assert exc.value.response["Error"]["Code"] == "NoSuchHostedZone"


def test_change_tags_for_resource(route53_client):
    resp = route53_client.create_hosted_zone(
        Name="tags.r53test.com", CallerReference="ref-tags-1"
    )
    zone_id = resp["HostedZone"]["Id"].split("/")[-1]
    route53_client.change_tags_for_resource(
        ResourceType="hostedzone",
        ResourceId=zone_id,
        AddTags=[{"Key": "env", "Value": "test"}],
    )
    tags = route53_client.list_tags_for_resource(
        ResourceType="hostedzone", ResourceId=zone_id
    )
    tag_map = {t["Key"]: t["Value"] for t in tags["ResourceTagSet"]["Tags"]}
    assert tag_map.get("env") == "test"
    route53_client.delete_hosted_zone(Id=zone_id)


def test_health_check(route53_client):
    resp = route53_client.create_health_check(
        CallerReference="test-hc-1",
        HealthCheckConfig={
            "IPAddress": "192.0.2.1",
            "Port": 80,
            "Type": "HTTP",
            "ResourcePath": "/health",
        },
    )
    hc_id = resp["HealthCheck"]["Id"]
    got = route53_client.get_health_check(HealthCheckId=hc_id)
    assert got["HealthCheck"]["Id"] == hc_id
    listing = route53_client.list_health_checks()
    assert any(h["Id"] == hc_id for h in listing["HealthChecks"])
    route53_client.delete_health_check(HealthCheckId=hc_id)


def test_traffic_policy(route53_client):
    resp = route53_client.create_traffic_policy(
        Name="test-tp",
        Document='{"AWSPolicyFormatVersion":"2015-10-01","RecordType":"A","Endpoints":{}}',
    )
    tp_id = resp["TrafficPolicy"]["Id"]
    route53_client.delete_traffic_policy(Id=tp_id, Version=1)


def test_query_logging(route53_client):
    hz = route53_client.create_hosted_zone(
        Name="example.com.", CallerReference="qlog-1"
    )
    resp = route53_client.create_query_logging_config(
        HostedZoneId=hz["HostedZone"]["Id"],
        CloudWatchLogsLogGroupArn="arn:aws:logs:us-east-1:000000000000:log-group:dns-queries",
    )
    config_id = resp["QueryLoggingConfig"]["Id"]
    route53_client.delete_query_logging_config(Id=config_id)


def test_dnssec(route53_client):
    hz = route53_client.create_hosted_zone(
        Name="dnssec-test.com.", CallerReference="dnssec-1"
    )
    zone_id = hz["HostedZone"]["Id"].split("/")[-1]
    resp = route53_client.get_dnssec(HostedZoneId=zone_id)
    assert "Status" in resp

    # Enable DNSSEC
    resp = route53_client.enable_hosted_zone_dnssec(HostedZoneId=zone_id)
    assert "ChangeInfo" in resp
    assert resp["ChangeInfo"]["Status"] == "INSYNC"

    # Verify it's enabled
    resp = route53_client.get_dnssec(HostedZoneId=zone_id)
    assert resp["Status"]["ServeSignature"] in ("Enabled", "ENABLED")

    # Disable DNSSEC
    resp = route53_client.disable_hosted_zone_dnssec(HostedZoneId=zone_id)
    assert "ChangeInfo" in resp

    # Verify it's disabled
    resp = route53_client.get_dnssec(HostedZoneId=zone_id)
    assert resp["Status"]["ServeSignature"] == "NOT_SIGNING"

    route53_client.delete_hosted_zone(Id=zone_id)


def test_key_signing_key(route53_client):
    hz = route53_client.create_hosted_zone(
        Name="ksk-test.com.", CallerReference="ksk-1"
    )
    zone_id = hz["HostedZone"]["Id"].split("/")[-1]

    # Create key signing key
    resp = route53_client.create_key_signing_key(
        Name="my-ksk",
        HostedZoneId=zone_id,
        Use="SigningOnly",
        Algorithm="ECDSA_P256_SHA256",
        KeySpec="ECDSA_P256",
    )
    assert resp["KeySigningKey"]["Name"] == "my-ksk"
    assert resp["KeySigningKey"]["State"] == "Pending"
    ksk_name = resp["KeySigningKey"]["Name"]

    # List key signing keys
    listing = route53_client.list_key_signing_keys()
    assert "KeySigningKeys" in listing

    # Activate key signing key
    resp = route53_client.activate_key_signing_key(HostedZoneId=zone_id, Name=ksk_name)
    assert resp["KeySigningKey"]["Name"] == ksk_name

    # Deactivate key signing key
    resp = route53_client.deactivate_key_signing_key(
        HostedZoneId=zone_id, Name=ksk_name
    )
    assert "KeySigningKey" in resp

    # Delete key signing key
    route53_client.delete_key_signing_key(HostedZoneId=zone_id, Name=ksk_name)

    route53_client.delete_hosted_zone(Id=zone_id)


def test_traffic_policy_instance(route53_client):
    hz = route53_client.create_hosted_zone(
        Name="tpi-test.com.", CallerReference="tpi-1"
    )
    zone_id = hz["HostedZone"]["Id"].split("/")[-1]

    # Create traffic policy first
    tp_resp = route53_client.create_traffic_policy(
        Name="test-tp",
        Document='{"AWSPolicyFormatVersion":"2015-10-01","RecordType":"A","Endpoints":{}}',
    )
    tp_id = tp_resp["TrafficPolicy"]["Id"]

    # Create traffic policy instance
    resp = route53_client.create_traffic_policy_instance(
        **_tpi_create_kwargs(zone_id, tp_id),
    )
    assert resp["TrafficPolicyInstance"]["State"] == "Applied"
    tpi_id = resp["TrafficPolicyInstance"]["Id"]

    # Get traffic policy instance
    get_resp = route53_client.get_traffic_policy_instance(Id=tpi_id)
    assert get_resp["TrafficPolicyInstance"]["Id"] == tpi_id
    assert get_resp["TrafficPolicyInstance"]["TrafficPolicyId"] == tp_id

    # List traffic policy instances
    listing = route53_client.list_traffic_policy_instances()
    assert "TrafficPolicyInstances" in listing

    # Update traffic policy instance
    route53_client.update_traffic_policy_instance(
        **_tpi_update_kwargs(tpi_id, tp_id),
    )

    # Delete traffic policy instance
    route53_client.delete_traffic_policy_instance(Id=tpi_id)

    # Verify deletion
    with pytest.raises(ClientError) as exc:
        route53_client.get_traffic_policy_instance(Id=tpi_id)
    assert exc.value.response["Error"]["Code"] == "NoSuchTrafficPolicyInstance"

    route53_client.delete_hosted_zone(Id=zone_id)


def test_cidr_collection(route53_client):
    # Create CIDR collection
    resp = route53_client.create_cidr_collection(
        Name="my-cidr-collection",
        CallerReference="cidr-ref-1",
    )
    assert resp["CidrCollection"]["Name"] == "my-cidr-collection"
    assert resp["CidrCollection"]["State"] == "Created"
    cidr_id = resp["CidrCollection"]["CidrCollectionId"]

    # List CIDR collections
    listing = route53_client.list_cidr_collections()
    assert "CidrCollections" in listing
    assert any(c["CidrCollectionId"] == cidr_id for c in listing["CidrCollections"])

    # List CIDR blocks
    blocks = route53_client.list_cidr_blocks(CidrCollectionId=cidr_id)
    assert "CidrBlocks" in blocks

    # List CIDR locations
    locations = route53_client.list_cidr_locations(CidrCollectionId=cidr_id)
    assert "Locations" in locations

    # Delete CIDR collection
    route53_client.delete_cidr_collection(CidrCollectionId=cidr_id)

    # Verify deletion
    listing = route53_client.list_cidr_collections()
    assert not any(c["CidrCollectionId"] == cidr_id for c in listing["CidrCollections"])


def test_reusable_delegation_set(route53_client):
    # Create reusable delegation set
    resp = route53_client.create_reusable_delegation_set(
        CallerReference="rds-ref-1",
    )
    assert resp["DelegationSet"]["DelegationSetId"]
    ds_id = resp["DelegationSet"]["DelegationSetId"]

    # Get reusable delegation set
    get_resp = route53_client.get_reusable_delegation_set(DelegationSetId=ds_id)
    assert get_resp["DelegationSet"]["DelegationSetId"] == ds_id
    assert "NameServers" in get_resp["DelegationSet"]

    # List reusable delegation sets
    listing = route53_client.list_reusable_delegation_sets()
    assert "ReusableDelegationSets" in listing
    assert any(d["DelegationSetId"] == ds_id for d in listing["ReusableDelegationSets"])

    # Delete reusable delegation set
    route53_client.delete_reusable_delegation_set(DelegationSetId=ds_id)

    # Verify deletion
    listing = route53_client.list_reusable_delegation_sets()
    assert not any(
        d["DelegationSetId"] == ds_id for d in listing["ReusableDelegationSets"]
    )
