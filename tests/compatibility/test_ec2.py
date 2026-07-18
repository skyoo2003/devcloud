import pytest
from botocore.exceptions import ClientError


def test_run_and_describe_instances(ec2_client):
    resp = ec2_client.run_instances(ImageId="ami-12345678", MinCount=1, MaxCount=1)
    assert len(resp["Instances"]) == 1
    instance_id = resp["Instances"][0]["InstanceId"]
    assert instance_id.startswith("i-")

    desc = ec2_client.describe_instances(InstanceIds=[instance_id])
    instances = [i for r in desc["Reservations"] for i in r["Instances"]]
    assert any(i["InstanceId"] == instance_id for i in instances)


def test_terminate_instances(ec2_client):
    resp = ec2_client.run_instances(ImageId="ami-abc", MinCount=1, MaxCount=1)
    instance_id = resp["Instances"][0]["InstanceId"]
    term = ec2_client.terminate_instances(InstanceIds=[instance_id])
    assert term["TerminatingInstances"][0]["CurrentState"]["Name"] == "terminated"


def test_vpc_lifecycle(ec2_client):
    resp = ec2_client.create_vpc(CidrBlock="10.1.0.0/16")
    vpc_id = resp["Vpc"]["VpcId"]
    assert vpc_id.startswith("vpc-")

    desc = ec2_client.describe_vpcs(VpcIds=[vpc_id])
    assert len(desc["Vpcs"]) >= 1

    subnet = ec2_client.create_subnet(VpcId=vpc_id, CidrBlock="10.1.1.0/24")
    assert subnet["Subnet"]["SubnetId"].startswith("subnet-")


def test_security_group(ec2_client):
    resp = ec2_client.create_security_group(GroupName="test-sg", Description="test")
    assert resp["GroupId"].startswith("sg-")
    desc = ec2_client.describe_security_groups()
    names = [sg["GroupName"] for sg in desc["SecurityGroups"]]
    assert "test-sg" in names


def test_terminate_nonexistent_instance(ec2_client):
    with pytest.raises(ClientError) as exc:
        ec2_client.terminate_instances(InstanceIds=["i-nonexistent000"])
    assert exc.value.response["Error"]["Code"] in (
        "InvalidInstanceID.NotFound",
        "InvalidInstanceID.Malformed",
    )


def test_create_tags(ec2_client):
    resp = ec2_client.run_instances(ImageId="ami-tag-test", MinCount=1, MaxCount=1)
    instance_id = resp["Instances"][0]["InstanceId"]
    ec2_client.create_tags(
        Resources=[instance_id], Tags=[{"Key": "Name", "Value": "test-instance"}]
    )
    desc = ec2_client.describe_instances(InstanceIds=[instance_id])
    tags = desc["Reservations"][0]["Instances"][0].get("Tags", [])
    tag_map = {t["Key"]: t["Value"] for t in tags}
    assert tag_map.get("Name") == "test-instance"


def test_describe_vpcs(ec2_client):
    ec2_client.create_vpc(CidrBlock="10.50.0.0/16")
    resp = ec2_client.describe_vpcs()
    assert len(resp["Vpcs"]) >= 1


def test_describe_subnets(ec2_client):
    vpc = ec2_client.create_vpc(CidrBlock="10.60.0.0/16")
    vpc_id = vpc["Vpc"]["VpcId"]
    ec2_client.create_subnet(VpcId=vpc_id, CidrBlock="10.60.1.0/24")
    resp = ec2_client.describe_subnets(Filters=[{"Name": "vpc-id", "Values": [vpc_id]}])
    assert len(resp["Subnets"]) >= 1


def test_allocate_address(ec2_client):
    resp = ec2_client.allocate_address(Domain="vpc")
    assert "AllocationId" in resp
    assert "PublicIp" in resp


def test_volume_lifecycle(ec2_client):
    resp = ec2_client.create_volume(Size=10, AvailabilityZone="us-east-1a")
    vid = resp["VolumeId"]
    assert vid.startswith("vol-")
    desc = ec2_client.describe_volumes(VolumeIds=[vid])
    assert len(desc["Volumes"]) == 1
    ec2_client.delete_volume(VolumeId=vid)


def test_snapshot(ec2_client):
    vol = ec2_client.create_volume(Size=5, AvailabilityZone="us-east-1a")
    snap = ec2_client.create_snapshot(VolumeId=vol["VolumeId"], Description="test")
    assert snap["SnapshotId"].startswith("snap-")
    desc = ec2_client.describe_snapshots(SnapshotIds=[snap["SnapshotId"]])
    assert len(desc["Snapshots"]) == 1
    ec2_client.delete_snapshot(SnapshotId=snap["SnapshotId"])


def test_keypair(ec2_client):
    resp = ec2_client.create_key_pair(KeyName="test-key")
    assert "KeyMaterial" in resp
    desc = ec2_client.describe_key_pairs(KeyNames=["test-key"])
    assert len(desc["KeyPairs"]) == 1
    ec2_client.delete_key_pair(KeyName="test-key")


def test_route_table(ec2_client):
    vpc = ec2_client.create_vpc(CidrBlock="10.0.0.0/16")
    rt = ec2_client.create_route_table(VpcId=vpc["Vpc"]["VpcId"])
    rtid = rt["RouteTable"]["RouteTableId"]
    assert rtid.startswith("rtb-")
    desc = ec2_client.describe_route_tables(RouteTableIds=[rtid])
    assert len(desc["RouteTables"]) == 1
    ec2_client.delete_route_table(RouteTableId=rtid)


def test_internet_gateway(ec2_client):
    vpc = ec2_client.create_vpc(CidrBlock="10.1.0.0/16")
    igw = ec2_client.create_internet_gateway()
    igwid = igw["InternetGateway"]["InternetGatewayId"]
    assert igwid.startswith("igw-")
    ec2_client.attach_internet_gateway(
        InternetGatewayId=igwid, VpcId=vpc["Vpc"]["VpcId"]
    )
    ec2_client.detach_internet_gateway(
        InternetGatewayId=igwid, VpcId=vpc["Vpc"]["VpcId"]
    )


def test_ami(ec2_client):
    resp = ec2_client.register_image(
        Name="test-ami",
        Architecture="x86_64",
        RootDeviceName="/dev/xvda",
    )
    assert resp["ImageId"].startswith("ami-")
    desc = ec2_client.describe_images(ImageIds=[resp["ImageId"]])
    assert len(desc["Images"]) == 1
    ec2_client.deregister_image(ImageId=resp["ImageId"])


def test_modify_vpc_attribute(ec2_client):
    vpc = ec2_client.create_vpc(CidrBlock="10.2.0.0/16")
    vpc_id = vpc["Vpc"]["VpcId"]
    ec2_client.modify_vpc_attribute(VpcId=vpc_id, EnableDnsHostnames={"Value": True})
    resp = ec2_client.describe_vpc_attribute(
        VpcId=vpc_id, Attribute="enableDnsHostnames"
    )
    assert resp["EnableDnsHostnames"]["Value"] is True


def test_network_acl(ec2_client):
    vpc = ec2_client.create_vpc(CidrBlock="10.3.0.0/16")
    acl = ec2_client.create_network_acl(VpcId=vpc["Vpc"]["VpcId"])
    aclid = acl["NetworkAcl"]["NetworkAclId"]
    assert aclid.startswith("acl-")
    ec2_client.create_network_acl_entry(
        NetworkAclId=aclid,
        RuleNumber=100,
        Protocol="-1",
        RuleAction="allow",
        CidrBlock="0.0.0.0/0",
        Egress=False,
    )
    ec2_client.delete_network_acl(NetworkAclId=aclid)


def test_instance_stop_start(ec2_client):
    iid = ec2_client.run_instances(ImageId="ami-life", MinCount=1, MaxCount=1)[
        "Instances"
    ][0]["InstanceId"]

    stopped = ec2_client.stop_instances(InstanceIds=[iid])
    assert stopped["StoppingInstances"][0]["CurrentState"]["Name"] == "stopped"
    assert stopped["StoppingInstances"][0]["PreviousState"]["Name"] == "running"

    started = ec2_client.start_instances(InstanceIds=[iid])
    assert started["StartingInstances"][0]["CurrentState"]["Name"] == "running"
    assert started["StartingInstances"][0]["PreviousState"]["Name"] == "stopped"

    # Reboot succeeds without changing reported state.
    ec2_client.reboot_instances(InstanceIds=[iid])


def test_reboot_nonexistent_instance(ec2_client):
    with pytest.raises(ClientError) as exc:
        ec2_client.reboot_instances(InstanceIds=["i-doesnotexist0"])
    assert exc.value.response["Error"]["Code"] in (
        "InvalidInstanceID.NotFound",
        "InvalidInstanceID.Malformed",
    )


def test_describe_availability_zones(ec2_client):
    resp = ec2_client.describe_availability_zones()
    zones = resp["AvailabilityZones"]
    assert len(zones) >= 3
    assert any(z["ZoneName"] == "us-east-1a" for z in zones)
    assert all(z["State"] == "available" for z in zones)


def test_describe_regions(ec2_client):
    resp = ec2_client.describe_regions()
    names = [r["RegionName"] for r in resp["Regions"]]
    assert "us-east-1" in names


def test_delete_subnet(ec2_client):
    vpc = ec2_client.create_vpc(CidrBlock="10.9.0.0/16")
    subnet_id = ec2_client.create_subnet(
        VpcId=vpc["Vpc"]["VpcId"], CidrBlock="10.9.1.0/24"
    )["Subnet"]["SubnetId"]
    ec2_client.delete_subnet(SubnetId=subnet_id)
    remaining = [s["SubnetId"] for s in ec2_client.describe_subnets()["Subnets"]]
    assert subnet_id not in remaining


def test_delete_security_group(ec2_client):
    gid = ec2_client.create_security_group(GroupName="del-sg", Description="d")[
        "GroupId"
    ]
    ec2_client.delete_security_group(GroupId=gid)
    ids = [
        sg["GroupId"] for sg in ec2_client.describe_security_groups()["SecurityGroups"]
    ]
    assert gid not in ids


def test_security_group_rules(ec2_client):
    gid = ec2_client.create_security_group(GroupName="rules-sg", Description="d")[
        "GroupId"
    ]
    ec2_client.authorize_security_group_ingress(
        GroupId=gid,
        IpPermissions=[
            {
                "IpProtocol": "tcp",
                "FromPort": 22,
                "ToPort": 22,
                "IpRanges": [{"CidrIp": "0.0.0.0/0"}],
            }
        ],
    )
    sg = [
        g
        for g in ec2_client.describe_security_groups(GroupIds=[gid])["SecurityGroups"]
        if g["GroupId"] == gid
    ][0]
    perms = sg["IpPermissions"]
    assert any(
        p["FromPort"] == 22 and p["IpRanges"][0]["CidrIp"] == "0.0.0.0/0" for p in perms
    )

    ec2_client.revoke_security_group_ingress(
        GroupId=gid,
        IpPermissions=[
            {
                "IpProtocol": "tcp",
                "FromPort": 22,
                "ToPort": 22,
                "IpRanges": [{"CidrIp": "0.0.0.0/0"}],
            }
        ],
    )
    sg2 = [
        g
        for g in ec2_client.describe_security_groups(GroupIds=[gid])["SecurityGroups"]
        if g["GroupId"] == gid
    ][0]
    assert not sg2.get("IpPermissions")


def test_elastic_ip_association(ec2_client):
    iid = ec2_client.run_instances(ImageId="ami-eip", MinCount=1, MaxCount=1)[
        "Instances"
    ][0]["InstanceId"]
    alloc = ec2_client.allocate_address(Domain="vpc")
    alloc_id = alloc["AllocationId"]

    assoc = ec2_client.associate_address(AllocationId=alloc_id, InstanceId=iid)
    assoc_id = assoc["AssociationId"]
    assert assoc_id.startswith("eipassoc-")

    addrs = ec2_client.describe_addresses()["Addresses"]
    mine = [a for a in addrs if a["AllocationId"] == alloc_id][0]
    assert mine["InstanceId"] == iid

    ec2_client.disassociate_address(AssociationId=assoc_id)
    ec2_client.release_address(AllocationId=alloc_id)
    remaining = [
        a["AllocationId"] for a in ec2_client.describe_addresses()["Addresses"]
    ]
    assert alloc_id not in remaining


def test_delete_tags(ec2_client):
    iid = ec2_client.run_instances(ImageId="ami-tagdel", MinCount=1, MaxCount=1)[
        "Instances"
    ][0]["InstanceId"]
    ec2_client.create_tags(Resources=[iid], Tags=[{"Key": "team", "Value": "x"}])
    ec2_client.delete_tags(Resources=[iid], Tags=[{"Key": "team"}])
    desc = ec2_client.describe_instances(InstanceIds=[iid])
    tags = desc["Reservations"][0]["Instances"][0].get("Tags", [])
    assert "team" not in {t["Key"] for t in tags}
