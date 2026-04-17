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
