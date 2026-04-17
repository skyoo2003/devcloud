import pytest
from botocore.exceptions import ClientError


def test_create_and_describe_load_balancer(elbv2_client, ec2_client):
    vpc = ec2_client.create_vpc(CidrBlock="10.70.0.0/16")
    vpc_id = vpc["Vpc"]["VpcId"]
    sub1 = ec2_client.create_subnet(VpcId=vpc_id, CidrBlock="10.70.1.0/24")
    sub2 = ec2_client.create_subnet(VpcId=vpc_id, CidrBlock="10.70.2.0/24")

    resp = elbv2_client.create_load_balancer(
        Name="compat-alb",
        Subnets=[sub1["Subnet"]["SubnetId"], sub2["Subnet"]["SubnetId"]],
        Type="application",
    )
    assert len(resp["LoadBalancers"]) == 1
    assert resp["LoadBalancers"][0]["LoadBalancerName"] == "compat-alb"
    lb_arn = resp["LoadBalancers"][0]["LoadBalancerArn"]

    desc = elbv2_client.describe_load_balancers(LoadBalancerArns=[lb_arn])
    assert len(desc["LoadBalancers"]) == 1

    elbv2_client.delete_load_balancer(LoadBalancerArn=lb_arn)


def test_create_and_describe_target_group(elbv2_client, ec2_client):
    vpc = ec2_client.create_vpc(CidrBlock="10.71.0.0/16")
    vpc_id = vpc["Vpc"]["VpcId"]

    resp = elbv2_client.create_target_group(
        Name="compat-tg",
        Protocol="HTTP",
        Port=80,
        VpcId=vpc_id,
        TargetType="instance",
    )
    assert resp["TargetGroups"][0]["TargetGroupName"] == "compat-tg"
    tg_arn = resp["TargetGroups"][0]["TargetGroupArn"]

    desc = elbv2_client.describe_target_groups(TargetGroupArns=[tg_arn])
    assert len(desc["TargetGroups"]) == 1

    elbv2_client.delete_target_group(TargetGroupArn=tg_arn)


def test_describe_nonexistent_lb(elbv2_client):
    with pytest.raises(ClientError) as exc:
        elbv2_client.describe_load_balancers(
            LoadBalancerArns=[
                "arn:aws:elasticloadbalancing:us-east-1:000000000000:loadbalancer/app/no-such/0000"
            ]
        )
    assert exc.value.response["Error"]["Code"] == "LoadBalancerNotFound"
