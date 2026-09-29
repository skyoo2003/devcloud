def test_create_and_get_resolver_endpoint(route53resolver_client, ec2_client):
    vpc = ec2_client.create_vpc(CidrBlock="10.80.0.0/16")
    vpc_id = vpc["Vpc"]["VpcId"]
    sub1 = ec2_client.create_subnet(VpcId=vpc_id, CidrBlock="10.80.1.0/24")
    sg = ec2_client.create_security_group(
        GroupName="resolver-sg", Description="test", VpcId=vpc_id
    )

    sub2 = ec2_client.create_subnet(VpcId=vpc_id, CidrBlock="10.80.2.0/24")

    resp = route53resolver_client.create_resolver_endpoint(
        CreatorRequestId="compat-ep-1",
        SecurityGroupIds=[sg["GroupId"]],
        Direction="INBOUND",
        IpAddresses=[
            {"SubnetId": sub1["Subnet"]["SubnetId"]},
            {"SubnetId": sub2["Subnet"]["SubnetId"]},
        ],
    )
    ep_id = resp["ResolverEndpoint"]["Id"]
    assert resp["ResolverEndpoint"]["Direction"] == "INBOUND"

    get = route53resolver_client.get_resolver_endpoint(ResolverEndpointId=ep_id)
    assert get["ResolverEndpoint"]["Id"] == ep_id


def test_list_resolver_endpoints(route53resolver_client):
    resp = route53resolver_client.list_resolver_endpoints()
    assert "ResolverEndpoints" in resp


def test_create_and_get_resolver_rule(route53resolver_client):
    resp = route53resolver_client.create_resolver_rule(
        CreatorRequestId="compat-rule-1",
        RuleType="FORWARD",
        DomainName="example.com",
        TargetIps=[{"Ip": "10.0.0.1", "Port": 53}],
    )
    rule_id = resp["ResolverRule"]["Id"]
    assert resp["ResolverRule"]["DomainName"] == "example.com."

    get = route53resolver_client.get_resolver_rule(ResolverRuleId=rule_id)
    assert get["ResolverRule"]["Id"] == rule_id


def test_list_resolver_rules(route53resolver_client):
    resp = route53resolver_client.list_resolver_rules()
    assert "ResolverRules" in resp


def test_delete_resolver_rule(route53resolver_client):
    resp = route53resolver_client.create_resolver_rule(
        CreatorRequestId="del-rule-1",
        RuleType="FORWARD",
        DomainName="del.example.com",
        TargetIps=[{"Ip": "10.0.0.1", "Port": 53}],
    )
    rule_id = resp["ResolverRule"]["Id"]
    route53resolver_client.delete_resolver_rule(ResolverRuleId=rule_id)
