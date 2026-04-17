def test_create_and_get_network(managedblockchain_client):
    resp = managedblockchain_client.create_network(
        Name="compat-net",
        Framework="HYPERLEDGER_FABRIC",
        FrameworkVersion="2.2",
        VotingPolicy={
            "ApprovalThresholdPolicy": {
                "ThresholdPercentage": 50,
                "ProposalDurationInHours": 24,
                "ThresholdComparator": "GREATER_THAN",
            }
        },
        MemberConfiguration={
            "Name": "member-1",
            "FrameworkConfiguration": {
                "Fabric": {"AdminUsername": "admin", "AdminPassword": "Password123"}
            },
        },
    )
    network_id = resp["NetworkId"]
    assert network_id

    desc = managedblockchain_client.get_network(NetworkId=network_id)
    assert desc["Network"]["Name"] == "compat-net"


def test_list_networks(managedblockchain_client):
    managedblockchain_client.create_network(
        Name="list-net-1",
        Framework="HYPERLEDGER_FABRIC",
        FrameworkVersion="2.2",
        VotingPolicy={
            "ApprovalThresholdPolicy": {
                "ThresholdPercentage": 50,
                "ProposalDurationInHours": 24,
                "ThresholdComparator": "GREATER_THAN",
            }
        },
        MemberConfiguration={
            "Name": "member-1",
            "FrameworkConfiguration": {
                "Fabric": {"AdminUsername": "admin", "AdminPassword": "Password123"}
            },
        },
    )
    resp = managedblockchain_client.list_networks()
    assert "Networks" in resp
    assert len(resp["Networks"]) >= 1
