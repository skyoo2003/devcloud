def test_create_and_describe_compute_environment(batch_client):
    resp = batch_client.create_compute_environment(
        computeEnvironmentName="compat-ce",
        type="MANAGED",
        computeResources={
            "type": "EC2",
            "minvCpus": 0,
            "maxvCpus": 256,
            "instanceTypes": ["m5.xlarge"],
            "subnets": ["subnet-1"],
            "securityGroupIds": ["sg-1"],
        },
    )
    assert resp["computeEnvironmentName"] == "compat-ce"

    desc = batch_client.describe_compute_environments(computeEnvironments=["compat-ce"])
    assert len(desc["computeEnvironments"]) == 1
    assert desc["computeEnvironments"][0]["computeEnvironmentName"] == "compat-ce"


def test_register_and_describe_job_definition(batch_client):
    resp = batch_client.register_job_definition(
        jobDefinitionName="compat-jobdef",
        type="container",
        containerProperties={
            "image": "busybox",
            "vcpus": 1,
            "memory": 512,
        },
    )
    assert resp["jobDefinitionName"] == "compat-jobdef"

    desc = batch_client.describe_job_definitions(jobDefinitionName="compat-jobdef")
    assert len(desc["jobDefinitions"]) >= 1


def test_create_and_describe_job_queue(batch_client):
    batch_client.create_compute_environment(
        computeEnvironmentName="jq-ce",
        type="MANAGED",
        computeResources={
            "type": "EC2",
            "minvCpus": 0,
            "maxvCpus": 256,
            "instanceTypes": ["m5.xlarge"],
            "subnets": ["subnet-1"],
            "securityGroupIds": ["sg-1"],
        },
    )
    resp = batch_client.create_job_queue(
        jobQueueName="compat-jq",
        priority=1,
        computeEnvironmentOrder=[
            {"order": 1, "computeEnvironment": "jq-ce"},
        ],
    )
    assert resp["jobQueueName"] == "compat-jq"

    desc = batch_client.describe_job_queues(jobQueues=["compat-jq"])
    assert len(desc["jobQueues"]) == 1


def test_delete_compute_environment(batch_client):
    batch_client.create_compute_environment(
        computeEnvironmentName="del-ce",
        type="MANAGED",
        computeResources={
            "type": "EC2",
            "minvCpus": 0,
            "maxvCpus": 256,
            "instanceTypes": ["m5.xlarge"],
            "subnets": ["subnet-1"],
            "securityGroupIds": ["sg-1"],
        },
    )
    batch_client.delete_compute_environment(computeEnvironment="del-ce")
