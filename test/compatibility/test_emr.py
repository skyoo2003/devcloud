def test_run_job_flow_and_describe(emr_client):
    resp = emr_client.run_job_flow(
        Name="compat-cluster",
        ReleaseLabel="emr-7.0.0",
        Instances={
            "MasterInstanceType": "m5.xlarge",
            "InstanceCount": 1,
            "KeepJobFlowAliveWhenNoSteps": True,
        },
    )
    cluster_id = resp["JobFlowId"]
    assert cluster_id.startswith("j-")

    desc = emr_client.describe_cluster(ClusterId=cluster_id)
    assert desc["Cluster"]["Name"] == "compat-cluster"


def test_list_clusters(emr_client):
    emr_client.run_job_flow(
        Name="list-cluster",
        ReleaseLabel="emr-7.0.0",
        Instances={"MasterInstanceType": "m5.xlarge", "InstanceCount": 1},
    )
    resp = emr_client.list_clusters()
    assert "Clusters" in resp
    assert len(resp["Clusters"]) >= 1


def test_terminate_job_flows(emr_client):
    resp = emr_client.run_job_flow(
        Name="term-cluster",
        ReleaseLabel="emr-7.0.0",
        Instances={"MasterInstanceType": "m5.xlarge", "InstanceCount": 1},
    )
    cluster_id = resp["JobFlowId"]
    emr_client.terminate_job_flows(JobFlowIds=[cluster_id])

    desc = emr_client.describe_cluster(ClusterId=cluster_id)
    assert desc["Cluster"]["Status"]["State"] == "TERMINATED"


def test_add_and_list_steps(emr_client):
    resp = emr_client.run_job_flow(
        Name="step-cluster",
        ReleaseLabel="emr-7.0.0",
        Instances={
            "MasterInstanceType": "m5.xlarge",
            "InstanceCount": 1,
            "KeepJobFlowAliveWhenNoSteps": True,
        },
    )
    cluster_id = resp["JobFlowId"]

    step_resp = emr_client.add_job_flow_steps(
        JobFlowId=cluster_id,
        Steps=[
            {
                "Name": "test-step",
                "ActionOnFailure": "CONTINUE",
                "HadoopJarStep": {
                    "Jar": "command-runner.jar",
                    "Args": ["echo", "hello"],
                },
            }
        ],
    )
    assert len(step_resp["StepIds"]) == 1
    step_id = step_resp["StepIds"][0]

    steps = emr_client.list_steps(ClusterId=cluster_id)
    assert len(steps["Steps"]) >= 1

    step_desc = emr_client.describe_step(ClusterId=cluster_id, StepId=step_id)
    assert step_desc["Step"]["Name"] == "test-step"

    emr_client.terminate_job_flows(JobFlowIds=[cluster_id])


def test_security_configuration(emr_client):
    resp = emr_client.create_security_configuration(
        Name="test-sec-config",
        SecurityConfiguration='{"EncryptionConfiguration": {}}',
    )
    assert resp["Name"] == "test-sec-config"

    desc = emr_client.describe_security_configuration(Name="test-sec-config")
    assert desc["Name"] == "test-sec-config"

    configs = emr_client.list_security_configurations()
    names = [c["Name"] for c in configs["SecurityConfigurations"]]
    assert "test-sec-config" in names

    emr_client.delete_security_configuration(Name="test-sec-config")


def test_studio_lifecycle(emr_client):
    resp = emr_client.create_studio(
        Name="test-studio",
        AuthMode="IAM",
        VpcId="vpc-12345",
        SubnetIds=["subnet-12345"],
        ServiceRole="arn:aws:iam::000000000000:role/emr-studio",
        WorkspaceSecurityGroupId="sg-12345",
        EngineSecurityGroupId="sg-67890",
        DefaultS3Location="s3://my-emr-studio-bucket/",
    )
    studio_id = resp["StudioId"]
    assert studio_id

    desc = emr_client.describe_studio(StudioId=studio_id)
    assert desc["Studio"]["Name"] == "test-studio"

    studios = emr_client.list_studios()
    ids = [s["StudioId"] for s in studios["Studios"]]
    assert studio_id in ids

    emr_client.delete_studio(StudioId=studio_id)
