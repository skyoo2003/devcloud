def test_cluster_lifecycle(ecs_client):
    resp = ecs_client.create_cluster(clusterName="test-cluster")
    assert resp["cluster"]["clusterName"] == "test-cluster"
    assert resp["cluster"]["status"] == "ACTIVE"

    arns = ecs_client.list_clusters()["clusterArns"]
    assert any("test-cluster" in a for a in arns)


def test_task_definition(ecs_client):
    resp = ecs_client.register_task_definition(
        family="my-task",
        containerDefinitions=[{"name": "web", "image": "nginx:latest", "memory": 128}],
    )
    td = resp["taskDefinition"]
    assert td["family"] == "my-task"
    assert td["revision"] == 1
    assert td["taskDefinitionArn"].endswith(":1")


def test_run_and_stop_task(ecs_client):
    cluster = ecs_client.create_cluster(clusterName="run-cluster")["cluster"][
        "clusterArn"
    ]
    td = ecs_client.register_task_definition(
        family="run-task",
        containerDefinitions=[{"name": "app", "image": "alpine", "memory": 64}],
    )["taskDefinition"]["taskDefinitionArn"]

    run = ecs_client.run_task(cluster=cluster, taskDefinition=td)
    task_arn = run["tasks"][0]["taskArn"]
    assert run["tasks"][0]["lastStatus"] == "RUNNING"

    stop = ecs_client.stop_task(cluster=cluster, task=task_arn)
    assert stop["task"]["lastStatus"] == "STOPPED"


def test_service_crud(ecs_client):
    cluster = ecs_client.create_cluster(clusterName="svc-cluster")["cluster"][
        "clusterArn"
    ]
    td = ecs_client.register_task_definition(
        family="svc-task",
        containerDefinitions=[{"name": "app", "image": "alpine", "memory": 64}],
    )["taskDefinition"]["taskDefinitionArn"]

    svc = ecs_client.create_service(
        cluster=cluster,
        serviceName="my-svc",
        taskDefinition=td,
        desiredCount=1,
    )["service"]
    assert svc["serviceName"] == "my-svc"

    arns = ecs_client.list_services(cluster=cluster)["serviceArns"]
    assert len(arns) == 1

    ecs_client.delete_service(cluster=cluster, service=svc["serviceArn"])


def test_describe_nonexistent_cluster(ecs_client):
    resp = ecs_client.describe_clusters(clusters=["no-such-cluster"])
    assert len(resp["failures"]) >= 1


def test_describe_tasks(ecs_client):
    cluster = ecs_client.create_cluster(clusterName="desc-task-cluster")["cluster"][
        "clusterArn"
    ]
    td = ecs_client.register_task_definition(
        family="desc-task",
        containerDefinitions=[{"name": "app", "image": "alpine", "memory": 64}],
    )["taskDefinition"]["taskDefinitionArn"]
    run = ecs_client.run_task(cluster=cluster, taskDefinition=td)
    task_arn = run["tasks"][0]["taskArn"]
    resp = ecs_client.describe_tasks(cluster=cluster, tasks=[task_arn])
    assert len(resp["tasks"]) == 1
    assert resp["tasks"][0]["taskArn"] == task_arn


def test_update_service(ecs_client):
    cluster = ecs_client.create_cluster(clusterName="upd-svc-cluster")["cluster"][
        "clusterArn"
    ]
    td = ecs_client.register_task_definition(
        family="upd-svc-task",
        containerDefinitions=[{"name": "app", "image": "alpine", "memory": 64}],
    )["taskDefinition"]["taskDefinitionArn"]
    ecs_client.create_service(
        cluster=cluster, serviceName="upd-svc", taskDefinition=td, desiredCount=1
    )
    resp = ecs_client.update_service(cluster=cluster, service="upd-svc", desiredCount=2)
    assert resp["service"]["desiredCount"] == 2


def test_deregister_task_definition(ecs_client):
    td = ecs_client.register_task_definition(
        family="dereg-task",
        containerDefinitions=[{"name": "app", "image": "alpine", "memory": 64}],
    )["taskDefinition"]["taskDefinitionArn"]
    resp = ecs_client.deregister_task_definition(taskDefinition=td)
    assert resp["taskDefinition"]["status"] == "INACTIVE"


def test_capacity_provider_lifecycle(ecs_client):
    # Create
    resp = ecs_client.create_capacity_provider(
        name="my-cp",
        autoScalingGroupProvider={
            "autoScalingGroupArn": "arn:aws:autoscaling:us-east-1:000000000000:autoScalingGroup:1:autoScalingGroupName/asg1"
        },
    )
    cp = resp["capacityProvider"]
    assert cp["name"] == "my-cp"
    assert cp["status"] == "ACTIVE"

    # Describe
    resp2 = ecs_client.describe_capacity_providers(capacityProviders=["my-cp"])
    assert len(resp2["capacityProviders"]) == 1
    assert resp2["capacityProviders"][0]["name"] == "my-cp"

    # Describe all
    resp3 = ecs_client.describe_capacity_providers()
    assert any(c["name"] == "my-cp" for c in resp3["capacityProviders"])

    # Delete
    ecs_client.delete_capacity_provider(capacityProvider="my-cp")


def test_put_cluster_capacity_providers(ecs_client):
    cluster = ecs_client.create_cluster(clusterName="cp-cluster")["cluster"][
        "clusterArn"
    ]
    resp = ecs_client.put_cluster_capacity_providers(
        cluster=cluster,
        capacityProviders=[],
        defaultCapacityProviderStrategy=[],
    )
    assert "cluster" in resp


def test_attributes(ecs_client):
    cluster = ecs_client.create_cluster(clusterName="attr-cluster")["cluster"][
        "clusterArn"
    ]

    # Put attributes
    resp = ecs_client.put_attributes(
        cluster=cluster,
        attributes=[
            {
                "name": "custom.attr",
                "value": "myvalue",
                "targetType": "container-instance",
                "targetId": "ci-001",
            }
        ],
    )
    assert len(resp["attributes"]) == 1

    # List attributes
    list_resp = ecs_client.list_attributes(
        cluster=cluster,
        targetType="container-instance",
    )
    assert len(list_resp["attributes"]) >= 1

    # Delete attributes
    ecs_client.delete_attributes(
        cluster=cluster,
        attributes=[
            {
                "name": "custom.attr",
                "targetType": "container-instance",
                "targetId": "ci-001",
            }
        ],
    )
    list_resp2 = ecs_client.list_attributes(
        cluster=cluster,
        targetType="container-instance",
    )
    assert len(list_resp2["attributes"]) == 0


def test_task_protection(ecs_client):
    cluster = ecs_client.create_cluster(clusterName="prot-cluster")["cluster"][
        "clusterArn"
    ]
    td = ecs_client.register_task_definition(
        family="prot-task",
        containerDefinitions=[{"name": "app", "image": "alpine", "memory": 64}],
    )["taskDefinition"]["taskDefinitionArn"]
    task_arn = ecs_client.run_task(cluster=cluster, taskDefinition=td)["tasks"][0][
        "taskArn"
    ]

    # UpdateTaskProtection
    resp = ecs_client.update_task_protection(
        cluster=cluster,
        tasks=[task_arn],
        protectionEnabled=True,
    )
    assert len(resp["protectedTasks"]) == 1

    # GetTaskProtection
    resp2 = ecs_client.get_task_protection(cluster=cluster, tasks=[task_arn])
    assert len(resp2["protectedTasks"]) == 1


def test_submit_task_state_change(ecs_client):
    cluster = ecs_client.create_cluster(clusterName="stsc-cluster")["cluster"][
        "clusterArn"
    ]
    resp = ecs_client.submit_task_state_change(cluster=cluster)
    assert "acknowledgment" in resp


def test_ecs_tag_resource(ecs_client):
    cluster = ecs_client.create_cluster(clusterName="tagged-cluster")["cluster"][
        "clusterArn"
    ]
    ecs_client.tag_resource(
        resourceArn=cluster,
        tags=[{"key": "env", "value": "dev"}, {"key": "team", "value": "core"}],
    )
    resp = ecs_client.list_tags_for_resource(resourceArn=cluster)
    tags = {t["key"]: t["value"] for t in resp["tags"]}
    assert tags.get("env") == "dev"
    assert tags.get("team") == "core"

    ecs_client.untag_resource(resourceArn=cluster, tagKeys=["env"])
    resp2 = ecs_client.list_tags_for_resource(resourceArn=cluster)
    keys = [t["key"] for t in resp2["tags"]]
    assert "env" not in keys


def test_account_settings(ecs_client):
    resp = ecs_client.put_account_setting(name="serviceLongArnFormat", value="enabled")
    assert resp["setting"]["value"] == "enabled"

    listed = ecs_client.list_account_settings()
    assert any(s["name"] == "serviceLongArnFormat" for s in listed["settings"])

    ecs_client.delete_account_setting(name="serviceLongArnFormat")


def test_describe_services(ecs_client):
    cluster = ecs_client.create_cluster(clusterName="desc-svc-cluster")["cluster"][
        "clusterArn"
    ]
    td = ecs_client.register_task_definition(
        family="desc-svc-task",
        containerDefinitions=[{"name": "app", "image": "alpine", "memory": 64}],
    )["taskDefinition"]["taskDefinitionArn"]
    ecs_client.create_service(
        cluster=cluster, serviceName="desc-svc", taskDefinition=td, desiredCount=1
    )
    resp = ecs_client.describe_services(cluster=cluster, services=["desc-svc"])
    assert len(resp["services"]) == 1


def test_list_tasks(ecs_client):
    cluster = ecs_client.create_cluster(clusterName="lt-cluster")["cluster"][
        "clusterArn"
    ]
    td = ecs_client.register_task_definition(
        family="lt-task",
        containerDefinitions=[{"name": "app", "image": "alpine", "memory": 64}],
    )["taskDefinition"]["taskDefinitionArn"]
    ecs_client.run_task(cluster=cluster, taskDefinition=td)
    resp = ecs_client.list_tasks(cluster=cluster)
    assert len(resp["taskArns"]) >= 1


def test_list_task_definition_families(ecs_client):
    ecs_client.register_task_definition(
        family="fam-a",
        containerDefinitions=[{"name": "x", "image": "alpine", "memory": 64}],
    )
    resp = ecs_client.list_task_definition_families()
    assert "fam-a" in resp["families"]
