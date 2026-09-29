def test_register_scalable_target(applicationautoscaling_client):
    resp = applicationautoscaling_client.register_scalable_target(
        ServiceNamespace="ecs",
        ResourceId="service/default/sample-webapp",
        ScalableDimension="ecs:service:DesiredCount",
        MinCapacity=1,
        MaxCapacity=10,
    )
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
    assert "ScalableTargetARN" in resp


def test_describe_scalable_targets(applicationautoscaling_client):
    applicationautoscaling_client.register_scalable_target(
        ServiceNamespace="ecs",
        ResourceId="service/default/desc-svc",
        ScalableDimension="ecs:service:DesiredCount",
        MinCapacity=1,
        MaxCapacity=5,
    )
    resp = applicationautoscaling_client.describe_scalable_targets(
        ServiceNamespace="ecs",
        ResourceIds=["service/default/desc-svc"],
    )
    assert len(resp["ScalableTargets"]) >= 1
    target = resp["ScalableTargets"][0]
    assert target["ResourceId"] == "service/default/desc-svc"
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


def test_deregister_scalable_target(applicationautoscaling_client):
    applicationautoscaling_client.register_scalable_target(
        ServiceNamespace="ecs",
        ResourceId="service/default/dereg-svc",
        ScalableDimension="ecs:service:DesiredCount",
        MinCapacity=1,
        MaxCapacity=5,
    )
    resp = applicationautoscaling_client.deregister_scalable_target(
        ServiceNamespace="ecs",
        ResourceId="service/default/dereg-svc",
        ScalableDimension="ecs:service:DesiredCount",
    )
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


def test_put_and_describe_scaling_policy(applicationautoscaling_client):
    applicationautoscaling_client.register_scalable_target(
        ServiceNamespace="ecs",
        ResourceId="service/default/pol-svc",
        ScalableDimension="ecs:service:DesiredCount",
        MinCapacity=1,
        MaxCapacity=5,
    )
    resp = applicationautoscaling_client.put_scaling_policy(
        PolicyName="cpu-policy",
        ServiceNamespace="ecs",
        ResourceId="service/default/pol-svc",
        ScalableDimension="ecs:service:DesiredCount",
        PolicyType="TargetTrackingScaling",
        TargetTrackingScalingPolicyConfiguration={
            "TargetValue": 70.0,
            "PredefinedMetricSpecification": {
                "PredefinedMetricType": "ECSServiceAverageCPUUtilization"
            },
        },
    )
    assert "PolicyARN" in resp

    desc = applicationautoscaling_client.describe_scaling_policies(
        ServiceNamespace="ecs"
    )
    assert len(desc["ScalingPolicies"]) >= 1


def test_scheduled_actions(applicationautoscaling_client):
    applicationautoscaling_client.put_scheduled_action(
        ServiceNamespace="ecs",
        ScheduledActionName="my-scheduled",
        ResourceId="service/default/sched-svc",
        ScalableDimension="ecs:service:DesiredCount",
        Schedule="rate(5 minutes)",
        ScalableTargetAction={"MinCapacity": 1, "MaxCapacity": 10},
    )
    desc = applicationautoscaling_client.describe_scheduled_actions(
        ServiceNamespace="ecs"
    )
    assert any(
        a["ScheduledActionName"] == "my-scheduled" for a in desc["ScheduledActions"]
    )


def test_describe_scaling_activities(applicationautoscaling_client):
    resp = applicationautoscaling_client.describe_scaling_activities(
        ServiceNamespace="ecs"
    )
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
    assert "ScalingActivities" in resp


def test_tags(applicationautoscaling_client):
    arn = "arn:aws:application-autoscaling:us-east-1:000000000000:scalableTarget/tagged"
    applicationautoscaling_client.tag_resource(
        ResourceARN=arn,
        Tags={"env": "prod"},
    )
    resp = applicationautoscaling_client.list_tags_for_resource(ResourceARN=arn)
    assert resp["Tags"].get("env") == "prod"

    applicationautoscaling_client.untag_resource(ResourceARN=arn, TagKeys=["env"])
