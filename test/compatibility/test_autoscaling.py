def test_create_launch_configuration(autoscaling_client):
    autoscaling_client.create_launch_configuration(
        LaunchConfigurationName="compat-lc",
        ImageId="ami-12345678",
        InstanceType="t3.micro",
    )
    resp = autoscaling_client.describe_launch_configurations(
        LaunchConfigurationNames=["compat-lc"]
    )
    assert len(resp["LaunchConfigurations"]) == 1
    assert resp["LaunchConfigurations"][0]["LaunchConfigurationName"] == "compat-lc"


def test_create_and_describe_asg(autoscaling_client):
    autoscaling_client.create_launch_configuration(
        LaunchConfigurationName="asg-lc",
        ImageId="ami-12345678",
        InstanceType="t3.micro",
    )
    autoscaling_client.create_auto_scaling_group(
        AutoScalingGroupName="compat-asg",
        LaunchConfigurationName="asg-lc",
        MinSize=0,
        MaxSize=2,
        DesiredCapacity=1,
        AvailabilityZones=["us-east-1a"],
    )
    resp = autoscaling_client.describe_auto_scaling_groups(
        AutoScalingGroupNames=["compat-asg"]
    )
    assert len(resp["AutoScalingGroups"]) == 1
    assert resp["AutoScalingGroups"][0]["DesiredCapacity"] == 1


def test_delete_asg(autoscaling_client):
    autoscaling_client.create_launch_configuration(
        LaunchConfigurationName="del-asg-lc",
        ImageId="ami-12345678",
        InstanceType="t3.micro",
    )
    autoscaling_client.create_auto_scaling_group(
        AutoScalingGroupName="del-asg",
        LaunchConfigurationName="del-asg-lc",
        MinSize=0,
        MaxSize=1,
        AvailabilityZones=["us-east-1a"],
    )
    autoscaling_client.delete_auto_scaling_group(
        AutoScalingGroupName="del-asg", ForceDelete=True
    )
    autoscaling_client.delete_launch_configuration(LaunchConfigurationName="del-asg-lc")


def test_describe_nonexistent_asg(autoscaling_client):
    resp = autoscaling_client.describe_auto_scaling_groups(
        AutoScalingGroupNames=["no-such-asg"]
    )
    assert len(resp["AutoScalingGroups"]) == 0
