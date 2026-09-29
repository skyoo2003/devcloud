def test_create_and_describe_application(elasticbeanstalk_client):
    resp = elasticbeanstalk_client.create_application(
        ApplicationName="compat-app",
        Description="compatibility test app",
    )
    assert resp["Application"]["ApplicationName"] == "compat-app"

    desc = elasticbeanstalk_client.describe_applications(
        ApplicationNames=["compat-app"]
    )
    assert len(desc["Applications"]) == 1


def test_delete_application(elasticbeanstalk_client):
    elasticbeanstalk_client.create_application(ApplicationName="del-app")
    elasticbeanstalk_client.delete_application(ApplicationName="del-app")
    desc = elasticbeanstalk_client.describe_applications(ApplicationNames=["del-app"])
    assert len(desc["Applications"]) == 0


def test_create_environment(elasticbeanstalk_client):
    elasticbeanstalk_client.create_application(ApplicationName="env-app")
    resp = elasticbeanstalk_client.create_environment(
        ApplicationName="env-app",
        EnvironmentName="compat-env",
        SolutionStackName="64bit Amazon Linux 2 v5.8.0 running Python 3.8",
    )
    assert resp["EnvironmentName"] == "compat-env"

    desc = elasticbeanstalk_client.describe_environments(
        ApplicationName="env-app", EnvironmentNames=["compat-env"]
    )
    assert len(desc["Environments"]) == 1


def test_list_available_solution_stacks(elasticbeanstalk_client):
    resp = elasticbeanstalk_client.list_available_solution_stacks()
    assert "SolutionStacks" in resp
