import pytest
from botocore.exceptions import ClientError


def test_create_and_get_application(serverlessrepo_client):
    resp = serverlessrepo_client.create_application(
        Name="compat-app",
        Author="Test Author",
        Description="A test application",
        SemanticVersion="1.0.0",
        TemplateBody="AWSTemplateFormatVersion: '2010-09-09'\nResources: {}",
    )
    assert "ApplicationId" in resp
    app_id = resp["ApplicationId"]

    get_resp = serverlessrepo_client.get_application(ApplicationId=app_id)
    assert get_resp["Name"] == "compat-app"
    assert get_resp["Description"] == "A test application"


def test_list_applications(serverlessrepo_client):
    serverlessrepo_client.create_application(
        Name="list-app",
        Author="Author",
        Description="List test",
    )
    resp = serverlessrepo_client.list_applications()
    assert "Applications" in resp
    names = [a["Name"] for a in resp["Applications"]]
    assert "list-app" in names


def test_update_application(serverlessrepo_client):
    resp = serverlessrepo_client.create_application(
        Name="update-app",
        Author="Author",
        Description="Old description",
    )
    app_id = resp["ApplicationId"]

    serverlessrepo_client.update_application(
        ApplicationId=app_id,
        Description="New description",
    )

    get_resp = serverlessrepo_client.get_application(ApplicationId=app_id)
    assert get_resp["Description"] == "New description"


def test_delete_application(serverlessrepo_client):
    resp = serverlessrepo_client.create_application(
        Name="del-app",
        Author="Author",
        Description="To be deleted",
    )
    app_id = resp["ApplicationId"]

    serverlessrepo_client.delete_application(ApplicationId=app_id)

    with pytest.raises(ClientError):
        serverlessrepo_client.get_application(ApplicationId=app_id)


def test_application_versions(serverlessrepo_client):
    resp = serverlessrepo_client.create_application(
        Name="versioned-app",
        Author="Author",
        Description="Versioned application",
    )
    app_id = resp["ApplicationId"]

    serverlessrepo_client.create_application_version(
        ApplicationId=app_id,
        SemanticVersion="1.0.0",
        TemplateBody="AWSTemplateFormatVersion: '2010-09-09'\nResources: {}",
    )
    serverlessrepo_client.create_application_version(
        ApplicationId=app_id,
        SemanticVersion="1.1.0",
        TemplateBody="AWSTemplateFormatVersion: '2010-09-09'\nResources: {}",
    )

    list_resp = serverlessrepo_client.list_application_versions(ApplicationId=app_id)
    assert "Versions" in list_resp
    semvers = [v["SemanticVersion"] for v in list_resp["Versions"]]
    assert "1.0.0" in semvers
    assert "1.1.0" in semvers


def test_cloudformation_change_set(serverlessrepo_client):
    resp = serverlessrepo_client.create_application(
        Name="cf-cs-app",
        Author="Author",
        Description="CF changeset test",
        SemanticVersion="1.0.0",
        TemplateBody="AWSTemplateFormatVersion: '2010-09-09'\nResources: {}",
    )
    app_id = resp["ApplicationId"]

    cs = serverlessrepo_client.create_cloud_formation_change_set(
        ApplicationId=app_id,
        StackName="my-stack",
        SemanticVersion="1.0.0",
    )
    assert cs["ChangeSetId"]
    assert cs["StackId"]


def test_cloudformation_template(serverlessrepo_client):
    resp = serverlessrepo_client.create_application(
        Name="cf-tmpl-app",
        Author="Author",
        Description="CF template test",
        SemanticVersion="1.0.0",
        TemplateBody="AWSTemplateFormatVersion: '2010-09-09'\nResources: {}",
    )
    app_id = resp["ApplicationId"]

    tmpl = serverlessrepo_client.create_cloud_formation_template(
        ApplicationId=app_id,
        SemanticVersion="1.0.0",
    )
    template_id = tmpl["TemplateId"]

    got = serverlessrepo_client.get_cloud_formation_template(
        ApplicationId=app_id, TemplateId=template_id
    )
    assert got["TemplateId"] == template_id


def test_list_application_dependencies(serverlessrepo_client):
    resp = serverlessrepo_client.create_application(
        Name="dep-app",
        Author="Author",
        Description="Deps test",
        SemanticVersion="1.0.0",
        TemplateBody="AWSTemplateFormatVersion: '2010-09-09'\nResources: {}",
    )
    app_id = resp["ApplicationId"]
    out = serverlessrepo_client.list_application_dependencies(ApplicationId=app_id)
    assert "Dependencies" in out


def test_unshare_application(serverlessrepo_client):
    resp = serverlessrepo_client.create_application(
        Name="unshare-app",
        Author="Author",
        Description="Unshare test",
        SemanticVersion="1.0.0",
        TemplateBody="AWSTemplateFormatVersion: '2010-09-09'\nResources: {}",
    )
    app_id = resp["ApplicationId"]
    serverlessrepo_client.unshare_application(
        ApplicationId=app_id, OrganizationId="o-1234"
    )
