def test_create_experiment_template(fis_client):
    resp = fis_client.create_experiment_template(
        description="compat template",
        roleArn="arn:aws:iam::000000000000:role/fis-role",
        stopConditions=[{"source": "none"}],
        actions={
            "stop-instances": {
                "actionId": "aws:ec2:stop-instances",
                "parameters": {},
                "targets": {"Instances": "test-target"},
            }
        },
        targets={
            "test-target": {
                "resourceType": "aws:ec2:instance",
                "selectionMode": "ALL",
            }
        },
        tags={},
    )
    template = resp["experimentTemplate"]
    assert template["id"]
    assert template["description"] == "compat template"
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


def test_get_experiment_template(fis_client):
    create_resp = fis_client.create_experiment_template(
        description="get template",
        roleArn="arn:aws:iam::000000000000:role/fis-role",
        stopConditions=[{"source": "none"}],
        actions={},
        targets={},
        tags={},
    )
    template_id = create_resp["experimentTemplate"]["id"]
    resp = fis_client.get_experiment_template(id=template_id)
    assert resp["experimentTemplate"]["id"] == template_id
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


def test_list_experiment_templates(fis_client):
    fis_client.create_experiment_template(
        description="list template",
        roleArn="arn:aws:iam::000000000000:role/fis-role",
        stopConditions=[{"source": "none"}],
        actions={},
        targets={},
        tags={},
    )
    resp = fis_client.list_experiment_templates()
    assert len(resp["experimentTemplates"]) >= 1
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
