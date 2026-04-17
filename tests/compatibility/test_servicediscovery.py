def test_create_and_get_namespace(servicediscovery_client):
    resp = servicediscovery_client.create_http_namespace(Name="test-ns")
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
    op_id = resp["OperationId"]
    assert op_id

    # Get the operation to find the namespace ID
    op = servicediscovery_client.get_operation(OperationId=op_id)
    assert op["ResponseMetadata"]["HTTPStatusCode"] == 200
    ns_id = op["Operation"]["Targets"]["NAMESPACE"]

    ns = servicediscovery_client.get_namespace(Id=ns_id)
    assert ns["ResponseMetadata"]["HTTPStatusCode"] == 200
    assert ns["Namespace"]["Name"] == "test-ns"
    assert ns["Namespace"]["Type"] == "HTTP"

    servicediscovery_client.delete_namespace(Id=ns_id)


def test_list_namespaces(servicediscovery_client):
    resp = servicediscovery_client.create_http_namespace(Name="list-ns")
    op = servicediscovery_client.get_operation(OperationId=resp["OperationId"])
    ns_id = op["Operation"]["Targets"]["NAMESPACE"]

    namespaces = servicediscovery_client.list_namespaces()
    assert namespaces["ResponseMetadata"]["HTTPStatusCode"] == 200
    ids = [n["Id"] for n in namespaces["Namespaces"]]
    assert ns_id in ids

    servicediscovery_client.delete_namespace(Id=ns_id)


def test_delete_namespace(servicediscovery_client):
    resp = servicediscovery_client.create_http_namespace(Name="del-ns")
    op = servicediscovery_client.get_operation(OperationId=resp["OperationId"])
    ns_id = op["Operation"]["Targets"]["NAMESPACE"]

    del_resp = servicediscovery_client.delete_namespace(Id=ns_id)
    assert del_resp["ResponseMetadata"]["HTTPStatusCode"] == 200


def test_create_and_get_service(servicediscovery_client):
    resp = servicediscovery_client.create_http_namespace(Name="svc-ns")
    op = servicediscovery_client.get_operation(OperationId=resp["OperationId"])
    ns_id = op["Operation"]["Targets"]["NAMESPACE"]

    svc_resp = servicediscovery_client.create_service(
        Name="test-svc", NamespaceId=ns_id
    )
    assert svc_resp["ResponseMetadata"]["HTTPStatusCode"] == 200
    svc = svc_resp["Service"]
    svc_id = svc["Id"]
    assert svc["Name"] == "test-svc"

    get_svc = servicediscovery_client.get_service(Id=svc_id)
    assert get_svc["ResponseMetadata"]["HTTPStatusCode"] == 200
    assert get_svc["Service"]["Name"] == "test-svc"

    servicediscovery_client.delete_service(Id=svc_id)
    servicediscovery_client.delete_namespace(Id=ns_id)
