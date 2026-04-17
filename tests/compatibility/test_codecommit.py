def test_create_repository(codecommit_client):
    resp = codecommit_client.create_repository(
        repositoryName="compat-repo",
        repositoryDescription="test repo",
    )
    info = resp["repositoryMetadata"]
    assert info["repositoryName"] == "compat-repo"
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


def test_get_repository(codecommit_client):
    codecommit_client.create_repository(repositoryName="get-repo")
    resp = codecommit_client.get_repository(repositoryName="get-repo")
    assert resp["repositoryMetadata"]["repositoryName"] == "get-repo"
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


def test_list_repositories(codecommit_client):
    codecommit_client.create_repository(repositoryName="list-repo")
    resp = codecommit_client.list_repositories()
    names = [r["repositoryName"] for r in resp["repositories"]]
    assert "list-repo" in names
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


def test_delete_repository(codecommit_client):
    codecommit_client.create_repository(repositoryName="del-repo")
    resp = codecommit_client.delete_repository(repositoryName="del-repo")
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
    list_resp = codecommit_client.list_repositories()
    names = [r["repositoryName"] for r in list_resp["repositories"]]
    assert "del-repo" not in names
