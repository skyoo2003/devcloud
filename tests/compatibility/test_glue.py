import pytest
from botocore.exceptions import ClientError


def test_create_and_get_database(glue_client):
    glue_client.create_database(
        DatabaseInput={"Name": "compat_db", "Description": "test database"}
    )
    resp = glue_client.get_database(Name="compat_db")
    assert resp["Database"]["Name"] == "compat_db"


def test_get_databases(glue_client):
    glue_client.create_database(DatabaseInput={"Name": "list_db"})
    resp = glue_client.get_databases()
    names = [d["Name"] for d in resp["DatabaseList"]]
    assert "list_db" in names


def test_delete_database(glue_client):
    glue_client.create_database(DatabaseInput={"Name": "del_db"})
    glue_client.delete_database(Name="del_db")
    with pytest.raises(ClientError) as exc:
        glue_client.get_database(Name="del_db")
    assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"


def test_create_and_get_table(glue_client):
    glue_client.create_database(DatabaseInput={"Name": "table_db"})
    glue_client.create_table(
        DatabaseName="table_db",
        TableInput={
            "Name": "compat_table",
            "StorageDescriptor": {
                "Columns": [
                    {"Name": "id", "Type": "int"},
                    {"Name": "name", "Type": "string"},
                ],
                "Location": "s3://bucket/path",
                "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
                "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
                "SerdeInfo": {
                    "SerializationLibrary": "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"
                },
            },
        },
    )
    resp = glue_client.get_table(DatabaseName="table_db", Name="compat_table")
    assert resp["Table"]["Name"] == "compat_table"


def test_create_and_get_crawler(glue_client):
    glue_client.create_crawler(
        Name="compat-crawler",
        Role="arn:aws:iam::000000000000:role/glue-role",
        Targets={"S3Targets": [{"Path": "s3://bucket/data"}]},
        DatabaseName="default",
    )
    get = glue_client.get_crawler(Name="compat-crawler")
    assert get["Crawler"]["Name"] == "compat-crawler"


def test_get_nonexistent_database(glue_client):
    with pytest.raises(ClientError) as exc:
        glue_client.get_database(Name="no_such_db")
    assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"
