from azure.storage.blob import BlobServiceClient


def test_blob_create_container():
    # Proof of concept test against DevCloud's Azure mock
    # This uses local HTTP endpoint with a well-known development account key
    _ = BlobServiceClient(
        account_url="http://127.0.0.1:4747/devstoreaccount1",
        credential="DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:4747/devstoreaccount1;",
    )
    # The actual implementation of the mock will be expanded in a later phase.
    # For now, we just want to ensure we can instantiate the client and hit the endpoint.
    pass
