# Azure Services

DevCloud is expanding its local development capabilities to include Microsoft Azure services, starting with Azure Blob Storage. This represents the beginning of our multi-CSP architectural vision.

## Supported Services

| Service | Supported | Local limits |
|---|---|---|
| Blob Storage | Initializing | Mock engine returning generic responses; strict semantic verification in progress. |

## Credentials
DevCloud accepts Azure Shared Key credentials without validating signatures. For local development, use any dummy account key (or the standard local development key):

```bash
export AZURE_STORAGE_CONNECTION_STRING="DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:4747/devstoreaccount1;"
```
