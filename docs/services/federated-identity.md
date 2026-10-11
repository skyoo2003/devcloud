# Federated Identity Playground

DevCloud includes a built-in **Federated Identity Playground** designed to simulate cross-cloud workload identity federation, local OpenID Connect (OIDC) identity issuance, and multi-CSP token exchanges without relying on third-party identity providers or active internet connections.

## Overview

In modern multi-cloud architectures, workloads running in GitHub Actions, Kubernetes, AWS, GCP, or Azure frequently authenticate across CSP boundaries using OpenID Connect (OIDC) tokens and OAuth 2.0 Token Exchange (RFC 8693) rather than hardcoded long-lived credentials.

DevCloud simulates this end-to-end:
1. **Local OIDC Provider**: Mints cryptographically signed RS256 JWTs with arbitrary claims (`sub`, `iss`, `aud`, custom claims).
2. **Standard OIDC Discovery & JWKS**: Publishes `/.well-known/openid-configuration` and `/jwks.json` for validation.
3. **Cross-CSP Token Exchange**: Exchanges local OIDC or AWS STS tokens for simulated Azure AD Bearer tokens or Google Cloud access tokens (`ya29...`).
4. **AWS STS Web Identity Federation**: `sts.AssumeRoleWithWebIdentity` parses and validates JWT claims, returning temporary AWS credentials tied to the claimed external identity.

## Endpoints

| Area | Operation | Method & URI | Notes |
|---|---|---|---|
| OIDC Provider | `GetOpenIDConfiguration` | `GET /devcloud/identity/.well-known/openid-configuration` | Returns discovery JSON document with endpoints, signing algorithms, and supported claims |
| OIDC Provider | `GetJWKS` | `GET /devcloud/identity/jwks.json` | Returns public JSON Web Key Set (RS256) |
| OIDC Provider | `MintToken` | `POST /devcloud/identity/token` | Generates a signed RS256 JWT with custom `sub`, `iss`, `aud`, and custom claims |
| Azure Exchange | `ExchangeAzureToken` | `POST /devcloud/identity/azure/oauth2/v2.0/token` | Exchanges client assertion or STS token for Azure AD Bearer token |
| GCP Exchange | `ExchangeGCPToken` | `POST /devcloud/identity/gcp/v1/token` | Exchanges subject token for Google OAuth2 access token (`ya29...`) |

## Quickstarts

### 1. Minting a Signed OIDC Token

You can request a signed JWT for testing automated workflows:

```bash
curl -X POST http://localhost:4747/devcloud/identity/token \
     -H "Content-Type: application/json" \
     -d '{
       "sub": "repo:my-org/my-service:ref:refs/heads/main",
       "aud": "sts.amazonaws.com",
       "claims": {
         "environment": "staging",
         "runner": "github-hosted"
       }
     }'
```

Response:
```json
{
  "access_token": "eyJhbGciOiJSUzI1NiIsImtpZCI6ImRldmNsb3VkLWtleS0xIiwidHlwIjoiSldUIn0...",
  "id_token": "eyJhbGciOiJSUzI1NiIsImtpZCI6ImRldmNsb3VkLWtleS0xIiwidHlwIjoiSldUIn0...",
  "token_type": "Bearer",
  "expires_in": 3600,
  "sub": "repo:my-org/my-service:ref:refs/heads/main"
}
```

### 2. AWS STS AssumeRoleWithWebIdentity Exchange

Feed the minted OIDC token directly to AWS STS using standard AWS SDKs:

```python
import boto3
import requests

# 1. Mint token from DevCloud
resp = requests.post("http://localhost:4747/devcloud/identity/token", json={
    "sub": "repo:acme/backend:ref:refs/heads/production",
    "aud": "sts.amazonaws.com"
})
oidc_token = resp.json()["access_token"]

# 2. Assume Role via STS
sts = boto3.client("sts", endpoint_url="http://localhost:4747", region_name="us-east-1")
assumed = sts.assume_role_with_web_identity(
    RoleArn="arn:aws:iam::123456789012:role/DeployRole",
    RoleSessionName="ci-session",
    WebIdentityToken=oidc_token
)

print("Temporary Access Key:", assumed["Credentials"]["AccessKeyId"])
print("Subject from Token:", assumed["SubjectFromWebIdentityToken"])
print("Provider:", assumed["Provider"])
```

### 3. Cross-CSP Token Exchange (Workload Federation)

Simulate exchanging an AWS or OIDC identity for Azure AD or Google Cloud tokens:

```python
import requests

# Exchange for Azure AD Bearer Token
resp_azure = requests.post(
    "http://localhost:4747/devcloud/identity/azure/oauth2/v2.0/token",
    data={
        "grant_type": "urn:ietf:params:oauth:grant-type:token-exchange",
        "client_assertion": oidc_token,
        "scope": "https://management.azure.com/.default"
    }
)
azure_token = resp_azure.json()["access_token"]
print("Azure Bearer Token:", azure_token)

# Exchange for Google Cloud ya29 Token
resp_gcp = requests.post(
    "http://localhost:4747/devcloud/identity/gcp/v1/token",
    data={
        "grant_type": "urn:ietf:params:oauth:grant-type:token-exchange",
        "subject_token": oidc_token
    }
)
gcp_token = resp_gcp.json()["access_token"]
print("GCP Access Token:", gcp_token)
```
