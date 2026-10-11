import pytest
import requests
from conftest import DEVCLOUD_URL


@pytest.fixture
def identity_url(devcloud_server):
    return f"{DEVCLOUD_URL}/devcloud/identity"


def test_federated_identity_oidc_discovery_and_jwks(identity_url):
    # 1. Discovery document
    resp_disc = requests.get(f"{identity_url}/.well-known/openid-configuration")
    assert resp_disc.status_code == 200
    disc = resp_disc.json()
    assert disc["issuer"].endswith("/devcloud/identity")
    assert disc["jwks_uri"].endswith("/jwks.json")
    assert "RS256" in disc["id_token_signing_alg_values_supported"]

    # 2. JWKS
    resp_jwks = requests.get(f"{identity_url}/jwks.json")
    assert resp_jwks.status_code == 200
    jwks = resp_jwks.json()
    assert len(jwks.get("keys", [])) >= 1
    key = jwks["keys"][0]
    assert key["kty"] == "RSA"
    assert key["alg"] == "RS256"
    assert key["use"] == "sig"
    assert len(key["n"]) > 0
    assert len(key["e"]) > 0


def test_federated_identity_mint_and_sts_web_identity_exchange(
    identity_url, sts_client
):
    # 1. Mint an OIDC token from local provider
    custom_sub = "repo:octocat/microservice-api:ref:refs/heads/main"
    mint_payload = {
        "sub": custom_sub,
        "aud": "sts.amazonaws.com",
        "claims": {
            "repository_owner": "octocat",
            "environment": "production",
        },
    }
    resp_mint = requests.post(f"{identity_url}/token", json=mint_payload)
    assert resp_mint.status_code == 200
    token_data = resp_mint.json()
    token = token_data.get("access_token")
    assert token is not None
    assert token_data.get("token_type") == "Bearer"

    # 2. Exchange OIDC token with AWS STS AssumeRoleWithWebIdentity via boto3
    sts = sts_client
    role_arn = "arn:aws:iam::123456789012:role/DeployProductionRole"
    assumed = sts.assume_role_with_web_identity(
        RoleArn=role_arn,
        RoleSessionName="github-actions-session",
        WebIdentityToken=token,
    )

    assert "Credentials" in assumed
    creds = assumed["Credentials"]
    assert creds["AccessKeyId"].startswith("ASIA")
    assert len(creds["SecretAccessKey"]) > 0
    assert len(creds["SessionToken"]) > 0
    assert assumed["SubjectFromWebIdentityToken"] == custom_sub
    assert assumed["Audience"] == "sts.amazonaws.com"
    assert "AssumedRoleUser" in assumed
    assert "github-actions-session" in assumed["AssumedRoleUser"]["Arn"]


def test_cross_csp_token_exchange(identity_url, sts_client):
    # 1. Mint token from DevCloud OIDC
    resp_mint = requests.post(
        f"{identity_url}/token", json={"sub": "service-mesh-controller"}
    )
    assert resp_mint.status_code == 200
    token = resp_mint.json()["access_token"]

    # 2. Exchange for Azure AD Bearer token
    resp_az = requests.post(
        f"{identity_url}/azure/oauth2/v2.0/token",
        data={
            "grant_type": "urn:ietf:params:oauth:grant-type:token-exchange",
            "client_assertion": token,
            "scope": "https://management.azure.com/.default",
        },
    )
    assert resp_az.status_code == 200
    az_data = resp_az.json()
    assert az_data["token_type"] == "Bearer"
    assert len(az_data["access_token"]) > 0

    # 3. Exchange for GCP OAuth2 access token
    resp_gcp = requests.post(
        f"{identity_url}/gcp/v1/token",
        data={
            "grant_type": "urn:ietf:params:oauth:grant-type:token-exchange",
            "subject_token": token,
        },
    )
    assert resp_gcp.status_code == 200
    gcp_data = resp_gcp.json()
    assert gcp_data["token_type"] == "Bearer"
    gcp_token = gcp_data["access_token"]
    assert gcp_token.startswith("ya29.")

    # 4. Exchange GCP token for AWS STS temporary credentials
    sts = sts_client
    assumed_gcp = sts.assume_role_with_web_identity(
        RoleArn="arn:aws:iam::123456789012:role/CrossCloudGCPRole",
        RoleSessionName="gcp-federated-session",
        WebIdentityToken=gcp_token,
    )
    assert assumed_gcp["Credentials"]["AccessKeyId"].startswith("ASIA")
    assert "gcp:" in assumed_gcp["SubjectFromWebIdentityToken"]
    assert assumed_gcp["Provider"] == "https://accounts.google.com"
