# IAM / STS

## Overview

DevCloud IAM and STS share a single SQLite database (WAL mode). IAM manages users, roles, and access keys. STS generates temporary credentials and caller identity information.

Both services use the Query protocol (form-encoded requests, XML responses).

## Supported IAM APIs

These 58 operations are `hand-verified` — implemented by the provider, not by
the [CRUD engine](../crud-engine.md). Grouped by the resource they act on;
[fidelity-manifest.md](../fidelity-manifest.md) is the per-operation answer.

| Resource | Operations |
|----------|------------|
| Users | CreateUser, GetUser, UpdateUser, DeleteUser, ListUsers |
| Groups | CreateGroup, GetGroup, DeleteGroup, ListGroups, AddUserToGroup, RemoveUserFromGroup |
| Roles | CreateRole, GetRole, DeleteRole, ListRoles, UpdateAssumeRolePolicy |
| Instance profiles | CreateInstanceProfile, GetInstanceProfile, DeleteInstanceProfile, ListInstanceProfiles, AddRoleToInstanceProfile, RemoveRoleFromInstanceProfile |
| Managed policies | CreatePolicy, GetPolicy, DeletePolicy, CreatePolicyVersion, GetPolicyVersion, ListPolicyVersions |
| Policy attachment | Attach/Detach {User,Group,Role}Policy, ListAttached{User,Group,Role}Policies |
| Inline policies | Put/Get/Delete {User,Group,Role}Policy, List{User,Group,Role}Policies |
| Access keys | CreateAccessKey, UpdateAccessKey, DeleteAccessKey, ListAccessKeys |
| Tags | TagUser, UntagUser, ListUserTags, TagRole, UntagRole, ListRoleTags |

## Supported STS APIs

| Operation | Description |
|-----------|-------------|
| GetCallerIdentity | Return account ID, ARN, and user ID |
| AssumeRole | Generate temporary credentials (ASIA-prefixed keys, 1-hour expiry) |
| GetSessionToken | Generate session credentials |
| GetAccessKeyInfo | Return the account an access key ID belongs to |

## boto3 Examples

### IAM: Create users and roles

```python
import boto3

iam = boto3.client(
    "iam",
    endpoint_url="http://localhost:4747",
    aws_access_key_id="test",
    aws_secret_access_key="test",
    region_name="us-east-1",
)

# Create user
iam.create_user(UserName="testuser")

# Create access key for user
keys = iam.create_access_key(UserName="testuser")
print(keys["AccessKey"]["AccessKeyId"])
print(keys["AccessKey"]["SecretAccessKey"])

# Create role
iam.create_role(
    RoleName="lambda-role",
    AssumeRolePolicyDocument='{"Version":"2012-10-17","Statement":[]}',
)

# List users and roles
for user in iam.list_users()["Users"]:
    print(user["UserName"])
for role in iam.list_roles()["Roles"]:
    print(role["RoleName"])
```

### STS: Get caller identity and assume role

```python
sts = boto3.client(
    "sts",
    endpoint_url="http://localhost:4747",
    aws_access_key_id="test",
    aws_secret_access_key="test",
    region_name="us-east-1",
)

# Get caller identity
identity = sts.get_caller_identity()
print(identity["Account"])  # 000000000000
print(identity["Arn"])

# Assume role
response = sts.assume_role(
    RoleArn="arn:aws:iam::000000000000:role/lambda-role",
    RoleSessionName="my-session",
)
creds = response["Credentials"]
print(creds["AccessKeyId"])      # ASIA-prefixed
print(creds["SecretAccessKey"])
print(creds["SessionToken"])
print(creds["Expiration"])       # 1 hour from now
```

## AWS CLI Examples

```bash
# Create user
aws --endpoint-url http://localhost:4747 iam create-user --user-name testuser

# Create access key
aws --endpoint-url http://localhost:4747 iam create-access-key --user-name testuser

# Get caller identity
aws --endpoint-url http://localhost:4747 sts get-caller-identity

# Assume role
aws --endpoint-url http://localhost:4747 sts assume-role \
  --role-arn arn:aws:iam::000000000000:role/my-role \
  --role-session-name my-session
```

## Known Limitations

**IAM:**
- **No policy evaluation.** Managed and inline policy documents are stored and
  returned verbatim; nothing parses or enforces them, so attaching a `Deny` to a
  user changes nothing about what that user can call.
- No MFA device management
- No login profiles / password management
- No service-linked roles, SAML or OIDC identity providers
- No access advisor, credential reports, or policy simulation

**STS:**
- Temporary credentials are generated but not tracked or validated — they are
  never checked on a later request, and neither are long-lived ones
- Fixed 1-hour expiration; `DurationSeconds` is ignored
- `AssumeRole` does not evaluate the target role's trust policy, and no
  `ExternalId` is required or checked
- `GetSessionToken` ignores `SerialNumber` / `TokenCode` — there is no MFA
- No `AssumeRoleWithWebIdentity` or `AssumeRoleWithSAML`
- Single account model (account ID: `000000000000`)
