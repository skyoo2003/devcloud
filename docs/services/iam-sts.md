# IAM / STS

## Overview

DevCloud IAM and STS share a single SQLite database (WAL mode). IAM manages users, roles, and access keys. STS generates temporary credentials and caller identity information.

Both services use the Query protocol (form-encoded requests, XML responses).

## Supported IAM APIs

| Operation | Description |
|-----------|-------------|
| CreateUser | Create an IAM user |
| ListUsers | List all IAM users |
| CreateRole | Create an IAM role with assume role policy document |
| ListRoles | List all IAM roles |
| AttachRolePolicy | Attach a managed policy ARN to a role |
| CreateAccessKey | Generate an access key pair for a user |

## Supported STS APIs

| Operation | Description |
|-----------|-------------|
| GetCallerIdentity | Return account ID, ARN, and user ID |
| AssumeRole | Generate temporary credentials (ASIA-prefixed keys, 1-hour expiry) |
| GetSessionToken | Generate MFA-backed session credentials |

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
- No GetUser, DeleteUser, UpdateUser
- No DeleteRole, UpdateRole
- No inline policies (PutRolePolicy, PutUserPolicy)
- No groups
- No MFA device management
- No login profiles / password management
- No tagging
- No policy enforcement — policies are stored but not evaluated

**STS:**
- Temporary credentials are generated but not tracked or validated
- Fixed 1-hour expiration (no custom duration)
- No external ID validation
- No policy enforcement on assumed roles
- Single account model (account ID: `000000000000`)
