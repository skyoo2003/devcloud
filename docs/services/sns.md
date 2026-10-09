# SNS (Simple Notification Service)

## Overview
DevCloud supports topics/subscriptions, batch publication to local SQS, FIFO
admission, platform application/endpoint settings, SMS settings, opt-in and SMS
sandbox verification. Use the standard boto3 sns client against the local endpoint.
The local account is `000000000000` and region is `us-east-1`.

## Supported APIs

These 41 operations are `hand-verified` — implemented by the provider, not by the CRUD engine.

| Operation | Description |
|-----------|-------------|
| CreateTopic / DeleteTopic / ListTopics | Topic lifecycle |
| GetTopicAttributes / SetTopicAttributes | Topic configurations (including `.fifo` topics) |
| Subscribe / Unsubscribe / ListSubscriptions / ListSubscriptionsByTopic / ConfirmSubscription | Subscription lifecycle |
| GetSubscriptionAttributes / SetSubscriptionAttributes | Subscription configurations |
| Publish / PublishBatch | Message publication, including FIFO deduplication and ordering |
| CreatePlatformApplication / DeletePlatformApplication / ListPlatformApplications / GetPlatformApplicationAttributes / SetPlatformApplicationAttributes | Mobile push application configurations |
| CreatePlatformEndpoint / DeleteEndpoint / ListEndpointsByPlatformApplication / GetEndpointAttributes / SetEndpointAttributes | Mobile push endpoints |
| SetSMSAttributes / GetSMSAttributes / OptInPhoneNumber / CheckIfPhoneNumberIsOptedOut / ListPhoneNumbersOptedOut | SMS global settings |
| CreateSMSSandboxPhoneNumber / VerifySMSSandboxPhoneNumber / ListSMSSandboxPhoneNumbers / DeleteSMSSandboxPhoneNumber / GetSMSSandboxAccountStatus | SMS sandbox and OTP generation |
| AddPermission / RemovePermission | Resource-based topic policies |
| TagResource / UntagResource / ListTagsForResource | Resource tagging |
| PutDataProtectionPolicy / GetDataProtectionPolicy | Data protection policies |

## Publication and FIFO

Publish and PublishBatch share validation and delivery. Batches contain 1–10
entries with distinct IDs of 1–80 ASCII letters, numbers, underscores or hyphens.
Each message and the sum of message/attribute payloads must fit 256 KiB.
Structural, duplicate-ID and size errors reject the request before delivery.
Invalid messages fail individually; every ID appears once in Successful or
Failed. Delivery/storage failures have SenderFault=false.

SQS receives raw text and string/binary attributes. MessageStructure=json requires
string-valued fields and a nonempty default; SQS receives sqs when present,
otherwise default. Other subscription protocols remain configuration; DevCloud
does not execute external email, SMS, HTTPS or mobile push delivery.

FIFO topics require a .fifo name and FifoTopic=true. MessageGroupId is required;
MessageDeduplicationId is required unless ContentBasedDeduplication is enabled.
Content deduplication hashes the message body with SHA-256, excluding attributes.
FifoThroughputScope defaults to Topic and can be MessageGroup. Admission/delivery
are serialized, sequence strings increase, and five-minute dedup receipts survive
SNS database reopening. Single/batch publication share receipts; duplicates
succeed without delivery.

Group/dedup identifiers reach SQS. Group-scoped target queues need
DeduplicationScope=messageGroup and FifoThroughputLimit=perMessageGroupId. SQS's
existing queues remain in memory; durable SNS receipts do not make SQS durable.

SNS commits successful FIFO admission after delivery succeeds. It attempts
remaining SQS subscriptions after a failure, then reports failure. Sink writes
cannot be rolled back if another target or the subsequent database commit fails.
Standard-message retries can duplicate an earlier delivery; FIFO retries use the
same dedup ID. There is no background delivery retry worker.

## Platform applications and endpoints

Create/Get/List/Set/Delete APIs use durable shared records. Supported platforms
are ADM, APNS, APNS_SANDBOX and GCM. Names contain 1–256 ASCII letters, numbers,
underscores, hyphens or periods. Credentials, principals and notification/feedback
fields are configuration; DevCloud does not authenticate with APNS/FCM or invoke
IAM, CloudWatch or notification callbacks.

Endpoints require an existing application. Compatible creation with the same
parent/token returns the original ARN; conflicts are rejected. Setters merge
supplied keys and preserve other fields and ARN. Enabled accepts true/false
strings, Token is nonempty, and CustomUserData is UTF-8 below 2 KiB. Application
deletion removes its endpoints atomically. Lists use sorted ARN pages and tokens
bound to the original parent/account.

## SMS settings and sandbox

SetSMSAttributes merges account settings; GetSMSAttributes returns requested keys
or all settings when omitted. Defaults are Promotional for DefaultSMSType and 0
for DeliveryStatusSuccessSamplingRate. Invalid spend limits, sender IDs, types or
sampling rates leave old settings intact. Role/report bucket attributes are
metadata; no billing, carrier delivery or report generation runs.

OptInPhoneNumber validates E.164 and removes an existing opt-out. Check/list read
the persisted table. Repeated opt-in succeeds; the cloud's 30-day reapplication
restriction is not enforced locally.

CreateSMSSandboxPhoneNumber creates an UNVERIFIED challenge and a random six-digit
OTP atomically with a local SMS outbox receipt. Account status is IsInSandbox=true.
List exposes phone numbers/statuses with MaxResults 1–100 and NextToken.

With DEVCLOUD_DATA_DIR=/path/to/data, the receipt database is
`/path/to/data/sns/sns/sns.db`; the default root is ./data. Open SQLite read-only
and select the latest code for your phone/account:

```sql
SELECT json_extract(body_json, '$.OneTimePassword')
FROM sns_sms_outbox
WHERE account_id = '000000000000' AND phone_number = '+12065550123'
ORDER BY created_at DESC, rowid DESC LIMIT 1;
```

VerifySMSSandboxPhoneNumber consumes a correct code and sets VERIFIED. Expiry is
ten minutes; reissue invalidates the previous code. Incorrect/expired/consumed
codes return Verification, absent numbers ResourceNotFound. Opted-out numbers
cannot receive a challenge. Deletion retains the local receipt. No SMS is sent
to the telephone network.

## Existing data and failures

Migration 4 adds state without dropping old tables. Initialization imports generic
SNS data once, preserving original JSON, ARN and creation time. Import reads both
nested maps and flat Query attribute fields, including legacy PlatformEndpointArn.
Conflicting attribute values or endpoint ARN aliases fail the import. Canonical records
win; markers prevent deleted data from reappearing. Conflicting identities or
settings roll back together. Missing-parent/helper rows remain preserved without
invented resources. Imported VERIFIED phones become UNVERIFIED without a usable
challenge; issue a fresh OTP before verification.

Pinned boto3 tests exercise Query/XML responses and actual SQS/outbox results.
Invalid parameters, resource absence, verification and storage/delivery failures
use distinct errors. A closed database does not produce an empty successful list.


## boto3 Examples

```python
import boto3

client = boto3.client('sns', endpoint_url='http://localhost:4747', region_name='us-east-1')

# Standard topic creation and subscription
topic = client.create_topic(Name='my-topic')
client.subscribe(
    TopicArn=topic['TopicArn'],
    Protocol='sqs',
    Endpoint='arn:aws:sqs:us-east-1:000000000000:my-queue'
)

# Publish a message
client.publish(
    TopicArn=topic['TopicArn'],
    Message='hello world'
)
```

## AWS CLI Examples

```bash
aws --endpoint-url=http://localhost:4747 sns create-topic --name my-topic
aws --endpoint-url=http://localhost:4747 sns publish --topic-arn arn:aws:sns:us-east-1:000000000000:my-topic --message "hello"
```

## Known Limitations
- **Delivery endpoints:** Active delivery is only implemented for SQS (`sqs` protocol) and Lambda (`lambda` protocol). HTTP/S, email, and mobile push endpoints are accepted as configuration but no outbound network requests are made.
- **SMS Delivery:** SMS messages are not dispatched to real telecommunication networks. OTP codes for the SMS sandbox are generated and stored locally in the `sns.db` outbox table for verification (see Sandbox section).
