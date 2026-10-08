# EventBridge

DevCloud supports EventBridge rules and local SQS, SNS and Lambda targets, partner
event sources, reusable connections and event bus resource policies. Requests use
the standard boto3 `events` client against the DevCloud endpoint. The local
account is `000000000000`.

## Partner event sources

`CreatePartnerEventSource` creates a PENDING source. Names follow
`aws.partner/<partner>/<source>`, for example `aws.partner/devcloud/orders`.
The Account parameter must name the local account. `DescribePartnerEventSource`,
`DescribeEventSource`, and the source/account list APIs read the same stored row.
Source timestamps are SDK datetimes. Lists support prefixes, Limit 1–100 and
NextToken; a token belongs to its original list operation and filters.

Create a bus with matching Name and EventSourceName to bind the source and make
it ACTIVE. `DeactivateEventSource` changes it to PENDING; `ActivateEventSource`
restores ACTIVE on a bound source. Repeated transitions succeed. Rules and targets
remain stored through either transition.

`PutPartnerEvents` accepts 1–20 entries. Each entry requires Source, DetailType and
a JSON Detail string. An ACTIVE bound source sends matching events through the
existing local target delivery path. PENDING or missing sources fail individually;
the response keeps the original entry order and reports FailedEntryCount. EventId
acknowledges admission; target delivery is asynchronous and failures are logged.
A matching target requires a configured local server endpoint.

Deleting a source preserves its bus. Deleting a bus unbinds the source and returns
it to PENDING. DevCloud does not run background source expiration or partner
account onboarding workflows.

## Connections

Create, describe, list, update and delete operations share durable connection
state. API_KEY and BASIC credentials become AUTHORIZED locally when their required
fields are provided. OAuth client credentials are retained as configuration with
state DEAUTHORIZED; DevCloud does not contact the authorization endpoint or issue
and refresh tokens.

`DeauthorizeConnection` removes the stored AuthParameters and sets DEAUTHORIZED.
It preserves the ARN, creation time, description and previous authorization time.
Updating with valid credentials allows reuse of the same connection. Updates that
omit AuthParameters preserve them; invalid supplied authentication fails without
changing the existing row. Deleting returns DELETING and removes the row.

Partial credential and invocation-parameter updates retain omitted fields. After
deauthorization or when changing authorization type, reauthorization requires
complete credentials for that type.

Describe responses omit passwords, API key values, OAuth client secrets and HTTP
parameter values marked IsValueSecret. CreationTime, LastModifiedTime and
LastAuthorizedTime decode as SDK datetimes. KmsKeyIdentifier and connectivity
parameters are stored metadata; they do not enable KMS encryption, VPC networking
or external API destination execution.

## Bus policies

`PutPermission` accepts either statement fields (StatementId, Action, Principal
and optional Condition) or a full JSON Policy string. A policy's Statement may be
an object or array and must contain unique, addressable Sid values. A repeated Sid
replaces that statement. `DescribeEventBus` returns Policy as a JSON string.

`RemovePermission` requires one StatementId or RemoveAllPermissions=true. Removing
the last statement omits Policy from subsequent descriptions. Missing buses and
statement IDs return ResourceNotFoundException. Custom and default bus policies
are isolated, and deleting a bus removes its policy. Stored resource policies do
not perform IAM authorization of event admission.

## Existing data and verification

EventBridge adds schema migration 1001 without altering older tables. On startup
it imports addressable legacy generic CRUD records once, preserving their ARN,
timestamps and original JSON. Canonical EventBridge rows take precedence. Import
markers prevent deleted resources from being restored on restart. Conflicting or
malformed records fail the import transaction; ambiguous permissions and
unaddressable rows are retained without inventing resources or grants.

Go tests cover upgrades, rollback, import retries and concurrent policy mutations.
The boto3 compatibility suite verifies all four newly supported operations,
typed dates, empty Unit responses, connection reuse and real partner delivery to
SQS with Unicode and punctuation intact. See [Coverage](../coverage.md) for the
whole AWS surface and [Lambda](lambda.md) for Docker runtime requirements.
