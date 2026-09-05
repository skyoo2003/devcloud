# Demand for unregistered services

Sampled 2026-09-05. Re-derive with `python3 scripts/demand_rank.py`.

## What this measures, and what it does not

DevCloud has no usage telemetry, so this page does **not** measure what
DevCloud's users ask for. It measures *revealed* demand: which of the AWS
services DevCloud does not register have already been built by three
independent projects serving the same populations DevCloud names as its
users.

| Source | What it is | Services |
|---|---|---|
| [moto](https://github.com/getmoto/moto) | Python AWS mocking library; the same population as DevCloud's primary user, boto3 developers running tests | 163 |
| [LocalStack](https://docs.localstack.cloud/references/coverage/) | Local AWS emulator; community and pro tiers combined | 119 |
| [terraform-provider-aws](https://github.com/hashicorp/terraform-provider-aws) | One Go package per AWS service; the IaC population | 273 |

Each of those projects adds a service because somebody asked for it. None
of them measured DevCloud's users. Read a high support count as *this
service is worth emulating to someone*, not as *our users want this*.

The live measurement of DevCloud's own traffic is
`GET /devcloud/api/unrouted`, which accrues behind this page. Its ceiling
is documented in [coverage.md](coverage.md).

## Readings

| Reading | Value |
|---|---|
| Upstream model files | 431 |
| Registered by DevCloud | 148 |
| **R1 — missing set `M`** | **283** |
| R2 — `M` with support 3 | 8 |
| R2 — `M` with support 2 | 49 |
| R2 — `M` with support 1 | 111 |
| R2 — `M` with support 0 | 115 |
| **R2 — `M` with support ≥ 2** | **57 (20.1% of `M`)** |

Join diagnostics — a name a source publishes that matches neither `M` nor a
registered DevCloud service. A large number here means the name
normalisation is dropping matches and the support counts are too low:

| Source | Unmatched names |
|---|---|
| moto | 0 |
| LocalStack | 4 |
| terraform-provider-aws | 4 |

## Ranking

Ordered by support count, then name. This is the order Milestone 4 worked
in, so stopping at any point would have stopped on the best surface available.

**All 57 services with support ≥ 2 are now registered** — the rows down to and
including `workspaces-web`. 54 of them serve at least one operation; the three
that do not (`elastic-load-balancing`, `rds-data`, `s3-control`) are named with
their reasons in [coverage.md](coverage.md). The rows below them, support 1 and
support 0, are the 226 services that remain explicitly not targeted.

| Service | moto | LocalStack | terraform-provider-aws | Support |
|---|---|---|---|---|
| `api-gateway` | yes | yes | yes | 3 |
| `elastic-load-balancing` | yes | yes | yes | 3 |
| `emr-serverless` | yes | yes | yes | 3 |
| `mediastore` | yes | yes | yes | 3 |
| `rds-data` | yes | yes | yes | 3 |
| `redshift-data` | yes | yes | yes | 3 |
| `s3-control` | yes | yes | yes | 3 |
| `timestream-query` | yes | yes | yes | 3 |
| `amp` | yes | — | yes | 2 |
| `apigatewaymanagementapi` | yes | yes | — | 2 |
| `app-mesh` | yes | — | yes | 2 |
| `budgets` | yes | — | yes | 2 |
| `cleanrooms` | yes | — | yes | 2 |
| `cloudhsm-v2` | yes | — | yes | 2 |
| `codestar-connections` | — | yes | yes | 2 |
| `connect` | yes | — | yes | 2 |
| `data-pipeline` | yes | — | yes | 2 |
| `databrew` | yes | — | yes | 2 |
| `datasync` | yes | — | yes | 2 |
| `dax` | yes | — | yes | 2 |
| `devops-agent` | yes | — | yes | 2 |
| `direct-connect` | yes | — | yes | 2 |
| `directory-service` | yes | — | yes | 2 |
| `dsql` | yes | — | yes | 2 |
| `emr-containers` | yes | — | yes | 2 |
| `fsx` | yes | — | yes | 2 |
| `greengrass` | yes | — | yes | 2 |
| `guardduty` | yes | — | yes | 2 |
| `inspector2` | yes | — | yes | 2 |
| `ivs` | yes | — | yes | 2 |
| `kinesis-analytics` | — | yes | yes | 2 |
| `kinesis-video` | yes | — | yes | 2 |
| `macie2` | yes | — | yes | 2 |
| `mediaconnect` | yes | — | yes | 2 |
| `medialive` | yes | — | yes | 2 |
| `mediapackage` | yes | — | yes | 2 |
| `mediapackagev2` | yes | — | yes | 2 |
| `mediastore-data` | yes | yes | — | 2 |
| `network-firewall` | yes | — | yes | 2 |
| `networkmanager` | yes | — | yes | 2 |
| `opensearchserverless` | yes | — | yes | 2 |
| `osis` | yes | — | yes | 2 |
| `payment-cryptography` | yes | — | yes | 2 |
| `quicksight` | yes | — | yes | 2 |
| `resiliencehub` | yes | — | yes | 2 |
| `route-53-domains` | yes | — | yes | 2 |
| `s3vectors` | yes | — | yes | 2 |
| `securityhub` | yes | — | yes | 2 |
| `service-catalog` | yes | — | yes | 2 |
| `service-catalog-appregistry` | yes | — | yes | 2 |
| `service-quotas` | yes | — | yes | 2 |
| `signer` | yes | — | yes | 2 |
| `synthetics` | yes | — | yes | 2 |
| `timestream-influxdb` | yes | — | yes | 2 |
| `vpc-lattice` | yes | — | yes | 2 |
| `workspaces` | yes | — | yes | 2 |
| `workspaces-web` | yes | — | yes | 2 |
| `accessanalyzer` | — | — | yes | 1 |
| `account-access` | — | — | yes | 1 |
| `agent-registry` | — | — | yes | 1 |
| `appconfigdata` | — | yes | — | 1 |
| `appfabric` | — | — | yes | 1 |
| `appflow` | — | — | yes | 1 |
| `appintegrations` | — | — | yes | 1 |
| `application-insights` | — | — | yes | 1 |
| `application-signals` | — | — | yes | 1 |
| `apprunner` | — | — | yes | 1 |
| `appstream` | — | — | yes | 1 |
| `arc-region-switch` | — | — | yes | 1 |
| `arc-zonal-shift` | — | — | yes | 1 |
| `auditmanager` | — | — | yes | 1 |
| `auto-scaling-plans` | — | — | yes | 1 |
| `bcm-data-exports` | — | — | yes | 1 |
| `billing` | — | — | yes | 1 |
| `chatbot` | — | — | yes | 1 |
| `chime` | — | — | yes | 1 |
| `chime-sdk-media-pipelines` | — | — | yes | 1 |
| `chime-sdk-voice` | — | — | yes | 1 |
| `cloud9` | — | — | yes | 1 |
| `clouddirectory` | yes | — | — | 1 |
| `cloudfront-keyvaluestore` | — | — | yes | 1 |
| `codecatalyst` | — | — | yes | 1 |
| `codestar-notifications` | — | — | yes | 1 |
| `compute-optimizer` | — | — | yes | 1 |
| `connectcampaigns` | yes | — | — | 1 |
| `connectcases` | — | — | yes | 1 |
| `controltower` | — | — | yes | 1 |
| `cost-and-usage-report-service` | — | — | yes | 1 |
| `cost-optimization-hub` | — | — | yes | 1 |
| `customer-profiles` | — | — | yes | 1 |
| `dataexchange` | — | — | yes | 1 |
| `datazone` | — | — | yes | 1 |
| `detective` | — | — | yes | 1 |
| `device-farm` | — | — | yes | 1 |
| `directory-service-data` | — | — | yes | 1 |
| `dlm` | — | — | yes | 1 |
| `docdb-elastic` | — | — | yes | 1 |
| `drs` | — | — | yes | 1 |
| `ec2-instance-connect` | yes | — | — | 1 |
| `ecr-public` | — | — | yes | 1 |
| `evs` | — | — | yes | 1 |
| `finspace` | — | — | yes | 1 |
| `fms` | — | — | yes | 1 |
| `gamelift` | — | — | yes | 1 |
| `global-accelerator` | — | — | yes | 1 |
| `grafana` | — | — | yes | 1 |
| `groundstation` | — | — | yes | 1 |
| `imagebuilder` | — | — | yes | 1 |
| `inspector` | — | — | yes | 1 |
| `interconnect` | — | — | yes | 1 |
| `internetmonitor` | — | — | yes | 1 |
| `invoicing` | — | — | yes | 1 |
| `ivschat` | — | — | yes | 1 |
| `kafkaconnect` | — | — | yes | 1 |
| `keyspaces` | — | — | yes | 1 |
| `kinesis-video-archived-media` | yes | — | — | 1 |
| `lambda-core` | — | — | yes | 1 |
| `lambda-microvms` | — | — | yes | 1 |
| `launch-wizard` | — | — | yes | 1 |
| `license-manager` | — | — | yes | 1 |
| `lightsail` | — | — | yes | 1 |
| `location` | — | — | yes | 1 |
| `m2` | — | — | yes | 1 |
| `mailmanager` | — | — | yes | 1 |
| `marketplace-metering` | yes | — | — | 1 |
| `mediapackage-vod` | — | — | yes | 1 |
| `mgn` | — | — | yes | 1 |
| `mpa` | — | — | yes | 1 |
| `mwaa-serverless` | — | — | yes | 1 |
| `neptune-graph` | — | — | yes | 1 |
| `networkflowmonitor` | — | — | yes | 1 |
| `networkmonitor` | — | — | yes | 1 |
| `notifications` | — | — | yes | 1 |
| `notificationscontacts` | — | — | yes | 1 |
| `oam` | — | — | yes | 1 |
| `observabilityadmin` | — | — | yes | 1 |
| `odb` | — | — | yes | 1 |
| `outposts` | — | — | yes | 1 |
| `pca-connector-ad` | — | — | yes | 1 |
| `pcs` | — | — | yes | 1 |
| `pinpoint-sms-voice-v2` | — | — | yes | 1 |
| `pricing` | — | — | yes | 1 |
| `rbin` | — | — | yes | 1 |
| `redshift-serverless` | — | — | yes | 1 |
| `resiliencehubv2` | — | — | yes | 1 |
| `resource-explorer-2` | — | — | yes | 1 |
| `rolesanywhere` | — | — | yes | 1 |
| `route53-recovery-control-config` | — | — | yes | 1 |
| `route53-recovery-readiness` | — | — | yes | 1 |
| `route53profiles` | — | — | yes | 1 |
| `rum` | — | — | yes | 1 |
| `s3files` | — | — | yes | 1 |
| `s3outposts` | — | — | yes | 1 |
| `savingsplans` | — | — | yes | 1 |
| `schemas` | — | — | yes | 1 |
| `securitylake` | — | — | yes | 1 |
| `simpledbv2` | yes | — | — | 1 |
| `ssm-contacts` | — | — | yes | 1 |
| `ssm-incidents` | — | — | yes | 1 |
| `ssm-quicksetup` | — | — | yes | 1 |
| `ssm-sap` | — | — | yes | 1 |
| `sso` | — | — | yes | 1 |
| `storage-gateway` | — | — | yes | 1 |
| `taxsettings` | — | — | yes | 1 |
| `uxc` | — | — | yes | 1 |
| `waf-regional` | — | — | yes | 1 |
| `wellarchitected` | — | — | yes | 1 |
| `workmail` | — | — | yes | 1 |
| `agent-registry-control` | — | — | — | 0 |
| `aiops` | — | — | — | 0 |
| `amplifybackend` | — | — | — | 0 |
| `amplifyuibuilder` | — | — | — | 0 |
| `application-discovery-service` | — | — | — | 0 |
| `applicationcostprofiler` | — | — | — | 0 |
| `artifact` | — | — | — | 0 |
| `b2bi` | — | — | — | 0 |
| `backup-gateway` | — | — | — | 0 |
| `backupsearch` | — | — | — | 0 |
| `bcm-dashboards` | — | — | — | 0 |
| `bcm-pricing-calculator` | — | — | — | 0 |
| `bcm-recommended-actions` | — | — | — | 0 |
| `billingconductor` | — | — | — | 0 |
| `braket` | — | — | — | 0 |
| `chime-sdk-identity` | — | — | — | 0 |
| `chime-sdk-meetings` | — | — | — | 0 |
| `chime-sdk-messaging` | — | — | — | 0 |
| `cleanroomsml` | — | — | — | 0 |
| `cloudhsm` | — | — | — | 0 |
| `cloudsearch-domain` | — | — | — | 0 |
| `cloudtrail-data` | — | — | — | 0 |
| `cloudwatch-events` | — | — | — | 0 |
| `cognito-sync` | — | — | — | 0 |
| `compute-optimizer-automation` | — | — | — | 0 |
| `connect-contact-lens` | — | — | — | 0 |
| `connectcampaignsv2` | — | — | — | 0 |
| `connecthealth` | — | — | — | 0 |
| `connectparticipant` | — | — | — | 0 |
| `controlcatalog` | — | — | — | 0 |
| `deadline` | — | — | — | 0 |
| `eks-auth` | — | — | — | 0 |
| `elementalinference` | — | — | — | 0 |
| `finspace-data` | — | — | — | 0 |
| `freetier` | — | — | — | 0 |
| `gameliftstreams` | — | — | — | 0 |
| `geo-maps` | — | — | — | 0 |
| `geo-places` | — | — | — | 0 |
| `geo-routes` | — | — | — | 0 |
| `greengrassv2` | — | — | — | 0 |
| `health` | — | — | — | 0 |
| `iam-toolbox` | — | — | — | 0 |
| `inspector-scan` | — | — | — | 0 |
| `iot-jobs-data-plane` | — | — | — | 0 |
| `iot-managed-integrations` | — | — | — | 0 |
| `iotdeviceadvisor` | — | — | — | 0 |
| `iotfleetwise` | — | — | — | 0 |
| `iotsecuretunneling` | — | — | — | 0 |
| `iotsitewise` | — | — | — | 0 |
| `iotthingsgraph` | — | — | — | 0 |
| `iottwinmaker` | — | — | — | 0 |
| `ivs-realtime` | — | — | — | 0 |
| `keyspacesstreams` | — | — | — | 0 |
| `kinesis-video-media` | — | — | — | 0 |
| `kinesis-video-signaling` | — | — | — | 0 |
| `kinesis-video-webrtc-storage` | — | — | — | 0 |
| `license-manager-linux-subscriptions` | — | — | — | 0 |
| `license-manager-user-subscriptions` | — | — | — | 0 |
| `machine-learning` | — | — | — | 0 |
| `managedblockchain-query` | — | — | — | 0 |
| `marketplace-agreement` | — | — | — | 0 |
| `marketplace-catalog` | — | — | — | 0 |
| `marketplace-commerce-analytics` | — | — | — | 0 |
| `marketplace-deployment` | — | — | — | 0 |
| `marketplace-discovery` | — | — | — | 0 |
| `marketplace-entitlement-service` | — | — | — | 0 |
| `marketplace-reporting` | — | — | — | 0 |
| `mediatailor` | — | — | — | 0 |
| `medical-imaging` | — | — | — | 0 |
| `migration-hub` | — | — | — | 0 |
| `migration-hub-refactor-spaces` | — | — | — | 0 |
| `migrationhub-config` | — | — | — | 0 |
| `migrationhuborchestrator` | — | — | — | 0 |
| `migrationhubstrategy` | — | — | — | 0 |
| `mturk` | — | — | — | 0 |
| `neptunedata` | — | — | — | 0 |
| `nova-act` | — | — | — | 0 |
| `partnercentral-account` | — | — | — | 0 |
| `partnercentral-benefits` | — | — | — | 0 |
| `partnercentral-channel` | — | — | — | 0 |
| `partnercentral-revenue-measurement` | — | — | — | 0 |
| `partnercentral-selling` | — | — | — | 0 |
| `payment-cryptography-data` | — | — | — | 0 |
| `pca-connector-scep` | — | — | — | 0 |
| `pi` | — | — | — | 0 |
| `pinpoint-email` | — | — | — | 0 |
| `pinpoint-sms-voice` | — | — | — | 0 |
| `pricing-plan-manager` | — | — | — | 0 |
| `proton` | — | — | — | 0 |
| `qconnect` | — | — | — | 0 |
| `repostspace` | — | — | — | 0 |
| `route53-recovery-cluster` | — | — | — | 0 |
| `route53globalresolver` | — | — | — | 0 |
| `rtbfabric` | — | — | — | 0 |
| `security-ir` | — | — | — | 0 |
| `securityagent` | — | — | — | 0 |
| `signer-data` | — | — | — | 0 |
| `signin` | — | — | — | 0 |
| `snow-device-management` | — | — | — | 0 |
| `snowball` | — | — | — | 0 |
| `socialmessaging` | — | — | — | 0 |
| `ssm-guiconnect` | — | — | — | 0 |
| `sso-oidc` | — | — | — | 0 |
| `supplychain` | — | — | — | 0 |
| `support-app` | — | — | — | 0 |
| `supportauthz` | — | — | — | 0 |
| `sustainability` | — | — | — | 0 |
| `tnb` | — | — | — | 0 |
| `trustedadvisor` | — | — | — | 0 |
| `wickr` | — | — | — | 0 |
| `wisdom` | — | — | — | 0 |
| `workdocs` | — | — | — | 0 |
| `workmailmessageflow` | — | — | — | 0 |
| `workspaces-instances` | — | — | — | 0 |
| `workspaces-thin-client` | — | — | — | 0 |
