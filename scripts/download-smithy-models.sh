#!/usr/bin/env bash
# scripts/download-smithy-models.sh
# Downloads AWS Smithy JSON models from aws-sdk-go-v2 repository.
set -euo pipefail

MODELS_DIR="${1:-./smithy-models}"
BASE_URL="https://raw.githubusercontent.com/aws/aws-sdk-go-v2/main/codegen/sdk-codegen/aws-models"

# Complete list of services to download, organized by DevCloud tiers.
SERVICES=(
  # Existing
  s3 sqs dynamodb lambda iam sts
  # Tier 1
  sns kms secretsmanager ssm logs monitoring events ec2 ecs ecr route53 acm
  # Tier 2
  cognito-identity cognito-identity-provider elasticloadbalancingv2 ebs efs
  sfn apigateway apigatewayv2 kinesis firehose ses sesv2 rds cloudformation
  # Tier 3
  elasticache cloudfront wafv2 glue athena organizations cloudtrail eks
  autoscaling appsync emr batch
  # Tier 4 (extended coverage)
  amplify appconfig application-auto-scaling backup bedrock
  cloud-control cloudsearch
  codeartifact codebuild codecommit codedeploy codepipeline
  config-service cost-explorer dms docdb dynamodb-streams
  elastic-beanstalk elasticsearch-service
  eventbridge-pipes fis glacier identity-store
  iot iot-data-plane iot-wireless
  kafka kinesis-analytics-v2 lakeformation
  managedblockchain memorydb mq mwaa neptune
  opensearch pinpoint acm-pca redshift
  ram resource-groups resource-groups-tagging-api
  route53resolver sagemaker
  serverlessrepo servicediscovery shield
  sso-admin support swf textract
  timestream-write transcribe transfer
  verified-permissions waf xray
)

mkdir -p "$MODELS_DIR"

for service in "${SERVICES[@]}"; do
  dest="${MODELS_DIR}/${service}.json"
  if [ -f "$dest" ]; then
    echo "SKIP $service (exists)"
    continue
  fi
  echo "Downloading $service..."
  curl -sfL "${BASE_URL}/${service}.json" -o "$dest" 2>/dev/null || {
    echo "WARN: failed to download $service, skipping"
    rm -f "$dest"
  }
done

echo "Done. $(ls "$MODELS_DIR"/*.json 2>/dev/null | wc -l | tr -d ' ') models downloaded."
