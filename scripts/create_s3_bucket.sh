#!/usr/bin/env bash
set -euo pipefail

BUCKET="amazon-books-etl-aws-915238109570"
PROFILE="amazon-books-etl-dev"
REGION="eu-central-1"

aws s3api create-bucket --bucket "$BUCKET" --region "$REGION" \
  --create-bucket-configuration LocationConstraint="$REGION" --profile "$PROFILE"

aws s3api put-public-access-block --bucket "$BUCKET" \
  --public-access-block-configuration BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true \
  --profile "$PROFILE"

aws s3api put-bucket-tagging --bucket "$BUCKET" \
  --tagging 'TagSet=[{Key=Project,Value=amazon-books-etl},{Key=Environment,Value=dev},{Key=Owner,Value=jarek}]' \
  --profile "$PROFILE"

aws s3api put-bucket-lifecycle-configuration --bucket "$BUCKET" \
  --lifecycle-configuration '{"Rules":[{"ID":"abort-incomplete-mpu","Status":"Enabled","Filter":{},"AbortIncompleteMultipartUpload":{"DaysAfterInitiation":7}}]}' \
  --profile "$PROFILE"