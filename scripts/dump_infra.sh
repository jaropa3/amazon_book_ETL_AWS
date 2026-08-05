#!/usr/bin/env bash
# Dumps live AWS resource definitions into infra/ as version-controlled snapshots.
# Run after every console change, then `git diff infra/` to see what moved.
set -euo pipefail

PROFILE="${AWS_PROFILE:-amazon-books-etl-dev}"
REGION="${AWS_REGION:-eu-central-1}"
ACCOUNT_ID="915238109570"
INFRA_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/infra"

# Atomic write: build in a temp file, rename only on success — an interrupted dump
# must never leave a truncated snapshot that looks like a real infrastructure change.
dump() {
    local target="$INFRA_DIR/$1"
    shift
    local tmp="${target}.tmp"
    "$@" >"$tmp"
    mv "$tmp" "$target"
    echo "  ${target##*/}"
}

state_machine() {
    aws stepfunctions describe-state-machine \
        --state-machine-arn "arn:aws:states:${REGION}:${ACCOUNT_ID}:stateMachine:amazon-books-pipeline" \
        --profile "$PROFILE" --region "$REGION" \
        --query 'definition' --output text | python3 -m json.tool
}

schedule_rule() {
    aws events describe-rule --name amazon-books-schedule \
        --profile "$PROFILE" --region "$REGION" \
        --query '{Name:Name,ScheduleExpression:ScheduleExpression,State:State,Description:Description}'
}

task_definition() {
    # Registered revisions are immutable, so drop the fields AWS stamps per revision —
    # otherwise every dump produces a diff even when nothing was actually changed.
    aws ecs describe-task-definition --task-definition dbt-runner \
        --profile "$PROFILE" --region "$REGION" \
        --query 'taskDefinition.{family:family,cpu:cpu,memory:memory,networkMode:networkMode,
                 requiresCompatibilities:requiresCompatibilities,executionRoleArn:executionRoleArn,
                 taskRoleArn:taskRoleArn,containerDefinitions:containerDefinitions}'
}

echo "Dumping infrastructure snapshots (profile=$PROFILE region=$REGION)"
dump stepfunctions-amazon-books-pipeline.json state_machine
dump eventbridge-amazon-books-schedule.json schedule_rule
dump ecs-taskdef-dbt-runner.json task_definition
echo "Done. Review with: git diff infra/"
