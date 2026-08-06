aws stepfunctions list-executions \
  --state-machine-arn arn:aws:states:eu-central-1:915238109570:stateMachine:amazon-books-pipeline \
  --max-results 10 --profile amazon-books-etl-dev --region eu-central-1 --output table \
  --query 'executions[].{Name:name,Status:status,Start:startDate,Stop:stopDate}'