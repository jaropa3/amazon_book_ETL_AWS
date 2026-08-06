aws logs tail /aws/lambda/amazon-books-scraper --since 3h \
  --profile amazon-books-etl-dev --region eu-central-1 2>&1 | tail -25