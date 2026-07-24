FROM python:3.11-slim

WORKDIR /app

COPY docker/requirements-dbt.txt .
RUN pip install --no-cache-dir -r requirements-dbt.txt

COPY dbt_project/ ./dbt_project/
COPY docker/profiles.yml ./

CMD ["dbt", "run", "--project-dir", "dbt_project", "--profiles-dir", ".", "--target", "athena"]
