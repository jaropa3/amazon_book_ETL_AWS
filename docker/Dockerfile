FROM python:3.11-slim

WORKDIR /app

COPY docker/requirements-dbt.txt .
RUN pip install --no-cache-dir -r requirements-dbt.txt

COPY dbt_project/ ./dbt_project/
COPY docker/profiles.yml ./

# `build`, not `run`: it executes models and their data tests in DAG order, so a failing test
# stops downstream models and exits non-zero. With plain `run` the tests in schema.yml were
# never executed by the orchestrator, which is how a 110x duplication in fct_books_history
# survived two weeks unnoticed.
CMD ["dbt", "build", "--project-dir", "dbt_project", "--profiles-dir", ".", "--target", "athena"]
