-- Iceberg + merge is not a preference here, it is the only combination on Athena in which
-- `unique_key` does anything: dbt-athena ignores it under every other strategy, and on a Hive
-- table it silently downgrades `insert_overwrite` to `append` when `partitioned_by` is absent
-- (see incremental.sql, "we fall back to append mode"). That downgrade grew this table to
-- 1.2M rows over 11k distinct keys before it was caught.
{{ config(
    materialized='incremental',
    table_type='iceberg',
    incremental_strategy='merge',
    unique_key='book_sk'
) }}

select
    to_hex(md5(to_utf8(asin || '_' || cast(scraped_at as varchar)))) as book_sk,
    asin,
    title,
    author,
    price,
    rating,
    scraped_at
from {{ ref('int_books') }}
