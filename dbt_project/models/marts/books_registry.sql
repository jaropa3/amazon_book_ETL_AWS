-- Iceberg here follows fct_books_history upstream, it is not an independent preference:
-- Iceberg stores timestamps as timestamp(6) and a Hive/Parquet CTAS only accepts milliseconds,
-- so reading scraped_at from the fact into a Hive table fails outright. Casting down to
-- timestamp(3) would hide a format mismatch behind silent precision loss.
{{ config(materialized='table', table_type='iceberg') }}

with history as (
    select * from {{ ref('fct_books_history') }}
),

ranked as (
    select
        *,
        row_number() over (
            partition by asin
            order by scraped_at desc
        ) as rn
    from history
)

select
    asin,
    title,
    author,
    scraped_at as last_seen_at
from ranked
where rn = 1
