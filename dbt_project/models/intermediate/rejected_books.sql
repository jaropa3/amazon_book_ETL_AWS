-- Same trap as fct_books_history (see its header): on Athena, `unique_key` is honoured only
-- under Iceberg + merge. As a Hive table this model silently appended instead of merging and
-- reached 46k rows over 533 distinct keys before it was caught. Iceberg also fixes two other
-- things here: the table lands under `s3_data_dir` (not the query-results prefix a lifecycle
-- rule is meant to expire), and writes commit atomically instead of exposing a half-written
-- table to concurrent readers.
{{ config(
    materialized='incremental',
    table_type='iceberg',
    incremental_strategy='merge',
    unique_key='rejected_sk'
) }}

-- Rejestr odrzuconych rekordów — AKUMULUJE historię wszystkich sesji.
-- Incremental (nie table), bo bronze trzyma tylko bieżącą sesję: 'table' kasowałby
-- odrzuty poprzednich runów. Brak filtra scraped_at (jak w gold) — chroni late data.
-- rejected_sk = hash treści wiersza → idempotentny merge (reprocessing nie dubluje).

with rejected as (
    -- rekordy z bronze odrzucone w staging (brak asin)
    select
        cast(null as varchar) as asin,
        title,
        author,
        price,
        cast(from_iso8601_timestamp(scraped_at) at time zone 'UTC' as timestamp) as scraped_at,
        'brak asin' as rejection_reason
    from {{ source('bronze', 'raw') }}
    where asin is null

    union all

    -- duplikaty w ramach tej samej sesji odrzucone w int_books
    select
        asin,
        title,
        author,
        cast(price as varchar),
        scraped_at,
        'duplikat w sesji (asin+scraped_at)' as rejection_reason
    from (
        select
            *,
            row_number() over (
                partition by asin, scraped_at
                order by (price is null) asc, (rating is null) asc
            ) as rn
        from {{ ref('stg_books') }}
    ) ranked
    where rn > 1
),

keyed as (
    select
        to_hex(md5(to_utf8(
            coalesce(asin, '') || '|' || coalesce(title, '') || '|' || coalesce(author, '')
            || '|' || coalesce(cast(price as varchar), '') || '|' || coalesce(cast(scraped_at as varchar), '')
            || '|' || rejection_reason
        ))) as rejected_sk,
        asin,
        title,
        author,
        price,
        scraped_at,
        rejection_reason
    from rejected
)

-- Amazon can return the same listing several times within one session, so rn=2 and rn=3 above
-- hash identically. Every column feeds the hash, so a repeated key means a byte-identical row —
-- collapsing costs no information, and merge rejects the duplicates outright
-- (MERGE_TARGET_ROW_MULTIPLE_MATCHES). Grain: one row per distinct rejected record.
select
    rejected_sk,
    asin,
    title,
    author,
    price,
    scraped_at,
    rejection_reason
from (
    select *, row_number() over (partition by rejected_sk order by scraped_at) as rn_key
    from keyed
) deduped
where rn_key = 1
