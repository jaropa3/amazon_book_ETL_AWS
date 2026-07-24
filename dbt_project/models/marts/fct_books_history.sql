{{ config(
    materialized='incremental',
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
