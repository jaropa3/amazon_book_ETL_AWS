with source as (
    select * from {{ source('bronze', 'raw') }}
)

select
    asin,
    title,
    author,
    cast(nullif(nullif(replace(replace(replace(replace(price, '$', ''), 'PLN', ''), 'EUR', ''), chr(160), ''), '0.00'), '') as double) as price,
    cast(nullif(split_part(rating, ' ', 1), '') as double) as rating,
    cast(from_iso8601_timestamp(scraped_at) at time zone 'UTC' as timestamp) as scraped_at
from source
where asin is not null
