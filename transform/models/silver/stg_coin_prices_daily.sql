-- Grain: one row per (coin_id, price_date). The history spine every daily fact
-- and datamart is built on.

with source as (

    select * from {{ ref('br_coin_market_chart') }}

),

typed as (

    select
        lower(trim(coin_id))                        as coin_id,
        cast(price_date as date)                    as price_date,
        cast(price_ts_ms as bigint)                 as price_ts_ms,
        cast(price_usd as decimal(38, 18))          as price_usd,
        cast(market_cap_usd as decimal(38, 18))     as market_cap_usd,
        cast(total_volume_usd as decimal(38, 18))   as total_volume_usd,
        cast(_ingested_at as timestamptz)           as ingested_at
    from source
    where price_usd is not null
      and cast(price_usd as decimal(38, 18)) > 0

)

select
    coin_id,
    price_date,
    price_ts_ms,
    price_usd,
    market_cap_usd,
    total_volume_usd,
    ingested_at
from typed
-- CoinGecko finalises a UTC day only after it closes; today's point is intraday
-- and would change on every run, so daily facts stop at yesterday.
where price_date < current_date
-- The same day can arrive from both the 365-day backfill and a later 2-day
-- incremental window. Keep the most recently ingested value.
qualify row_number() over (
    partition by coin_id, price_date
    order by ingested_at desc, price_ts_ms desc
) = 1
