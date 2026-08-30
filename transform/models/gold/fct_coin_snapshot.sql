{{
    config(
        materialized='incremental',
        unique_key=['coin_id', 'snapshot_ts'],
        incremental_strategy='delete+insert'
    )
}}

-- Grain: one row per (coin, snapshot timestamp).
--
-- Intraday market state, deliberately separate from fct_coin_price_daily. The
-- daily fact answers "what did this coin close at"; this one answers "what did
-- the market look like when we last polled", including 24h highs/lows and rank
-- that the history endpoint does not expose.

with snapshots as (

    select * from {{ ref('stg_coin_snapshots') }}

    {% if is_incremental() %}
    where snapshot_ts > (
        select coalesce(max(snapshot_ts), timestamptz '1970-01-01 00:00:00+00')
        from {{ this }}
    )
    {% endif %}

)

select
    cast(md5(coin_id) as varchar)                       as coin_key,
    cast(strftime(snapshot_date, '%Y%m%d') as integer)  as date_key,
    coin_id,
    snapshot_ts,
    snapshot_date,
    price_usd,
    market_cap_usd,
    fully_diluted_valuation_usd,
    total_volume_usd,
    high_24h_usd,
    low_24h_usd,
    price_change_24h_usd,
    price_change_percentage_24h,
    price_change_percentage_7d,
    price_change_percentage_30d,
    market_cap_change_24h_usd,
    market_cap_change_percentage_24h,
    market_cap_rank,
    circulating_supply,
    total_supply,
    ath_usd,
    ath_change_percentage,
    atl_usd,
    atl_change_percentage
from snapshots
