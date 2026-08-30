-- Grain: one row per (coin_id, snapshot_ts).
--
-- Cleansing, typing and dedupe only. No joins to conformed dimensions and no
-- business logic; that belongs in gold.

with source as (

    select * from {{ ref('br_coins_markets') }}

),

typed as (

    select
        lower(trim(id))                                     as coin_id,
        upper(trim(symbol))                                 as symbol,
        trim(name)                                          as name,
        cast(_ingested_at as timestamptz)                   as snapshot_ts,
        cast(cast(_ingested_at as timestamptz) as date)     as snapshot_date,

        cast(current_price as decimal(38, 18))              as price_usd,
        cast(market_cap as decimal(38, 18))                 as market_cap_usd,
        cast(fully_diluted_valuation as decimal(38, 18))    as fully_diluted_valuation_usd,
        cast(total_volume as decimal(38, 18))               as total_volume_usd,
        cast(high_24h as decimal(38, 18))                   as high_24h_usd,
        cast(low_24h as decimal(38, 18))                    as low_24h_usd,
        cast(price_change_24h as decimal(38, 18))           as price_change_24h_usd,
        cast(price_change_percentage_24h as decimal(38, 18))            as price_change_percentage_24h,
        cast(price_change_percentage_7d_in_currency as decimal(38, 18)) as price_change_percentage_7d,
        cast(price_change_percentage_30d_in_currency as decimal(38, 18)) as price_change_percentage_30d,
        cast(market_cap_change_24h as decimal(38, 18))      as market_cap_change_24h_usd,
        cast(market_cap_change_percentage_24h as decimal(38, 18)) as market_cap_change_percentage_24h,
        cast(circulating_supply as decimal(38, 18))         as circulating_supply,
        cast(total_supply as decimal(38, 18))               as total_supply,
        cast(max_supply as decimal(38, 18))                 as max_supply,
        cast(ath as decimal(38, 18))                        as ath_usd,
        cast(ath_change_percentage as decimal(38, 18))      as ath_change_percentage,
        cast(atl as decimal(38, 18))                        as atl_usd,
        cast(atl_change_percentage as decimal(38, 18))      as atl_change_percentage,

        cast(market_cap_rank as integer)                    as market_cap_rank,
        try_cast(ath_date as timestamptz)                   as ath_date,
        try_cast(atl_date as timestamptz)                   as atl_date,
        -- Per-coin and frequently weeks stale. Retained for reference only;
        -- snapshot_ts is the pipeline's own, reliable timestamp.
        try_cast(last_updated as timestamptz)               as source_last_updated,
        _dlt_load_id

    from source
    -- A coin with no price carries no analytical signal.
    where current_price is not null

)

select
    coin_id,
    symbol,
    name,
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
    circulating_supply,
    total_supply,
    max_supply,
    ath_usd,
    ath_change_percentage,
    atl_usd,
    atl_change_percentage,
    market_cap_rank,
    ath_date,
    atl_date,
    source_last_updated
from typed
-- A retried dlt load can write the same snapshot twice; keep the latest load.
qualify row_number() over (
    partition by coin_id, snapshot_ts
    order by _dlt_load_id desc
) = 1
