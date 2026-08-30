{{
    config(
        materialized='incremental',
        unique_key=['coin_id', 'price_date'],
        incremental_strategy='delete+insert'
    )
}}

-- Grain: one row per (coin, date). The core fact of the star schema.
--
-- delete+insert rather than merge because whole-day partitions are replaced
-- wholesale, and the lag() window needs preceding rows to be present.

with prices as (

    select * from {{ ref('stg_coin_prices_daily') }}

    {% if is_incremental() %}
    -- Reach back beyond the incremental window so lag() sees real preceding
    -- values instead of restarting the series mid-history and emitting a
    -- spurious NULL return on the first reprocessed day.
    where price_date >= (
        select coalesce(max(price_date) - interval 7 day, date '1970-01-01')
        from {{ this }}
    )
    {% endif %}

),

with_lag as (

    select
        coin_id,
        price_date,
        price_usd,
        market_cap_usd,
        total_volume_usd,
        lag(price_usd) over (partition by coin_id order by price_date) as prev_price_usd
    from prices

)

select
    cast(md5(coin_id) as varchar)                       as coin_key,
    cast(strftime(price_date, '%Y%m%d') as integer)     as date_key,
    coin_id,
    price_date,
    price_usd,
    market_cap_usd,
    total_volume_usd,
    prev_price_usd,
    price_usd - prev_price_usd                          as price_change_usd,
    cast(
        ({{ money('price_usd') }} / nullif({{ money('prev_price_usd') }}, 0) - 1) * 100
        as decimal(18, 6)
    )                                                   as daily_return_pct
from with_lag
