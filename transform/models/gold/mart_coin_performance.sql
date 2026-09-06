{{ config(materialized='table') }}

-- Grain: one row per (coin, currency), as of the latest available price date.
--
-- Trailing returns are computed against the price N rows back in each coin's own
-- daily series. Because stg_coin_prices_daily is a gapless calendar spine per
-- coin, "N rows back" is equivalent to "N days back" without a date join.

with prices as (

    select * from {{ ref('fct_coin_price_daily') }}

),

windowed as (

    select
        coin_key,
        coin_id,
        price_date,
        price_usd,
        market_cap_usd,
        total_volume_usd,
        daily_return_pct,
        lag(price_usd, 7)   over (partition by coin_id order by price_date) as price_7d_ago,
        lag(price_usd, 30)  over (partition by coin_id order by price_date) as price_30d_ago,
        lag(price_usd, 90)  over (partition by coin_id order by price_date) as price_90d_ago,
        stddev_samp(daily_return_pct) over (
            partition by coin_id order by price_date
            rows between 29 preceding and current row
        )                                                                   as volatility_30d_pct,
        count(*) over (partition by coin_id)                                as days_of_history
    from prices

),

latest as (

    select *
    from windowed
    qualify row_number() over (partition by coin_id order by price_date desc) = 1

),

latest_rates as (

    -- A filter, not a join: restrict the FX fact to the newest priced date.
    -- Expressed as a scalar subquery so no join is introduced at all.
    select *
    from {{ ref('fct_fx_rate_daily') }}
    where rate_date = (select max(price_date) from latest)

)
,

-- market_cap_rank is only published on the snapshot endpoint, so it comes from
-- the snapshot fact rather than the daily price fact.
latest_rank as (

    select
        coin_id,
        market_cap_rank
    from {{ ref('fct_coin_snapshot') }}
    qualify row_number() over (partition by coin_id order by snapshot_ts desc) = 1

)

select
    latest.coin_key,
    latest_rates.currency_key,
    latest.coin_id,
    dim_coin.symbol,
    dim_coin.name                                   as coin_name,
    dim_coin.is_active,
    latest.price_date                               as as_of_date,
    latest_rates.quote_currency                     as currency_code,
    dim_currency.currency_name,
    latest.price_usd                                as latest_price_usd,
    {{ to_local('latest.price_usd', 'latest_rates.rate_to_usd') }}
                                                    as latest_price_local,
    latest.market_cap_usd,
    latest_rank.market_cap_rank,
    {{ to_local('latest.market_cap_usd', 'latest_rates.rate_to_usd') }}
                                                    as market_cap_local,
    latest.total_volume_usd,
    latest.daily_return_pct                         as return_1d_pct,
    cast(({{ money('latest.price_usd') }} / nullif({{ money('latest.price_7d_ago') }}, 0) - 1) * 100 as decimal(18, 6))
                                                    as return_7d_pct,
    cast(({{ money('latest.price_usd') }} / nullif({{ money('latest.price_30d_ago') }}, 0) - 1) * 100 as decimal(18, 6))
                                                    as return_30d_pct,
    cast(({{ money('latest.price_usd') }} / nullif({{ money('latest.price_90d_ago') }}, 0) - 1) * 100 as decimal(18, 6))
                                                    as return_90d_pct,
    cast(latest.volatility_30d_pct as decimal(18, 6)) as volatility_30d_pct,
    latest.days_of_history,
    latest_rates.rate_to_usd
from latest
-- Grain-defining: deliberately fans every coin across every reporting currency.
cross join latest_rates
-- Lookups, anchored on the fact. market_cap_rank is genuinely optional (a coin
-- may have price history but no snapshot yet); the dimension joins are left for
-- the same reason as elsewhere — a missing label must never delete a coin's
-- performance row.
left join latest_rank
    on latest_rank.coin_id = latest.coin_id
left join {{ ref('dim_coin') }} as dim_coin
    on dim_coin.coin_key = latest.coin_key
left join {{ ref('dim_currency') }} as dim_currency
    on dim_currency.currency_key = latest_rates.currency_key
