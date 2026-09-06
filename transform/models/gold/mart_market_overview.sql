{{ config(materialized='table') }}

-- Grain: one row per (date, currency).
--
-- Market-wide breadth and totals across the tracked universe.

with daily as (

    select * from {{ ref('fct_coin_price_daily') }}

),

by_date as (

    select
        date_key,
        price_date,
        count(distinct coin_id)                                     as tracked_coins,
        sum(market_cap_usd)                                         as total_market_cap_usd,
        sum(total_volume_usd)                                       as total_volume_usd,
        cast(avg(daily_return_pct) as decimal(38, 18))              as avg_daily_return_pct,
        cast(median(daily_return_pct) as decimal(38, 18))           as median_daily_return_pct,
        count(*) filter (where daily_return_pct > 0)                as advancing_coins,
        count(*) filter (where daily_return_pct < 0)                as declining_coins,
        arg_max(coin_id, daily_return_pct)                          as top_gainer_coin_id,
        max(daily_return_pct)                                       as top_gainer_return_pct,
        arg_min(coin_id, daily_return_pct)                          as top_loser_coin_id,
        min(daily_return_pct)                                       as top_loser_return_pct
    from daily
    group by date_key, price_date

)

select
    by_date.date_key,
    by_date.price_date,
    fx.currency_key,
    fx.quote_currency                                               as currency_code,
    dim_currency.currency_name,
    by_date.tracked_coins,
    by_date.total_market_cap_usd,
    {{ to_local('by_date.total_market_cap_usd', 'fx.rate_to_usd') }}
                                                                    as total_market_cap_local,
    by_date.total_volume_usd,
    {{ to_local('by_date.total_volume_usd', 'fx.rate_to_usd') }}
                                                                    as total_volume_local,
    by_date.avg_daily_return_pct,
    by_date.median_daily_return_pct,
    by_date.advancing_coins,
    by_date.declining_coins,
    by_date.top_gainer_coin_id,
    by_date.top_gainer_return_pct,
    by_date.top_loser_coin_id,
    by_date.top_loser_return_pct,
    fx.rate_to_usd,
    fx.is_filled                                                    as is_filled_rate
from by_date
-- Grain-defining: fans each day across every reporting currency.
inner join {{ ref('fct_fx_rate_daily') }} as fx
    on fx.date_key = by_date.date_key
-- Attribute lookup only; the fact side is the anchor.
left join {{ ref('dim_currency') }} as dim_currency
    on dim_currency.currency_key = fx.currency_key
