{{ config(materialized='table') }}

-- Grain: one row per (holding, date, currency).
--
-- Datamart: reads exclusively from the star schema (dims + facts), never from
-- silver or bronze. That restriction is what makes the star schema load-bearing
-- rather than decorative.

with holdings as (

    select
        holding_id,
        lower(trim(coin_id))                    as coin_id,
        cast(quantity as decimal(38, 18))       as quantity,
        acquired_date,
        cast(cost_basis_usd as decimal(38, 18)) as cost_basis_usd
    from {{ ref('portfolio_holdings') }}

),

positions_usd as (

    select
        holdings.holding_id,
        holdings.coin_id,
        holdings.quantity,
        holdings.acquired_date,
        holdings.cost_basis_usd,
        prices.coin_key,
        prices.date_key,
        prices.price_date,
        prices.price_usd,
        prices.daily_return_pct,
        {{ position_value('holdings.quantity', 'prices.price_usd') }} as position_value_usd
    from holdings
    -- Grain-defining, not a lookup: this join is what expands one lot into one
    -- row per day it was held. Inner is correct — a day with no price is a day
    -- with no valuation, and a left join would emit NULL-valued phantom rows.
    inner join {{ ref('fct_coin_price_daily') }} as prices
        on  prices.coin_id = holdings.coin_id
        -- A position only exists from the day it was acquired.
        and prices.price_date >= holdings.acquired_date

),

-- Fan out each USD position across every reporting currency for that date.
converted as (

    select
        positions_usd.*,
        fx.currency_key,
        fx.quote_currency   as currency_code,
        fx.rate_to_usd,
        fx.is_filled        as is_filled_rate
    from positions_usd
    -- Also grain-defining: fans each USD position across every reporting
    -- currency. stg_fx_rates_filled guarantees a rate on every date in the
    -- price spine, so this drops nothing.
    inner join {{ ref('fct_fx_rate_daily') }} as fx
        on fx.date_key = positions_usd.date_key

)

select
    converted.holding_id,
    converted.coin_key,
    converted.currency_key,
    converted.date_key,
    converted.coin_id,
    dim_coin.symbol,
    dim_coin.name                                   as coin_name,
    converted.price_date,
    converted.currency_code,
    dim_currency.currency_name,
    converted.quantity,
    converted.acquired_date,
    converted.price_usd,
    converted.position_value_usd,
    converted.cost_basis_usd,
    converted.rate_to_usd,
    converted.is_filled_rate,
    converted.daily_return_pct,

    {{ to_local('converted.price_usd', 'converted.rate_to_usd') }}
        as price_local,
    {{ to_local('converted.position_value_usd', 'converted.rate_to_usd') }}
        as position_value_local,
    {{ to_local('converted.cost_basis_usd', 'converted.rate_to_usd') }}
        as cost_basis_local,
    converted.position_value_usd - converted.cost_basis_usd
        as unrealized_pnl_usd,
    cast(
        ({{ money('converted.position_value_usd') }}
            / nullif({{ money('converted.cost_basis_usd') }}, 0) - 1) * 100
        as decimal(18, 6)
    )   as unrealized_pnl_pct

from converted
-- Pure attribute lookups. The fact side is the anchor and already fixes the row
-- set, so these are left joins: a missing dimension row must surface as a NULL
-- label for the relationships test to catch, never as a silently dropped
-- valuation.
left join {{ ref('dim_coin') }} as dim_coin
    on dim_coin.coin_key = converted.coin_key
left join {{ ref('dim_currency') }} as dim_currency
    on dim_currency.currency_key = converted.currency_key
