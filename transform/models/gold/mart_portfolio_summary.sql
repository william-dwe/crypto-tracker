{{ config(materialized='table') }}

-- Grain: one row per (date, currency).
--
-- Whole-portfolio rollup. Aggregates the per-holding datamart rather than
-- re-deriving from facts, so the two can never disagree.

with daily as (

    select * from {{ ref('mart_portfolio_value_daily') }}

),

aggregated as (

    select
        date_key,
        price_date,
        currency_key,
        currency_code,
        currency_name,
        count(distinct holding_id)                              as holdings_count,
        sum(position_value_usd)                                 as total_value_usd,
        sum(position_value_local)                               as total_value_local,
        sum(cost_basis_usd)                                     as total_cost_basis_usd,
        sum(cost_basis_local)                                   as total_cost_basis_local,
        sum(unrealized_pnl_usd)                                 as total_unrealized_pnl_usd,
        sum(position_value_local) - sum(cost_basis_local)       as total_unrealized_pnl_local,
        bool_or(is_filled_rate)                                 as has_filled_rate
    from daily
    group by date_key, price_date, currency_key, currency_code, currency_name

)

select
    date_key,
    price_date,
    currency_key,
    currency_code,
    currency_name,
    holdings_count,
    total_value_usd,
    total_value_local,
    total_cost_basis_usd,
    total_cost_basis_local,
    total_unrealized_pnl_usd,
    total_unrealized_pnl_local,
    cast(
        ({{ money('total_value_usd') }}
            / nullif({{ money('total_cost_basis_usd') }}, 0) - 1) * 100
        as decimal(18, 6)
    )                                                           as total_unrealized_pnl_pct,
    lag(total_value_local) over (
        partition by currency_code order by price_date
    )                                                           as prev_total_value_local,
    cast(
        (
            {{ money('total_value_local') }}
            / nullif({{ money('lag(total_value_local) over (partition by currency_code order by price_date)') }}, 0)
            - 1
        ) * 100 as decimal(18, 6)
    )                                                           as day_over_day_change_pct,
    has_filled_rate
from aggregated
