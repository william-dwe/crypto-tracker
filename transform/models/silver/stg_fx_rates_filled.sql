{{ config(materialized='table') }}

-- Grain: one row per (rate_date, quote_currency), on a *calendar-day* spine.
--
-- ECB publishes only on TARGET business days: roughly 255 dates across a 365-day
-- span. Crypto trades every day, so joining daily crypto facts straight to
-- published FX rates yields NULL on every weekend and holiday. This model
-- forward-fills the last published rate across those gaps and flags which rows
-- were carried forward.

with calendar as (

    -- The crypto history spine defines which calendar days need a rate.
    select distinct price_date as rate_date
    from {{ ref('stg_coin_prices_daily') }}

),

currencies as (

    select distinct quote_currency, base_currency
    from {{ ref('stg_fx_rates') }}

),

spine as (

    select
        calendar.rate_date,
        currencies.base_currency,
        currencies.quote_currency
    from calendar
    cross join currencies

),

joined as (

    select
        spine.rate_date,
        spine.base_currency,
        spine.quote_currency,
        fx.rate_to_usd as published_rate
    from spine
    left join {{ ref('stg_fx_rates') }} as fx
        on  fx.rate_date       = spine.rate_date
        and fx.base_currency   = spine.base_currency
        and fx.quote_currency  = spine.quote_currency

),

filled as (

    select
        rate_date,
        base_currency,
        quote_currency,
        published_rate,
        coalesce(
            published_rate,
            last_value(published_rate ignore nulls) over (
                partition by base_currency, quote_currency
                order by rate_date
                rows between unbounded preceding and current row
            )
        ) as rate_to_usd
    from joined

)

select
    rate_date,
    base_currency,
    quote_currency,
    rate_to_usd,
    published_rate is null as is_filled
from filled
-- Dates before a currency's first published rate have nothing to carry forward.
where rate_to_usd is not null
