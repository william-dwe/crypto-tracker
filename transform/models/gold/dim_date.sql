{{ config(materialized='table') }}

-- Grain: one calendar day.
--
-- Spans the observed price history and extends a year past it so that portfolio
-- rows and any forward-looking reporting still resolve a date key.

with bounds as (

    select
        min(price_date)                     as min_date,
        max(price_date) + interval 1 year   as max_date
    from {{ ref('stg_coin_prices_daily') }}

),

spine as (

    select cast(generated as date) as full_date
    from bounds,
         unnest(generate_series(bounds.min_date, cast(bounds.max_date as date), interval 1 day))
             as t(generated)

)

select
    cast(strftime(full_date, '%Y%m%d') as integer)  as date_key,
    full_date,
    cast(year(full_date) as integer)                as year,
    cast(quarter(full_date) as integer)             as quarter,
    cast(month(full_date) as integer)               as month,
    monthname(full_date)                            as month_name,
    cast(day(full_date) as integer)                 as day,
    -- DuckDB dayofweek: 0 = Sunday .. 6 = Saturday.
    cast(dayofweek(full_date) as integer)           as day_of_week,
    dayname(full_date)                              as day_name,
    dayofweek(full_date) in (0, 6)                  as is_weekend,
    cast(week(full_date) as integer)                as iso_week,
    cast(dayofyear(full_date) as integer)           as day_of_year
from spine
