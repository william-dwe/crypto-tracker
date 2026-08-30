-- Singular test: the daily price fact must never contain a date at or after
-- today. CoinGecko only finalises a UTC day once it closes, so today's point is
-- intraday and would change on every run. Enforced upstream by the
-- `price_date < current_date` filter in stg_coin_prices_daily; asserted here so
-- a future refactor cannot silently drop it.
--
-- A dbt singular test passes when it returns zero rows.

select
    coin_id,
    price_date
from {{ ref('fct_coin_price_daily') }}
where price_date >= current_date
