-- Grain: one row per (date, quote currency).
--
-- Built on the forward-filled silver model, so there is a rate for every
-- calendar day a crypto price exists — including weekends, when ECB publishes
-- nothing. `is_filled` preserves the distinction for consumers that care.

select
    cast(strftime(rate_date, '%Y%m%d') as integer)  as date_key,
    cast(md5(quote_currency) as varchar)            as currency_key,
    rate_date,
    base_currency,
    quote_currency,
    rate_to_usd,
    is_filled
from {{ ref('stg_fx_rates_filled') }}
