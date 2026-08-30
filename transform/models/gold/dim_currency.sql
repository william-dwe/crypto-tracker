-- Grain: one row per currency.
--
-- Currency codes are discovered from the FX data rather than hard-coded, so
-- changing CRYPTO_FIAT_CURRENCIES flows through without a model edit. Only the
-- display names are a static lookup: fetching /v2/currencies would be an extra
-- API call for nine labels.

with observed as (

    select distinct quote_currency as currency_code
    from {{ ref('stg_fx_rates') }}

)

select
    cast(md5(currency_code) as varchar) as currency_key,
    currency_code,
    case currency_code
        when 'USD' then 'US Dollar'
        when 'EUR' then 'Euro'
        when 'GBP' then 'British Pound Sterling'
        when 'JPY' then 'Japanese Yen'
        when 'IDR' then 'Indonesian Rupiah'
        when 'SGD' then 'Singapore Dollar'
        when 'AUD' then 'Australian Dollar'
        when 'CHF' then 'Swiss Franc'
        when 'CAD' then 'Canadian Dollar'
        when 'CNY' then 'Chinese Yuan'
        when 'HKD' then 'Hong Kong Dollar'
        when 'INR' then 'Indian Rupee'
        when 'KRW' then 'South Korean Won'
        when 'NZD' then 'New Zealand Dollar'
        when 'SEK' then 'Swedish Krona'
        when 'NOK' then 'Norwegian Krone'
        when 'DKK' then 'Danish Krone'
        when 'PLN' then 'Polish Zloty'
        when 'BRL' then 'Brazilian Real'
        when 'ZAR' then 'South African Rand'
        else currency_code
    end                                 as currency_name,
    currency_code = 'USD'               as is_base
from observed
