-- Grain: one row per (rate_date, base_currency, quote_currency).
--
-- Includes a synthetic USD->USD identity row for every date so that reporting in
-- the base currency uses exactly the same join path as any other currency,
-- rather than needing a special case in every downstream model.

with source as (

    select * from {{ ref('br_fx_rates') }}

),

typed as (

    select
        -- Lands as VARCHAR from the API; cast explicitly.
        cast("date" as date)                as rate_date,
        upper(trim(base))                   as base_currency,
        upper(trim(quote))                  as quote_currency,
        cast(rate as decimal(38, 18))       as rate_to_usd,
        cast(_ingested_at as timestamptz)   as ingested_at
    from source
    where rate is not null
      and cast(rate as decimal(38, 18)) > 0

),

deduped as (

    select
        rate_date,
        base_currency,
        quote_currency,
        rate_to_usd,
        ingested_at
    from typed
    -- Frankfurter restates the current day until ECB finalises it, so the same
    -- key legitimately arrives more than once. Keep the newest.
    qualify row_number() over (
        partition by rate_date, base_currency, quote_currency
        order by ingested_at desc
    ) = 1

),

identity_rows as (

    select distinct
        rate_date,
        base_currency,
        base_currency                       as quote_currency,
        cast(1 as decimal(38, 18))          as rate_to_usd,
        ingested_at
    from deduped

)

select * from deduped
union all
select * from identity_rows
