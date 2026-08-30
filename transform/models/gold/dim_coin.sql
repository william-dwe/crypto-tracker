-- Grain: one row per coin.
--
-- Surrogate key rule for the whole star schema: md5 of the natural key, cast to
-- varchar, computed identically in the dimension and in every fact. Deterministic
-- across full refreshes, unlike a sequence.

with snapshots as (

    select * from {{ ref('stg_coin_snapshots') }}

),

latest_snapshot_ts as (

    select max(snapshot_ts) as max_snapshot_ts from snapshots

),

latest_attributes as (

    select
        coin_id,
        symbol,
        name,
        max_supply,
        snapshot_ts
    from snapshots
    qualify row_number() over (partition by coin_id order by snapshot_ts desc) = 1

),

observed as (

    select
        coin_id,
        min(snapshot_date) as first_seen_date,
        max(snapshot_date) as last_seen_date,
        count(*)           as snapshot_count
    from snapshots
    group by coin_id

),

history as (

    select
        coin_id,
        min(price_date) as first_price_date,
        max(price_date) as last_price_date,
        count(*)        as days_of_history
    from {{ ref('stg_coin_prices_daily') }}
    group by coin_id

)

select
    cast(md5(latest_attributes.coin_id) as varchar)     as coin_key,
    latest_attributes.coin_id,
    latest_attributes.symbol,
    latest_attributes.name,
    latest_attributes.max_supply,
    observed.first_seen_date,
    observed.last_seen_date,
    coalesce(observed.snapshot_count, 0)                as snapshot_count,
    history.first_price_date,
    history.last_price_date,
    coalesce(history.days_of_history, 0)                as days_of_history,
    -- Present in the most recent snapshot the pipeline took.
    latest_attributes.snapshot_ts = latest_snapshot_ts.max_snapshot_ts as is_active
from latest_attributes
-- `observed` is derived from the same rows as `latest_attributes`, so a match is
-- guaranteed. Left-joined anyway: the anchor decides the row set, and no lookup
-- should be able to silently drop a coin from its own dimension.
left join observed
    on observed.coin_id = latest_attributes.coin_id
left join history
    on history.coin_id = latest_attributes.coin_id
-- Single-row CTE; a cross join is the correct way to broadcast a scalar.
cross join latest_snapshot_ts
