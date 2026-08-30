-- ============================================================================
-- CRYPTO TRACKER — GOLD LAYER ANALYSIS DOCUMENT
-- Open directly in DuckDB UI (http://localhost:4213) or run in DuckDB CLI
-- STRICT CONSTRAINT: Reads exclusively from `gold` schema (marts, facts, dims)
-- ============================================================================

-- ----------------------------------------------------------------------------
-- 1. PORTFOLIO EXECUTIVE SUMMARY (LATEST DATE ACROSS ALL FIAT CURRENCIES)
-- ----------------------------------------------------------------------------
-- High-level overview of portfolio value, cost basis, unrealized PnL,
-- and day-over-day change across all 9 supported fiat currencies.
select
    s.price_date as as_of_date,
    s.currency_code,
    s.currency_name,
    s.holdings_count,
    round(s.total_value_local, 2) as portfolio_value,
    round(s.total_cost_basis_local, 2) as cost_basis,
    round(s.total_unrealized_pnl_local, 2) as unrealized_pnl,
    round(s.total_unrealized_pnl_pct, 2) as unrealized_pnl_pct,
    round(s.day_over_day_change_pct, 2) as day_over_day_change_pct,
    s.has_filled_rate as fx_rate_forward_filled
from gold.mart_portfolio_summary s
where s.price_date = (select max(price_date) from gold.mart_portfolio_summary)
order by s.currency_code;


-- ----------------------------------------------------------------------------
-- 2. CURRENT HOLDINGS & ASSET ALLOCATION (USD)
-- ----------------------------------------------------------------------------
-- Detailed breakdown of individual crypto positions, cost basis, PnL,
-- and portfolio allocation percentage as of the latest market date.
with latest_positions as (
    select
        p.holding_id,
        p.coin_id,
        p.symbol,
        p.coin_name,
        p.quantity,
        p.acquired_date,
        p.price_usd as current_price_usd,
        p.position_value_usd,
        p.cost_basis_usd,
        p.unrealized_pnl_usd,
        p.unrealized_pnl_pct
    from gold.mart_portfolio_value_daily p
    where p.currency_code = 'USD'
      and p.price_date = (select max(price_date) from gold.mart_portfolio_value_daily)
),
portfolio_total as (
    select sum(position_value_usd) as total_portfolio_val_usd
    from latest_positions
)
select
    pos.holding_id,
    pos.symbol,
    pos.coin_name,
    round(pos.quantity, 4) as quantity_held,
    pos.acquired_date,
    round(pos.current_price_usd, 2) as price_usd,
    round(pos.position_value_usd, 2) as position_value_usd,
    round(pos.cost_basis_usd, 2) as cost_basis_usd,
    round(pos.unrealized_pnl_usd, 2) as unrealized_pnl_usd,
    round(pos.unrealized_pnl_pct, 2) as unrealized_pnl_pct,
    round((pos.position_value_usd / tot.total_portfolio_val_usd) * 100, 2) as allocation_pct
from latest_positions pos
cross join portfolio_total tot
order by pos.position_value_usd desc;


-- ----------------------------------------------------------------------------
-- 3. HISTORICAL PORTFOLIO TRAJECTORY & ROLLING DRAWDOWN (USD)
-- ----------------------------------------------------------------------------
-- Daily tracking of portfolio net worth, rolling 7-day / 30-day moving average,
-- peak historical value, and drawdown percentage from peak.
with daily_curve as (
    select
        s.price_date,
        s.total_value_usd,
        s.total_cost_basis_usd,
        s.total_unrealized_pnl_usd,
        s.total_unrealized_pnl_pct,
        s.day_over_day_change_pct,
        avg(s.total_value_usd) over (
            order by s.price_date
            rows between 6 preceding and current row
        ) as ma_7d_value_usd,
        avg(s.total_value_usd) over (
            order by s.price_date
            rows between 29 preceding and current row
        ) as ma_30d_value_usd,
        max(s.total_value_usd) over (
            order by s.price_date
            rows between unbounded preceding and current row
        ) as peak_value_usd
    from gold.mart_portfolio_summary s
    where s.currency_code = 'USD'
)
select
    d.price_date,
    round(d.total_value_usd, 2) as portfolio_value_usd,
    round(d.total_cost_basis_usd, 2) as cost_basis_usd,
    round(d.total_unrealized_pnl_usd, 2) as unrealized_pnl_usd,
    round(d.total_unrealized_pnl_pct, 2) as unrealized_pnl_pct,
    round(d.day_over_day_change_pct, 2) as dod_return_pct,
    round(d.ma_7d_value_usd, 2) as ma_7d_usd,
    round(d.ma_30d_value_usd, 2) as ma_30d_usd,
    round(d.peak_value_usd, 2) as all_time_high_usd,
    round(((d.total_value_usd - d.peak_value_usd) / nullif(d.peak_value_usd, 0)) * 100, 2) as drawdown_pct
from daily_curve d
order by d.price_date desc;


-- ----------------------------------------------------------------------------
-- 4. COIN PERFORMANCE & VOLATILITY MATRIX (TRACKED UNIVERSE)
-- ----------------------------------------------------------------------------
-- Trailing returns across 1D, 7D, 30D, 90D and 30-day historical volatility
-- for all coins in the tracked crypto universe.
select
    m.market_cap_rank,
    m.symbol,
    m.coin_name,
    round(m.latest_price_usd, 4) as price_usd,
    round(m.market_cap_usd / 1e9, 2) as market_cap_billions_usd,
    round(m.total_volume_usd / 1e6, 2) as volume_24h_millions_usd,
    round(m.return_1d_pct, 2) as return_1d_pct,
    round(m.return_7d_pct, 2) as return_7d_pct,
    round(m.return_30d_pct, 2) as return_30d_pct,
    round(m.return_90d_pct, 2) as return_90d_pct,
    round(m.volatility_30d_pct, 2) as volatility_30d_pct,
    m.days_of_history
from gold.mart_coin_performance m
where m.currency_code = 'USD'
order by coalesce(m.market_cap_rank, 999), m.market_cap_usd desc;


-- ----------------------------------------------------------------------------
-- 5. RISK-ADJUSTED MOMENTUM & SHARPE-LIKE RATIO (30-DAY WINDOW)
-- ----------------------------------------------------------------------------
-- Quantifies return generated per unit of volatility (Return-to-Risk Ratio).
select
    m.symbol,
    m.coin_name,
    round(m.return_30d_pct, 2) as return_30d_pct,
    round(m.volatility_30d_pct, 2) as volatility_30d_pct,
    round(
        m.return_30d_pct / nullif(m.volatility_30d_pct, 0),
        2
    ) as return_to_risk_ratio_30d,
    case
        when (m.return_30d_pct / nullif(m.volatility_30d_pct, 0)) >= 2.0 then 'Strong Outperformer'
        when (m.return_30d_pct / nullif(m.volatility_30d_pct, 0)) >= 0.5 then 'Moderate Outperformer'
        when (m.return_30d_pct / nullif(m.volatility_30d_pct, 0)) >= 0.0 then 'Neutral'
        else 'Underperformer'
    end as risk_adjusted_tier
from gold.mart_coin_performance m
where m.currency_code = 'USD'
  and m.volatility_30d_pct is not null
order by return_to_risk_ratio_30d desc;


-- ----------------------------------------------------------------------------
-- 6. MARKET BREADTH, SENTIMENT & GAINERS/LOSERS OVER TIME
-- ----------------------------------------------------------------------------
-- Daily breadth statistics: advancing vs declining coins, market dominance,
-- and daily top gainer / loser.
select
    o.price_date,
    o.tracked_coins,
    o.advancing_coins,
    o.declining_coins,
    round(cast(o.advancing_coins as double) / nullif(o.tracked_coins, 0) * 100, 1) as advancing_ratio_pct,
    round(o.avg_daily_return_pct, 2) as market_avg_daily_return_pct,
    round(o.median_daily_return_pct, 2) as market_median_daily_return_pct,
    round(o.total_market_cap_usd / 1e9, 2) as total_market_cap_billions_usd,
    round(o.total_volume_usd / 1e6, 2) as total_volume_millions_usd,
    o.top_gainer_coin_id,
    round(o.top_gainer_return_pct, 2) as top_gainer_return_pct,
    o.top_loser_coin_id,
    round(o.top_loser_return_pct, 2) as top_loser_return_pct
from gold.mart_market_overview o
where o.currency_code = 'USD'
order by o.price_date desc
limit 30;


-- ----------------------------------------------------------------------------
-- 7. MULTI-CURRENCY FX SENSITIVITY MATRIX
-- ----------------------------------------------------------------------------
-- Demonstrates currency translation effects on total portfolio valuation and
-- displays current ECB exchange rates quoted against base USD.
select
    dim_c.currency_code,
    dim_c.currency_name,
    dim_c.is_base,
    round(fx.rate_to_usd, 6) as fx_rate_to_usd,
    round(s.total_value_local, 2) as portfolio_value_local,
    round(s.total_cost_basis_local, 2) as cost_basis_local,
    round(s.total_unrealized_pnl_local, 2) as unrealized_pnl_local,
    round(s.total_unrealized_pnl_pct, 2) as unrealized_pnl_pct,
    fx.is_filled as is_rate_carried_forward
from gold.mart_portfolio_summary s
inner join gold.dim_currency dim_c
    on dim_c.currency_key = s.currency_key
inner join gold.fct_fx_rate_daily fx
    on fx.date_key = s.date_key
   and fx.currency_key = s.currency_key
where s.price_date = (select max(price_date) from gold.mart_portfolio_summary)
order by dim_c.is_base desc, dim_c.currency_code asc;


-- ----------------------------------------------------------------------------
-- 8. INTRADAY MARKET LIQUIDITY & 24H SPREAD ANALYSIS
-- ----------------------------------------------------------------------------
-- Compares latest snapshot data against dimension attributes to evaluate
-- 24h high/low spread and turnover ratio (Volume / Market Cap).
select
    c.symbol,
    c.name as coin_name,
    s.market_cap_rank,
    round(s.price_usd, 4) as current_price_usd,
    round(s.high_24h_usd, 4) as high_24h_usd,
    round(s.low_24h_usd, 4) as low_24h_usd,
    round(((s.high_24h_usd - s.low_24h_usd) / nullif(s.low_24h_usd, 0)) * 100, 2) as spread_24h_pct,
    round(s.total_volume_usd / 1e6, 2) as volume_24h_millions_usd,
    round(s.market_cap_usd / 1e9, 2) as market_cap_billions_usd,
    round((s.total_volume_usd / nullif(s.market_cap_usd, 0)) * 100, 2) as turnover_ratio_pct,
    s.snapshot_ts as snapshot_timestamp_utc
from gold.fct_coin_snapshot s
inner join gold.dim_coin c
    on c.coin_key = s.coin_key
where s.snapshot_ts = (select max(snapshot_ts) from gold.fct_coin_snapshot)
order by coalesce(s.market_cap_rank, 999), s.market_cap_usd desc;
