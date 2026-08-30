# Repo tour — layer by layer

Where to look when the participant asks "what is X?" The four docs cover 90% of the questions; the rest live in the code, in the why-comments.

## Top-level shape

```
.
├── AGENTS.md                   # agent instructions (this repo)
├── README.md                   # project overview, command tables, contributing house style
├── pyproject.toml              # pinned stack + ct console script (no setup.py)
├── .env.example                # 4 user knobs (CRYPTO_TRACKED_COINS, CRYPTO_FIAT_CURRENCIES, CRYPTO_LOG_LEVEL, INTER_COIN_SLEEP_SECONDS)
├── LICENSE                     # MIT
│
├── ingest/                     # dlt sources + entry point
│   ├── __init__.py             # ALL pipeline config: coins, currencies, DECIMAL_HINT, history windows
│   ├── coingecko.py            # /coins/markets (snapshot) + /coins/{id}/market_chart (history); _coerce_numerics fixes variant split
│   ├── fx.py                   # Frankfurter ECB rates
│   └── run_ingest.py           # python -m ingest.run_ingest
│
├── transform/                  # dbt project
│   ├── models/
│   │   ├── bronze/             # 4 dbt trust-filter views, schema.yml
│   │   ├── silver/             # 4 dbt models (3 views + 1 table for the FX spine)
│   │   ├── gold/               # 10 dbt tables (3 dims, 3 facts, 4 marts)
│   │   └── sources/            # dlt source yml declarations (one per dlt landing table)
│   ├── macros/                 # generate_schema_name, money, fx_rate, to_local, position_value
│   ├── seeds/                  # portfolio_holdings.csv (user's holdings, schema fixed in yml)
│   ├── tests/                  # singular dbt tests (e.g. assert_no_future_price_dates)
│   ├── profiles.yml            # duckdb, path = env_var('CRYPTO_DB_PATH'), threads: 1
│   └── dbt_project.yml
│
├── dags/crypto_tracker_daily.py  # Airflow 3 DAG (ingest_raw >> dbt_build)
│
├── scripts/
│   ├── cli.py                  # uv run ct <sub>; sets AIRFLOW_HOME, CRYPTO_DB_PATH, PYTHONPATH, PATH
│   └── duckdb_cli.py           # python -m scripts.duckdb_cli <tables|portfolio|performance|shell|sql|check-lock>
│
├── docs/                       # ARCHITECTURE, workshop, TROUBLESHOOTING
├── analysis_gold.sql           # 8 curated analyses on the gold layer
│
├── data/                       # gitignored: the DuckDB file lives here
├── .airflow/                   # gitignored: Airflow metadata, logs, generated admin password
└── .agents/skills/ask-william/ # this skill
```

## Bronze layer (dlt landing zone + dbt trust-filter views)

**Owner: dlt** lands three source tables and bookkeeping in the `bronze` schema:

| Object | dlt resource | Shape |
|---|---|---|
| `coins_markets_raw` | `coingecko_markets_source` | snapshot, one row per coin per pipeline run, append-only |
| `coin_market_chart_raw` | `coingecko_history_source` | daily history, merge on natural key, one row per (coin, day) |
| `fx_rates_raw` | `fx_source` | FX rates, one row per (date, currency) |
| `_dlt_loads` | dlt bookkeeping | every load attempt, with status |
| `_dlt_version` | dlt bookkeeping | schema versions |
| `_dlt_pipeline_state` | dlt bookkeeping | resume state |
| `bronze_staging` | dlt bookkeeping | temporary merge tables |

**Owner: dbt** adds four trust-filter views in the same `bronze` schema:

| View | Purpose |
|---|---|
| `br_coins_markets` | `coins_markets_raw` filtered to completed loads |
| `br_coin_market_chart` | `coin_market_chart_raw` filtered to completed loads |
| `br_fx_rates` | `fx_rates_raw` filtered to completed loads |
| `br_completed_loads` | list of load IDs with `_dlt_loads.status = 0` |

The trust boundary is `br_completed_loads`. Every bronze view inner-joins it. Naming convention: dlt owns `*_raw` and `_dlt_*`; dbt owns `br_*`. Sources yml lives in `transform/models/sources/`; the dbt models in `transform/models/bronze/`.

Materialised as **views** because each is a thin filter over a dlt table; views occupy no disk space. A physical materialisation would duplicate the whole landing zone on disk.

## Silver layer (dbt — clean, deduplicate, gap-fill)

Four models:

| Model | Material | Purpose |
|---|---|---|
| `stg_coin_snapshots` | view | one row per (coin, snapshot_ts) from `br_coins_markets` |
| `stg_coin_prices_daily` | view | daily price history from `br_coin_market_chart` |
| `stg_fx_rates` | view | FX rates from `br_fx_rates` |
| `stg_fx_rates_filled` | **table** | FX rates with weekend/holiday gaps forward-filled |

The table is necessary because downstream gold facts need every day to exist (for `lag()` and rolling windows). Forward-filling is a non-negotiable: ECB does not publish on Sat/Sun, and the gold layer cannot see missing days.

## Gold layer (dbt — star schema, facts, marts)

Ten tables:

| Object | Type | Purpose |
|---|---|---|
| `dim_coin` | dimension | one row per CoinGecko coin; surrogate key `coin_key` = md5(`coin_id`) |
| `dim_date` | dimension | one row per date; surrogate key `price_date_key` |
| `dim_currency` | dimension | one row per fiat currency; surrogate key `currency_key` = md5(`currency_code`) |
| `fct_coin_snapshot` | fact | one row per (coin, snapshot_ts) with USD prices, market cap, supply |
| `fct_coin_price_daily` | fact | one row per (coin, price_date) with USD price, market cap, volume |
| `fct_fx_rate_daily` | fact | one row per (currency, price_date) with USD→local rate |
| `mart_market_overview` | mart | daily market breadth (advancing/declining coins, top gainer) |
| `mart_coin_performance` | mart | trailing returns + 30-day volatility per coin, per currency |
| `mart_portfolio_summary` | mart | latest portfolio value + P&L per currency |
| `mart_portfolio_value_daily` | mart | per-holding daily value, per currency |

The incremental facts (`fct_coin_price_daily`, `fct_fx_rate_daily`) use `delete+insert` with a 7-day reach-back so `lag()` always has preceding rows. First run: `uv run ct dbt-refresh` (otherwise: `SchemaNotImplementedError ... incremental load requires an existing table`).

## Macros (the non-obvious ones)

`transform/macros/`:

- **`generate_schema_name.sql`** — removes dbt-duckdb's default `main_` prefix. Without it, gold tables live in `main_gold` and the workshop SQL breaks.
- **`money.sql`** — `{{ money(col) }}` casts to `decimal(24,6)`. `{{ fx_rate(col) }}` casts to `decimal(14,8)`. `{{ to_local(amount_usd, currency, date) }}` combines the two for a 38-digit-safe local-currency expression. The reason: `DECIMAL(38,18) × DECIMAL(38,18)` overflows DuckDB's 38-digit ceiling; intermediate casts stay within range.

## DAG (Airflow 3)

`dags/crypto_tracker_daily.py`:

- `from airflow.sdk import dag, task` (Airflow 3 SDK; not the old `airflow.decorators`).
- `schedule="0 2 * * *"`, `catchup=False`, `max_active_runs=1`.
- Two tasks: `ingest_raw` (retries 3, delay 5 min) → `dbt_build` (retries 1, delay 2 min).
- Both on `pool="duckdb_writer"` (1 slot, created by `uv run ct airflow-init`).
- Assets declared with `from airflow.sdk import Asset`: `crypto_bronze` and `crypto_gold`.

## Scripts

`scripts/cli.py` (~415 lines):

- Sets `AIRFLOW_HOME`, `CRYPTO_DB_PATH`, `PYTHONPATH`, `PATH` at import time.
- Loads `.env` for the four user knobs only (via `os.environ.setdefault`; real shell env wins).
- `_COMMANDS` table at lines 105-137 — the canonical subcommand list.
- Help text uses workshop vocabulary ("ingest then build everything (the usual command)").

`scripts/duckdb_cli.py` (~232 lines):

- Subcommands: `tables`, `portfolio`, `performance`, `ui`, `shell`, `sql`, `check-lock`.
- `tables`, `portfolio`, `performance` use a short-lived read-only connection (open, print, exit — safe to run while a pipeline is mid-task).
- `shell --readonly` is read-only; `shell` is read-write. Both hold the DuckDB write lock for their lifetime.
- Readline for arrow-key history in the REPL (Linux/macOS).

## Common "what is this file?" answers

| Asked about | Point to |
|---|---|
| Where is the pipeline config? | `ingest/__init__.py` (every other module imports from here) |
| Where does the DECIMAL coercion happen? | `ingest/coingecko.py` (`_coerce_numerics`, `_to_decimal`) |
| Where is the trust boundary? | `transform/models/bronze/br_completed_loads.sql` and `br_coins_markets.sql` etc. |
| Why is the FX spine a table and not a view? | `transform/models/silver/stg_fx_rates_filled.sql` (forward-fills weekends) |
| Where is the warehouse? | `data/crypto.duckdb` (gitignored) |
| Where is Airflow's metadata? | `.airflow/` (gitignored) |
| Why are the gold tables called `gold.*` and not `main_gold.*`? | `transform/macros/generate_schema_name.sql` |
| Where is the `duckdb_writer` pool created? | `scripts/cli.py` `cmd_airflow_init` (called by `uv run ct airflow-init`) |
| Where is the DAG scheduled? | `dags/crypto_tracker_daily.py` (`schedule="0 2 * * *"`) |
| Where are the test counts declared? | `transform/models/{bronze,silver,gold}/*/schema.yml` (74 declared) + `transform/tests/assert_no_future_price_dates.sql` (1 singular) = 75 |

| Where is the workshop? | `docs/workshop.md` (classic commands; the canonical teaching flow) |
