# AGENTS.md

Repo instructions for coding agents working on `crypto-tracker`. Read this first; the four docs in `docs/` are the source of truth for *why*.

## What this repo is

A local-first crypto ELT teaching pipeline. CoinGecko and Frankfurter (both keyless, no API keys) feed **dlt**, which lands in **DuckDB** at `data/crypto.duckdb`. **dbt** then transforms through a bronze/silver/gold medallion into a star schema. **Airflow 3** runs the whole thing on a daily schedule. The repo doubles as a guided workshop (`docs/workshop.md`).

The point of the design is that every seam is visible: dlt tables sit in `bronze` alongside dbt trust-filter views, raw API columns survive untouched, and the daily DAG is two tasks. There is no cloud dependency and no auth.

## Ground rules before you touch anything

1. **House style: every non-obvious decision gets a why-comment.** Canonical examples are the variant-column trap (`ingest/coingecko.py`), the single-writer pool (`dags/crypto_tracker_daily.py`), the forward-filled FX spine (`transform/models/silver/stg_fx_rates_filled.sql`), and the `DECIMAL(38,18) × DECIMAL(38,18)` overflow (`transform/macros/money.sql`). Match that style in new code; do not strip existing why-comments.
2. **Never install with the Airflow constraints file.** `pyproject.toml` lines 17-20 pin the whole stack for one venv. Use `uv sync` from the lockfile, or `uv run ct setup` for first-time setup.
3. **DuckDB is single-writer.** A 1-slot `duckdb_writer` pool is the correctness mechanism, not a perf tweak. Even read-only clients (`uv run ct ui`, `query-ro`, `query`) hold the lock; quit them (Ctrl-D) before running the pipeline. Only `uv run ct tables` / `portfolio` / `performance` are safe to run any time.
4. **Artifacts are gitignored.** `data/`, `.airflow/`, `transform/target/`, `transform/logs/`, `transform/dbt_packages/` are regenerable. The warehouse is built from APIs and seeds; `uv run ct clean-db && uv run ct run` rebuilds it from scratch.

## Two command forms

Every command in this repo exists in two forms that run the same subprocesses. The workshop uses classic form on purpose; agents should default to shortcut form for normal work.

| Task | Classic form | Shortcut form |
|---|---|---|
| first-time setup | `uv sync` | `uv run ct setup` |
| init Airflow DB + pool | `airflow db migrate && airflow pools set duckdb_writer 1 "Serializes DuckDB write access"` | `uv run ct airflow-init` |
| ingest (dlt) | `python -m ingest.run_ingest` | `uv run ct ingest` (or `uv run ct run` for ingest+dbt) |
| dbt build | `cd transform && export CRYPTO_DB_PATH="$PWD/../data/crypto.duckdb" && dbt build --profiles-dir . && cd ..` | `uv run ct dbt` |
| dbt tests only | `dbt test --profiles-dir .` (from `transform/`) | `uv run ct test` |
| dbt full refresh | `dbt build --full-refresh --profiles-dir .` (from `transform/`) | `uv run ct dbt-refresh` |
| dbt lineage docs | `dbt docs generate --profiles-dir . && dbt docs serve --port 8081 --profiles-dir .` (from `transform/`) | `uv run ct docs` |
| list tables | `python -m scripts.duckdb_cli tables` | `uv run ct tables` |
| portfolio | `python -m scripts.duckdb_cli portfolio` | `uv run ct portfolio` |
| performance | `python -m scripts.duckdb_cli performance` | `uv run ct performance` |
| SQL shell (RO) | `python -m scripts.duckdb_cli shell --readonly` | `uv run ct query-ro` |
| SQL shell (RW) | `python -m scripts.duckdb_cli shell` | `uv run ct query` |
| one-shot SQL | `python -m scripts.duckdb_cli sql "<query>"` | `uv run ct sql "<query>"` |
| lock probe | `python -m scripts.duckdb_cli check-lock` | `uv run ct check-lock` |
| Airflow UI | `airflow standalone` | `uv run ct airflow-ui` |
| admin creds | `cat "$AIRFLOW_HOME/simple_auth_manager_passwords.json.generated"` | `uv run ct creds` |
| DAG test (no scheduler) | `airflow dags reserialize && airflow dags test crypto_tracker_daily` | `uv run ct dag-test` |
| DAG trigger | `airflow dags trigger crypto_tracker_daily` | `uv run ct trigger` |
| DAG run history | `airflow dags list-runs crypto_tracker_daily` | `uv run ct status` |
| unpause / pause | `airflow dags unpause crypto_tracker_daily` / `airflow dags pause crypto_tracker_daily` | `uv run ct unpause` / `uv run ct pause` |
| clean warehouse + dlt state | `rm -f data/crypto.duckdb data/crypto.duckdb.wal && rm -rf ~/.dlt/pipelines/crypto_tracker` | `uv run ct clean-db` |
| drop dbt artifacts + Airflow metadata | `rm -rf transform/target transform/logs .airflow` | `uv run ct clean` |

The classic form needs the venv activated and these exports per shell (the dbt rows additionally need `cd transform` and `CRYPTO_DB_PATH`):

```bash
source .venv/bin/activate
export AIRFLOW_HOME="$PWD/.airflow" AIRFLOW__CORE__LOAD_EXAMPLES=False PYTHONPATH="$PWD"
```

`uv run ct` with no subcommand prints the grouped list. The hidden `check-lock` is the fast-fail probe used internally by `tables` / `portfolio` / `performance`.

## Environment variables

Only these four are user-tunable and may go in `.env` (defaults in `ingest/__init__.py`):

| Var | Default | Purpose |
|---|---|---|
| `CRYPTO_TRACKED_COINS` | `bitcoin,ethereum,solana,cardano,ripple,polkadot,chainlink,dogecoin,avalanche-2,litecoin` | CoinGecko coins to track |
| `CRYPTO_FIAT_CURRENCIES` | `EUR,GBP,JPY,IDR,SGD,AUD,CHF,CAD` | Quoted currencies in gold marts |
| `CRYPTO_LOG_LEVEL` | unset (INFO) | `logging` level for ingest |
| `INTER_COIN_SLEEP_SECONDS` | `3.0` | Pacing between per-coin history requests |

`uv run ct` loads `.env` for these four only. Path-style vars (`AIRFLOW_HOME`, `CRYPTO_DB_PATH`, `PYTHONPATH`) are managed by `ct` and must NOT appear in `.env` — `scripts/cli.py` forces them to absolute values and prepends the venv to `PATH` so bare-name subprocesses resolve.

## Architecture quick-reference

dlt lands three source tables + bookkeeping (`_dlt_loads`, `_dlt_version`, `_dlt_pipeline_state`, `bronze_staging`) in the `bronze` schema. dbt adds four trust-filter views in the same schema that inner-join `br_completed_loads` (load IDs with `_dlt_loads.status = 0`) — materialised as views because each is a thin filter, and views occupy no disk space. Silver has 4 models (3 views + `stg_fx_rates_filled` table). Gold has 10 tables (3 dims, 3 facts, 4 marts).

| Layer | Object kind | Owner | Naming |
|---|---|---|---|
| bronze source tables | dlt landing tables | dlt | `*_raw`, `_dlt_*` |
| bronze trust views | dbt views | dbt | `br_*` |
| silver | dbt (3 views, 1 table) | dbt | `stg_*` |
| gold | dbt tables | dbt | `dim_*`, `fct_*`, `mart_*` |

Six hard-won constraints — every one is the reason behind a why-comment in the code:

- **DuckDB single-writer**: a 1-slot `duckdb_writer` Airflow pool is the correctness mechanism, not a performance tweak.
- **dlt variant-column split**: CoinGecko returns whole numbers for expensive coins and decimals for cheap ones; dlt infers from the first row and diverges later values into `<col>__v_double`. Fixed by `Decimal(str(v))` coercion in `ingest/coingecko.py` before dlt inspects rows.
- **ECB weekend gaps**: FX rates skip Sat/Sun; the silver `stg_fx_rates_filled` table forward-fills so the gold layer never sees a missing day.
- **DECIMAL(38,18) × DECIMAL(38,18) overflow**: solved by `transform/macros/money.sql` (`money`=decimal(24,6), `fx_rate`=decimal(14,8), `to_local(...)` macro).
- **`main_` schema prefix**: dbt-duckdb prepends `main_` to schemas by default. `transform/macros/generate_schema_name.sql` removes the prefix so gold tables live in `gold`, not `main_gold`.
- **Incremental facts with `lag()`**: 7-day reach-back + `delete+insert` so the lag window always has preceding rows. First-run error: run `uv run ct dbt-refresh` once.

DAG: `dags/crypto_tracker_daily.py` — Airflow 3 SDK (`from airflow.sdk import dag, task`), `schedule="0 2 * * *"`, `catchup=False`, two tasks `ingest_raw` (retries 3, delay 5 min) → `dbt_build` (retries 1, delay 2 min), both on `pool="duckdb_writer"`.

Deep *why* lives in `docs/ARCHITECTURE.md`.

## Where things live

```
.
├── ingest/                       # dlt sources + entry point
│   ├── __init__.py               # ALL pipeline config (coins, currencies, DECIMAL_HINT, history windows)
│   ├── coingecko.py              # /coins/markets (snapshot) + /coins/{id}/market_chart (history); _coerce_numerics fixes variant split
│   ├── fx.py                     # Frankfurter ECB rates
│   └── run_ingest.py             # python -m ingest.run_ingest
├── transform/
│   ├── models/
│   │   ├── bronze/               # 4 dbt trust-filter views, schema.yml
│   │   ├── silver/               # 4 dbt models (3 views + 1 table for the FX spine)
│   │   ├── gold/                 # 10 dbt tables (dims/facts/marts)
│   │   └── sources/              # dlt source yml declarations
│   ├── macros/                   # generate_schema_name (removes main_), money/fx_rate/position_value/to_local
│   ├── seeds/                    # portfolio_holdings.csv (user's holdings, schema fixed in portfolio_holdings.yml)
│   ├── profiles.yml              # dbt profile: duckdb, path = env_var('CRYPTO_DB_PATH'), threads: 1
│   └── dbt_project.yml
├── dags/crypto_tracker_daily.py  # Airflow 3 DAG (ingest_raw >> dbt_build)
├── scripts/
│   ├── cli.py                    # uv run ct <sub>; sets AIRFLOW_HOME, CRYPTO_DB_PATH, PYTHONPATH, PATH; loads .env
│   └── duckdb_cli.py             # python -m scripts.duckdb_cli <tables|portfolio|performance|shell|sql|check-lock>
├── docs/                         # ARCHITECTURE, workshop, TROUBLESHOOTING
├── analysis_gold.sql             # 8 curated analyses on the gold layer
├── pyproject.toml                # pinned stack + ct console script
├── .env.example                  # the 4 user knobs (no secrets)
└── LICENSE                       # MIT
```

## Verification before you say "done"

After any change that affects the data, run all four:

```bash
uv run ct tables         # bronze, silver, gold all have non-zero rows
uv run ct portfolio      # 9 rows (one per currency, latest price_date)
uv run ct performance    # 10 rows (one per coin, USD)
uv run ct test           # PASS=75 WARN=0 ERROR=0 SKIP=0
```

Additional rules:

- **Incremental model change**: run `uv run ct dbt-refresh` (full refresh) at least once. Incremental reach-back alone may keep the old data.
- **Source change / new coin**: `rm -f data/crypto.duckdb data/crypto.duckdb.wal && rm -rf ~/.dlt/pipelines/crypto_tracker` then `uv run ct run`. The dlt merge disposition absorbs the overlap on incremental windows.
- **Test addition**: dbt test count must rise by exactly the number you added; `PASS=` should climb to match.
- **Smoke after a fix**: `uv run ct tables` is the only explore subcommand that's safe to run while a pipeline is mid-task. Do not hold `ui` / `query` / `query-ro` open while a DAG task is running.

## Docs map

- `README.md` — project overview, setup, the two command-form tables, exploration, contributing house style, license.
- `docs/ARCHITECTURE.md` — *why the code looks like this*: layers, trust boundary, star schema, join discipline, every hard-won constraint.
- `docs/workshop.md` — 9 modules / 9 exercises, classic commands only (`uv run ct` deliberately not used); the workshop IS the canonical teaching flow.
- `docs/TROUBLESHOOTING.md` — symptom → cause → fix table; read this first when something breaks.

## Workshop

This repository also ships as a guided workshop for people new to data engineering. The default walkthrough (`docs/workshop.md`) uses the underlying tools directly so every command is visible. A `uv run ct <sub>` twin with the same flow is also available, plus the `ask-william` skill at `.agents/skills/ask-william/` for participant-facing guidance.
