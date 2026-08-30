# Workshop guide — nine modules

The session in `docs/workshop.md`. ~3.5 hours total, classic commands only (`uv run ct` deliberately not used). Every module uses the same four headings: **Concept → In this repo → Try it → Exercise**.

For each module: a 2-3 line concept summary, the exact classic Try-it command, the `uv run ct` shortcut equivalent, and a pointer to the exercise with its done-condition. Read `docs/workshop.md` for the full prose.

## Suggested timing

| Module | Topic | Minutes |
|---|---|---|
| 1 | What a data pipeline is, and what ELT means | 20 |
| 2 | Extract & Load with dlt | 30 |
| 3 | The warehouse: DuckDB | 20 |
| 4 | Transform with dbt | 35 |
| 5 | Medallion layering | 25 |
| 6 | Dimensional modelling | 35 |
| 7 | Data quality | 25 |
| 8 | Orchestration with Airflow | 35 |
| 9 | Analytics on the gold layer | 25 |
| **Total** | | **~3.5 h** |

The four-heading convention is non-negotiable: every module announces the concept first, grounds it in this repo, gives one hands-on command, then ends with an exercise. Use it as the rhythm.

---

## Module 1 — What a data pipeline is, and what ELT means (20 min)

**Concept.** A pipeline takes data from somewhere, transforms it, and puts it somewhere useful. ELT lands data first and transforms it *in the warehouse*; ETL transforms before loading. ELT wins because source systems often can't transform, warehouses are fast and cheap, and the dlt landing zone is a backup if a transformation breaks.

**In this repo.** Two sources (CoinGecko, Frankfurter) → dlt → DuckDB warehouse → dbt → bronze/silver/gold. One orchestrator (Airflow 3). No cloud, no auth.

**Try it.** No command — read the architecture diagram in `docs/ARCHITECTURE.md` lines 7-41 and the source list in `README.md`.

**ct shortcut.** None — conceptual only.

**Exercise (verbal, no code).** "Why ELT here instead of ETL?" Expected answer: the dlt landing zone is byte-faithful to the APIs, so a transformation break never costs a re-fetch from rate-limited APIs.

---

## Module 2 — Extract & Load with dlt (30 min)

**Concept.** `dlt` is a Python library that extracts from REST APIs and loads into a warehouse. Resources are generators; sources group resources. The `merge` write disposition is idempotent (re-runs update, not duplicate).

**In this repo.** Two sources: `ingest/coingecko.py` (`coins_markets_raw` snapshot + `coin_market_chart_raw` history) and `ingest/fx.py` (`fx_rates_raw`). dlt pipeline name `crypto_tracker`, dataset `bronze`. Pacing: 3 s between per-coin history requests.

**Try it.**

```bash
source .venv/bin/activate
export AIRFLOW_HOME="$PWD/.airflow" AIRFLOW__CORE__LOAD_EXAMPLES=False PYTHONPATH="$PWD"
python -m ingest.run_ingest
```

**ct shortcut.** `uv run ct ingest` (or `uv run ct run` for ingest + dbt in one).

**Exercise 1 (track a new coin).** Add `CRYPTO_TRACKED_COINS="bitcoin,...,solana,polkadot"` in `.env`, copy to `.env`, re-run. **Done condition:** `select count(distinct coin_id) from bronze.coin_market_chart_raw;` returns the new coin.

Gotcha: the incremental window is per-table, not per-coin. To get the new coin's *full* history:

```bash
rm -f data/crypto.duckdb data/crypto.duckdb.wal
rm -rf ~/.dlt/pipelines/crypto_tracker
uv run ct run
```

---

## Module 3 — The warehouse: DuckDB (20 min)

**Concept.** DuckDB is an in-process analytical database (columnar, vectorised, single-file). One file = one warehouse. Single-writer: any open connection holds the file lock, including read-only ones.

**In this repo.** Warehouse at `data/crypto.duckdb` (gitignored). The helper `scripts/duckdb_cli.py` wraps four SQL reports (`tables`, `portfolio`, `performance`) and a shell.

**Try it.**

```bash
python -m scripts.duckdb_cli tables
python -m scripts.duckdb_cli query-ro
```

**ct shortcut.** `uv run ct tables`, `uv run ct query-ro`.

**Exercise 2 (variant-column trap).** Add a *new* coin whose `current_price` is always a whole number, ingest, then `select coin_id, current_price from bronze.coins_markets_raw where coin_id = 'newcoin';`. **Done condition:** column comes back NULL because dlt inferred BIGINT and split decimals into `current_price__v_double`. Then look at `_coerce_numerics` / `_to_decimal` in `ingest/coingecko.py` and explain the fix in your own words.

**Exercise 3 (feel the lock).** Open `uv run ct query-ro` in one terminal. In another, run `uv run ct ingest`. **Done condition:** ingest blocks with `IO Error: Could not set lock on file "data/crypto.duckdb": Conflicting lock is held`. Ctrl-D out of the shell, watch ingest resume. (Even read-only holds the lock; `uv run ct tables` is the exception — opens, prints, exits.)

---

## Module 4 — Transform with dbt (35 min)

**Concept.** dbt is SQL with `ref()` and `source()` macros, a DAG of model dependencies, and a test runner. Materialisations: `view` (no disk), `table` (rebuilt every run), `incremental` (merge on natural key).

**In this repo.** Profile `crypto_tracker` (DuckDB), `transform/dbt_project.yml` declares schemas (bronze/silver=view, gold=table). 18 models. 74 declared tests + 1 singular = 75 passing on `uv run ct test`. The four macros live in `transform/macros/`: `generate_schema_name` (removes `main_` prefix), `money`, `fx_rate`, `to_local`, `position_value`.


**Try it.**

```bash
cd transform
export CRYPTO_DB_PATH="$PWD/../data/crypto.duckdb"
dbt build --profiles-dir .
cd ..
```

**ct shortcut.** `uv run ct dbt` (builds + tests in one).

**Exercise 4 (add a column).** Add `wallet_address` (varchar) to `transform/seeds/portfolio_holdings.csv` (use one of the existing rows), declare it in `transform/seeds/portfolio_holdings.yml` `+column_types`, and `dbt build --select +portfolio_holdings mart_portfolio_value_daily` (from `transform/`). **Done condition:** `select wallet_address from gold.mart_portfolio_value_daily limit 5;` returns non-NULL values.

Gotcha: changing the seed schema means editing the yml `+column_types` to match; otherwise dbt infers the wrong type.

---

## Module 5 — Medallion layering (25 min)

**Concept.** Bronze = landing zone (byte-faithful to the API). Silver = cleaned, deduplicated, gap-filled. Gold = star schema for analytics. Each layer has a single ownership rule: dlt owns `*_raw` and `_dlt_*`; dbt owns `br_*`, `stg_*`, `dim_*`, `fct_*`, `mart_*`.

**In this repo.** Bronze: 3 dlt tables + 4 dbt trust-filter views (`br_coins_markets`, `br_coin_market_chart`, `br_fx_rates`, `br_completed_loads`). The trust boundary is `br_completed_loads` filtering `_dlt_loads.status = 0`. Silver: 3 views + 1 table. Gold: 3 dims, 3 facts, 4 marts.

**Try it.**

```bash
# inspect the trust boundary
python -m scripts.duckdb_cli sql "select load_id, status from bronze._dlt_loads order by load_id desc limit 5;"
python -m scripts.duckdb_cli sql "select count(*) from bronze.br_completed_loads;"
```

**ct shortcut.** `uv run ct sql "..."`.

**Exercise 5 (write a data test, starts here, finishes in Module 7).** Add a singular test that catches the workshop's "track a new coin with full history" mistake. **Done condition:** `dbt test --profiles-dir .` (from `transform/`) reports a failure when you temporarily violate the test.

---

## Module 6 — Dimensional modelling (35 min)

**Concept.** Kimball star schema: dimensions (who/what/when) and facts (events/measures). Surrogate keys (`coin_key`, `price_date_key`) are integers; natural keys (`coin_id`, `price_date`) are the business identifiers. Surrogates decouple the warehouse from source-system changes.

**In this repo.** `dim_coin` (md5 of `coin_id`), `dim_date`, `dim_currency` (md5 of `currency_code`). Facts: `fct_coin_snapshot`, `fct_coin_price_daily`, `fct_fx_rate_daily`. Marts aggregate the facts for analytics (`mart_market_overview`, `mart_coin_performance`, `mart_portfolio_summary`, `mart_portfolio_value_daily`).

**Try it.**

```bash
python -m scripts.duckdb_cli sql "select * from gold.dim_coin;"
python -m scripts.duckdb_cli sql "select min(price_date), max(price_date) from gold.fct_coin_price_daily;"
```

**ct shortcut.** `uv run ct sql "..."`.

**Exercise 6 (change the portfolio).** Add a row to `transform/seeds/portfolio_holdings.csv` (different coin + quantity), rebuild the gold mart. **Done condition:** `select * from gold.mart_portfolio_value_daily where holding_id = <new_id>;` shows the new holding in every `currency_code`.

---

## Module 7 — Data quality (25 min)

**Concept.** Generic tests (not_null, unique, relationships, accepted_values) declared in schema.yml run for free. Singular tests (custom SQL in `transform/tests/`) catch business rules. dbt also has `dbt source freshness` for SLAs on the landing zone.

**In this repo.** 51 not_null, 15 relationships, 7 unique, 1 accepted_values = 74 declared + 1 singular = 75 total. Source freshness warn_after 26h on `bronze.coins_markets_raw`. `transform/tests/assert_no_future_price_dates.sql` is the canonical singular.


**Try it.**

```bash
cd transform
export CRYPTO_DB_PATH="$PWD/../data/crypto.duckdb"
dbt test --profiles-dir .
dbt source freshness
cd ..
```

**ct shortcut.** `uv run ct test` (note: `ct` does not expose `source freshness`; use classic form for that one).

**Exercise 5 (finish).** Implement the singular test from Module 5. **Done condition:** `dbt test` reports the violation when a row is bad, and PASS once fixed. Confirm the `PASS=75 WARN=0 ERROR=0 SKIP=0` line.


---

## Module 8 — Orchestration with Airflow (35 min)

**Concept.** Airflow 3 DAG: tasks with `>>` dependencies, retries, pools, schedules. The 1-slot `duckdb_writer` pool is a correctness gate: tasks that touch the DuckDB file declare the pool, so they serialise, never overlap.

**In this repo.** `dags/crypto_tracker_daily.py`: Airflow 3 SDK (`from airflow.sdk import dag, task`), `schedule="0 2 * * *"`, `catchup=False`, two tasks `ingest_raw` (retries 3, delay 5 min) → `dbt_build` (retries 1, delay 2 min), both on `pool="duckdb_writer"`.

**Try it.**

```bash
source .venv/bin/activate
export AIRFLOW_HOME="$PWD/.airflow" AIRFLOW__CORE__LOAD_EXAMPLES=False PYTHONPATH="$PWD"
airflow standalone    # foreground: scheduler + webserver on :8080
# admin password: cat $AIRFLOW_HOME/simple_auth_manager_passwords.json.generated
# or: uv run ct creds
```

**ct shortcut.** `uv run ct airflow-ui` (foreground) + `uv run ct creds`.

**Exercise 7 (run the DAG).** Open the UI, unpause `crypto_tracker_daily`, watch the next scheduled run, OR `airflow dags trigger crypto_tracker_daily` from a shell. **Done condition:** both tasks succeed; the green run appears in `airflow dags list-runs crypto_tracker_daily` (or `uv run ct status`).

**Exercise 8 (break a dependency).** Edit `dags/crypto_tracker_daily.py` to remove the `>>` between the two tasks. Run the DAG. **Done condition:** the failure is NOT a lock conflict (the pool serialises them regardless of `>>`) — the failure is "missing task dependency" in the Airflow UI. Re-add `>>`.

Gotcha: the pool serialises (1 slot) but does not sequence. `>>` is the sequencing. Both are needed for correctness.

---

## Module 9 — Analytics on the gold layer (25 min)

**Concept.** Marts are pre-joined for the common questions. `analysis_gold.sql` shows 8 curated analyses (drawdown, Sharpe-like momentum, FX sensitivity, intraday spread). All queries are read-only against `gold.*`.

**In this repo.** `analysis_gold.sql` — the "what can I do with this?" document. `mart_market_overview` (advancing/declining coins per day), `mart_coin_performance` (trailing returns + volatility per coin), `mart_portfolio_summary` (value + P&L in 9 currencies), `mart_portfolio_value_daily` (per-holding daily value).

**Try it.**

```bash
python -m scripts.duckdb_cli sql "select currency_code, total_value_local, total_unrealized_pnl_pct from gold.mart_portfolio_summary where price_date = (select max(price_date) from gold.mart_portfolio_summary);"
```

**ct shortcut.** `uv run ct portfolio`, `uv run ct performance`.

**Exercise 9 (gold analysis).** Pick one analysis from `analysis_gold.sql`, run it, modify it. **Done condition:** the participant can explain what the analysis answers in one sentence.
