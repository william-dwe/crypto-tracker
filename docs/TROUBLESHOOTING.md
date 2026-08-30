# Troubleshooting

Every symptom observed against the live APIs and DuckDB.

## Read dbt errors first

When a dbt build fails, **read the error block carefully.** It tells you:

1. The model that failed and why (SQL syntax, compilation error, test failure).
2. The compiled SQL at `target/compiled/crypto_tracker/…` (exact query that ran).
3. The raw database error (DuckDB errors are usually terse and exact).

Example (data test failure):

```
[ERROR]: in test relationships_fct_coin_price_daily_coin_key__coin_key (tests/…)
  1 record(s) matched but no cluster had all of them.
  Compiled code at target/compiled/crypto_tracker/tests/…
```

Open the compiled file — that's the exact SQL dbt ran. Paste it into `uv run ct
query` and inspect the data yourself.

---

## Symptom → Cause → Fix

| Symptom | Cause | Fix |
|---|---|---|
| `Could not set lock on file "data/crypto.duckdb": Conflicting lock is held` | Another process (shell, other DAG task, UI) holds the file open. | Close the `uv run ct query`, `uv run ct query-ro`, or `uv run ct ui` session. Quit any DBeaver connection. |
| `ModuleNotFoundError: No module named 'dbt'` | venv not activated. | `uv run ct setup` re-creates it. Or `source .venv/bin/activate` manually. |
| `(psycopg2.OperationalError) could not translate host name "localhost" to address` | dbt profile is pointing at Postgres, but Postgres isn't running. | Confirm `transform/profiles.yml` has `type: duckdb`. The default is correct. |
| `Error: CRYPTO_TRACKED_COINS not set in environment` | `.env` not copied, or the key in `.env` has a typo. | `cp .env.example .env`. `uv run ct` loads only the user-tunable knobs (`CRYPTO_TRACKED_COINS`, `CRYPTO_FIAT_CURRENCIES`, `CRYPTO_LOG_LEVEL`, `INTER_COIN_SLEEP_SECONDS`); paths are managed by `ct` and must NOT appear in `.env`. |
| `ValueError: 'coins_markets_raw' does not exist` (during dbt) | No raw data (ingest hasn't run). | `uv run ct ingest` first, or `uv run ct run` for the full pipeline. |
| `[dlt.load.utils_load_data.SchemaNotImplementedError]: Table 'fct_coin_price_daily' was truncated … incremental load requires an existing table` | Incremental model first run (table doesn't exist yet). | Run `uv run ct dbt-refresh` to do a full refresh, then subsequent runs are incremental. Or uv run ct clean-db && `uv run ct run`. |
| `(pydantic_core._pydantic_core.ValidationError) 1 validation error for EventLogConfig` | Airflow metadata DB is corrupted or schema mismatch. | `uv run ct clean` (drops Airflow metadata) then `uv run ct airflow-init` (recreates it). |
| `Unauthorized. You must provide a valid access token` | CoinGecko returned 401 (hit the history-request ceiling). | Wait a few hours. CoinGecko free tier caps at 4 history requests per minute. `ingest/coingecko.py` has exponential backoff; 401 is never retried because retrying cannot help. |
| `<Response [429]>` in the logs (but pipeline continues) | CoinGecko returned 429 (rate limited). | Expected; `ingest/coingecko.py` backs off 3 seconds and retries. Check the log interval; if retries are aggressive, slow down `CRYPTO_TRACKED_COINS`. |
| `TypeError: unsupported operand type(s) for \*: 'Decimal' and 'Decimal'` in dbt | Arithmetic overflowed the DECIMAL(38,18) ceiling. | Use `{{ to_local(…) }}` and `{{ money(…) }}` / `{{ fx_rate(…) }}` macros to keep intermediate results within 38 digits. See `transform/macros/money.sql`. |
| `dbt_versions` not in state; cannot run selection check | dbt version mismatch or `dbt parse` not run yet. | `dbt parse` (usually run by `dbt build`). If the error persists, run uv run ct clean-db && `uv run ct run`. |
| `Traceback: IO Error: Catalog "main_gold" does not exist!` | Schema prefix from dbt-duckdb defaults is applied. | Confirm `generate_schema_name` macro exists in `transform/macros/`. It removes the `main_` prefix. See `transform/dbt_project.yml` line 18: `macro_namespace: crypto_tracker`. |
| `<DatabaseError> 23505: Duplicate key value violates unique constraint…` | A dbt test or unique constraint is failing. | Read the error message for the table and key. `dbt test` will also print the offending rows. Check the ETL — is deduplication in silver missing? Is an incremental model's grain definition wrong? |
| `UnicodeDecodeError: 'utf-8' codec can't decode byte 0x…` | dlt or dbt tried to read a file with encoding issues. | Rare. Check that `.env`, `pyproject.toml`, and SQL files are UTF-8 (not UTF-16BOM). `file -I .env`. |
| `ModuleNotFoundError: No module named 'scripts'` | The `python -m scripts.duckdb_cli …` form was run from outside the repo root, so the `scripts/` package is not on `PYTHONPATH`. | Run from the repo root, or use the `uv run ct tables` / `uv run ct query-ro` targets — they export `PYTHONPATH` for you. |

---

## Specific scenarios

### The first run is slow (~4 minutes)

**Cause:** CoinGecko's free tier rate limit (4 history requests per minute).

One call per coin per table = up to 20 requests. The ingest paces them 3
seconds apart with exponential backoff on 429s.

**Fix:** None needed. This is normal. Second runs are fast (2-day window,
usually 1-2 minutes). Set a timer and come back.

### Portfolio value is NULL

**Cause 1:** Holdings and coins have no join match.

```bash
select h.holding_id, h.coin_id from transform.seeds.portfolio_holdings h
left join gold.dim_coin d on d.coin_id = h.coin_id
where d.coin_key is null;
```

If this returns rows, the coin isn't in the warehouse. Add it to
`CRYPTO_TRACKED_COINS` and re-ingest.

**Cause 2:** Price date and holding date don't overlap.

```bash
select min(h.acquired_date), max(p.price_date)
from transform.seeds.portfolio_holdings h
cross join gold.fct_coin_price_daily p
where p.coin_id = h.coin_id;
```

If `acquired_date > max(price_date)`, the holding was acquired after the
last price in the warehouse. Re-run `uv run ct ingest` to fetch today's prices.

### Incremental models show old data after a fix

**Cause:** Incremental logic reached back 7 days, fetched old state, then
`delete+insert` overwrote the fix.

**Fix:** Full refresh.

```bash
uv run ct dbt-refresh
```

This runs with `dbt build --full-refresh`, triggering `execute: true` on the
incremental models and rebuilding them from scratch.

### "No rows returned" from a test or query

**Cause 1:** Schema or table name mismatch.

```sql
.schema gold.fct_coin_price_daily
select * from gold.fct_coin_price_daily limit 1;
```

Confirm the table exists and has rows.

**Cause 2:** Filter is too narrow.

```sql
select count(*) from gold.fct_coin_price_daily
where price_date >= current_date;  -- might be zero if current_date hasn't loaded yet
```

Check the range:

```sql
select min(price_date), max(price_date) from gold.fct_coin_price_daily;
```

If `max(price_date) < current_date`, re-run `uv run ct ingest`.

### dbt test reports "Unexpected number of rows"

**Cause 1:** `dbt source freshness` threshold exceeded.

```
[ERROR]: Source "raw.coins_markets_raw" has not been updated since 26 hours ago.
```

This is a warning by design. Ignore it if you intentionally paused the DAG.

**Cause 2:** A relationships test found nulls or foreign-key mismatches.

Read the error message; it names the model, column, and foreign table. Inspect
the data:

```sql
select * from gold.fct_coin_price_daily
where coin_key is null limit 5;
```

If rows exist with NULL keys, the upstream transformation is dropping data.
Check the silver model's `ref()` join.

### Airflow task retries forever

**Cause:** Pool is exhausted or a hard dependency is not met.

**Fix:** Run `uv run ct status` to see task states. Check:

1. Is another task holding the pool? `uv run ct tables` if the pipeline is stuck.
2. Is the source data fresh? `uv run ct ingest` manually.
3. Are there DuckDB lock errors? Quit all open shells and re-run `uv run ct
   trigger`.
### `dlt.pipeline.config` is missing

**Cause:** dlt `__init__.py` incomplete or version mismatch.

**Fix:** Reinstall the venv.

```bash
rm -rf .venv
uv run ct setup
uv run ct airflow-init
uv run ct run
```

---

## Did it work?

After `uv run ct run` completes:

```bash
uv run ct tables         # four schemas; each has rows
uv run ct portfolio      # 9 rows (one per currency)
uv run ct performance    # 10 rows (one per coin, USD)
uv run ct test           # PASS=80 WARN=0 ERROR=0
```

If all four succeed, the pipeline is live.
