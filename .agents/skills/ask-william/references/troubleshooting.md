# Troubleshooting — quick reference

Condensed from `docs/TROUBLESHOOTING.md`. For the full symptom→cause→fix table and detailed scenarios, read that doc.

## Read dbt errors first

When `dbt build` fails, the error block tells you:

1. The model that failed and why (SQL syntax, compilation, test failure).
2. The compiled SQL at `transform/target/compiled/crypto_tracker/...` — the exact query that ran.
3. The raw database error (DuckDB errors are usually terse and exact).

Open the compiled file. Paste it into `uv run ct query-ro` and inspect the data yourself.

## Symptom → cause → fix

| Symptom | Cause | Fix |
|---|---|---|
| `IO Error: Could not set lock on file "data/crypto.duckdb": Conflicting lock is held` | Another process (shell, DAG task, UI, DBeaver) holds the file open. | Close `uv run ct query` / `query-ro` / `ui`. Quit any DBeaver connection. The `duckdb_writer` Airflow pool will free when the task finishes. |
| `ModuleNotFoundError: No module named 'dbt'` | venv not activated. | `source .venv/bin/activate`, or `uv run ct setup` to re-create. |
| `(psycopg2.OperationalError) could not translate host name "localhost" to address` | dbt profile is pointing at Postgres, but Postgres isn't running. | Confirm `transform/profiles.yml` has `type: duckdb` (the default is correct). |
| `Error: CRYPTO_TRACKED_COINS not set in environment` | `.env` not copied, or the key in `.env` has a typo. | `cp .env.example .env`. `ct` loads only the four user knobs; path vars must NOT be in `.env`. |
| `ValueError: 'coins_markets_raw' does not exist` (during dbt) | No data yet (ingest hasn't run). | `uv run ct ingest` first, or `uv run ct run` for the full pipeline. |
| `SchemaNotImplementedError: ... incremental load requires an existing table` | Incremental model first run (table doesn't exist yet). | `uv run ct dbt-refresh` (full refresh), or `uv run ct clean-db && uv run ct run`. |
| `pydantic_core._pydantic_core.ValidationError ... EventLogConfig` | Airflow metadata DB corrupted or schema mismatch. | `uv run ct clean` (drops Airflow metadata) then `uv run ct airflow-init` (recreates). |
| `Unauthorized. You must provide a valid access token` | CoinGecko 401 — hit the history-request ceiling. | Wait a few hours. CoinGecko free tier caps at 4 history requests per minute. 401 is never retried (retrying cannot help). |
| `<Response [429]>` in the logs (but pipeline continues) | CoinGecko 429 — rate limited. | Expected. `ingest/coingecko.py` backs off 3 seconds and retries. If aggressive, lower `CRYPTO_TRACKED_COINS`. |
| `TypeError: unsupported operand type(s) for *: 'Decimal' and 'Decimal'` in dbt | Arithmetic overflowed `DECIMAL(38,18)`. | Use `{{ to_local(...) }}`, `{{ money(...) }}`, `{{ fx_rate(...) }}` macros; see `transform/macros/money.sql`. |
| `dbt_versions not in state; cannot run selection check` | dbt version mismatch or `dbt parse` not run yet. | `dbt parse` (usually run by `dbt build`). If persistent, `uv run ct clean-db && uv run ct run`. |
| `IO Error: Catalog "main_gold" does not exist!` | dbt-duckdb's default `main_` schema prefix is applied. | Confirm `transform/macros/generate_schema_name.sql` exists; it removes the `main_` prefix. |
| `DatabaseError 23505: Duplicate key value violates unique constraint` | dbt test or unique constraint failure. | Read the error; check the model named. Inspect silver for missing deduplication or wrong grain. |
| `UnicodeDecodeError: 'utf-8' codec can't decode byte 0x...` | dlt or dbt read a file with the wrong encoding. | Rare. Check `.env`, `pyproject.toml`, SQL files are UTF-8 (not UTF-16 BOM). `file -I .env`. |
| `ModuleNotFoundError: No module named 'scripts'` | `python -m scripts.duckdb_cli ...` was run from outside the repo root, so `PYTHONPATH` doesn't include `.`. | Run from the repo root, or use `uv run ct tables` / `uv run ct query-ro` (which export `PYTHONPATH` for you). |
| `Binder Error: Catalog "_duckdb_ui" does not exist!` | Tried to open the warehouse read-only for `ui` — but the UI extension needs read-write. | Use `uv run ct query-ro` for a guaranteed read-only session. The `ui` command is read-write by design. |

## Did it work? — four checks

After `uv run ct run` completes (or `python -m ingest.run_ingest` then `dbt build`):

```bash
uv run ct tables         # four schemas; each has rows
uv run ct portfolio      # 9 rows (one per currency)
uv run ct performance    # 10 rows (one per coin, USD)
uv run ct test           # PASS=75 WARN=0 ERROR=0 SKIP=0
```


If all four succeed, the pipeline is live. If `uv run ct test` reports a different `PASS=` count, count the dbt tests in `transform/models/{bronze,silver,gold}/*/schema.yml` to find the gap.

## Specific scenarios

### The first run is slow (~4 minutes)

**Cause.** CoinGecko free tier rate limit (4 history requests per minute). One call per coin per table = up to ~20 requests. The ingest paces them 3 s apart with exponential backoff on 429s.

**Fix.** None needed. Second runs are fast (2-day window, usually 1-2 minutes). Set a timer and come back.

### Portfolio value is NULL

**Cause 1 — no join match.** Holdings and coins have no overlap.

```sql
select h.holding_id, h.coin_id
from transform.seeds.portfolio_holdings h
left join gold.dim_coin d on d.coin_id = h.coin_id
where d.coin_key is null;
```

If rows come back, the coin isn't in the warehouse. Add it to `CRYPTO_TRACKED_COINS` and re-ingest.

**Cause 2 — no date overlap.** Holding acquired after the last loaded price.

```sql
select min(h.acquired_date), max(p.price_date)
from transform.seeds.portfolio_holdings h
cross join gold.fct_coin_price_daily p
where p.coin_id = h.coin_id;
```

If `acquired_date > max(price_date)`, re-run `uv run ct ingest` to fetch today's prices.

### Incremental models show old data after a fix

**Cause.** Incremental logic reached back 7 days, fetched old state, then `delete+insert` overwrote the fix.

**Fix.** `uv run ct dbt-refresh` runs with `dbt build --full-refresh`, triggering `execute: true` on the incremental models and rebuilding from scratch.

### "No rows returned" from a test or query

**Cause 1 — schema or table name mismatch.**

```sql
.schema gold.fct_coin_price_daily
select * from gold.fct_coin_price_daily limit 1;
```

**Cause 2 — filter too narrow.**

```sql
select min(price_date), max(price_date) from gold.fct_coin_price_daily;
```

If `max(price_date) < current_date`, re-run `uv run ct ingest`.

### dbt test reports "Unexpected number of rows"

**Cause 1 — source freshness threshold exceeded.** `[ERROR]: Source "bronze.coins_markets_raw" has not been updated since 26 hours ago.` This is a warning by design. Ignore it if you intentionally paused the DAG.

**Cause 2 — relationships test found nulls.** Read the error; it names the model, column, and foreign table. Inspect:

```sql
select * from gold.fct_coin_price_daily where coin_key is null limit 5;
```

If rows exist with NULL keys, the upstream transformation is dropping data. Check the silver model's `ref()` join.

### Airflow task retries forever

**Cause.** Pool exhausted or a hard dependency not met.

**Fix.** `uv run ct status` to see task states. Then:

1. Another task holding the pool? `uv run ct tables` (only safe explore subcommand during a pipeline run).
2. Source data stale? `uv run ct ingest` manually.
3. DuckDB lock errors? Quit all open shells and `uv run ct trigger`.

### `dlt.pipeline.config` is missing

**Cause.** dlt `__init__.py` incomplete or version mismatch.

**Fix.** Reinstall the venv.

```bash
rm -rf .venv
uv run ct setup
uv run ct airflow-init
uv run ct run
```

## When to escalate

If the symptom is not in this file or in `docs/TROUBLESHOOTING.md`:

1. Read the error message carefully — it usually names the file/line/column.
2. For dbt errors, open `transform/target/compiled/crypto_tracker/...` for the compiled SQL.
3. For dlt errors, check `~/.dlt/pipelines/crypto_tracker/` for trace JSON.
4. For Airflow errors, the task log in the UI is canonical; the Airflow metadata DB is at `.airflow/airflow.db`.
5. For DuckDB errors, the error is usually terse and exact — re-read it.
