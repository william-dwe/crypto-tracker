# Exercises — nine total, with gotchas

The nine exercises from `docs/workshop.md`. For each: goal, files you'll touch, the steps in order (no solution SQL), the done-condition, and the one gotcha that bites people. The full solution SQL is in the `<details>` blocks of `docs/workshop.md` — reveal it only on request or after a genuine attempt.

---

## Exercise 1 — Track a new coin (Module 2)

**Goal.** Add a new coin to the tracked set, re-ingest, verify it appears in the warehouse.

**Files.**
- `.env` (copy from `.env.example` first; edit `CRYPTO_TRACKED_COINS`).

**Steps.**
1. Pick a CoinGecko coin id not in the default list (e.g. `matic-network`).
2. Append it to `CRYPTO_TRACKED_COINS` in `.env`.
3. Run `uv run ct ingest` (classic: `python -m ingest.run_ingest`).
4. Query bronze for the new coin.

**Done condition.** `select count(distinct coin_id) from bronze.coin_market_chart_raw;` returns the new coin in the count.

**Gotcha.** dlt's incremental window is per-table, not per-coin. Re-running ingest with the new coin added gives you only the *last 2 days* of history for the new coin (the `INCREMENTAL_DAYS=2` window in `ingest/__init__.py`). To get full 365-day history for the new coin:

```bash
rm -f data/crypto.duckdb data/crypto.duckdb.wal
rm -rf ~/.dlt/pipelines/crypto_tracker
uv run ct run
```

This wipes the dlt pipeline state and the warehouse; re-ingest fetches the full `HISTORY_DAYS=365` window for every coin. Expect ~4 minutes for the first re-ingest.

---

## Exercise 2 — Variant-column trap (Module 3)

**Goal.** Encounter the dlt type-inference trap, then read the fix and explain it.

**Files.** None to edit. Observe in `uv run ct query-ro` and read `ingest/coingecko.py`.

**Steps.**
1. Add a coin whose `current_price` is always a whole number to `CRYPTO_TRACKED_COINS` (e.g. a stablecoin like `tether`).
2. Run `uv run ct ingest`.
3. In `uv run ct query-ro`:
   ```sql
   .schema bronze.coins_markets_raw
   select coin_id, current_price from bronze.coins_markets_raw where coin_id = 'tether';
   ```
4. Read the module docstring of `ingest/coingecko.py` and `_coerce_numerics` / `_to_decimal`.

**Done condition.** Observe that `current_price` is NULL for the new coin, and the column type is `BIGINT` (with a sibling `current_price__v_double` for the decimal-valued coins). Explain in your own words why this happens (dlt infers the column type from the *first* row it sees) and what the fix is (coerce every value to `Decimal` *before* dlt inspects it).

**Gotcha.** Declarative `columns` hints alone do NOT prevent the variant split — the values themselves must be `Decimal` before dlt inspects them. The why-comment is in the `ingest/coingecko.py` module docstring.

---

## Exercise 3 — Feel the lock (Module 3)

**Goal.** Experience the DuckDB single-writer lock and learn that read-only sessions also hold it.

**Files.** None to edit.

**Steps.**
1. Terminal A: `uv run ct query-ro` (or `python -m scripts.duckdb_cli shell --readonly`).
2. Terminal B: `uv run ct ingest`.
3. Watch Terminal B fail with `IO Error: Could not set lock on file "data/crypto.duckdb": Conflicting lock is held`.
4. Ctrl-D out of Terminal A; Terminal B's ingest resumes.

**Done condition.** You've seen the lock error and understand the rule: only `uv run ct tables` / `portfolio` / `performance` are safe to run while a pipeline is mid-task (they open, print, exit immediately). Everything else (`ui`, `query`, `query-ro`, `query`, DBeaver) blocks the writer.

**Gotcha.** The `ui` extension (`uv run ct ui`) opens the warehouse **read-write** because the UI extension stores its own notebooks in a `_duckdb_ui` catalog — opening read-only fails with `Binder Error: Catalog "_duckdb_ui" does not exist!`. So even "the UI" is a writer; quit it before running the pipeline.

---

## Exercise 4 — Add a column to a seed (Module 4)

**Goal.** Extend the portfolio seed with a new column, propagate it through dbt to a gold mart.

**Files.**
- `transform/seeds/portfolio_holdings.csv` (add the column with at least one populated row).
- `transform/seeds/portfolio_holdings.yml` (declare the column type in `+column_types`).

**Steps.**
1. Decide a column (e.g. `wallet_address varchar`).
2. Add the column to the CSV header and one row.
3. Add `wallet_address: varchar` under `+column_types` in `portfolio_holdings.yml`.
4. From `transform/`: `dbt build --select +portfolio_holdings+ mart_portfolio_value_daily` (classic) or `uv run ct dbt --select +mart_portfolio_value_daily` (shortcut, from repo root).
5. Query the gold mart.

**Done condition.** `select wallet_address from gold.mart_portfolio_value_daily limit 5;` returns non-NULL values for the row(s) that have it.

**Gotcha.** Skipping the `+column_types` declaration makes dbt infer the type. dbt-duckdb's type inference for an empty `varchar` column is often `VARCHAR` but the actual CSV parsing can give `NULL`-cast surprises; declare explicitly.

---

## Exercise 5 — Write a singular data test (Modules 5 + 7)

**Goal.** Write a singular dbt test that catches a class of bad data, then verify it fires when violated.

**Files.**
- `transform/tests/<your_test_name>.sql` (new).
- Optionally `transform/seeds/portfolio_holdings.csv` for the violation step.

**Steps.**
1. Pick a rule. Example: every holding in `transform.seeds.portfolio_holdings` must have `acquired_date <= current_date`.
2. Write a SQL query that returns rows on violation: `select h.* from {{ ref('portfolio_holdings') }} h where h.acquired_date > current_date;`
3. dbt singular tests: if the query returns rows, the test fails. End the file with `-- depends_on: {{ ref('portfolio_holdings') }}` if needed.
4. `cd transform && export CRYPTO_DB_PATH="$PWD/../data/crypto.duckdb" && dbt test --profiles-dir .` (classic) or `uv run ct test` (shortcut).
5. Temporarily violate your rule (edit the seed) to confirm the test fires. Revert.

**Done condition.** `uv run ct test` reports `PASS=75 WARN=0 ERROR=0 SKIP=0` (or `76` if you added a new test) when the data is clean, and the test name appears in the failure output when violated.



**Gotcha.** The canonical example `transform/tests/assert_no_future_price_dates.sql` is the right place to look for the depends_on pattern. Don't duplicate it; write a *different* rule.

---

## Exercise 6 — Change the portfolio (Module 6)

**Goal.** Add a holding for a coin not currently in the portfolio, see the gold mart reflect it.

**Files.**
- `transform/seeds/portfolio_holdings.csv`.

**Steps.**
1. Add a new row with a fresh `holding_id`, a `coin_id` from the tracked set, a `quantity`, an `acquired_date` at least 1 day before today, and a `cost_basis_usd`.
2. From `transform/`: `dbt build --select +mart_portfolio_value_daily` (classic) or `uv run ct dbt --select +mart_portfolio_value_daily` (shortcut, from repo root).
3. Query the gold mart.

**Done condition.** `select * from gold.mart_portfolio_value_daily where holding_id = <new_id>;` shows the new holding with daily values in every `currency_code` from `acquired_date` onward.

**Gotcha.** If the holding's `acquired_date` is *after* the latest `price_date` in the warehouse, the value is NULL. Re-run `uv run ct ingest` to fetch today's prices.

---

## Exercise 7 — Run the DAG (Module 8)

**Goal.** Trigger the `crypto_tracker_daily` DAG and watch both tasks succeed.

**Files.** None to edit.

**Steps.**
1. `uv run ct airflow-ui` (classic: `airflow standalone` with the env exports). Foreground: scheduler + webserver on `:8080`.
2. Get the auto-generated admin password: `uv run ct creds` (classic: `cat "$AIRFLOW_HOME/simple_auth_manager_passwords.json.generated"`).
3. Log in to the UI on `http://localhost:8080`. Find the `crypto_tracker_daily` DAG.
4. Trigger: `uv run ct trigger` (classic: `airflow dags trigger crypto_tracker_daily`). Or unpause to wait for the schedule.
5. Watch the run; both `ingest_raw` and `dbt_build` should turn green.
6. Verify: `uv run ct status` (classic: `airflow dags list-runs crypto_tracker_daily`).

**Done condition.** Both tasks succeed; the latest run appears with `state: success` in `uv run ct status`.

**Gotcha.** The `duckdb_writer` pool is needed for the DAG tasks to schedule. If you skipped `airflow pools set duckdb_writer 1 "Serializes DuckDB write access"` (or `uv run ct airflow-init`), the tasks will queue forever. Run `uv run ct airflow-init` (or the classic form) and retry.

---

## Exercise 8 — Break a dependency (Module 8)

**Goal.** Remove the `>>` between the two DAG tasks, observe the failure mode, restore it.

**Files.**
- `dags/crypto_tracker_daily.py`.

**Steps.**
1. Open the file; find the `ingest_raw >> dbt_build` line.
2. Replace it with two separate task definitions and no `>>` between them.
3. `airflow dags reserialize` (classic) or `uv run ct dag-test` (shortcut, runs end-to-end without a scheduler).
4. Observe the failure: a *missing dependency* error, NOT a lock conflict.
5. Restore the `>>`.

**Done condition.** The failure is reported as a missing dependency, not a DuckDB lock. The participant can articulate: the `duckdb_writer` pool serialises (1 slot) but does not sequence; `>>` does the sequencing. Both are required for correctness.

**Gotcha.** A DAG without `>>` could *still* succeed by accident (the scheduler happens to schedule them in order). Use `uv run ct dag-test` to force a deterministic order; the reserialize-and-test path surfaces the dependency problem reliably.

---

## Exercise 9 — Gold analysis (Module 9)

**Goal.** Pick an analysis from `analysis_gold.sql`, run it, modify it for a different question.

**Files.** None to edit.

**Steps.**
1. Open `analysis_gold.sql`; pick one of the 8 curated analyses.
2. Run it: `uv run ct sql "<paste the query>"` or copy into `uv run ct query-ro`.
3. Modify it for a related question (e.g. add a filter, change the time window, swap a measure).

**Done condition.** The participant can articulate what the original analysis answers in one sentence, and the modified analysis answers a related but different question.

**Gotcha.** The 8 analyses in `analysis_gold.sql` are not exhaustive — they're a starting point. The marts (`mart_market_overview`, `mart_coin_performance`, `mart_portfolio_summary`, `mart_portfolio_value_daily`) are the join points; reach for them when the analysis_gold queries don't fit.
