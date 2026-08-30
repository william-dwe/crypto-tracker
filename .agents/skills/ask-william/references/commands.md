# Commands — classic vs `uv run ct`

Every command in the repo exists in two forms that run the same subprocesses. The workshop (`docs/workshop.md`) uses classic form so every seam is visible. The shortcut `uv run ct <sub>` (`scripts/cli.py`) runs the same subprocess with `AIRFLOW_HOME`, `CRYPTO_DB_PATH`, `PYTHONPATH`, `PATH`, and `.env` (4 user knobs only) already set.

`uv run ct` with no subcommand prints the grouped list. `check-lock` and `sql` are technically present but `check-lock` is hidden from the printed help; both still work.

The full source of truth for subcommand names is `_COMMANDS` in `scripts/cli.py:105-137`.

## One-time shell exports (classic form)

Set these once per shell (or in your shell profile). The dbt rows additionally need `cd transform` and `CRYPTO_DB_PATH` exported.

```bash
source .venv/bin/activate
export AIRFLOW_HOME="$PWD/.airflow"
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export PYTHONPATH="$PWD"

# for dbt rows only, from transform/:
export CRYPTO_DB_PATH="$PWD/../data/crypto.duckdb"
```

## Mapping table

| Task | Classic command | Shortcut command |
|---|---|---|
| first-time setup | `uv sync` | `uv run ct setup` |
| init Airflow DB + pool | `airflow db migrate && airflow pools set duckdb_writer 1 "Serializes DuckDB write access"` | `uv run ct airflow-init` |
| full pipeline (ingest + dbt) | `python -m ingest.run_ingest && cd transform && export CRYPTO_DB_PATH="$PWD/../data/crypto.duckdb" && dbt build --profiles-dir . && cd ..` | `uv run ct run` |
| ingest only (dlt) | `python -m ingest.run_ingest` | `uv run ct ingest` |
| dbt build + test | `cd transform && export CRYPTO_DB_PATH="$PWD/../data/crypto.duckdb" && dbt build --profiles-dir . && cd ..` | `uv run ct dbt` |
| dbt full refresh | `cd transform && export CRYPTO_DB_PATH="$PWD/../data/crypto.duckdb" && dbt build --full-refresh --profiles-dir . && cd ..` | `uv run ct dbt-refresh` |
| dbt tests only | `cd transform && export CRYPTO_DB_PATH="$PWD/../data/crypto.duckdb" && dbt test --profiles-dir . && cd ..` | `uv run ct test` |
| dbt source freshness | `cd transform && export CRYPTO_DB_PATH="$PWD/../data/crypto.duckdb" && dbt source freshness && cd ..` | (use classic) |
| dbt lineage docs | `cd transform && export CRYPTO_DB_PATH="$PWD/../data/crypto.duckdb" && dbt docs generate --profiles-dir . && dbt docs serve --port 8081 --profiles-dir . && cd ..` | `uv run ct docs` |
| list tables | `python -m scripts.duckdb_cli tables` | `uv run ct tables` |
| portfolio | `python -m scripts.duckdb_cli portfolio` | `uv run ct portfolio` |
| performance | `python -m scripts.duckdb_cli performance` | `uv run ct performance` |
| SQL shell (RO) | `python -m scripts.duckdb_cli shell --readonly` | `uv run ct query-ro` |
| SQL shell (RW) | `python -m scripts.duckdb_cli shell` | `uv run ct query` |
| one-shot SQL | `python -m scripts.duckdb_cli sql "<query>"` | `uv run ct sql "<query>"` |
| lock probe | `python -m scripts.duckdb_cli check-lock` | `uv run ct check-lock` |
| browser SQL IDE | (none — open DBeaver / `duckdb-ui` against `data/crypto.duckdb`) | `uv run ct ui` (port 4213) |
| Airflow UI (standalone) | `airflow standalone` | `uv run ct airflow-ui` (port 8080) |
| admin creds | `cat "$AIRFLOW_HOME/simple_auth_manager_passwords.json.generated"` | `uv run ct creds` |
| DAG test (no scheduler) | `airflow dags reserialize && airflow dags test crypto_tracker_daily` | `uv run ct dag-test` |
| DAG trigger | `airflow dags trigger crypto_tracker_daily` | `uv run ct trigger` |
| DAG run history | `airflow dags list-runs crypto_tracker_daily` | `uv run ct status` |
| unpause schedule | `airflow dags unpause crypto_tracker_daily` | `uv run ct unpause` |
| pause schedule | `airflow dags pause crypto_tracker_daily` | `uv run ct pause` |
| clean warehouse + dlt state | `rm -f data/crypto.duckdb data/crypto.duckdb.wal && rm -rf ~/.dlt/pipelines/crypto_tracker` | `uv run ct clean-db` |
| drop dbt artifacts + Airflow metadata | `rm -rf transform/target transform/logs .airflow` | `uv run ct clean` |
| full reset | (compose clean + clean-db) | `uv run ct clean && uv run ct airflow-init && uv run ct run` |

## Quick reference — what `ct` does

`uv run ct <sub>` invokes the venv binaries directly (no `source .venv/bin/activate` needed) and sets:

- `AIRFLOW_HOME="$PWD/.airflow"` — keeps Airflow metadata, logs, and the auto-generated admin password inside the repo. `rm -rf .airflow` fully resets.
- `CRYPTO_DB_PATH="$PWD/data/crypto.duckdb"` — absolute path to the warehouse.
- `PYTHONPATH="$PWD"` — lets `python -m scripts.duckdb_cli ...` and `python -m ingest.run_ingest` resolve from the repo root.
- `PATH` — venv `bin/` prepended so `airflow`, `dbt`, `python` resolve bare-name.
- `.env` — only the four user knobs (`CRYPTO_TRACKED_COINS`, `CRYPTO_FIAT_CURRENCIES`, `CRYPTO_LOG_LEVEL`, `INTER_COIN_SLEEP_SECONDS`) are loaded via `setdefault`. Real shell env wins. Path-style vars must NOT be in `.env` — `ct` will overwrite them anyway.

Help text per subcommand (verbatim from `_COMMANDS` in `scripts/cli.py`):

| Subcommand | Group | Help |
|---|---|---|
| `help` | Setup | show this help |
| `setup` | Setup | create the venv, install everything, create `data/` |
| `airflow-init` | Setup | migrate the Airflow DB and create the 1-slot DuckDB pool |
| `run` | Pipeline | ingest then build everything (the usual command) |
| `ingest` | Pipeline | load CoinGecko + FX into the bronze layer |
| `dbt` | Pipeline | build + test silver and gold |
| `dbt-refresh` | Pipeline | full-refresh rebuild of the incremental facts |
| `test` | Pipeline | run dbt tests only, without rebuilding |
| `check-lock` | (hidden) | internal prerequisite; real subcommand, hidden from help |
| `tables` | Explore | list every table and view with row counts |
| `portfolio` | Explore | portfolio value and P&L in every currency |
| `performance` | Explore | trailing returns and volatility per coin (USD) |
| `ui` | Explore | browser SQL notebook + schema tree on :4213 |
| `query` | Explore | read-write DuckDB shell |
| `query-ro` | Explore | read-only DuckDB shell |
| `sql` | Explore | execute one SQL statement and exit (read-only) |
| `airflow-ui` | Airflow | start scheduler + web UI on :8080 (foreground) |
| `creds` | Airflow | print the admin username and password |
| `trigger` | Airflow | trigger a DAG run now (needs the scheduler running) |
| `dag-test` | Airflow | run the DAG end to end without a scheduler |
| `status` | Airflow | show the last 5 DAG runs and their task states |
| `unpause` | Airflow | enable the schedule (runs daily at 02:00 UTC) |
| `pause` | Airflow | disable the schedule |
| `docs` | Maintenance | generate and serve the dbt lineage docs on :8081 |
| `clean-db` | Maintenance | delete the warehouse and dlt state (forces a full re-ingest) |
| `clean` | Maintenance | clean-db plus dbt artifacts and Airflow metadata |

## First run (shortcut form)

```bash
uv run ct setup
uv run ct airflow-init
uv run ct run           # ~4 minutes first time (rate limited)
uv run ct tables        # verify
uv run ct portfolio     # 9 rows
```

## First run (classic form)

```bash
git clone <repo-url> && cd crypto-tracker
uv sync
source .venv/bin/activate
export AIRFLOW_HOME="$PWD/.airflow" AIRFLOW__CORE__LOAD_EXAMPLES=False PYTHONPATH="$PWD"
airflow db migrate
airflow pools set duckdb_writer 1 "Serializes DuckDB write access"
python -m ingest.run_ingest    # ~4 minutes
cd transform && export CRYPTO_DB_PATH="$PWD/../data/crypto.duckdb" && dbt build --profiles-dir . && cd ..
python -m scripts.duckdb_cli tables
python -m scripts.duckdb_cli portfolio
```
