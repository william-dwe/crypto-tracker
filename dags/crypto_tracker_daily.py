"""Daily crypto tracker ELT: CoinGecko + Frankfurter -> bronze -> silver -> gold.

Airflow 3 authoring: imports come from ``airflow.sdk``, ``schedule`` replaces
``schedule_interval``, and ``Asset`` replaces ``Dataset``.

The single most important detail is ``pool="duckdb_writer"``. DuckDB allows only
one writing process at a time, and a held write lock rejects even *read-only*
connections from other processes. Airflow tasks are separate processes, so the
1-slot pool is what prevents the ingest and dbt tasks — or a manual run happening
at the same time — from colliding on the file.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import pendulum
from airflow.sdk import Asset, dag, task

REPO_ROOT = Path(__file__).resolve().parent.parent
TRANSFORM_DIR = REPO_ROOT / "transform"
VENV_BIN = REPO_ROOT / ".venv" / "bin"
DB_PATH = os.environ.get("CRYPTO_DB_PATH", str(REPO_ROOT / "data" / "crypto.duckdb"))

# The `ingest` package lives at the repo root, not inside dags/, so make it
# importable even when PYTHONPATH was not exported by the caller.
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

# The ingest task lands directly in dlt's `bronze` schema; dbt adds its `br_*`
# views there and materializes silver/gold into their own schemas.
BRONZE = Asset(name="crypto_bronze", uri="duckdb://crypto.duckdb/bronze")
GOLD = Asset(name="crypto_gold", uri="duckdb://crypto.duckdb/gold")

DUCKDB_POOL = "duckdb_writer"


@dag(
    dag_id="crypto_tracker_daily",
    description="Ingest CoinGecko + FX into DuckDB and build the silver/gold medallion layers.",
    # 02:00 UTC: after CoinGecko finalises the previous UTC day (~00:10 UTC).
    schedule="0 2 * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    # Both APIs serve a rolling current window; replaying old logical dates would
    # re-fetch identical data.
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    default_args={"owner": "data-platform"},
    tags=["crypto", "elt", "duckdb"],
)
def crypto_tracker_daily():
    @task(
        task_id="ingest_raw",
        pool=DUCKDB_POOL,
        outlets=[BRONZE],
        # Absorbs CoinGecko 429s that outlive the in-resource backoff.
        retries=3,
        retry_delay=pendulum.duration(minutes=5),
    )
    def ingest_raw() -> str:
        from ingest.run_ingest import run_ingest

        run_ingest()
        return DB_PATH

    @task.bash(
        task_id="dbt_build",
        pool=DUCKDB_POOL,
        outlets=[GOLD],
        # Without an explicit cwd the command runs in a temp directory.
        cwd=str(TRANSFORM_DIR),
        retries=1,
        retry_delay=pendulum.duration(minutes=2),
        env={"CRYPTO_DB_PATH": DB_PATH, "PATH": os.environ.get("PATH", "")},
        append_env=True,
    )
    def dbt_build() -> str:
        # Absolute path into the project venv; a bare `dbt` may not be on PATH
        # in the worker environment.
        return (
            f"{VENV_BIN}/dbt build "
            f"--project-dir {TRANSFORM_DIR} "
            f"--profiles-dir {TRANSFORM_DIR}"
        )

    ingest_raw() >> dbt_build()


crypto_tracker_daily()
