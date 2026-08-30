"""Entry point that loads both sources directly into the `bronze` layer of the DuckDB file.

Run directly (``python -m ingest.run_ingest``) or via the Airflow
``ingest_raw`` task, which calls :func:`run_ingest`.
"""

from __future__ import annotations

import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import dlt
import duckdb

from ingest import (
    DB_PATH,
    FIAT_CURRENCIES,
    HISTORY_DAYS,
    INCREMENTAL_DAYS,
    INCREMENTAL_FX_DAYS,
    TRACKED_COINS,
)
from ingest.coingecko import coingecko_history_source, coingecko_markets_source
from ingest.fx import fx_source

logger = logging.getLogger(__name__)

# Must differ from `dataset_name`; identical values make the DuckDB catalog and
# schema names ambiguous and produce a binder error.
PIPELINE_NAME = "crypto_tracker"

# dlt binds ONE dataset per pipeline, so the source tables (`coins_markets_raw`,
# `coin_market_chart_raw`, `fx_rates_raw`) land together with dlt's bookkeeping
# (`_dlt_loads`, `_dlt_version`, `_dlt_pipeline_state`) and the
# `<dataset>_staging` merge schema — none of which can be split across schemas.
# So dlt lands directly in `bronze`, the dbt `br_*` views live in the same schema
# alongside the dlt tables, and the trust boundary is `br_completed_loads`
# filtering `_dlt_loads.status = 0`.
DATASET_NAME = "bronze"


def _table_has_rows(table: str) -> bool:
    """Check for existing bronze data using a short-lived read-only connection.

    Opened and closed *before* dlt takes its write lock. DuckDB refuses a
    read-only connection while another process holds a write lock, so these two
    must never overlap.
    """
    if not Path(DB_PATH).exists():
        return False
    try:
        con = duckdb.connect(DB_PATH, read_only=True)
    except duckdb.Error as exc:
        logger.warning("Could not open %s read-only (%s); assuming first run", DB_PATH, exc)
        return False
    try:
        exists = con.execute(
            "select count(*) from information_schema.tables "
            "where table_schema = ? and table_name = ?",
            [DATASET_NAME, table],
        ).fetchone()[0]
        if not exists:
            return False
        return con.execute(f'select count(*) from {DATASET_NAME}."{table}"').fetchone()[0] > 0
    finally:
        con.close()


def run_ingest() -> None:
    Path(DB_PATH).parent.mkdir(parents=True, exist_ok=True)

    history_backfilled = _table_has_rows("coin_market_chart_raw")
    fx_backfilled = _table_has_rows("fx_rates_raw")

    history_days = INCREMENTAL_DAYS if history_backfilled else HISTORY_DAYS
    fx_days = INCREMENTAL_FX_DAYS if fx_backfilled else HISTORY_DAYS

    # One timestamp shared by every row of this run. `last_updated` from the API
    # is per-coin and can be weeks stale, so it cannot identify a snapshot.
    ingested_at = datetime.now(timezone.utc).isoformat()

    logger.info(
        "Ingesting %s coins (history window %s days) and %s currencies (FX window %s days) into %s",
        len(TRACKED_COINS),
        history_days,
        len(FIAT_CURRENCIES),
        fx_days,
        DB_PATH,
    )

    pipeline = dlt.pipeline(
        pipeline_name=PIPELINE_NAME,
        destination=dlt.destinations.duckdb(DB_PATH),
        dataset_name=DATASET_NAME,
    )

    load_info = pipeline.run(
        [
            coingecko_markets_source(ingested_at=ingested_at),
            coingecko_history_source(days=history_days, ingested_at=ingested_at),
            fx_source(days=fx_days, ingested_at=ingested_at),
        ]
    )

    if load_info.has_failed_jobs:
        raise RuntimeError(f"dlt load had failed jobs: {load_info}")

    logger.info("Load complete: %s", load_info)


def main() -> int:
    logging.basicConfig(
        level=os.environ.get("CRYPTO_LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    run_ingest()
    return 0


if __name__ == "__main__":
    sys.exit(main())
