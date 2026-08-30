"""Frankfurter FX raw-layer resource.

The entire 365-day backfill for all tracked currencies is a *single* request:
8 quotes x 255 ECB business days = 2040 rows in one response.

Two non-obvious requirements:

* ``providers=ECB`` is mandatory. Without it Frankfurter blends providers and
  returns quotes with *different dates in the same response* (JPY on one day,
  everything else on the next), which corrupts any date-keyed join.
* v2 (not v1) returns a flat ``[{date, base, quote, rate}, ...]`` array. v1
  returns a nested date-keyed object that would need unnesting.

ECB publishes only on TARGET business days, so roughly 110 of every 365 calendar
days are absent. That gap is closed downstream in ``stg_fx_rates_filled``, not
here — the raw layer stays faithful to the source.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Iterator

import dlt
import requests

from ingest import (
    BASE_CURRENCY,
    DECIMAL_HINT,
    FIAT_CURRENCIES,
    FRANKFURTER_BASE_URL,
    HISTORY_DAYS,
)

logger = logging.getLogger(__name__)

REQUEST_TIMEOUT_SECONDS = 45


@dlt.resource(
    name="fx_rates_raw",
    write_disposition="merge",
    # Frankfurter restates the current day until ECB finalises it, so re-running
    # must update in place instead of appending a second row for the same key.
    primary_key=["date", "base", "quote"],
    columns={"rate": DECIMAL_HINT},
)
def fx_rates_raw(
    days: int = HISTORY_DAYS,
    currencies: list[str] | None = None,
    ingested_at: str | None = None,
) -> Iterator[dict[str, Any]]:
    currencies = currencies if currencies is not None else FIAT_CURRENCIES
    now = datetime.now(timezone.utc)
    ingested_at = ingested_at or now.isoformat()

    end_date = now.date()
    start_date = end_date.toordinal() - days
    start_date = datetime.fromordinal(start_date).date()

    params = {
        "base": BASE_CURRENCY,
        "quotes": ",".join(currencies),
        "from": start_date.isoformat(),
        "to": end_date.isoformat(),
        # Pins every quote in the response to a single publication date.
        "providers": "ECB",
    }

    response = requests.get(
        f"{FRANKFURTER_BASE_URL}/rates", params=params, timeout=REQUEST_TIMEOUT_SECONDS
    )

    if response.status_code == 422:
        # A currency is not in ECB's ~30-currency set. Fail loudly rather than
        # silently dropping a currency the user asked to report in.
        try:
            message = response.json().get("message", response.text)
        except ValueError:
            message = response.text
        raise RuntimeError(
            f"Frankfurter rejected the requested currencies {currencies}: {message}"
        )

    if response.status_code != 200:
        raise RuntimeError(
            f"Frankfurter returned HTTP {response.status_code}: {response.text[:500]}"
        )

    payload = response.json()
    if not isinstance(payload, list):
        raise RuntimeError(f"Expected a JSON array from Frankfurter v2, got {type(payload).__name__}")

    count = 0
    for item in payload:
        rate = item.get("rate")
        if rate is None:
            continue
        yield {
            "date": item["date"],
            "base": item["base"],
            "quote": item["quote"],
            "rate": Decimal(str(rate)),
            "_ingested_at": ingested_at,
        }
        count += 1

    logger.info(
        "Loaded %s FX rows for %s quotes between %s and %s",
        count,
        len(currencies),
        start_date,
        end_date,
    )


def fx_source(
    days: int = HISTORY_DAYS,
    currencies: list[str] | None = None,
    ingested_at: str | None = None,
):
    return fx_rates_raw(days=days, currencies=currencies, ingested_at=ingested_at)
