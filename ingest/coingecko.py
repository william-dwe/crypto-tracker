"""CoinGecko resources.

Two resources with deliberately different shapes:

* ``coins_markets_raw`` — a *snapshot* of current market state, one row per coin
  per pipeline run, append-only.
* ``coin_market_chart_raw`` — *daily history*, one row per (coin, day), merged on
  its natural key so re-runs update rather than duplicate.

The single most important thing in this module is :func:`_coerce_numerics`.
CoinGecko returns ``current_price`` as a whole number for expensive coins
(bitcoin: ``78727``) and as a decimal for cheaper ones (ethereum: ``2498.2``).
dlt infers the column type from the first row it sees, so it lands BIGINT and
then diverts every later decimal value into a separate ``current_price__v_double``
variant column. A plain ``select current_price`` then silently returns NULL for
half the coins. Declarative ``columns`` hints alone do NOT prevent this — the
values themselves must be ``Decimal`` before dlt inspects them.
"""

from __future__ import annotations

import logging
import os
import time
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Iterator

import dlt
import requests
from dlt.sources.rest_api import rest_api_source

from ingest import (
    COINGECKO_BASE_URL,
    DECIMAL_HINT,
    HISTORY_DAYS,
    TRACKED_COINS,
)

logger = logging.getLogger(__name__)

# Every numeric field on /coins/markets. All of them are vulnerable to the
# whole-number-vs-decimal variant-column split described in the module docstring.
NUMERIC_FIELDS = [
    "current_price",
    "market_cap",
    "fully_diluted_valuation",
    "total_volume",
    "high_24h",
    "low_24h",
    "price_change_24h",
    "price_change_percentage_24h",
    "market_cap_change_24h",
    "market_cap_change_percentage_24h",
    "circulating_supply",
    "total_supply",
    "max_supply",
    "ath",
    "ath_change_percentage",
    "atl",
    "atl_change_percentage",
    "price_change_percentage_24h_in_currency",
    "price_change_percentage_7d_in_currency",
    "price_change_percentage_30d_in_currency",
]

CHART_NUMERIC_FIELDS = ["price_usd", "market_cap_usd", "total_volume_usd"]

# Pacing between per-coin history requests. The keyless tier sustains roughly
# 10-30 calls/min and a 20s recovery pause was measured as sometimes insufficient.
INTER_COIN_SLEEP_SECONDS = float(os.environ.get("INTER_COIN_SLEEP_SECONDS", "3.0"))
RETRY_BACKOFF_SECONDS = [15, 30, 60, 60]
REQUEST_TIMEOUT_SECONDS = 45

# CoinGecko signals "you asked for more history than your plan allows" with
# HTTP 401 (not 400/403) and this code. Retrying it never succeeds.
HISTORY_WINDOW_EXCEEDED_CODE = 10012


def _utc_now_iso() -> str:
    """One timestamp per pipeline run, shared by every row of a snapshot."""
    return datetime.now(timezone.utc).isoformat()


def _to_decimal(value: Any) -> Decimal | None:
    """Coerce a JSON number to ``Decimal`` via ``str`` to avoid binary float error."""
    if value is None:
        return None
    try:
        return Decimal(str(value))
    except (ArithmeticError, ValueError):
        logger.warning("Could not coerce %r to Decimal; storing NULL", value)
        return None


def _coerce_numerics(row: dict[str, Any], fields: list[str]) -> dict[str, Any]:
    for field in fields:
        if row.get(field) is not None:
            row[field] = _to_decimal(row[field])
    return row


def _stamp_market_row(row: dict[str, Any], ingested_at: str) -> dict[str, Any]:
    """Map step applied to every /coins/markets row before dlt types it."""
    out = {**row, "_ingested_at": ingested_at}
    # `roi` is a nested object that is null for almost every coin; keeping it
    # would create a child table for no analytical value.
    out.pop("roi", None)
    return _coerce_numerics(out, NUMERIC_FIELDS)


def _extract_error_codes(payload: Any) -> set[int]:
    """Find every ``error_code`` in an arbitrarily nested CoinGecko error body.

    Rate-limit errors nest one level (``{"status": {"error_code": 429}}``) but
    plan-limit errors nest two (``{"error": {"status": {"error_code": 10012}}}``),
    so this walks the whole structure rather than assuming a depth.
    """
    codes: set[int] = set()
    if isinstance(payload, dict):
        for key, value in payload.items():
            if key == "error_code" and isinstance(value, int):
                codes.add(value)
            else:
                codes |= _extract_error_codes(value)
    elif isinstance(payload, list):
        for item in payload:
            codes |= _extract_error_codes(item)
    return codes


class HistoryWindowExceeded(RuntimeError):
    """Raised when CoinGecko rejects the requested history window (code 10012)."""


def _get_with_backoff(url: str, params: dict[str, Any]) -> Any:
    """GET with capped exponential backoff on HTTP 429.

    Deliberately does not honour ``Retry-After`` as an upper bound: the observed
    keyless behaviour is that the advertised wait is sometimes too short.
    """
    last_error: Exception | None = None

    for attempt in range(len(RETRY_BACKOFF_SECONDS) + 1):
        response = requests.get(url, params=params, timeout=REQUEST_TIMEOUT_SECONDS)

        if response.status_code == 200:
            return response.json()

        try:
            body = response.json()
        except ValueError:
            body = {}
        codes = _extract_error_codes(body)

        if HISTORY_WINDOW_EXCEEDED_CODE in codes:
            raise HistoryWindowExceeded(
                f"CoinGecko rejected the requested history window for {url}: {body}"
            )

        if response.status_code == 429:
            if attempt < len(RETRY_BACKOFF_SECONDS):
                wait = RETRY_BACKOFF_SECONDS[attempt]
                logger.warning(
                    "CoinGecko rate limited %s; sleeping %ss (attempt %s/%s)",
                    url,
                    wait,
                    attempt + 1,
                    len(RETRY_BACKOFF_SECONDS),
                )
                time.sleep(wait)
                last_error = RuntimeError(f"HTTP 429 from {url}: {body}")
                continue
            raise RuntimeError(
                f"CoinGecko still rate limiting {url} after "
                f"{len(RETRY_BACKOFF_SECONDS)} retries: {body}"
            )

        raise RuntimeError(f"CoinGecko returned HTTP {response.status_code} for {url}: {body}")

    raise RuntimeError(f"Exhausted retries for {url}") from last_error


@dlt.resource(
    name="coin_market_chart_raw",
    write_disposition="merge",
    primary_key=["coin_id", "price_ts_ms"],
    columns={field: DECIMAL_HINT for field in CHART_NUMERIC_FIELDS},
)
def coin_market_chart_raw(
    days: int = HISTORY_DAYS,
    coins: list[str] | None = None,
    ingested_at: str | None = None,
) -> Iterator[dict[str, Any]]:
    """Daily OHLC-style history, one request per coin, paced to survive rate limits.

    ``/coins/{id}/market_chart`` returns three independent arrays of
    ``[timestamp_ms, value]``. They share timestamps, so they are zipped back into
    one row per (coin, day) rather than loaded as three child tables.
    """
    coins = coins if coins is not None else TRACKED_COINS
    ingested_at = ingested_at or _utc_now_iso()
    requested_days = min(days, HISTORY_DAYS)

    for index, coin_id in enumerate(coins):
        if index:
            time.sleep(INTER_COIN_SLEEP_SECONDS)

        url = f"{COINGECKO_BASE_URL}coins/{coin_id}/market_chart"
        params = {"vs_currency": "usd", "days": requested_days, "interval": "daily"}

        try:
            payload = _get_with_backoff(url, params)
        except HistoryWindowExceeded:
            # Free-tier ceiling. Clamp and retry once at the known-good window
            # rather than failing the whole load.
            if requested_days >= HISTORY_DAYS:
                logger.error("History window rejected for %s even at %s days", coin_id, HISTORY_DAYS)
                continue
            logger.warning("Clamping history window for %s to %s days", coin_id, HISTORY_DAYS)
            params["days"] = HISTORY_DAYS
            payload = _get_with_backoff(url, params)

        by_ts: dict[int, dict[str, Any]] = {}
        for series_key, column in (
            ("prices", "price_usd"),
            ("market_caps", "market_cap_usd"),
            ("total_volumes", "total_volume_usd"),
        ):
            for point in payload.get(series_key) or []:
                if not point or point[0] is None:
                    continue
                ts_ms = int(point[0])
                by_ts.setdefault(ts_ms, {})[column] = point[1]

        for ts_ms in sorted(by_ts):
            values = by_ts[ts_ms]
            row = {
                "coin_id": coin_id,
                "price_ts_ms": ts_ms,
                "price_date": datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
                .date()
                .isoformat(),
                "price_usd": values.get("price_usd"),
                "market_cap_usd": values.get("market_cap_usd"),
                "total_volume_usd": values.get("total_volume_usd"),
                "_ingested_at": ingested_at,
            }
            yield _coerce_numerics(row, CHART_NUMERIC_FIELDS)

        logger.info("Loaded %s daily history points for %s", len(by_ts), coin_id)


def coingecko_markets_source(coins: list[str] | None = None, ingested_at: str | None = None):
    """Declarative source for the /coins/markets snapshot endpoint."""
    coins = coins if coins is not None else TRACKED_COINS
    ingested_at = ingested_at or _utc_now_iso()

    source = rest_api_source(
        {
            "client": {
                "base_url": COINGECKO_BASE_URL,
                # Snapshot endpoint: no cursor, no next-page link.
                "paginator": "single_page",
            },
            "resources": [
                {
                    "name": "coins_markets_raw",
                    "write_disposition": "append",
                    "endpoint": {
                        "path": "coins/markets",
                        "data_selector": "$",
                        "params": {
                            "vs_currency": "usd",
                            "ids": ",".join(coins),
                            # Verified max; larger values silently fall back to 100.
                            "per_page": 250,
                            "page": 1,
                            "sparkline": False,
                            "price_change_percentage": "24h,7d,30d",
                        },
                    },
                    "processing_steps": [
                        {"map": lambda row: _stamp_market_row(row, ingested_at)},
                    ],
                }
            ],
        }
    )
    source.resources["coins_markets_raw"].apply_hints(
        columns={field: DECIMAL_HINT for field in NUMERIC_FIELDS}
    )
    return source


def coingecko_history_source(
    days: int = HISTORY_DAYS,
    coins: list[str] | None = None,
    ingested_at: str | None = None,
):
    """Wrap the history resource so the runner treats both sources alike."""
    return coin_market_chart_raw(days=days, coins=coins, ingested_at=ingested_at)
