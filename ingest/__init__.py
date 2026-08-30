"""Single source of configuration for the crypto-tracker pipeline.

Every other module imports these names. Do not redefine the coin or currency
lists anywhere else.
"""

from __future__ import annotations

import os
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent

DB_PATH = os.environ.get("CRYPTO_DB_PATH", str(REPO_ROOT / "data" / "crypto.duckdb"))

TRACKED_COINS = [
    c.strip()
    for c in os.environ.get(
        "CRYPTO_TRACKED_COINS",
        "bitcoin,ethereum,solana,cardano,ripple,polkadot,chainlink,dogecoin,avalanche-2,litecoin",
    ).split(",")
    if c.strip()
]

FIAT_CURRENCIES = [
    c.strip().upper()
    for c in os.environ.get("CRYPTO_FIAT_CURRENCIES", "EUR,GBP,JPY,IDR,SGD,AUD,CHF,CAD").split(",")
    if c.strip()
]

BASE_CURRENCY = "USD"

# CoinGecko keyless free tier hard ceiling. Verified: days=366 and days=max are
# rejected; days=365 returns 366 daily points.
HISTORY_DAYS = 365

# Window re-fetched on incremental runs. 2 days covers the previous UTC day plus
# today and tolerates one missed run; the merge write disposition absorbs the
# overlap.
INCREMENTAL_DAYS = 2

# FX window re-fetched on incremental runs. Wider than the crypto window because
# ECB restates recent rates and skips weekends.
INCREMENTAL_FX_DAYS = 7

COINGECKO_BASE_URL = "https://api.coingecko.com/api/v3/"
FRANKFURTER_BASE_URL = "https://api.frankfurter.dev/v2"

# dlt column hint used for every monetary/quantity field. Without an explicit
# decimal hint AND value coercion, dlt infers BIGINT from the first whole-number
# row and then splits later decimal rows into `<col>__v_double` variant columns.
DECIMAL_HINT = {"data_type": "decimal", "precision": 38, "scale": 18}

__all__ = [
    "REPO_ROOT",
    "DB_PATH",
    "TRACKED_COINS",
    "FIAT_CURRENCIES",
    "BASE_CURRENCY",
    "HISTORY_DAYS",
    "INCREMENTAL_DAYS",
    "INCREMENTAL_FX_DAYS",
    "COINGECKO_BASE_URL",
    "FRANKFURTER_BASE_URL",
    "DECIMAL_HINT",
]
