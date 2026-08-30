"""DuckDB helper for the workshop.

Uses the in-venv ``duckdb`` package (already pinned in ``pyproject.toml``),
so participants do not need a separate ``brew install duckdb``.

Run as ``python -m scripts.duckdb_cli <subcommand>``; the ``ct`` entry
point sets ``PYTHONPATH`` and ``CRYPTO_DB_PATH`` for us, and falls back to
the project ``data/crypto.duckdb`` if ``CRYPTO_DB_PATH`` is unset.
"""

from __future__ import annotations

import argparse
import os
import sys
import threading
from pathlib import Path

import duckdb

# arrow-key history in the REPL. Verified importable in the project venv.
try:
    import readline  # noqa: F401
except ImportError:  # pragma: no cover - readline is missing on Windows
    pass


DEFAULT_DB = Path(__file__).resolve().parents[1] / "data" / "crypto.duckdb"


def resolve_db(path: str | None) -> str:
    """Return ``path`` if given, else ``$CRYPTO_DB_PATH``, else the project default."""
    if path:
        return path
    env = os.environ.get("CRYPTO_DB_PATH")
    return env if env else str(DEFAULT_DB)


# Workshop SQL: backslash continuations in shell one-liners become
# ordinary newlines inside a triple-quoted string.
TABLES_SQL = """
    select schema_name as layer, table_name as object, 'TABLE' as kind, estimated_size as approx_rows
    from duckdb_tables() where schema_name in ('raw','bronze','silver','gold')
    union all
    select schema_name, view_name, 'VIEW', null from duckdb_views()
    where schema_name in ('raw','bronze','silver','gold') and not internal
    order by 1, 2
"""

PORTFOLIO_SQL = """
    select currency_code, round(total_value_local, 2) as value,
           round(total_unrealized_pnl_pct, 2) as pnl_pct, holdings_count
    from gold.mart_portfolio_summary
    where price_date = (select max(price_date) from gold.mart_portfolio_summary)
    order by currency_code
"""

PERFORMANCE_SQL = """
    select symbol, round(latest_price_usd, 2) as price_usd,
           round(return_7d_pct, 1) as d7, round(return_30d_pct, 1) as d30,
           round(volatility_30d_pct, 1) as vol_30d, market_cap_rank as rank
    from gold.mart_coin_performance where currency_code = 'USD'
    order by market_cap_rank
"""


def cmd_check_lock(args: argparse.Namespace) -> int:
    db = resolve_db(args.db)
    try:
        with duckdb.connect(db, read_only=True):
            pass
    except duckdb.IOException as e:
        # DuckDB's message already names the holding path and PID, so we just
        # pass it through inside an actionable wrapper.
        print("The warehouse is locked by another process:", file=sys.stderr)
        print(str(e).strip(), file=sys.stderr)
        print("Close it first - Ctrl-D in that shell, or quit 'uv run ct ui'.", file=sys.stderr)
        return 1
    return 0
def _show(rel: duckdb.DuckDBPyRelation) -> None:
    # CLI prints every row; the Python default paginates past 20 rows.
    rel.show(max_rows=10_000)


def _run_sql(db: str, sql: str, read_only: bool) -> None:
    import contextlib
    import io
    with duckdb.connect(db, read_only=read_only) as con:
        # Capture ``.show()`` and trim its trailing blank line so output
        # matches what the CLI's ``-c`` mode prints (no trailing blank).
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            con.sql(sql).show(max_rows=10_000)
        sys.stdout.write(buf.getvalue().rstrip("\n") + "\n")

def cmd_tables(args: argparse.Namespace) -> int:
    _run_sql(resolve_db(args.db), TABLES_SQL, read_only=True)
    return 0


def cmd_portfolio(args: argparse.Namespace) -> int:
    _run_sql(resolve_db(args.db), PORTFOLIO_SQL, read_only=True)
    return 0


def cmd_performance(args: argparse.Namespace) -> int:
    _run_sql(resolve_db(args.db), PERFORMANCE_SQL, read_only=True)
    return 0


def cmd_ui(args: argparse.Namespace) -> int:
    db = resolve_db(args.db)
    # The UI extension stores notebooks in a `_duckdb_ui` catalog, so we cannot
    # open read-only (Binder Error: Catalog "_duckdb_ui" does not exist). Keep
    # the connection alive until Ctrl-C releases the lock.
    print("DuckDB UI on http://localhost:4213")
    print("NOTE: read-write, holds the lock. Ctrl-D before running the pipeline.")
    stop = threading.Event()
    try:
        con = duckdb.connect(db, read_only=False)
        con.sql("install ui; load ui;")
        con.sql("call start_ui_server()")
        stop.wait()
    except KeyboardInterrupt:
        pass
    finally:
        try:
            con.close()
        except Exception:  # noqa: BLE001 - best-effort cleanup
            pass
    return 0


def _shell_loop(con: duckdb.DuckDBPyConnection) -> None:
    """Prompt loop. Accumulates multi-line input until it ends in ``;``."""
    buf = ""
    while True:
        try:
            line = input("D ") if not buf else input(".. ")
        except EOFError:
            print()
            return
        except KeyboardInterrupt:
            print()
            buf = ""
            continue

        stripped = line.strip()
        if not buf and stripped.startswith("."):
            if stripped == ".tables":
                _show(con.sql("show all tables"))
            elif stripped.startswith(".schema"):
                parts = stripped.split(None, 1)
                name = parts[1].strip() if len(parts) == 2 else None
                if not name:
                    print("Usage: .schema <name>")
                else:
                    _show(con.sql(f"describe {name}"))
            else:
                print("Unsupported dot-command. Use .tables or .schema <name>.")
            continue

        buf = (buf + " " + line).strip() if buf else line
        if buf.endswith(";"):
            try:
                _show(con.sql(buf))
            except duckdb.Error as e:
                print(e)
            buf = ""


def cmd_shell(args: argparse.Namespace) -> int:
    db = resolve_db(args.db)
    with duckdb.connect(db, read_only=args.readonly) as con:
        _shell_loop(con)
    return 0


def cmd_sql(args: argparse.Namespace) -> int:
    _run_sql(resolve_db(args.db), args.query, read_only=True)
    return 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="python -m scripts.duckdb_cli")
    sub = p.add_subparsers(dest="subcommand", required=True)

    p_check = sub.add_parser("check-lock", help="probe the warehouse lock")
    p_check.add_argument("--db", default=None)
    p_check.set_defaults(func=cmd_check_lock)

    p_tables = sub.add_parser("tables", help="list tables and views")
    p_tables.add_argument("--db", default=None)
    p_tables.set_defaults(func=cmd_tables)

    p_port = sub.add_parser("portfolio", help="portfolio value and P&L by currency")
    p_port.add_argument("--db", default=None)
    p_port.set_defaults(func=cmd_portfolio)

    p_perf = sub.add_parser("performance", help="trailing returns per coin (USD)")
    p_perf.add_argument("--db", default=None)
    p_perf.set_defaults(func=cmd_performance)

    p_ui = sub.add_parser("ui", help="launch the DuckDB UI on :4213")
    p_ui.add_argument("--db", default=None)
    p_ui.set_defaults(func=cmd_ui)

    p_shell = sub.add_parser("shell", help="interactive SQL shell")
    p_shell.add_argument("--db", default=None)
    p_shell.add_argument(
        "--readonly",
        action="store_true",
        help="open the database read-only (use for inspection)",
    )
    p_shell.set_defaults(func=cmd_shell)

    p_sql = sub.add_parser("sql", help="execute one SQL statement and exit")
    p_sql.add_argument("--db", default=None)
    p_sql.add_argument("query", help="SQL statement to execute (read-only)")
    p_sql.set_defaults(func=cmd_sql)

    return p


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
