"""Single entry point for the crypto-tracker workshop.

Installs as the ``ct`` console script via ``[project.scripts]`` in
pyproject.toml. Every workshop command (``uv run ct <subcommand>``) is
dispatched through here so users have exactly one tool to remember (``uv``).

Usage::

    uv run ct           # print the grouped help
    uv run ct setup     # create the venv, install everything
    uv run ct run       # ingest + dbt
    uv run ct tables    # list warehouse objects
    ...

The environment block below runs at import time so every subprocess we
spawn sees the same paths. Airflow's standalone command and the dbt task
both spawn children by bare name, so the venv must be on PATH, not just
addressable by absolute path.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import sqlite3
import subprocess
import sys
import time
from pathlib import Path

# ---------------------------------------------------------------------------
# Environment (executed at import time so subprocesses see the same paths)
# ---------------------------------------------------------------------------

ROOT = Path(__file__).resolve().parents[1]
VENV = ROOT / ".venv" / "bin"
TRANSFORM = ROOT / "transform"
DATA_DIR = ROOT / "data"
AIRFLOW_HOME = ROOT / ".airflow"
DB_PATH = DATA_DIR / "crypto.duckdb"
DAG_ID = "crypto_tracker_daily"
# User-tunable knobs. Real shell env wins: `setdefault` only fills when a
# key is missing. uv does not auto-load .env for `uv run`; without this,
# customised tracked-coin / currency lists are silently ignored. Path-like
# keys (AIRFLOW_HOME, CRYPTO_DB_PATH, PYTHONPATH) are deliberately NOT
# loaded from .env: their defaults are absolute paths anchored at ROOT so
# the cwd-varied subprocesses (dbt, airflow standalone) always find them.
_ENV_FILE = ROOT / ".env"
_USER_KNOBS = {
    "CRYPTO_TRACKED_COINS",
    "CRYPTO_FIAT_CURRENCIES",
    "CRYPTO_LOG_LEVEL",
    "INTER_COIN_SLEEP_SECONDS",
}
if _ENV_FILE.exists():
    for _line in _ENV_FILE.read_text().splitlines():
        _line = _line.strip()
        if not _line or _line.startswith("#") or "=" not in _line:
            continue
        _key, _, _val = _line.partition("=")
        _key = _key.strip()
        if _key in _USER_KNOBS:
            os.environ.setdefault(_key, _val.strip().strip('"').strip("'"))

os.environ.setdefault("AIRFLOW_HOME", str(AIRFLOW_HOME))
os.environ.setdefault("AIRFLOW__CORE__LOAD_EXAMPLES", "False")
os.environ.setdefault("AIRFLOW__CORE__DAGS_FOLDER", str(ROOT / "dags"))
os.environ.setdefault("CRYPTO_DB_PATH", str(DB_PATH))

# venv must be on PATH (not merely addressable by absolute path) because
# `airflow standalone` and the dbt task spawn children by bare name.
_existing_pp = os.environ.get("PYTHONPATH")
os.environ["PYTHONPATH"] = f"{ROOT}{os.pathsep}{_existing_pp}" if _existing_pp else str(ROOT)
os.environ["PATH"] = f"{VENV}{os.pathsep}{os.environ.get('PATH', '')}"

# Imported AFTER env block so scripts.duckdb_cli sees the same AIRFLOW_HOME /
# CRYPTO_DB_PATH via os.environ. We only need the command functions + SQL
# constants from it; argparse inside duckdb_cli is not used here.
from scripts import duckdb_cli  # noqa: E402


# ---------------------------------------------------------------------------
# Subprocess helper
# ---------------------------------------------------------------------------

def _run(argv: list[str], cwd: Path | None = None, check: bool = True) -> int:
    """Run a subprocess, echoing its argv first so failures are diagnosable.

    Returns the child's exit code when check=False; raises CalledProcessError
    when check=True (the default).
    """
    print(f"$ {' '.join(str(a) for a in argv)}", file=sys.stderr)
    return subprocess.run(argv, cwd=cwd, check=check).returncode


# ---------------------------------------------------------------------------
# Command table. Group order is the help-section order; within each group,
# the order is the workshop execution order. Help strings are workshop
# vocabulary. `check-lock` is intentionally hidden from the printed help
# but is still a real subcommand (used internally by tables/portfolio/
# performance to fail fast on warehouse locks).

_COMMANDS: list[tuple[str, str, str]] = [
    # Setup
    ("help",         "Setup",       "show this help"),
    ("setup",        "Setup",       "create the venv, install everything, create data/"),
    ("airflow-init", "Setup",       "migrate the Airflow DB and create the 1-slot DuckDB pool"),
    # Pipeline
    ("run",          "Pipeline",    "ingest then build everything (the usual command)"),
    ("ingest",       "Pipeline",    "load CoinGecko + FX into the raw layer"),
    ("dbt",          "Pipeline",    "build + test silver and gold"),
    ("dbt-refresh",  "Pipeline",    "full-refresh rebuild of the incremental facts"),
    ("test",         "Pipeline",    "run dbt tests only, without rebuilding"),
    # Explore
    ("check-lock",   "",            ""),  # real subcommand; hidden from help (internal prerequisite)
    ("tables",       "Explore",     "list every table and view with row counts"),
    ("portfolio",    "Explore",     "portfolio value and P&L in every currency"),
    ("performance",  "Explore",     "trailing returns and volatility per coin (USD)"),
    ("ui",           "Explore",     "browser SQL notebook + schema tree on :4213"),
    ("query",        "Explore",     "read-write DuckDB shell"),
    ("query-ro",     "Explore",     "read-only DuckDB shell"),
    ("sql",          "Explore",     "execute one SQL statement and exit (read-only)"),
    # Airflow
    ("airflow-ui",   "Airflow",     "start scheduler + web UI on :8080 (foreground)"),
    ("creds",        "Airflow",     "print the admin username and password"),
    ("trigger",      "Airflow",     "trigger a DAG run now (needs the scheduler running)"),
    ("dag-test",     "Airflow",     "run the DAG end to end without a scheduler"),
    ("status",       "Airflow",     "show the last 5 DAG runs and their task states"),
    ("unpause",      "Airflow",     "enable the schedule (runs daily at 02:00 UTC)"),
    ("pause",        "Airflow",     "disable the schedule"),
    # Maintenance
    ("docs",         "Maintenance", "generate and serve the dbt lineage docs on :8081"),
    ("clean-db",     "Maintenance", "delete the warehouse and dlt state (forces a full re-ingest)"),
    ("clean",        "Maintenance", "clean-db plus dbt artifacts and Airflow metadata"),
]
# Help-visible: every entry with a non-empty group label. `sql` and
# `check-lock` have an empty group so they don't appear in the printed help
# table. `sql` still shows in argparse -h because it has a non-empty help
# string passed to add_parser. `check-lock` is a private prerequisite of
# tables/portfolio/performance.
_VISIBLE = [(n, g, h) for n, g, h in _COMMANDS if g and n != "sql"]


# ---------------------------------------------------------------------------
# Command implementations
# ---------------------------------------------------------------------------

def cmd_help(_: argparse.Namespace) -> int:
    print()
    print("  Crypto Tracker — CoinGecko + FX → DuckDB → dbt → Airflow")
    print()
    last_group = None
    for name, group, desc in _VISIBLE:
        if group != last_group:
            print(f"\n  {group}")
            last_group = group
        print(f"    {name:<14}{desc}")
    print()
    print("  First run:  uv run ct setup && uv run ct airflow-init && uv run ct run")
    print("  Then look:  uv run ct tables   (or  uv run ct ui  for a browser IDE)")
    print()
    return 0


def cmd_setup(_: argparse.Namespace) -> int:
    _run(["uv", "venv", "--python", "3.11", ".venv"], cwd=ROOT)
    _run(
        [
            "uv", "pip", "install", "--python", ".venv/bin/python",
            "apache-airflow==3.3.1", "dlt[duckdb]==1.30.0",
            "dbt-core==1.12.3", "dbt-duckdb==1.11.0", "duckdb==1.5.5",
            "pendulum>=3.1.0", "requests>=2.32.0",
        ],
        cwd=ROOT,
    )
    DATA_DIR.mkdir(exist_ok=True)
    return 0


def cmd_airflow_init(_: argparse.Namespace) -> int:
    _run([str(VENV / "airflow"), "db", "migrate"])
    _run(
        [
            str(VENV / "airflow"), "pools", "set",
            "duckdb_writer", "1", "Serializes DuckDB write access",
        ]
    )
    return 0


def cmd_run(_: argparse.Namespace) -> int:
    cmd_ingest(argparse.Namespace())
    cmd_dbt(argparse.Namespace())
    return 0


def cmd_ingest(_: argparse.Namespace) -> int:
    _run([str(VENV / "python"), "-m", "ingest.run_ingest"], cwd=ROOT)
    return 0


def _dbt_args(extra: list[str]) -> list[str]:
    return [
        str(VENV / "dbt"), *extra,
        "--project-dir", str(TRANSFORM),
        "--profiles-dir", str(TRANSFORM),
    ]


def cmd_dbt(_: argparse.Namespace) -> int:
    _run(_dbt_args(["build"]), cwd=TRANSFORM)
    return 0


def cmd_dbt_refresh(_: argparse.Namespace) -> int:
    _run(_dbt_args(["build", "--full-refresh"]), cwd=TRANSFORM)
    return 0


def cmd_test(_: argparse.Namespace) -> int:
    _run(_dbt_args(["test"]), cwd=TRANSFORM)
    return 0


def cmd_check_lock(args: argparse.Namespace) -> int:
    return duckdb_cli.cmd_check_lock(argparse.Namespace(db=args.db))


def _with_lock_check(fn, args: argparse.Namespace) -> int:
    rc = cmd_check_lock(args)
    if rc != 0:
        return rc
    return fn(args)


def cmd_tables(args: argparse.Namespace) -> int:
    return _with_lock_check(duckdb_cli.cmd_tables, argparse.Namespace(db=args.db))


def cmd_portfolio(args: argparse.Namespace) -> int:
    return _with_lock_check(duckdb_cli.cmd_portfolio, argparse.Namespace(db=args.db))


def cmd_performance(args: argparse.Namespace) -> int:
    return _with_lock_check(duckdb_cli.cmd_performance, argparse.Namespace(db=args.db))


def cmd_ui(args: argparse.Namespace) -> int:
    return duckdb_cli.cmd_ui(argparse.Namespace(db=args.db))


def cmd_query(args: argparse.Namespace) -> int:
    return duckdb_cli.cmd_shell(argparse.Namespace(db=args.db, readonly=False))


def cmd_query_ro(args: argparse.Namespace) -> int:
    return duckdb_cli.cmd_shell(argparse.Namespace(db=args.db, readonly=True))


def cmd_sql(args: argparse.Namespace) -> int:
    return duckdb_cli.cmd_sql(argparse.Namespace(db=args.db, query=args.query))


def cmd_creds(_: argparse.Namespace) -> int:
    cred_file = AIRFLOW_HOME / "simple_auth_manager_passwords.json.generated"
    if cred_file.exists():
        data = json.loads(cred_file.read_text())
        print(f"user: admin  password: {data['admin']}")
    else:
        print("No credentials yet - run 'uv run ct airflow-ui' once to generate them.")
    return 0


def cmd_airflow_ui(_: argparse.Namespace) -> int:
    print("Airflow UI on http://localhost:8080")
    cmd_creds(argparse.Namespace())
    _run([str(VENV / "airflow"), "standalone"])
    return 0


def cmd_trigger(_: argparse.Namespace) -> int:
    run_id = f"manual_{int(time.time())}"
    _run([str(VENV / "airflow"), "dags", "trigger", DAG_ID, "--run-id", run_id])
    return 0


def cmd_dag_test(_: argparse.Namespace) -> int:
    _run([str(VENV / "airflow"), "dags", "reserialize"])
    _run([str(VENV / "airflow"), "dags", "test", DAG_ID])
    return 0


def cmd_status(_: argparse.Namespace) -> int:
    db = AIRFLOW_HOME / "airflow.db"
    if not db.exists():
        print("No Airflow metadata DB yet - run 'uv run ct airflow-init'.")
        return 0
    try:
        con = sqlite3.connect(db)
        try:
            print('%-42s %-9s %s' % ('RUN', 'STATE', 'ENDED'))
            for r in con.execute(
                "select run_id, state, coalesce(end_date,'running') "
                "from dag_run where dag_id=? order by start_date desc limit 5",
                (DAG_ID,),
            ):
                print('%-42s %-9s %s' % (r[0][:42], r[1], r[2]))
            print()
            print('latest run tasks:')
            for t in con.execute(
                "select task_id, state from task_instance where run_id=("
                "  select run_id from dag_run where dag_id=? "
                "  order by start_date desc limit 1)",
                (DAG_ID,),
            ):
                print('  %-16s %s' % t)
        finally:
            con.close()
    except sqlite3.Error:
        print("No Airflow metadata DB yet - run 'uv run ct airflow-init'.")
    return 0


def cmd_unpause(_: argparse.Namespace) -> int:
    _run([str(VENV / "airflow"), "dags", "unpause", DAG_ID])
    return 0


def cmd_pause(_: argparse.Namespace) -> int:
    _run([str(VENV / "airflow"), "dags", "pause", DAG_ID])
    return 0


def cmd_docs(_: argparse.Namespace) -> int:
    _run(_dbt_args(["docs", "generate"]), cwd=TRANSFORM)
    _run(_dbt_args(["docs", "serve", "--port", "8081"]), cwd=TRANSFORM)
    return 0


def cmd_clean_db(_: argparse.Namespace) -> int:
    DB_PATH.unlink(missing_ok=True)
    wal = DB_PATH.with_suffix(DB_PATH.suffix + ".wal")
    wal.unlink(missing_ok=True)
    shutil.rmtree(Path.home() / ".dlt" / "pipelines" / "crypto_tracker", ignore_errors=True)
    return 0


def cmd_clean(_: argparse.Namespace) -> int:
    cmd_clean_db(argparse.Namespace())
    for rel in ("transform/target", "transform/logs", ".airflow"):
        shutil.rmtree(ROOT / rel, ignore_errors=True)
    return 0


# ---------------------------------------------------------------------------
# Argparse dispatch
# ---------------------------------------------------------------------------

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="ct",
        description="Single entry point for the crypto-tracker workshop.",
    )
    sub = p.add_subparsers(dest="command")

    for name, _, desc in _COMMANDS:
        sp = sub.add_parser(name, help=desc or None)
        sp.add_argument("--db", default=None, help="override CRYPTO_DB_PATH")
        if name == "sql":
            sp.add_argument("query", help="SQL statement to execute")

    return p

_DISPATCH = {
    "help":         cmd_help,
    "setup":        cmd_setup,
    "airflow-init": cmd_airflow_init,
    "run":          cmd_run,
    "ingest":       cmd_ingest,
    "dbt":          cmd_dbt,
    "dbt-refresh":  cmd_dbt_refresh,
    "test":         cmd_test,
    "check-lock":   cmd_check_lock,
    "tables":       cmd_tables,
    "portfolio":    cmd_portfolio,
    "performance":  cmd_performance,
    "ui":           cmd_ui,
    "query":        cmd_query,
    "query-ro":     cmd_query_ro,
    "sql":          cmd_sql,
    "airflow-ui":   cmd_airflow_ui,
    "creds":        cmd_creds,
    "trigger":      cmd_trigger,
    "dag-test":     cmd_dag_test,
    "status":       cmd_status,
    "unpause":      cmd_unpause,
    "pause":        cmd_pause,
    "docs":         cmd_docs,
    "clean-db":     cmd_clean_db,
    "clean":        cmd_clean,
}
assert set(_DISPATCH) == {n for n, _, _ in _COMMANDS}


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if not args.command:
        return cmd_help(args)
    return _DISPATCH[args.command](args)


if __name__ == "__main__":
    sys.exit(main())
