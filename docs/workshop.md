# Workshop

A guided walkthrough for people new to data engineering. Nine modules, nine
exercises with solutions. ~3.5 hours with breaks.

We won't be utilizing the `ct` wrapper script on this session to make sure you are getting used with the tool's common commands.

## Who this is for

You are comfortable with:

- Basic SQL: `select`, `from`, `where`, `join`, `group by`.
- A terminal: you can `cd`, `ls`, and edit text files.
- `git` basics: clone, add, commit.

You have **not** used dbt, dlt, Airflow, dimensional modelling, or any cloud
data platform. That is fine — each is introduced as it appears.

## Before the session

**Install the prerequisites,** the first full pipeline run is ~4
minutes of rate-limited HTTP 

This workshop utilize various different tools such as: 
1. `dlt` (through a Python module), 
2. `dbt`,
3. `airflow`, 
4. DuckDB. 

No need to install them all by yourself 1-by-1, you can rely on the `UV` to create a virtual environtment (more detail mentioned below).

### Tools you install at home

You install two tools. Everything else — Airflow 3.3.1, dbt-core 1.12.3, dbt-duckdb 1.11.0, dlt 1.30.0, DuckDB 1.5.5 — arrives pinned inside the `.venv` the repo creates via `uv sync`. No manual installs of those, no API keys required.

| Tool | Why | Install (macOS) | Install (Windows) | Install (Ubuntu/Debian) |
|---|---|---|---|---|
| `uv` | Creates the venv, installs the pinned stack, and runs every command below | `brew install uv` | `winget install astral-sh.uv` | `curl -LsSf https://astral.sh/uv/install.sh \| sh` (or `sudo apt install uv` after [adding the uv apt repo](https://docs.astral.sh/uv/getting-started/installation/)) |
| `git` | Cloning the repo | `xcode-select --install` (bundled with the Xcode Command Line Tools) | `winget install Git.Git` (or install [Git for Windows](https://git-scm.com/download/win)) | `sudo apt install git` |

### Step 0 — one-time environment setup

```bash
# 1. clone the repo
git clone <repo-url> && cd crypto-tracker

# 2. create the virtualenv from the lockfile
uv sync

# 3. activate it. This puts `dbt`, `airflow`, and the project `python` on PATH.
#    Activation is not optional cosmetics: `airflow standalone` spawns its
#    scheduler and webserver children by bare name, so they must resolve on PATH.
source .venv/bin/activate
```

Every command in this document assumes the venv is active. If you prefer not to
activate, prefix each command with `.venv/bin/` (e.g. `.venv/bin/dbt`), except
the Airflow ones, which need the venv on PATH anyway.

**On Windows (PowerShell)**, the activation script and binary paths differ.
Use these in place of the POSIX forms above:

```powershell
# activate the venv
.venv\Scripts\Activate.ps1

# if you prefer not to activate, run tools by absolute path:
.venv\Scripts\python.exe -m scripts.duckdb_cli tables
# Airflow still needs the venv on PATH either way:
$env:PATH = "$PWD\.venv\Scripts;$env:PATH"
```

### Step 0b — environment variables

Run these from the repo root in every new shell (or add them to your shell
profile):

```bash
# Airflow keeps its metadata DB, logs, and generated password inside the repo
# instead of ~/airflow, so a `rm -rf .airflow` fully resets it.
export AIRFLOW_HOME="$PWD/.airflow"

# Without this, Airflow ships ~50 example DAGs into your UI and hides ours.
export AIRFLOW__CORE__LOAD_EXAMPLES=False

# Lets `python -m ingest.run_ingest` and `python -m scripts.duckdb_cli` import
# the repo packages regardless of which directory you are standing in.
export PYTHONPATH="$PWD"

# OPTIONAL: your own coin/currency choices. Nothing auto-loads .env, so source
# it yourself. First time only: copy the template, then edit it. Skip the
# whole block and the defaults in ingest/__init__.py apply (10 coins,
# 8 fiat currencies). Only CRYPTO_TRACKED_COINS, CRYPTO_FIAT_CURRENCIES,
# CRYPTO_LOG_LEVEL, and INTER_COIN_SLEEP_SECONDS are meant to be set this way.
cp .env.example .env   # first time only; edit your coins/currencies in it
set -a; source .env; set +a
```

`CRYPTO_DB_PATH` is deliberately **not** exported here. Both the ingest code and
the DuckDB helper fall back to the repo's `data/crypto.duckdb`. Only dbt needs
it, and it is exported inside the dbt section below where the correct relative
path is obvious.

**On Windows (PowerShell)**, the equivalents are:

```powershell
$env:AIRFLOW_HOME               = "$PWD\.airflow"
$env:AIRFLOW__CORE__LOAD_EXAMPLES = "False"
$env:PYTHONPATH                 = "$PWD"

# OPTIONAL: your own coin/currency choices
Copy-Item .env.example .env     # first time only; edit it
Get-Content .env | ForEach-Object {
    if ($_ -match '^\s*([^#][^=]*)=(.*)$') {
        [System.Environment]::SetEnvironmentVariable($matches[1].Trim(), $matches[2].Trim(), 'Process')
    }
}
```

### Step 0c — initialise Airflow

```bash
airflow db migrate
airflow pools set duckdb_writer 1 "Serializes DuckDB write access"
```

The pool is not optional decoration: both DAG tasks declare
`pool="duckdb_writer"`, so without this 1-slot pool they would queue forever.
It serialises the two tasks so they never hold the DuckDB write lock at the
same time (Module 8, Exercise 3).

No `airflow users create` here on purpose: `airflow standalone` (Module 8)
auto-generates an admin password and writes it to
`$AIRFLOW_HOME/simple_auth_manager_passwords.json.generated`.

### Step 1 — load source data (dlt)

```bash
python -m ingest.run_ingest   # take a coffee break; this is the slow one
```

### Step 2 — build the models (dbt)

```bash
cd transform
export CRYPTO_DB_PATH="$PWD/../data/crypto.duckdb"
dbt build --profiles-dir .
cd ..
```

### Step 3 — did it work?

```bash
python -m scripts.duckdb_cli tables       # every schema and row count
python -m scripts.duckdb_cli portfolio    # 9 rows of currency, value, P&L
```

If `tables` shows non-zero rows in all three schemas (`bronze`, `silver`,
`gold`) and `portfolio` prints 9 rows, you are ready.

## Suggested timing

| Module | Topic | Minutes |
|---|---|---|
| 1 | What a data pipeline is, and what ELT means | 20 |
| 2 | Extract & Load with dlt | 30 |
| 3 | The warehouse: DuckDB | 20 |
| 4 | Transform with dbt | 35 |
| 5 | Medallion layering | 25 |
| 6 | Dimensional modelling | 35 |
| 7 | Data quality | 25 |
| 8 | Orchestration with Airflow | 35 |
| 9 | Analytics on the gold layer | 25 |
| **Total** | | **~3.5 h** |

Every module uses the same four headings: **Concept** → **In this repo** → **Try it** → **Exercise**.

---

## Module 1 — What a data pipeline is, and what ELT means

### Concept

A data pipeline takes data from somewhere, transforms it, and puts it
somewhere useful. "Somewhere" is a system (a file, a database, an API) and the
transformations are the steps in between.

The "ELT" variant lands data **first** and transforms it **after**, in the
warehouse itself. The opposite ("ETL") transforms data *before* loading. ELT is
now dominant because:

- Source systems often can't or won't transform (external APIs, third-party
  data).
- Warehouses are fast and cheap — putting the data there once and reusing it
  beats re-extracting.
- The dlt tables are a backup: if your transformation breaks, the source data
  is still safe.

### In this repo

Two sources: CoinGecko (crypto prices) and Frankfurter (ECB FX rates). One
warehouse: DuckDB at `data/crypto.duckdb`. One transformation: dbt, into
bronze/silver/gold. One orchestrator: Airflow.

### Try it

```bash
python -m scripts.duckdb_cli tables   # see every schema and row count
```

Now re-run the pipeline and watch the log. It is **two separate tools**, run one
after the other — this repo never hides that behind a single command:

**Step A — ingest (dlt).** A Python program that calls the APIs and lands the
source tables. dlt is a library, not a CLI: you run the project's own module.


```bash
python -m ingest.run_ingest
```

**Step B — transform (dbt).** A separate tool with its own project directory,
`transform/`, that reshapes what dlt landed. It never touches the APIs.

```bash
cd transform
export CRYPTO_DB_PATH="$PWD/../data/crypto.duckdb"
dbt build --profiles-dir .
cd ..
```

dbt is downstream of dlt. Nothing in dbt can run before dlt has landed the
source tables, which is exactly why they are two commands and not one.


### Exercise

See [Exercise 1 — Track a new coin](#exercise-1--track-a-new-coin) at the end.

---

## Module 2 — Extract & Load with dlt

### Concept

dlt (data load tool) is a Python library that extracts data from any source
and loads it into a warehouse. Key ideas:

- A **resource** is a function that yields rows (dictionaries).
- A **source** is a collection of related resources.
- A **pipeline** runs the source into a destination (DuckDB here) and tracks
  load state.

dlt inspects the first batch of rows and infers a schema. It writes the data,
remembers what it wrote, and on the next run only loads new data (incremental).

### In this repo

`ingest/coingecko.py` has two resources (`coins_markets_raw` and
`coin_market_chart_raw`), and `ingest/fx.py` has one (`fx_rates_raw`). They are
wrapped into sources and loaded by `ingest/run_ingest.py`.

### Try it

```bash
# re-run ingest only; fast on the second run because dlt loads incrementally
python -m ingest.run_ingest
ls data/            # the DuckDB file grew slightly
python -m scripts.duckdb_cli sql "select count(*) from bronze.coins_markets_raw"

```

Prefer zero local setup? Run this module in Colab: [`notebooks/ingest_dlt_colab.ipynb`](https://colab.research.google.com/github/william-dwe/crypto-tracker/blob/main/notebooks/ingest_dlt_colab.ipynb).

No dbt here on purpose: this is the load half of ELT, and it stands alone.

### Exercise

See [Exercise 2 — Reproduce the dlt variant-column trap](#exercise-2--reproduce-the-dlt-variant-column-trap).

---

## Module 3 — The warehouse: DuckDB

### Concept

DuckDB is an **embedded** analytical database: a single-file SQL engine in your
process, with the speed of a columnar warehouse and no server to manage. It is
ideal for local development, notebooks, and small-to-medium analytics.

The one rule to remember: **DuckDB allows only one writer at a time**. A held
write lock rejects even read-only connections from other processes. This is
why the Airflow tasks share a 1-slot pool (Module 8) and why you must close
your shell before re-running the pipeline.

### In this repo
The file is at `data/crypto.duckdb`. dlt writes directly to the `bronze`
schema; dbt adds its `br_*` views there and materializes silver/gold into
their own schemas (DuckDB calls these "databases" — each is a separate
namespace within the file).


### Try it

```bash
# read-only shell; --readonly keeps you from taking the write lock
python -m scripts.duckdb_cli shell --readonly
.tables             # list every table/view
.schema gold.dim_coin   # show the CREATE statement
```

### Exercise

See [Exercise 3 — Feel the lock](#exercise-3--feel-the-lock).

---

## Module 4 — Transform with dbt

### Concept

dbt (data build tool) is SQL with a DAG. You write `select` statements; dbt
turns them into a dependency graph and runs them in order, with tests on each
model.

- `ref('model_name')` creates a dependency on another model.
- `{{ config(materialized='table') }}` controls how the model is built (view,
  table, incremental).
- `dbt build` runs models, tests, and seeds in dependency order.
- `dbt run` runs models only. `dbt test` runs tests only. `dbt seed` loads
  CSVs.

### In this repo

`transform/models/` holds 18 `.sql` files in `bronze/`, `silver/`, and
`gold/`. `transform/macros/` holds reusable SQL fragments. `transform/seeds/`
holds CSVs (the portfolio holdings). `transform/dbt_project.yml` sets per-folder
materialization (bronze/silver = view, gold = table).

### Try it

dbt is its own tool with its own project root. Every dbt command is run from
`transform/`, and needs two things that are easy to forget:

- `CRYPTO_DB_PATH`, because `transform/profiles.yml` reads
  `{{ env_var('CRYPTO_DB_PATH') }}` and has no default — dbt errors out without it.
- `--profiles-dir .`, because the profile lives in the project directory, not in
  the default `~/.dbt/`.

```bash
cd transform
export CRYPTO_DB_PATH="$PWD/../data/crypto.duckdb"

dbt build --profiles-dir .            # build + test the medallion
dbt docs generate --profiles-dir .    # build the lineage metadata
dbt docs serve --port 8081 --profiles-dir .
# open http://localhost:8081 and click around the DAG
```

Other dbt subcommands you will use later, same directory, same flags:

```bash
dbt run --profiles-dir .                    # models only, no tests
dbt test --profiles-dir .                   # tests only
dbt build --full-refresh --profiles-dir .   # rebuild incremental models from scratch
```

The lineage graph is the moment the project becomes visual. Every node is a
model; every line is a `ref()`. Click any node to see its columns and tests.

Prefer zero local setup? Run this module in Colab: [`notebooks/dbt_colab.ipynb`](https://colab.research.google.com/github/william-dwe/crypto-tracker/blob/main/notebooks/dbt_colab.ipynb).

### Exercise

See [Exercise 4 — Add a column through the layers](#exercise-4--add-a-column-through-the-layers).

---

## Module 5 — Medallion layering

### Concept

The medallion pattern has three layers, each refining the one below:

- **Bronze**: as-landed from the source, with quality filters applied.
- **Silver**: cleaned, typed, deduplicated, joined.
- **Gold**: conformed to the dimensional model, business-facing.

The discipline is: each layer is a peer-reviewed transition. If a fact
"belongs" in bronze, it stays in bronze. If a fact "belongs" in gold, it
moves to gold. No back-edges, no skipping layers.

### In this repo
- **bronze** (4 views): filters the dlt tables (in the same `bronze` schema) to completed loads.

- **silver** (4 models): types, dedupes, fills gaps.
- **gold** (10 tables): dims, facts, marts.

### Try it

```bash
python -m scripts.duckdb_cli tables
# look at row counts at each layer; bronze views should roughly match the dlt tables,
# and gold should have derived numbers (facts, marts) with much higher counts
# because of the cross-join fan-out in mart_portfolio_value_daily
```

### Exercise

See [Exercise 5 — Write a data test](#exercise-5--write-a-data-test).

---

## Module 6 — Dimensional modelling

### Concept

Dimensional modelling organises data for analytics. Two structures:

- **Dimensions**: descriptive context (who, what, where, when).
- **Facts**: measurements or events, with foreign keys to dimensions.

A **star schema** puts facts at the centre and dimensions around them like
points of a star. This makes analytical queries fast (small fact tables, wide
denormalised dimensions) and predictable (joins always go through
fact → dimension).

The **grain** of a fact is the answer to "what does one row mean?". For
`fct_coin_price_daily`, the grain is "one coin on one date". Every measure
(price, market cap, volume) is at that grain. Every foreign key
(`coin_key`, `date_key`) is at that grain.

A **surrogate key** is an artificial primary key, often a hash of the natural
key. Surrogate keys are stable across full refreshes, while natural keys
(coingecko `id` slugs) are stable in their own right but harder to join on
because of type mismatches.

### In this repo

Three dimensions (`dim_coin`, `dim_currency`, `dim_date`) and three facts
(`fct_coin_price_daily`, `fct_coin_snapshot`, `fct_fx_rate_daily`). Four
datamarts read from dims and facts (not from silver/bronze). That last rule is
load-bearing: it guarantees the star schema is real, not decorative.

### Try it

```bash
python -m scripts.duckdb_cli shell --readonly
.schema gold.fct_coin_price_daily
# look at the comment in fct_coin_price_daily.sql:
# "delete+insert rather than merge because whole-day partitions are replaced
#  wholesale, and the lag() window needs preceding rows to be present."
```

### Exercise

See [Exercise 6 — Change the portfolio](#exercise-6--change-the-portfolio).

---

## Module 7 — Data quality

### Concept

Three kinds of dbt tests:

- **Generic tests**: built-in (not_null, unique, relationships, accepted_values).
  You list them in a model's YAML.
- **Singular tests**: one-off SQL files in `tests/`. A test passes when it
  returns zero rows.
- **Source freshness**: dbt checks the `_ingested_at` column of a source
  against a threshold.

A test that "always passes" is not a test — it is decoration. Tests must be
able to fail, and they must fail loudly enough that you notice.

### In this repo
75 declared tests (51 not_null, 16 relationships, 7 unique, 1 accepted_values)
plus the singular test in `transform/tests/assert_no_future_price_dates.sql`.
Source freshness is configured on `bronze.coins_markets_raw` (warn after 26
hours).

### Try it

```bash
cd transform
export CRYPTO_DB_PATH="$PWD/../data/crypto.duckdb"

dbt test --profiles-dir .
# all green? try breaking one
sed -i.tmp 's/where price_date >= current_date/where price_date < current_date/' tests/assert_no_future_price_dates.sql
rm tests/assert_no_future_price_dates.sql.tmp
dbt test --profiles-dir .   # should fail with 3650+ rows
# revert
git checkout tests/assert_no_future_price_dates.sql
dbt test --profiles-dir .   # green again
cd ..
```

### Exercise

See [Exercise 7 — Run the DAG](#exercise-7--run-the-dag).

---

## Module 8 — Orchestration with Airflow

### Concept

Airflow orchestrates tasks: a DAG is a directed acyclic graph of tasks, with
dependencies (`>>`), retries, schedules, and pools.

A **pool** is a concurrency limit. A 1-slot pool means "at most one task from
this pool can run at a time". Pools are for serialising access to a shared
resource (here, the DuckDB file).

Airflow 3 (the version this project uses) introduced a new authoring syntax:
imports from `airflow.sdk`, `schedule` instead of `schedule_interval`, and
`Asset` instead of `Dataset`.

### In this repo

`dags/crypto_tracker_daily.py` defines the DAG. Two tasks: `ingest_raw` and
`dbt_build`. Both run in the `duckdb_writer` pool (1 slot). The schedule is
`0 2 * * *` (02:00 UTC daily). Both tasks have retries with backoff.

### Try it

Airflow needs `AIRFLOW_HOME` exported (Step 0b) so it finds this repo's
metadata DB and the `dags/` folder instead of `~/airflow`.

```bash
# run the DAG end-to-end without a scheduler
airflow dags reserialize      # pick up any edits to dags/*.py
airflow dags test crypto_tracker_daily

# start scheduler + web UI on :8080 (foreground; Ctrl-C to stop)
airflow standalone
```

In another terminal, get the generated admin password:

```bash
cat "$AIRFLOW_HOME/simple_auth_manager_passwords.json.generated"
```

```text
# log in at http://localhost:8080, find crypto_tracker_daily, click "Graph"
# Admin -> Pools -> see duckdb_writer with 1 slot
```

Other Airflow commands you will want:

```bash
airflow dags unpause crypto_tracker_daily      # let the schedule fire
airflow dags pause crypto_tracker_daily        # stop it firing
airflow dags trigger crypto_tracker_daily      # run it now via the scheduler
airflow dags list-runs crypto_tracker_daily    # recent run states
```

### Exercise

See [Exercise 8 — Break and fix a dependency](#exercise-8--break-and-fix-a-dependency).

---

## Module 9 — Analytics on the gold layer

### Concept

The gold layer is built for queries. Window functions, joins, aggregations,
multi-currency conversions — all of it is ready to go.

Two patterns you'll see often:

- **Window functions** (`lag`, `lead`, `row_number`, `last_value ignore nulls`)
  for time-series computations.
- **Cross joins** for fan-out: take one row in a fact and turn it into many by
  joining against a calendar spine, currency list, or portfolio holdings.

### In this repo

`analysis_gold.sql` is a curated set of eight analyses: portfolio executive
summary, holdings & asset allocation, historical trajectory & rolling
drawdown, coin performance & volatility matrix, risk-adjusted momentum,
market breadth & sentiment, multi-currency FX sensitivity, intraday liquidity
& 24h spread. Open the file and run each section.

### Try it

```bash
python -m scripts.duckdb_cli shell --readonly
# paste this:
.mode line
select currency_code, round(total_value_local, 2), round(total_unrealized_pnl_pct, 2)
from gold.mart_portfolio_summary
where price_date = (select max(price_date) from gold.mart_portfolio_summary);
```

### Exercise

See [Exercise 9 — Write a gold-layer analysis](#exercise-9--write-a-gold-layer-analysis).

---

## Exercises

Each exercise has a goal, files, steps, a done condition, and a hidden
solution (click to reveal).

### Exercise 1 — Track a new coin

**Goal:** Add a coin to the tracked list and see it appear in the analysis.

**Files:** `.env` (or `.env.example` if you haven't copied it yet).

**Steps:**

1. Look up the CoinGecko slug for a coin you want to add. **Slug, not
   ticker** — `avalanche-2`, not `AVAX`. Try:
   ```bash
   curl -s 'https://api.coingecko.com/api/v3/search?query=tron' | python -m json.tool | head -30
   ```
2. Add the slug to `CRYPTO_TRACKED_COINS` in `.env` (comma-separated), then
   re-export it so this shell sees the change: `set -a; source .env; set +a`.
3. Re-run the pipeline — two tools, in order:
   ```bash
   python -m ingest.run_ingest   # dlt: fetch the coin, land it in bronze
   cd transform && export CRYPTO_DB_PATH="$PWD/../data/crypto.duckdb" && dbt build --profiles-dir . && cd ..
   ```
4. Check: `python -m scripts.duckdb_cli performance` — is the new symbol there?

**Done when:** the new symbol appears in the `performance` output.

<details><summary>Solution</summary>

`.env`:

```bash
CRYPTO_TRACKED_COINS=bitcoin,ethereum,solana,cardano,ripple,polkadot,chainlink,dogecoin,avalanche-2,litecoin,tron
```

Then, after sourcing `.env` again:

```bash
python -m ingest.run_ingest
cd transform && export CRYPTO_DB_PATH="$PWD/../data/crypto.duckdb" && dbt build --profiles-dir . && cd ..
python -m scripts.duckdb_cli performance
```

The new symbol should appear in the trailing-returns table.

**Watch out:** `ingest/run_ingest.py` decides the history window per **table**,
not per coin. If the warehouse already has any data, it fetches only a 2-day
window for every coin (including the new one), not a 365-day backfill. To get
full history for the new coin, wipe both stores —

```bash
rm -f data/crypto.duckdb data/crypto.duckdb.wal
rm -rf ~/.dlt/pipelines/crypto_tracker
```

— then re-run ingest and dbt, and accept the ~4-minute re-ingest.

</details>

### Exercise 2 — Reproduce the dlt variant-column trap

**Goal:** See the variant-column split happen, then fix it.

**Files:** a new file at `/tmp/trap.py` (not in the repo).

**Steps:**

1. Write a script that pipes `[{...}]` with whole-number and decimal prices
   through `dlt.pipeline(destination=dlt.destinations.duckdb("/tmp/trap.duckdb"))`.
2. Inspect the resulting columns: is there a `price__v_double`?
3. Modify the script to coerce values with `Decimal(str(v))` before yielding.
   Inspect again. Is the column unified?

**Done when:** you can point at `price__v_double` in the unfixed run and at a
single `price` column in the fixed run.

<details><summary>Solution</summary>

```python
# /tmp/trap.py
import dlt
from decimal import Decimal

# The trap: first row is an int, second is a float. dlt infers BIGINT and
# then splits later decimal rows into price__v_double.
unfixed = [
    {"id": 1, "price": 45000},
    {"id": 2, "price": 2345.67},
]
fixed = [
    {"id": 1, "price": Decimal("45000")},
    {"id": 2, "price": Decimal("2345.67")},
]

@dlt.resource(name="unfixed")
def unfixed_rows():
    yield from unfixed

@dlt.resource(name="fixed")
def fixed_rows_fn():
    yield from fixed

pipeline = dlt.pipeline(destination=dlt.destinations.duckdb("/tmp/trap.duckdb"))
pipeline.run([unfixed_rows(), fixed_rows_fn()])
print(pipeline.last_trace.last_normalize_info)
```

Run `python /tmp/trap.py`, then:

```bash
python -m scripts.duckdb_cli shell --readonly --db /tmp/trap.duckdb
describe trap_unfixed.price;     -- two columns: price BIGINT, price__v_double DOUBLE
describe trap_fixed.price;       -- one column: price DECIMAL(38,18)
```

The fix in `ingest/coingecko.py` is exactly this `Decimal(str(v))` coercion
before dlt ever inspects the row. See `_coerce_numerics` and `_to_decimal`.

</details>

### Exercise 3 — Feel the lock

**Goal:** Reproduce the DuckDB single-writer lock in two terminals.

**Files:** none (two terminals).

**Steps:**

1. Terminal A: `python -m scripts.duckdb_cli shell` (read-write shell — it stays open).
2. Terminal B: `python -m scripts.duckdb_cli tables`.
3. Read the error: `IO Error: Could not set lock on file ... Conflicting lock is held ...`.
4. In Terminal A, press Ctrl-D to exit.
5. Re-run Terminal B: it succeeds.

**Done when:** you have seen the lock error naming Terminal A's PID, then
watched the second command succeed once A quits.

<details><summary>Solution</summary>

Terminal A:

```bash
python -m scripts.duckdb_cli shell
```

The shell opens and waits at the prompt. While it's open, it holds an exclusive
lock on the DuckDB file.

Terminal B:

```bash
python -m scripts.duckdb_cli tables
```

Output (paths abbreviated):

```
The warehouse is locked by another process:
IO Error: Could not set lock on file "data/crypto.duckdb":
Conflicting lock is held in /Users/.../duckdb (PID 17432).
See also https://duckdb.org/docs/stable/connect/concurrency
Close it first - Ctrl-D in that shell, or quit 'uv run ct ui'.
```

That last line is printed verbatim by `scripts/duckdb_cli.py`; it names the
repo's optional `ct` helper, but the process actually holding the lock here is
your `shell` in Terminal A (or a `python -m scripts.duckdb_cli ui` session,
which is read-write and holds the lock until you Ctrl-C it).

Quit Terminal A (Ctrl-D), then Terminal B succeeds.

The production answer to this in `dags/crypto_tracker_daily.py` is the
`pool="duckdb_writer"` parameter on both tasks. A 1-slot pool guarantees no two
tasks can hold the file at once. You can probe the lock yourself before running
anything heavy:

```bash
python -m scripts.duckdb_cli check-lock   # exit 0 = free, exit 1 = held
```

It turns the raw `IOError` into an actionable message instead of a stack trace.

</details>

### Exercise 4 — Add a column through the layers

**Goal:** Add a new attribute to `dim_coin` and verify it propagates.

**Files:** `transform/models/gold/dim_coin.sql`,
`transform/models/gold/dim_coin.yml`.

**Steps:**

1. Read `dim_coin.sql` and find the final `select`. Notice the `history` CTE
   already computes `days_of_history`. We will add `days_since_first_price` as
   a derived column.
2. Add this line to the final select (just after `coalesce(history.days_of_history, 0)`):
   ```sql
   , date_diff('day', history.first_price_date, current_date) as days_since_first_price
   ```
3. In `dim_coin.yml`, add a corresponding entry under `columns:`:
   ```yaml
     data_tests: [not_null]
   ```
4. Rebuild with dbt:
   ```bash
   cd transform && export CRYPTO_DB_PATH="$PWD/../data/crypto.duckdb" && dbt build --profiles-dir . && cd ..
   ```

**Done when:** `select coin_id, days_since_first_price from gold.dim_coin` returns
sensible integers (between 0 and ~365).

<details><summary>Solution</summary>

The final select in `dim_coin.sql` becomes:

```sql
select
    cast(md5(latest_attributes.coin_id) as varchar)     as coin_key,
    latest_attributes.coin_id,
    latest_attributes.symbol,
    latest_attributes.name,
    latest_attributes.max_supply,
    observed.first_seen_date,
    observed.last_seen_date,
    coalesce(observed.snapshot_count, 0)                as snapshot_count,
    history.first_price_date,
    history.last_price_date,
    coalesce(history.days_of_history, 0)                as days_of_history,
    date_diff('day', history.first_price_date, current_date) as days_since_first_price,
    latest_attributes.snapshot_ts = latest_snapshot_ts.max_snapshot_ts as is_active
from latest_attributes
left join observed on observed.coin_id = latest_attributes.coin_id
left join history on history.coin_id = latest_attributes.coin_id
cross join latest_snapshot_ts
```

`dim_coin.yml` adds:

```yaml
        data_tests: [not_null]
```

That `dbt build` rebuilds the model and the test. Verify:

```bash
python -m scripts.duckdb_cli sql "select coin_id, days_since_first_price from gold.dim_coin order by days_since_first_price desc"
```

</details>

### Exercise 5 — Write a data test

**Goal:** Write a singular dbt test that catches a future regression.

**Files:** `transform/tests/assert_no_future_price_dates.sql` (new).

**Steps:**

1. Write a singular test that asserts `fct_coin_price_daily` contains no rows
   with `price_date >= current_date`.
2. Run the tests:
   ```bash
   cd transform && export CRYPTO_DB_PATH="$PWD/../data/crypto.duckdb" && dbt test --profiles-dir . && cd ..
   ```
   The new test should pass.
3. Temporarily invert the test to `where price_date < current_date`. Re-run
   the same `dbt test`. Confirm it fails with a row count.
4. Revert.

**Done when:** `dbt test` reports `PASS` for the new test, and the inverted
version reports a `FAIL` with a non-zero row count.

<details><summary>Solution</summary>

`transform/tests/assert_no_future_price_dates.sql`:

```sql
select
    coin_id,
    price_date
from {{ ref('fct_coin_price_daily') }}
where price_date >= current_date
```

`dbt test` should include `assert_no_future_price_dates` in its 80 tests and
report `PASS=80 ERROR=0`.

Invert to `where price_date < current_date`: `dbt test` reports
`FAIL 3650` (10 coins × 365 days) and `ERROR=1`.

Revert and confirm green again.

</details>

### Exercise 6 — Change the portfolio

**Goal:** Add a 4th holding to the seed CSV and see it in the analysis.

**Files:** `transform/seeds/portfolio_holdings.csv`.

**Steps:**

1. Add a row. Format: `holding_id,coin_id,quantity,acquired_date,cost_basis_usd`.
2. Rebuild the seed and its downstream models:
   ```bash
   cd transform && export CRYPTO_DB_PATH="$PWD/../data/crypto.duckdb" && dbt build --profiles-dir . && cd ..
   ```
3. Check: `python -m scripts.duckdb_cli portfolio` — does it now show 4 holdings?

**Done when:** the `portfolio` output shows `holdings_count = 4`.

<details><summary>Solution</summary>

Append a row (the file is gitignored-ish in spirit but tracked, so commit it):

```csv
holding_id,coin_id,quantity,acquired_date,cost_basis_usd
1,bitcoin,0.5,2025-11-15,42000.00
2,ethereum,4.0,2025-12-02,11200.00
3,solana,25.0,2026-01-20,4800.00
4,chainlink,120.0,2026-03-01,1800.00
```

Then `dbt build --profiles-dir .` and `python -m scripts.duckdb_cli portfolio`.

**Two non-obvious points:**

1. `cost_basis_usd` is the **total** paid for the lot, not a unit price. For
   120 LINK at an average entry of $15, write `1800.00`, not `15.00`.
2. `coin_id` must be in the tracked list. If you add a coin that isn't in
   `CRYPTO_TRACKED_COINS`, the inner join in `mart_portfolio_value_daily`
   silently drops the holding — that is the correct behaviour, and it means you
   need to add the coin to your `.env` and re-ingest first.

</details>

### Exercise 7 — Run the DAG

**Goal:** Run the DAG through the orchestrator and find the pool in the UI.

**Files:** none.

**Steps:**

1. `airflow dags reserialize && airflow dags test crypto_tracker_daily`
   (no scheduler needed).
2. `airflow standalone` (foreground; let it start the scheduler and webserver).
3. `cat "$AIRFLOW_HOME/simple_auth_manager_passwords.json.generated"` in another
   terminal to get the admin password.
4. Log in at <http://localhost:8080>.
5. Find `crypto_tracker_daily`. Open the **Graph** view.
6. Go to **Admin → Pools**. Find `duckdb_writer` with 1 slot.

**Done when:** you can state which task holds the slot and why the two tasks
can never run concurrently.

<details><summary>Solution</summary>

`airflow dags test crypto_tracker_daily` runs the DAG end-to-end. It
returns the task states and exit codes. The two tasks `ingest_raw` and
`dbt_build` are visible in the Graph view as boxes connected by an arrow
showing `ingest_raw >> dbt_build`.

Admin → Pools shows a table with `duckdb_writer` having **1 slot**. The DAG
file (`dags/crypto_tracker_daily.py`) declares
`pool="duckdb_writer"` on both `@task` decorators, so both tasks draw from
the same pool. With 1 slot, they cannot run concurrently.

The reason this is correct (not just faster) is the DuckDB single-writer lock:
two concurrent writers would corrupt the file. The pool is correctness, not
throughput.

</details>

### Exercise 8 — Break and fix a dependency

**Goal:** Reverse the DAG's task order, observe the failure, then revert.

**Files:** `dags/crypto_tracker_daily.py`.

**Steps:**

1. Find the final line of the `crypto_tracker_daily()` function:
   `ingest_raw() >> dbt_build()`. Reverse it to `dbt_build() >> ingest_raw()`.
2. Run `airflow dags reserialize && airflow dags test crypto_tracker_daily`.
3. Read the error or observe the result. Why is this wrong?
4. Revert: `ingest_raw() >> dbt_build()`.
5. Run `airflow dags reserialize && airflow dags test crypto_tracker_daily`
   again. Confirm it passes.

**Done when:** you can explain why the order matters even though both tasks
share one pool slot (the pool serialises, it does not sequence).

<details><summary>Solution</summary>

either build against the previous run's data (silently stale) or fail on a
fresh warehouse (no data at all, the bronze models' inner joins return nothing).

The pool gate is **not** a substitute for explicit dependencies. A 1-slot pool
guarantees one-at-a-time, but it does not say *which order* they run in. Only
the `>>` operator does that. The two are complementary: the pool prevents
concurrency, the operator defines sequence.

Revert:

```python
ingest_raw() >> dbt_build()
```

`airflow dags reserialize && airflow dags test crypto_tracker_daily` should now succeed.

</details>

### Exercise 9 — Write a gold-layer analysis

**Goal:** Write a query against the gold layer that combines windowed returns,
multi-currency conversion, and a history filter.

**Files:** none.

**Steps:**

1. Open `python -m scripts.duckdb_cli shell --readonly`.
2. Write a query against `gold.mart_coin_performance` that returns the top 3
   coins by 30-day return, priced in IDR, excluding coins with fewer than 90
   days of history.
3. Verify: 3 rows, IDR values, sorted descending by return.

**Done when:** the query returns exactly 3 rows with non-null `price_idr`.

<details><summary>Solution</summary>

```sql
select symbol, coin_name,
       round(latest_price_local, 2) as price_idr,
       round(return_30d_pct, 2)     as return_30d_pct,
       days_of_history
from gold.mart_coin_performance
where currency_code = 'IDR'
  and days_of_history >= 90
  and return_30d_pct is not null
order by return_30d_pct desc
limit 3;
```

Run with `python -m scripts.duckdb_cli shell --readonly`. Three rows; values in IDR; sorted by `return_30d_pct`
descending. The `mart_coin_performance` model is built per (coin, currency),
so filtering by `currency_code = 'IDR'` selects the IDR-denominated rows and
`latest_price_local` is already converted via `stg_fx_rates_filled`.

</details>


## Where to go next

Five directions, one line each:

  `transform/profiles.yml` and re-run `dbt build --profiles-dir .`. The medallion model is
  portable.
  project is currently all built-in generics on purpose.
  tests are deterministic and self-contained; no secrets needed.
  `dim_coin` (if symbol or name changes over time, you want history).
  real deployment, switch to the KubernetesExecutor or CeleryExecutor and put
  the DuckDB file on a shared volume.