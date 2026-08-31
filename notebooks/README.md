# Colab notebooks

Zero-local-setup twins of two workshop modules, for participants who want the dlt or dbt experience without cloning the repo, creating a venv, or holding any API key. Everything runs on Colab's free tier and disappears with the VM.

## The two notebooks

| Notebook | Workshop module | What you build | Pinned stack | Slowest cell |
| --- | --- | --- | --- | --- |
| [ingest_dlt_colab.ipynb](https://colab.research.google.com/github/william-dwe/crypto-tracker/blob/main/notebooks/ingest_dlt_colab.ipynb) | Module 2 — Extract & Load with dlt | CoinGecko prices and Frankfurter FX rates landed into a DuckDB `bronze` schema by dlt, plus the variant-column trap exercise | `dlt[duckdb]==1.30.0`, `duckdb==1.5.5` | the first ingest, about 4 minutes (CoinGecko's keyless rate limit, not slow code) |
| [dbt_colab.ipynb](https://colab.research.google.com/github/william-dwe/crypto-tracker/blob/main/notebooks/dbt_colab.ipynb) | Module 4 — Transform with dbt | a miniature bronze to silver to gold project — 2 sources, 2 trust-filter staging views, 1 joined mart, plus the `generate_schema_name` macro that removes dbt-duckdb's `main_` prefix | `dbt-core==1.12.3`, `dbt-duckdb==1.11.0`, `duckdb==1.5.5` | the install, 2–3 minutes; everything after is seconds |

The notebooks are independent — either can be run first, and neither needs the other's output.

## Copy one into your own Colab

1. Click a notebook link in the table above. It opens Colab with the copy that lives on GitHub.
2. Click **Copy to Drive** in the Colab toolbar (equivalently `File → Save a copy in Drive`). A new browser tab opens holding your own copy. **Why:** the GitHub-backed view is read-only, so without this step your edits and cell outputs have nowhere to be saved — Colab will refuse to save and you lose everything on the first disconnect.
3. Sign in with any Google account if prompted. There is nothing further to configure: no API keys, no secrets, no Drive mounting, no billing — CoinGecko's free tier and Frankfurter's ECB endpoint are both keyless.
4. Leave the runtime on its default. `Runtime → Change runtime type` should stay on CPU; neither notebook uses a GPU or TPU, and requesting one only slows down VM allocation. Colab's default Python is 3.12 and both notebooks' pinned stacks install cleanly on it.
5. Run the cells from top to bottom in order, waiting for each to finish before starting the next. **Why:** the first cell installs the pinned packages that every later cell imports, and within each notebook the later cells form a chain — in `dbt_colab.ipynb` the land cell creates the DuckDB file, the next cell writes the dbt project that reads it, and the build cell needs both. Running out of order fails with `ModuleNotFoundError` or a dbt compilation error rather than doing something subtly wrong.

`Runtime → Run all` works for either notebook end to end; use it once you have read the cells, not before, since the point of the workshop is reading them.

## What survives a shutdown

Everything either notebook creates lives under `/content` and is deleted when the VM stops — Colab recycles idle free-tier VMs. Your Drive copy keeps the notebook and its saved cell outputs; the DuckDB files (`/content/mini.duckdb`, the ingest warehouse) are always gone, so the intended loop is simply re-running from the top.

`dbt_colab.ipynb` was dry-run on the maintainer's machine against the exact pins above but has not yet been executed end-to-end inside Colab itself; if the install cell reports a conflict with a preinstalled Colab package, restart the runtime once (`Runtime → Restart session`) and re-run that cell — the notebook's own install cell carries this instruction.

## If you want the real thing

- [docs/workshop.md](../docs/workshop.md) — the full nine-module walkthrough these notebooks are excerpted from; each module's classic commands are the local twin of what you just ran in Colab.
- Local setup in three commands:

  ```bash
  git clone https://github.com/william-dwe/crypto-tracker
  uv sync
  uv run ct setup
  ```

  This is the only place `uv` appears in this document, and it is optional — the Colab path above needs nothing but a browser.
- [docs/ARCHITECTURE.md](../docs/ARCHITECTURE.md) — why the full repo looks the way it does: 18 dbt models, 75 declared tests, the forward-filled FX spine, and the other constraints the miniature deliberately skips.
