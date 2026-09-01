# Colab notebook

A zero-local-setup twin of the workshop's extract/load and transform modules, for participants who want the dlt and dbt experience without cloning the repo, creating a venv, or holding any API key. Everything runs on Colab's free tier and disappears with the VM.

## The notebook

| Notebook | What you build | Pinned stack | Slowest cell |
| --- | --- | --- | --- |
| [workshop.ipynb](https://colab.research.google.com/github/william-dwe/crypto-tracker/blob/main/notebooks/workshop.ipynb) | CoinGecko prices and Frankfurter FX rates landed into a DuckDB `bronze` schema by dlt, then transformed by the repo's real dbt project into the silver and gold medallion — 18 models, 1 seed, 80 tests | `dlt[duckdb]==1.30.0`, `dbt-core==1.12.3`, `dbt-duckdb==1.11.0`, `duckdb==1.5.5` | the first ingest, about 4 minutes (CoinGecko's keyless rate limit, not slow code) |

Both halves share one warehouse: the notebook clones the repo, dlt lands `bronze` in the clone's `data/crypto.duckdb`, and dbt transforms that same file. There is nothing to copy between steps.

## Copy it into your own Colab

1. Click the notebook link above. It opens Colab with the copy that lives on GitHub.
2. Click **Copy to Drive** in the Colab toolbar (equivalently `File → Save a copy in Drive`). A new browser tab opens holding your own copy. **Why:** the GitHub-backed view is read-only, so without this step your edits and cell outputs have nowhere to be saved — Colab will refuse to save and you lose everything on the first disconnect.
3. Sign in with any Google account if prompted. There is nothing further to configure: no API keys, no secrets, no Drive mounting, no billing — CoinGecko's free tier and Frankfurter's ECB endpoint are both keyless.
4. Leave the runtime on its default. `Runtime → Change runtime type` should stay on CPU; nothing here uses a GPU or TPU, and requesting one only slows down VM allocation.
5. Run the cells from top to bottom in order, waiting for each to finish before starting the next. **Why:** the first cell installs the pinned packages and clones the repo that every later cell imports, and the cells form a chain — dlt has to land `bronze` before dbt has anything to transform. Running out of order fails with `ModuleNotFoundError` or a dbt compilation error rather than doing something subtly wrong.

`Runtime → Run all` works end to end; use it once you have read the cells, not before, since the point of the workshop is reading them.

## What survives a shutdown

Everything the notebook creates lives under `/content` (the clone and its `data/crypto.duckdb`) and is deleted when the VM stops — Colab recycles idle free-tier VMs. Your Drive copy keeps the notebook and its saved cell outputs; the warehouse is always gone, so the intended loop is simply re-running from the top.

## If you want the real thing

- [docs/workshop.md](../docs/workshop.md) — the full nine-module walkthrough these notebooks are excerpted from; each module's classic commands are the local twin of what you just ran in Colab.
- Local setup in three commands:

  ```bash
  git clone https://github.com/william-dwe/crypto-tracker
  uv sync
  uv run ct setup
  ```

  This is the only place `uv` appears in this document, and it is optional — the Colab path above needs nothing but a browser.
- [docs/ARCHITECTURE.md](../docs/ARCHITECTURE.md) — why the full repo looks the way it does: the medallion layers, the trust boundary, the star schema, the forward-filled FX spine, and every other hard-won constraint behind the models you just built.
