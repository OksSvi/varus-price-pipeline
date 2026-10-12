# Varus Price Monitoring Pipeline

A Databricks / Delta Lake ETL pipeline that scrapes product prices from https://varus.ua and tracks them over time, built on a medallion (Bronze → Silver → Gold) architecture.

## Architecture

```
00_setup_ddl            Creates all tables (run once)
        │
        ▼
01_scraping_varus        Scrapes product pages → raw JSON in AWS S3 (Bronze source)
        │
        ▼
02_bronze_silver         Cleans, casts types, deduplicates → silver_products (Delta)
        │
        ▼
03_silver_gold           Builds dim_products and fct_prices (Delta)
        │
        ├──▶ 04_visualization      Dashboard: price trends per product over time

```

**Bronze** — raw JSON scraped from product pages, stored on S3 under a date-stamped folder structure (`raw/products/date=YYYY-MM-DD/`) for organization 
and easy manual inspection.

**Silver** — `gold_db.silver_products`: cleaned and type-cast (price → `DOUBLE`, scraped_at → `TIMESTAMP`), deduplicated on `(sku, scraped_at)`.

**Gold**
- `gold_db.dim_products` — one row per product (`product_id = md5(sku + brand)`), with `first_seen_date` / `last_seen_date`.
- `gold_db.fct_prices` — price history per product, keyed by `price_id`, populated via idempotent `MERGE`.

**Storage & access** — all tables are external Delta tables on S3, accessed through a Unity Catalog External Location. No AWS credentials are stored in code; access is authorized entirely through Unity Catalog.

**Orchestration** — a scheduled Databricks Job runs the pipeline daily: scrape → bronze → silver → gold → visualization.

**Visualization** — a Lakeview Dashboard built on the Gold layer shows price history per product over time.

<img width="1510" height="944" alt="image" src="https://github.com/user-attachments/assets/20cca047-8823-420c-916c-877a7972d0ae" />


## Star Schema (Gold layer)

The Gold layer follows a simple star schema: one dimension table describing *what* a product is, and one fact table recording *price observations* over time, joined on `product_id`.

```
        gold_db.dim_products                    gold_db.fct_prices
   ┌───────────────────────────┐          ┌───────────────────────────┐
   │ product_id (PK)           │◄─────────│ product_id (FK)           │
   │ sku_code                  │          │ price_id (PK)             │
   │ product_name              │          │ scraped_date              │
   │ brand                     │          │ scraped_at                │
   │ category_path             │          │ price                     │
   │ categories                │          └───────────────────────────┘
   │ first_seen_date           │
   │ last_seen_date            │
   └───────────────────────────┘
```

- **`dim_products`** — one row per product. Slowly-changing attributes (name, brand, category) plus `first_seen_date` / `last_seen_date` to track how long a product has been observed. Rebuilt with `INSERT OVERWRITE` on every run.
- **`fct_prices`** — one row per price observation (grain: product × scrape event). Append-only in spirit, but written with `MERGE` (keyed on `price_id`) so reruns don't duplicate history.

This separation keeps product attributes (which rarely change) out of the price history table (which grows every day), and lets the dashboard join them cheaply on a single integer-like key (`product_id`) instead of repeating product metadata on every price row.

## Setup

1. **Unity Catalog**: create a Storage Credential and an External Location pointing at the target S3 bucket, and grant `READ FILES` / `WRITE FILES` to the relevant user/group. No access keys are needed after this — all notebooks read/write via `s3a://` paths authorized through the External Location.
2. **Run `00_setup_ddl`** once to create the database and all tables (`silver_products`, `dim_products`, `fct_prices`).
3. **Update `PRODUCT_URLS`** in `01_scraping_varus` with the product pages you want to track.
4. **Create a Databricks Job** with four tasks in order: `01_scraping_varus` → `02_bronze_silver` → `03_silver_gold` → `04_visualization`, each depending on the previous. Set `Maximum concurrent runs = 1` (default) to avoid two runs writing to the same tables at once.
5. **Set a schedule** (e.g. daily) on the job.
6. **Build the dashboard** (Lakeview) on top of `dim_products` / `fct_prices` for the price-trend visualization.

## Design Decisions

**DDL separated from DML.** Table creation lives entirely in `00_setup_ddl`, run once. The other notebooks only read and write data — they never create or alter table schemas. This avoids the two ETL notebooks silently redefining the same table in incompatible ways (an early version of this pipeline had `silver_products` created both as a managed Delta table via `saveAsTable` and, separately, as an external Parquet table — this split removes that entire class of bug).

**`MERGE` instead of `INSERT INTO` for `fct_prices`.** The Silver layer is fully rebuilt (`overwrite`) on every run and always contains the full scraped history, not just new rows. A plain `INSERT INTO` on every Gold run would therefore re-insert the entire price history on every single execution, multiplying duplicates with each run. `MERGE` keyed on `price_id` makes the Gold build idempotent — rerunning the pipeline any number of times converges to the same result.

**No hardcoded AWS credentials.** An early version of the scraper authenticated to S3 with a hardcoded access key and secret. Since Unity Catalog was already available, this was replaced with a Unity Catalog External Location: notebooks read/write via `s3a://` paths and Spark/`dbutils.fs` handle authorization transparently — no secrets in code.

**`s3a://` everywhere, not `s3://`.** `s3a://` is the actively maintained Hadoop-AWS connector; `s3://` is a legacy/ambiguous scheme whose behavior depends on cluster configuration. All table locations and read paths were unified on `s3a://`.

**Product URL list kept in code, not a queue table.** A control/queue table (with per-URL status and retry tracking) would give resumability and easier scaling, but was deliberately left out to keep the scraper simple and readable for a portfolio-scale project (a handful of URLs). The scraping and upload functions are structured so this could be swapped in later without changing their interfaces.

**Data quality checks before write, not after.** `02_bronze_silver` asserts `price IS NOT NULL` and that `(sku, scraped_at)` has no duplicates on the in-memory `df_silver`, before the `overwrite` write. Checking the already-written table would be too late — bad data would already have replaced the previous good state.

## Stack

PySpark · Delta Lake · Databricks Jobs · Unity Catalog · S3 · SQL · `curl_cffi` / `lxml` (scraping)
