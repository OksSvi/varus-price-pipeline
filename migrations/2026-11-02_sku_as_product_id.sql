-- Migration: product_id = sku (brand removed from key)
-- Run AFTER merging this PR, BEFORE the next scheduled pipeline run.

CREATE OR REPLACE TABLE gold_db.dim_products (
    product_id STRING,
    product_name STRING,
    brand STRING,
    category_path STRING,
    categories ARRAY<STRING>,
    product_url STRING,
    first_seen_date TIMESTAMP,
    last_seen_date TIMESTAMP
)
USING DELTA
LOCATION 's3a://aws-scraping-test-oks/gold/dim_products/';

TRUNCATE TABLE gold_db.fct_prices;

-- Then run 03_silver_gold to repopulate.
