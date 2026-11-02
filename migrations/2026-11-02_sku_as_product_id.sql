-- Databricks notebook source
-- Migration: product_id = sku (brand removed from key)
-- Run before the first pipeline run on the new code.

-- COMMAND ----------

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

-- COMMAND ----------

TRUNCATE TABLE gold_db.fct_prices;

-- COMMAND ----------

-- Then run 03_silver_gold to repopulate.
