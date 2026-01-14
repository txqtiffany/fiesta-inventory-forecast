# Fiesta Inventory Forecast

An end-to-end inventory analytics + restocking recommendation pipeline for a Shopify-based retail business (Fiesta Carnival). This project automates daily data ingestion from Shopify into BigQuery, builds clean fact/dimension tables for sales and inventory, and generates vendor-level restock recommendations designed for weekly and bi-weekly purchasing cycles.

## What it does
- **Daily Shopify sync (GraphQL API):** pulls products, variants/SKUs, orders (line items), and inventory snapshots.
- **BigQuery warehouse layer:** loads raw/staging data and merges into partitioned tables for cost-efficient querying.
- **Forecast-ready outputs:** produces SKU-level demand inputs for BigQuery ML and downstream reporting.
- **Restock recommendations:** generates vendor/SKU restock lists using configurable defaults (lead time, MOQ, pack size) and supports excluding vendors/items via an **Archived** flag.
- **Dashboard-ready tables:** outputs tables that can be connected directly to Looker Studio for alerts and reporting.

## Architecture (high level)
Shopify → Python sync → BigQuery (staging + partitioned tables) → Forecasting/Restock SQL → Looker Studio

## Key tables
- `products` / `variants` / `locations` (dimensions)
- `sales_history` (partitioned by sale date)
- `inventory_snapshots` (partitioned by snapshot date)
- `vendors` + optional vendor status/cadence tables
- forecast + restock output tables (e.g., `demand_forecasts`, `stockout_predictions`, `vendor_restocks_*`)

## Why this approach
- **Cost-saving:** partitioned fact tables + required partition filters reduce scan costs.
- **Operational:** designed around real ordering cadence (weekly/bi-weekly), lead times, and minimum order constraints.
- **Maintainable:** ingestion, loading, and analytics are separated into clear steps and scripts.

## Repo layout
- `shopify_sync.py` — extracts Shopify data (GraphQL) and writes local JSON outputs
- `load_to_bigquery.py` — loads data to BigQuery staging and merges into partitioned tables
- `/sql/` — forecasting + restock SQL (BigQuery ML + recommendation queries)
- `.env.example` — environment variable template (no secrets)

> **Security note:** This repo intentionally excludes `.env`, `credentials.json`, and any data exports. Use `.env.example` to create your local `.env`.
