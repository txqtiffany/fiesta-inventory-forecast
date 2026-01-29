"""
load_to_bigquery.py
Loads synced Shopify data (sync_data.json) into BigQuery.

Pattern:
- products/variants/locations = full refresh (WRITE_TRUNCATE)
- inventory & sales = load into *_stg (WRITE_TRUNCATE), then MERGE into *_raw backup tables

Why *_raw?
- Acts as daily backup/history (append/dedupe by ID)
- Your analytics tables can be derived/aggregated from raw for cost efficiency

Requires:
  pip install google-cloud-bigquery google-auth python-dotenv
"""

import os
import json
from typing import Any, Dict, List
from dotenv import load_dotenv
from google.cloud import bigquery
from google.oauth2 import service_account

load_dotenv()


def must_getenv(name: str) -> str:
    v = os.getenv(name)
    if not v:
        raise ValueError(f"Missing required env var: {name}")
    return v


# ---- Env ----
PROJECT_ID = must_getenv("GOOGLE_CLOUD_PROJECT")
DATASET_NAME = must_getenv("BIGQUERY_DATASET")
CREDS_PATH = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")  # optional: local only

DATASET_ID = f"{PROJECT_ID}.{DATASET_NAME}"

# ---- Client ----
if CREDS_PATH:
    # Local dev: use a downloaded service account key JSON
    credentials = service_account.Credentials.from_service_account_file(CREDS_PATH)
    client = bigquery.Client(credentials=credentials, project=PROJECT_ID)
else:
    # Cloud Run / GCE / GKE: use Application Default Credentials (service account identity)
    client = bigquery.Client(project=PROJECT_ID)


def load_data_to_table(
    table_name: str,
    data: List[Dict[str, Any]],
    write_disposition: str = "WRITE_TRUNCATE",
) -> None:
    """Load JSON records into a BigQuery table."""
    if not data:
        print(f"  ⚠️  No data to load for {table_name}")
        return

    table_id = f"{DATASET_ID}.{table_name}"

    job_config = bigquery.LoadJobConfig(
        write_disposition=write_disposition,
        autodetect=False,  # schema exists already
    )

    job = client.load_table_from_json(data, table_id, job_config=job_config)
    job.result()

    table = client.get_table(table_id)
    print(f"  ✓ Loaded {table.num_rows} rows into {table_name}")


def run_sql(sql: str) -> None:
    job = client.query(sql)
    job.result()


def truncate_table(table_name: str) -> None:
    """TRUNCATE a table so staging doesn't keep old rows on days with no data."""
    run_sql(f"TRUNCATE TABLE `{DATASET_ID}.{table_name}`")


def ensure_backup_tables_exist() -> None:
    """
    Ensure *_raw backup tables + staging tables exist.
    Does NOT depend on analytics tables (sales_history / inventory_snapshots).
    """
    # 1) sales_history_raw (partitioned)
    run_sql(f"""
    CREATE TABLE IF NOT EXISTS `{DATASET_ID}.sales_history_raw` (
      sale_id STRING NOT NULL,
      order_id STRING,
      order_name STRING,
      variant_id STRING,
      sku STRING,
      product_title STRING,
      quantity_sold INT64,
      sale_date DATE NOT NULL,
      sale_timestamp TIMESTAMP,
      vendor STRING,
      line_price NUMERIC
    )
    PARTITION BY sale_date
    CLUSTER BY sku;
    """)

    # 2) inventory_snapshots_raw (partitioned)
    run_sql(f"""
    CREATE TABLE IF NOT EXISTS `{DATASET_ID}.inventory_snapshots_raw` (
      snapshot_id STRING NOT NULL,
      variant_id STRING,
      sku STRING,
      location_id STRING,
      available_qty INT64,
      incoming_qty INT64,
      committed_qty INT64,
      snapshot_date DATE NOT NULL,
      snapshot_timestamp TIMESTAMP
    )
    PARTITION BY snapshot_date
    CLUSTER BY sku;
    """)

    # 3) staging tables (non-partitioned is fine; truncated each run)
    run_sql(f"""
    CREATE TABLE IF NOT EXISTS `{DATASET_ID}.sales_history_stg` (
      sale_id STRING,
      order_id STRING,
      order_name STRING,
      variant_id STRING,
      sku STRING,
      product_title STRING,
      quantity_sold INT64,
      sale_date DATE,
      sale_timestamp TIMESTAMP,
      vendor STRING,
      line_price NUMERIC
    );
    """)

    run_sql(f"""
    CREATE TABLE IF NOT EXISTS `{DATASET_ID}.inventory_snapshots_stg` (
      snapshot_id STRING,
      variant_id STRING,
      sku STRING,
      location_id STRING,
      available_qty INT64,
      incoming_qty INT64,
      committed_qty INT64,
      snapshot_date DATE,
      snapshot_timestamp TIMESTAMP
    );
    """)


def main():
    print("=" * 60)
    print("LOADING DATA TO BIGQUERY (dimensions + staging + merge into *_raw backups)")
    print("=" * 60)

    sync_path = os.getenv("SYNC_DATA_PATH", "sync_data.json")
    with open(sync_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    # 0) Ensure raw backup tables exist (one-time / safe to run every time)
    ensure_backup_tables_exist()

    # 1) Full refresh dimension-like tables
    print("\n📤 Uploading dimensions (truncate/replace)...")
    load_data_to_table("products", data.get("products", []), write_disposition="WRITE_TRUNCATE")
    load_data_to_table("variants", data.get("variants", []), write_disposition="WRITE_TRUNCATE")
    load_data_to_table("locations", data.get("locations", []), write_disposition="WRITE_TRUNCATE")

    # 2) Facts via staging then MERGE (dedupe by IDs) into *_raw backups
    print("\n📤 Uploading facts to staging (truncate staging)...")
    inventory_rows = data.get("inventory", [])
    sales_rows = data.get("sales", [])

    if inventory_rows:
        load_data_to_table("inventory_snapshots_stg", inventory_rows, write_disposition="WRITE_TRUNCATE")
    else:
        print("  ⚠️  No inventory rows in sync_data.json. Truncating inventory_snapshots_stg and skipping inventory merge.")
        truncate_table("inventory_snapshots_stg")

    if sales_rows:
        load_data_to_table("sales_history_stg", sales_rows, write_disposition="WRITE_TRUNCATE")
    else:
        print("  ⚠️  No sales rows in sync_data.json. Truncating sales_history_stg and skipping sales merge.")
        truncate_table("sales_history_stg")

    print("\n🔁 Merging staging → *_raw backup tables (partition-pruned by date ranges)...")

    # Inventory merge → inventory_snapshots_raw
    if inventory_rows:
        run_sql(f"""
        DECLARE min_d DATE DEFAULT (SELECT MIN(snapshot_date) FROM `{DATASET_ID}.inventory_snapshots_stg`);
        DECLARE max_d DATE DEFAULT (SELECT MAX(snapshot_date) FROM `{DATASET_ID}.inventory_snapshots_stg`);

        MERGE `{DATASET_ID}.inventory_snapshots_raw` T
        USING `{DATASET_ID}.inventory_snapshots_stg` S
        ON T.snapshot_id = S.snapshot_id
           AND T.snapshot_date BETWEEN min_d AND max_d
        WHEN MATCHED THEN UPDATE SET
          variant_id = S.variant_id,
          sku = S.sku,
          location_id = S.location_id,
          available_qty = S.available_qty,
          incoming_qty = S.incoming_qty,
          committed_qty = S.committed_qty,
          snapshot_timestamp = S.snapshot_timestamp
        WHEN NOT MATCHED THEN
          INSERT (
            snapshot_id, variant_id, sku, location_id,
            available_qty, incoming_qty, committed_qty,
            snapshot_date, snapshot_timestamp
          )
          VALUES (
            S.snapshot_id, S.variant_id, S.sku, S.location_id,
            S.available_qty, S.incoming_qty, S.committed_qty,
            S.snapshot_date, S.snapshot_timestamp
          );
        """)

    # Sales merge → sales_history_raw
    if sales_rows:
        run_sql(f"""
        DECLARE min_d DATE DEFAULT (SELECT MIN(sale_date) FROM `{DATASET_ID}.sales_history_stg`);
        DECLARE max_d DATE DEFAULT (SELECT MAX(sale_date) FROM `{DATASET_ID}.sales_history_stg`);

        MERGE `{DATASET_ID}.sales_history_raw` T
        USING `{DATASET_ID}.sales_history_stg` S
        ON T.sale_id = S.sale_id
           AND T.sale_date BETWEEN min_d AND max_d
        WHEN MATCHED THEN UPDATE SET
          order_id = S.order_id,
          order_name = S.order_name,
          variant_id = S.variant_id,
          sku = S.sku,
          product_title = S.product_title,
          quantity_sold = S.quantity_sold,
          sale_timestamp = S.sale_timestamp,
          vendor = S.vendor
        WHEN NOT MATCHED THEN
          INSERT (
            sale_id, order_id, order_name, variant_id, sku,
            product_title, quantity_sold, sale_date, sale_timestamp, vendor
          )
          VALUES (
            S.sale_id, S.order_id, S.order_name, S.variant_id, S.sku,
            S.product_title, S.quantity_sold, S.sale_date, S.sale_timestamp, S.vendor
          );
        """)

    print("\n✅ Load + merge complete!")
    print("Next: Use *_raw tables for backups + build derived/aggregated tables for analytics.")


if __name__ == "__main__":
    main()
