"""
setup_bigquery.py
Creates BigQuery dataset and tables for inventory forecasting
"""

from google.cloud import bigquery
from google.oauth2 import service_account
import os
from dotenv import load_dotenv

load_dotenv()

# Initialize BigQuery client
credentials = service_account.Credentials.from_service_account_file(
    os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
)
client = bigquery.Client(
    credentials=credentials,
    project=os.getenv('GOOGLE_CLOUD_PROJECT')
)

dataset_id = f"{os.getenv('GOOGLE_CLOUD_PROJECT')}.{os.getenv('BIGQUERY_DATASET')}"

def create_dataset():
    """Create BigQuery dataset"""
    dataset = bigquery.Dataset(dataset_id)
    dataset.location = "US"
    dataset = client.create_dataset(dataset, exists_ok=True)
    print(f"✓ Dataset {dataset_id} created")

def create_tables():
    """Create all required tables"""

    # Products table
    products_schema = [
        bigquery.SchemaField("product_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("title", "STRING"),
        bigquery.SchemaField("vendor", "STRING"),
        bigquery.SchemaField("status", "STRING"),
        bigquery.SchemaField("created_at", "TIMESTAMP"),
        bigquery.SchemaField("updated_at", "TIMESTAMP"),
    ]
    create_table("products", products_schema)

    # Variants table
    variants_schema = [
        bigquery.SchemaField("variant_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("product_id", "STRING"),
        bigquery.SchemaField("sku", "STRING"),
        bigquery.SchemaField("title", "STRING"),
        bigquery.SchemaField("price", "FLOAT64"),
        bigquery.SchemaField("inventory_item_id", "STRING"),
    ]
    create_table("variants", variants_schema)

    # Locations table
    locations_schema = [
        bigquery.SchemaField("location_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("name", "STRING"),
        bigquery.SchemaField("active", "BOOLEAN"),
        bigquery.SchemaField("location_gid", "STRING"),
    ]
    create_table("locations", locations_schema)

    # Inventory snapshots table (partitioned by date)
    inventory_schema = [
        bigquery.SchemaField("snapshot_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("variant_id", "STRING"),
        bigquery.SchemaField("sku", "STRING"),
        bigquery.SchemaField("location_id", "STRING"),
        bigquery.SchemaField("available_qty", "INT64"),
        bigquery.SchemaField("incoming_qty", "INT64"),
        bigquery.SchemaField("committed_qty", "INT64"),
        bigquery.SchemaField("snapshot_date", "DATE", mode="REQUIRED"),
        bigquery.SchemaField("snapshot_timestamp", "TIMESTAMP"),
    ]
    create_table("inventory_snapshots", inventory_schema, partition_field="snapshot_date")

    # Sales history table (partitioned by date)
    sales_schema = [
        bigquery.SchemaField("sale_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("order_id", "STRING"),
        bigquery.SchemaField("order_name", "STRING"),
        bigquery.SchemaField("variant_id", "STRING"),
        bigquery.SchemaField("sku", "STRING"),
        bigquery.SchemaField("product_title", "STRING"),
        bigquery.SchemaField("quantity_sold", "INT64"),
        bigquery.SchemaField("sale_date", "DATE", mode="REQUIRED"),
        bigquery.SchemaField("sale_timestamp", "TIMESTAMP"),
        bigquery.SchemaField("vendor", "STRING"),
    ]
    create_table("sales_history", sales_schema, partition_field="sale_date")

    # Vendors table
    vendors_schema = [
        bigquery.SchemaField("vendor_id", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("vendor_name", "STRING"),
        bigquery.SchemaField("lead_time_days", "INT64"),
        bigquery.SchemaField("moq", "INT64"),
        bigquery.SchemaField("pack_size", "INT64"),
    ]
    create_table("vendors", vendors_schema)

    print("\n✓ All tables created successfully!")

def create_table(table_name, schema, partition_field=None):
    """Helper function to create a table"""
    table_id = f"{dataset_id}.{table_name}"
    table = bigquery.Table(table_id, schema=schema)

    # Add partitioning if specified
    if partition_field:
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field=partition_field
        )

    table = client.create_table(table, exists_ok=True)
    print(f"  ✓ Table {table_name} created")

if __name__ == "__main__":
    print("Setting up BigQuery dataset and tables...\n")
    create_dataset()
    create_tables()
    print("\n✅ Setup complete! Ready to sync Shopify data.")
