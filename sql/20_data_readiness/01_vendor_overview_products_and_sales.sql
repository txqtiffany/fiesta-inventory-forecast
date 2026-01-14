SELECT table_name
FROM `fiesta-inventory-forecast.fiesta_inventory.INFORMATION_SCHEMA.TABLES`
WHERE table_name IN ('inventory_snapshots_stg', 'sales_history_stg');

CREATE TABLE IF NOT EXISTS `fiesta-inventory-forecast.fiesta_inventory.inventory_snapshots_stg` (
  snapshot_id STRING NOT NULL,
  variant_id STRING,
  sku STRING,
  location_id STRING,
  available_qty INT64,
  incoming_qty INT64,
  committed_qty INT64,
  snapshot_date DATE NOT NULL,
  snapshot_timestamp TIMESTAMP
);

CREATE TABLE IF NOT EXISTS `fiesta-inventory-forecast.fiesta_inventory.sales_history_stg` (
  sale_id STRING NOT NULL,
  order_id STRING,
  order_name STRING,
  variant_id STRING,
  sku STRING,
  product_title STRING,
  quantity_sold INT64,
  sale_date DATE NOT NULL,
  sale_timestamp TIMESTAMP,
  vendor STRING
);
