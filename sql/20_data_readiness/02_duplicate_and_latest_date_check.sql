DECLARE min_d DATE DEFAULT (
  SELECT MIN(sale_date) FROM `fiesta-inventory-forecast.fiesta_inventory.sales_history_stg`
);
DECLARE max_d DATE DEFAULT (
  SELECT MAX(sale_date) FROM `fiesta-inventory-forecast.fiesta_inventory.sales_history_stg`
);
-- duplicates should be 0
SELECT COUNT(*) - COUNT(DISTINCT sale_id) AS dup_sales
FROM `fiesta-inventory-forecast.fiesta_inventory.sales_history_raw`
WHERE sale_date BETWEEN min_d AND max_d;

-- latest inventory date
SELECT MAX(PARSE_DATE('%Y%m%d', partition_id)) AS latest_snapshot_date
FROM `fiesta-inventory-forecast.fiesta_inventory.INFORMATION_SCHEMA.PARTITIONS`
WHERE table_name = 'inventory_snapshots_raw'
  AND partition_id NOT IN ('__NULL__', '__UNPARTITIONED__');

-- latest sales date
SELECT MAX(PARSE_DATE('%Y%m%d', partition_id)) AS latest_sale_date
FROM `fiesta-inventory-forecast.fiesta_inventory.INFORMATION_SCHEMA.PARTITIONS`
WHERE table_name = 'sales_history_raw'
  AND partition_id NOT IN ('__NULL__', '__UNPARTITIONED__');

-- check sales_daily
SELECT MAX(sale_date) AS sales_daily_latest
FROM `fiesta-inventory-forecast.fiesta_inventory.sales_daily`
WHERE sale_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY);

-- check current_inventory
SELECT
  COUNT(*) AS sku_count,
  SUM(current_stock) AS total_stock
FROM `fiesta-inventory-forecast.fiesta_inventory.current_inventory`;