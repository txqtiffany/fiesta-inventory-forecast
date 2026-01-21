DECLARE latest_snapshot_date DATE;

SET latest_snapshot_date = (
  SELECT MAX(PARSE_DATE('%Y%m%d', partition_id))
  FROM `fiesta-inventory-forecast.fiesta_inventory.INFORMATION_SCHEMA.PARTITIONS`
  WHERE table_name = 'inventory_snapshots_raw'
    AND partition_id NOT IN ('__NULL__', '__UNPARTITIONED__')
);

CREATE OR REPLACE TABLE `fiesta-inventory-forecast.fiesta_inventory.negative_stock_skus` AS
WITH sku_vendor AS (
  SELECT
    v.sku,
    p.vendor AS vendor_name,
    p.title AS product_title,
    v.title AS variant_title
  FROM `fiesta-inventory-forecast.fiesta_inventory.variants` v
  JOIN `fiesta-inventory-forecast.fiesta_inventory.products` p
    ON v.product_id = p.product_id
  WHERE v.sku IS NOT NULL AND v.sku != ''
    AND p.vendor IS NOT NULL AND p.vendor != ''
),
neg AS (
  SELECT
    inv.sku,
    SUM(inv.available_qty) AS raw_stock
  FROM `fiesta-inventory-forecast.fiesta_inventory.inventory_snapshots_raw` inv
  WHERE inv.snapshot_date = latest_snapshot_date
  GROUP BY inv.sku
  HAVING raw_stock < 0
)
SELECT
  latest_snapshot_date AS snapshot_date,
  sv.vendor_name,
  sv.sku,
  sv.product_title,
  sv.variant_title,
  n.raw_stock,
  CURRENT_TIMESTAMP() AS created_at
FROM neg n
JOIN sku_vendor sv USING (sku)
WHERE sv.vendor_name <> 'Fiesta Carnival'   -- Exclude Internal Products
ORDER BY sv.vendor_name, n.raw_stock ASC;