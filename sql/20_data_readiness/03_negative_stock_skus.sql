DECLARE latest_snapshot_date DATE;

SET latest_snapshot_date = (
  SELECT MAX(snapshot_date)
  FROM `fiesta-inventory-forecast.fiesta_inventory.inventory_snapshots_raw`
  WHERE snapshot_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
);

CREATE OR REPLACE TABLE `fiesta-inventory-forecast.fiesta_inventory.negative_stock_skus` AS
WITH variant_vendor AS (
  SELECT
    v.variant_id,
    ANY_VALUE(NULLIF(v.sku, '')) AS sku,          -- label only
    p.vendor AS vendor_name,
    p.title AS product_title,
    v.title AS variant_title
  FROM `fiesta-inventory-forecast.fiesta_inventory.variants` v
  JOIN `fiesta-inventory-forecast.fiesta_inventory.products` p
    ON v.product_id = p.product_id
  WHERE v.variant_id IS NOT NULL
    AND p.vendor IS NOT NULL AND p.vendor != ''
    AND p.vendor <> 'Fiesta Carnival'             -- exclude internal vendor
  GROUP BY v.variant_id, p.vendor, p.title, v.title
),
neg AS (
  SELECT
    inv.variant_id,
    ANY_VALUE(NULLIF(inv.sku, '')) AS sku,        -- label only
    SUM(inv.available_qty) AS raw_stock
  FROM `fiesta-inventory-forecast.fiesta_inventory.inventory_snapshots_raw` inv
  WHERE inv.snapshot_date = latest_snapshot_date
    AND inv.variant_id IS NOT NULL
  GROUP BY inv.variant_id
  HAVING raw_stock < 0
)
SELECT
  latest_snapshot_date AS snapshot_date,
  vv.vendor_name,
  n.variant_id,
  COALESCE(vv.sku, n.sku) AS sku,
  vv.product_title,
  vv.variant_title,
  n.raw_stock,
  CURRENT_TIMESTAMP() AS created_at
FROM neg n
JOIN variant_vendor vv
  ON vv.variant_id = n.variant_id
ORDER BY
  vv.vendor_name,
  n.raw_stock ASC;