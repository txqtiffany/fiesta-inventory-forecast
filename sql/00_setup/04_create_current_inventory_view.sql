CREATE OR REPLACE VIEW `fiesta-inventory-forecast.fiesta_inventory.current_inventory` AS
WITH latest_snapshot_date AS (
  SELECT MAX(snapshot_date) AS snapshot_date
  FROM `fiesta-inventory-forecast.fiesta_inventory.inventory_snapshots_raw`
  WHERE snapshot_date IS NOT NULL
),
inv AS (
  SELECT
    inv.snapshot_date,
    inv.variant_id,
    ANY_VALUE(NULLIF(inv.sku, '')) AS sku,
    SUM(inv.available_qty) AS raw_stock
  FROM `fiesta-inventory-forecast.fiesta_inventory.inventory_snapshots_raw` inv
  JOIN latest_snapshot_date ls
    ON inv.snapshot_date = ls.snapshot_date
  GROUP BY inv.snapshot_date, inv.variant_id
)
SELECT
  snapshot_date,
  variant_id,
  sku,
  raw_stock,
  GREATEST(raw_stock, 0) AS current_stock,
  raw_stock < 0 AS negative_stock_flag
FROM inv;
