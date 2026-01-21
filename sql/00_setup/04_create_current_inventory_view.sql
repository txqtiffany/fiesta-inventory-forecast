CREATE OR REPLACE VIEW `fiesta-inventory-forecast.fiesta_inventory.current_inventory` AS
WITH latest AS (
  SELECT MAX(snapshot_date) AS latest_snapshot_date
  FROM `fiesta-inventory-forecast.fiesta_inventory.inventory_snapshots_raw`
  WHERE snapshot_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
)
SELECT
  inv.sku,
  GREATEST(SUM(inv.available_qty), 0) AS current_stock,
  SUM(inv.available_qty) AS raw_stock,
  (SUM(inv.available_qty) < 0) AS negative_stock_flag,
  (SELECT latest_snapshot_date FROM latest) AS snapshot_date
FROM `fiesta-inventory-forecast.fiesta_inventory.inventory_snapshots_raw` inv
WHERE inv.snapshot_date = (SELECT latest_snapshot_date FROM latest)
GROUP BY inv.sku;