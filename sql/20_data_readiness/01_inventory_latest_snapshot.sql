DECLARE latest_snapshot_date DATE;

SET latest_snapshot_date = (
  SELECT MAX(snapshot_date)
  FROM `fiesta-inventory-forecast.fiesta_inventory.inventory_snapshots_raw`
  WHERE snapshot_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 365 DAY)  -- adjust if needed
);

SELECT
  latest_snapshot_date AS latest_snapshot_date,
  COUNT(*) AS rows_on_latest_snapshot
FROM `fiesta-inventory-forecast.fiesta_inventory.inventory_snapshots_raw`
WHERE snapshot_date = latest_snapshot_date;