-- Require partition filters (prevents accidental expensive scans)
ALTER TABLE `fiesta-inventory-forecast.fiesta_inventory.sales_history_raw`
SET OPTIONS (require_partition_filter = TRUE);

ALTER TABLE `fiesta-inventory-forecast.fiesta_inventory.inventory_snapshots_raw`
SET OPTIONS (require_partition_filter = TRUE);

-- Set retention time
ALTER TABLE `fiesta-inventory-forecast.fiesta_inventory.sales_history_raw`
SET OPTIONS (
  partition_expiration_days = 365
);

ALTER TABLE `fiesta-inventory-forecast.fiesta_inventory.inventory_snapshots_raw`
SET OPTIONS (
  partition_expiration_days = 365
);