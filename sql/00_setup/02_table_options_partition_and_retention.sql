-- Require partition filters (prevents accidental expensive scans)
ALTER TABLE `fiesta-inventory-forecast.fiesta_inventory.sales_history`
SET OPTIONS (require_partition_filter = TRUE);

ALTER TABLE `fiesta-inventory-forecast.fiesta_inventory.inventory_snapshots`
SET OPTIONS (require_partition_filter = TRUE);

-- Set Inventory snapshot retention
ALTER TABLE `fiesta-inventory-forecast.fiesta_inventory.inventory_snapshots`
SET OPTIONS (partition_expiration_days = 365);