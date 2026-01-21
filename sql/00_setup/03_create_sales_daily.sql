CREATE OR REPLACE TABLE `fiesta-inventory-forecast.fiesta_inventory.sales_daily`
PARTITION BY sale_date
CLUSTER BY sku AS
SELECT
  sale_date,
  sku,
  SUM(quantity_sold) AS qty_sold
FROM `fiesta-inventory-forecast.fiesta_inventory.sales_history_raw`
WHERE sale_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 365 DAY)
  AND sku IS NOT NULL AND sku != ''
GROUP BY sale_date, sku;

ALTER TABLE `fiesta-inventory-forecast.fiesta_inventory.sales_daily`
SET OPTIONS (require_partition_filter = TRUE);

ALTER TABLE `fiesta-inventory-forecast.fiesta_inventory.sales_daily`
SET OPTIONS (partition_expiration_days = 365);