CREATE OR REPLACE TABLE `fiesta-inventory-forecast.fiesta_inventory.sales_daily`
PARTITION BY sale_date
CLUSTER BY variant_id AS
SELECT
  s.sale_date,
  s.variant_id,
  ANY_VALUE(NULLIF(s.sku, '')) AS sku,
  SUM(s.quantity_sold) AS qty_sold,
  -- gross revenue uses variants.price as proxy
  SUM(
    s.quantity_sold * COALESCE(SAFE_CAST(v.price AS NUMERIC), 0)
  ) AS gross_revenue

FROM `fiesta-inventory-forecast.fiesta_inventory.sales_history_raw` s
LEFT JOIN `fiesta-inventory-forecast.fiesta_inventory.variants` v
  ON v.variant_id = s.variant_id
WHERE s.sale_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 365 DAY)
  AND s.variant_id IS NOT NULL
GROUP BY
  s.sale_date,
  s.variant_id;

ALTER TABLE `fiesta-inventory-forecast.fiesta_inventory.sales_daily`
SET OPTIONS (require_partition_filter = TRUE);

ALTER TABLE `fiesta-inventory-forecast.fiesta_inventory.sales_daily`
SET OPTIONS (partition_expiration_days = 365);