-- Create ARIMA model (VARIANT-level)
-- NOTE: variant_id is the unique identifier; sku is optional/non-unique.
CREATE OR REPLACE MODEL `fiesta-inventory-forecast.fiesta_inventory.demand_arima_model`
OPTIONS(
  model_type='ARIMA_PLUS',
  time_series_timestamp_col='sale_date',
  time_series_data_col='qty_sold',
  time_series_id_col='variant_id',
  holiday_region='US',
  auto_arima=TRUE,
  data_frequency='AUTO_FREQUENCY'
) AS
WITH variant_vendor AS (
  SELECT
    v.variant_id,
    p.vendor AS vendor_name
  FROM `fiesta-inventory-forecast.fiesta_inventory.variants` v
  JOIN `fiesta-inventory-forecast.fiesta_inventory.products` p
    ON v.product_id = p.product_id
  WHERE v.variant_id IS NOT NULL
)
SELECT
  sd.sale_date,
  sd.variant_id,
  sd.qty_sold
FROM `fiesta-inventory-forecast.fiesta_inventory.sales_daily` sd
JOIN variant_vendor vv
  ON vv.variant_id = sd.variant_id
WHERE vv.vendor_name <> 'Fiesta Carnival'
  AND sd.sale_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 365 DAY)
  AND sd.qty_sold > 0;