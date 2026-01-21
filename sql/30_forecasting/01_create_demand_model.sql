-- Create ARIMA model
CREATE OR REPLACE MODEL `fiesta-inventory-forecast.fiesta_inventory.demand_arima_model`
OPTIONS(
  model_type='ARIMA_PLUS',
  time_series_timestamp_col='sale_date',
  time_series_data_col='qty_sold',
  time_series_id_col='sku',
  holiday_region='US',
  auto_arima=TRUE,
  data_frequency='AUTO_FREQUENCY'
) AS
WITH sku_vendor AS (
  SELECT
    v.sku,
    p.vendor AS vendor_name
  FROM `fiesta-inventory-forecast.fiesta_inventory.variants` v
  JOIN `fiesta-inventory-forecast.fiesta_inventory.products` p
    ON v.product_id = p.product_id
  WHERE v.sku IS NOT NULL AND v.sku != ''
)
SELECT
  sd.sale_date,
  sd.sku,
  sd.qty_sold
FROM `fiesta-inventory-forecast.fiesta_inventory.sales_daily` sd
JOIN sku_vendor sv
  ON sv.sku = sd.sku
WHERE sv.vendor_name <> 'Fiesta Carnival'
  AND sd.sale_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 365 DAY)
  AND sd.qty_sold > 0;