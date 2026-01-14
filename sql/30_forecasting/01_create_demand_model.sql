-- Create ARIMA model
CREATE OR REPLACE MODEL `fiesta-inventory-forecast.fiesta_inventory.demand_arima_model`
OPTIONS(
  model_type='ARIMA_PLUS',
  time_series_timestamp_col='sale_date',
  time_series_data_col='quantity_sold',
  time_series_id_col='sku',
  holiday_region='US',
  auto_arima=TRUE,
  data_frequency='AUTO_FREQUENCY',
  decompose_time_series=TRUE
) AS
WITH daily_sales AS (
  SELECT
    sale_date,
    sku,
    SUM(quantity_sold) AS quantity_sold
  FROM `fiesta-inventory-forecast.fiesta_inventory.sales_history`
  WHERE sale_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 365 DAY)   -- ✅ partition filter
    AND sku IS NOT NULL AND sku != ''
  GROUP BY sale_date, sku
)
SELECT
  sale_date,
  sku,
  quantity_sold
FROM daily_sales
WHERE quantity_sold > 0;
