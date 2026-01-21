-- Create ARIMA model
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
SELECT
  sale_date,
  sku,
  qty_sold
FROM `fiesta-inventory-forecast.fiesta_inventory.sales_daily`
WHERE sale_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 365 DAY)
  AND qty_sold > 0;
