
-- Generate 60-day forecasts
CREATE OR REPLACE TABLE `fiesta-inventory-forecast.fiesta_inventory.demand_forecasts` AS
SELECT
  sku,
  CAST(forecast_timestamp AS DATE) AS forecast_date,
  CAST(forecast_value AS INT64) AS predicted_qty,
  CAST(prediction_interval_lower_bound AS INT64) AS confidence_lower,
  CAST(prediction_interval_upper_bound AS INT64) AS confidence_upper,
  CURRENT_TIMESTAMP() AS created_at
FROM ML.FORECAST(
  MODEL `fiesta-inventory-forecast.fiesta_inventory.demand_arima_model`,
  STRUCT(60 AS horizon, 0.95 AS confidence_level)
)
WHERE forecast_value > 0;

