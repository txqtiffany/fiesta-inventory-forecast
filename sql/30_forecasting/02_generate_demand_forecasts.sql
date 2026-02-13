
-- Generate 60-day forecasts
CREATE OR REPLACE TABLE `fiesta-inventory-forecast.fiesta_inventory.demand_forecasts` AS
WITH f AS (
  SELECT
    variant_id,
    CAST(forecast_timestamp AS DATE) AS forecast_date,
    GREATEST(CAST(forecast_value AS INT64), 0) AS predicted_qty,
    GREATEST(CAST(prediction_interval_lower_bound AS INT64), 0) AS confidence_lower,
    GREATEST(CAST(prediction_interval_upper_bound AS INT64), 0) AS confidence_upper,
    CURRENT_TIMESTAMP() AS created_at
  FROM ML.FORECAST(
    MODEL `fiesta-inventory-forecast.fiesta_inventory.demand_arima_model`,
    STRUCT(60 AS horizon, 0.95 AS confidence_level)
  )
  WHERE forecast_value > 0
)
SELECT
  f.variant_id,
  v.sku,
  f.forecast_date,
  f.predicted_qty,
  f.confidence_lower,
  f.confidence_upper,
  f.created_at
FROM f
LEFT JOIN `fiesta-inventory-forecast.fiesta_inventory.variants` v
  ON v.variant_id = f.variant_id;

