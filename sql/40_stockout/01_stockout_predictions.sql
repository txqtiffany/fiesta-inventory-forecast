CREATE OR REPLACE TABLE `fiesta-inventory-forecast.fiesta_inventory.stockout_predictions` AS
WITH daily_forecast AS (
  SELECT sku, forecast_date, predicted_qty
  FROM `fiesta-inventory-forecast.fiesta_inventory.demand_forecasts`
),
cum_calc AS (
  SELECT
    f.sku,
    f.forecast_date,
    i.current_stock,
    SUM(f.predicted_qty) OVER (
      PARTITION BY f.sku
      ORDER BY f.forecast_date
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cum_demand
  FROM daily_forecast f
  JOIN `fiesta-inventory-forecast.fiesta_inventory.current_inventory` i
    ON f.sku = i.sku
  WHERE i.current_stock > 0
)
SELECT
  sku,
  current_stock,
  MIN(forecast_date) AS stockout_date,
  DATE_DIFF(MIN(forecast_date), CURRENT_DATE(), DAY) AS days_remaining,
  CURRENT_TIMESTAMP() AS created_at
FROM cum_calc
WHERE cum_demand >= current_stock
GROUP BY sku, current_stock;