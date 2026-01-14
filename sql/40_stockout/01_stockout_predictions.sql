-- Stockout predictions (partition-safe inventory)
DECLARE latest_snapshot_date DATE;

SET latest_snapshot_date = (
  SELECT MAX(PARSE_DATE('%Y%m%d', partition_id))
  FROM `fiesta-inventory-forecast.fiesta_inventory.INFORMATION_SCHEMA.PARTITIONS`
  WHERE table_name = 'inventory_snapshots'
    AND partition_id NOT IN ('__NULL__', '__UNPARTITIONED__')
);

CREATE OR REPLACE TABLE `fiesta-inventory-forecast.fiesta_inventory.stockout_predictions` AS
WITH current_inventory AS (
  SELECT
    sku,
    SUM(available_qty) AS current_stock
  FROM `fiesta-inventory-forecast.fiesta_inventory.inventory_snapshots`
  WHERE snapshot_date = latest_snapshot_date      -- ✅ partition filter
  GROUP BY sku
),
daily_forecast AS (
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
  JOIN current_inventory i USING (sku)
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