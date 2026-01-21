-- ============================================================
-- 00_model_validation_weekly.sql
-- Weekly self-validation (backtest) for demand forecasting (WEEKLY granularity)
-- Excludes vendor_name = 'Fiesta Carnival'
--
-- Outputs (same final table for restock):
--   - sales_daily
--   - sku_vendor_map
--   - sales_weekly
--   - demand_arima_backtest_weekly
--   - backtest_forecast_4w
--   - backtest_metrics_sku_4w
--   - backtest_baseline_4w
--   - model_quality_flags
-- ============================================================

-- Train on up to ~1 year, holdout last 4 completed weeks
DECLARE holdout_weeks INT64 DEFAULT 4;
DECLARE last_complete_week_start DATE;
DECLARE cutoff_week_start DATE;
-- week_start uses Monday weeks; change WEEK(SUNDAY) if you prefer
SET last_complete_week_start = DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 1 WEEK), WEEK(MONDAY));
SET cutoff_week_start = DATE_SUB(last_complete_week_start, INTERVAL holdout_weeks WEEK);

-- ---------- 0) Build/refresh sales_daily ----------
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

-- SKU -> vendor mapping (exclude Fiesta Carnival)
CREATE OR REPLACE TABLE `fiesta-inventory-forecast.fiesta_inventory.sku_vendor_map` AS
SELECT
  v.sku,
  p.vendor AS vendor_name
FROM `fiesta-inventory-forecast.fiesta_inventory.variants` v
JOIN `fiesta-inventory-forecast.fiesta_inventory.products` p
  ON v.product_id = p.product_id
WHERE v.sku IS NOT NULL AND v.sku != ''
  AND p.vendor IS NOT NULL AND p.vendor != ''
  AND p.vendor <> 'Fiesta Carnival';

-- ---------- 0b) Build/refresh sales_weekly ----------
CREATE OR REPLACE TABLE `fiesta-inventory-forecast.fiesta_inventory.sales_weekly`
PARTITION BY week_start
CLUSTER BY sku AS
SELECT
  DATE_TRUNC(sd.sale_date, WEEK(MONDAY)) AS week_start,
  sd.sku,
  SUM(sd.qty_sold) AS qty_sold
FROM `fiesta-inventory-forecast.fiesta_inventory.sales_daily` sd
JOIN `fiesta-inventory-forecast.fiesta_inventory.sku_vendor_map` svm
  ON svm.sku = sd.sku
WHERE sd.sale_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 365 DAY)
GROUP BY week_start, sku;

-- ---------- 1) Train weekly backtest model (exclude last 4 weeks) ----------
CREATE OR REPLACE MODEL `fiesta-inventory-forecast.fiesta_inventory.demand_arima_backtest_weekly`
OPTIONS(
  model_type='ARIMA_PLUS',
  time_series_timestamp_col='week_start',
  time_series_data_col='qty_sold',
  time_series_id_col='sku',
  auto_arima=TRUE,
  data_frequency='AUTO_FREQUENCY'
) AS
SELECT
  week_start,
  sku,
  qty_sold
FROM `fiesta-inventory-forecast.fiesta_inventory.sales_weekly`
WHERE week_start >= DATE_SUB(last_complete_week_start, INTERVAL 52 WEEK)
  AND week_start < cutoff_week_start
  AND qty_sold > 0;

-- ---------- 2) Forecast the holdout (4 weeks) ----------
CREATE OR REPLACE TABLE `fiesta-inventory-forecast.fiesta_inventory.backtest_forecast_4w` AS
SELECT
  sku,
  CAST(forecast_timestamp AS DATE) AS week_start,
  GREATEST(CAST(forecast_value AS INT64), 0) AS predicted_qty
FROM ML.FORECAST(
  MODEL `fiesta-inventory-forecast.fiesta_inventory.demand_arima_backtest_weekly`,
  STRUCT(4 AS horizon)   -- ✅ must be a literal constant
);

-- ---------- 3) Metrics per SKU on holdout (WAPE + MAPE) ----------
CREATE OR REPLACE TABLE `fiesta-inventory-forecast.fiesta_inventory.backtest_metrics_sku_4w` AS
WITH actual AS (
  SELECT
    sku,
    week_start,
    qty_sold AS actual_qty
  FROM `fiesta-inventory-forecast.fiesta_inventory.sales_weekly`
  WHERE week_start >= cutoff_week_start
    AND week_start <= last_complete_week_start
),
joined AS (
  SELECT
    COALESCE(a.sku, f.sku) AS sku,
    COALESCE(a.week_start, f.week_start) AS week_start,
    COALESCE(a.actual_qty, 0) AS actual_qty,
    COALESCE(f.predicted_qty, 0) AS predicted_qty
  FROM actual a
  FULL OUTER JOIN `fiesta-inventory-forecast.fiesta_inventory.backtest_forecast_4w` f
  USING (sku, week_start)
),
agg AS (
  SELECT
    sku,
    SUM(actual_qty) AS sum_actual,
    SUM(ABS(actual_qty - predicted_qty)) AS sum_abs_error,
    AVG(
      CASE
        WHEN actual_qty = 0 THEN NULL
        ELSE ABS(actual_qty - predicted_qty) / actual_qty
      END
    ) AS mape
  FROM joined
  GROUP BY sku
)
SELECT
  sku,
  sum_actual,
  sum_abs_error,
  SAFE_DIVIDE(sum_abs_error, NULLIF(sum_actual, 0)) AS wape,
  mape,
  CURRENT_TIMESTAMP() AS created_at
FROM agg;

-- ---------- 4) Weekly baseline: "previous week same sku" ----------
CREATE OR REPLACE TABLE `fiesta-inventory-forecast.fiesta_inventory.backtest_baseline_4w` AS
WITH w AS (
  SELECT
    sku,
    week_start,
    qty_sold
  FROM `fiesta-inventory-forecast.fiesta_inventory.sales_weekly`
  WHERE week_start >= DATE_SUB(cutoff_week_start, INTERVAL 1 WEEK)
    AND week_start <= last_complete_week_start
),
pairs AS (
  SELECT
    a.sku,
    a.week_start,
    a.qty_sold AS actual_qty,
    COALESCE(b.qty_sold, 0) AS baseline_qty
  FROM w a
  LEFT JOIN w b
    ON a.sku = b.sku
   AND b.week_start = DATE_SUB(a.week_start, INTERVAL 1 WEEK)
  WHERE a.week_start >= cutoff_week_start
)
SELECT
  sku,
  SAFE_DIVIDE(SUM(ABS(actual_qty - baseline_qty)), NULLIF(SUM(actual_qty), 0)) AS baseline_wape,
  SUM(actual_qty) AS sum_actual_holdout
FROM pairs
GROUP BY sku;

-- ---------- 5) Quality flags for restock (same table name used by restock SQL) ----------
CREATE OR REPLACE TABLE `fiesta-inventory-forecast.fiesta_inventory.model_quality_flags` AS
WITH m AS (
  SELECT sku, wape, sum_actual
  FROM `fiesta-inventory-forecast.fiesta_inventory.backtest_metrics_sku_4w`
),
b AS (
  SELECT sku, baseline_wape, sum_actual_holdout
  FROM `fiesta-inventory-forecast.fiesta_inventory.backtest_baseline_4w`
)
SELECT
  m.sku,
  m.wape,
  b.baseline_wape,
  m.sum_actual AS sum_actual,

  CASE
    -- weekly holdout is only 4 points; use lower threshold than daily
    WHEN COALESCE(b.sum_actual_holdout, 0) < 4 THEN 'NO_DATA'
    WHEN m.wape IS NULL THEN 'NO_DATA'

    -- absolute-quality shortcut so we can still have GOOD even if baseline is strong
    WHEN m.wape <= 0.60 THEN 'GOOD'

    WHEN b.baseline_wape IS NOT NULL AND m.wape >= b.baseline_wape THEN 'WORSE_THAN_BASELINE'
    WHEN m.wape > 1.00 THEN 'BAD'
    WHEN m.wape > 0.80 THEN 'WEAK'
    ELSE 'GOOD'
  END AS model_quality,

  CURRENT_TIMESTAMP() AS created_at
FROM m
LEFT JOIN b USING (sku);