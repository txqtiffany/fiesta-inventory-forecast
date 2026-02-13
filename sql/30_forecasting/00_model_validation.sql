-- ============================================================
-- 00_model_validation_weekly.sql
-- Weekly self-validation (backtest) for demand forecasting (WEEKLY granularity)
-- Excludes vendor_name = 'Fiesta Carnival' and archived vendors
--
-- Outputs:
--   - sales_daily
--   - sku_vendor_map
--   - sales_weekly
--   - demand_arima_backtest_weekly (MODEL)
--   - backtest_forecast_4w
--   - backtest_metrics_sku_4w
--   - backtest_baseline_4w
--   - model_quality_flags   <-- used by restock SQL
-- ============================================================

-- ---------- Parameters ----------
DECLARE holdout_weeks INT64 DEFAULT 4;
DECLARE last_complete_week_start DATE;
DECLARE cutoff_week_start DATE;
DECLARE train_rows INT64;

-- Monday-based weeks. Change to WEEK(SUNDAY) if desired.
SET last_complete_week_start = DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 1 WEEK), WEEK(MONDAY));
SET cutoff_week_start = DATE_SUB(last_complete_week_start, INTERVAL holdout_weeks WEEK);


-- ---------- Active vendors ----------
CREATE OR REPLACE TEMP TABLE active_vendors AS
SELECT vendor_name
FROM `fiesta-inventory-forecast.fiesta_inventory.vendor_status`
WHERE COALESCE(archived, FALSE) = FALSE
  AND vendor_name <> 'Fiesta Carnival';

-- ---------- Canonical variant -> vendor map ----------
CREATE OR REPLACE TABLE `fiesta-inventory-forecast.fiesta_inventory.variant_vendor_map` AS
SELECT
  v.variant_id,
  v.sku,
  p.vendor AS vendor_name
FROM `fiesta-inventory-forecast.fiesta_inventory.variants` v
JOIN `fiesta-inventory-forecast.fiesta_inventory.products` p
  ON v.product_id = p.product_id
JOIN active_vendors av
  ON av.vendor_name = p.vendor
WHERE v.variant_id IS NOT NULL AND v.variant_id != ''
  AND p.vendor IS NOT NULL AND p.vendor != '';

-- ---------- 0) sales_daily (DROP+CREATE to change clustering to variant_id) ----------
DROP TABLE IF EXISTS `fiesta-inventory-forecast.fiesta_inventory.sales_daily`;

CREATE TABLE `fiesta-inventory-forecast.fiesta_inventory.sales_daily`
PARTITION BY sale_date
CLUSTER BY variant_id AS
SELECT
  sale_date,
  variant_id,
  ANY_VALUE(sku) AS sku,               -- label only
  SUM(quantity_sold) AS qty_sold
FROM `fiesta-inventory-forecast.fiesta_inventory.sales_history_raw`
WHERE sale_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 365 DAY)   -- ✅ partition filter
  AND variant_id IS NOT NULL AND variant_id != ''
GROUP BY sale_date, variant_id;

-- ---------- 0b) sales_weekly ----------
DROP TABLE IF EXISTS `fiesta-inventory-forecast.fiesta_inventory.sales_weekly`;

CREATE TABLE `fiesta-inventory-forecast.fiesta_inventory.sales_weekly`
PARTITION BY week_start
CLUSTER BY variant_id AS
SELECT
  DATE_TRUNC(sd.sale_date, WEEK(MONDAY)) AS week_start,
  sd.variant_id,
  ANY_VALUE(sd.sku) AS sku,
  SUM(sd.qty_sold) AS qty_sold
FROM `fiesta-inventory-forecast.fiesta_inventory.sales_daily` sd
JOIN `fiesta-inventory-forecast.fiesta_inventory.variant_vendor_map` vvm
  ON vvm.variant_id = sd.variant_id
WHERE sd.sale_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 365 DAY)
GROUP BY week_start, variant_id;

-- ---------- Guard: training rows ----------
SET train_rows = (
  SELECT COUNT(*)
  FROM `fiesta-inventory-forecast.fiesta_inventory.sales_weekly`
  WHERE week_start >= DATE_SUB(last_complete_week_start, INTERVAL 52 WEEK)
    AND week_start < cutoff_week_start
    AND qty_sold > 0
);

IF train_rows = 0 THEN

  CREATE OR REPLACE TABLE `fiesta-inventory-forecast.fiesta_inventory.backtest_forecast_4w` AS
  SELECT CAST(NULL AS STRING) AS variant_id, CAST(NULL AS DATE) AS week_start, CAST(NULL AS INT64) AS predicted_qty
  WHERE FALSE;

  CREATE OR REPLACE TABLE `fiesta-inventory-forecast.fiesta_inventory.backtest_metrics_sku_4w` AS
  SELECT CAST(NULL AS STRING) AS variant_id, CAST(NULL AS INT64) AS sum_actual, CAST(NULL AS INT64) AS sum_abs_error,
         CAST(NULL AS FLOAT64) AS wape, CAST(NULL AS FLOAT64) AS mape, CURRENT_TIMESTAMP() AS created_at
  WHERE FALSE;

  CREATE OR REPLACE TABLE `fiesta-inventory-forecast.fiesta_inventory.backtest_baseline_4w` AS
  SELECT CAST(NULL AS STRING) AS variant_id, CAST(NULL AS FLOAT64) AS baseline_wape, CAST(NULL AS INT64) AS sum_actual_holdout
  WHERE FALSE;

  CREATE OR REPLACE TABLE `fiesta-inventory-forecast.fiesta_inventory.model_quality_flags` AS
  SELECT
    vvm.variant_id,
    CAST(NULL AS FLOAT64) AS wape,
    CAST(NULL AS FLOAT64) AS baseline_wape,
    CAST(NULL AS INT64) AS sum_actual,
    'NO_DATA' AS model_quality,
    CURRENT_TIMESTAMP() AS created_at
  FROM `fiesta-inventory-forecast.fiesta_inventory.variant_vendor_map` vvm
  WHERE FALSE;

ELSE

  -- ---------- 1) Train weekly backtest model ----------
  CREATE OR REPLACE MODEL `fiesta-inventory-forecast.fiesta_inventory.demand_arima_backtest_weekly`
  OPTIONS(
    model_type='ARIMA_PLUS',
    time_series_timestamp_col='week_start',
    time_series_data_col='qty_sold',
    time_series_id_col='variant_id',
    auto_arima=TRUE,
    data_frequency='AUTO_FREQUENCY'
  ) AS
  SELECT week_start, CAST(variant_id AS STRING) AS variant_id, qty_sold
  FROM `fiesta-inventory-forecast.fiesta_inventory.sales_weekly`
  WHERE week_start >= DATE_SUB(last_complete_week_start, INTERVAL 52 WEEK)
    AND week_start < cutoff_week_start
    AND qty_sold > 0;

  -- ---------- 2) Forecast holdout (4 weeks) ----------
  CREATE OR REPLACE TABLE `fiesta-inventory-forecast.fiesta_inventory.backtest_forecast_4w` AS
  SELECT
    variant_id,
    CAST(forecast_timestamp AS DATE) AS week_start,
    GREATEST(CAST(forecast_value AS INT64), 0) AS predicted_qty
  FROM ML.FORECAST(
    MODEL `fiesta-inventory-forecast.fiesta_inventory.demand_arima_backtest_weekly`,
    STRUCT(4 AS horizon)
  );

  -- ---------- 3) Metrics ----------
CREATE OR REPLACE TABLE `fiesta-inventory-forecast.fiesta_inventory.backtest_metrics_variant_4w` AS
  WITH actual AS (
    SELECT variant_id, week_start, qty_sold AS actual_qty
    FROM `fiesta-inventory-forecast.fiesta_inventory.sales_weekly`
    WHERE week_start >= cutoff_week_start
      AND week_start <= last_complete_week_start
  ),
  joined AS (
    SELECT
      COALESCE(a.variant_id, f.variant_id) AS variant_id,
      COALESCE(a.week_start, f.week_start) AS week_start,
      COALESCE(a.actual_qty, 0) AS actual_qty,
      COALESCE(f.predicted_qty, 0) AS predicted_qty
    FROM actual a
    FULL OUTER JOIN `fiesta-inventory-forecast.fiesta_inventory.backtest_forecast_4w` f
      ON a.variant_id = f.variant_id
     AND a.week_start = f.week_start
  ),
  agg AS (
    SELECT
      variant_id,
      SUM(actual_qty) AS sum_actual,
      SUM(ABS(actual_qty - predicted_qty)) AS sum_abs_error,
      AVG(CASE WHEN actual_qty = 0 THEN NULL ELSE ABS(actual_qty - predicted_qty) / actual_qty END) AS mape
    FROM joined
    GROUP BY variant_id
  )
  SELECT
    variant_id,
    sum_actual,
    sum_abs_error,
    SAFE_DIVIDE(sum_abs_error, NULLIF(sum_actual, 0)) AS wape,
    mape,
    CURRENT_TIMESTAMP() AS created_at
  FROM agg;

  -- ---------- 4) Baseline (previous week) ----------
  CREATE OR REPLACE TABLE `fiesta-inventory-forecast.fiesta_inventory.backtest_baseline_4w` AS
  WITH w AS (
    SELECT variant_id, week_start, qty_sold
    FROM `fiesta-inventory-forecast.fiesta_inventory.sales_weekly`
    WHERE week_start >= DATE_SUB(cutoff_week_start, INTERVAL 1 WEEK)
      AND week_start <= last_complete_week_start
  ),
  pairs AS (
    SELECT
      a.variant_id,
      a.week_start,
      a.qty_sold AS actual_qty,
      COALESCE(b.qty_sold, 0) AS baseline_qty
    FROM w a
    LEFT JOIN w b
      ON a.variant_id = b.variant_id
     AND b.week_start = DATE_SUB(a.week_start, INTERVAL 1 WEEK)
    WHERE a.week_start >= cutoff_week_start
  )
  SELECT
    variant_id,
    SAFE_DIVIDE(SUM(ABS(actual_qty - baseline_qty)), NULLIF(SUM(actual_qty), 0)) AS baseline_wape,
    SUM(actual_qty) AS sum_actual_holdout
  FROM pairs
  GROUP BY variant_id;

  -- ---------- 5) Final quality flags (same table name you already use) ----------
  CREATE OR REPLACE TABLE `fiesta-inventory-forecast.fiesta_inventory.model_quality_flags` AS
  WITH m AS (
    SELECT variant_id, wape, sum_actual
    FROM `fiesta-inventory-forecast.fiesta_inventory.backtest_metrics_variant_4w`
  ),
  b AS (
    SELECT variant_id, baseline_wape, sum_actual_holdout
    FROM `fiesta-inventory-forecast.fiesta_inventory.backtest_baseline_4w`
  )
  SELECT
    m.variant_id,
    m.wape,
    b.baseline_wape,
    m.sum_actual AS sum_actual,
    CASE
      WHEN COALESCE(b.sum_actual_holdout, 0) < 4 THEN 'NO_DATA'
      WHEN m.wape IS NULL THEN 'NO_DATA'
      WHEN m.wape <= 0.60 THEN 'GOOD'
      WHEN b.baseline_wape IS NOT NULL AND m.wape >= b.baseline_wape THEN 'WORSE_THAN_BASELINE'
      WHEN m.wape > 1.00 THEN 'BAD'
      WHEN m.wape > 0.80 THEN 'WEAK'
      ELSE 'GOOD'
    END AS model_quality,
    CURRENT_TIMESTAMP() AS created_at
  FROM m
  LEFT JOIN b
    ON m.variant_id = b.variant_id;

END IF;