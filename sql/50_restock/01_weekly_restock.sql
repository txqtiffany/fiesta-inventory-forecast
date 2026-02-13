-- ============================================================
-- weekly_restock_pipeline.sql
-- Weekly pipeline: model -> forecasts -> stockouts -> restocks
-- Assumes weekly_model_validation.sql ran first (refreshes sales_daily + model_quality_flags)
-- Uses variant_id as canonical key (STRING)
-- ============================================================

DECLARE latest_snapshot_date DATE;

-- ---------- latest inventory snapshot (partition-safe) ----------
SET latest_snapshot_date = (
  SELECT MAX(snapshot_date)
  FROM `fiesta-inventory-forecast.fiesta_inventory.inventory_snapshots_raw`
  WHERE snapshot_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
);

-- ---------- 1) Train/refresh ARIMA model (variant_id) ----------
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
SELECT
  sd.sale_date,
  sd.variant_id,
  sd.qty_sold
FROM `fiesta-inventory-forecast.fiesta_inventory.sales_daily` sd
JOIN `fiesta-inventory-forecast.fiesta_inventory.variant_vendor_map` vvm
  ON vvm.variant_id = sd.variant_id
WHERE sd.sale_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 365 DAY)
  AND sd.qty_sold > 0;

-- ---------- 2) Forecasts (60 days) ----------
CREATE OR REPLACE TABLE `fiesta-inventory-forecast.fiesta_inventory.demand_forecasts` AS
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
WHERE forecast_value > 0;

-- ---------- 3) Stockout predictions ----------
CREATE OR REPLACE TABLE `fiesta-inventory-forecast.fiesta_inventory.stockout_predictions` AS
WITH current_inventory AS (
  SELECT
    variant_id,
    SUM(available_qty) AS raw_stock,
    GREATEST(SUM(available_qty), 0) AS current_stock
  FROM `fiesta-inventory-forecast.fiesta_inventory.inventory_snapshots_raw`
  WHERE snapshot_date = latest_snapshot_date   -- ✅ partition filter
    AND variant_id IS NOT NULL AND variant_id != ''
  GROUP BY variant_id
),
daily_forecast AS (
  SELECT variant_id, forecast_date, predicted_qty
  FROM `fiesta-inventory-forecast.fiesta_inventory.demand_forecasts`
),
cum_calc AS (
  SELECT
    f.variant_id,
    f.forecast_date,
    ci.current_stock,
    SUM(f.predicted_qty) OVER (
      PARTITION BY f.variant_id
      ORDER BY f.forecast_date
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cum_demand
  FROM daily_forecast f
  JOIN current_inventory ci
    ON f.variant_id = ci.variant_id
  WHERE ci.current_stock > 0
)
SELECT
  variant_id,
  current_stock,
  MIN(forecast_date) AS stockout_date,
  DATE_DIFF(MIN(forecast_date), CURRENT_DATE(), DAY) AS days_remaining,
  CURRENT_TIMESTAMP() AS created_at
FROM cum_calc
WHERE cum_demand >= current_stock
GROUP BY variant_id, current_stock;

-- ---------- 4) Weekly vendor restocks ----------
CREATE OR REPLACE TABLE `fiesta-inventory-forecast.fiesta_inventory.vendor_restocks_weekly` AS
WITH active_vendors AS (
  SELECT vendor_name
  FROM `fiesta-inventory-forecast.fiesta_inventory.vendor_status`
  WHERE COALESCE(archived, FALSE) = FALSE
    AND vendor_name <> 'Fiesta Carnival'
),
cadence AS (
  SELECT vendor_name, restock_frequency_days
  FROM `fiesta-inventory-forecast.fiesta_inventory.vendor_restock_cadence_one_time`
),
vendor_defaults AS (
  SELECT
    vendor_name,
    COALESCE(lead_time_days, 5) AS lead_time_days,
    COALESCE(moq, 6) AS moq,
    COALESCE(pack_size, 6) AS pack_size
  FROM `fiesta-inventory-forecast.fiesta_inventory.vendors`
),
variant_vendor AS (
  SELECT
    vvm.variant_id,
    vvm.sku,
    vvm.vendor_name,
    p.title AS product_title,
    v.title AS variant_title
  FROM `fiesta-inventory-forecast.fiesta_inventory.variant_vendor_map` vvm
  JOIN `fiesta-inventory-forecast.fiesta_inventory.variants` v
    ON v.variant_id = vvm.variant_id
  JOIN `fiesta-inventory-forecast.fiesta_inventory.products` p
    ON p.product_id = v.product_id
),
raw_inventory AS (
  SELECT
    variant_id,
    SUM(available_qty) AS raw_stock
  FROM `fiesta-inventory-forecast.fiesta_inventory.inventory_snapshots_raw`
  WHERE snapshot_date = latest_snapshot_date   -- ✅ partition filter
    AND variant_id IS NOT NULL AND variant_id != ''
  GROUP BY variant_id
),
current_inventory AS (
  SELECT variant_id, GREATEST(raw_stock, 0) AS current_stock
  FROM raw_inventory
),
demand_window AS (
  SELECT
    vv.vendor_name,
    vv.variant_id,
    vd.lead_time_days,
    DATE_ADD(CURRENT_DATE(), INTERVAL (vd.lead_time_days + 7 + 3) DAY) AS horizon_end,
    (vd.lead_time_days + 7 + 3) AS horizon_days
  FROM variant_vendor vv
  JOIN vendor_defaults vd
    ON vd.vendor_name = vv.vendor_name
),
forecast_demand AS (
  SELECT
    w.vendor_name,
    w.variant_id,
    SUM(f.predicted_qty) AS expected_demand_forecast
  FROM demand_window w
  JOIN `fiesta-inventory-forecast.fiesta_inventory.demand_forecasts` f
    ON f.variant_id = w.variant_id
   AND f.forecast_date BETWEEN CURRENT_DATE() AND w.horizon_end
  GROUP BY w.vendor_name, w.variant_id
),
fallback_demand AS (
  SELECT
    variant_id,
    SAFE_DIVIDE(SUM(quantity_sold), 56) AS avg_daily_units_56d
  FROM `fiesta-inventory-forecast.fiesta_inventory.sales_history_raw`
  WHERE sale_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 56 DAY)   -- ✅ partition filter
    AND variant_id IS NOT NULL AND variant_id != ''
  GROUP BY variant_id
),
mq AS (
  SELECT variant_id, model_quality
  FROM `fiesta-inventory-forecast.fiesta_inventory.model_quality_flags`
),
calc AS (
  SELECT
    vv.vendor_name,
    vv.variant_id,
    vv.sku,
    vv.product_title,
    vv.variant_title,

    COALESCE(ci.current_stock, 0) AS current_stock,
    COALESCE(ri.raw_stock, 0) AS raw_stock,
    (COALESCE(ri.raw_stock, 0) < 0) AS negative_stock_flag,

    vd.lead_time_days,
    vd.moq,
    vd.pack_size,
    w.horizon_days,

    COALESCE(mq.model_quality, 'NO_DATA') AS model_quality,

    CASE
      WHEN COALESCE(mq.model_quality, 'NO_DATA') = 'GOOD'
        THEN COALESCE(fd.expected_demand_forecast, 0)
      ELSE CAST(ROUND(COALESCE(fb.avg_daily_units_56d, 0) * w.horizon_days) AS INT64)
    END AS expected_demand,

    CASE
      WHEN COALESCE(mq.model_quality, 'NO_DATA') = 'GOOD' THEN 'FORECAST'
      ELSE 'FALLBACK_56D'
    END AS demand_source,

    CASE
      WHEN (
        CASE
          WHEN COALESCE(mq.model_quality, 'NO_DATA') = 'GOOD'
            THEN COALESCE(fd.expected_demand_forecast, 0)
          ELSE CAST(ROUND(COALESCE(fb.avg_daily_units_56d, 0) * w.horizon_days) AS INT64)
        END
      ) - COALESCE(ci.current_stock, 0) <= 0 THEN 0
      ELSE GREATEST(
        vd.moq,
        CAST(
          CEIL((
            (
              CASE
                WHEN COALESCE(mq.model_quality, 'NO_DATA') = 'GOOD'
                  THEN COALESCE(fd.expected_demand_forecast, 0)
                ELSE CAST(ROUND(COALESCE(fb.avg_daily_units_56d, 0) * w.horizon_days) AS INT64)
              END
            ) - COALESCE(ci.current_stock, 0)
          ) / vd.pack_size) * vd.pack_size AS INT64
        )
      )
    END AS reorder_qty,

    COALESCE(fd.expected_demand_forecast, 0) AS expected_demand_forecast,
    CAST(ROUND(COALESCE(fb.avg_daily_units_56d, 0) * w.horizon_days) AS INT64) AS expected_demand_fallback,

    latest_snapshot_date AS snapshot_date,
    CURRENT_TIMESTAMP() AS created_at

  FROM variant_vendor vv
  JOIN active_vendors av
    ON av.vendor_name = vv.vendor_name
  JOIN cadence c
    ON c.vendor_name = vv.vendor_name
  JOIN vendor_defaults vd
    ON vd.vendor_name = vv.vendor_name
  JOIN demand_window w
    ON w.vendor_name = vv.vendor_name
   AND w.variant_id = vv.variant_id

  LEFT JOIN raw_inventory ri
    ON ri.variant_id = vv.variant_id
  LEFT JOIN current_inventory ci
    ON ci.variant_id = vv.variant_id
  LEFT JOIN forecast_demand fd
    ON fd.vendor_name = vv.vendor_name
   AND fd.variant_id = vv.variant_id
  LEFT JOIN fallback_demand fb
    ON fb.variant_id = vv.variant_id
  LEFT JOIN mq
    ON mq.variant_id = vv.variant_id

  WHERE c.restock_frequency_days = 7
)

SELECT *
FROM calc
WHERE reorder_qty > 0
ORDER BY
  negative_stock_flag DESC,
  reorder_qty DESC,
  vendor_name,
  variant_id;