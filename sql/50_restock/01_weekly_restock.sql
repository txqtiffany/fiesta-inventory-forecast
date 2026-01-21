DECLARE latest_snapshot_date DATE;

SET latest_snapshot_date = (
  SELECT MAX(PARSE_DATE('%Y%m%d', partition_id))
  FROM `fiesta-inventory-forecast.fiesta_inventory.INFORMATION_SCHEMA.PARTITIONS`
  WHERE table_name = 'inventory_snapshots_raw'
    AND partition_id NOT IN ('__NULL__', '__UNPARTITIONED__')
);

CREATE OR REPLACE TABLE `fiesta-inventory-forecast.fiesta_inventory.vendor_restocks_weekly` AS
WITH active_vendors AS (
  SELECT vendor_name
  FROM `fiesta-inventory-forecast.fiesta_inventory.vendor_status`
  WHERE COALESCE(archived, FALSE) = FALSE
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
sku_vendor AS (
  SELECT
    v.sku,
    p.vendor AS vendor_name,
    p.title AS product_title,
    v.title AS variant_title
  FROM `fiesta-inventory-forecast.fiesta_inventory.variants` v
  JOIN `fiesta-inventory-forecast.fiesta_inventory.products` p
    ON v.product_id = p.product_id
  WHERE v.sku IS NOT NULL AND v.sku != ''
    AND p.vendor IS NOT NULL AND p.vendor != ''
),
raw_inventory AS (
  SELECT
    inv.sku,
    SUM(inv.available_qty) AS raw_stock
  FROM `fiesta-inventory-forecast.fiesta_inventory.inventory_snapshots_raw` AS inv
  WHERE inv.snapshot_date = latest_snapshot_date
  GROUP BY inv.sku
),
current_inventory AS (
  SELECT
    sku,
    GREATEST(raw_stock, 0) AS current_stock
  FROM raw_inventory
),
demand_window AS (
  -- weekly vendors: lead_time + cadence(7) + buffer(3)
  SELECT
    sv.vendor_name,
    sv.sku,
    vd.lead_time_days,
    DATE_ADD(CURRENT_DATE(), INTERVAL (vd.lead_time_days + 7 + 3) DAY) AS horizon_end,
    (vd.lead_time_days + 7 + 3) AS horizon_days
  FROM sku_vendor sv
  JOIN vendor_defaults vd USING (vendor_name)
),
forecast_demand AS (
  SELECT
    w.vendor_name,
    w.sku,
    SUM(f.predicted_qty) AS expected_demand_forecast
  FROM demand_window w
  JOIN `fiesta-inventory-forecast.fiesta_inventory.demand_forecasts` f
    ON f.sku = w.sku
   AND f.forecast_date BETWEEN CURRENT_DATE() AND w.horizon_end
  GROUP BY w.vendor_name, w.sku
),
fallback_demand AS (
  -- fallback: avg daily units over last 56 days (8 weeks)
  SELECT
    sku,
    SAFE_DIVIDE(SUM(qty_sold), 56) AS avg_daily_units_56d
  FROM `fiesta-inventory-forecast.fiesta_inventory.sales_daily`
  WHERE sale_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 56 DAY)
  GROUP BY sku
),
calc AS (
  SELECT
    sv.vendor_name,
    sv.sku,
    sv.product_title,
    sv.variant_title,

    COALESCE(i.current_stock, 0) AS current_stock,
    COALESCE(r.raw_stock, 0) AS raw_stock,
    (COALESCE(r.raw_stock, 0) < 0) AS negative_stock_flag,

    -- Use forecast only if model is GOOD; otherwise fallback to sales-based estimate
    CASE
      WHEN COALESCE(mq.model_quality, 'NO_DATA') = 'GOOD'
        THEN COALESCE(fd.expected_demand_forecast, 0)
      ELSE CAST(ROUND(COALESCE(fb.avg_daily_units_56d, 0) * w.horizon_days) AS INT64)
    END AS expected_demand,
    CASE
      WHEN COALESCE(mq.model_quality, 'NO_DATA') = 'GOOD' THEN 'FORECAST'
      ELSE 'FALLBACK_56D'
    END AS demand_source,

    -- reorder = max(moq, round up to pack size) on the shortfall
    CASE
      WHEN (
        CASE
          WHEN COALESCE(mq.model_quality, 'NO_DATA') = 'GOOD'
            THEN COALESCE(fd.expected_demand_forecast, 0)
          ELSE CAST(ROUND(COALESCE(fb.avg_daily_units_56d, 0) * w.horizon_days) AS INT64)
        END
      ) - COALESCE(i.current_stock, 0) <= 0 THEN 0
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
            ) - COALESCE(i.current_stock, 0)
          ) / vd.pack_size) * vd.pack_size AS INT64
        )
      )
    END AS reorder_qty,

    COALESCE(fd.expected_demand_forecast, 0) AS expected_demand_forecast,
    COALESCE(mq.model_quality, 'NO_DATA') AS model_quality,
    CAST(ROUND(COALESCE(fb.avg_daily_units_56d, 0) * w.horizon_days) AS INT64) AS expected_demand_fallback,
    vd.lead_time_days,
    vd.moq,
    vd.pack_size

  FROM sku_vendor sv
  JOIN cadence c USING (vendor_name)
  JOIN active_vendors av USING (vendor_name)
  JOIN vendor_defaults vd USING (vendor_name)
  JOIN demand_window w
    ON w.vendor_name = sv.vendor_name AND w.sku = sv.sku

  LEFT JOIN raw_inventory r
    ON r.sku = sv.sku

  LEFT JOIN current_inventory i
    ON i.sku = sv.sku

  LEFT JOIN forecast_demand fd
    ON fd.vendor_name = sv.vendor_name
   AND fd.sku = sv.sku

  LEFT JOIN fallback_demand fb
    ON fb.sku = sv.sku

  LEFT JOIN `fiesta-inventory-forecast.fiesta_inventory.model_quality_flags` mq
    ON mq.sku = sv.sku

  WHERE c.restock_frequency_days = 7
)
SELECT
  *,
  CURRENT_TIMESTAMP() AS created_at
FROM calc
WHERE reorder_qty > 0
ORDER BY
  negative_stock_flag DESC,
  reorder_qty DESC,
  vendor_name,
  sku;