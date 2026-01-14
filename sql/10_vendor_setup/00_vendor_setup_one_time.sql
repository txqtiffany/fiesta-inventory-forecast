-- One-time vendor revenue ranking (last 6 months)
CREATE OR REPLACE TABLE `fiesta-inventory-forecast.fiesta_inventory.vendor_sales_rank_one_time` AS
WITH sku_vendor_price AS (
  SELECT
    v.sku,
    p.vendor AS vendor_name,
    SAFE_CAST(v.price AS FLOAT64) AS unit_price
  FROM `fiesta-inventory-forecast.fiesta_inventory.variants` v
  JOIN `fiesta-inventory-forecast.fiesta_inventory.products` p
    ON v.product_id = p.product_id
  WHERE v.sku IS NOT NULL AND v.sku != ''
    AND p.vendor IS NOT NULL AND p.vendor != ''
),
sales_6m AS (
  SELECT
    sh.sku,
    SUM(sh.quantity_sold) AS units_sold_6m
  FROM `fiesta-inventory-forecast.fiesta_inventory.sales_history` sh
  WHERE sh.sale_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 6 MONTH)
    AND sh.sku IS NOT NULL AND sh.sku != ''
  GROUP BY sh.sku
),
vendor_sales AS (
  SELECT
    svp.vendor_name,
    SUM(s.units_sold_6m) AS vendor_units_sold_6m,
    SUM(s.units_sold_6m * COALESCE(svp.unit_price, 0)) AS vendor_revenue_6m
  FROM sales_6m s
  JOIN sku_vendor_price svp USING (sku)
  GROUP BY svp.vendor_name
)
SELECT
  vendor_name,
  vendor_units_sold_6m,
  vendor_revenue_6m,
  DENSE_RANK() OVER (ORDER BY vendor_revenue_6m DESC) AS revenue_rank,
  CURRENT_DATE() AS frozen_as_of_date
FROM vendor_sales
ORDER BY revenue_rank;

-- Freeze cadence: Top 8 weekly, others bi-weekly
CREATE OR REPLACE TABLE `fiesta-inventory-forecast.fiesta_inventory.vendor_restock_cadence_one_time` AS
SELECT
  vendor_name,
  revenue_rank,
  vendor_revenue_6m,
  vendor_units_sold_6m,
  CASE WHEN revenue_rank <= 8 THEN 7 ELSE 14 END AS restock_frequency_days,
  frozen_as_of_date
FROM `fiesta-inventory-forecast.fiesta_inventory.vendor_sales_rank_one_time`;


-- Ensure vendors table has defaults + includes any new vendors
CREATE OR REPLACE TABLE `fiesta-inventory-forecast.fiesta_inventory.vendors` AS
WITH all_vendors AS (
  SELECT DISTINCT vendor AS vendor_name
  FROM `fiesta-inventory-forecast.fiesta_inventory.products`
  WHERE vendor IS NOT NULL AND vendor != ''
),
existing AS (
  SELECT vendor_id, vendor_name, lead_time_days, moq, pack_size
  FROM `fiesta-inventory-forecast.fiesta_inventory.vendors`
),
new_vendors AS (
  SELECT a.vendor_name
  FROM all_vendors a
  LEFT JOIN existing e
    ON a.vendor_name = e.vendor_name
  WHERE e.vendor_name IS NULL
),
new_with_ids AS (
  SELECT
    (SELECT COALESCE(MAX(vendor_id), 0) FROM existing)
      + ROW_NUMBER() OVER (ORDER BY vendor_name) AS vendor_id,
    vendor_name,
    5 AS lead_time_days,
    6 AS moq,
    1 AS pack_size
  FROM new_vendors
),
existing_clean AS (
  SELECT
    vendor_id,
    vendor_name,
    COALESCE(lead_time_days, 5) AS lead_time_days,
    COALESCE(moq, 6) AS moq,
    COALESCE(pack_size, 1) AS pack_size
  FROM existing
)
SELECT * FROM existing_clean
UNION ALL
SELECT * FROM new_with_ids;

-- Add “Archived vendor” option (never restock)
CREATE TABLE IF NOT EXISTS `fiesta-inventory-forecast.fiesta_inventory.vendor_status` (
  vendor_name STRING NOT NULL,
  archived BOOL,
  archived_at TIMESTAMP,
  note STRING
);
MERGE `fiesta-inventory-forecast.fiesta_inventory.vendor_status` T
USING (
  SELECT DISTINCT vendor_name
  FROM `fiesta-inventory-forecast.fiesta_inventory.vendors`
) S
ON T.vendor_name = S.vendor_name
WHEN NOT MATCHED THEN
  INSERT (vendor_name, archived, archived_at, note)
  VALUES (S.vendor_name, FALSE, NULL, NULL);

/*
-- How to archive a vendor
UPDATE `fiesta-inventory-forecast.fiesta_inventory.vendor_status`
SET archived = TRUE,
    archived_at = CURRENT_TIMESTAMP(),
    note = 'Stop reordering from this vendor'
WHERE vendor_name = '<<PUT EXACT VENDOR NAME HERE>>';

-- How to unarchive a vendor
UPDATE `fiesta-inventory-forecast.fiesta_inventory.vendor_status`
SET archived = FALSE,
    archived_at = NULL
WHERE vendor_name = '<<PUT EXACT VENDOR NAME HERE>>';

*/
