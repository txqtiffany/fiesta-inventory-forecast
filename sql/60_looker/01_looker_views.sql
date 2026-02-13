-- ============================================================
-- Looker-friendly views
-- Uses:
--   current_inventory (sku-level snapshot)
--   vendor_restocks_weekly (sku + vendor mapping + restock metrics)
--   vendor_status (vendor archived flag)
-- ============================================================

-- 1) Active vendors
CREATE OR REPLACE VIEW `fiesta-inventory-forecast.fiesta_inventory.v_active_vendors` AS
SELECT vendor_name
FROM `fiesta-inventory-forecast.fiesta_inventory.vendor_status`
WHERE COALESCE(archived, FALSE) = FALSE;


-- 2) Canonical mapping: variant_id -> vendor/product/variant attributes
CREATE OR REPLACE VIEW `fiesta-inventory-forecast.fiesta_inventory.v_variant_vendor_map` AS
SELECT
  v.variant_id,
  NULLIF(v.sku, '') AS sku,
  p.vendor AS vendor_name,
  p.title AS product_title,
  v.title AS variant_title
FROM `fiesta-inventory-forecast.fiesta_inventory.variants` v
JOIN `fiesta-inventory-forecast.fiesta_inventory.products` p
  ON v.product_id = p.product_id
WHERE v.variant_id IS NOT NULL
  AND p.vendor IS NOT NULL AND p.vendor != '';

-- 3) Current inventory with vendor attached + active vendor filter
CREATE OR REPLACE VIEW `fiesta-inventory-forecast.fiesta_inventory.v_current_inventory_active` AS
WITH active_vendors AS (
  SELECT vendor_name
  FROM `fiesta-inventory-forecast.fiesta_inventory.vendor_status`
  WHERE COALESCE(archived, FALSE) = FALSE
)
SELECT
  ci.snapshot_date,
  ci.variant_id,
  vvm.sku,
  vvm.vendor_name,
  vvm.product_title,
  vvm.variant_title,
  ci.raw_stock,
  ci.current_stock,
  ci.negative_stock_flag
FROM `fiesta-inventory-forecast.fiesta_inventory.current_inventory` ci
LEFT JOIN `fiesta-inventory-forecast.fiesta_inventory.v_variant_vendor_map` vvm
  ON vvm.variant_id = ci.variant_id
WHERE vvm.vendor_name IN (SELECT vendor_name FROM active_vendors);

-- 4) Restock list filtered to active vendors (and optional internal vendor exclusion)
CREATE OR REPLACE VIEW `fiesta-inventory-forecast.fiesta_inventory.v_vendor_restocks_weekly_active` AS
SELECT r.*
FROM `fiesta-inventory-forecast.fiesta_inventory.vendor_restocks_weekly` r
JOIN `fiesta-inventory-forecast.fiesta_inventory.v_active_vendors` av
  ON r.vendor_name = av.vendor_name
WHERE r.vendor_name <> 'Fiesta Carnival';


-- 5) Vendor summary for charts
CREATE OR REPLACE VIEW `fiesta-inventory-forecast.fiesta_inventory.v_vendor_summary` AS
SELECT
  vendor_name,
  COUNT(DISTINCT variant_id) AS active_variant_count,
  SUM(current_stock) AS total_current_stock,
  SUM(CASE WHEN negative_stock_flag THEN 1 ELSE 0 END) AS negative_variants
FROM `fiesta-inventory-forecast.fiesta_inventory.v_current_inventory_active`
GROUP BY vendor_name;


-- 6) Inventory exceptions (negatives + missing vendor mapping)
CREATE OR REPLACE VIEW `fiesta-inventory-forecast.fiesta_inventory.v_inventory_exceptions` AS
SELECT
  ci.snapshot_date,
  ci.variant_id,
  vvm.sku,
  ci.raw_stock,
  ci.current_stock,
  ci.negative_stock_flag,
  vvm.vendor_name,
  CASE
    WHEN vvm.vendor_name IS NULL THEN 'MISSING_VENDOR_MAPPING'
    WHEN ci.negative_stock_flag THEN 'NEGATIVE_STOCK'
    ELSE 'OK'
  END AS exception_type
FROM `fiesta-inventory-forecast.fiesta_inventory.current_inventory` ci
LEFT JOIN `fiesta-inventory-forecast.fiesta_inventory.v_variant_vendor_map` vvm
  ON vvm.variant_id = ci.variant_id
WHERE ci.negative_stock_flag = TRUE
   OR vvm.vendor_name IS NULL;
