WITH orders AS (
  SELECT
    *
  FROM {{ ref('marks_stg_orders') }}
)
, order_metrics AS (
  SELECT
    *
  FROM {{ ref('marks_orders_parts_metrics') }}
)

SELECT
  orders.*
  , order_metrics.unique_parts_count
  , order_metrics.cnc_parts_count
  , order_metrics.laser_parts_count
  , order_metrics.bending_parts_count
  , order_metrics.total_bends_count
  , order_metrics.surface_coated_parts_count
  , order_metrics.insert_operations_count
  , order_metrics.all_ral_codes
  , order_metrics.all_ral_finishes
  , order_metrics.all_surface_finishes
  , order_metrics.all_secondary_surface_finishes
FROM orders
LEFT JOIN order_metrics
USING (order_id)