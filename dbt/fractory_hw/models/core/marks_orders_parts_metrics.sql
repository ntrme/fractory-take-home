/*
  Since orders_ext is a core model, we can't make this an int. In the simple case of aggregations
  it's also not pattern breaking design choice. We could rely on stg and int and join them again
  but it would be double-processing for no benefit other than aesthetics :)
*/

WITH parts AS (
  SELECT *
  FROM {{ ref('marks_parts_ext') }}
)
SELECT
  order_id
  , COUNT(DISTINCT order_part_id) unique_parts_count
  , COUNTIF(selected_process_type = 'cnc_machining') cnc_parts_count
  , COUNTIF(selected_process_type = 'laser_cutting') laser_parts_count
  , COUNTIF(has_bending = 1) bending_parts_count
  , COUNTIF(has_surface_coating) surface_coated_parts_count
  , IFNULL(SUM(bends_count), 0) total_bends_count
  , COUNTIF(has_insert_operations) insert_operations_count
  , STRING_AGG(DISTINCT ral_code) all_ral_codes
  , STRING_AGG(DISTINCT ral_finish) all_ral_finishes
  , STRING_AGG(DISTINCT surface_finish) all_surface_finishes
  , STRING_AGG(DISTINCT secondary_surface_finish) all_secondary_surface_finishes
FROM parts
GROUP BY order_id