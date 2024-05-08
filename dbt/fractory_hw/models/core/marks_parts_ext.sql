
WITH stg_parts AS (
  SELECT *
  FROM {{ ref('marks_stg_parts') }}
)
, int_surface_finish AS (
  SELECT *
  FROM {{ ref('marks_int_surface_finish') }}
)
SELECT
  stg_parts.*
  , int_surface_finish.surface_finish
  , int_surface_finish.secondary_surface_finish
  , int_surface_finish.ral_code
  , int_surface_finish.ral_finish
FROM stg_parts
LEFT JOIN int_surface_finish
  USING (order_part_id)