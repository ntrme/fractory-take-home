SELECT
  order_part_id
  , process_name
  , process_config
FROM {{ source('fractory', 'parts_surface_finish_config') }}
