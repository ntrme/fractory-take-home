
/*
  Quick note on why this solution. We could just do the extraction and IF/case-when in our core, extended model.
  However, that's just overcomplicating the core model, which is supposed to bring things together and contain
  as little "tech logic" as possible.

  Similarly, breaking down finishes into own CTEs makes our approach modular. What if we want to add complexity by
  adding more surface finish options? Or a json value changes and ralCode starts to arrive also as RALCODE?
  This way we can also add incrementality, if needed, and even break out CTEs into their own models, should the
  complexity increase.
*/

WITH parsed_surface_finish AS (
  SELECT
    order_part_id
    , process_name
    , PARSE_JSON(process_config) process_config_json
  FROM {{ ref('marks_stg_parts_surface_finish_config') }}
)
, distinct_keys AS ( -- this avoids duplication in final block and eases off join.
  SELECT DISTINCT
    order_part_id
  FROM parsed_surface_finish
)
, surface_finish AS (
  SELECT
    order_part_id
    , JSON_VALUE(process_config_json.value) AS surface_finish
  FROM parsed_surface_finish
  WHERE process_name = 'SURFACE_FINISH'
)
, secondary_surface_finish AS (
  SELECT
    order_part_id
    , JSON_VALUE(process_config_json.value) AS secondary_surface_finish
  FROM parsed_surface_finish
  WHERE process_name = 'SECONDARY_SURFACE_FINISH'
)
, secondary_surface_finish_ral AS (
  SELECT
    order_part_id
    , JSON_VALUE(process_config_json.ralCode) ral_code
    , JSON_VALUE(process_config_json.ralFinish) ral_finish
  FROM parsed_surface_finish
  WHERE process_name = 'SECONDARY_SURFACE_FINISH_RAL'
)
SELECT DISTINCT
  distinct_keys.order_part_id
  , surface_finish.surface_finish
  , secondary_surface_finish.secondary_surface_finish
  , secondary_surface_finish_ral.ral_finish
  , secondary_surface_finish_ral.ral_code
FROM distinct_keys
-- if we assume that we can't have secondary without primary finish, we could use inner join for surface_finish
-- though left join would non-violently catch any DQ issues
LEFT JOIN surface_finish
  USING (order_part_id)
LEFT JOIN secondary_surface_finish
  USING (order_part_id)
LEFT JOIN secondary_surface_finish_ral
  USING (order_part_id)
