-- models/reference/ref_dim_openaq_sensors.sql
-- models the ref.dim_openaq_sensors table in the warehouse
-- materialized as a 'view' so that any sensitive/unnecessary columns could be excluded in the future
{{ config(
    materialized='view'
) }}

SELECT sensors_id_sk,
       openaq_sensors_id,
       locations_id_sk,
       parameter_name,
       parameter_id,
       parameter_display_name,
       parameter_units,
       last_updated_at_dtz,
       effective_start_at_dtz,
       effective_end_at_dtz,
       is_current
FROM ref.dim_openaq_sensors
WHERE is_current = true
