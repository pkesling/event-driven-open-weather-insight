-- models/reference/ref_dim_openaq_locations.sql
-- models the ref.dim_openaq_locations table in the warehouse
-- materialized as a 'view' so that any sensitive/unnecessary columns could be excluded in the future
{{ config(
    materialized='view'
) }}

SELECT locations_id_sk,
       openaq_locations_id,
       name,
       locality,
       timezone,
       country_code,
       country_id,
       country_name,
       owner_id,
       owner_name,
       provider_id,
       provider_name,
       latitude,
       longitude,
       first_seen_at_dtz,
       last_seen_at_dtz,
       effective_start_at_dtz,
       effective_end_at_dtz,
       is_current
  FROM ref.dim_openaq_locations
 WHERE is_current = true
