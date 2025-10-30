-- models/reference/ref_openaq_locations_license_bridge.sql
-- models the ref.openaq_locations_license_bridgetable in the warehouse
-- materialized as a 'view' so that any sensitive/unnecessary columns could be excluded in the future
{{ config(
    materialized='view'
) }}

SELECT bridge_id,
       locations_id_sk,
       license_id_sk,
       license_attribution_name,
       license_attribution_url,
       license_date_from_dtz,
       license_date_to_dtz,
       last_updated_at_dtz,
       effective_start_at_dtz,
       effective_end_at_dtz,
       is_current
FROM ref.openaq_locations_license_bridge
WHERE is_current = true
