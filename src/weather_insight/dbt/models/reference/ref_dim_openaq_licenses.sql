-- models/reference/ref_dim_openaq_licenses.sql
-- models the ref.dim_openaq_licenses table in the warehouse
-- materialized as a 'view' so that any sensitive/unnecessary columns could be excluded in the future
{{ config(
    materialized='view'
) }}

SELECT license_id_sk,
       openaq_license_id,
       name,
       commercial_use_allowed,
       attribution_required,
       share_alike_required,
       modification_allowed,
       redistribution_allowed,
       source_url,
       last_updated_at_dtz,
       effective_start_at_dtz,
       effective_end_at_dtz,
       is_current
FROM ref.dim_openaq_licenses
WHERE is_current = true
