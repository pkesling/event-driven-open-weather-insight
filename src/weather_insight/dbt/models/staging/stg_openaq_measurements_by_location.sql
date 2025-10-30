-- models/staging/stg_openaq_measurements_by_location.sql
-- models the stg.openaq_measurements_by_location table in the warehouse
-- materialized as a 'view' so that any sensitive/unnecessary columns could be excluded in the future
{{ config(
    materialized='view'
) }}

SELECT
    event_id,
    openaq_locations_id,
    openaq_sensors_id,
    datetime_utc,
    value,
    value_normalized,
    -- convenience alias for downstream models: already filtered for invalid/negative
    value_normalized AS value_clean,
    is_valid,
    quality_status,
    source,
    event_type,
    source_version,
    ingested_at_dtz,
    last_updated_at_dtz
FROM stg.openaq_measurements_by_location
