-- models/marts/air_quality/fct_openaq_latest_by_location.sql
-- view returning information about the latest openaq data for a location
{{ config(
    materialized='materialized_view',
    schema='mart'
) }}

{% set latest_event_type = var(
    'openaq_latest_event_type',
    env_var('OPENAQ_LATEST_MEASURES_EVENT_TYPE') or 'openaq.locations.latest'
) %}

WITH latest_measurements AS
(
    SELECT event_id,
           openaq_locations_id,
           openaq_sensors_id,
           datetime_utc,
           value AS value_raw,
           value_clean,
           is_valid,
           source,
           event_type,
           source_version,
           ingested_at_dtz,
           last_updated_at_dtz,
           latest_datetime
    FROM (
        SELECT event_id,
               openaq_locations_id,
               openaq_sensors_id,
               datetime_utc,
               value,
               value_clean,
               is_valid,
               source,
               event_type,
               source_version,
               ingested_at_dtz,
               last_updated_at_dtz,
               MAX(datetime_utc) OVER (PARTITION BY openaq_locations_id, openaq_sensors_id) AS latest_datetime
          FROM {{ ref('stg_openaq_measurements_by_location') }} m
         WHERE event_type = '{{ latest_event_type }}'
      ) a
     WHERE datetime_utc = latest_datetime
       AND datetime_utc >= now() - INTERVAL '1 day'
)
SELECT lm.event_id,
       lm.openaq_locations_id,
       lm.openaq_sensors_id,
       lm.datetime_utc AS measurement_datetime_utc,
       lm.value_clean AS measurement_value,
       lm.value_raw AS measurement_value_raw,
       s.parameter_name,
       s.parameter_display_name,
       s.parameter_units,
       l.name AS location_name,
       l.country_code,
       l.country_name,
       l.owner_name,
       l.provider_name,
       l.latitude,
       l.longitude,
       l2.name AS license_name,
       b.license_attribution_name,
       b.license_attribution_url,
       b.license_date_from_dtz,
       b.license_date_to_dtz,
       l2.commercial_use_allowed AS license_commercial_use_allowed,
       l2.attribution_required AS license_attribution_required,
       l2.share_alike_required AS license_share_alike_required,
       l2.modification_allowed AS license_modification_allowed,
       l2.redistribution_allowed AS license_redistribution_allowed,
       l2.source_url AS license_source_url
  FROM latest_measurements lm
  JOIN {{ ref('ref_dim_openaq_locations') }} l ON l.openaq_locations_id = lm.openaq_locations_id AND l.is_current
  JOIN {{ ref('ref_dim_openaq_sensors') }} s ON lm.openaq_sensors_id = s.openaq_sensors_id AND s.is_current
  JOIN {{ ref('ref_openaq_locations_license_bridge') }} b ON l.locations_id_sk = b.locations_id_sk AND b.is_current
  JOIN {{ ref('ref_dim_openaq_licenses') }} l2 ON l2.license_id_sk = b.license_id_sk AND l2.is_current
 WHERE lm.is_valid = true
