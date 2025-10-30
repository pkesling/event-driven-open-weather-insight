{{ config(
    materialized='materialized_view',
    schema='mart'
) }}

WITH airnow AS (
     SELECT a.event_id,
            a.source,
            a.date_issue_dtz,
            a.date_forecast_dtz,
            a.reporting_area,
            a.state_code,
            a.latitude,
            a.longitude,
            a.parameter_name,
            a.aqi_clean AS aqi,
            CASE
                WHEN a.aqi_clean = '-1'::integer THEN
                CASE
                    WHEN a.category_number = 1 THEN 25
                    WHEN a.category_number = 2 THEN 75
                    WHEN a.category_number = 3 THEN 125
                    WHEN a.category_number = 4 THEN 175
                    WHEN a.category_number = 5 THEN 150
                    WHEN a.category_number = 6 THEN 301
                    ELSE NULL::integer
                END
                ELSE a.aqi_clean
            END AS adjusted_aqi,
            a.category_name,
            a.action_day,
            a.discussion
       FROM {{ ref('stg_airnow_air_quality_forecast') }} a
      WHERE a.is_current = true
        AND a.is_valid = true
        AND (a.date_forecast_dtz = date_trunc('day'::text, now())
                 OR a.date_forecast_dtz = date_trunc('day'::text, now()) + interval '1 day')
),
openaq AS (
     SELECT a.openaq_locations_id,
            a.latest_temperature_sensor_id,
            a.measurement_datetime_utc,
            EXTRACT(epoch FROM a.measurement_datetime_utc - now())::bigint AS latest_measurement_age_in_seconds,
            a.latest_temperature,
            a.latest_temperature_units,
            a.latest_pm25_sensor_id,
            a.latest_pm25,
            a.latest_pm25_units,
            a.latest_relative_humidity_sensor_id,
            a.latest_relative_humidity,
            a.latest_relative_humidity_units
       FROM (
            SELECT f.openaq_locations_id,
                   f.measurement_datetime_utc,
                   max(f.measurement_value) FILTER (WHERE f.parameter_name = 'temperature'::text) AS latest_temperature,
                   max(f.parameter_units) FILTER (WHERE f.parameter_name = 'temperature'::text) AS latest_temperature_units,
                   max(f.openaq_sensors_id) FILTER (WHERE f.parameter_name = 'temperature'::text) AS latest_temperature_sensor_id,
                   max(f.measurement_value) FILTER (WHERE f.parameter_name = 'pm25'::text) AS latest_pm25,
                   max(f.parameter_units) FILTER (WHERE f.parameter_name = 'pm25'::text) AS latest_pm25_units,
                   max(f.openaq_sensors_id) FILTER (WHERE f.parameter_name = 'pm25'::text) AS latest_pm25_sensor_id,
                   max(f.measurement_value) FILTER (WHERE f.parameter_name = 'relativehumidity'::text) AS latest_relative_humidity,
                   max(f.parameter_units) FILTER (WHERE f.parameter_name = 'relativehumidity'::text) AS latest_relative_humidity_units,
                   max(f.openaq_sensors_id) FILTER (WHERE f.parameter_name = 'relativehumidity'::text) AS latest_relative_humidity_sensor_id,
                   date_trunc('hour'::text, f.measurement_datetime_utc) AS measurement_datetime_utc_hour
               FROM {{ ref('fct_openaq_latest_by_location') }} f
              WHERE date_trunc('hour'::text, f.measurement_datetime_utc) > (date_trunc('hour'::text, now()) - '02:00:00'::interval)
              GROUP BY f.openaq_locations_id, f.measurement_datetime_utc) a
),
weather AS (
     SELECT f.event_id,
            f.event_type,
            f.source,
            f.office,
            f.grid_id,
            f.grid_x,
            f.grid_y,
            f.start_time,
            f.end_time,
            f.is_daytime,
            f.temperature_clean AS temperature,
            f.temperature_unit,
            f.temperature_trend,
            f.relative_humidity_clean AS relative_humidity,
            f.relative_humidity_unit,
            f.dewpoint,
            f.dewpoint_unit,
            f.wind_speed_clean AS wind_speed,
            f.wind_speed_unit,
            f.wind_direction,
            f.wind_gust,
            f.wind_gust_unit,
            f.probability_of_precipitation_clean AS probability_of_precipitation,
            f.probability_of_precipitation_unit,
            f.short_forecast,
            f.detailed_forecast,
            date_trunc('day'::text, f.start_time) AS forecast_day_dtz
       FROM {{ ref('stg_weather_hourly_forecast') }} f
      WHERE f.start_time >= now()
        AND f.is_valid = true
        AND f.event_type = 'nws.hourly_forecast'::text
)
SELECT now() AS current_datetime_utc,
       w.start_time AS forecast_start_time_utc,
       w.end_time AS forecast_end_time_utc,
       o.measurement_datetime_utc AS latest_measurement_datetime_utc,
       'latest'::text AS openaq_measurement_scope,
       'daily'::text AS air_quality_forecast_scope,
       'hourly'::text AS weather_forecast_scope,
       o.latest_temperature AS latest_measured_temperature,
       o.latest_temperature_units,
       o.latest_pm25 AS latest_measured_pm25,
       o.latest_pm25_units,
       o.latest_relative_humidity AS latest_measured_relative_humidity,
       o.latest_relative_humidity_units,
       a.parameter_name AS air_quality_parameter,
       a.aqi,
       a.adjusted_aqi,
       a.category_name AS air_quality_category,
       w.temperature,
       w.temperature_unit,
       w.temperature_trend,
       w.relative_humidity,
       w.relative_humidity_unit,
       w.dewpoint,
       w.dewpoint_unit,
       w.wind_speed,
       w.wind_speed_unit,
       w.wind_direction,
       w.wind_gust,
       w.wind_gust_unit,
       w.probability_of_precipitation,
       w.probability_of_precipitation_unit,
       a.action_day AS air_quality_action_day,
       a.discussion AS air_quality_discussion,
       w.short_forecast,
       w.detailed_forecast,
       w.forecast_day_dtz
   FROM airnow a
   JOIN weather w ON a.date_forecast_dtz = w.forecast_day_dtz
  CROSS JOIN openaq o
  ORDER BY w.start_time
