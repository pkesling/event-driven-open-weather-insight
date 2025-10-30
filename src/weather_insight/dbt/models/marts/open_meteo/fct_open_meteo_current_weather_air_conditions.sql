{{ config(
    materialized='materialized_view',
    schema='mart'
) }}
WITH current_weather_forecast AS (
    SELECT
        event_id,
        open_meteo_start_time,
        open_meteo_end_time,
        latitude,
        longitude,
        temperature,
        temperature_unit,
        rel_humidity,
        rel_humidity_unit,
        dew_point,
        dew_point_unit,
        apparent_temperature,
        apparent_temperature_unit,
        precipitation_prob,
        precipitation_prob_unit,
        precipitation,
        precipitation_unit,
        cloud_cover,
        cloud_cover_unit,
        wind_speed,
        wind_speed_unit,
        wind_gusts,
        wind_gusts_unit,
        wind_direction,
        wind_direction_unit,
        is_day
      FROM stg.open_meteo_weather w
     WHERE w.record_type = 'current'
     ORDER BY w.ingested_at_dtz DESC
     LIMIT 1
),
current_air_forecast AS (
    SELECT
        event_id,
        open_meteo_start_time,
        open_meteo_end_time,
        latitude,
        longitude,
        pm2_5,
        pm2_5_unit,
        pm10,
        pm10_unit,
        us_aqi,
        us_aqi_unit,
        ozone,
        ozone_unit,
        uv_index,
        uv_index_unit
      FROM stg.open_meteo_air a
     WHERE a.record_type = 'current'
     ORDER BY a.ingested_at_dtz DESC
     LIMIT 1
)
SELECT w.event_id AS weather_event_id,
       a.event_id AS air_event_id,
       w.open_meteo_start_time,
       w.open_meteo_end_time,
       w.latitude,
       w.longitude,
       temperature,
       temperature_unit,
       rel_humidity,
       rel_humidity_unit,
       dew_point,
       dew_point_unit,
       apparent_temperature,
       apparent_temperature_unit,
       precipitation_prob,
       precipitation_prob_unit,
       precipitation,
       precipitation_unit,
       cloud_cover,
       cloud_cover_unit,
       wind_speed,
       wind_speed_unit,
       wind_gusts,
       wind_gusts_unit,
       wind_direction,
       wind_direction_unit,
       is_day,
       pm2_5,
       pm2_5_unit,
       pm10,
       pm10_unit,
       us_aqi,
       us_aqi_unit,
       ozone,
       ozone_unit,
       uv_index,
       uv_index_unit
  FROM current_weather_forecast w
  JOIN current_air_forecast a ON w.latitude = a.latitude
                             AND w.longitude = a.longitude

