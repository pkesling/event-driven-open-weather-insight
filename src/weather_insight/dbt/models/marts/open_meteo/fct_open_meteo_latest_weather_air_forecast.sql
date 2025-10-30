{{ config(
    materialized='materialized_view',
    schema='mart'
) }}
WITH latest_weather_forecast AS (
    SELECT DISTINCT ON (w.open_meteo_start_time, w.open_meteo_end_time)
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
     WHERE w.record_type = 'forecast'
     ORDER BY w.open_meteo_start_time, w.open_meteo_end_time, w.ingested_at_dtz
),
latest_air_forecast AS (
    SELECT DISTINCT ON (a.open_meteo_start_time, a.open_meteo_end_time)
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
     WHERE a.record_type = 'forecast'
     ORDER BY a.open_meteo_start_time, a.open_meteo_end_time, a.ingested_at_dtz
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
  FROM latest_weather_forecast w
  JOIN latest_air_forecast a ON w.latitude = a.latitude
                            AND w.longitude = a.longitude
                            AND w.open_meteo_start_time = a.open_meteo_start_time
                            AND w.open_meteo_end_time = a.open_meteo_end_time
