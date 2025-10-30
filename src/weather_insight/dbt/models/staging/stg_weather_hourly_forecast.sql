{{ config(
    materialized='view'
) }}

SELECT
    event_id,
    event_type,
    source,
    office,
    grid_id,
    grid_x,
    grid_y,
    start_time,
    end_time,
    is_daytime,
    temperature,
    temperature_min,
    temperature_max,
    temperature_unit,
    temperature_trend,
    relative_humidity,
    relative_humidity_min,
    relative_humidity_max,
    relative_humidity_unit,
    dewpoint,
    dewpoint_min,
    dewpoint_max,
    dewpoint_unit,
    wind_speed,
    wind_speed_min,
    wind_speed_max,
    wind_speed_unit,
    wind_direction,
    wind_gust,
    wind_gust_min,
    wind_gust_max,
    wind_gust_unit,
    probability_of_precipitation,
    probability_of_precipitation_min,
    probability_of_precipitation_max,
    probability_of_precipitation_unit,
    short_forecast,
    detailed_forecast,
    ingested_at_dtz,
    last_updated_at_dtz,
    -- cleaned/validated fields (NULL when out of range)
    CASE WHEN temperature BETWEEN -80 AND 80 THEN temperature ELSE NULL END AS temperature_clean,
    CASE WHEN relative_humidity BETWEEN 0 AND 100 THEN relative_humidity ELSE NULL END AS relative_humidity_clean,
    CASE WHEN probability_of_precipitation BETWEEN 0 AND 100 THEN probability_of_precipitation ELSE NULL END AS probability_of_precipitation_clean,
    CASE WHEN wind_speed >= 0 AND wind_speed <= 300 THEN wind_speed ELSE NULL END AS wind_speed_clean,
    -- status/flags
    CASE
        WHEN temperature IS NOT NULL AND (temperature < -80 OR temperature > 80) THEN 'temperature_out_of_range'
        WHEN relative_humidity IS NOT NULL AND (relative_humidity < 0 OR relative_humidity > 100) THEN 'humidity_out_of_range'
        WHEN probability_of_precipitation IS NOT NULL AND (probability_of_precipitation < 0 OR probability_of_precipitation > 100) THEN 'precip_probability_out_of_range'
        WHEN wind_speed IS NOT NULL AND (wind_speed < 0 OR wind_speed > 300) THEN 'wind_speed_out_of_range'
        ELSE NULL
    END AS quality_status,
    CASE
        WHEN temperature IS NOT NULL AND (temperature < -80 OR temperature > 80) THEN FALSE
        WHEN relative_humidity IS NOT NULL AND (relative_humidity < 0 OR relative_humidity > 100) THEN FALSE
        WHEN probability_of_precipitation IS NOT NULL AND (probability_of_precipitation < 0 OR probability_of_precipitation > 100) THEN FALSE
        WHEN wind_speed IS NOT NULL AND (wind_speed < 0 OR wind_speed > 300) THEN FALSE
        ELSE TRUE
    END AS is_valid
FROM stg.weather_hourly_forecast
