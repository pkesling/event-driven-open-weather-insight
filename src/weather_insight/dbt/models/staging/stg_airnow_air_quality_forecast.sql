{{ config(
    materialized='view'
) }}

WITH remediated AS (
        SELECT
        event_id,
        source,
        date_issue_dtz,
        date_forecast_dtz,
        reporting_area,
        state_code,
        latitude,
        longitude,
        parameter_name,
        aqi,
        category_number,
        category_name,
        action_day,
        discussion,
        raw_data,
        effective_start_at_dtz,
        effective_end_at_dtz,
        is_current,
        CASE
            WHEN aqi BETWEEN 0 AND 500 THEN aqi
            ELSE
            -- If AQI is out of range, but AQI category was provided, set it to the mid-value of the aqi category range
                CASE
                    WHEN category_number = 1 THEN 25
                    WHEN category_number = 2 THEN 75
                    WHEN category_number = 3 THEN 125
                    WHEN category_number = 4 THEN 175
                    WHEN category_number = 5 THEN 250
                    WHEN category_number = 6 THEN 400
                    ELSE NULL::integer
                END
            END AS aqi_clean
    FROM stg.airnow_air_quality_forecast
)
SELECT event_id,
       source,
       date_issue_dtz,
       date_forecast_dtz,
       reporting_area,
       state_code,
       latitude,
       longitude,
       parameter_name,
       aqi,
       category_number,
       category_name,
       action_day,
       discussion,
       raw_data,
       effective_start_at_dtz,
       effective_end_at_dtz,
       is_current,
       aqi_clean,
        CASE
            WHEN aqi_clean IS NOT NULL AND (aqi_clean < 0 OR aqi_clean > 500) THEN 'aqi_out_of_range'
            ELSE NULL
        END AS quality_status,
        CASE
            WHEN aqi_clean IS NOT NULL AND (aqi_clean < 0 OR aqi_clean > 500) THEN FALSE
            ELSE TRUE
        END AS is_valid
  FROM remediated
 WHERE is_current = true
