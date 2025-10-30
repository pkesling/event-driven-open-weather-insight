{{ config(materialized='view') }}

select
    event_id,
    source,
    observed_at,
    metric_value,
    metric_unit
from stg.your_source
