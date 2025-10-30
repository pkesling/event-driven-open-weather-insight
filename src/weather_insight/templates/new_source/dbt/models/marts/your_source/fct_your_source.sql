{{ config(materialized='view', schema='mart') }}

select
    event_id,
    source,
    observed_at,
    metric_value,
    metric_unit
from {{ ref('stg_your_source') }}
