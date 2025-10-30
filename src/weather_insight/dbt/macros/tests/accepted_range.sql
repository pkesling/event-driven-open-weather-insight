{% test accepted_range(model, column_name, min_value=None, max_value=None) %}
with violations as (
    select *
    from {{ model }}
    where 1=1
    {% if min_value is not none %}
      and {{ adapter.quote(column_name) }} < {{ min_value }}
    {% endif %}
    {% if max_value is not none %}
      and {{ adapter.quote(column_name) }} > {{ max_value }}
    {% endif %}
)

select * from violations
{% endtest %}
