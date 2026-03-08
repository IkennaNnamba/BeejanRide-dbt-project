{% macro standardize_timestamp(column_name) %}
    timestamp({{ column_name }})
{% endmacro %}