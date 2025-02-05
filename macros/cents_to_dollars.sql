{% macro cents_to_dollars(column, precision = 2) -%}
    round({{ column }} / 100.0, {{ precision }})
{% endmacro %}