{#- Convert a naive UTC timestamp to naive America/New_York wall-clock time (the TLC convention).
    Open-Meteo responses use one fixed UTC offset per request, which is wrong on one side of every
    DST change, so we always request GMT and convert here with the ICU time-zone database. -#}
{% macro to_nyc_local(ts) -%}
    cast(timezone('America/New_York', timezone('UTC', {{ ts }})) as timestamp)
{%- endmacro %}
