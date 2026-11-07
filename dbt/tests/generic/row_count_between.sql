{#- Volume check: the relation's row count must fall within [min_rows, max_rows]. Catches empty
    loads, silently truncated files, and accidental fan-out joins. -#}
{% test row_count_between(model, min_rows=0, max_rows=none) %}
select n_rows
from (select count(*) as n_rows from {{ model }}) as t
where n_rows < {{ min_rows }}
{% if max_rows is not none %}
   or n_rows > {{ max_rows }}
{% endif %}
{% endtest %}
