-- Rows removed by each quality rule, per source month. Columns reconcile exactly:
-- rows_raw = removed_duplicate + sum(removed_<rule>) + rows_clean (asserted by a singular test).
{% set rules = ['pickup_outside_file_month', 'invalid_duration', 'negative_amount', 'unknown_pickup_zone', 'implausible_distance'] %}
select
    source_month,
    sum(n_copies)                       as rows_raw,
    sum(n_copies - 1)                   as removed_duplicate,
    {% for r in rules %}
    count(*) filter (where failed_rule = '{{ r }}') as removed_{{ r }},
    {% endfor %}
    count(*) filter (where failed_rule is null)     as rows_clean
from {{ ref('int_trips__validated') }}
group by 1
order by 1
