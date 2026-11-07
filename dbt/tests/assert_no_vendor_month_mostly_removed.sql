-- A quality rule that deletes most of one vendor's records is almost always a reporting quirk
-- being mistaken for bad data (this is how vendor 7's dropoff = pickup convention was found).
-- Fails if any vendor-month with >= 10k raw rows loses more than half of them.
select
    source_month,
    vendor_id,
    sum(n_copies) as rows_raw,
    sum(n_copies) filter (where failed_rule is not null) + sum(n_copies - 1) filter (where failed_rule is null) as rows_removed
from {{ ref('int_trips__validated') }}
group by 1, 2
having sum(n_copies) >= 10000
   and rows_removed > 0.5 * sum(n_copies)
