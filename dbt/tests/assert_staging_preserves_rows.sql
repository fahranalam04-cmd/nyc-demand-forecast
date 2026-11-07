-- Staging must be a pure rename/cast: same row count as the raw Parquet view.
select src.n as raw_rows, stg.n as staged_rows
from (select count(*) as n from {{ source('raw', 'yellow_tripdata') }}) as src,
     (select count(*) as n from {{ ref('stg_tlc__yellow_trips') }}) as stg
where src.n != stg.n
