-- One row per raw trip record: renamed, typed, and tagged with the month of the file it came
-- from. No rows are removed here; every filter lives in int_trips__validated so it can be counted.
select
    cast(VendorID as integer)                     as vendor_id,
    cast(tpep_pickup_datetime as timestamp)       as pickup_at,
    cast(tpep_dropoff_datetime as timestamp)      as dropoff_at,
    cast(passenger_count as integer)              as passenger_count,
    cast(trip_distance as double)                 as trip_distance_mi,
    cast(PULocationID as integer)                 as pickup_zone_id,
    cast(DOLocationID as integer)                 as dropoff_zone_id,
    cast(payment_type as integer)                 as payment_type,
    cast(fare_amount as double)                   as fare_amount,
    cast(total_amount as double)                  as total_amount,
    cast(strptime(regexp_extract(filename, '(\d{4}-\d{2})\.parquet$', 1), '%Y-%m') as date)
                                                  as source_month
from {{ source('raw', 'yellow_tripdata') }}
