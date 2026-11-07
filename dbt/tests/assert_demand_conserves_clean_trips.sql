-- Densifying to the zone x hour grid must neither drop nor invent trips.
select f.n as fact_pickups, c.n as clean_trips
from (select sum(pickups) as n from {{ ref('fct_hourly_zone_demand') }}) as f,
     (select count(*) as n from {{ ref('int_trips__clean') }}) as c
where f.n != c.n
