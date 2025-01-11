with src as (select *,
date_trunc('hour', event_timestamp) 
    + date_part('minute', event_timestamp)::int / 5 * interval '5 min' as event_window_timestamp
from processed_events pe limit 1000)

, processed_events_ip_aggregated_source_cpy as (
select event_window_timestamp, host, ip, count(1) as num_hits
from src
group by 1,2,3
)


-- Q1: Average number of request on bootcamp.techcreator.io is 2.9 in a window
-- and on www.techcreator.io is 1.5
, q1 as (

select host, avg(num_hits) as avg_num_hits from processed_events_ip_aggregated_source_cpy
where host like '%techcreator.io'
group by host
)

-- Q2: I didnt find any traffic on these websites 'zachwilson.techcreator.io', 'zachwilson.tech', 'lulu.techcreator.io' but here is the request
-- that would give the answer
, q2 as (
select host, avg(num_hits) as avg_num_hits from processed_events_ip_aggregated_source_cpy
where host in ('zachwilson.techcreator.io', 'zachwilson.tech', 'lulu.techcreator.io')
group by host

)

select * from q1