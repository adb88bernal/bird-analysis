with updated_data as (
select
	rider_id
	, ride_rating
	, start_odometer
	, to_timestamp(start_timestamp) as real_start_time_full
	, to_timestamp(start_timestamp)::date as real_start_time_date
	, end_odometer
	, to_timestamp(end_timestamp) as real_end_time_full
	, to_timestamp(end_timestamp)::date as real_end_time_date
	, case when to_timestamp(end_timestamp)::time between cast('00:00:00' as time) and cast('03:59:59' as time) then '0 Late Night'
			when to_timestamp(end_timestamp)::time between cast('04:00:00' as time) and cast('07:59:59' as time) then '1 Early Morning'
			when to_timestamp(end_timestamp)::time between cast('08:00:00' as time) and cast('11:59:59' as time) then '2 Morning'
			when to_timestamp(end_timestamp)::time between cast('12:00:00' as time) and cast('15:59:59' as time) then '3 Afternoon'
			when to_timestamp(end_timestamp)::time between cast('16:00:00' as time) and cast('19:59:59' as time) then '4 Evening'
			when to_timestamp(end_timestamp)::time between cast('20:00:00' as time) and cast('23:59:59' as time) then '5 Night'
		else 'unknown' end as end_ride_day_section
	, ride_triggered_maintenance
	, ride_neighborhood
	, rider_age
	, bird_model_id
	, bird_id
from sample_3
)
, time_data as (
select
	bird_id
	, ride_neighborhood
	, real_start_time_full
	, real_end_time_full
	, end_odometer
	, ride_triggered_maintenance
	, lag(real_end_time_full,1) over (partition BY bird_id ORDER BY real_end_time_full) AS previous_end_time
	, lag(ride_triggered_maintenance,1) over (partition BY bird_id ORDER BY real_end_time_full) AS previous_trigger
	, real_start_time_full - lag(real_end_time_full) over (partition BY bird_id ORDER BY real_end_time_full) as downtime
from 
	updated_data
)


select
	ride_neighborhood
	, avg(downtime) as avg_downtime
from
	time_data
where previous_trigger = 'TRUE'
group by 1


