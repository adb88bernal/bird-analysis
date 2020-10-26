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

	, ride_triggered_maintenance
	, ride_neighborhood
	, rider_age
	, bird_model_id
	, bird_id
from sample_3
)

, agg_stats as (
select
	real_start_time_date
	, count(bird_id) as cnt_of_rides
from 
	updated_data
group by 1
order by 1
)

select
	round(avg(cnt_of_rides),2) as avg_rides_per_day
from 
	agg_stats
