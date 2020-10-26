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
	, case when end_odometer between 0 and 499.99 then '0 Less Than 500'
			when end_odometer between 500 and 1099.99 then '1 Between 500 - 1,100'
			when end_odometer between 1100 and 1649.99 then '2 Between 1,100 and 1,650'
			when end_odometer between 1650 and 2199.99 then '3  Between 1,650 and 2,200'
			when end_odometer between 2200 and 2749.99 then '4  Between 2,200 and 2,750'
			when end_odometer between 2750 and 3300 then '5 More Than 2,750'
		else 'Unknown' end as end_odometer_range
	, ride_triggered_maintenance
	, ride_neighborhood
	, rider_age
	, bird_model_id
	, bird_id
from sample_3
)


select
	a.end_odometer_range
	, a.bird_model_id
	, a.count_of_maintenances
	, b.count_of_non_maintenances
	, round((a.count_of_maintenances / sum(a.count_of_maintenances + b.count_of_non_maintenances))*100, 2) as pct_of_maintenance_trigger
from 
	(
	select
		end_odometer_range
		, bird_model_id
		, count(bird_id) as count_of_maintenances
	from 
		updated_data
	where ride_triggered_maintenance = 'TRUE' and bird_model_id = 2
	group by 1, 2
	order by 1, 2
	) a 
	join 
	(
	select
		end_odometer_range
		, bird_model_id
		, count(bird_id) as count_of_non_maintenances
	from 
		updated_data
	where ride_triggered_maintenance = 'FALSE' and bird_model_id = 2
	group by 1, 2
	order by 1, 2
	) b 
	on a.end_odometer_range = b.end_odometer_range
group by 1,2,3,4
order by 1

