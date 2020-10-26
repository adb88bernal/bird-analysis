with updated_data_1 as (
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

, updated_data_2 as (
	select a.*
			, case when a.end_ride_day_section in ('0 Late Night', '1 Early Morning') then '1'
					when a.end_ride_day_section in ('2 Morning', '3 Afternoon') then '2'
					when a.end_ride_day_section in ('4 Evening', '5 Night') then '3'
		else '0' end as shift
	from updated_data_1 a
)

, agg_maint_stats_neighbor as (
select end_ride_day_section
		, ride_neighborhood
		, real_end_time_date
		, shift
		, count(ride_triggered_maintenance) as count_of_birds
from updated_data_2
where ride_triggered_maintenance = 'TRUE'
group by 1,2,3,4 
order by 2,3,1
)

select 
	end_ride_day_section
	, round(avg(count_of_birds))
	, round(avg(count_of_birds)/4) as avg_bird_maintence_hourly
from agg_maint_stats_neighbor
group by 1
order by 1

