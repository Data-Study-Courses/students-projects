--группируем сессии по причине остановки
select  stop_session_cause,
		count(session_id) 
from new_project.session_list
group by stop_session_cause
order by count(session_id) desc;

--распределение нулевых сессии по типам электробусов 
select electrobus_type,
		count(pk_id) as session_quantity
from new_project.session_list
where supplied_energy = 0
group by electrobus_type
order by count(pk_id) desc;

--и станциям
select address_id, station_number,
		count(pk_id)
from new_project.session_list
where supplied_energy = 0
group by address_id, station_number 
order by count(pk_id) desc;

--какая станция с самой большой мощностью
select address_id, station_number,
		max(avg_power_session) as max_power,
		min(avg_power_session) as min_power,
		round(avg(avg_power_session)) as avg_power,
		count(pk_id) as count_session
from new_project.session_list
where supplied_energy != 0
group by address_id, station_number
order by avg_power desc;

--самые загруженные дни недели в феврале
select day_of_week,
		count(pk_id) as session_quantity
from new_project.session_list
where date_part('month', start_date) = 2
group by day_of_week
--having station_number = 2743
order by count(pk_id) desc;

--самые загруженные дни недели в марте
select day_of_week,
		count(pk_id) as session_quantity
from new_project.session_list
where date_part('month', start_date) = 3
group by day_of_week
--having station_number = 2743
order by count(pk_id) desc;

--распределение зарядных сессий по часам
select distinct session_hour,
		count(pk_id) as session_quantity
from new_project.session_list
group by session_hour 
order by count(pk_id) desc;

--самая высокопроизводительная станция в феврале
with temp_table0 as
	(select address_id,
			station_number,
			start_date::date as session_date,
			sum(supplied_energy) as sum_energy
	from new_project.session_list
	group by address_id, station_number,
			start_date::date
	order by address_id, station_number,
			start_date::date)

select *,
		max(sum_energy) over (partition by station_number),
		round(avg(sum_energy) over (partition by station_number))
from temp_table0
where date_part('month', session_date) = 2
order by round(avg(sum_energy) over (partition by station_number)) desc;

--самая высокопроизводительная станция в марте
with temp_table0 as
	(select address_id,
			station_number,
			start_date::date as session_date,
			sum(supplied_energy) as sum_energy
	from new_project.session_list
	group by address_id, station_number,
			start_date::date
	order by address_id, station_number,
			start_date::date)

select *,
		max(sum_energy) over (partition by station_number),
		round(avg(sum_energy) over (partition by station_number))
from temp_table0
where date_part('month', session_date) = 3
order by round(avg(sum_energy) over (partition by station_number)) desc;



--рассчет потерь энергии
with temp_table1 as
	(select pk_id,
		address_id,
		station_number,
		start_date::date AS session_date,
		battery_capacity,
		start_battery_charge/100*battery_capacity as battery_start,
		final_battery_charge/100*battery_capacity as battery_final,
		supplied_energy
from new_project.session_list sl
left join new_project.electrobus_type et
on sl.electrobus_type_id = et.electrobus_type_id),

temp_table2 AS (select *,
		battery_final - battery_start as difference,
		supplied_energy - (battery_final - battery_start) as loss_energy
from temp_table1
order by session_date, address_id, station_number, battery_capacity)

SELECT pk_id,
		address_id,
		station_number,
		session_date,
		battery_capacity,
		round(loss_energy/battery_capacity*100) AS percent_loss_energy
FROM temp_table2




--самая часто используемая станция в марте
select address_id, station_number,
		count(pk_id) as count_session
from new_project.session_list
where date_part('month', start_date) = 2
group by address_id, station_number 
order by count(pk_id) desc;
	
--самая часто используемая станция в феврале
select address_id, station_number,
		count(pk_id) as count_session
from new_project.session_list
where date_part('month', start_date) = 3
group by address_id, station_number 
order by count(pk_id) desc;

--время работы и время простоя по датам для каждой площадки и станции

select address_id,
		station_number,
		start_date::date as session_date,
		sum(stop_date - start_date) as working_time,
		'23:59:59' - sum(stop_date - start_date) as down_time
from new_project.session_list
group by address_id, station_number, start_date::date
order by address_id, station_number, session_date;

--необходимо создать витрину для визуализации в Superset

CREATE TABLE new_project.data_mart AS
(SELECT sl.station_number,
		sl.session_id,
		sl.starting,
		sl.ending,
		sl.duration,
		sl.start_date,
		sl.stop_date,
		sl.start_date::date AS session_date,
		w.temperature_mean,
		sl.stop_date - sl.start_date AS session_time,
		sl.electrobus_type_id,
		et.battery_capacity,
		sl.parking_number,
		sl.start_battery_charge,
		sl.final_battery_charge,
		sl.start_battery_charge/100*et.battery_capacity AS battery_start,
		sl.final_battery_charge/100*et.battery_capacity AS battery_final,
		sl.supplied_energy,
		sl.supplied_energy - (sl.final_battery_charge/100*et.battery_capacity - sl.start_battery_charge/100*et.battery_capacity) AS loss_energy,
		round((sl.supplied_energy - (sl.final_battery_charge/100*et.battery_capacity - sl.start_battery_charge/100*et.battery_capacity))/et.battery_capacity*100) AS percent_loss_energy,
		sl.avg_power_session,
		sl.cause_id,
		ssc.stop_session_cause,
		sl.session_hour,
		sl.day_of_week,
		sl.address_id,
		sl.pk_id
FROM new_project.session_list sl
LEFT JOIN new_project.electrobus_type et
ON sl.electrobus_type_id = et.electrobus_type_id 
LEFT JOIN new_project.stop_session_cause ssc
ON sl.cause_id = ssc.cause_id 
LEFT JOIN new_project.weather w 
ON sl.start_date::date = w.start_date);
