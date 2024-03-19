set hive.exec.dynamic.partition = true;
set hive.exec.dynamic.partition.mode = nonstrict;
set hive.exec.max.dynamic.partitions.pernode=500;
set hive.exec.max.dynamic.partitions = 1000;
insert overwrite table  ky.dm_heavy_cargo.dm_handover_has_arrive_dtl_di partition (inc_day)
select
        dept_code,trip_id,depart_time,plan_arrive_time,arrive_time,
        pre_arrive_time,
        weight,ticket,
        batch_code,
        batch_date,
        inc_day
	from(
		select
				dept_code,trip_id,depart_time,plan_arrive_time,arrive_time,
				nvl(arrive_time,plan_arrive_time) pre_arrive_time,
				weight,ticket,
				nvl(batch_code,plan_batch_code) batch_code,
				nvl(batch_date,plan_batch_date) batch_date,
				date_format(nvl(arrive_time,plan_arrive_time), 'yyyyMMdd') as inc_day
		from
			(select
					dept_code,trip_id,depart_time,plan_arrive_time,arrive_time,weight,ticket,
					max(plan_batch_code) over(partition by dept_code,trip_id) plan_batch_code,
					max(plan_batch_date) over(partition by dept_code,trip_id) plan_batch_date,
					max(batch_code) over(partition by dept_code,trip_id) batch_code,
					max(batch_date) over(partition by dept_code,trip_id) batch_date,
					row_number() over(partition by dept_code,trip_id) rn,
					inc_day
			from
				(select
						dept_code,trip_id,depart_time,plan_arrive_time,arrive_time,weight,ticket,
						if((b.plan_arr_tm>=a.last_last_arrive_tm and b.plan_arr_tm<a.last_arrive_tm),batch_code,null) plan_batch_code,
						if((b.plan_arr_tm>=a.last_last_arrive_tm and b.plan_arr_tm<a.last_arrive_tm),batch_date,null) plan_batch_date,
						if((b.arr_tm>=a.last_last_arrive_tm and b.arr_tm<a.last_arrive_tm),batch_code,null) batch_code,
						if((b.arr_tm>=a.last_last_arrive_tm and b.arr_tm<a.last_arrive_tm),batch_date,null) batch_date,
						inc_day
				from
					(SELECT dept_code,trip_id,weight,ticket,
							FROM_UNIXTIME(bigint(bigint(depart_time)/1000)) depart_time,
							FROM_UNIXTIME(bigint(bigint(arrive_time)/1000)) arrive_time,
							FROM_UNIXTIME(bigint(bigint(depart_time)/1000)+(60*60*3)) plan_arrive_time,
							FROM_UNIXTIME(bigint(bigint(depart_time)/1000)+(60*60*3),'yyyy-MM-dd HH:mm') plan_arr_tm,
							FROM_UNIXTIME(bigint(bigint(arrive_time)/1000),'yyyy-MM-dd HH:mm') arr_tm,
							inc_day1 as inc_day
						FROM (SELECT
									zone_code as dept_code,
									trip_id,
									min(depart_time) depart_time,
									max(arrive_time) arrive_time, --为空,就标记为event_type='TRANSPORT','ARRIVE_END'
									sum(cast(weight as decimal(16,2))) weight,
									count(*) ticket,
									min(inc_day) inc_day1
								FROM ky.dm_freight.dm_wl_handover_forecast_dtl_df
								where  inc_day  ='$[time(yyyyMMdd,-1d)]'
								and zone_code in (select dept_code from ky.dim_freight.dim_heavy_transi_info)
								group  by  trip_id,zone_code
							)c
					) b
						left join
					(select operate_zone_code,batch_code,batch_date, last_last_arrive_tm, last_arrive_tm
					from ky.dm_heavy_cargo.dm_arrive_batch_info_dtl_di
					where inc_day between '$[time(yyyyMMdd,-2d)]' and '$[time(yyyyMMdd)]'
					   and operate_zone_code in (select dept_code from ky.dim_freight.dim_heavy_transi_info)
					) a
						on b.dept_code=a.operate_zone_code
				)t1
			)t2
		where rn=1
		union 
		select
				dept_code,trip_id,depart_time,plan_arrive_time,arrive_time,
				pre_arrive_time,
				weight,ticket,
				batch_code,
				batch_date,
				inc_day
		from ky.dm_heavy_cargo.dm_handover_has_arrive_dtl_di
		where inc_day between '$[time(yyyyMMdd,-2d)]' and '$[time(yyyyMMdd)]'
)t4
where pre_arrive_time between '$[time(yyyy-MM-dd 00:00:00,-2d)]' and '$[time(yyyy-MM-dd 00:00:00)]'