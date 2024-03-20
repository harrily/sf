
drop table if exists tmp_ordi_predict.detail_data_460984;
create table tmp_ordi_predict.detail_data_460984
stored as parquet as  
select
  *
from
  (
    select
      *,
      row_number() over(partition by eventtype,tripid,packageno order by eventtime) as rn 
    from
      bdp.dm_freight.op_tp_wl_handover_detail_info
    where
      inc_day between '$[time(yyyyMMdd,-3d)]' and '$[time(yyyyMMdd)]'
  ) t
where t.rn = 1
;


drop table if exists tmp_ordi_predict.tmp_rs1_460984;
create table tmp_ordi_predict.tmp_rs1_460984
stored as parquet as  
select
  eventtype as event_type,
  deptcode as dept_code,
  tripid as trip_id,
  min(eventtime) as event_time,
  sum(packageweight) as weight,
  count(packageno) as ticket
from
  tmp_ordi_predict.detail_data_460984
where
  eventtype = 'ARRIVE_END'
group by
  eventtype,
  deptcode,
  tripid
 ; 



drop table if exists tmp_ordi_predict.tmp_result_no_batch_460984;
create table tmp_ordi_predict.tmp_result_no_batch_460984
stored as parquet as
select
  id_key,
  event_type,
  dept_code,
  FROM_UNIXTIME(int(event_time / 1000)) as event_time,
  trip_id,
  weight,
  ticket,
  if(
    event_type = 'TRANSPORT',
    FROM_UNIXTIME(bigint(bigint(event_time) / 1000) +(60 * 60 * 3)),
    FROM_UNIXTIME(int(event_time / 1000))
  ) as plan_arrive_time,
  if(
    event_type = 'TRANSPORT',
    FROM_UNIXTIME(
      bigint(bigint(event_time) / 1000) +(60 * 60 * 3),
      'yyyy-MM-dd HH:mm'
    ),
    FROM_UNIXTIME(int(event_time / 1000), 'yyyy-MM-dd HH:mm')
  ) as arrTm,
  '$[time(yyyyMMdd)]' as count_date,
  '$[time(yyyy-MM-dd HH:mm:ss)]' as count_time
from
(
	select
	  md5(dept_code) as id_key,
	  event_type,
	  dept_code,
	  trip_id,
	  event_time,
	  weight,
	  ticket
	from (
		select 
		  event_type,
		  dept_code,
		  trip_id,
		  event_time,
		  weight,
		  ticket
		from tmp_ordi_predict.tmp_rs1_460984
		union 
		 select
		  eventtype as event_type,
		  deptcode as dept_code,
		  tripid as trip_id,
		  min(eventtime) as event_time,
		  sum(packageweight) as weight,
		  count(packageno) as ticket
		from
		  (
			select
			  t1.*
			from
			  (
				select
				  *
				from
				  tmp_ordi_predict.detail_data_460984
				where
				  eventtype = 'TRANSPORT'
			  ) t1
			  left join tmp_ordi_predict.tmp_rs1_460984 t2 
			on t1.tripid = t2.trip_id
			where
			  t2.trip_id is null
		  ) t
		group by
		  t.eventtype,
		  t.deptcode,
		  t.tripid
	) r1 
)r
;

set hive.merge.sparkFiles=true;
set mapred.max.split.size=268435456;
set mapred.min.split.size.per.node=268435456;
set mapred.min.split.size.per.rack=268435456;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.exec.reducers.bytes.per.reducer=268435456;
set hive.exec.reducers.max=1099;
set hive.merge.size.per.task=268435456;
set hive.merge.smallfiles.avgsize=134217728;


 insert overwrite table tmp_ordi_predict.tmp_dm_cargo_quantity_handover_dtl_rt partition(inc_day, inc_hour)
select 
	dept_code,
	trip_id,
	if(event_type='TRANSPORT',event_time,null) as depart_time,
	plan_arrive_time,
	if(event_type='ARRIVE_END',event_time,null) as arrive_time,
	if(event_type='ARRIVE_END',event_time,plan_arrive_time) as pre_arrive_time,
	weight,
	ticket,
	if(event_type='ARRIVE_END','3','2') status,
	batch_code,
	batch_date,
	count_time,
	'${incDay}' as inc_day,
	'${incHour}' as inc_hour
from (
	 select
		id_key,event_type,event_time,plan_arrive_time,trip_id,weight,ticket,count_date,dept_code,
		batch_code,
		if(batch_date is not null,date_format(batch_date, 'yyyyMMdd'),batch_date)  as batch_date,count_time
	from
	(select
			id_key,event_type,dept_code,event_time,plan_arrive_time,trip_id,weight,ticket,count_time,count_date,
			max(batch_code) over(partition by trip_id,dept_code,event_type) batch_code,
			max(batch_date) over(partition by trip_id,dept_code,event_type) batch_date,
			row_number() over(partition by trip_id,dept_code,event_type order by plan_arrive_time) rn
	from
		(select
				id_key,event_type,dept_code,event_time,plan_arrive_time,trip_id,weight,ticket,count_time,count_date,arrTm,
				if((b.arrTm>=a.last_last_arrive_tm and b.arrTm<a.last_arrive_tm),batch_code,null) batch_code,
				if((b.arrTm>=a.last_last_arrive_tm and b.arrTm<a.last_arrive_tm),batch_date,null) batch_date
				from tmp_ordi_predict.tmp_result_no_batch_460984 b 
				left join 
				(
					select
					  operate_zone_code,
					  batch_code,
					  batch_date,
					  last_last_arrive_tm,
					  last_arrive_tm
					from
					  ky.dm_heavy_cargo.dm_arrive_batch_info_dtl_di
					where
					  inc_day between '$[time(yyyyMMdd,-4d)]' and '$[time(yyyyMMdd,+5d)]'
				) a
				on b.dept_code=a.operate_zone_code
		)t1
	)t2
	where rn=1
) r 
;