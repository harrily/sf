
-- 读取hbase处理后的数据 , 计算在途

drop table if exists tmp_ordi_predict.tmp_in_road_cargo_2_470818;
create table tmp_ordi_predict.tmp_in_road_cargo_2_470818
stored as parquet as  
select 
  requireId,
  carNo,
  transLevel,
  carStatus,
  srcZoneCode,
  preArriveTm,
  preArriveZoneCode,
  tickets,
  weight,
  status,
  countTime,
  countDate
from (
	select
	  requireId,
	  "" as carNo,
	  transLevel,
	  carStatus,
	  srcZoneCode,
	  case
		when b.prologis_in_tm is null then preArriveTm
		else unix_timestamp(b.prologis_in_tm, 'yyyy-MM-dd HH:mm:ss') * 1000
	  end as preArriveTm,
	  preArriveZoneCode,
	  tickets,
	  weight,
	  2 as status,
	 '$[time(yyyy-MM-dd)]' as countTime,	
	 '$[time(yyyy-MM-dd HH:mm:ss)]' as countDate
	from
	(
	select
	  *
	from
	  (
		select
		  requireId,
		  transLevel,
		  carStatus,
		  srcZoneCode,
		  preArriveTm,
		  preArriveZoneCode,
		  sum(tickets) over(partition by requireId, preArriveZoneCode) as tickets,
		  sum(weight) over(partition by requireId, preArriveZoneCode) as weight,
		  row_number() over(partition by requireId,preArriveZoneCode order by preArriveTm desc) as num
		from
			tmp_ordi_predict.tmp_ky_all_car_nos_behind_hbase_470818
		  ) t
		where
		  t.num = 1
	) a
	  left join 
	(
		--  获取快管修改车辆预计到达时间
		select require_id,zone_code,next_zone_code,prologis_in_tm 
			from tmp_ordi_predict.sdmti_t_transportct_info_hf
		where inc_day = '20230925' and inc_hour = '1400'
		and prologis_in_tm is not null
	) b on a.requireId = b.require_id
	and a.srcZoneCode = b.zone_code
	and a.preArriveZoneCode = b.next_zone_code
union 
	select 
	  requireId,
	  carNo,
	  transLevel,
	  carStatus,
	  srcZoneCode,
	  preArriveTm,
	  preArriveZoneCode,
	  tickets,
	  weight,
	  status,
	  countTime,
	  countDate
	from 
	(	
		select
		  requireId,
		  "" as carNo,
		  translevel as transLevel,
		  carStatus,
		  srcZoneCode,
		  case
			when b.prologis_in_tm is null then preArriveTm
			else unix_timestamp(b.prologis_in_tm, 'yyyy-MM-dd HH:mm:ss') * 1000
		  end as preArriveTm,
		  preArriveZoneCode,
		  0 as tickets,
		  0 as weight,
		  2 as status,
		  '$[time(yyyy-MM-dd)]' as countTime,	
            '$[time(yyyy-MM-dd HH:mm:ss)]' as countDate,
		  row_number() over(partition by requireId,srcZoneCode,preArriveZoneCode) as rn
		from
			tmp_ordi_predict.tmp_ky_not_car_nos_470818 a
		left join 
		(
			--  获取快管修改车辆预计到达时间
			select require_id,zone_code,next_zone_code,prologis_in_tm 
				from tmp_ordi_predict.sdmti_t_transportct_info_hf        	-- mysql同步hive 
			where inc_day = '20230925' and inc_hour = '1400'
			and prologis_in_tm is not null
		)  b on a.requireId = b.require_id
		and a.srcZoneCode = b.zone_code
		and a.preArriveZoneCode = b.next_zone_code
	) al where al.rn = 1 
) r where r.srcZoneCode != preArriveZoneCode
;


-- 4、开始计算已到达
drop table if exists tmp_ordi_predict.dmpdsp_vt_has_arrive_cars_hf_tmp1_470818;
create table tmp_ordi_predict.dmpdsp_vt_has_arrive_cars_hf_tmp1_470818
stored as parquet as  
select * from tmp_ordi_predict.dmpdsp_vt_has_arrive_cars_hf	-- mysql同步hive 
where 
    -- inc_day = '$[time(yyyyMMdd)]'  -- 指定T-1D分区数据
    inc_day = '20230925'    -- 手动指定
	and inc_hour = '1500'    -- 手动指定
	and actualTime > unix_timestamp('$[time(yyyy-MM-dd HH:mm:ss,-3d)]') * 1000   -- 分钟/秒-固定   T-3D
	and actualTime <= unix_timestamp('$[time(yyyy-MM-dd HH:mm:ss)]') * 1000      -- 分钟/秒-固定   T-D
;

drop table if exists tmp_ordi_predict.has_arrive_toal_tmp_1_470818;
create table tmp_ordi_predict.has_arrive_toal_tmp_1_470818
stored as parquet as  
select
	requireId,
	"" as carNo,
	transLevel,
	carStatus,
	srcZoneCode,
	preArriveTm,
	preArriveZoneCode,
	tickets,
	weight,
	3 as status,
   '$[time(yyyy-MM-dd)]' as countTime,	
   '$[time(yyyy-MM-dd HH:mm:ss)]' as countDate
from
  (
    select
      requireId as requireId,
      transLevel as translevel,
      carStatus as carStatus,
      srcZoneCode as srcZoneCode,
      actualTime as preArriveTm,
      destZoneCode as preArriveZoneCode,
      sum(ticket) over(partition by requireId, destZoneCode) as tickets,
      sum(weight) over(partition by requireId, destZoneCode) as weight,
      row_number() over( partition by requireId,destZoneCode order by actualTime ) as num
    from
    (select * from (select *,row_number() over(partition by carNo order by actualTime) rn from tmp_ordi_predict.dmpdsp_vt_has_arrive_cars_hf_tmp1_470818 )t where t.rn = 1)
  ) t
where
  t.num = 1
 ;
 
-- 补充已到达车辆没有车标的情况
drop table if exists tmp_ordi_predict.has_arrive_toal_tmp_no_car_470818;
create table tmp_ordi_predict.has_arrive_toal_tmp_no_car_470818
stored as parquet as 	
select
  requireId,
  "" as carNo,
  transLevel,
  carStatus,
  srcZoneCode,
  secondActualArriveTm as preArriveTm,
  secondZoneCode as preArriveZoneCode,
  0 as tickets,
  0 as weight,
  3 as status,
   '$[time(yyyy-MM-dd)]' as countTime,	
   '$[time(yyyy-MM-dd HH:mm:ss)]' as countDate
from
  tmp_ordi_predict.new_require_task_info_tmp_470818
where
  (
    carNo is null
    or carNo = ''
  )
  and srcActualDepartTm > 0
  and secondActualArriveTm > 0
  and secondActualDepartTm is null
union 
select
  requireId,
  "" as carNo,
  transLevel,
  carStatus,
  secondZoneCode as srcZoneCode,
  thirdActualArriveTm as preArriveTm,
  thirdZoneCode as preArriveZoneCode,
  0 as tickets,
  0 as weight,
  3 as status,
   '$[time(yyyy-MM-dd)]' as countTime,	
   '$[time(yyyy-MM-dd HH:mm:ss)]' as countDate
from
  tmp_ordi_predict.new_require_task_info_tmp_470818
where
  (
    carNo is null
    or carNo = ''
  )
  and secondActualDepartTm > 0
  and thirdActualArriveTm > 0
  and thirdActualDepartTm is null
union 
select
  requireId,
  "" as carNo,
  transLevel,
  carStatus,
  thirdZoneCode as srcZoneCode,
  destActualArriveTm as preArriveTm,
  destZoneCode as preArriveZoneCode,
  0 as tickets,
  0 as weight,
  3 as status,
   '$[time(yyyy-MM-dd)]' as countTime,	
   '$[time(yyyy-MM-dd HH:mm:ss)]' as countDate
from
  tmp_ordi_predict.new_require_task_info_tmp_470818
where
  (
    carNo is null
    or carNo = ''
  )
  and thirdActualDepartTm > 0
  and destActualArriveTm > 0
union 
select
  requireId,
  "" as carNo,
  transLevel,
  carStatus,
  secondZoneCode as srcZoneCode,
  destActualArriveTm as preArriveTm,
  destZoneCode as preArriveZoneCode,
  0 as tickets,
  0 as weight,
  3 as status,
   '$[time(yyyy-MM-dd)]' as countTime,	
   '$[time(yyyy-MM-dd HH:mm:ss)]' as countDate
from
  tmp_ordi_predict.new_require_task_info_tmp_470818
where
  (
    carNo is null
    or carNo = ''
  )
  and secondActualDepartTm > 0
  and thirdZoneCode is null
  and destActualArriveTm > 0 
union 
select
  requireId,
  "" as carNo,
  transLevel,
  carStatus,
  srcZoneCode,
  destActualArriveTm as preArriveTm,
  destZoneCode as preArriveZoneCode,
  0 as tickets,
  0 as weight,
  3 as status,
   '$[time(yyyy-MM-dd)]' as countTime,	
   '$[time(yyyy-MM-dd HH:mm:ss)]' as countDate
from
  tmp_ordi_predict.new_require_task_info_tmp_470818
where
  (
    carNo is null
    or carNo = ''
  )
  and srcActualDepartTm > 0
  and secondZoneCode is null
  and thirdZoneCode is null
  and destActualArriveTm > 0
;


drop table if exists tmp_ordi_predict.has_arrive_all_tmp_470818;
create table tmp_ordi_predict.has_arrive_all_tmp_470818
stored as parquet as 
select 
	requireId,
	carNo,
	transLevel,
	carStatus,
	srcZoneCode,
	from_unixtime(int(preArriveTm/1000), 'yyyy-MM-dd HH:mm:ss') as preArriveTm,
	preArriveZoneCode,
	tickets,
	weight,
	status,
	countTime,
	countDate,
	from_unixtime(int(preArriveTm/1000), 'yyyy-MM-dd HH:mm') as arrTm
from(
	select 
	  requireId,
	  carNo,
	  transLevel,
	  carStatus,
	  srcZoneCode,
	  preArriveTm,
	  preArriveZoneCode,
	  tickets,
	  weight,
	  status,
	  countTime,
	  countDate
	 from 
	 tmp_ordi_predict.tmp_in_road_cargo_1_470818   --  未发车辆
	union 
    select 
	  requireId,
	  carNo,
	  transLevel,
	  carStatus,
	  srcZoneCode,
	  preArriveTm,
	  preArriveZoneCode,
	  tickets,
	  weight,
	  status,
	  countTime,
	  countDate
	from 
	tmp_ordi_predict.tmp_in_road_cargo_2_470818   --在途（关联hbase计算有车标，拼接无车标）
	union 
	select 
	  requireId,
	  carNo,
	  transLevel,
	  carStatus,
	  srcZoneCode,
	  preArriveTm,
	  preArriveZoneCode,
	  tickets,
	  weight,
	  status,
	   countTime,	
	   countDate
	from 
	tmp_ordi_predict.has_arrive_toal_tmp_1_470818    -- 已到达
	union 
	select     -- 补充已到达无车标情况
	  a.requireId,
	  a.carNo,
	  a.transLevel,
	  a.carStatus,
	  a.srcZoneCode,
	  a.preArriveTm,
	  a.preArriveZoneCode,
	  a.tickets,
	  a.weight,
	  a.status,
	  a.countTime,	
	  a.countDate
	from
	(
		select * from (
			select *, row_number() over( partition by requireId,preArriveZoneCode) as rn
			from  tmp_ordi_predict.has_arrive_toal_tmp_no_car_470818 
		) al where 
			al.preArriveTm between unix_timestamp('$[time(yyyy-MM-dd HH:mm:ss,-3d)]') * 1000 and unix_timestamp('$[time(yyyy-MM-dd HH:mm:ss,+4d)]') * 1000
			and al.rn = 1 
	)a
	join tmp_ordi_predict.fmsrms_dim_heavy_transit_info_df_tmp_470818 b
	 on a.preArriveZoneCode = b.deptCode 
) r 
;


-- 已到达货量计算完毕 写入结果表
insert overwrite table tmp_ordi_predict.dm_cargo_quantity_prediction_dtl_rt partition(inc_day,inc_hour)
select 
	r.requireId as require_id,
	r.carNo as car_no,
	r.transLevel as trans_level,
	r.carStatus as car_status,
	r.srcZoneCode as src_zone_code,
	r.preArriveTm as pre_arrive_tm,
	r.preArriveZoneCode as pre_arrive_zone_code,
	r.tickets as tickets,
	r.weight as weight,
	r.status as status,
	r.countTime as count_time,
	r.countDate as count_date,
	r.batchCode as batch_code,
	r.batchDate as batch_date,
	'$[time(yyyyMMdd)]' as inc_day ,
	'${inchour1}' as inc_hour
from(
	select
		requireId,carNo,transLevel,carStatus,srcZoneCode,preArriveTm,preArriveZoneCode,tickets,weight,status,countTime,countDate,
		batch_code as batchCode,
		if(batch_date is not null,date_format(batch_date, 'yyyyMMdd'),batch_date)  as batchDate
	from
	(select
			requireId,carNo,transLevel,carStatus,srcZoneCode,preArriveTm,preArriveZoneCode,tickets,weight,status,countTime,countDate,
			max(batch_code) over(partition by requireId,srcZoneCode,preArriveZoneCode) batch_code,
			max(batch_date) over(partition by requireId,srcZoneCode,preArriveZoneCode) batch_date,
			row_number() over(partition by requireId,srcZoneCode,preArriveZoneCode order by weight desc) rn
	from
		(select
				requireId,carNo,transLevel,carStatus,srcZoneCode,preArriveTm,preArriveZoneCode,tickets,weight,status,countTime,countDate,arrTm,
				if((b.arrTm>=a.last_last_arrive_tm and b.arrTm<a.last_arrive_tm),batch_code,null) batch_code,
				if((b.arrTm>=a.last_last_arrive_tm and b.arrTm<a.last_arrive_tm),batch_date,null) batch_date
				from tmp_ordi_predict.has_arrive_all_tmp_470818 b left join 
				(select
					  operate_zone_code,
					  batch_code,
					  batch_date,
					  last_last_arrive_tm,
					  last_arrive_tm
					from
					  ky.dm_heavy_cargo.dm_arrive_batch_info_dtl_di
					where
					  inc_day between '$[time(yyyyMMdd,-4d)]' and '$[time(yyyyMMdd,+5d)]'
				)a
				on b.preArriveZoneCode=a.operate_zone_code
		)t1
	)t2
	where rn=1
)r 
;