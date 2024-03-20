
-- 读取hbase处理后的数据 , 计算在途

drop table if exists tmp_ordi_predict.tmp_in_road_cargo_2;
create table tmp_ordi_predict.tmp_in_road_cargo_2
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
			tmp_ordi_predict.tmp_ky_all_car_nos_behind_hbase
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
			tmp_ordi_predict.tmp_ky_not_car_nos a
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

-- 在途货量计算完毕 写入结果表
insert into table dm_ordi_predict.dmpdsp_t_monitor_in_road_cargo_new_hf partition(inc_day,inc_hour)
SELECT
  requireid,
  carno,
  translevel,
  carstatus,
  srczonecode,
  prearrivetm,
  prearrivezonecode,
  tickets,
  weight,
  status,
  counttime,
  countdate,
  '$[time(yyyyMMdd)]' as inc_day ,
  '${inchour1}' as inc_hour
FROM tmp_ordi_predict.tmp_in_road_cargo_2
;


-- 4、开始计算已到达
drop table if exists tmp_ordi_predict.dmpdsp_vt_has_arrive_cars_hf_tmp1;
create table tmp_ordi_predict.dmpdsp_vt_has_arrive_cars_hf_tmp1
stored as parquet as  
select * from tmp_ordi_predict.dmpdsp_vt_has_arrive_cars_hf	-- mysql同步hive 
where 
    -- inc_day = '$[time(yyyyMMdd)]'  -- 指定T-1D分区数据
    inc_day = '20230925'    -- 手动指定
	and inc_hour = '1500'    -- 手动指定
	and actualTime > unix_timestamp('$[time(yyyy-MM-dd HH:mm:ss,-1d)]') * 1000   -- 分钟/秒-固定
	and actualTime <= unix_timestamp('$[time(yyyy-MM-dd HH:mm:ss)]') * 1000      -- 分钟/秒-固定
;

drop table if exists tmp_ordi_predict.has_arrive_toal_tmp_1;
create table tmp_ordi_predict.has_arrive_toal_tmp_1
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
    (select * from (select *,row_number() over(partition by carNo order by actualTime) rn from tmp_ordi_predict.dmpdsp_vt_has_arrive_cars_hf_tmp1 )t where t.rn = 1)
  ) t
where
  t.num = 1
 ;
 
-- 补充已到达车辆没有车标的情况
drop table if exists tmp_ordi_predict.has_arrive_toal_tmp_no_car;
create table tmp_ordi_predict.has_arrive_toal_tmp_no_car
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
  tmp_ordi_predict.new_require_task_info_tmp
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
  tmp_ordi_predict.new_require_task_info_tmp
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
  tmp_ordi_predict.new_require_task_info_tmp
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
  tmp_ordi_predict.new_require_task_info_tmp
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
  tmp_ordi_predict.new_require_task_info_tmp
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



drop table if exists tmp_ordi_predict.has_arrive_all_tmp;
create table tmp_ordi_predict.has_arrive_all_tmp
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
from 
tmp_ordi_predict.has_arrive_toal_tmp_1
union 
select
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
		from  tmp_ordi_predict.has_arrive_toal_tmp_no_car 
	) al where al.rn = 1 
)a
join tmp_ordi_predict.fmsrms_dim_heavy_transit_info_df_tmp b
 on a.preArriveZoneCode = b.deptCode 
;


drop table if exists tmp_ordi_predict.has_arrive_all_tmp_1;
create table tmp_ordi_predict.has_arrive_all_tmp_1
stored as parquet as 	
select
  t.*
from
  (
    select
      a.*,
      row_number() over( partition by requireId,preArriveZoneCode order by weight desc ) as rn
    from
      tmp_ordi_predict.has_arrive_all_tmp a
  ) t
where
  t.rn = 1
  and srcZoneCode != preArriveZoneCode
;


-- 已到达货量计算完毕 写入结果表
insert into table dm_ordi_predict.dmpdsp_t_monitor_in_road_cargo_new_hf partition(inc_day,inc_hour)
SELECT
  requireid,
  carno,
  translevel,
  carstatus,
  srczonecode,
  prearrivetm,
  prearrivezonecode,
  tickets,
  weight,
  status,
  counttime,
  countdate,
  '$[time(yyyyMMdd)]' as inc_day ,
  '${inchour1}' as inc_hour
FROM tmp_ordi_predict.has_arrive_all_tmp_1
;


-- 更新状态信息
insert overwrite table dm_ordi_predict.dmpdsp_t_monitor_detail_data_process_hf partition(inc_day,inc_hour)
SELECT '1' as id,
		't_monitor_in_road_cargo_new' as table_name,
		'2' as statues,
		'$[time(yyyy-MM-dd HH:mm:ss)]' as start_time,
		'$[time(yyyy-MM-dd HH:mm:ss)]' as end_time,
		'$[time(yyyy-MM-dd HH:mm:ss)]' as inserttime, 
		'$[time(yyyyMMdd)]' as inc_day ,
		'${inchour1}' as inc_hour
FROM dm_ordi_predict.dmpdsp_t_monitor_detail_data_process_hf 
limit 1 
;


--更新条数信息
insert overwrite table tmp_ordi_predict.dmpdsp_t_monitor_detail_data_row_hf partition(inc_day,inc_hour)
select  
        't_monitor_in_road_cargo_new' as table_name,
         sum(al.num) as table_rows ,
         '$[time(yyyy-MM-dd HH:mm:ss)]' as create_time,
		'$[time(yyyyMMdd)]' as inc_day ,
		'${inchour1}' as inc_hour
from (
select count(1) as num  from tmp_ordi_predict.tmp_in_road_cargo_1
union
select count(1) as num from tmp_ordi_predict.tmp_in_road_cargo_2
union
select count(1)as num  from tmp_ordi_predict.has_arrive_all_tmp_1
) al 
;