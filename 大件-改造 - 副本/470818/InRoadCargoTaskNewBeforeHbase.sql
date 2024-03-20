-- 1、获取车辆信息T-60D车辆任务，lastUpdateTm 小于当前时间数据。
drop table if exists tmp_ordi_predict.new_require_task_info_tmp_60d_470818;
create table tmp_ordi_predict.new_require_task_info_tmp_60d_470818
stored as parquet as  
select
	case when idKey = 'null' then null else idKey end as idKey,
	case when requireId = 'null' then null else requireId end as requireId,
	case when translevel = 'null' then null else cast(translevel as int) end as translevel,
	case when carNo = 'null' then null else carNo end as carNo,
	case when lineCode = 'null' then null else lineCode end as lineCode,
	case when carStatus = 'null' then null else cast(carStatus as int) end as carStatus,
	case when lastUpdateTm = 'null' then null else cast(lastUpdateTm as bigint) end as lastUpdateTm,
	case when srcZoneCode = 'null' then null else srcZoneCode end as srcZoneCode,
	case when srcPlanReachTm = 'null' then null else cast(srcPlanReachTm as bigint) end as srcPlanReachTm,
	case when srcPlanDepartTm = 'null' then null else cast(srcPlanDepartTm as bigint) end as srcPlanDepartTm,
	case when srcActualDepartTm = 'null' then null else cast(srcActualDepartTm as bigint) end as srcActualDepartTm,
	case when srcPreDepartTm = 'null' then null else cast(srcPreDepartTm as bigint) end as srcPreDepartTm,
	case when srcPlanArriveTm = 'null' then null else cast(srcPlanArriveTm as bigint) end as srcPlanArriveTm,
	case when srcActualArriveTm = 'null' then null else cast(srcActualArriveTm as bigint) end as srcActualArriveTm,
	case when srcPreArriveTm = 'null' then null else cast(srcPreArriveTm as bigint) end as srcPreArriveTm,
	case when secondZoneCode = 'null' then null else secondZoneCode end as secondZoneCode,
	case when secondPlanReachTm = 'null' then null else cast(secondPlanReachTm as bigint) end as secondPlanReachTm,
	case when secondPlanDepartTm = 'null' then null else cast(secondPlanDepartTm as bigint) end as secondPlanDepartTm,
	case when secondActualDepartTm = 'null' then null else cast(secondActualDepartTm as bigint) end as secondActualDepartTm,
	case when secondPreDepartTm = 'null' then null else cast(secondPreDepartTm as bigint) end as secondPreDepartTm,
	case when secondPlanArriveTm = 'null' then null else cast(secondPlanArriveTm as bigint) end as secondPlanArriveTm,
	case when secondActualArriveTm = 'null' then null else cast(secondActualArriveTm as bigint) end as secondActualArriveTm,
	case when secondPreArriveTm = 'null' then null else cast(secondPreArriveTm as bigint) end as secondPreArriveTm,
	case when thirdZoneCode = 'null' then null else thirdZoneCode end as thirdZoneCode,
	case when thirdPlanReachTm = 'null' then null else cast(thirdPlanReachTm as bigint) end as thirdPlanReachTm,
	case when thirdPlanDepartTm = 'null' then null else cast(thirdPlanDepartTm as bigint) end as thirdPlanDepartTm,
	case when thirdActualDepartTm = 'null' then null else cast(thirdActualDepartTm as bigint) end as thirdActualDepartTm,
	case when thirdPreDepartTm = 'null' then null else cast(thirdPreDepartTm as bigint) end as thirdPreDepartTm,
	case when thirdPlanArriveTm = 'null' then null else cast(thirdPlanArriveTm as bigint) end as thirdPlanArriveTm,
	case when thirdActualArriveTm = 'null' then null else cast(thirdActualArriveTm as bigint) end as thirdActualArriveTm,
	case when thirdPreArriveTm = 'null' then null else cast(thirdPreArriveTm as bigint) end as thirdPreArriveTm,
	case when destZoneCode = 'null' then null else destZoneCode end as destZoneCode,
	case when destPlanReachTm = 'null' then null else cast(destPlanReachTm as bigint) end as destPlanReachTm,
	case when destPlanDepartTm = 'null' then null else cast(destPlanDepartTm as bigint) end as destPlanDepartTm,
	case when destActualDepartTm = 'null' then null else cast(destActualDepartTm as bigint) end as destActualDepartTm,
	case when destPreDepartTm = 'null' then null else cast(destPreDepartTm as bigint) end as destPreDepartTm,
	case when destPlanArriveTm = 'null' then null else cast(destPlanArriveTm as bigint) end as destPlanArriveTm,
	case when destActualArriveTm = 'null' then null else cast(destActualArriveTm as bigint) end as destActualArriveTm,
	case when destPreArriveTm = 'null' then null else cast(destPreArriveTm as bigint) end as destPreArriveTm,
	case when insertTime = 'null' then null else cast(insertTime as timestamp) end as insertTime,
	case when fullLoadWeight = 'null' then null else cast(fullLoadWeight as double) end as fullLoadWeight,
	case when srcLoadContnrNos = 'null' then null else srcLoadContnrNos end as srcLoadContnrNos,
	case when srcArriveContnrNos = 'null' then null else srcArriveContnrNos end as srcArriveContnrNos,
	case when srcUnloadContnrNos = 'null' then null else srcUnloadContnrNos end as srcUnloadContnrNos,
	case when secondLoadContnrNos = 'null' then null else secondLoadContnrNos end as secondLoadContnrNos,
	case when secondArriveContnrNos = 'null' then null else secondArriveContnrNos end as secondArriveContnrNos,
	case when secondUnloadContnrNos = 'null' then null else secondUnloadContnrNos end as secondUnloadContnrNos,
	case when thirdLoadContnrNos = 'null' then null else thirdLoadContnrNos end as thirdLoadContnrNos,
	case when thirdArriveContnrNos = 'null' then null else thirdArriveContnrNos end as thirdArriveContnrNos,
	case when thirdUnloadContnrNos = 'null' then null else thirdUnloadContnrNos end as thirdUnloadContnrNos,
	case when destLoadContnrNos = 'null' then null else destLoadContnrNos end as destLoadContnrNos,
	case when destArriveContnrNos = 'null' then null else destArriveContnrNos end as destArriveContnrNos,
	case when destUnloadContnrNos = 'null' then null else destUnloadContnrNos end as destUnloadContnrNos,
	case when srcJobType = 'null' then null else srcJobType end as srcJobType,
	case when secondJobType = 'null' then null else secondJobType end as secondJobType,
	case when thirdJobType = 'null' then null else thirdJobType end as thirdJobType,
	case when destJobType = 'null' then null else destJobType end as destJobType,
	case when nextzonecodedynamicprearrivetime = 'null' then null else nextzonecodedynamicprearrivetime end as nextzonecodedynamicprearrivetime,
	inc_day
from bdp.dm_heavy_cargo.rt_vehicle_task_monitor_for_not_send_detail4 where inc_day >= '$[time(yyyyMMdd,-60d)]'  and lastUpdateTm <=  unix_timestamp('$[time(yyyy-MM-dd HH:mm:ss)]') * 1000
;

-- 取T-60D ，开窗
drop table if exists tmp_ordi_predict.new_require_task_info_tmp_470818;
create table tmp_ordi_predict.new_require_task_info_tmp_470818
stored as parquet as  
select 
	* 
from 
	(select *,row_number() over(partition by requireId order by lastUpdateTm desc) rn
	from tmp_ordi_predict.new_require_task_info_tmp_60d_470818
	)t 
where t.rn = 1  
;


-- 获取中转场信息
drop table if exists tmp_ordi_predict.fmsrms_dim_heavy_transit_info_df_tmp_470818;
create table tmp_ordi_predict.fmsrms_dim_heavy_transit_info_df_tmp_470818
stored as parquet as  
select dept_code as deptCode from dm_ordi_predict.fmsrms_dim_heavy_transit_info_df where inc_day >= '$[time(yyyyMMdd,-1d)]' group by dept_code 
;


-- 2、未发车辆计算 （关联-中转场信息 ）
drop table if exists tmp_ordi_predict.tmp_in_road_cargo_1_470818;
create table tmp_ordi_predict.tmp_in_road_cargo_1_470818
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
(
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
		-- 1、计算未发车-车辆信息
		select
			* ,
			row_number() over(partition by requireId,srcZoneCode,preArriveZoneCode) rn 
		from 
		(
			select
			  a.requireId as requireId,
			  a.carNo as carNo,
			  a.translevel as transLevel,
			  a.carStatus as carStatus,
			  a.srcZoneCode as srcZoneCode,
			  a.secondPlanArriveTm as preArriveTm,
			  a.secondZoneCode as preArriveZoneCode,
			  0 as tickets,
			  0 as weight,
			  1 as status,
			  '$[time(yyyy-MM-dd)]' as countTime,	
			  '$[time(yyyy-MM-dd HH:mm:ss)]' as countDate
			from 
				-- 过滤-实际未发车车辆
				(select * from tmp_ordi_predict.new_require_task_info_tmp_470818 where carStatus in (1,2,3,4,5) and srcActualDepartTm is null) a
			join 
				-- 中转场信息
				tmp_ordi_predict.fmsrms_dim_heavy_transit_info_df_tmp_470818 b 
			on a.secondZoneCode = b.deptCode
			where
			  a.secondZoneCode is not null   -- 途经网点
			  and a.secondJobType <> '1'
		union  
			select
			  a.requireId as requireId,
			  a.carNo as carNo,
			  a.translevel as transLevel,
			  a.carStatus as carStatus,
			  a.srcZoneCode as srcZoneCode,
			  a.destPlanArriveTm as preArriveTm,
			  a.destZoneCode as preArriveZoneCode,
			  0 as tickets,
			  0 as weight,
			  1 as status,
			  '$[time(yyyy-MM-dd)]' as countTime,	
			  '$[time(yyyy-MM-dd HH:mm:ss)]' as countDate
			from
				-- 过滤-实际未发车车辆
				(select * from tmp_ordi_predict.new_require_task_info_tmp_470818 where carStatus in (1,2,3,4,5) and srcActualDepartTm is null) a
			join 
				-- 中转场信息
				tmp_ordi_predict.fmsrms_dim_heavy_transit_info_df_tmp_470818 b 
			on a.destZoneCode = b.deptCode
			where
			  destZoneCode is not null  -- 目的网点
		union  
			select
			  a.requireId as requireId,
			  a.carNo as carNo,
			  a.translevel as transLevel,
			  a.carStatus as carStatus,
			  a.srcZoneCode as srcZoneCode,
			  a.thirdPlanArriveTm as preArriveTm,
			  a.thirdZoneCode as preArriveZoneCode,
			  0 as tickets,
			  0 as weight,
			  1 as status,
			  '$[time(yyyy-MM-dd)]' as countTime,	
			  '$[time(yyyy-MM-dd HH:mm:ss)]' as countDate
			from
				-- 过滤-实际未发车车辆
				(select * from tmp_ordi_predict.new_require_task_info_tmp_470818 where carStatus in (1,2,3,4,5) and srcActualDepartTm is null) a
			join 
				-- 中转场信息
				tmp_ordi_predict.fmsrms_dim_heavy_transit_info_df_tmp_470818 b 
			on a.thirdZoneCode = b.deptCode
			where
			  a.thirdZoneCode is not null -- 途径网点2
			  and a.thirdJobType <> '1'
		) r_1 
	)r_1_1 where 
			r_1_1.preArriveTm between unix_timestamp('$[time(yyyy-MM-dd HH:mm:ss,-3d)]') * 1000 and unix_timestamp('$[time(yyyy-MM-dd HH:mm:ss,+4d)]') * 1000
			and r_1_1.rn = 1 
union 
	-- 2、计算实际已发车-车辆信息
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
			* ,
			row_number() over(partition by requireId,srcZoneCode,preArriveZoneCode) rn 
		from 
		(
			select
			  a.requireId as requireId,
			  a.carNo as carNo,
			  a.translevel as transLevel,
			  a.carStatus as carStatus,
			  a.secondZoneCode as srcZoneCode,
			  a.thirdPlanArriveTm as preArriveTm,
			  a.thirdZoneCode as preArriveZoneCode,
			  0 as tickets,
			  0 as weight,
			  1 as status,
			 '$[time(yyyy-MM-dd)]' as countTime,	
			 '$[time(yyyy-MM-dd HH:mm:ss)]' as countDate
			from
				-- 过滤实际已发车-车辆信息
				(select * from tmp_ordi_predict.new_require_task_info_tmp_470818 where carStatus in (1,2,3,4,5) and srcActualDepartTm >0 and secondActualDepartTm is null) a
			  join
				-- 中转场信息
				tmp_ordi_predict.fmsrms_dim_heavy_transit_info_df_tmp_470818 b 
			  on a.thirdZoneCode = b.deptCode
			where
			  a.secondZoneCode is not null  -- 途径网点1
			  and a.thirdZoneCode is not null -- 途径网点2
			  and a.thirdJobType <> '1'
		 union 
			select
			  a.requireId as requireId,
			  a.carNo as carNo,
			  a.translevel as transLevel,
			  a.carStatus as carStatus,
			  a.secondZoneCode as srcZoneCode,
			  a.destPlanArriveTm as preArriveTm,
			  a.destZoneCode as preArriveZoneCode,
			  0 as tickets,
			  0 as weight,
			  1 as status,
			  '$[time(yyyy-MM-dd)]' as countTime,	
			  '$[time(yyyy-MM-dd HH:mm:ss)]' as countDate
			from
				-- 过滤实际已发车-车辆信息
				(select * from tmp_ordi_predict.new_require_task_info_tmp_470818 where carStatus in (1,2,3,4,5) and srcActualDepartTm >0 and secondActualDepartTm is null) a
			  join
				-- 中转场信息
				tmp_ordi_predict.fmsrms_dim_heavy_transit_info_df_tmp_470818 b  
			  on a.destZoneCode = b.deptCode
			where
			  a.secondZoneCode is not null   -- 途径网点1
			  and a.destZoneCode is not null -- 目的网点
		)r_2
	)r_2_1 where
		r_2_1.preArriveTm between unix_timestamp('$[time(yyyy-MM-dd HH:mm:ss,-3d)]') * 1000 and unix_timestamp('$[time(yyyy-MM-dd HH:mm:ss,+4d)]') * 1000
		and r_2_1.rn = 1 
union 
	-- 3、计算实际已到-途经网点1-车辆信息
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
			* ,
			row_number() over(partition by requireId,srcZoneCode,preArriveZoneCode) rn 
		from 
		(
			select
			  requireId as requireId,
			  carNo as carNo,
			  translevel as transLevel,
			  carStatus as carStatus,
			  thirdZoneCode as srcZoneCode,
			  destPlanArriveTm as preArriveTm,
			  destZoneCode as preArriveZoneCode,
			  0 as tickets,
			  0 as weight,
			  1 as status,
			  '$[time(yyyy-MM-dd)]' as countTime,	
			  '$[time(yyyy-MM-dd HH:mm:ss)]' as countDate
			from  
				-- 过滤实际已到-途经网点1-车辆信息
				(select * from tmp_ordi_predict.new_require_task_info_tmp_470818 where carStatus in (1,2,3,4,5) and srcActualDepartTm >0 and secondActualDepartTm >0 and thirdActualDepartTm is null) a
			join 
			  -- 中转场信息
			tmp_ordi_predict.fmsrms_dim_heavy_transit_info_df_tmp_470818 b 
			on a.destZoneCode = b.deptCode
			where
			  a.thirdZoneCode is not null  -- 途经网点2
			  and a.destZoneCode is not null -- 目的网点
		)r_3
	)r_3_1 where 
			r_3_1.preArriveTm between unix_timestamp('$[time(yyyy-MM-dd HH:mm:ss,-3d)]') * 1000 and unix_timestamp('$[time(yyyy-MM-dd HH:mm:ss,+4d)]') * 1000
			and r_3_1.rn = 1 
)r where r.srcZoneCode != preArriveZoneCode
;

-- 3:开始计算在途

-- 取T-3D ，开窗
drop table if exists tmp_ordi_predict.new_require_task_info_tmp_3d_470818;
create table tmp_ordi_predict.new_require_task_info_tmp_3d_470818
stored as parquet as  
select 
	* 
from 
	(select *,row_number() over(partition by requireId order by lastUpdateTm desc) rn
	 from tmp_ordi_predict.new_require_task_info_tmp_60d_470818
	 where inc_day  >= '$[time(yyyyMMdd,-3d)]' 
	)t 
where t.rn = 1  
;

drop table if exists tmp_ordi_predict.tmp_in_road_7_d_470818;
create table tmp_ordi_predict.tmp_in_road_7_d_470818
stored as parquet as  
select * from tmp_ordi_predict.new_require_task_info_tmp_3d_470818 where carStatus in (1,2,3,4,5) and lastUpdateTm >  unix_timestamp('$[time(yyyy-MM-dd HH:mm:ss,-7d)]') * 1000   -- 取T-7D
;

drop table if exists tmp_ordi_predict.tmp_in_all_car_nos_470818;
create table tmp_ordi_predict.tmp_in_all_car_nos_470818
stored as parquet as  
select 
  requireId,
  carNo,
  translevel,
  carStatus,
  srcZoneCode,
  preArriveZoneCode,
  preArriveTm
from 
(
	select
	  requireId,
	  adTable.carNo as carNo,
	  translevel,
	  carStatus,
	  srcZoneCode,
	  secondZoneCode as preArriveZoneCode,
	  coalesce(
		nextzonecodedynamicprearrivetime,
		secondPreArriveTm,
		secondPlanArriveTm
	  ) as preArriveTm
	from
	  (
		select
		  *
		from
		  tmp_ordi_predict.tmp_in_road_7_d_470818
		where
		  srcActualDepartTm > 0
		  and secondZoneCode is not null
		  and secondActualArriveTm is null
		  and thirdActualArriveTm is null
		  and destActualArriveTm is null
		  and secondJobType <> '1'
	  ) LATERAL VIEW explode(split(secondArriveContnrNos, ',')) adTable AS carNo
	union 
	select
	  requireId,
	  adTable.carNo as carNo,
	  translevel,
	  carStatus,
	  srcZoneCode,
	  destZoneCode as preArriveZoneCode,
	  coalesce(
		nextzonecodedynamicprearrivetime,
		destPreArriveTm,
		destPlanArriveTm
	  ) as preArriveTm
	from
	  (
		select
		  *
		from
		  tmp_ordi_predict.tmp_in_road_7_d_470818
		where
		  srcActualDepartTm > 0
		  and secondZoneCode is null
		  and secondActualArriveTm is null
		  and thirdActualArriveTm is null
		  and destActualArriveTm is null
		  and destZoneCode is not null
	  ) LATERAL VIEW explode(split(destArriveContnrNos, ',')) adTable AS carNo
	union 
	select
	  requireId,
	  adTable.carNo as carNo,
	  translevel,
	  carStatus,
	  secondZoneCode as srcZoneCode,
	  thirdZoneCode as preArriveZoneCode,
	  coalesce(
		nextzonecodedynamicprearrivetime,
		thirdPreArriveTm,
		thirdPlanArriveTm
	  ) as preArriveTm
	from
	  (
		select
		  *
		from
		  tmp_ordi_predict.tmp_in_road_7_d_470818
		where
		  srcActualDepartTm > 0
		  and secondZoneCode is not null
		  and secondActualDepartTm > 0
		  and thirdZoneCode is not null
		  and thirdActualArriveTm is null
		  and destActualArriveTm is null
		  and thirdJobType <> '1'
	  ) LATERAL VIEW explode(split(thirdArriveContnrNos, ',')) adTable AS carNo
	union 
	select
	  requireId,
	  adTable.carNo as carNo,
	  translevel,
	  carStatus,
	  secondZoneCode as srcZoneCode,
	  destZoneCode as preArriveZoneCode,
	  coalesce(
		nextzonecodedynamicprearrivetime,
		destPreArriveTm,
		destPlanArriveTm
	  ) as preArriveTm
	from
	  (
		select
		  *
		from
		  tmp_ordi_predict.tmp_in_road_7_d_470818
		where
		  srcActualDepartTm > 0
		  and secondZoneCode is not null
		  and secondActualDepartTm > 0
		  and thirdZoneCode is null
		  and destActualArriveTm is null
		  and destZoneCode is not null
	  ) LATERAL VIEW explode(split(destArriveContnrNos, ',')) adTable AS carNo
	union 
	select
	  requireId,
	  adTable.carNo as carNo,
	  translevel,
	  carStatus,
	  thirdZoneCode as srcZoneCode,
	  destZoneCode as preArriveZoneCode,
	  coalesce(
		nextzonecodedynamicprearrivetime,
		destPreArriveTm,
		destPlanArriveTm
	  ) as preArriveTm
	from
	  (
		select
		  *
		from
		  tmp_ordi_predict.tmp_in_road_7_d_470818
		where
		  srcActualDepartTm > 0
		  and thirdZoneCode is not null
		  and thirdActualDepartTm > 0
		  and destActualArriveTm is null
		  and destZoneCode is not null
	  ) LATERAL VIEW explode(split(destArriveContnrNos, ',')) adTable AS carNo
)r where r.preArriveTm between unix_timestamp('$[time(yyyy-MM-dd HH:mm:ss,-3d)]') * 1000 and unix_timestamp('$[time(yyyy-MM-dd HH:mm:ss,+4d)]') * 1000
;

-- 关联中转场 ，1、处理车标为null，  2、车标不为null，关联hbase获取重量，票量
drop table if exists tmp_ordi_predict.tmp_ky_all_car_nos_before_hbase_470818;
create table tmp_ordi_predict.tmp_ky_all_car_nos_before_hbase_470818
stored as parquet as  
select a.* from tmp_ordi_predict.tmp_in_all_car_nos_470818 a join tmp_ordi_predict.fmsrms_dim_heavy_transit_info_df_tmp_470818 b on a.preArriveZoneCode = b.deptCode where carNo is not null and carNo <> '' ;

drop table if exists tmp_ordi_predict.tmp_ky_not_car_nos_470818;
create table tmp_ordi_predict.tmp_ky_not_car_nos_470818
stored as parquet as  
select a.* from tmp_ordi_predict.tmp_in_all_car_nos_470818 a join tmp_ordi_predict.fmsrms_dim_heavy_transit_info_df_tmp_470818 b on a.preArriveZoneCode = b.deptCode where carNo is null or carNo = ''  ;
