
-- 1、获取车辆信息T-24D车辆任务，lastUpdateTm 小于当前时间数据。
drop table if exists tmp_ordi_predict.new_require_task_info_tmp_24d_248838;
create table tmp_ordi_predict.new_require_task_info_tmp_24d_248838
stored as parquet as  
select 
	*
from 
(select a.*,row_number() over(partition by requireId order by lastUpdateTm desc) rn 
	from (
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
			case when destUnloadContnrNos = 'null' then null else destUnloadContnrNos end as destUnloadContnrNos
		from dm_heavy_cargo.rt_vehicle_task_monitor_for_not_send_detail4 
		where inc_day between '$[time(yyyyMMdd,-24d)]' and '$[time(yyyyMMdd,-0d)]'
	) a  
	where (a.srcActualDepartTm >0 or a.secondActualDepartTm>0 or a.thirdActualDepartTm >0)
)aa where aa.rn=1 and aa.carStatus in (3,4,5,6)
;

-- 获取中转场信息
drop table if exists tmp_ordi_predict.fmsrms_dim_heavy_transit_info_df_tmp_248838;
create table tmp_ordi_predict.fmsrms_dim_heavy_transit_info_df_tmp_248838
stored as parquet as  
select dept_code as deptCode from dm_ordi_predict.fmsrms_dim_heavy_transit_info_df 
where inc_day >= '$[time(yyyyMMdd,-1d)]' group by dept_code 
;


drop table if exists tmp_ordi_predict.tmp_all_arrive_before_hbase_248838;
create table tmp_ordi_predict.tmp_all_arrive_before_hbase_248838
stored as parquet as  
select
  aa.*,
  adTable.car_no as car_no
from
  (
    select
      a.requireId as require_id,
      a.translevel,
      a.srcZoneCode as src_zone_code,
      a.srcActualDepartTm as send_time,
      a.secondZoneCode as dest_zone_code,
      a.secondActualArriveTm as arrive_time,
      a.srcLoadContnrnos as car_nos
    from
      tmp_ordi_predict.new_require_task_info_tmp_24d_248838 a
      join tmp_ordi_predict.fmsrms_dim_heavy_transit_info_df_tmp_248838 b 
	on a.srcZoneCode = b.deptCode
    where
      a.srcZoneCode is not null
      and a.secondZoneCode is not null
      and a.srcActualDepartTm > 0
      and a.srcLoadContnrnos is not null
 ) aa LATERAL VIEW explode(split(car_nos, ',')) adTable AS car_no
union 
select
  aa.*,
  adTable.car_no as car_no
from
  (
    select
      a.requireId as require_id,
      a.translevel,
      a.secondZoneCode as src_zone_code,
      a.secondActualDepartTm as send_time,
      a.thirdZoneCode as dest_zone_code,
      a.thirdActualArriveTm as arrive_time,
      a.secondLoadContnrnos as car_nos
    from
       tmp_ordi_predict.new_require_task_info_tmp_24d_248838 a
      join tmp_ordi_predict.fmsrms_dim_heavy_transit_info_df_tmp_248838 b 
	on a.secondZoneCode = b.deptCode
    where
      a.secondZoneCode is not null
      and a.thirdZoneCode is not null
      and a.secondActualDepartTm > 0
      and a.secondLoadContnrnos is not null
  ) aa LATERAL VIEW explode(split(car_nos, ',')) adTable AS car_no
union 
select
  aa.*,
  adTable.car_no as car_no
from
  (
    select
      a.requireId as require_id,
      a.translevel,
      a.thirdZoneCode as src_zone_code,
      a.thirdActualDepartTm as send_time,
      a.destZoneCode as dest_zone_code,
      a.destActualArriveTm as arrive_time,
      a.thirdLoadContnrnos as car_nos
    from
      tmp_ordi_predict.new_require_task_info_tmp_24d_248838 a
      join tmp_ordi_predict.fmsrms_dim_heavy_transit_info_df_tmp_248838 b
    on a.thirdZoneCode = b.deptCode
    where
      a.thirdZoneCode is not null
      and a.destZoneCode is not null
      and a.thirdActualDepartTm > 0
      and a.thirdLoadContnrnos is not null
  ) aa LATERAL VIEW explode(split(car_nos, ',')) adTable AS car_no
union 
select
  aa.*,
  adTable.car_no as car_no
from
  (
    select
      a.requireId as require_id,
      a.translevel,
      a.srcZoneCode as src_zone_code,
      a.srcActualDepartTm as send_time,
      a.destZoneCode as dest_zone_code,
      a.destActualArriveTm as arrive_time,
      a.srcLoadContnrnos as car_nos
    from
      tmp_ordi_predict.new_require_task_info_tmp_24d_248838 a
      join tmp_ordi_predict.fmsrms_dim_heavy_transit_info_df_tmp_248838 b 
	on a.srcZoneCode = b.deptCode
    where
      a.srcZoneCode is not null
      and a.destZoneCode is not null
      and a.srcActualDepartTm > 0
      and a.secondZoneCode is null
  ) aa LATERAL VIEW explode(split(car_nos, ',')) adTable AS car_no
union 
select
  aa.*,
  adTable.car_no as car_no
from
  (
    select
      a.requireId as require_id,
      a.translevel,
      a.secondZoneCode as src_zone_code,
      a.secondActualDepartTm as send_time,
      a.destZoneCode as dest_zone_code,
      a.destActualArriveTm as arrive_time,
      secondLoadContnrnos as car_nos
    from
      tmp_ordi_predict.new_require_task_info_tmp_24d_248838 a
      join tmp_ordi_predict.fmsrms_dim_heavy_transit_info_df_tmp_248838 b 
	on a.secondZoneCode = b.deptCode
    where
      a.secondZoneCode is not null
      and a.destZoneCode is not null
      and a.secondActualDepartTm > 0
      and a.secondZoneCode is not null
      and a.thirdZoneCode is null
  ) aa LATERAL VIEW explode(split(car_nos, ',')) adTable AS car_no
;