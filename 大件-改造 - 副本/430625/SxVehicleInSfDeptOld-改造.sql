
--1.读取SX车辆实时进港明细  ，T-14D~T-0D
drop table if exists tmp_ordi_predict.cargo_info_430625;
create table tmp_ordi_predict.cargo_info_430625
stored as parquet as  
select
	shift_no,site_code,next_site_code,
	sum(total_out_weight) as total_out_weight,
	min(first_unlaod_time) as first_unlaod_time,
	max(standard_plan_arrival_time) as standard_plan_arrival_time,
	max(in_confirm_time) as in_confirm_time,
	max(transport_level) as transport_level,
	max(ewblist_send_time) as ewblist_send_time,
	max(vehicle_status) as vehicle_status,
	max(total_out_ewbcount) as total_out_ewbcount,
	max(total_out_piece) as total_out_piece,
	max(total_out_vol) as total_out_vol
from
  ky.dm_freight.dwd_sx_vehicle_dtl_di
where
  inc_day between '$[time(yyyyMMdd,-14d)]' and '$[time(yyyyMMdd)]'
group by shift_no,site_code,next_site_code
;


--2.场地过滤  快运/小件场地 获取中转场信息
drop table if exists tmp_ordi_predict.fmsrms_dim_heavy_transit_info_df_tmp_430625;
create table tmp_ordi_predict.fmsrms_dim_heavy_transit_info_df_tmp_430625
stored as parquet as  
select dept_code as deptCode from dm_ordi_predict.fmsrms_dim_heavy_transit_info_df where inc_day >= '$[time(yyyyMMdd,-1d)]' group by dept_code 
;

-- 3.顺丰车辆数据过滤
drop table if exists tmp_ordi_predict.dmpdsp_vt_has_arrive_cars_hf_tmp_430625;
create table tmp_ordi_predict.dmpdsp_vt_has_arrive_cars_hf_tmp_430625
stored as parquet as  
select requireId from (
select requireId  ,row_number() over(partition by requireId order by inc_day desc) as rn 
from tmp_ordi_predict.dmpdsp_vt_has_arrive_cars_hf	-- mysql同步hive 
where 
    -- inc_day = '$[time(yyyyMMdd)]'  -- 指定T-1D分区数据
    inc_day = '20230925'    -- 手动指定
	and inc_hour = '1500'    -- 手动指定
	and actualTime > unix_timestamp('$[time(yyyy-MM-dd HH:mm:ss,-14d)]') * 1000   -- 分钟/秒-固定
	and actualTime <= unix_timestamp('$[time(yyyy-MM-dd HH:mm:ss)]') * 1000      -- 分钟/秒-固定
) r where r.rn = 1 
	
;

-- 更新状态
insert overwrite table tmp_ordi_predict.dmpdsp_pub_table_sync_version_hf partition(inc_day, inc_hour)
select 
	table_name,
	if(version_id = '1' ,'2' ,'1') as version_id,
	'$[time(yyyy-MM-dd HH:mm:ss)]' as sync_time,
	'$[time(yyyyMMdd)]' as inc_day,
	'$(incHour)' as inc_hour 
from tmp_ordi_predict.dmpdsp_pub_table_sync_version_hf
where table_name = 'dws_sx_vehicle_sum'
and inc_day >= '$[time(yyyyMMdd,-1d)]'
order by sync_time desc 
limit 1 


-- 写入结果表
insert overwrite table tmp_ordi_predict.dm_sx_vehicle_his_sum_di partition(inc_day, inc_hour)
select 
	if(t4.version_id = '1' ,'2' ,'1') as version_id,
	r.shift_no,
	r.site_code,
	r.next_site_code,
	r.total_out_weight,
	r.total_out_ewbcount,
	r.total_out_piece,
	r.total_out_vol,
	r.first_unlaod_time,
	r.standard_plan_arrival_time,
	r.in_confirm_time,
	cast(r.transport_level as int) transport_level,
	r.ewblist_send_time,
	r.vehicle_status,
	'$[time(yyyy-MM-dd HH:mm:ss)]' as modify_time,
	'$[time(yyyyMMdd)]' as inc_day,
	'$(incHour)' as inc_hour 
from (
select 
	t1.shift_no,
	t1.site_code,
	t1.next_site_code,
	t1.total_out_weight,
	t1.total_out_ewbcount,
	t1.total_out_piece,
	t1.total_out_vol,
	t1.first_unlaod_time,
	t1.standard_plan_arrival_time,
	t1.in_confirm_time,
	cast(t1.transport_level as int) transport_level,
	t1.ewblist_send_time,
	t1.vehicle_status,
	'$[time(yyyy-MM-dd HH:mm:ss)]' as modify_time,
	row_number() over(partition by shift_no,site_code,next_site_code) as rn 
from 
tmp_ordi_predict.cargo_info_430625 t1 
left join 
tmp_ordi_predict.fmsrms_dim_heavy_transit_info_df_tmp_430625 t2 
on t1.next_site_code = t2.dept_code
left join 
tmp_ordi_predict.dmpdsp_vt_has_arrive_cars_hf_tmp_430625 t3 
on t1.shift_no = t2.requireId
where t2.deptCode is not null
and t3.requireId is null
) r 
left join 
(select version_id from tmp_ordi_predict.dmpdsp_pub_table_sync_version_hf
where table_name = 'dws_sx_vehicle_sum'
and inc_day >= '$[time(yyyyMMdd,-1d)]'
order by sync_time desc 
limit 1 ) t4 
on 1=1 
where r.rn = 1 
;
