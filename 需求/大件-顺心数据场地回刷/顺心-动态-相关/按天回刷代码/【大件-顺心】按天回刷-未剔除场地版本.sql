/**
    summary：大件472976-zip任务-改造为Spark
        1、按天回刷顺心车辆数据     
**/

set hive.exec.dynamic.partition = true;
set hive.exec.dynamic.partition.mode = nonstrict;
set hive.exec.max.dynamic.partitions.pernode=500;
set hive.exec.max.dynamic.partitions = 1000;


-- 三个测试场地小件场地顺心车辆数据
insert overwrite table dm_ordi_predict.dm_cargo_quantity_sx_vehicle_dtl_rt_hi partition (inc_day,inc_hour)
select 
	shift_no,
	site_code,
	next_site_code,
	total_out_weight,
	total_out_ewbcount,
	total_out_piece,
	total_out_vol,
	ewblist_send_time,
	in_confirm_time,
	first_unlaod_time,
	standard_plan_arrival_time,
	pre_arrive_time,
	transport_level,
	case vehicle_status
	  when '0' then '1'
	  when '1' then '2'
	else '3' end status,
	batch_code,
	if(batch_date is not null,date_format(batch_date, 'yyyyMMdd'),batch_date) as batch_date,
    '$[time(yyyy-MM-dd 00:00:00)]' as count_time,  -- 回刷手动指定-小时
    '$[time(yyyyMMdd)]' as inc_day,  -- 写入天分区
    '0000' as inc_hour                   -- 回刷手动指定-小时
from(
	select 
		shift_no,
		site_code,
		next_site_code,
		total_out_weight,
		total_out_ewbcount,
		total_out_piece,
		total_out_vol,
		first_unlaod_time,
		standard_plan_arrival_time,
		in_confirm_time,
		pre_arrive_time,
		transport_level,
		ewblist_send_time,
		vehicle_status,
		max(batch_code) over(partition by shift_no, site_code, next_site_code) batch_code,
		max(batch_date) over(partition by shift_no, site_code, next_site_code) batch_date,
		row_number() over(partition by shift_no,site_code,next_site_code order by pre_arrive_time) rn
	from(	
		select 
			shift_no,
			site_code,
			next_site_code,
			total_out_weight,
			total_out_ewbcount,
			total_out_piece,
			total_out_vol,
			first_unlaod_time,
			standard_plan_arrival_time,
			in_confirm_time,
			pre_arrive_time,
			arrTm,
			transport_level,
			ewblist_send_time,
			vehicle_status,
			if((t4.arrTm >= t5.last_last_arrive_tm and t4.arrTm < t5.last_arrive_tm ),batch_code,null) batch_code,
			if((t4.arrTm >= t5.last_last_arrive_tm and t4.arrTm < t5.last_arrive_tm ),batch_date,null) batch_date
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
				if(
					datediff(t1.first_unlaod_time,FROM_UNIXTIME(UNIX_TIMESTAMP()))>365,
					if(datediff(t1.in_confirm_time,FROM_UNIXTIME(UNIX_TIMESTAMP()))<-365,t1.standard_plan_arrival_time,t1.in_confirm_time),
					t1.first_unlaod_time 
				) as pre_arrive_time,
				date_format(
					if(
						datediff(t1.first_unlaod_time,FROM_UNIXTIME(UNIX_TIMESTAMP()))>365,
						if(datediff(t1.in_confirm_time,FROM_UNIXTIME(UNIX_TIMESTAMP()))<-365,t1.standard_plan_arrival_time,t1.in_confirm_time),
						t1.first_unlaod_time 
					),'yyyy-MM-dd HH:mm'
				) as arrTm,
				cast(t1.transport_level as int) transport_level,
				t1.ewblist_send_time,
				t1.vehicle_status
			from
				(select
					shift_no, 
					site_code,
					next_site_code,
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
				from ky.dm_freight.dwd_sx_vehicle_dtl_di     -- 读取SX车辆实时进港明细
				where 
                    inc_day between '$[time(yyyyMMdd,-14d)]' and '$[time(yyyyMMdd)]' 
                    and ewblist_send_time between '$[time(yyyy-MM-dd 00:00:00,-14d)]' and  '$[time(yyyy-MM-dd 00:00:00)]' 	-- 回刷手动指定-小时 (约束时点数据) 
				group by shift_no, site_code,next_site_code   
				) t1 
			left join 
				(
					select 
						dept_code as deptCode 
					from dm_ordi_predict.fmsrms_dim_heavy_transit_info_df   -- 场地过滤  快运/小件场地
					where inc_day = '20230905'  -- 写死日期
					group by dept_code
				) t2  
			on t1.next_site_code = t2.deptCode
			left join 
				(
					select 
						requireId  
					from dm_ordi_predict.dmpdsp_vt_has_arrive_cars_batch_df  -- 顺丰车辆数据过滤
					where inc_day = '20230905'  -- 写死日期
					and actualTime > '$[time(yyyy-MM-dd 00:00:00,-30d)]'  -- 回刷手动指定-小时
					and actualTime <= '$[time(yyyy-MM-dd 00:00:00)]'  -- 回刷手动指定-小时
					group by requireId
				) t3  
			on t1.shift_no = t3.requireId
			where t2.deptCode is not null 
			and t3.requireId is null 
			and if(
					datediff(t1.first_unlaod_time,FROM_UNIXTIME(UNIX_TIMESTAMP()))>365,
					if(datediff(t1.in_confirm_time,FROM_UNIXTIME(UNIX_TIMESTAMP()))<-365,t1.standard_plan_arrival_time,t1.in_confirm_time),
					t1.first_unlaod_time 
				) 
                 between '$[time(yyyy-MM-dd 00:00:00,-3d)]' and '$[time(yyyy-MM-dd 00:00:00,+5d)]' 
         
		)t4 
		left join 
		-- 读取班次信息
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
		)t5 
		on t4.next_site_code=t5.operate_zone_code
	)t6 
)t7 where t7.rn=1 

union all 
select 
	shift_no,
	site_code,
	next_site_code,
	total_out_weight,
	total_out_ewbcount,
	total_out_piece,
	total_out_vol,
	ewblist_send_time,
	in_confirm_time,
	first_unlaod_time,
	standard_plan_arrival_time,
	pre_arrive_time,
	transport_level,
	case vehicle_status
	  when '0' then '1'
	  when '1' then '2'
	else '3' end status,
	batch_code,
	if(batch_date is not null,date_format(batch_date, 'yyyyMMdd'),batch_date) as batch_date,
    '$[time(yyyy-MM-dd 01:00:00)]' as count_time,  -- 回刷手动指定-小时
    '$[time(yyyyMMdd)]' as inc_day,  -- 写入天分区
    '0100' as inc_hour                   -- 回刷手动指定-小时
from(
	select 
		shift_no,
		site_code,
		next_site_code,
		total_out_weight,
		total_out_ewbcount,
		total_out_piece,
		total_out_vol,
		first_unlaod_time,
		standard_plan_arrival_time,
		in_confirm_time,
		pre_arrive_time,
		transport_level,
		ewblist_send_time,
		vehicle_status,
		max(batch_code) over(partition by shift_no, site_code, next_site_code) batch_code,
		max(batch_date) over(partition by shift_no, site_code, next_site_code) batch_date,
		row_number() over(partition by shift_no,site_code,next_site_code order by pre_arrive_time) rn
	from(	
		select 
			shift_no,
			site_code,
			next_site_code,
			total_out_weight,
			total_out_ewbcount,
			total_out_piece,
			total_out_vol,
			first_unlaod_time,
			standard_plan_arrival_time,
			in_confirm_time,
			pre_arrive_time,
			arrTm,
			transport_level,
			ewblist_send_time,
			vehicle_status,
			if((t4.arrTm >= t5.last_last_arrive_tm and t4.arrTm < t5.last_arrive_tm ),batch_code,null) batch_code,
			if((t4.arrTm >= t5.last_last_arrive_tm and t4.arrTm < t5.last_arrive_tm ),batch_date,null) batch_date
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
				if(
					datediff(t1.first_unlaod_time,FROM_UNIXTIME(UNIX_TIMESTAMP()))>365,
					if(datediff(t1.in_confirm_time,FROM_UNIXTIME(UNIX_TIMESTAMP()))<-365,t1.standard_plan_arrival_time,t1.in_confirm_time),
					t1.first_unlaod_time 
				) as pre_arrive_time,
				date_format(
					if(
						datediff(t1.first_unlaod_time,FROM_UNIXTIME(UNIX_TIMESTAMP()))>365,
						if(datediff(t1.in_confirm_time,FROM_UNIXTIME(UNIX_TIMESTAMP()))<-365,t1.standard_plan_arrival_time,t1.in_confirm_time),
						t1.first_unlaod_time 
					),'yyyy-MM-dd HH:mm'
				) as arrTm,
				cast(t1.transport_level as int) transport_level,
				t1.ewblist_send_time,
				t1.vehicle_status
			from
				(select
					shift_no, 
					site_code,
					next_site_code,
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
				from ky.dm_freight.dwd_sx_vehicle_dtl_di     -- 读取SX车辆实时进港明细
				where 
                    inc_day between '$[time(yyyyMMdd,-14d)]' and '$[time(yyyyMMdd)]' 
                    and ewblist_send_time between '$[time(yyyy-MM-dd 00:00:00,-14d)]' and  '$[time(yyyy-MM-dd 01:00:00)]' 	-- 回刷手动指定-小时 (约束时点数据) 
				group by shift_no, site_code,next_site_code   
				) t1 
			left join 
				(
					select 
						dept_code as deptCode 
					from dm_ordi_predict.fmsrms_dim_heavy_transit_info_df   -- 场地过滤  快运/小件场地
					where inc_day = '20230905'  -- 写死日期
					group by dept_code
				) t2  
			on t1.next_site_code = t2.deptCode
			left join 
				(
					select 
						requireId  
					from dm_ordi_predict.dmpdsp_vt_has_arrive_cars_batch_df  -- 顺丰车辆数据过滤
					where inc_day = '20230905'  -- 写死日期
					and actualTime  > '$[time(yyyy-MM-dd 01:00:00,-30d)]'  -- 回刷手动指定-小时
					and actualTime <= '$[time(yyyy-MM-dd 01:00:00)]'  -- 回刷手动指定-小时
					group by requireId
				) t3  
			on t1.shift_no = t3.requireId
			where t2.deptCode is not null 
			and t3.requireId is null 
			and if(
					datediff(t1.first_unlaod_time,FROM_UNIXTIME(UNIX_TIMESTAMP()))>365,
					if(datediff(t1.in_confirm_time,FROM_UNIXTIME(UNIX_TIMESTAMP()))<-365,t1.standard_plan_arrival_time,t1.in_confirm_time),
					t1.first_unlaod_time 
				) 
                 between '$[time(yyyy-MM-dd 00:00:00,-3d)]' and '$[time(yyyy-MM-dd 00:00:00,+5d)]' 
         
		)t4 
		left join 
		-- 读取班次信息
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
		)t5 
		on t4.next_site_code=t5.operate_zone_code
	)t6 
)t7 where t7.rn=1 

union all 
select 
	shift_no,
	site_code,
	next_site_code,
	total_out_weight,
	total_out_ewbcount,
	total_out_piece,
	total_out_vol,
	ewblist_send_time,
	in_confirm_time,
	first_unlaod_time,
	standard_plan_arrival_time,
	pre_arrive_time,
	transport_level,
	case vehicle_status
	  when '0' then '1'
	  when '1' then '2'
	else '3' end status,
	batch_code,
	if(batch_date is not null,date_format(batch_date, 'yyyyMMdd'),batch_date) as batch_date,
    '$[time(yyyy-MM-dd 02:00:00)]' as count_time,  -- 回刷手动指定-小时
    '$[time(yyyyMMdd)]' as inc_day,  -- 写入天分区
    '0200' as inc_hour                   -- 回刷手动指定-小时
from(
	select 
		shift_no,
		site_code,
		next_site_code,
		total_out_weight,
		total_out_ewbcount,
		total_out_piece,
		total_out_vol,
		first_unlaod_time,
		standard_plan_arrival_time,
		in_confirm_time,
		pre_arrive_time,
		transport_level,
		ewblist_send_time,
		vehicle_status,
		max(batch_code) over(partition by shift_no, site_code, next_site_code) batch_code,
		max(batch_date) over(partition by shift_no, site_code, next_site_code) batch_date,
		row_number() over(partition by shift_no,site_code,next_site_code order by pre_arrive_time) rn
	from(	
		select 
			shift_no,
			site_code,
			next_site_code,
			total_out_weight,
			total_out_ewbcount,
			total_out_piece,
			total_out_vol,
			first_unlaod_time,
			standard_plan_arrival_time,
			in_confirm_time,
			pre_arrive_time,
			arrTm,
			transport_level,
			ewblist_send_time,
			vehicle_status,
			if((t4.arrTm >= t5.last_last_arrive_tm and t4.arrTm < t5.last_arrive_tm ),batch_code,null) batch_code,
			if((t4.arrTm >= t5.last_last_arrive_tm and t4.arrTm < t5.last_arrive_tm ),batch_date,null) batch_date
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
				if(
					datediff(t1.first_unlaod_time,FROM_UNIXTIME(UNIX_TIMESTAMP()))>365,
					if(datediff(t1.in_confirm_time,FROM_UNIXTIME(UNIX_TIMESTAMP()))<-365,t1.standard_plan_arrival_time,t1.in_confirm_time),
					t1.first_unlaod_time 
				) as pre_arrive_time,
				date_format(
					if(
						datediff(t1.first_unlaod_time,FROM_UNIXTIME(UNIX_TIMESTAMP()))>365,
						if(datediff(t1.in_confirm_time,FROM_UNIXTIME(UNIX_TIMESTAMP()))<-365,t1.standard_plan_arrival_time,t1.in_confirm_time),
						t1.first_unlaod_time 
					),'yyyy-MM-dd HH:mm'
				) as arrTm,
				cast(t1.transport_level as int) transport_level,
				t1.ewblist_send_time,
				t1.vehicle_status
			from
				(select
					shift_no, 
					site_code,
					next_site_code,
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
				from ky.dm_freight.dwd_sx_vehicle_dtl_di     -- 读取SX车辆实时进港明细
				where 
                    inc_day between '$[time(yyyyMMdd,-14d)]' and '$[time(yyyyMMdd)]' 
                    and ewblist_send_time between '$[time(yyyy-MM-dd 00:00:00,-14d)]' and  '$[time(yyyy-MM-dd 02:00:00)]' 	-- 回刷手动指定-小时 (约束时点数据) 
				group by shift_no, site_code,next_site_code   
				) t1 
			left join 
				(
					select 
						dept_code as deptCode 
					from dm_ordi_predict.fmsrms_dim_heavy_transit_info_df   -- 场地过滤  快运/小件场地
					where inc_day = '20230905'  -- 写死日期
					group by dept_code
				) t2  
			on t1.next_site_code = t2.deptCode
			left join 
				(
					select 
						requireId  
					from dm_ordi_predict.dmpdsp_vt_has_arrive_cars_batch_df  -- 顺丰车辆数据过滤
					where inc_day = '20230905'  -- 写死日期
					and actualTime  > '$[time(yyyy-MM-dd 02:00:00,-30d)]'  -- 回刷手动指定-小时
					and actualTime <= '$[time(yyyy-MM-dd 02:00:00)]'  -- 回刷手动指定-小时
					group by requireId
				) t3  
			on t1.shift_no = t3.requireId
			where t2.deptCode is not null 
			and t3.requireId is null 
			and if(
					datediff(t1.first_unlaod_time,FROM_UNIXTIME(UNIX_TIMESTAMP()))>365,
					if(datediff(t1.in_confirm_time,FROM_UNIXTIME(UNIX_TIMESTAMP()))<-365,t1.standard_plan_arrival_time,t1.in_confirm_time),
					t1.first_unlaod_time 
				) 
                 between '$[time(yyyy-MM-dd 00:00:00,-3d)]' and '$[time(yyyy-MM-dd 00:00:00,+5d)]' 
         
		)t4 
		left join 
		-- 读取班次信息
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
		)t5 
		on t4.next_site_code=t5.operate_zone_code
	)t6 
)t7 where t7.rn=1 

union all 
select 
	shift_no,
	site_code,
	next_site_code,
	total_out_weight,
	total_out_ewbcount,
	total_out_piece,
	total_out_vol,
	ewblist_send_time,
	in_confirm_time,
	first_unlaod_time,
	standard_plan_arrival_time,
	pre_arrive_time,
	transport_level,
	case vehicle_status
	  when '0' then '1'
	  when '1' then '2'
	else '3' end status,
	batch_code,
	if(batch_date is not null,date_format(batch_date, 'yyyyMMdd'),batch_date) as batch_date,
    '$[time(yyyy-MM-dd 03:00:00)]' as count_time,  -- 回刷手动指定-小时
    '$[time(yyyyMMdd)]' as inc_day,  -- 写入天分区
    '0300' as inc_hour                   -- 回刷手动指定-小时
from(
	select 
		shift_no,
		site_code,
		next_site_code,
		total_out_weight,
		total_out_ewbcount,
		total_out_piece,
		total_out_vol,
		first_unlaod_time,
		standard_plan_arrival_time,
		in_confirm_time,
		pre_arrive_time,
		transport_level,
		ewblist_send_time,
		vehicle_status,
		max(batch_code) over(partition by shift_no, site_code, next_site_code) batch_code,
		max(batch_date) over(partition by shift_no, site_code, next_site_code) batch_date,
		row_number() over(partition by shift_no,site_code,next_site_code order by pre_arrive_time) rn
	from(	
		select 
			shift_no,
			site_code,
			next_site_code,
			total_out_weight,
			total_out_ewbcount,
			total_out_piece,
			total_out_vol,
			first_unlaod_time,
			standard_plan_arrival_time,
			in_confirm_time,
			pre_arrive_time,
			arrTm,
			transport_level,
			ewblist_send_time,
			vehicle_status,
			if((t4.arrTm >= t5.last_last_arrive_tm and t4.arrTm < t5.last_arrive_tm ),batch_code,null) batch_code,
			if((t4.arrTm >= t5.last_last_arrive_tm and t4.arrTm < t5.last_arrive_tm ),batch_date,null) batch_date
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
				if(
					datediff(t1.first_unlaod_time,FROM_UNIXTIME(UNIX_TIMESTAMP()))>365,
					if(datediff(t1.in_confirm_time,FROM_UNIXTIME(UNIX_TIMESTAMP()))<-365,t1.standard_plan_arrival_time,t1.in_confirm_time),
					t1.first_unlaod_time 
				) as pre_arrive_time,
				date_format(
					if(
						datediff(t1.first_unlaod_time,FROM_UNIXTIME(UNIX_TIMESTAMP()))>365,
						if(datediff(t1.in_confirm_time,FROM_UNIXTIME(UNIX_TIMESTAMP()))<-365,t1.standard_plan_arrival_time,t1.in_confirm_time),
						t1.first_unlaod_time 
					),'yyyy-MM-dd HH:mm'
				) as arrTm,
				cast(t1.transport_level as int) transport_level,
				t1.ewblist_send_time,
				t1.vehicle_status
			from
				(select
					shift_no, 
					site_code,
					next_site_code,
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
				from ky.dm_freight.dwd_sx_vehicle_dtl_di     -- 读取SX车辆实时进港明细
				where 
                    inc_day between '$[time(yyyyMMdd,-14d)]' and '$[time(yyyyMMdd)]' 
                    and ewblist_send_time between '$[time(yyyy-MM-dd 00:00:00,-14d)]' and  '$[time(yyyy-MM-dd 03:00:00)]' 	-- 回刷手动指定-小时 (约束时点数据) 
				group by shift_no, site_code,next_site_code   
				) t1 
			left join 
				(
					select 
						dept_code as deptCode 
					from dm_ordi_predict.fmsrms_dim_heavy_transit_info_df   -- 场地过滤  快运/小件场地
					where inc_day = '20230905'  -- 写死日期
					group by dept_code
				) t2  
			on t1.next_site_code = t2.deptCode
			left join 
				(
					select 
						requireId  
					from dm_ordi_predict.dmpdsp_vt_has_arrive_cars_batch_df  -- 顺丰车辆数据过滤
					where inc_day = '20230905'  -- 写死日期
					and actualTime  > '$[time(yyyy-MM-dd 03:00:00,-30d)]'  -- 回刷手动指定-小时
					and actualTime <= '$[time(yyyy-MM-dd 03:00:00)]'  -- 回刷手动指定-小时
					group by requireId
				) t3  
			on t1.shift_no = t3.requireId
			where t2.deptCode is not null 
			and t3.requireId is null 
			and if(
					datediff(t1.first_unlaod_time,FROM_UNIXTIME(UNIX_TIMESTAMP()))>365,
					if(datediff(t1.in_confirm_time,FROM_UNIXTIME(UNIX_TIMESTAMP()))<-365,t1.standard_plan_arrival_time,t1.in_confirm_time),
					t1.first_unlaod_time 
				) 
                 between '$[time(yyyy-MM-dd 00:00:00,-3d)]' and '$[time(yyyy-MM-dd 00:00:00,+5d)]' 
         
		)t4 
		left join 
		-- 读取班次信息
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
		)t5 
		on t4.next_site_code=t5.operate_zone_code
	)t6 
)t7 where t7.rn=1 

union all 
select 
	shift_no,
	site_code,
	next_site_code,
	total_out_weight,
	total_out_ewbcount,
	total_out_piece,
	total_out_vol,
	ewblist_send_time,
	in_confirm_time,
	first_unlaod_time,
	standard_plan_arrival_time,
	pre_arrive_time,
	transport_level,
	case vehicle_status
	  when '0' then '1'
	  when '1' then '2'
	else '3' end status,
	batch_code,
	if(batch_date is not null,date_format(batch_date, 'yyyyMMdd'),batch_date) as batch_date,
    '$[time(yyyy-MM-dd 04:00:00)]' as count_time,  -- 回刷手动指定-小时
    '$[time(yyyyMMdd)]' as inc_day,  -- 写入天分区
    '0400' as inc_hour                   -- 回刷手动指定-小时
from(
	select 
		shift_no,
		site_code,
		next_site_code,
		total_out_weight,
		total_out_ewbcount,
		total_out_piece,
		total_out_vol,
		first_unlaod_time,
		standard_plan_arrival_time,
		in_confirm_time,
		pre_arrive_time,
		transport_level,
		ewblist_send_time,
		vehicle_status,
		max(batch_code) over(partition by shift_no, site_code, next_site_code) batch_code,
		max(batch_date) over(partition by shift_no, site_code, next_site_code) batch_date,
		row_number() over(partition by shift_no,site_code,next_site_code order by pre_arrive_time) rn
	from(	
		select 
			shift_no,
			site_code,
			next_site_code,
			total_out_weight,
			total_out_ewbcount,
			total_out_piece,
			total_out_vol,
			first_unlaod_time,
			standard_plan_arrival_time,
			in_confirm_time,
			pre_arrive_time,
			arrTm,
			transport_level,
			ewblist_send_time,
			vehicle_status,
			if((t4.arrTm >= t5.last_last_arrive_tm and t4.arrTm < t5.last_arrive_tm ),batch_code,null) batch_code,
			if((t4.arrTm >= t5.last_last_arrive_tm and t4.arrTm < t5.last_arrive_tm ),batch_date,null) batch_date
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
				if(
					datediff(t1.first_unlaod_time,FROM_UNIXTIME(UNIX_TIMESTAMP()))>365,
					if(datediff(t1.in_confirm_time,FROM_UNIXTIME(UNIX_TIMESTAMP()))<-365,t1.standard_plan_arrival_time,t1.in_confirm_time),
					t1.first_unlaod_time 
				) as pre_arrive_time,
				date_format(
					if(
						datediff(t1.first_unlaod_time,FROM_UNIXTIME(UNIX_TIMESTAMP()))>365,
						if(datediff(t1.in_confirm_time,FROM_UNIXTIME(UNIX_TIMESTAMP()))<-365,t1.standard_plan_arrival_time,t1.in_confirm_time),
						t1.first_unlaod_time 
					),'yyyy-MM-dd HH:mm'
				) as arrTm,
				cast(t1.transport_level as int) transport_level,
				t1.ewblist_send_time,
				t1.vehicle_status
			from
				(select
					shift_no, 
					site_code,
					next_site_code,
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
				from ky.dm_freight.dwd_sx_vehicle_dtl_di     -- 读取SX车辆实时进港明细
				where 
                    inc_day between '$[time(yyyyMMdd,-14d)]' and '$[time(yyyyMMdd)]' 
                    and ewblist_send_time between '$[time(yyyy-MM-dd 00:00:00,-14d)]' and  '$[time(yyyy-MM-dd 04:00:00)]' 	-- 回刷手动指定-小时 (约束时点数据) 
				group by shift_no, site_code,next_site_code   
				) t1 
			left join 
				(
					select 
						dept_code as deptCode 
					from dm_ordi_predict.fmsrms_dim_heavy_transit_info_df   -- 场地过滤  快运/小件场地
					where inc_day = '20230905'  -- 写死日期
					group by dept_code
				) t2  
			on t1.next_site_code = t2.deptCode
			left join 
				(
					select 
						requireId  
					from dm_ordi_predict.dmpdsp_vt_has_arrive_cars_batch_df  -- 顺丰车辆数据过滤
					where inc_day = '20230905'  -- 写死日期
					and actualTime  > '$[time(yyyy-MM-dd 04:00:00,-30d)]'  -- 回刷手动指定-小时
					and actualTime <= '$[time(yyyy-MM-dd 04:00:00)]'  -- 回刷手动指定-小时
					group by requireId
				) t3  
			on t1.shift_no = t3.requireId
			where t2.deptCode is not null 
			and t3.requireId is null 
			and if(
					datediff(t1.first_unlaod_time,FROM_UNIXTIME(UNIX_TIMESTAMP()))>365,
					if(datediff(t1.in_confirm_time,FROM_UNIXTIME(UNIX_TIMESTAMP()))<-365,t1.standard_plan_arrival_time,t1.in_confirm_time),
					t1.first_unlaod_time 
				) 
                 between '$[time(yyyy-MM-dd 00:00:00,-3d)]' and '$[time(yyyy-MM-dd 00:00:00,+5d)]' 
         
		)t4 
		left join 
		-- 读取班次信息
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
		)t5 
		on t4.next_site_code=t5.operate_zone_code
	)t6 
)t7 where t7.rn=1 

union all 
select 
	shift_no,
	site_code,
	next_site_code,
	total_out_weight,
	total_out_ewbcount,
	total_out_piece,
	total_out_vol,
	ewblist_send_time,
	in_confirm_time,
	first_unlaod_time,
	standard_plan_arrival_time,
	pre_arrive_time,
	transport_level,
	case vehicle_status
	  when '0' then '1'
	  when '1' then '2'
	else '3' end status,
	batch_code,
	if(batch_date is not null,date_format(batch_date, 'yyyyMMdd'),batch_date) as batch_date,
    '$[time(yyyy-MM-dd 05:00:00)]' as count_time,  -- 回刷手动指定-小时
    '$[time(yyyyMMdd)]' as inc_day,  -- 写入天分区
    '0500' as inc_hour                   -- 回刷手动指定-小时
from(
	select 
		shift_no,
		site_code,
		next_site_code,
		total_out_weight,
		total_out_ewbcount,
		total_out_piece,
		total_out_vol,
		first_unlaod_time,
		standard_plan_arrival_time,
		in_confirm_time,
		pre_arrive_time,
		transport_level,
		ewblist_send_time,
		vehicle_status,
		max(batch_code) over(partition by shift_no, site_code, next_site_code) batch_code,
		max(batch_date) over(partition by shift_no, site_code, next_site_code) batch_date,
		row_number() over(partition by shift_no,site_code,next_site_code order by pre_arrive_time) rn
	from(	
		select 
			shift_no,
			site_code,
			next_site_code,
			total_out_weight,
			total_out_ewbcount,
			total_out_piece,
			total_out_vol,
			first_unlaod_time,
			standard_plan_arrival_time,
			in_confirm_time,
			pre_arrive_time,
			arrTm,
			transport_level,
			ewblist_send_time,
			vehicle_status,
			if((t4.arrTm >= t5.last_last_arrive_tm and t4.arrTm < t5.last_arrive_tm ),batch_code,null) batch_code,
			if((t4.arrTm >= t5.last_last_arrive_tm and t4.arrTm < t5.last_arrive_tm ),batch_date,null) batch_date
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
				if(
					datediff(t1.first_unlaod_time,FROM_UNIXTIME(UNIX_TIMESTAMP()))>365,
					if(datediff(t1.in_confirm_time,FROM_UNIXTIME(UNIX_TIMESTAMP()))<-365,t1.standard_plan_arrival_time,t1.in_confirm_time),
					t1.first_unlaod_time 
				) as pre_arrive_time,
				date_format(
					if(
						datediff(t1.first_unlaod_time,FROM_UNIXTIME(UNIX_TIMESTAMP()))>365,
						if(datediff(t1.in_confirm_time,FROM_UNIXTIME(UNIX_TIMESTAMP()))<-365,t1.standard_plan_arrival_time,t1.in_confirm_time),
						t1.first_unlaod_time 
					),'yyyy-MM-dd HH:mm'
				) as arrTm,
				cast(t1.transport_level as int) transport_level,
				t1.ewblist_send_time,
				t1.vehicle_status
			from
				(select
					shift_no, 
					site_code,
					next_site_code,
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
				from ky.dm_freight.dwd_sx_vehicle_dtl_di     -- 读取SX车辆实时进港明细
				where 
                    inc_day between '$[time(yyyyMMdd,-14d)]' and '$[time(yyyyMMdd)]' 
                    and ewblist_send_time between '$[time(yyyy-MM-dd 00:00:00,-14d)]' and  '$[time(yyyy-MM-dd 05:00:00)]' 	-- 回刷手动指定-小时 (约束时点数据) 
				group by shift_no, site_code,next_site_code   
				) t1 
			left join 
				(
					select 
						dept_code as deptCode 
					from dm_ordi_predict.fmsrms_dim_heavy_transit_info_df   -- 场地过滤  快运/小件场地
					where inc_day = '20230905'  -- 写死日期
					group by dept_code
				) t2  
			on t1.next_site_code = t2.deptCode
			left join 
				(
					select 
						requireId  
					from dm_ordi_predict.dmpdsp_vt_has_arrive_cars_batch_df  -- 顺丰车辆数据过滤
					where inc_day = '20230905'  -- 写死日期
					and actualTime  > '$[time(yyyy-MM-dd 05:00:00,-30d)]'  -- 回刷手动指定-小时
					and actualTime <= '$[time(yyyy-MM-dd 05:00:00)]'  -- 回刷手动指定-小时
					group by requireId
				) t3  
			on t1.shift_no = t3.requireId
			where t2.deptCode is not null 
			and t3.requireId is null 
			and if(
					datediff(t1.first_unlaod_time,FROM_UNIXTIME(UNIX_TIMESTAMP()))>365,
					if(datediff(t1.in_confirm_time,FROM_UNIXTIME(UNIX_TIMESTAMP()))<-365,t1.standard_plan_arrival_time,t1.in_confirm_time),
					t1.first_unlaod_time 
				) 
                 between '$[time(yyyy-MM-dd 00:00:00,-3d)]' and '$[time(yyyy-MM-dd 00:00:00,+5d)]' 
         
		)t4 
		left join 
		-- 读取班次信息
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
		)t5 
		on t4.next_site_code=t5.operate_zone_code
	)t6 
)t7 where t7.rn=1 

union all 
select 
	shift_no,
	site_code,
	next_site_code,
	total_out_weight,
	total_out_ewbcount,
	total_out_piece,
	total_out_vol,
	ewblist_send_time,
	in_confirm_time,
	first_unlaod_time,
	standard_plan_arrival_time,
	pre_arrive_time,
	transport_level,
	case vehicle_status
	  when '0' then '1'
	  when '1' then '2'
	else '3' end status,
	batch_code,
	if(batch_date is not null,date_format(batch_date, 'yyyyMMdd'),batch_date) as batch_date,
    '$[time(yyyy-MM-dd 06:00:00)]' as count_time,  -- 回刷手动指定-小时
    '$[time(yyyyMMdd)]' as inc_day,  -- 写入天分区
    '0600' as inc_hour                   -- 回刷手动指定-小时
from(
	select 
		shift_no,
		site_code,
		next_site_code,
		total_out_weight,
		total_out_ewbcount,
		total_out_piece,
		total_out_vol,
		first_unlaod_time,
		standard_plan_arrival_time,
		in_confirm_time,
		pre_arrive_time,
		transport_level,
		ewblist_send_time,
		vehicle_status,
		max(batch_code) over(partition by shift_no, site_code, next_site_code) batch_code,
		max(batch_date) over(partition by shift_no, site_code, next_site_code) batch_date,
		row_number() over(partition by shift_no,site_code,next_site_code order by pre_arrive_time) rn
	from(	
		select 
			shift_no,
			site_code,
			next_site_code,
			total_out_weight,
			total_out_ewbcount,
			total_out_piece,
			total_out_vol,
			first_unlaod_time,
			standard_plan_arrival_time,
			in_confirm_time,
			pre_arrive_time,
			arrTm,
			transport_level,
			ewblist_send_time,
			vehicle_status,
			if((t4.arrTm >= t5.last_last_arrive_tm and t4.arrTm < t5.last_arrive_tm ),batch_code,null) batch_code,
			if((t4.arrTm >= t5.last_last_arrive_tm and t4.arrTm < t5.last_arrive_tm ),batch_date,null) batch_date
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
				if(
					datediff(t1.first_unlaod_time,FROM_UNIXTIME(UNIX_TIMESTAMP()))>365,
					if(datediff(t1.in_confirm_time,FROM_UNIXTIME(UNIX_TIMESTAMP()))<-365,t1.standard_plan_arrival_time,t1.in_confirm_time),
					t1.first_unlaod_time 
				) as pre_arrive_time,
				date_format(
					if(
						datediff(t1.first_unlaod_time,FROM_UNIXTIME(UNIX_TIMESTAMP()))>365,
						if(datediff(t1.in_confirm_time,FROM_UNIXTIME(UNIX_TIMESTAMP()))<-365,t1.standard_plan_arrival_time,t1.in_confirm_time),
						t1.first_unlaod_time 
					),'yyyy-MM-dd HH:mm'
				) as arrTm,
				cast(t1.transport_level as int) transport_level,
				t1.ewblist_send_time,
				t1.vehicle_status
			from
				(select
					shift_no, 
					site_code,
					next_site_code,
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
				from ky.dm_freight.dwd_sx_vehicle_dtl_di     -- 读取SX车辆实时进港明细
				where 
                    inc_day between '$[time(yyyyMMdd,-14d)]' and '$[time(yyyyMMdd)]' 
                    and ewblist_send_time between '$[time(yyyy-MM-dd 00:00:00,-14d)]' and  '$[time(yyyy-MM-dd 06:00:00)]' 	-- 回刷手动指定-小时 (约束时点数据) 
				group by shift_no, site_code,next_site_code   
				) t1 
			left join 
				(
					select 
						dept_code as deptCode 
					from dm_ordi_predict.fmsrms_dim_heavy_transit_info_df   -- 场地过滤  快运/小件场地
					where inc_day = '20230905'  -- 写死日期
					group by dept_code
				) t2  
			on t1.next_site_code = t2.deptCode
			left join 
				(
					select 
						requireId  
					from dm_ordi_predict.dmpdsp_vt_has_arrive_cars_batch_df  -- 顺丰车辆数据过滤
					where inc_day = '20230905'  -- 写死日期
					and actualTime  > '$[time(yyyy-MM-dd 06:00:00,-30d)]'  -- 回刷手动指定-小时
					and actualTime <= '$[time(yyyy-MM-dd 06:00:00)]'  -- 回刷手动指定-小时
					group by requireId
				) t3  
			on t1.shift_no = t3.requireId
			where t2.deptCode is not null 
			and t3.requireId is null 
			and if(
					datediff(t1.first_unlaod_time,FROM_UNIXTIME(UNIX_TIMESTAMP()))>365,
					if(datediff(t1.in_confirm_time,FROM_UNIXTIME(UNIX_TIMESTAMP()))<-365,t1.standard_plan_arrival_time,t1.in_confirm_time),
					t1.first_unlaod_time 
				) 
                 between '$[time(yyyy-MM-dd 00:00:00,-3d)]' and '$[time(yyyy-MM-dd 00:00:00,+5d)]' 
         
		)t4 
		left join 
		-- 读取班次信息
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
		)t5 
		on t4.next_site_code=t5.operate_zone_code
	)t6 
)t7 where t7.rn=1 

union all 
select 
	shift_no,
	site_code,
	next_site_code,
	total_out_weight,
	total_out_ewbcount,
	total_out_piece,
	total_out_vol,
	ewblist_send_time,
	in_confirm_time,
	first_unlaod_time,
	standard_plan_arrival_time,
	pre_arrive_time,
	transport_level,
	case vehicle_status
	  when '0' then '1'
	  when '1' then '2'
	else '3' end status,
	batch_code,
	if(batch_date is not null,date_format(batch_date, 'yyyyMMdd'),batch_date) as batch_date,
    '$[time(yyyy-MM-dd 07:00:00)]' as count_time,  -- 回刷手动指定-小时
    '$[time(yyyyMMdd)]' as inc_day,  -- 写入天分区
    '0700' as inc_hour                   -- 回刷手动指定-小时
from(
	select 
		shift_no,
		site_code,
		next_site_code,
		total_out_weight,
		total_out_ewbcount,
		total_out_piece,
		total_out_vol,
		first_unlaod_time,
		standard_plan_arrival_time,
		in_confirm_time,
		pre_arrive_time,
		transport_level,
		ewblist_send_time,
		vehicle_status,
		max(batch_code) over(partition by shift_no, site_code, next_site_code) batch_code,
		max(batch_date) over(partition by shift_no, site_code, next_site_code) batch_date,
		row_number() over(partition by shift_no,site_code,next_site_code order by pre_arrive_time) rn
	from(	
		select 
			shift_no,
			site_code,
			next_site_code,
			total_out_weight,
			total_out_ewbcount,
			total_out_piece,
			total_out_vol,
			first_unlaod_time,
			standard_plan_arrival_time,
			in_confirm_time,
			pre_arrive_time,
			arrTm,
			transport_level,
			ewblist_send_time,
			vehicle_status,
			if((t4.arrTm >= t5.last_last_arrive_tm and t4.arrTm < t5.last_arrive_tm ),batch_code,null) batch_code,
			if((t4.arrTm >= t5.last_last_arrive_tm and t4.arrTm < t5.last_arrive_tm ),batch_date,null) batch_date
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
				if(
					datediff(t1.first_unlaod_time,FROM_UNIXTIME(UNIX_TIMESTAMP()))>365,
					if(datediff(t1.in_confirm_time,FROM_UNIXTIME(UNIX_TIMESTAMP()))<-365,t1.standard_plan_arrival_time,t1.in_confirm_time),
					t1.first_unlaod_time 
				) as pre_arrive_time,
				date_format(
					if(
						datediff(t1.first_unlaod_time,FROM_UNIXTIME(UNIX_TIMESTAMP()))>365,
						if(datediff(t1.in_confirm_time,FROM_UNIXTIME(UNIX_TIMESTAMP()))<-365,t1.standard_plan_arrival_time,t1.in_confirm_time),
						t1.first_unlaod_time 
					),'yyyy-MM-dd HH:mm'
				) as arrTm,
				cast(t1.transport_level as int) transport_level,
				t1.ewblist_send_time,
				t1.vehicle_status
			from
				(select
					shift_no, 
					site_code,
					next_site_code,
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
				from ky.dm_freight.dwd_sx_vehicle_dtl_di     -- 读取SX车辆实时进港明细
				where 
                    inc_day between '$[time(yyyyMMdd,-14d)]' and '$[time(yyyyMMdd)]' 
                    and ewblist_send_time between '$[time(yyyy-MM-dd 00:00:00,-14d)]' and  '$[time(yyyy-MM-dd 07:00:00)]' 	-- 回刷手动指定-小时 (约束时点数据) 
				group by shift_no, site_code,next_site_code   
				) t1 
			left join 
				(
					select 
						dept_code as deptCode 
					from dm_ordi_predict.fmsrms_dim_heavy_transit_info_df   -- 场地过滤  快运/小件场地
					where inc_day = '20230905'  -- 写死日期
					group by dept_code
				) t2  
			on t1.next_site_code = t2.deptCode
			left join 
				(
					select 
						requireId  
					from dm_ordi_predict.dmpdsp_vt_has_arrive_cars_batch_df  -- 顺丰车辆数据过滤
					where inc_day = '20230905'  -- 写死日期
					and actualTime  > '$[time(yyyy-MM-dd 07:00:00,-30d)]'  -- 回刷手动指定-小时
					and actualTime <= '$[time(yyyy-MM-dd 07:00:00)]'  -- 回刷手动指定-小时
					group by requireId
				) t3  
			on t1.shift_no = t3.requireId
			where t2.deptCode is not null 
			and t3.requireId is null 
			and if(
					datediff(t1.first_unlaod_time,FROM_UNIXTIME(UNIX_TIMESTAMP()))>365,
					if(datediff(t1.in_confirm_time,FROM_UNIXTIME(UNIX_TIMESTAMP()))<-365,t1.standard_plan_arrival_time,t1.in_confirm_time),
					t1.first_unlaod_time 
				) 
                 between '$[time(yyyy-MM-dd 00:00:00,-3d)]' and '$[time(yyyy-MM-dd 00:00:00,+5d)]' 
         
		)t4 
		left join 
		-- 读取班次信息
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
		)t5 
		on t4.next_site_code=t5.operate_zone_code
	)t6 
)t7 where t7.rn=1 

union all 
select 
	shift_no,
	site_code,
	next_site_code,
	total_out_weight,
	total_out_ewbcount,
	total_out_piece,
	total_out_vol,
	ewblist_send_time,
	in_confirm_time,
	first_unlaod_time,
	standard_plan_arrival_time,
	pre_arrive_time,
	transport_level,
	case vehicle_status
	  when '0' then '1'
	  when '1' then '2'
	else '3' end status,
	batch_code,
	if(batch_date is not null,date_format(batch_date, 'yyyyMMdd'),batch_date) as batch_date,
    '$[time(yyyy-MM-dd 08:00:00)]' as count_time,  -- 回刷手动指定-小时
    '$[time(yyyyMMdd)]' as inc_day,  -- 写入天分区
    '0800' as inc_hour                   -- 回刷手动指定-小时
from(
	select 
		shift_no,
		site_code,
		next_site_code,
		total_out_weight,
		total_out_ewbcount,
		total_out_piece,
		total_out_vol,
		first_unlaod_time,
		standard_plan_arrival_time,
		in_confirm_time,
		pre_arrive_time,
		transport_level,
		ewblist_send_time,
		vehicle_status,
		max(batch_code) over(partition by shift_no, site_code, next_site_code) batch_code,
		max(batch_date) over(partition by shift_no, site_code, next_site_code) batch_date,
		row_number() over(partition by shift_no,site_code,next_site_code order by pre_arrive_time) rn
	from(	
		select 
			shift_no,
			site_code,
			next_site_code,
			total_out_weight,
			total_out_ewbcount,
			total_out_piece,
			total_out_vol,
			first_unlaod_time,
			standard_plan_arrival_time,
			in_confirm_time,
			pre_arrive_time,
			arrTm,
			transport_level,
			ewblist_send_time,
			vehicle_status,
			if((t4.arrTm >= t5.last_last_arrive_tm and t4.arrTm < t5.last_arrive_tm ),batch_code,null) batch_code,
			if((t4.arrTm >= t5.last_last_arrive_tm and t4.arrTm < t5.last_arrive_tm ),batch_date,null) batch_date
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
				if(
					datediff(t1.first_unlaod_time,FROM_UNIXTIME(UNIX_TIMESTAMP()))>365,
					if(datediff(t1.in_confirm_time,FROM_UNIXTIME(UNIX_TIMESTAMP()))<-365,t1.standard_plan_arrival_time,t1.in_confirm_time),
					t1.first_unlaod_time 
				) as pre_arrive_time,
				date_format(
					if(
						datediff(t1.first_unlaod_time,FROM_UNIXTIME(UNIX_TIMESTAMP()))>365,
						if(datediff(t1.in_confirm_time,FROM_UNIXTIME(UNIX_TIMESTAMP()))<-365,t1.standard_plan_arrival_time,t1.in_confirm_time),
						t1.first_unlaod_time 
					),'yyyy-MM-dd HH:mm'
				) as arrTm,
				cast(t1.transport_level as int) transport_level,
				t1.ewblist_send_time,
				t1.vehicle_status
			from
				(select
					shift_no, 
					site_code,
					next_site_code,
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
				from ky.dm_freight.dwd_sx_vehicle_dtl_di     -- 读取SX车辆实时进港明细
				where 
                    inc_day between '$[time(yyyyMMdd,-14d)]' and '$[time(yyyyMMdd)]' 
                    and ewblist_send_time between '$[time(yyyy-MM-dd 00:00:00,-14d)]' and  '$[time(yyyy-MM-dd 08:00:00)]' 	-- 回刷手动指定-小时 (约束时点数据) 
				group by shift_no, site_code,next_site_code   
				) t1 
			left join 
				(
					select 
						dept_code as deptCode 
					from dm_ordi_predict.fmsrms_dim_heavy_transit_info_df   -- 场地过滤  快运/小件场地
					where inc_day = '20230905'  -- 写死日期
					group by dept_code
				) t2  
			on t1.next_site_code = t2.deptCode
			left join 
				(
					select 
						requireId  
					from dm_ordi_predict.dmpdsp_vt_has_arrive_cars_batch_df  -- 顺丰车辆数据过滤
					where inc_day = '20230905'  -- 写死日期
					and actualTime  > '$[time(yyyy-MM-dd 08:00:00,-30d)]'  -- 回刷手动指定-小时
					and actualTime <= '$[time(yyyy-MM-dd 08:00:00)]'  -- 回刷手动指定-小时
					group by requireId
				) t3  
			on t1.shift_no = t3.requireId
			where t2.deptCode is not null 
			and t3.requireId is null 
			and if(
					datediff(t1.first_unlaod_time,FROM_UNIXTIME(UNIX_TIMESTAMP()))>365,
					if(datediff(t1.in_confirm_time,FROM_UNIXTIME(UNIX_TIMESTAMP()))<-365,t1.standard_plan_arrival_time,t1.in_confirm_time),
					t1.first_unlaod_time 
				) 
                 between '$[time(yyyy-MM-dd 00:00:00,-3d)]' and '$[time(yyyy-MM-dd 00:00:00,+5d)]' 
         
		)t4 
		left join 
		-- 读取班次信息
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
		)t5 
		on t4.next_site_code=t5.operate_zone_code
	)t6 
)t7 where t7.rn=1 

union all 
select 
	shift_no,
	site_code,
	next_site_code,
	total_out_weight,
	total_out_ewbcount,
	total_out_piece,
	total_out_vol,
	ewblist_send_time,
	in_confirm_time,
	first_unlaod_time,
	standard_plan_arrival_time,
	pre_arrive_time,
	transport_level,
	case vehicle_status
	  when '0' then '1'
	  when '1' then '2'
	else '3' end status,
	batch_code,
	if(batch_date is not null,date_format(batch_date, 'yyyyMMdd'),batch_date) as batch_date,
    '$[time(yyyy-MM-dd 09:00:00)]' as count_time,  -- 回刷手动指定-小时
    '$[time(yyyyMMdd)]' as inc_day,  -- 写入天分区
    '0900' as inc_hour                   -- 回刷手动指定-小时
from(
	select 
		shift_no,
		site_code,
		next_site_code,
		total_out_weight,
		total_out_ewbcount,
		total_out_piece,
		total_out_vol,
		first_unlaod_time,
		standard_plan_arrival_time,
		in_confirm_time,
		pre_arrive_time,
		transport_level,
		ewblist_send_time,
		vehicle_status,
		max(batch_code) over(partition by shift_no, site_code, next_site_code) batch_code,
		max(batch_date) over(partition by shift_no, site_code, next_site_code) batch_date,
		row_number() over(partition by shift_no,site_code,next_site_code order by pre_arrive_time) rn
	from(	
		select 
			shift_no,
			site_code,
			next_site_code,
			total_out_weight,
			total_out_ewbcount,
			total_out_piece,
			total_out_vol,
			first_unlaod_time,
			standard_plan_arrival_time,
			in_confirm_time,
			pre_arrive_time,
			arrTm,
			transport_level,
			ewblist_send_time,
			vehicle_status,
			if((t4.arrTm >= t5.last_last_arrive_tm and t4.arrTm < t5.last_arrive_tm ),batch_code,null) batch_code,
			if((t4.arrTm >= t5.last_last_arrive_tm and t4.arrTm < t5.last_arrive_tm ),batch_date,null) batch_date
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
				if(
					datediff(t1.first_unlaod_time,FROM_UNIXTIME(UNIX_TIMESTAMP()))>365,
					if(datediff(t1.in_confirm_time,FROM_UNIXTIME(UNIX_TIMESTAMP()))<-365,t1.standard_plan_arrival_time,t1.in_confirm_time),
					t1.first_unlaod_time 
				) as pre_arrive_time,
				date_format(
					if(
						datediff(t1.first_unlaod_time,FROM_UNIXTIME(UNIX_TIMESTAMP()))>365,
						if(datediff(t1.in_confirm_time,FROM_UNIXTIME(UNIX_TIMESTAMP()))<-365,t1.standard_plan_arrival_time,t1.in_confirm_time),
						t1.first_unlaod_time 
					),'yyyy-MM-dd HH:mm'
				) as arrTm,
				cast(t1.transport_level as int) transport_level,
				t1.ewblist_send_time,
				t1.vehicle_status
			from
				(select
					shift_no, 
					site_code,
					next_site_code,
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
				from ky.dm_freight.dwd_sx_vehicle_dtl_di     -- 读取SX车辆实时进港明细
				where 
                    inc_day between '$[time(yyyyMMdd,-14d)]' and '$[time(yyyyMMdd)]' 
                    and ewblist_send_time between '$[time(yyyy-MM-dd 00:00:00,-14d)]' and  '$[time(yyyy-MM-dd 09:00:00)]' 	-- 回刷手动指定-小时 (约束时点数据) 
				group by shift_no, site_code,next_site_code   
				) t1 
			left join 
				(
					select 
						dept_code as deptCode 
					from dm_ordi_predict.fmsrms_dim_heavy_transit_info_df   -- 场地过滤  快运/小件场地
					where inc_day = '20230905'  -- 写死日期
					group by dept_code
				) t2  
			on t1.next_site_code = t2.deptCode
			left join 
				(
					select 
						requireId  
					from dm_ordi_predict.dmpdsp_vt_has_arrive_cars_batch_df  -- 顺丰车辆数据过滤
					where inc_day = '20230905'  -- 写死日期
					and actualTime  > '$[time(yyyy-MM-dd 09:00:00,-30d)]'  -- 回刷手动指定-小时
					and actualTime <= '$[time(yyyy-MM-dd 09:00:00)]'  -- 回刷手动指定-小时
					group by requireId
				) t3  
			on t1.shift_no = t3.requireId
			where t2.deptCode is not null 
			and t3.requireId is null 
			and if(
					datediff(t1.first_unlaod_time,FROM_UNIXTIME(UNIX_TIMESTAMP()))>365,
					if(datediff(t1.in_confirm_time,FROM_UNIXTIME(UNIX_TIMESTAMP()))<-365,t1.standard_plan_arrival_time,t1.in_confirm_time),
					t1.first_unlaod_time 
				) 
                 between '$[time(yyyy-MM-dd 00:00:00,-3d)]' and '$[time(yyyy-MM-dd 00:00:00,+5d)]' 
         
		)t4 
		left join 
		-- 读取班次信息
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
		)t5 
		on t4.next_site_code=t5.operate_zone_code
	)t6 
)t7 where t7.rn=1 

union all 
select 
	shift_no,
	site_code,
	next_site_code,
	total_out_weight,
	total_out_ewbcount,
	total_out_piece,
	total_out_vol,
	ewblist_send_time,
	in_confirm_time,
	first_unlaod_time,
	standard_plan_arrival_time,
	pre_arrive_time,
	transport_level,
	case vehicle_status
	  when '0' then '1'
	  when '1' then '2'
	else '3' end status,
	batch_code,
	if(batch_date is not null,date_format(batch_date, 'yyyyMMdd'),batch_date) as batch_date,
    '$[time(yyyy-MM-dd 10:00:00)]' as count_time,  -- 回刷手动指定-小时
    '$[time(yyyyMMdd)]' as inc_day,  -- 写入天分区
    '1000' as inc_hour                   -- 回刷手动指定-小时
from(
	select 
		shift_no,
		site_code,
		next_site_code,
		total_out_weight,
		total_out_ewbcount,
		total_out_piece,
		total_out_vol,
		first_unlaod_time,
		standard_plan_arrival_time,
		in_confirm_time,
		pre_arrive_time,
		transport_level,
		ewblist_send_time,
		vehicle_status,
		max(batch_code) over(partition by shift_no, site_code, next_site_code) batch_code,
		max(batch_date) over(partition by shift_no, site_code, next_site_code) batch_date,
		row_number() over(partition by shift_no,site_code,next_site_code order by pre_arrive_time) rn
	from(	
		select 
			shift_no,
			site_code,
			next_site_code,
			total_out_weight,
			total_out_ewbcount,
			total_out_piece,
			total_out_vol,
			first_unlaod_time,
			standard_plan_arrival_time,
			in_confirm_time,
			pre_arrive_time,
			arrTm,
			transport_level,
			ewblist_send_time,
			vehicle_status,
			if((t4.arrTm >= t5.last_last_arrive_tm and t4.arrTm < t5.last_arrive_tm ),batch_code,null) batch_code,
			if((t4.arrTm >= t5.last_last_arrive_tm and t4.arrTm < t5.last_arrive_tm ),batch_date,null) batch_date
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
				if(
					datediff(t1.first_unlaod_time,FROM_UNIXTIME(UNIX_TIMESTAMP()))>365,
					if(datediff(t1.in_confirm_time,FROM_UNIXTIME(UNIX_TIMESTAMP()))<-365,t1.standard_plan_arrival_time,t1.in_confirm_time),
					t1.first_unlaod_time 
				) as pre_arrive_time,
				date_format(
					if(
						datediff(t1.first_unlaod_time,FROM_UNIXTIME(UNIX_TIMESTAMP()))>365,
						if(datediff(t1.in_confirm_time,FROM_UNIXTIME(UNIX_TIMESTAMP()))<-365,t1.standard_plan_arrival_time,t1.in_confirm_time),
						t1.first_unlaod_time 
					),'yyyy-MM-dd HH:mm'
				) as arrTm,
				cast(t1.transport_level as int) transport_level,
				t1.ewblist_send_time,
				t1.vehicle_status
			from
				(select
					shift_no, 
					site_code,
					next_site_code,
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
				from ky.dm_freight.dwd_sx_vehicle_dtl_di     -- 读取SX车辆实时进港明细
				where 
                    inc_day between '$[time(yyyyMMdd,-14d)]' and '$[time(yyyyMMdd)]' 
                    and ewblist_send_time between '$[time(yyyy-MM-dd 00:00:00,-14d)]' and  '$[time(yyyy-MM-dd 10:00:00)]' 	-- 回刷手动指定-小时 (约束时点数据) 
				group by shift_no, site_code,next_site_code   
				) t1 
			left join 
				(
					select 
						dept_code as deptCode 
					from dm_ordi_predict.fmsrms_dim_heavy_transit_info_df   -- 场地过滤  快运/小件场地
					where inc_day = '20230905'  -- 写死日期
					group by dept_code
				) t2  
			on t1.next_site_code = t2.deptCode
			left join 
				(
					select 
						requireId  
					from dm_ordi_predict.dmpdsp_vt_has_arrive_cars_batch_df  -- 顺丰车辆数据过滤
					where inc_day = '20230905'  -- 写死日期
					and actualTime  > '$[time(yyyy-MM-dd 10:00:00,-30d)]'  -- 回刷手动指定-小时
					and actualTime <= '$[time(yyyy-MM-dd 10:00:00)]'  -- 回刷手动指定-小时
					group by requireId
				) t3  
			on t1.shift_no = t3.requireId
			where t2.deptCode is not null 
			and t3.requireId is null 
			and if(
					datediff(t1.first_unlaod_time,FROM_UNIXTIME(UNIX_TIMESTAMP()))>365,
					if(datediff(t1.in_confirm_time,FROM_UNIXTIME(UNIX_TIMESTAMP()))<-365,t1.standard_plan_arrival_time,t1.in_confirm_time),
					t1.first_unlaod_time 
				) 
                 between '$[time(yyyy-MM-dd 00:00:00,-3d)]' and '$[time(yyyy-MM-dd 00:00:00,+5d)]' 
         
		)t4 
		left join 
		-- 读取班次信息
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
		)t5 
		on t4.next_site_code=t5.operate_zone_code
	)t6 
)t7 where t7.rn=1 

union all 
select 
	shift_no,
	site_code,
	next_site_code,
	total_out_weight,
	total_out_ewbcount,
	total_out_piece,
	total_out_vol,
	ewblist_send_time,
	in_confirm_time,
	first_unlaod_time,
	standard_plan_arrival_time,
	pre_arrive_time,
	transport_level,
	case vehicle_status
	  when '0' then '1'
	  when '1' then '2'
	else '3' end status,
	batch_code,
	if(batch_date is not null,date_format(batch_date, 'yyyyMMdd'),batch_date) as batch_date,
    '$[time(yyyy-MM-dd 11:00:00)]' as count_time,  -- 回刷手动指定-小时
    '$[time(yyyyMMdd)]' as inc_day,  -- 写入天分区
    '1100' as inc_hour                   -- 回刷手动指定-小时
from(
	select 
		shift_no,
		site_code,
		next_site_code,
		total_out_weight,
		total_out_ewbcount,
		total_out_piece,
		total_out_vol,
		first_unlaod_time,
		standard_plan_arrival_time,
		in_confirm_time,
		pre_arrive_time,
		transport_level,
		ewblist_send_time,
		vehicle_status,
		max(batch_code) over(partition by shift_no, site_code, next_site_code) batch_code,
		max(batch_date) over(partition by shift_no, site_code, next_site_code) batch_date,
		row_number() over(partition by shift_no,site_code,next_site_code order by pre_arrive_time) rn
	from(	
		select 
			shift_no,
			site_code,
			next_site_code,
			total_out_weight,
			total_out_ewbcount,
			total_out_piece,
			total_out_vol,
			first_unlaod_time,
			standard_plan_arrival_time,
			in_confirm_time,
			pre_arrive_time,
			arrTm,
			transport_level,
			ewblist_send_time,
			vehicle_status,
			if((t4.arrTm >= t5.last_last_arrive_tm and t4.arrTm < t5.last_arrive_tm ),batch_code,null) batch_code,
			if((t4.arrTm >= t5.last_last_arrive_tm and t4.arrTm < t5.last_arrive_tm ),batch_date,null) batch_date
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
				if(
					datediff(t1.first_unlaod_time,FROM_UNIXTIME(UNIX_TIMESTAMP()))>365,
					if(datediff(t1.in_confirm_time,FROM_UNIXTIME(UNIX_TIMESTAMP()))<-365,t1.standard_plan_arrival_time,t1.in_confirm_time),
					t1.first_unlaod_time 
				) as pre_arrive_time,
				date_format(
					if(
						datediff(t1.first_unlaod_time,FROM_UNIXTIME(UNIX_TIMESTAMP()))>365,
						if(datediff(t1.in_confirm_time,FROM_UNIXTIME(UNIX_TIMESTAMP()))<-365,t1.standard_plan_arrival_time,t1.in_confirm_time),
						t1.first_unlaod_time 
					),'yyyy-MM-dd HH:mm'
				) as arrTm,
				cast(t1.transport_level as int) transport_level,
				t1.ewblist_send_time,
				t1.vehicle_status
			from
				(select
					shift_no, 
					site_code,
					next_site_code,
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
				from ky.dm_freight.dwd_sx_vehicle_dtl_di     -- 读取SX车辆实时进港明细
				where 
                    inc_day between '$[time(yyyyMMdd,-14d)]' and '$[time(yyyyMMdd)]' 
                    and ewblist_send_time between '$[time(yyyy-MM-dd 00:00:00,-14d)]' and  '$[time(yyyy-MM-dd 11:00:00)]' 	-- 回刷手动指定-小时 (约束时点数据) 
				group by shift_no, site_code,next_site_code   
				) t1 
			left join 
				(
					select 
						dept_code as deptCode 
					from dm_ordi_predict.fmsrms_dim_heavy_transit_info_df   -- 场地过滤  快运/小件场地
					where inc_day = '20230905'  -- 写死日期
					group by dept_code
				) t2  
			on t1.next_site_code = t2.deptCode
			left join 
				(
					select 
						requireId  
					from dm_ordi_predict.dmpdsp_vt_has_arrive_cars_batch_df  -- 顺丰车辆数据过滤
					where inc_day = '20230905'  -- 写死日期
					and actualTime  > '$[time(yyyy-MM-dd 11:00:00,-30d)]'  -- 回刷手动指定-小时
					and actualTime <= '$[time(yyyy-MM-dd 11:00:00)]'  -- 回刷手动指定-小时
					group by requireId
				) t3  
			on t1.shift_no = t3.requireId
			where t2.deptCode is not null 
			and t3.requireId is null 
			and if(
					datediff(t1.first_unlaod_time,FROM_UNIXTIME(UNIX_TIMESTAMP()))>365,
					if(datediff(t1.in_confirm_time,FROM_UNIXTIME(UNIX_TIMESTAMP()))<-365,t1.standard_plan_arrival_time,t1.in_confirm_time),
					t1.first_unlaod_time 
				) 
                 between '$[time(yyyy-MM-dd 00:00:00,-3d)]' and '$[time(yyyy-MM-dd 00:00:00,+5d)]' 
         
		)t4 
		left join 
		-- 读取班次信息
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
		)t5 
		on t4.next_site_code=t5.operate_zone_code
	)t6 
)t7 where t7.rn=1 

union all 
select 
	shift_no,
	site_code,
	next_site_code,
	total_out_weight,
	total_out_ewbcount,
	total_out_piece,
	total_out_vol,
	ewblist_send_time,
	in_confirm_time,
	first_unlaod_time,
	standard_plan_arrival_time,
	pre_arrive_time,
	transport_level,
	case vehicle_status
	  when '0' then '1'
	  when '1' then '2'
	else '3' end status,
	batch_code,
	if(batch_date is not null,date_format(batch_date, 'yyyyMMdd'),batch_date) as batch_date,
    '$[time(yyyy-MM-dd 12:00:00)]' as count_time,  -- 回刷手动指定-小时
    '$[time(yyyyMMdd)]' as inc_day,  -- 写入天分区
    '1200' as inc_hour                   -- 回刷手动指定-小时
from(
	select 
		shift_no,
		site_code,
		next_site_code,
		total_out_weight,
		total_out_ewbcount,
		total_out_piece,
		total_out_vol,
		first_unlaod_time,
		standard_plan_arrival_time,
		in_confirm_time,
		pre_arrive_time,
		transport_level,
		ewblist_send_time,
		vehicle_status,
		max(batch_code) over(partition by shift_no, site_code, next_site_code) batch_code,
		max(batch_date) over(partition by shift_no, site_code, next_site_code) batch_date,
		row_number() over(partition by shift_no,site_code,next_site_code order by pre_arrive_time) rn
	from(	
		select 
			shift_no,
			site_code,
			next_site_code,
			total_out_weight,
			total_out_ewbcount,
			total_out_piece,
			total_out_vol,
			first_unlaod_time,
			standard_plan_arrival_time,
			in_confirm_time,
			pre_arrive_time,
			arrTm,
			transport_level,
			ewblist_send_time,
			vehicle_status,
			if((t4.arrTm >= t5.last_last_arrive_tm and t4.arrTm < t5.last_arrive_tm ),batch_code,null) batch_code,
			if((t4.arrTm >= t5.last_last_arrive_tm and t4.arrTm < t5.last_arrive_tm ),batch_date,null) batch_date
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
				if(
					datediff(t1.first_unlaod_time,FROM_UNIXTIME(UNIX_TIMESTAMP()))>365,
					if(datediff(t1.in_confirm_time,FROM_UNIXTIME(UNIX_TIMESTAMP()))<-365,t1.standard_plan_arrival_time,t1.in_confirm_time),
					t1.first_unlaod_time 
				) as pre_arrive_time,
				date_format(
					if(
						datediff(t1.first_unlaod_time,FROM_UNIXTIME(UNIX_TIMESTAMP()))>365,
						if(datediff(t1.in_confirm_time,FROM_UNIXTIME(UNIX_TIMESTAMP()))<-365,t1.standard_plan_arrival_time,t1.in_confirm_time),
						t1.first_unlaod_time 
					),'yyyy-MM-dd HH:mm'
				) as arrTm,
				cast(t1.transport_level as int) transport_level,
				t1.ewblist_send_time,
				t1.vehicle_status
			from
				(select
					shift_no, 
					site_code,
					next_site_code,
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
				from ky.dm_freight.dwd_sx_vehicle_dtl_di     -- 读取SX车辆实时进港明细
				where 
                    inc_day between '$[time(yyyyMMdd,-14d)]' and '$[time(yyyyMMdd)]' 
                    and ewblist_send_time between '$[time(yyyy-MM-dd 00:00:00,-14d)]' and  '$[time(yyyy-MM-dd 12:00:00)]' 	-- 回刷手动指定-小时 (约束时点数据) 
				group by shift_no, site_code,next_site_code   
				) t1 
			left join 
				(
					select 
						dept_code as deptCode 
					from dm_ordi_predict.fmsrms_dim_heavy_transit_info_df   -- 场地过滤  快运/小件场地
					where inc_day = '20230905'  -- 写死日期
					group by dept_code
				) t2  
			on t1.next_site_code = t2.deptCode
			left join 
				(
					select 
						requireId  
					from dm_ordi_predict.dmpdsp_vt_has_arrive_cars_batch_df  -- 顺丰车辆数据过滤
					where inc_day = '20230905'  -- 写死日期
					and actualTime  > '$[time(yyyy-MM-dd 12:00:00,-30d)]'  -- 回刷手动指定-小时
					and actualTime <= '$[time(yyyy-MM-dd 12:00:00)]'  -- 回刷手动指定-小时
					group by requireId
				) t3  
			on t1.shift_no = t3.requireId
			where t2.deptCode is not null 
			and t3.requireId is null 
			and if(
					datediff(t1.first_unlaod_time,FROM_UNIXTIME(UNIX_TIMESTAMP()))>365,
					if(datediff(t1.in_confirm_time,FROM_UNIXTIME(UNIX_TIMESTAMP()))<-365,t1.standard_plan_arrival_time,t1.in_confirm_time),
					t1.first_unlaod_time 
				) 
                 between '$[time(yyyy-MM-dd 00:00:00,-3d)]' and '$[time(yyyy-MM-dd 00:00:00,+5d)]' 
         
		)t4 
		left join 
		-- 读取班次信息
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
		)t5 
		on t4.next_site_code=t5.operate_zone_code
	)t6 
)t7 where t7.rn=1 

union all 
select 
	shift_no,
	site_code,
	next_site_code,
	total_out_weight,
	total_out_ewbcount,
	total_out_piece,
	total_out_vol,
	ewblist_send_time,
	in_confirm_time,
	first_unlaod_time,
	standard_plan_arrival_time,
	pre_arrive_time,
	transport_level,
	case vehicle_status
	  when '0' then '1'
	  when '1' then '2'
	else '3' end status,
	batch_code,
	if(batch_date is not null,date_format(batch_date, 'yyyyMMdd'),batch_date) as batch_date,
    '$[time(yyyy-MM-dd 13:00:00)]' as count_time,  -- 回刷手动指定-小时
    '$[time(yyyyMMdd)]' as inc_day,  -- 写入天分区
    '1300' as inc_hour                   -- 回刷手动指定-小时
from(
	select 
		shift_no,
		site_code,
		next_site_code,
		total_out_weight,
		total_out_ewbcount,
		total_out_piece,
		total_out_vol,
		first_unlaod_time,
		standard_plan_arrival_time,
		in_confirm_time,
		pre_arrive_time,
		transport_level,
		ewblist_send_time,
		vehicle_status,
		max(batch_code) over(partition by shift_no, site_code, next_site_code) batch_code,
		max(batch_date) over(partition by shift_no, site_code, next_site_code) batch_date,
		row_number() over(partition by shift_no,site_code,next_site_code order by pre_arrive_time) rn
	from(	
		select 
			shift_no,
			site_code,
			next_site_code,
			total_out_weight,
			total_out_ewbcount,
			total_out_piece,
			total_out_vol,
			first_unlaod_time,
			standard_plan_arrival_time,
			in_confirm_time,
			pre_arrive_time,
			arrTm,
			transport_level,
			ewblist_send_time,
			vehicle_status,
			if((t4.arrTm >= t5.last_last_arrive_tm and t4.arrTm < t5.last_arrive_tm ),batch_code,null) batch_code,
			if((t4.arrTm >= t5.last_last_arrive_tm and t4.arrTm < t5.last_arrive_tm ),batch_date,null) batch_date
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
				if(
					datediff(t1.first_unlaod_time,FROM_UNIXTIME(UNIX_TIMESTAMP()))>365,
					if(datediff(t1.in_confirm_time,FROM_UNIXTIME(UNIX_TIMESTAMP()))<-365,t1.standard_plan_arrival_time,t1.in_confirm_time),
					t1.first_unlaod_time 
				) as pre_arrive_time,
				date_format(
					if(
						datediff(t1.first_unlaod_time,FROM_UNIXTIME(UNIX_TIMESTAMP()))>365,
						if(datediff(t1.in_confirm_time,FROM_UNIXTIME(UNIX_TIMESTAMP()))<-365,t1.standard_plan_arrival_time,t1.in_confirm_time),
						t1.first_unlaod_time 
					),'yyyy-MM-dd HH:mm'
				) as arrTm,
				cast(t1.transport_level as int) transport_level,
				t1.ewblist_send_time,
				t1.vehicle_status
			from
				(select
					shift_no, 
					site_code,
					next_site_code,
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
				from ky.dm_freight.dwd_sx_vehicle_dtl_di     -- 读取SX车辆实时进港明细
				where 
                    inc_day between '$[time(yyyyMMdd,-14d)]' and '$[time(yyyyMMdd)]' 
                    and ewblist_send_time between '$[time(yyyy-MM-dd 00:00:00,-14d)]' and  '$[time(yyyy-MM-dd 13:00:00)]' 	-- 回刷手动指定-小时 (约束时点数据) 
				group by shift_no, site_code,next_site_code   
				) t1 
			left join 
				(
					select 
						dept_code as deptCode 
					from dm_ordi_predict.fmsrms_dim_heavy_transit_info_df   -- 场地过滤  快运/小件场地
					where inc_day = '20230905'  -- 写死日期
					group by dept_code
				) t2  
			on t1.next_site_code = t2.deptCode
			left join 
				(
					select 
						requireId  
					from dm_ordi_predict.dmpdsp_vt_has_arrive_cars_batch_df  -- 顺丰车辆数据过滤
					where inc_day = '20230905'  -- 写死日期
					and actualTime  > '$[time(yyyy-MM-dd 13:00:00,-30d)]'  -- 回刷手动指定-小时
					and actualTime <= '$[time(yyyy-MM-dd 13:00:00)]'  -- 回刷手动指定-小时
					group by requireId
				) t3  
			on t1.shift_no = t3.requireId
			where t2.deptCode is not null 
			and t3.requireId is null 
			and if(
					datediff(t1.first_unlaod_time,FROM_UNIXTIME(UNIX_TIMESTAMP()))>365,
					if(datediff(t1.in_confirm_time,FROM_UNIXTIME(UNIX_TIMESTAMP()))<-365,t1.standard_plan_arrival_time,t1.in_confirm_time),
					t1.first_unlaod_time 
				) 
                 between '$[time(yyyy-MM-dd 00:00:00,-3d)]' and '$[time(yyyy-MM-dd 00:00:00,+5d)]' 
         
		)t4 
		left join 
		-- 读取班次信息
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
		)t5 
		on t4.next_site_code=t5.operate_zone_code
	)t6 
)t7 where t7.rn=1 

union all 
select 
	shift_no,
	site_code,
	next_site_code,
	total_out_weight,
	total_out_ewbcount,
	total_out_piece,
	total_out_vol,
	ewblist_send_time,
	in_confirm_time,
	first_unlaod_time,
	standard_plan_arrival_time,
	pre_arrive_time,
	transport_level,
	case vehicle_status
	  when '0' then '1'
	  when '1' then '2'
	else '3' end status,
	batch_code,
	if(batch_date is not null,date_format(batch_date, 'yyyyMMdd'),batch_date) as batch_date,
    '$[time(yyyy-MM-dd 14:00:00)]' as count_time,  -- 回刷手动指定-小时
    '$[time(yyyyMMdd)]' as inc_day,  -- 写入天分区
    '1400' as inc_hour                   -- 回刷手动指定-小时
from(
	select 
		shift_no,
		site_code,
		next_site_code,
		total_out_weight,
		total_out_ewbcount,
		total_out_piece,
		total_out_vol,
		first_unlaod_time,
		standard_plan_arrival_time,
		in_confirm_time,
		pre_arrive_time,
		transport_level,
		ewblist_send_time,
		vehicle_status,
		max(batch_code) over(partition by shift_no, site_code, next_site_code) batch_code,
		max(batch_date) over(partition by shift_no, site_code, next_site_code) batch_date,
		row_number() over(partition by shift_no,site_code,next_site_code order by pre_arrive_time) rn
	from(	
		select 
			shift_no,
			site_code,
			next_site_code,
			total_out_weight,
			total_out_ewbcount,
			total_out_piece,
			total_out_vol,
			first_unlaod_time,
			standard_plan_arrival_time,
			in_confirm_time,
			pre_arrive_time,
			arrTm,
			transport_level,
			ewblist_send_time,
			vehicle_status,
			if((t4.arrTm >= t5.last_last_arrive_tm and t4.arrTm < t5.last_arrive_tm ),batch_code,null) batch_code,
			if((t4.arrTm >= t5.last_last_arrive_tm and t4.arrTm < t5.last_arrive_tm ),batch_date,null) batch_date
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
				if(
					datediff(t1.first_unlaod_time,FROM_UNIXTIME(UNIX_TIMESTAMP()))>365,
					if(datediff(t1.in_confirm_time,FROM_UNIXTIME(UNIX_TIMESTAMP()))<-365,t1.standard_plan_arrival_time,t1.in_confirm_time),
					t1.first_unlaod_time 
				) as pre_arrive_time,
				date_format(
					if(
						datediff(t1.first_unlaod_time,FROM_UNIXTIME(UNIX_TIMESTAMP()))>365,
						if(datediff(t1.in_confirm_time,FROM_UNIXTIME(UNIX_TIMESTAMP()))<-365,t1.standard_plan_arrival_time,t1.in_confirm_time),
						t1.first_unlaod_time 
					),'yyyy-MM-dd HH:mm'
				) as arrTm,
				cast(t1.transport_level as int) transport_level,
				t1.ewblist_send_time,
				t1.vehicle_status
			from
				(select
					shift_no, 
					site_code,
					next_site_code,
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
				from ky.dm_freight.dwd_sx_vehicle_dtl_di     -- 读取SX车辆实时进港明细
				where 
                    inc_day between '$[time(yyyyMMdd,-14d)]' and '$[time(yyyyMMdd)]' 
                    and ewblist_send_time between '$[time(yyyy-MM-dd 00:00:00,-14d)]' and  '$[time(yyyy-MM-dd 14:00:00)]' 	-- 回刷手动指定-小时 (约束时点数据) 
				group by shift_no, site_code,next_site_code   
				) t1 
			left join 
				(
					select 
						dept_code as deptCode 
					from dm_ordi_predict.fmsrms_dim_heavy_transit_info_df   -- 场地过滤  快运/小件场地
					where inc_day = '20230905'  -- 写死日期
					group by dept_code
				) t2  
			on t1.next_site_code = t2.deptCode
			left join 
				(
					select 
						requireId  
					from dm_ordi_predict.dmpdsp_vt_has_arrive_cars_batch_df  -- 顺丰车辆数据过滤
					where inc_day = '20230905'  -- 写死日期
					and actualTime  > '$[time(yyyy-MM-dd 14:00:00,-30d)]'  -- 回刷手动指定-小时
					and actualTime <= '$[time(yyyy-MM-dd 14:00:00)]'  -- 回刷手动指定-小时
					group by requireId
				) t3  
			on t1.shift_no = t3.requireId
			where t2.deptCode is not null 
			and t3.requireId is null 
			and if(
					datediff(t1.first_unlaod_time,FROM_UNIXTIME(UNIX_TIMESTAMP()))>365,
					if(datediff(t1.in_confirm_time,FROM_UNIXTIME(UNIX_TIMESTAMP()))<-365,t1.standard_plan_arrival_time,t1.in_confirm_time),
					t1.first_unlaod_time 
				) 
                 between '$[time(yyyy-MM-dd 00:00:00,-3d)]' and '$[time(yyyy-MM-dd 00:00:00,+5d)]' 
         
		)t4 
		left join 
		-- 读取班次信息
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
		)t5 
		on t4.next_site_code=t5.operate_zone_code
	)t6 
)t7 where t7.rn=1 

union all 
select 
	shift_no,
	site_code,
	next_site_code,
	total_out_weight,
	total_out_ewbcount,
	total_out_piece,
	total_out_vol,
	ewblist_send_time,
	in_confirm_time,
	first_unlaod_time,
	standard_plan_arrival_time,
	pre_arrive_time,
	transport_level,
	case vehicle_status
	  when '0' then '1'
	  when '1' then '2'
	else '3' end status,
	batch_code,
	if(batch_date is not null,date_format(batch_date, 'yyyyMMdd'),batch_date) as batch_date,
    '$[time(yyyy-MM-dd 15:00:00)]' as count_time,  -- 回刷手动指定-小时
    '$[time(yyyyMMdd)]' as inc_day,  -- 写入天分区
    '1500' as inc_hour                   -- 回刷手动指定-小时
from(
	select 
		shift_no,
		site_code,
		next_site_code,
		total_out_weight,
		total_out_ewbcount,
		total_out_piece,
		total_out_vol,
		first_unlaod_time,
		standard_plan_arrival_time,
		in_confirm_time,
		pre_arrive_time,
		transport_level,
		ewblist_send_time,
		vehicle_status,
		max(batch_code) over(partition by shift_no, site_code, next_site_code) batch_code,
		max(batch_date) over(partition by shift_no, site_code, next_site_code) batch_date,
		row_number() over(partition by shift_no,site_code,next_site_code order by pre_arrive_time) rn
	from(	
		select 
			shift_no,
			site_code,
			next_site_code,
			total_out_weight,
			total_out_ewbcount,
			total_out_piece,
			total_out_vol,
			first_unlaod_time,
			standard_plan_arrival_time,
			in_confirm_time,
			pre_arrive_time,
			arrTm,
			transport_level,
			ewblist_send_time,
			vehicle_status,
			if((t4.arrTm >= t5.last_last_arrive_tm and t4.arrTm < t5.last_arrive_tm ),batch_code,null) batch_code,
			if((t4.arrTm >= t5.last_last_arrive_tm and t4.arrTm < t5.last_arrive_tm ),batch_date,null) batch_date
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
				if(
					datediff(t1.first_unlaod_time,FROM_UNIXTIME(UNIX_TIMESTAMP()))>365,
					if(datediff(t1.in_confirm_time,FROM_UNIXTIME(UNIX_TIMESTAMP()))<-365,t1.standard_plan_arrival_time,t1.in_confirm_time),
					t1.first_unlaod_time 
				) as pre_arrive_time,
				date_format(
					if(
						datediff(t1.first_unlaod_time,FROM_UNIXTIME(UNIX_TIMESTAMP()))>365,
						if(datediff(t1.in_confirm_time,FROM_UNIXTIME(UNIX_TIMESTAMP()))<-365,t1.standard_plan_arrival_time,t1.in_confirm_time),
						t1.first_unlaod_time 
					),'yyyy-MM-dd HH:mm'
				) as arrTm,
				cast(t1.transport_level as int) transport_level,
				t1.ewblist_send_time,
				t1.vehicle_status
			from
				(select
					shift_no, 
					site_code,
					next_site_code,
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
				from ky.dm_freight.dwd_sx_vehicle_dtl_di     -- 读取SX车辆实时进港明细
				where 
                    inc_day between '$[time(yyyyMMdd,-14d)]' and '$[time(yyyyMMdd)]' 
                    and ewblist_send_time between '$[time(yyyy-MM-dd 00:00:00,-14d)]' and  '$[time(yyyy-MM-dd 15:00:00)]' 	-- 回刷手动指定-小时 (约束时点数据) 
				group by shift_no, site_code,next_site_code   
				) t1 
			left join 
				(
					select 
						dept_code as deptCode 
					from dm_ordi_predict.fmsrms_dim_heavy_transit_info_df   -- 场地过滤  快运/小件场地
					where inc_day = '20230905'  -- 写死日期
					group by dept_code
				) t2  
			on t1.next_site_code = t2.deptCode
			left join 
				(
					select 
						requireId  
					from dm_ordi_predict.dmpdsp_vt_has_arrive_cars_batch_df  -- 顺丰车辆数据过滤
					where inc_day = '20230905'  -- 写死日期
					and actualTime  > '$[time(yyyy-MM-dd 15:00:00,-30d)]'  -- 回刷手动指定-小时
					and actualTime <= '$[time(yyyy-MM-dd 15:00:00)]'  -- 回刷手动指定-小时
					group by requireId
				) t3  
			on t1.shift_no = t3.requireId
			where t2.deptCode is not null 
			and t3.requireId is null 
			and if(
					datediff(t1.first_unlaod_time,FROM_UNIXTIME(UNIX_TIMESTAMP()))>365,
					if(datediff(t1.in_confirm_time,FROM_UNIXTIME(UNIX_TIMESTAMP()))<-365,t1.standard_plan_arrival_time,t1.in_confirm_time),
					t1.first_unlaod_time 
				) 
                 between '$[time(yyyy-MM-dd 00:00:00,-3d)]' and '$[time(yyyy-MM-dd 00:00:00,+5d)]' 
         
		)t4 
		left join 
		-- 读取班次信息
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
		)t5 
		on t4.next_site_code=t5.operate_zone_code
	)t6 
)t7 where t7.rn=1 

union all 
select 
	shift_no,
	site_code,
	next_site_code,
	total_out_weight,
	total_out_ewbcount,
	total_out_piece,
	total_out_vol,
	ewblist_send_time,
	in_confirm_time,
	first_unlaod_time,
	standard_plan_arrival_time,
	pre_arrive_time,
	transport_level,
	case vehicle_status
	  when '0' then '1'
	  when '1' then '2'
	else '3' end status,
	batch_code,
	if(batch_date is not null,date_format(batch_date, 'yyyyMMdd'),batch_date) as batch_date,
    '$[time(yyyy-MM-dd 16:00:00)]' as count_time,  -- 回刷手动指定-小时
    '$[time(yyyyMMdd)]' as inc_day,  -- 写入天分区
    '1600' as inc_hour                   -- 回刷手动指定-小时
from(
	select 
		shift_no,
		site_code,
		next_site_code,
		total_out_weight,
		total_out_ewbcount,
		total_out_piece,
		total_out_vol,
		first_unlaod_time,
		standard_plan_arrival_time,
		in_confirm_time,
		pre_arrive_time,
		transport_level,
		ewblist_send_time,
		vehicle_status,
		max(batch_code) over(partition by shift_no, site_code, next_site_code) batch_code,
		max(batch_date) over(partition by shift_no, site_code, next_site_code) batch_date,
		row_number() over(partition by shift_no,site_code,next_site_code order by pre_arrive_time) rn
	from(	
		select 
			shift_no,
			site_code,
			next_site_code,
			total_out_weight,
			total_out_ewbcount,
			total_out_piece,
			total_out_vol,
			first_unlaod_time,
			standard_plan_arrival_time,
			in_confirm_time,
			pre_arrive_time,
			arrTm,
			transport_level,
			ewblist_send_time,
			vehicle_status,
			if((t4.arrTm >= t5.last_last_arrive_tm and t4.arrTm < t5.last_arrive_tm ),batch_code,null) batch_code,
			if((t4.arrTm >= t5.last_last_arrive_tm and t4.arrTm < t5.last_arrive_tm ),batch_date,null) batch_date
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
				if(
					datediff(t1.first_unlaod_time,FROM_UNIXTIME(UNIX_TIMESTAMP()))>365,
					if(datediff(t1.in_confirm_time,FROM_UNIXTIME(UNIX_TIMESTAMP()))<-365,t1.standard_plan_arrival_time,t1.in_confirm_time),
					t1.first_unlaod_time 
				) as pre_arrive_time,
				date_format(
					if(
						datediff(t1.first_unlaod_time,FROM_UNIXTIME(UNIX_TIMESTAMP()))>365,
						if(datediff(t1.in_confirm_time,FROM_UNIXTIME(UNIX_TIMESTAMP()))<-365,t1.standard_plan_arrival_time,t1.in_confirm_time),
						t1.first_unlaod_time 
					),'yyyy-MM-dd HH:mm'
				) as arrTm,
				cast(t1.transport_level as int) transport_level,
				t1.ewblist_send_time,
				t1.vehicle_status
			from
				(select
					shift_no, 
					site_code,
					next_site_code,
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
				from ky.dm_freight.dwd_sx_vehicle_dtl_di     -- 读取SX车辆实时进港明细
				where 
                    inc_day between '$[time(yyyyMMdd,-14d)]' and '$[time(yyyyMMdd)]' 
                    and ewblist_send_time between '$[time(yyyy-MM-dd 00:00:00,-14d)]' and  '$[time(yyyy-MM-dd 16:00:00)]' 	-- 回刷手动指定-小时 (约束时点数据) 
				group by shift_no, site_code,next_site_code   
				) t1 
			left join 
				(
					select 
						dept_code as deptCode 
					from dm_ordi_predict.fmsrms_dim_heavy_transit_info_df   -- 场地过滤  快运/小件场地
					where inc_day = '20230905'  -- 写死日期
					group by dept_code
				) t2  
			on t1.next_site_code = t2.deptCode
			left join 
				(
					select 
						requireId  
					from dm_ordi_predict.dmpdsp_vt_has_arrive_cars_batch_df  -- 顺丰车辆数据过滤
					where inc_day = '20230905'  -- 写死日期
					and actualTime  > '$[time(yyyy-MM-dd 16:00:00,-30d)]'  -- 回刷手动指定-小时
					and actualTime <= '$[time(yyyy-MM-dd 16:00:00)]'  -- 回刷手动指定-小时
					group by requireId
				) t3  
			on t1.shift_no = t3.requireId
			where t2.deptCode is not null 
			and t3.requireId is null 
			and if(
					datediff(t1.first_unlaod_time,FROM_UNIXTIME(UNIX_TIMESTAMP()))>365,
					if(datediff(t1.in_confirm_time,FROM_UNIXTIME(UNIX_TIMESTAMP()))<-365,t1.standard_plan_arrival_time,t1.in_confirm_time),
					t1.first_unlaod_time 
				) 
                 between '$[time(yyyy-MM-dd 00:00:00,-3d)]' and '$[time(yyyy-MM-dd 00:00:00,+5d)]' 
         
		)t4 
		left join 
		-- 读取班次信息
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
		)t5 
		on t4.next_site_code=t5.operate_zone_code
	)t6 
)t7 where t7.rn=1 


union all 
select 
	shift_no,
	site_code,
	next_site_code,
	total_out_weight,
	total_out_ewbcount,
	total_out_piece,
	total_out_vol,
	ewblist_send_time,
	in_confirm_time,
	first_unlaod_time,
	standard_plan_arrival_time,
	pre_arrive_time,
	transport_level,
	case vehicle_status
	  when '0' then '1'
	  when '1' then '2'
	else '3' end status,
	batch_code,
	if(batch_date is not null,date_format(batch_date, 'yyyyMMdd'),batch_date) as batch_date,
    '$[time(yyyy-MM-dd 17:00:00)]' as count_time,  -- 回刷手动指定-小时
    '$[time(yyyyMMdd)]' as inc_day,  -- 写入天分区
    '1700' as inc_hour                   -- 回刷手动指定-小时
from(
	select 
		shift_no,
		site_code,
		next_site_code,
		total_out_weight,
		total_out_ewbcount,
		total_out_piece,
		total_out_vol,
		first_unlaod_time,
		standard_plan_arrival_time,
		in_confirm_time,
		pre_arrive_time,
		transport_level,
		ewblist_send_time,
		vehicle_status,
		max(batch_code) over(partition by shift_no, site_code, next_site_code) batch_code,
		max(batch_date) over(partition by shift_no, site_code, next_site_code) batch_date,
		row_number() over(partition by shift_no,site_code,next_site_code order by pre_arrive_time) rn
	from(	
		select 
			shift_no,
			site_code,
			next_site_code,
			total_out_weight,
			total_out_ewbcount,
			total_out_piece,
			total_out_vol,
			first_unlaod_time,
			standard_plan_arrival_time,
			in_confirm_time,
			pre_arrive_time,
			arrTm,
			transport_level,
			ewblist_send_time,
			vehicle_status,
			if((t4.arrTm >= t5.last_last_arrive_tm and t4.arrTm < t5.last_arrive_tm ),batch_code,null) batch_code,
			if((t4.arrTm >= t5.last_last_arrive_tm and t4.arrTm < t5.last_arrive_tm ),batch_date,null) batch_date
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
				if(
					datediff(t1.first_unlaod_time,FROM_UNIXTIME(UNIX_TIMESTAMP()))>365,
					if(datediff(t1.in_confirm_time,FROM_UNIXTIME(UNIX_TIMESTAMP()))<-365,t1.standard_plan_arrival_time,t1.in_confirm_time),
					t1.first_unlaod_time 
				) as pre_arrive_time,
				date_format(
					if(
						datediff(t1.first_unlaod_time,FROM_UNIXTIME(UNIX_TIMESTAMP()))>365,
						if(datediff(t1.in_confirm_time,FROM_UNIXTIME(UNIX_TIMESTAMP()))<-365,t1.standard_plan_arrival_time,t1.in_confirm_time),
						t1.first_unlaod_time 
					),'yyyy-MM-dd HH:mm'
				) as arrTm,
				cast(t1.transport_level as int) transport_level,
				t1.ewblist_send_time,
				t1.vehicle_status
			from
				(select
					shift_no, 
					site_code,
					next_site_code,
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
				from ky.dm_freight.dwd_sx_vehicle_dtl_di     -- 读取SX车辆实时进港明细
				where 
                    inc_day between '$[time(yyyyMMdd,-14d)]' and '$[time(yyyyMMdd)]' 
                    and ewblist_send_time between '$[time(yyyy-MM-dd 00:00:00,-14d)]' and  '$[time(yyyy-MM-dd 17:00:00)]' 	-- 回刷手动指定-小时 (约束时点数据) 
				group by shift_no, site_code,next_site_code   
				) t1 
			left join 
				(
					select 
						dept_code as deptCode 
					from dm_ordi_predict.fmsrms_dim_heavy_transit_info_df   -- 场地过滤  快运/小件场地
					where inc_day = '20230905'  -- 写死日期
					group by dept_code
				) t2  
			on t1.next_site_code = t2.deptCode
			left join 
				(
					select 
						requireId  
					from dm_ordi_predict.dmpdsp_vt_has_arrive_cars_batch_df  -- 顺丰车辆数据过滤
					where inc_day = '20230905'  -- 写死日期
					and actualTime  > '$[time(yyyy-MM-dd 17:00:00,-30d)]'  -- 回刷手动指定-小时
					and actualTime <= '$[time(yyyy-MM-dd 17:00:00)]'  -- 回刷手动指定-小时
					group by requireId
				) t3  
			on t1.shift_no = t3.requireId
			where t2.deptCode is not null 
			and t3.requireId is null 
			and if(
					datediff(t1.first_unlaod_time,FROM_UNIXTIME(UNIX_TIMESTAMP()))>365,
					if(datediff(t1.in_confirm_time,FROM_UNIXTIME(UNIX_TIMESTAMP()))<-365,t1.standard_plan_arrival_time,t1.in_confirm_time),
					t1.first_unlaod_time 
				) 
                 between '$[time(yyyy-MM-dd 00:00:00,-3d)]' and '$[time(yyyy-MM-dd 00:00:00,+5d)]' 
         
		)t4 
		left join 
		-- 读取班次信息
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
		)t5 
		on t4.next_site_code=t5.operate_zone_code
	)t6 
)t7 where t7.rn=1 

union all 
select 
	shift_no,
	site_code,
	next_site_code,
	total_out_weight,
	total_out_ewbcount,
	total_out_piece,
	total_out_vol,
	ewblist_send_time,
	in_confirm_time,
	first_unlaod_time,
	standard_plan_arrival_time,
	pre_arrive_time,
	transport_level,
	case vehicle_status
	  when '0' then '1'
	  when '1' then '2'
	else '3' end status,
	batch_code,
	if(batch_date is not null,date_format(batch_date, 'yyyyMMdd'),batch_date) as batch_date,
    '$[time(yyyy-MM-dd 18:00:00)]' as count_time,  -- 回刷手动指定-小时
    '$[time(yyyyMMdd)]' as inc_day,  -- 写入天分区
    '1800' as inc_hour                   -- 回刷手动指定-小时
from(
	select 
		shift_no,
		site_code,
		next_site_code,
		total_out_weight,
		total_out_ewbcount,
		total_out_piece,
		total_out_vol,
		first_unlaod_time,
		standard_plan_arrival_time,
		in_confirm_time,
		pre_arrive_time,
		transport_level,
		ewblist_send_time,
		vehicle_status,
		max(batch_code) over(partition by shift_no, site_code, next_site_code) batch_code,
		max(batch_date) over(partition by shift_no, site_code, next_site_code) batch_date,
		row_number() over(partition by shift_no,site_code,next_site_code order by pre_arrive_time) rn
	from(	
		select 
			shift_no,
			site_code,
			next_site_code,
			total_out_weight,
			total_out_ewbcount,
			total_out_piece,
			total_out_vol,
			first_unlaod_time,
			standard_plan_arrival_time,
			in_confirm_time,
			pre_arrive_time,
			arrTm,
			transport_level,
			ewblist_send_time,
			vehicle_status,
			if((t4.arrTm >= t5.last_last_arrive_tm and t4.arrTm < t5.last_arrive_tm ),batch_code,null) batch_code,
			if((t4.arrTm >= t5.last_last_arrive_tm and t4.arrTm < t5.last_arrive_tm ),batch_date,null) batch_date
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
				if(
					datediff(t1.first_unlaod_time,FROM_UNIXTIME(UNIX_TIMESTAMP()))>365,
					if(datediff(t1.in_confirm_time,FROM_UNIXTIME(UNIX_TIMESTAMP()))<-365,t1.standard_plan_arrival_time,t1.in_confirm_time),
					t1.first_unlaod_time 
				) as pre_arrive_time,
				date_format(
					if(
						datediff(t1.first_unlaod_time,FROM_UNIXTIME(UNIX_TIMESTAMP()))>365,
						if(datediff(t1.in_confirm_time,FROM_UNIXTIME(UNIX_TIMESTAMP()))<-365,t1.standard_plan_arrival_time,t1.in_confirm_time),
						t1.first_unlaod_time 
					),'yyyy-MM-dd HH:mm'
				) as arrTm,
				cast(t1.transport_level as int) transport_level,
				t1.ewblist_send_time,
				t1.vehicle_status
			from
				(select
					shift_no, 
					site_code,
					next_site_code,
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
				from ky.dm_freight.dwd_sx_vehicle_dtl_di     -- 读取SX车辆实时进港明细
				where 
                    inc_day between '$[time(yyyyMMdd,-14d)]' and '$[time(yyyyMMdd)]' 
                    and ewblist_send_time between '$[time(yyyy-MM-dd 00:00:00,-14d)]' and  '$[time(yyyy-MM-dd 18:00:00)]' 	-- 回刷手动指定-小时 (约束时点数据) 
				group by shift_no, site_code,next_site_code   
				) t1 
			left join 
				(
					select 
						dept_code as deptCode 
					from dm_ordi_predict.fmsrms_dim_heavy_transit_info_df   -- 场地过滤  快运/小件场地
					where inc_day = '20230905'  -- 写死日期
					group by dept_code
				) t2  
			on t1.next_site_code = t2.deptCode
			left join 
				(
					select 
						requireId  
					from dm_ordi_predict.dmpdsp_vt_has_arrive_cars_batch_df  -- 顺丰车辆数据过滤
					where inc_day = '20230905'  -- 写死日期
					and actualTime  > '$[time(yyyy-MM-dd 18:00:00,-30d)]'  -- 回刷手动指定-小时
					and actualTime <= '$[time(yyyy-MM-dd 18:00:00)]'  -- 回刷手动指定-小时
					group by requireId
				) t3  
			on t1.shift_no = t3.requireId
			where t2.deptCode is not null 
			and t3.requireId is null 
			and if(
					datediff(t1.first_unlaod_time,FROM_UNIXTIME(UNIX_TIMESTAMP()))>365,
					if(datediff(t1.in_confirm_time,FROM_UNIXTIME(UNIX_TIMESTAMP()))<-365,t1.standard_plan_arrival_time,t1.in_confirm_time),
					t1.first_unlaod_time 
				) 
                 between '$[time(yyyy-MM-dd 00:00:00,-3d)]' and '$[time(yyyy-MM-dd 00:00:00,+5d)]' 
         
		)t4 
		left join 
		-- 读取班次信息
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
		)t5 
		on t4.next_site_code=t5.operate_zone_code
	)t6 
)t7 where t7.rn=1 

union all 
select 
	shift_no,
	site_code,
	next_site_code,
	total_out_weight,
	total_out_ewbcount,
	total_out_piece,
	total_out_vol,
	ewblist_send_time,
	in_confirm_time,
	first_unlaod_time,
	standard_plan_arrival_time,
	pre_arrive_time,
	transport_level,
	case vehicle_status
	  when '0' then '1'
	  when '1' then '2'
	else '3' end status,
	batch_code,
	if(batch_date is not null,date_format(batch_date, 'yyyyMMdd'),batch_date) as batch_date,
    '$[time(yyyy-MM-dd 19:00:00)]' as count_time,  -- 回刷手动指定-小时
    '$[time(yyyyMMdd)]' as inc_day,  -- 写入天分区
    '1900' as inc_hour                   -- 回刷手动指定-小时
from(
	select 
		shift_no,
		site_code,
		next_site_code,
		total_out_weight,
		total_out_ewbcount,
		total_out_piece,
		total_out_vol,
		first_unlaod_time,
		standard_plan_arrival_time,
		in_confirm_time,
		pre_arrive_time,
		transport_level,
		ewblist_send_time,
		vehicle_status,
		max(batch_code) over(partition by shift_no, site_code, next_site_code) batch_code,
		max(batch_date) over(partition by shift_no, site_code, next_site_code) batch_date,
		row_number() over(partition by shift_no,site_code,next_site_code order by pre_arrive_time) rn
	from(	
		select 
			shift_no,
			site_code,
			next_site_code,
			total_out_weight,
			total_out_ewbcount,
			total_out_piece,
			total_out_vol,
			first_unlaod_time,
			standard_plan_arrival_time,
			in_confirm_time,
			pre_arrive_time,
			arrTm,
			transport_level,
			ewblist_send_time,
			vehicle_status,
			if((t4.arrTm >= t5.last_last_arrive_tm and t4.arrTm < t5.last_arrive_tm ),batch_code,null) batch_code,
			if((t4.arrTm >= t5.last_last_arrive_tm and t4.arrTm < t5.last_arrive_tm ),batch_date,null) batch_date
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
				if(
					datediff(t1.first_unlaod_time,FROM_UNIXTIME(UNIX_TIMESTAMP()))>365,
					if(datediff(t1.in_confirm_time,FROM_UNIXTIME(UNIX_TIMESTAMP()))<-365,t1.standard_plan_arrival_time,t1.in_confirm_time),
					t1.first_unlaod_time 
				) as pre_arrive_time,
				date_format(
					if(
						datediff(t1.first_unlaod_time,FROM_UNIXTIME(UNIX_TIMESTAMP()))>365,
						if(datediff(t1.in_confirm_time,FROM_UNIXTIME(UNIX_TIMESTAMP()))<-365,t1.standard_plan_arrival_time,t1.in_confirm_time),
						t1.first_unlaod_time 
					),'yyyy-MM-dd HH:mm'
				) as arrTm,
				cast(t1.transport_level as int) transport_level,
				t1.ewblist_send_time,
				t1.vehicle_status
			from
				(select
					shift_no, 
					site_code,
					next_site_code,
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
				from ky.dm_freight.dwd_sx_vehicle_dtl_di     -- 读取SX车辆实时进港明细
				where 
                    inc_day between '$[time(yyyyMMdd,-14d)]' and '$[time(yyyyMMdd)]' 
                    and ewblist_send_time between '$[time(yyyy-MM-dd 00:00:00,-14d)]' and  '$[time(yyyy-MM-dd 19:00:00)]' 	-- 回刷手动指定-小时 (约束时点数据) 
				group by shift_no, site_code,next_site_code   
				) t1 
			left join 
				(
					select 
						dept_code as deptCode 
					from dm_ordi_predict.fmsrms_dim_heavy_transit_info_df   -- 场地过滤  快运/小件场地
					where inc_day = '20230905'  -- 写死日期
					group by dept_code
				) t2  
			on t1.next_site_code = t2.deptCode
			left join 
				(
					select 
						requireId  
					from dm_ordi_predict.dmpdsp_vt_has_arrive_cars_batch_df  -- 顺丰车辆数据过滤
					where inc_day = '20230905'  -- 写死日期
					and actualTime  > '$[time(yyyy-MM-dd 19:00:00,-30d)]'  -- 回刷手动指定-小时
					and actualTime <= '$[time(yyyy-MM-dd 19:00:00)]'  -- 回刷手动指定-小时
					group by requireId
				) t3  
			on t1.shift_no = t3.requireId
			where t2.deptCode is not null 
			and t3.requireId is null 
			and if(
					datediff(t1.first_unlaod_time,FROM_UNIXTIME(UNIX_TIMESTAMP()))>365,
					if(datediff(t1.in_confirm_time,FROM_UNIXTIME(UNIX_TIMESTAMP()))<-365,t1.standard_plan_arrival_time,t1.in_confirm_time),
					t1.first_unlaod_time 
				) 
                 between '$[time(yyyy-MM-dd 00:00:00,-3d)]' and '$[time(yyyy-MM-dd 00:00:00,+5d)]' 
         
		)t4 
		left join 
		-- 读取班次信息
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
		)t5 
		on t4.next_site_code=t5.operate_zone_code
	)t6 
)t7 where t7.rn=1 

union all 
select 
	shift_no,
	site_code,
	next_site_code,
	total_out_weight,
	total_out_ewbcount,
	total_out_piece,
	total_out_vol,
	ewblist_send_time,
	in_confirm_time,
	first_unlaod_time,
	standard_plan_arrival_time,
	pre_arrive_time,
	transport_level,
	case vehicle_status
	  when '0' then '1'
	  when '1' then '2'
	else '3' end status,
	batch_code,
	if(batch_date is not null,date_format(batch_date, 'yyyyMMdd'),batch_date) as batch_date,
    '$[time(yyyy-MM-dd 20:00:00)]' as count_time,  -- 回刷手动指定-小时
    '$[time(yyyyMMdd)]' as inc_day,  -- 写入天分区
    '2000' as inc_hour                   -- 回刷手动指定-小时
from(
	select 
		shift_no,
		site_code,
		next_site_code,
		total_out_weight,
		total_out_ewbcount,
		total_out_piece,
		total_out_vol,
		first_unlaod_time,
		standard_plan_arrival_time,
		in_confirm_time,
		pre_arrive_time,
		transport_level,
		ewblist_send_time,
		vehicle_status,
		max(batch_code) over(partition by shift_no, site_code, next_site_code) batch_code,
		max(batch_date) over(partition by shift_no, site_code, next_site_code) batch_date,
		row_number() over(partition by shift_no,site_code,next_site_code order by pre_arrive_time) rn
	from(	
		select 
			shift_no,
			site_code,
			next_site_code,
			total_out_weight,
			total_out_ewbcount,
			total_out_piece,
			total_out_vol,
			first_unlaod_time,
			standard_plan_arrival_time,
			in_confirm_time,
			pre_arrive_time,
			arrTm,
			transport_level,
			ewblist_send_time,
			vehicle_status,
			if((t4.arrTm >= t5.last_last_arrive_tm and t4.arrTm < t5.last_arrive_tm ),batch_code,null) batch_code,
			if((t4.arrTm >= t5.last_last_arrive_tm and t4.arrTm < t5.last_arrive_tm ),batch_date,null) batch_date
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
				if(
					datediff(t1.first_unlaod_time,FROM_UNIXTIME(UNIX_TIMESTAMP()))>365,
					if(datediff(t1.in_confirm_time,FROM_UNIXTIME(UNIX_TIMESTAMP()))<-365,t1.standard_plan_arrival_time,t1.in_confirm_time),
					t1.first_unlaod_time 
				) as pre_arrive_time,
				date_format(
					if(
						datediff(t1.first_unlaod_time,FROM_UNIXTIME(UNIX_TIMESTAMP()))>365,
						if(datediff(t1.in_confirm_time,FROM_UNIXTIME(UNIX_TIMESTAMP()))<-365,t1.standard_plan_arrival_time,t1.in_confirm_time),
						t1.first_unlaod_time 
					),'yyyy-MM-dd HH:mm'
				) as arrTm,
				cast(t1.transport_level as int) transport_level,
				t1.ewblist_send_time,
				t1.vehicle_status
			from
				(select
					shift_no, 
					site_code,
					next_site_code,
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
				from ky.dm_freight.dwd_sx_vehicle_dtl_di     -- 读取SX车辆实时进港明细
				where 
                    inc_day between '$[time(yyyyMMdd,-14d)]' and '$[time(yyyyMMdd)]' 
                    and ewblist_send_time between '$[time(yyyy-MM-dd 00:00:00,-14d)]' and  '$[time(yyyy-MM-dd 20:00:00)]' 	-- 回刷手动指定-小时 (约束时点数据) 
				group by shift_no, site_code,next_site_code   
				) t1 
			left join 
				(
					select 
						dept_code as deptCode 
					from dm_ordi_predict.fmsrms_dim_heavy_transit_info_df   -- 场地过滤  快运/小件场地
					where inc_day = '20230905'  -- 写死日期
					group by dept_code
				) t2  
			on t1.next_site_code = t2.deptCode
			left join 
				(
					select 
						requireId  
					from dm_ordi_predict.dmpdsp_vt_has_arrive_cars_batch_df  -- 顺丰车辆数据过滤
					where inc_day = '20230905'  -- 写死日期
					and actualTime  > '$[time(yyyy-MM-dd 20:00:00,-30d)]'  -- 回刷手动指定-小时
					and actualTime <= '$[time(yyyy-MM-dd 20:00:00)]'  -- 回刷手动指定-小时
					group by requireId
				) t3  
			on t1.shift_no = t3.requireId
			where t2.deptCode is not null 
			and t3.requireId is null 
			and if(
					datediff(t1.first_unlaod_time,FROM_UNIXTIME(UNIX_TIMESTAMP()))>365,
					if(datediff(t1.in_confirm_time,FROM_UNIXTIME(UNIX_TIMESTAMP()))<-365,t1.standard_plan_arrival_time,t1.in_confirm_time),
					t1.first_unlaod_time 
				) 
                 between '$[time(yyyy-MM-dd 00:00:00,-3d)]' and '$[time(yyyy-MM-dd 00:00:00,+5d)]' 
         
		)t4 
		left join 
		-- 读取班次信息
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
		)t5 
		on t4.next_site_code=t5.operate_zone_code
	)t6 
)t7 where t7.rn=1 

union all 
select 
	shift_no,
	site_code,
	next_site_code,
	total_out_weight,
	total_out_ewbcount,
	total_out_piece,
	total_out_vol,
	ewblist_send_time,
	in_confirm_time,
	first_unlaod_time,
	standard_plan_arrival_time,
	pre_arrive_time,
	transport_level,
	case vehicle_status
	  when '0' then '1'
	  when '1' then '2'
	else '3' end status,
	batch_code,
	if(batch_date is not null,date_format(batch_date, 'yyyyMMdd'),batch_date) as batch_date,
    '$[time(yyyy-MM-dd 21:00:00)]' as count_time,  -- 回刷手动指定-小时
    '$[time(yyyyMMdd)]' as inc_day,  -- 写入天分区
    '2100' as inc_hour                   -- 回刷手动指定-小时
from(
	select 
		shift_no,
		site_code,
		next_site_code,
		total_out_weight,
		total_out_ewbcount,
		total_out_piece,
		total_out_vol,
		first_unlaod_time,
		standard_plan_arrival_time,
		in_confirm_time,
		pre_arrive_time,
		transport_level,
		ewblist_send_time,
		vehicle_status,
		max(batch_code) over(partition by shift_no, site_code, next_site_code) batch_code,
		max(batch_date) over(partition by shift_no, site_code, next_site_code) batch_date,
		row_number() over(partition by shift_no,site_code,next_site_code order by pre_arrive_time) rn
	from(	
		select 
			shift_no,
			site_code,
			next_site_code,
			total_out_weight,
			total_out_ewbcount,
			total_out_piece,
			total_out_vol,
			first_unlaod_time,
			standard_plan_arrival_time,
			in_confirm_time,
			pre_arrive_time,
			arrTm,
			transport_level,
			ewblist_send_time,
			vehicle_status,
			if((t4.arrTm >= t5.last_last_arrive_tm and t4.arrTm < t5.last_arrive_tm ),batch_code,null) batch_code,
			if((t4.arrTm >= t5.last_last_arrive_tm and t4.arrTm < t5.last_arrive_tm ),batch_date,null) batch_date
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
				if(
					datediff(t1.first_unlaod_time,FROM_UNIXTIME(UNIX_TIMESTAMP()))>365,
					if(datediff(t1.in_confirm_time,FROM_UNIXTIME(UNIX_TIMESTAMP()))<-365,t1.standard_plan_arrival_time,t1.in_confirm_time),
					t1.first_unlaod_time 
				) as pre_arrive_time,
				date_format(
					if(
						datediff(t1.first_unlaod_time,FROM_UNIXTIME(UNIX_TIMESTAMP()))>365,
						if(datediff(t1.in_confirm_time,FROM_UNIXTIME(UNIX_TIMESTAMP()))<-365,t1.standard_plan_arrival_time,t1.in_confirm_time),
						t1.first_unlaod_time 
					),'yyyy-MM-dd HH:mm'
				) as arrTm,
				cast(t1.transport_level as int) transport_level,
				t1.ewblist_send_time,
				t1.vehicle_status
			from
				(select
					shift_no, 
					site_code,
					next_site_code,
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
				from ky.dm_freight.dwd_sx_vehicle_dtl_di     -- 读取SX车辆实时进港明细
				where 
                    inc_day between '$[time(yyyyMMdd,-14d)]' and '$[time(yyyyMMdd)]' 
                    and ewblist_send_time between '$[time(yyyy-MM-dd 00:00:00,-14d)]' and  '$[time(yyyy-MM-dd 21:00:00)]' 	-- 回刷手动指定-小时 (约束时点数据) 
				group by shift_no, site_code,next_site_code   
				) t1 
			left join 
				(
					select 
						dept_code as deptCode 
					from dm_ordi_predict.fmsrms_dim_heavy_transit_info_df   -- 场地过滤  快运/小件场地
					where inc_day = '20230905'  -- 写死日期
					group by dept_code
				) t2  
			on t1.next_site_code = t2.deptCode
			left join 
				(
					select 
						requireId  
					from dm_ordi_predict.dmpdsp_vt_has_arrive_cars_batch_df  -- 顺丰车辆数据过滤
					where inc_day = '20230905'  -- 写死日期
					and actualTime  > '$[time(yyyy-MM-dd 21:00:00,-30d)]'  -- 回刷手动指定-小时
					and actualTime <= '$[time(yyyy-MM-dd 21:00:00)]'  -- 回刷手动指定-小时
					group by requireId
				) t3  
			on t1.shift_no = t3.requireId
			where t2.deptCode is not null 
			and t3.requireId is null 
			and if(
					datediff(t1.first_unlaod_time,FROM_UNIXTIME(UNIX_TIMESTAMP()))>365,
					if(datediff(t1.in_confirm_time,FROM_UNIXTIME(UNIX_TIMESTAMP()))<-365,t1.standard_plan_arrival_time,t1.in_confirm_time),
					t1.first_unlaod_time 
				) 
                 between '$[time(yyyy-MM-dd 00:00:00,-3d)]' and '$[time(yyyy-MM-dd 00:00:00,+5d)]' 
         
		)t4 
		left join 
		-- 读取班次信息
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
		)t5 
		on t4.next_site_code=t5.operate_zone_code
	)t6 
)t7 where t7.rn=1 

union all 
select 
	shift_no,
	site_code,
	next_site_code,
	total_out_weight,
	total_out_ewbcount,
	total_out_piece,
	total_out_vol,
	ewblist_send_time,
	in_confirm_time,
	first_unlaod_time,
	standard_plan_arrival_time,
	pre_arrive_time,
	transport_level,
	case vehicle_status
	  when '0' then '1'
	  when '1' then '2'
	else '3' end status,
	batch_code,
	if(batch_date is not null,date_format(batch_date, 'yyyyMMdd'),batch_date) as batch_date,
    '$[time(yyyy-MM-dd 22:00:00)]' as count_time,  -- 回刷手动指定-小时
    '$[time(yyyyMMdd)]' as inc_day,  -- 写入天分区
    '2200' as inc_hour                   -- 回刷手动指定-小时
from(
	select 
		shift_no,
		site_code,
		next_site_code,
		total_out_weight,
		total_out_ewbcount,
		total_out_piece,
		total_out_vol,
		first_unlaod_time,
		standard_plan_arrival_time,
		in_confirm_time,
		pre_arrive_time,
		transport_level,
		ewblist_send_time,
		vehicle_status,
		max(batch_code) over(partition by shift_no, site_code, next_site_code) batch_code,
		max(batch_date) over(partition by shift_no, site_code, next_site_code) batch_date,
		row_number() over(partition by shift_no,site_code,next_site_code order by pre_arrive_time) rn
	from(	
		select 
			shift_no,
			site_code,
			next_site_code,
			total_out_weight,
			total_out_ewbcount,
			total_out_piece,
			total_out_vol,
			first_unlaod_time,
			standard_plan_arrival_time,
			in_confirm_time,
			pre_arrive_time,
			arrTm,
			transport_level,
			ewblist_send_time,
			vehicle_status,
			if((t4.arrTm >= t5.last_last_arrive_tm and t4.arrTm < t5.last_arrive_tm ),batch_code,null) batch_code,
			if((t4.arrTm >= t5.last_last_arrive_tm and t4.arrTm < t5.last_arrive_tm ),batch_date,null) batch_date
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
				if(
					datediff(t1.first_unlaod_time,FROM_UNIXTIME(UNIX_TIMESTAMP()))>365,
					if(datediff(t1.in_confirm_time,FROM_UNIXTIME(UNIX_TIMESTAMP()))<-365,t1.standard_plan_arrival_time,t1.in_confirm_time),
					t1.first_unlaod_time 
				) as pre_arrive_time,
				date_format(
					if(
						datediff(t1.first_unlaod_time,FROM_UNIXTIME(UNIX_TIMESTAMP()))>365,
						if(datediff(t1.in_confirm_time,FROM_UNIXTIME(UNIX_TIMESTAMP()))<-365,t1.standard_plan_arrival_time,t1.in_confirm_time),
						t1.first_unlaod_time 
					),'yyyy-MM-dd HH:mm'
				) as arrTm,
				cast(t1.transport_level as int) transport_level,
				t1.ewblist_send_time,
				t1.vehicle_status
			from
				(select
					shift_no, 
					site_code,
					next_site_code,
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
				from ky.dm_freight.dwd_sx_vehicle_dtl_di     -- 读取SX车辆实时进港明细
				where 
                    inc_day between '$[time(yyyyMMdd,-14d)]' and '$[time(yyyyMMdd)]' 
                    and ewblist_send_time between '$[time(yyyy-MM-dd 00:00:00,-14d)]' and  '$[time(yyyy-MM-dd 22:00:00)]' 	-- 回刷手动指定-小时 (约束时点数据) 
				group by shift_no, site_code,next_site_code   
				) t1 
			left join 
				(
					select 
						dept_code as deptCode 
					from dm_ordi_predict.fmsrms_dim_heavy_transit_info_df   -- 场地过滤  快运/小件场地
					where inc_day = '20230905'  -- 写死日期
					group by dept_code
				) t2  
			on t1.next_site_code = t2.deptCode
			left join 
				(
					select 
						requireId  
					from dm_ordi_predict.dmpdsp_vt_has_arrive_cars_batch_df  -- 顺丰车辆数据过滤
					where inc_day = '20230905'  -- 写死日期
					and actualTime  > '$[time(yyyy-MM-dd 22:00:00,-30d)]'  -- 回刷手动指定-小时
					and actualTime <= '$[time(yyyy-MM-dd 22:00:00)]'  -- 回刷手动指定-小时
					group by requireId
				) t3  
			on t1.shift_no = t3.requireId
			where t2.deptCode is not null 
			and t3.requireId is null 
			and if(
					datediff(t1.first_unlaod_time,FROM_UNIXTIME(UNIX_TIMESTAMP()))>365,
					if(datediff(t1.in_confirm_time,FROM_UNIXTIME(UNIX_TIMESTAMP()))<-365,t1.standard_plan_arrival_time,t1.in_confirm_time),
					t1.first_unlaod_time 
				) 
                 between '$[time(yyyy-MM-dd 00:00:00,-3d)]' and '$[time(yyyy-MM-dd 00:00:00,+5d)]' 
         
		)t4 
		left join 
		-- 读取班次信息
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
		)t5 
		on t4.next_site_code=t5.operate_zone_code
	)t6 
)t7 where t7.rn=1 

union all 
select 
	shift_no,
	site_code,
	next_site_code,
	total_out_weight,
	total_out_ewbcount,
	total_out_piece,
	total_out_vol,
	ewblist_send_time,
	in_confirm_time,
	first_unlaod_time,
	standard_plan_arrival_time,
	pre_arrive_time,
	transport_level,
	case vehicle_status
	  when '0' then '1'
	  when '1' then '2'
	else '3' end status,
	batch_code,
	if(batch_date is not null,date_format(batch_date, 'yyyyMMdd'),batch_date) as batch_date,
    '$[time(yyyy-MM-dd 23:00:00)]' as count_time,  -- 回刷手动指定-小时
    '$[time(yyyyMMdd)]' as inc_day,  -- 写入天分区
    '2300' as inc_hour                   -- 回刷手动指定-小时
from(
	select 
		shift_no,
		site_code,
		next_site_code,
		total_out_weight,
		total_out_ewbcount,
		total_out_piece,
		total_out_vol,
		first_unlaod_time,
		standard_plan_arrival_time,
		in_confirm_time,
		pre_arrive_time,
		transport_level,
		ewblist_send_time,
		vehicle_status,
		max(batch_code) over(partition by shift_no, site_code, next_site_code) batch_code,
		max(batch_date) over(partition by shift_no, site_code, next_site_code) batch_date,
		row_number() over(partition by shift_no,site_code,next_site_code order by pre_arrive_time) rn
	from(	
		select 
			shift_no,
			site_code,
			next_site_code,
			total_out_weight,
			total_out_ewbcount,
			total_out_piece,
			total_out_vol,
			first_unlaod_time,
			standard_plan_arrival_time,
			in_confirm_time,
			pre_arrive_time,
			arrTm,
			transport_level,
			ewblist_send_time,
			vehicle_status,
			if((t4.arrTm >= t5.last_last_arrive_tm and t4.arrTm < t5.last_arrive_tm ),batch_code,null) batch_code,
			if((t4.arrTm >= t5.last_last_arrive_tm and t4.arrTm < t5.last_arrive_tm ),batch_date,null) batch_date
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
				if(
					datediff(t1.first_unlaod_time,FROM_UNIXTIME(UNIX_TIMESTAMP()))>365,
					if(datediff(t1.in_confirm_time,FROM_UNIXTIME(UNIX_TIMESTAMP()))<-365,t1.standard_plan_arrival_time,t1.in_confirm_time),
					t1.first_unlaod_time 
				) as pre_arrive_time,
				date_format(
					if(
						datediff(t1.first_unlaod_time,FROM_UNIXTIME(UNIX_TIMESTAMP()))>365,
						if(datediff(t1.in_confirm_time,FROM_UNIXTIME(UNIX_TIMESTAMP()))<-365,t1.standard_plan_arrival_time,t1.in_confirm_time),
						t1.first_unlaod_time 
					),'yyyy-MM-dd HH:mm'
				) as arrTm,
				cast(t1.transport_level as int) transport_level,
				t1.ewblist_send_time,
				t1.vehicle_status
			from
				(select
					shift_no, 
					site_code,
					next_site_code,
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
				from ky.dm_freight.dwd_sx_vehicle_dtl_di     -- 读取SX车辆实时进港明细
				where 
                    inc_day between '$[time(yyyyMMdd,-14d)]' and '$[time(yyyyMMdd)]' 
                    and ewblist_send_time between '$[time(yyyy-MM-dd 00:00:00,-14d)]' and  '$[time(yyyy-MM-dd 23:00:00)]' 	-- 回刷手动指定-小时 (约束时点数据) 
				group by shift_no, site_code,next_site_code   
				) t1 
			left join 
				(
					select 
						dept_code as deptCode 
					from dm_ordi_predict.fmsrms_dim_heavy_transit_info_df   -- 场地过滤  快运/小件场地
					where inc_day = '20230905'  -- 写死日期
					group by dept_code
				) t2  
			on t1.next_site_code = t2.deptCode
			left join 
				(
					select 
						requireId  
					from dm_ordi_predict.dmpdsp_vt_has_arrive_cars_batch_df  -- 顺丰车辆数据过滤
					where inc_day = '20230905'  -- 写死日期
					and actualTime  > '$[time(yyyy-MM-dd 23:00:00,-30d)]'  -- 回刷手动指定-小时
					and actualTime <= '$[time(yyyy-MM-dd 23:00:00)]'  -- 回刷手动指定-小时
					group by requireId
				) t3  
			on t1.shift_no = t3.requireId
			where t2.deptCode is not null 
			and t3.requireId is null 
			and if(
					datediff(t1.first_unlaod_time,FROM_UNIXTIME(UNIX_TIMESTAMP()))>365,
					if(datediff(t1.in_confirm_time,FROM_UNIXTIME(UNIX_TIMESTAMP()))<-365,t1.standard_plan_arrival_time,t1.in_confirm_time),
					t1.first_unlaod_time 
				) 
                 between '$[time(yyyy-MM-dd 00:00:00,-3d)]' and '$[time(yyyy-MM-dd 00:00:00,+5d)]' 
         
		)t4 
		left join 
		-- 读取班次信息
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
		)t5 
		on t4.next_site_code=t5.operate_zone_code
	)t6 
)t7 where t7.rn=1 
;