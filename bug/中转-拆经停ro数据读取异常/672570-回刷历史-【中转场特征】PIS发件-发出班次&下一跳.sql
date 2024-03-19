/**
	-、背景：
		pass运力数据 ，ro表读取异常，切换rt表解决。需回刷历史数据
	-、脚本释义：【中转场特征】PIS发件-发出班次&下一跳
		1、回刷历史数据
		2、回刷范围：
**/

--step0	pis发件-获取运单

-- 取pis运单规划路由
set spark.sql.shuffle.partitions=1000;

drop table if exists tmp_ordi_predict.tmp_send_flow_static_pis_01_tmp20240111_v1;
create table if not exists tmp_ordi_predict.tmp_send_flow_static_pis_01_tmp20240111_v1 
stored as parquet as 
select 
    waybillno, 
    cast(index as int) as link_index,
    class_type,
    link_type,
     case when index in ('1','2') and static_zone is null then actual_zone else static_zone end as static_zone,
    case when index in ('1','2') and static_class is null then actual_class else static_class end as static_class,
    static_op_code,
    plan_process_time,
    from_unixtime(cast(substring(plan_process_time,0,10) as bigint),'yyyy-MM-dd HH:mm:ss') as process_time,
    from_unixtime(cast(substring(plan_process_time,0,10) as bigint),'yyyy-MM-dd') as process_date,
    from_unixtime(cast(substring(consigned_time,0,10) as bigint),'yyyy-MM-dd HH:mm:ss') as consigned_time,
    from_unixtime(cast(substring(consigned_time,0,10) as bigint),'yyyy-MM-dd') as consigned_date,
    inc_day,
    currentdate,
    meterageweightqty  -- 20230719添加
-- from  dm_ordi_predict.dwd_inc_pis_all_mor_hf t1 -- 20230718修改
from dm_predict.dwd_inc_pis_all_mor_di																					-- 回刷历史，使用历史快照表
where  
	-- inc_day between '$[time(yyyyMMdd,-1d)]'  and '$[time(yyyyMMdd)]'
(	
	(inc_day = '20240110' and inc_hour = '24')		
	or 
	(inc_day = '20240111' and inc_hour = '12')																			-- 回刷历史，约束小时时点
)
and inc_type='send'
and  from_unixtime(cast(substring(consigned_time,0,10) as bigint),'yyyy-MM-dd') between '2024-01-04'  and '2024-01-11'	-- 回刷历史，约束小时时点
and currentdate<=  '2024-01-11 12:05:00'																				-- 回刷历史，约束小时时点
and link_type in ('装车','到车','快件到达')
;


-- step1 pis发件-拼接离线数据
drop table if exists tmp_ordi_predict.tmp_send_flow_static_pis_03_tmp20240111_v1;
create table if not exists tmp_ordi_predict.tmp_send_flow_static_pis_03_tmp20240111_v1
stored as parquet as 
-- 计算T-1D ~ T-0D
select 
    waybillno, --运单号
    link_index,--环节序号
    class_type,--班次类型
    link_type,--环节类型
    static_zone,--静态网点
    static_class,--静态班次
    static_op_code,--静态操作码
   -- plan_process_time,--计划操作时间
    process_time,--计划操作时间
    process_date,--计划操作日期
    consigned_time,--寄件时间
    consigned_date,--寄件日期
    inc_day,-- ,--系统日期
    meterageweightqty  -- 20230719添加
from(
		select 
			t1.waybillno, --运单号
			link_index,--环节序号
			class_type,--班次类型
			link_type,--环节类型
			static_zone,--静态网点
			static_class,--静态班次
			static_op_code,--静态操作码
			plan_process_time,--计划操作时间
			process_time,--计划操作时间
			process_date,--计划操作日期
			consigned_time,--寄件时间
			consigned_date,--寄件日期
			inc_day,-- ,--系统日期
			meterageweightqty -- 20230719添加
		  --   d.dept_name as zone_name,
		  --   d.dept_type_code as zone_type_code,
		  --    d.dept_type_name as zone_type_name,
		  --   d.dept_transfer_flag as zone_trans_flag,
		  --  case when d.dept_type_code in ('ZZC04-TCJS','ZZC04-TL','ZZC04-YJ','ZZC04-SN','ZZC04-LS','ZZC04-ERJ','ZZC05-SJ','DB05-HHWZ','ZZC04-HKHZ','ZZC04-HK','GWB04','ZZC04-JYHK','ZZC05-KYJS') then 1 else 0 end as zone_is_trans,
		from tmp_ordi_predict.tmp_send_flow_static_pis_01_tmp20240111_v1  t1
		inner join (
			select 
				waybillno,max(inc_day) as max_inc_day
			from tmp_ordi_predict.tmp_send_flow_static_pis_01_tmp20240111_v1
			group by waybillno
		) t2 
		on t1.waybillno=t2.waybillno 
		and t1.inc_day=t2.max_inc_day
)  t1
union ALL
-- 计算 T-7D ~ T-2D 
select 
	t2.waybill_no,	--运单号
	route_index,	--路由序号
	null as class_type,
	null as link_type,
	s_zone_code,	--网点
	s_batch_code,	--班次编码,发车和到车环节班次编码为线路编码
	s_opt_type,	--操作类型
	s_opt_tm,	--计划操作时间
	to_date(s_opt_tm) as s_opt_dt,
	consignor_date as consigned_time,	--寄件日期 时间
	to_date(consignor_date) as consignor_date,
	inc_day,
	meterage_weight_qty as meterageweightqty
from 
	(
		select
			waybill_no,	--运单号
			cast(route_index as int) as route_index,	--路由序号
			null as class_type,
			null as link_type,
			s_zone_code,	--网点
			s_batch_code,	--班次编码,发车和到车环节班次编码为线路编码
			s_opt_type,	--操作类型
			s_opt_tm,	--计划操作时间
			consignor_date,	--寄件日期 时间
			inc_day
		from 
			ods_pis.tt_waybill_route_info
		-- where inc_day>='$[time(yyyyMMdd,-7d)]' and inc_day<='$[time(yyyyMMdd,-2d)]'  
		where inc_day between  '20240104' and '20240109'											-- 回刷历史，约束时间范围
		and s_opt_type in ('30','305','311')
		and nvl(s_batch_code,'')!='' and s_batch_code!='null'
		and nvl(s_zone_code,'')!='' and s_zone_code!='null'
		and nvl(s_opt_tm,'')!='' and s_opt_tm!='null'
	) t2
left join 
(
	select 
		waybillno 
	from(
			select 
				t1.waybillno, --运单号
				link_index,--环节序号
				class_type,--班次类型
				link_type,--环节类型
				static_zone,--静态网点
				static_class,--静态班次
				static_op_code,--静态操作码
				plan_process_time,--计划操作时间
				process_time,--计划操作时间
				process_date,--计划操作日期
				consigned_time,--寄件时间
				consigned_date,--寄件日期
				inc_day,-- ,--系统日期
				meterageweightqty -- 20230719添加
			  --   d.dept_name as zone_name,
			  --   d.dept_type_code as zone_type_code,
			  --    d.dept_type_name as zone_type_name,
			  --   d.dept_transfer_flag as zone_trans_flag,
			  --  case when d.dept_type_code in ('ZZC04-TCJS','ZZC04-TL','ZZC04-YJ','ZZC04-SN','ZZC04-LS','ZZC04-ERJ','ZZC05-SJ','DB05-HHWZ','ZZC04-HKHZ','ZZC04-HK','GWB04','ZZC04-JYHK','ZZC05-KYJS') then 1 else 0 end as zone_is_trans,
			from tmp_ordi_predict.tmp_send_flow_static_pis_01_tmp20240111_v1  t1
			inner join (
				select 
					waybillno,max(inc_day) as max_inc_day
				from tmp_ordi_predict.tmp_send_flow_static_pis_01_tmp20240111_v1
				group by waybillno
			) t2 
			on t1.waybillno=t2.waybillno 
			and t1.inc_day=t2.max_inc_day
		) a 
	group by waybillno
) t3
on t2.waybill_no=t3.waybillno
left join 
(
	select waybill_no,meterage_weight_qty from dwd.dwd_waybill_info_dtl_di
	-- where inc_day>='$[time(yyyyMMdd,-7d)]' and inc_day<='$[time(yyyyMMdd,-2d)]'
	where inc_day between  '20240104' and '20240109'											-- 回刷历史，约束时间范围
) t4
on t2.waybill_no=t4.waybill_no
where t3.waybillno is null;
	
	
-- step2 pis发件-获取下一跳及类型

-- 获取运单下一环节信息
set spark.sql.shuffle.partitions=1000;

-- 关联下一环节信息，并获取场地类型
drop table if exists tmp_ordi_predict.tmp_send_flow_static_pis_05_tmp20240111_v1;
create table if not exists tmp_ordi_predict.tmp_send_flow_static_pis_05_tmp20240111_v1 
stored as parquet as 
select 
    t2.*
    ,t4.dept_name as arr_zone_name
	,t4.dept_type_code as arr_zone_type_code
	,t4.dept_type_name as arr_zone_type_name
	,t4.dept_transfer_flag as arr_zone_trans_flag
	,case when t4.dept_type_code in ('ZZC04-TCJS','ZZC04-TL','ZZC04-YJ','ZZC04-SN','ZZC04-LS','ZZC04-ERJ','ZZC05-SJ',
		'DB05-HHWZ','ZZC04-HKHZ','ZZC04-HK','GWB04','ZZC04-JYHK','ZZC05-KYJS'
	) then 1 else 0 end as arr_zone_is_trans
from 
(
	select 
		t1.*
	   --,case when t3.next_s_zone_code is null and (link_type='装车' or static_op_code='30')  then substr(regexp_replace(t1.static_class,t1.static_zone,''),1,length(regexp_replace(t1.static_class,t1.static_zone,''))-4) else t3.next_s_zone_code end as next_s_zone_code
	   ,t3.next_s_zone_code
	   ,t3.next_process_date
		,case 
				when t1.process_time <= first_start_tm and t1.static_class!=first_batch_code then date_add(to_date( t1.process_time),-1)
				-- when  t1.process_time > last_start_tm and t1.static_class!=last_batch_code then date_add(to_date( t1.process_time),+1)
				when  t1.process_time >= last_arrv_tm and t1.static_class!=last_batch_code then date_add(to_date( t1.process_time),+1)
				else to_date(t1.process_time)
			end as batch_date
		,d.dept_name as send_zone_name
		,d.dept_type_code as send_zone_type_code
		,d.dept_type_name as send_zone_type_name
		,d.dept_transfer_flag as send_zone_trans_flag
		,case when d.dept_type_code in ('ZZC04-TCJS','ZZC04-TL','ZZC04-YJ','ZZC04-SN','ZZC04-LS',
								'ZZC04-ERJ','ZZC05-SJ','DB05-HHWZ','ZZC04-HKHZ','ZZC04-HK','GWB04','ZZC04-JYHK','ZZC05-KYJS'
							) then 1 else 0 end as send_zone_is_trans
	from tmp_ordi_predict.tmp_send_flow_static_pis_03_tmp20240111_v1 t1
	left join 
	(
		select
			dept_code,
			first_batch_code,
			first_start_tm,
			first_arrv_tm,
			first_end_tm,
			last_batch_code,
			last_start_tm,
			last_arrv_tm,
			last_end_tm,
			inc_day
		from dm_ordi_predict.dim_trans_first_last_batch_info_df
		-- where inc_day>='$[time(yyyyMMdd,-10d)]' and inc_day<='$[time(yyyyMMdd,+5d)]'
		where inc_day  between  '20240101' and '20240116'											-- 回刷历史，约束时间范围
	) t2
		on replace(t1.process_date,'-','')=t2.inc_day
		and t1.static_zone=t2.dept_code
	left join 
	   --  tmp_ordi_predict.tmp_send_flow_static_pis_04  t3
	(select 
		t.*
		 ,lead(static_zone)over(partition by waybillno order by link_index) as next_s_zone_code
		 ,lead(process_date)over(partition by waybillno order by link_index) as next_process_date
		from tmp_ordi_predict.tmp_send_flow_static_pis_03_tmp20240111_v1 t
	) t3
		on  t1.waybillno=t3.waybillno
		and t1.static_zone=t3.static_zone
		and t1.link_index=t3.link_index
	left join 
		 dim.dim_department d
	on t1.static_zone = d.dept_code
) t2
left join  dim.dim_department t4
on t2.next_s_zone_code=t4.dept_code;


-- step3  -- pis发件-获取运力信息
	-- 约束历史小时时点。
set runner.support.reading-hudi=true;

drop table if exists tmp_ordi_predict.tmp_send_flow_static_pis_05_1_tmp20240111_v1;
create table if not exists tmp_ordi_predict.tmp_send_flow_static_pis_05_1_tmp20240111_v1 
stored as parquet as 
select 
        line_code
        ,plan_send_batch
        ,src_zone_code
        ,dest_zone_code
        ,cast(datediff(plan_arrive_dt,plan_send_batch_dt) as int) as dt_period
    from  
        (select line_code
            ,plan_send_batch
            ,src_zone_code
            ,dest_zone_code
            ,plan_send_batch_dt
            ,to_date(plan_arrive_tm) as plan_arrive_dt
            ,plan_arrive_batch_dt
            ,row_number()over(partition by line_code,src_zone_code order by create_tm desc) as rn 
        from  dm_oia.super_flow_vehicle_tasks_split_info_full_hudi_pro_rt a
        where 
		-- line_require_date>='$[time(yyyy-MM-dd,-30d)]' and line_require_date<='$[time(yyyy-MM-dd,+7d)]'
		line_require_date BETWEEN '2023-12-12' and '2024-01-18'					--  回刷历史，约束时间范围 
		and last_update_tm  <= '2024-01-11 12:00:00'							--  回刷历史，约束时间范围  last_update_tm	2022-01-02 09:07:06.068
		-- 	and create_tm  <= '2024-01-11 12:00:00'								--  回刷历史，约束时间范围  create_tm	2021-12-25 06:14:47.743
        ) t
        where t.rn=1 and line_code is not null 
    group by  line_code
        ,plan_send_batch
        ,src_zone_code
        ,dest_zone_code
        ,cast(datediff(plan_arrive_dt,plan_send_batch_dt) as int);


-- step4  -- pis发件-获取班次及结果

set spark.sql.shuffle.partitions=1000;

-- 关联获取发出班次

drop table if exists tmp_ordi_predict.tmp_send_flow_static_pis_06_tmp20240111_v1;
create table if not exists tmp_ordi_predict.tmp_send_flow_static_pis_06_tmp20240111_v1 
stored as parquet as 
select 
    waybillno 
    ,meterageweightqty   -- 20230719添加               
	,link_index              
	,class_type              
	,link_type               
	,static_zone    
	,static_class      
	,static_op_code       
	,process_time            
	,process_date            
	,consigned_time          
	,consigned_date          
	,nvl(next_s_zone_code,t2.dest_zone_code) as  next_s_zone_code    
	,nvl(next_process_date, date_add(batch_date,dt_period)) as next_process_date     
	,batch_date                 
	,send_zone_name          
	,send_zone_type_code     
	,send_zone_type_name     
	,send_zone_trans_flag    
	,send_zone_is_trans      
	,arr_zone_name           
	,arr_zone_type_code      
	,arr_zone_type_name      
	,arr_zone_trans_flag     
	,arr_zone_is_trans  
    ,plan_send_batch     
from (select * from tmp_ordi_predict.tmp_send_flow_static_pis_05_1_tmp20240111_v1 where line_code is not null)t2 
right join 
(select * from tmp_ordi_predict.tmp_send_flow_static_pis_05_tmp20240111_v1 
where send_zone_is_trans='1' and (static_op_code ='30' or link_type='装车')) t1
on t1.static_class=t2.line_code
and t1.static_zone=t2.src_zone_code;


-- insert overwrite table dm_ordi_predict.dws_trans_sendbatch_nextcode_quantity_hi partition(inc_day='$[time(yyyyMMddHH)]')
insert overwrite table tmp_ordi_predict.dws_trans_sendbatch_nextcode_quantity_hi_backup20240111 partition(inc_day='2024011112')		--  回刷历史，约束时间范围 
select 
	 t1.trans_code
	, t1.trans_batch_code
	, t1.trans_batch_dt
	, t1.trans_name
	, t1.trans_type_code
	, t1.trans_type_name
	, t1.next_arrive_dt
	, t1.next_zone_code
	, t1.next_zone_name
	, t1.next_type_code
	, t1.next_type_name
	, t1.send_quantity
	,t1.col_01
	,case when send_quantity=0 and t1.col_01>0
		  then 0
		  when send_quantity>0 and t1.col_01=0
		  then send_quantity*median_actual_vote_weight_30
		  when public_holiday not in ('春节','端午节','劳动节','中秋节','国庆节') 
			  and t1.col_01/send_quantity>max_actual_vote_weight_30
			  and send_quantity>max_actual_weight_30
			  and nvl(median_actual_vote_weight_30,0)>0
		  then send_quantity*median_actual_vote_weight_30
			  when public_holiday  in ('春节','端午节','劳动节','中秋节','国庆节') 
			  and t1.col_01/send_quantity>2*nvl(max_actual_vote_weight_30,0)
			  and t1.col_01>2*nvl(max_actual_weight_30,0)
			  and nvl(median_actual_vote_weight_30,0)>0
		  then send_quantity*median_actual_vote_weight_30
		  else t1.col_01
		 end as col_02
	,t1.col_03
	,t1.col_04
	,t1.col_05
from (
select 
    static_zone as trans_code
    ,plan_send_batch as trans_batch_code
    ,batch_date as trans_batch_dt
    ,nvl(send_zone_name,t2.dept_name) as trans_name
    ,nvl(send_zone_type_code,t2.dept_type_code) as trans_type_code
    ,nvl(send_zone_type_name,t2.dept_type_name) as trans_type_name
    ,next_process_date as next_arrive_dt
    ,next_s_zone_code as next_zone_code
    ,nvl(arr_zone_name,t3.dept_name) as next_zone_name
    ,nvl(arr_zone_type_code,t3.dept_type_code) as next_type_code
    ,nvl(arr_zone_type_name,t3.dept_type_name) as next_type_name
    ,count(distinct waybillno) as send_quantity
    ,sum(meterageweightqty) as col_01
    ,'' as col_02
    ,'' as col_03
    ,'' as col_04
    ,'' as col_05
from tmp_ordi_predict.tmp_send_flow_static_pis_06_tmp20240111_v1 T1
left join 
(select dept_code,dept_name,dept_type_code,dept_type_name from dim.dim_department a where hq_code <>'CN39') t2
on t1.static_zone=t2.dept_code
left join 
(select dept_code,dept_name,dept_type_code,dept_type_name from dim.dim_department a where hq_code <>'CN39') t3
on t1.next_s_zone_code=t3.dept_code
where t2.dept_code is not null and t3.dept_code is not null
and  plan_send_batch is not null 
and next_s_zone_code is not null 
and next_process_date is not null
and batch_date is not null
-- and consigned_date between '$[time(yyyy-MM-dd,-7d)]'  and '$[time(yyyy-MM-dd)]'
and consigned_date between '2024-01-04'  and '2024-01-11'								--  回刷历史，约束时间范围 	
-- and batch_date between '$[time(yyyy-MM-dd,-2d)]'  and '$[time(yyyy-MM-dd,5d)]' 		 -- 20230605修改,由于会存在班次跨天的情况
and batch_date between '2024-01-09'  and '2024-01-16'  									--  回刷历史，约束时间范围 
group by static_zone 
    ,plan_send_batch  
    ,batch_date 
    ,nvl(send_zone_name,t2.dept_name) 
    ,nvl(send_zone_type_code,t2.dept_type_code)  
    ,nvl(send_zone_type_name,t2.dept_type_name) 
    ,next_process_date 
    ,next_s_zone_code  
    ,nvl(arr_zone_name,t3.dept_name) 
    ,nvl(arr_zone_type_code,t3.dept_type_code) 
    ,nvl(arr_zone_type_name,t3.dept_type_name)) t1
left join 		
(select * from 	dm_ordi_predict.dws_trans_next_zone_vote_weight_avg_di t
      where exists (select inc_day_max 
                        from  (select max(inc_day) as  inc_day_max
                                    from dm_ordi_predict.dws_trans_next_zone_vote_weight_avg_di
                                -- where inc_day>= '$[time(yyyyMMdd,-1d)]' 
								where inc_day>= '20240110' 								--  回刷历史，约束时间范围 	
                             ) t1
                     where t.inc_day=t1.inc_day_max)
     ) t2				
on t1.trans_batch_code=t2.batch_code
and t1.next_zone_code=t2.next_zone_code
left join 
	(select day_date,public_holiday 
           from dm_predict.dm_bfms_calendar_cfg_dtl  
			where data_type='trans'
	) t3
on t1.trans_batch_dt=t3.day_date;

