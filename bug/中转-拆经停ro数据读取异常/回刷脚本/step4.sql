
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
insert overwrite table tmp_ordi_predict.dws_trans_sendbatch_nextcode_quantity_hi_backup20240111 partition(inc_day='$[time(yyyyMMddHH)]')		--  回刷历史，约束时间范围 
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
and consigned_date between '$[time(yyyy-MM-dd,-7d)]'  and '$[time(yyyy-MM-dd)]'
-- and consigned_date between '2024-01-04'  and '2024-01-11'								--  回刷历史，约束时间范围 	
and batch_date between '$[time(yyyy-MM-dd,-2d)]'  and '$[time(yyyy-MM-dd,5d)]' 		 -- 20230605修改,由于会存在班次跨天的情况
-- and batch_date between '2024-01-09'  and '2024-01-16'  									--  回刷历史，约束时间范围 
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
                               where inc_day>= '$[time(yyyyMMdd,-1d)]' 
								 -- where inc_day>= '20240110' 								--  回刷历史，约束时间范围 	
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

