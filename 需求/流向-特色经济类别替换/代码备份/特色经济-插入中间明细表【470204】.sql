
-----配置完插入中间表

set hive.execution.engine=tez;
-- 队列
set tez.queue.name=root.predict;

drop table if exists tmp_ordi_predict.tmp_shenhy_special_eco_waybill_ref_tmp4;
create table tmp_ordi_predict.tmp_shenhy_special_eco_waybill_ref_tmp4  
stored as parquet as
select 
	   cons_name_concat	,
      fetch_data,
      priority_no,
      broad_heading	,
      subdivision 
from tmp_ordi_predict.tmp_shenhy_special_eco_waybill_ref_tmp3 t
where t.fetch_data not in ('苹果','小米')
 or (t.fetch_data='苹果' 
 and upper(cons_name_concat) 
 not rlike '手机|电脑|U盘|配件|膜|智能|数码|电器|数据线|无线｜AIR|IPHONE|IAPD|PLUS|APPLE|6|7|8|9|10|11|白色|金色|银色|绿色|蓝色|橘黄色|黑色|粉色')
or (t.fetch_data='小米' and substr(cons_name_concat,-2)='小米');


drop table if exists tmp_ordi_predict.tmp_shenhy_special_eco_waybill_ref_tmp5_0;
create table tmp_ordi_predict.tmp_shenhy_special_eco_waybill_ref_tmp5_0
stored as parquet as
select t.waybill_no,                                        --运单号    
       t.product_code,
       t.source_zone_code,                                  --原寄件网点
	   t.dest_zone_code,
       t.consigned_tm,                                      --寄件时间
       t.cons_name,                                         --托寄物内容
       t.cons_name_concat,
       fetch_data,
       priority_no,                                         --优先级
       broad_heading,                                       --大类
       case when t.product_code='SE0034' and subdivision is null then '蟹类' else subdivision end as subdivision, --小类
       meterage_weight_qty,                                  --计费总重量
	   quantity,                                             --包裹总件数
	   freight_monthly_acct_code as monthly_acct_code,               --运费月结账号
    --     signin_tm
	--    t2.area_code
    t.src_area_code as area_code
  from tmp_ordi_predict.tmp_shenhy_air_six_weidu_tmp1  t  -- tmp_shenhy_special_eco_waybill_ref_tmp1
left join tmp_ordi_predict.tmp_shenhy_special_eco_waybill_ref_tmp4  b 
on trim(t.cons_name_concat)=trim(b.cons_name_concat) 
where t.inc_day between  '$[time(yyyyMMdd,-10d)]' and '$[time(yyyyMMdd,-1d)]' ;

/*
set hive.auto.convert.join=true; 
drop table if exists tmp_ordi_predict.tmp_shenhy_special_eco_waybill_ref_tmp5;
create table tmp_ordi_predict.tmp_shenhy_special_eco_waybill_ref_tmp5
stored as parquet as
select t.waybill_no,                                        --运单号    
       t.product_code,
       t.source_zone_code,                                  --原寄件网点
	   t.dest_zone_code,
       t.consigned_tm,                                      --寄件时间
       t.cons_name,                                         --托寄物内容
       t.cons_name_concat,
       fetch_data,
       priority_no,                                         --优先级
       broad_heading,                                       --大类
        subdivision, --小类
       meterage_weight_qty,                                  --计费总重量
	   quantity,                                             --包裹总件数
	   monthly_acct_code,              --运费月结账号
      --  signin_tm,
	  --  t2.area_code
      area_code
  from tmp_ordi_predict.tmp_shenhy_special_eco_waybill_ref_tmp5_0 t;*/

/*  
left join 
(select 
	 dept_code
	,area_code
from dim.dim_department 
group by 
	 dept_code
	,area_code) t2
on nvl(t.source_zone_code,'a')=nvl(t2.dept_code,'b')*/



set hive.exec.dynamic.partition= true;
set hive.exec.dynamic.partition.mode= nostrict;
set hive.exec.max.dynamic.partitions=2000;  -- 所有执行节点上最多可以多少个动态分区
set hive.exec.max.dynamic.partitions.pernode = 500;

insert overwrite table dm_ordi_predict.dws_spec_consign_waybill_day partition (inc_day)
select 
  waybill_no                  
 ,product_code 	           
 ,source_zone_code            
 ,dest_zone_code				
 ,consigned_tm                
 ,cons_name                   
 ,cons_name_concat				
 ,fetch_data					
 ,priority_no                 
 ,broad_heading               
 ,subdivision                 
 ,meterage_weight_qty         
 ,quantity                    
 ,monthly_acct_code           
 ,'' as signin_tm 	               
 ,area_code 
,regexp_replace(to_date(consigned_tm),'-','') as inc_day 
from  tmp_ordi_predict.tmp_shenhy_special_eco_waybill_ref_tmp5_0
where subdivision is not null;