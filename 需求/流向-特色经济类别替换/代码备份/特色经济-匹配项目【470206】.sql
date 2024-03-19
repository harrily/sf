

set hive.execution.engine=tez;
-- 队列
set tez.queue.name=root.predict;
set hive.auto.convert.join=true;

--------------------------------关联项目信息----------------------
drop table if exists tmp_ordi_predict.tmp_shenhy_special_eco_waybill_ref_tmp6;
create table tmp_ordi_predict.tmp_shenhy_special_eco_waybill_ref_tmp6
stored as parquet as
select 
       a.waybill_no,                                        --运单号    
       a.source_zone_code,                                  --原寄件网点
	   a.area_code,
       b.PRO_NAME,
       b.LEVEL_1_TYPE,
       b.LEVEL_2_TYPE,
       b.LEVEL_3_TYPE,
      --  b.PRO_START,
      --  b.PRO_END,
       a.consigned_tm,                                      --寄件时间
       a.cons_name,                                         --托寄物内容
       cons_name_concat,
       fetch_data,
       a.priority_no,                                         --托寄物优先级
       broad_heading,                                       --大类
       subdivision,                                         --小类
	 --   b.estimate_inc,
	 --   b.inc_unit,
	   b.priority_no as pro_prio_no,                        --项目优先级
	   a.dest_zone_code,
       meterage_weight_qty,                                  --计费总重量
	   quantity,                                             --包裹总件数
	   monthly_acct_code,                                    --运费月结账号
       product_code,
       signin_tm,
       inc_day
 from (select 
            area_code
            ,PRO_NAME
            ,LEVEL_2_TYPE
            ,priority_no
            ,inc_unit
            ,LEVEL_1_TYPE
            ,LEVEL_3_TYPE
--  from dm_bie.special_eco_pro 
from dm_ordi_predict.special_eco_pro   -- 原上游配置表被删除，用备份表恢复 2023-08-31
 where pro_year='2022'
 group by area_code
            ,PRO_NAME
            ,priority_no
            ,inc_unit
            ,LEVEL_1_TYPE
            ,LEVEL_2_TYPE
            ,LEVEL_3_TYPE)b 
 join dm_ordi_predict.dws_spec_consign_waybill_day a
 -- tmp_ordi_predict.tmp_shenhy_special_eco_waybill_ref_tmp5 a
-- inner join  
on a.area_code=b.area_code 
and a.subdivision=b.LEVEL_2_TYPE
where a.subdivision is not null 
-- and a.inc_day between '20180701' and '20180901'
and a.inc_day between '$[time(yyyyMMdd,-10d)]' and  '$[time(yyyyMMdd,-1d)]';




---- 跑太慢 0909去掉多余字段关联，并先去重
/*
drop table if exists tmp_ordi_predict.tmp_shenhy_special_eco_waybill_ref_tmp6;
create table tmp_ordi_predict.tmp_shenhy_special_eco_waybill_ref_tmp6
stored as parquet as
select 
       a.waybill_no,                                        --运单号    
       a.source_zone_code,                                  --原寄件网点
	   a.area_code,
       b.PRO_NAME,
       b.LEVEL_1_TYPE,
       b.LEVEL_2_TYPE,
       b.LEVEL_3_TYPE,
       b.PRO_START,
       b.PRO_END,
       a.consigned_tm,                                      --寄件时间
       a.cons_name,                                         --托寄物内容
       cons_name_concat,
       fetch_data,
       a.priority_no,                                         --托寄物优先级
       broad_heading,                                       --大类
       subdivision,                                         --小类
	   b.estimate_inc,
	   b.inc_unit,
	   b.priority_no as pro_prio_no,                        --项目优先级
	   a.dest_zone_code,
       meterage_weight_qty,                                  --计费总重量
	   quantity,                                             --包裹总件数
	   monthly_acct_code,                                    --运费月结账号
       product_code,
       signin_tm,
       inc_day
 from dm_bie.special_eco_pro b 
 join dm_ordi_predict.dws_spec_consign_waybill_day a
 -- tmp_ordi_predict.tmp_shenhy_special_eco_waybill_ref_tmp5 a
-- inner join  
on a.area_code=b.area_code 
and a.subdivision=b.LEVEL_2_TYPE
where a.subdivision is not null 
and b.pro_year='2022'
-- and a.inc_day between '20180701' and '20180901'
and a.inc_day between '$[time(yyyyMMdd,-10d)]' and  '$[time(yyyyMMdd,-1d)]';*/

