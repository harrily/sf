
set hive.execution.engine=tez;
-- 队列
set tez.queue.name=root.predict;
set hive.auto.convert.join=true;

drop table if exists tmp_ordi_predict.tmp_shenhy_special_eco_waybill_ref_tmp7;
create table tmp_ordi_predict.tmp_shenhy_special_eco_waybill_ref_tmp7
stored as parquet as
select 
	t1.*
from (
select 
	t1.*
	 ,row_number() over(partition by waybill_no order by cast(pro_prio_no as int)) rn  
from tmp_ordi_predict.tmp_shenhy_special_eco_waybill_ref_tmp6 t1) t1
where rn=1;

/*
drop table dm_ordi_predict.dws_spec_eco_pro_waybill_day;
create table if not exists dm_ordi_predict.dws_spec_eco_pro_waybill_day(
	
   waybill_no                        string  comment  '收件日期'
  ,source_zone_code                  string  comment  '流向对'
  ,PRO_NAME						     string  comment  '起始城市名称'
  ,LEVEL_1_TYPE                      string  comment  '目的城市名称'
  ,LEVEL_2_TYPE                      string  comment  '是否航空流向'
  ,LEVEL_3_TYPE                      string  comment  '项目名称'
)
partitioned by (inc_day string comment'预测日期yyyymmdd')
stored as parquet;

*/

 --  and to_date(a.consigned_tm)>=to_date( regexp_replace(pro_start,'/','-'))
 --  and to_date(a.consigned_tm)<=to_date( regexp_replace(PRO_END,'/','-'));
  
set hive.exec.dynamic.partition= true;
set hive.exec.dynamic.partition.mode= nostrict;
set hive.exec.max.dynamic.partitions=2000;  -- 所有执行节点上最多可以多少个动态分区
set hive.exec.max.dynamic.partitions.pernode = 500;
insert overwrite table dm_ordi_predict.dws_spec_eco_pro_waybill_day partition (inc_day)
select 
      waybill_no,                                        --运单号    
      source_zone_code,                                  --原寄件网点
      PRO_NAME,
      LEVEL_1_TYPE,
      LEVEL_2_TYPE,
      LEVEL_3_TYPE,
      inc_day                             
from tmp_ordi_predict.tmp_shenhy_special_eco_waybill_ref_tmp7;