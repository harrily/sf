
/**

	非空网 format 

**/
drop table if exists  tmp_dm_predict.dws_fc_nohk_tekuai_predict_collecting_di_20230606;
create table if not exists tmp_dm_predict.dws_fc_nohk_tekuai_predict_collecting_di_20230606 
stored as parquet as
SELECT data_version,feature_version,model_version,predict_datetime,origin_code,dest_code,origin_name,dest_name,object_type_code,object_type,weight_level,product_type,operation_type,predict_quantity,predict_waybill,predict_weight,predict_volume,record_time,src_area_code,src_fbq_code,src_hq_code,dest_area_code,dest_fbq_code,dest_hq_code,src_province,dest_province,partition_key FROM dm_predict.dws_fc_nohk_tekuai_predict_collecting_di 
where partition_key >= '20230401' ;


set hive.exec.parallel=true;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;  -- 非严格模式
set hive.exec.max.dynamic.partitions=2000;  -- 所有执行节点上最多可以多少个动态分区
set hive.exec.max.dynamic.partitions.pernode = 500;

insert overwrite table dm_predict.dws_fc_nohk_tekuai_predict_collecting_di partition(partition_key)
SELECT
  data_version,
  feature_version,
  model_version,
  predict_datetime,
  origin_code,
  dest_code,
  if(t2.city_name is not null,t2.city_name,origin_name) as origin_name,
  if(t3.city_name is not null,t3.city_name,dest_name) as dest_name,
  object_type_code,
  object_type,
  weight_level,
  product_type,
  operation_type,
  predict_quantity,
  predict_waybill,
  predict_weight,
  predict_volume,
  record_time,
  src_area_code,
  src_fbq_code,
  src_hq_code,
  dest_area_code,
  dest_fbq_code,
  dest_hq_code,
  if(t2.province is not null,t2.province,src_province) as src_province,
  if(t3.province is not null,t3.province,dest_province) as dest_province,
  partition_key
FROM
 (select * from dm_predict.dws_fc_nohk_tekuai_predict_collecting_di 
 where partition_key >= '20230401' 
 and origin_code regexp '(\\\d{3})' 
 and  dest_code regexp '(\\\d{3})'
 ) t1 
 left join
(select
     city_code   --城市代码
    ,city_name  --城市名称
    ,province   --省份名称
from dm_ordi_predict.dim_city_level_mapping_df 
where inc_day='$[time(yyyyMMdd,-1d)]'
and  if_foreign='0'  -- 筛选国内
) t2
on origin_code=t2.city_code
left join
(select
     city_code   --城市代码
    ,city_name  --城市名称
    ,province   --省份名称
from dm_ordi_predict.dim_city_level_mapping_df 
where inc_day='$[time(yyyyMMdd,-1d)]'
and  if_foreign='0' -- 筛选国内 
) t3
 on dest_code=t3.city_code ;
 
 
 
 
/**
	
	dm_predict.dws_fc_flow_predict_collecting_di
【预测归集-流向】城市&航空0至75D归集new
**/
  drop table if exists  tmp_dm_predict.dws_fc_flow_predict_collecting_di_20230606;
create table if not exists tmp_dm_predict.dws_fc_flow_predict_collecting_di_20230606 
stored as parquet as
SELECT data_version,feature_version,model_version,predict_datetime,origin_code,dest_code,origin_name,dest_name,object_type_code,object_type,weight_level,product_type,predict_quantity,predict_waybill,predict_weight,predict_volume,record_time,operation_type,src_area_code,src_fbq_code,src_hq_code,dest_area_code,dest_fbq_code,dest_hq_code,partition_key FROM dm_predict.dws_fc_flow_predict_collecting_di
where partition_key >= '20230401' ;


set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;  -- 非严格模式
set hive.exec.max.dynamic.partitions=2000;  -- 所有执行节点上最多可以多少个动态分区
set hive.exec.max.dynamic.partitions.pernode = 500;

insert overwrite table dm_predict.dws_fc_flow_predict_collecting_di partition(partition_key)
 SELECT
  data_version,
  feature_version,
  model_version,
  predict_datetime,
  origin_code,
  dest_code,
if(t2.city_name is not null ,t2.city_name,origin_name)  as Origin_name ,       -- 匹配城市  20230526
if(t3.city_name is not null ,t3.city_name,dest_name)  as dest_name ,           -- 匹配城市  20230526
  object_type_code,
  object_type,
  weight_level,
  product_type,
  predict_quantity,
  predict_waybill,
  predict_weight,
  predict_volume,
  record_time,
  operation_type,
  src_area_code,
  src_fbq_code,
  src_hq_code,
  dest_area_code,
  dest_fbq_code,
  dest_hq_code,
  partition_key
FROM
 (select * from dm_predict.dws_fc_flow_predict_collecting_di where partition_key >= '20230401' ) t1
left join
(select
     city_code   --城市代码
    ,city_name  --城市名称
    ,province   --省份名称
from dm_ordi_predict.dim_city_level_mapping_df 
where inc_day='$[time(yyyyMMdd,-1d)]'
and  if_foreign='0'  -- 筛选国内
) t2
on origin_code=t2.city_code
left join
(select
     city_code   --城市代码
    ,city_name  --城市名称
    ,province   --省份名称
from dm_ordi_predict.dim_city_level_mapping_df 
where inc_day='$[time(yyyyMMdd,-1d)]'
and  if_foreign='0' -- 筛选国内 
) t3
 on dest_code=t3.city_code ;
 
 
 
  
/**
dm_predict.dws_fc_hk_six_dims_predict_collecting_di
--航空流向五维度预测结果表
dm_predict.hk_cityflow_qty_predict_day_short_period 
--航空流向短期预测结果表
**/
drop table if exists  tmp_dm_predict.hk_cityflow_qty_predict_day_short_period_20230606;
create table if not exists tmp_dm_predict.hk_cityflow_qty_predict_day_short_period_20230606 
stored as parquet as
SELECT data_version,feature_version,model_version,flag_predict,flag_outlier,record_time,object_code,object_name,object_type,weight_level,product_type,all_detail,operation_type,predict_value,predict_period,predicted_datetime,task_type,object_type_code,inc_day FROM dm_predict.hk_cityflow_qty_predict_day_short_period ;

set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;  -- 非严格模式
set hive.exec.max.dynamic.partitions=2000;  -- 所有执行节点上最多可以多少个动态分区
set hive.exec.max.dynamic.partitions.pernode = 500;
insert overwrite table dm_predict.hk_cityflow_qty_predict_day_short_period partition(task_type,object_type_code,inc_day)
SELECT
  data_version,
  feature_version,
  model_version,
  flag_predict,
  flag_outlier,
  record_time,
  object_code,
  concat(if(t2.city_name is not null ,t2.city_name,split(t1.object_name,'-')[0]),'-',if(t3.city_name is not null ,t3.city_name,split(t1.object_name,'-')[1])) as object_name,
  object_type,
  weight_level,
  product_type,
  all_detail,
  operation_type,
  predict_value,
  predict_period,
  predicted_datetime,
  task_type,
  object_type_code,
  inc_day
FROM
  (select * from  dm_predict.hk_cityflow_qty_predict_day_short_period where inc_day >='20230401' )t1
 left join
(select
     city_code   --城市代码
    ,city_name  --城市名称
    ,province   --省份名称
from dm_ordi_predict.dim_city_level_mapping_df    -- 关联城市
where inc_day='$[time(yyyyMMdd,-1d)]'
and  if_foreign='0'  -- 筛选国内
) t2
on nvl(split(t1.object_code,'-')[0],'a')=t2.city_code
left join
(select
     city_code   --城市代码
    ,city_name  --城市名称
    ,province   --省份名称
from dm_ordi_predict.dim_city_level_mapping_df     -- 关联城市
where inc_day='$[time(yyyyMMdd,-1d)]'
and  if_foreign='0' -- 筛选国内 
) t3
on nvl(split(t1.object_code,'-')[1],'a')=t3.city_code;



--- step2 
drop table if exists  tmp_dm_predict.dws_fc_hk_six_dims_predict_collecting_di_20230606;
create table if not exists tmp_dm_predict.dws_fc_hk_six_dims_predict_collecting_di_20230606 
stored as parquet as
SELECT data_version,feature_version,model_version,predict_datetime,origin_code,dest_code,origin_name,dest_name,object_type_code,object_type,weight_level,operation_type,product_type,predict_quantity,predict_waybill,predict_weight,predict_volume,record_time,src_area_code,src_fbq_code,src_hq_code,dest_area_code,dest_fbq_code,dest_hq_code,src_province,dest_province ,partition_key FROM dm_predict.dws_fc_hk_six_dims_predict_collecting_di ;


set hive.exec.parallel=true;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;  -- 非严格模式
set hive.exec.max.dynamic.partitions=2000;  -- 所有执行节点上最多可以多少个动态分区
set hive.exec.max.dynamic.partitions.pernode = 500;

insert overwrite table dm_predict.dws_fc_hk_six_dims_predict_collecting_di partition(partition_key)
 SELECT
  data_version,
  feature_version,
  model_version,
  predict_datetime,
  origin_code,
  dest_code,
	if(t2.city_name is not null ,t2.city_name,t1.Origin_name) as Origin_name ,        -- 关联修改 20230526
	if(t3.city_name is not null ,t3.city_name,t1.Dest_name) as Dest_name ,        -- 关联修改 20230526
  object_type_code,
  object_type,
  weight_level,
  operation_type,
  product_type,
  predict_quantity,
  predict_waybill,
  predict_weight,
  predict_volume,
  record_time,
  src_area_code,
  src_fbq_code,
  src_hq_code,
  dest_area_code,
  dest_fbq_code,
  dest_hq_code,
    if(t2.province is not null ,t2.province,t1.src_province) as src_province,    -- 关联修改 20230526
    if(t3.province is not null ,t3.province,t1.dest_province) as dest_province,    -- 关联修改 20230526
  partition_key
FROM
 (select * from  dm_predict.dws_fc_hk_six_dims_predict_collecting_di where partition_key >= '20230401')  t1
 left join
(select
     city_code   --城市代码
    ,city_name  --城市名称
    ,province   --省份名称
from dm_ordi_predict.dim_city_level_mapping_df 
where inc_day='$[time(yyyyMMdd,-1d)]'
and  if_foreign='0'  -- 筛选国内
) t2
on t1.Origin_code=t2.city_code
left join
(select
     city_code   --城市代码
    ,city_name  --城市名称
    ,province   --省份名称
from dm_ordi_predict.dim_city_level_mapping_df 
where inc_day='$[time(yyyyMMdd,-1d)]'
and  if_foreign='0' -- 筛选国内 
) t3
on t1.Dest_code=t3.city_code ;


/**


select count(1) from  dm_predict.dws_fc_nohk_tekuai_predict_collecting_di where partition_key >= '20230401'   -- 3189064  -- 剔除null的城市和国外 2620983 
select count(1) from dm_predict.dws_fc_nohk_tekuai_predict_collecting_di -- 5121631    -- 4553550


select * from dm_predict.dws_fc_nohk_tekuai_predict_collecting_di
where origin_code regexp '(\\\d{3})'
 
select count(1) from dm_predict.dws_fc_flow_predict_collecting_di  -- 283325826
select count(1) from dm_predict.dws_fc_flow_predict_collecting_di  where partition_key >= '20230401'  -- 97543885


select count(1) from  dm_predict.hk_cityflow_qty_predict_day_short_period  -- 158429991
select count(1) from   dm_predict.hk_cityflow_qty_predict_day_short_period  where inc_day >= '20230401'  --  47973337


**/