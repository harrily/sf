
drop table if exists tmp_dm_predict.tmp_dm_cityflow_tekuai_month_wgt_2;
create table if not exists tmp_dm_predict.tmp_dm_cityflow_tekuai_month_wgt_2 
stored as parquet as
SELECT
  data_version,
  feature_version,
  model_version,
  flag_predict,
  flag_outlier,
  record_time,
  object_code,
  concat(if(t2.city_name is not null ,t2.city_name,split(t1.object_name,'-')[0]) ,'-',if(t3.city_name is not null ,t3.city_name,split(t1.object_name,'-')[1])) as object_name,
  object_type,
  weight_level,
  product_type,
  all_detail,
  operation_type,
  is_air_flow,
  predict_quantity,
  predict_weight,
  predicted_datetime,
  task_type,
  object_type_code,
  predict_period,
  inc_day,
  if(t2.province is not null,t2.province,'') as src_province,
  if(t3.province is not null,t3.province,'') as dest_province
from 
(select * from dm_predict.hk_cityflow_predict_month_period 
    where inc_day = '$[time(yyyyMMdd)]' and object_code regexp '(\\\d{3})-(\\\d{3})') t1 -- 20230519 添加过滤国际数据
left join
(select
     city_code   --城市代码
    ,city_name  --城市名称
    ,province   --省份名称
from dm_ordi_predict.dim_city_level_mapping_df 
where inc_day='$[time(yyyyMMdd,-1d)]'
and  if_foreign='0'  -- 筛选国内
) t2
on nvl(split(t1.object_code,'-')[0],'a')=t2.city_code
left join
(select
     city_code   --城市代码
    ,city_name  --城市名称
    ,province   --省份名称
from dm_ordi_predict.dim_city_level_mapping_df 
where inc_day='$[time(yyyyMMdd,-1d)]'
and  if_foreign='0' -- 筛选国内 
) t3
on nvl(split(t1.object_code,'-')[1],'a')=t3.city_code;


  



