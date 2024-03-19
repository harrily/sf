
-- 3.城市流向特快（非空网） （1）归集省份对 （2）城市对增加省份字段
-- （1）dm_ordi_predict.cityflow_fast_resultz_online
drop table if exists  tmp_dm_predict.tmp_shenhy_noairflow_collect_tmp1;
create table if not exists tmp_dm_predict.tmp_shenhy_noairflow_collect_tmp1 
stored as parquet as
select 
t1.*
from dm_ordi_predict.cityflows_fast_resultz_online t1
left join 
dm_ordi_predict.cf_air_list t2
on t1.object_code=t2.cityflow
where t2.cityflow is null
and t1.inc_day='$[time(yyyyMMdd,-1d)]';  --20230417修改取数分区为昨日的分区
and object_type = '航空城市流向'


drop table if exists  tmp_dm_predict.tmp_shenhy_noairflow_collect_tmp2;
create table if not exists tmp_dm_predict.tmp_shenhy_noairflow_collect_tmp2 
stored as parquet as
select 
data_version
,feature_version
,model_version
,predicted_datetime as predict_datetime
,split(t1.object_code,'-')[0] as origin_Code
,split(t1.object_code,'-')[1] as dest_Code
,split(t1.object_name,'-')[0] as origin_Name
,split(t1.object_name,'-')[1] as dest_Name
,'8' as object_Type_Code
,'非空网流向-城市' as object_Type
,'' as weight_Level
,'' as product_Type
,'all' as operation_Type
,predict_value as predict_Quantity
,'' as predict_Waybill
,'' as predict_Weight
,'' as predict_Volume
,t4.area_code as src_area_code
,t4.fbq_code as src_fbq_code
,t4.hq_code as src_hq_code
,t5.area_code as dest_area_code 
,t5.fbq_code as dest_fbq_code
,t5.hq_code as dest_hq_code
,t2.province as src_province
,t3.province as dest_province
from tmp_dm_predict.tmp_shenhy_noairflow_collect_tmp1 t1
left join 
dm_ops.dim_city_area_distribution t2
on split(t1.object_code,'-')[0]=t2.city_code
left join 
dm_ops.dim_city_area_distribution t3
on split(t1.object_code,'-')[1]=t3.city_code
left join
 tmp_dm_predict.tmp_dm_fc_city_2_hq_df_1 t4
on split(t1.object_code,'-')[0]=t4.city_code
left join 
 tmp_dm_predict.tmp_dm_fc_city_2_hq_df_1 t5
on split(t1.object_code,'-')[1]=t5.city_code;

drop table if exists  tmp_dm_predict.tmp_shenhy_noairflow_collect_tmp3;
create table if not exists tmp_dm_predict.tmp_shenhy_noairflow_collect_tmp3 
stored as parquet as
select 
data_version
,feature_version
,model_version
,predict_datetime
,src_province as origin_Code
,dest_province as dest_Code
,src_province as origin_Name
,dest_province as dest_Name
,'9' as object_Type_Code
,'非空网流向-省份' as object_Type
,'' as weight_Level
,'' as product_Type
,'all' as operation_Type
,sum(nvl(predict_quantity,0)) as predict_quantity
,sum(predict_Waybill) as predict_Waybill
,sum(predict_Weight) as predict_Weight
,sum(predict_Volume) as predict_Volume
,'' as src_Area_Code
,'' as src_Fbq_Code
,'' as src_Hq_Code
,'' as dest_Area_Code
,'' as dest_Fbq_Code
,'' as dest_Hq_Code
,src_province
,dest_province
from tmp_dm_predict.tmp_shenhy_noairflow_collect_tmp2 t1
group by 
data_version
,feature_version
,model_version
,predict_datetime
,src_province
,dest_province;


drop table if exists  tmp_dm_predict.tmp_shenhy_noairflow_collect_tmp4;
create table if not exists tmp_dm_predict.tmp_shenhy_noairflow_collect_tmp4 
stored as parquet as
select 
data_version
,feature_version
,model_version
,predict_datetime
,origin_Code
,dest_Code
,origin_Name
,dest_Name
,object_Type_Code
,object_Type
,weight_Level
,product_Type
,operation_Type
,predict_Quantity
,predict_Waybill
,predict_Weight
,predict_Volume
,from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') as record_Time
,src_area_code
,src_fbq_code
,src_hq_code
,dest_area_code 
,dest_fbq_code
,dest_hq_code
,src_province
,dest_province
from tmp_dm_predict.tmp_shenhy_noairflow_collect_tmp2 t1
union ALL
select 
data_version
,feature_version
,model_version
,predict_datetime
,origin_Code
,dest_Code
,origin_Name
,dest_Name
,object_Type_Code
,object_Type
,weight_Level
,product_Type
,operation_Type
,predict_Quantity
,predict_Waybill
,predict_Weight
,predict_Volume
,from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') as record_Time
,src_area_code
,src_fbq_code
,src_hq_code
,dest_area_code 
,dest_fbq_code
,dest_hq_code
,src_province
,dest_province
from tmp_dm_predict.tmp_shenhy_noairflow_collect_tmp3 t2;


set hive.exec.parallel=true;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;  -- 非严格模式
set hive.exec.max.dynamic.partitions=2000;  -- 所有执行节点上最多可以多少个动态分区
set hive.exec.max.dynamic.partitions.pernode = 500;

insert overwrite table dm_predict.dws_fc_nohk_tekuai_predict_collecting_di partition(partition_key)
select
data_version
,feature_version
,model_version
,predict_datetime
,origin_Code
,dest_Code
,origin_Name
,dest_Name
,object_Type_Code
,object_Type
,weight_Level
,product_Type
,operation_Type
,round(predict_Quantity,0) as predict_Quantity
,predict_Waybill
,predict_Weight
,predict_Volume
,record_Time
,src_area_code
,src_fbq_code
,src_hq_code
,dest_area_code 
,dest_fbq_code
,dest_hq_code
,src_province
,dest_province
,regexp_replace(predict_datetime,'-','') as partition_key
from tmp_dm_predict.tmp_shenhy_noairflow_collect_tmp4 ;

/*
drop table dm_predict.dws_fc_nohk_tekuai_predict_collecting_di;
CREATE TABLE dm_predict.dws_fc_nohk_tekuai_predict_collecting_di(
`data_version` string COMMENT '数据版本',
`feature_version` string COMMENT '特征版本',
`model_version` string COMMENT '模型版本',
`predict_datetime` date COMMENT '日期',
`origin_code` string COMMENT '起始城市代码',
`dest_code` string COMMENT '目的城市代码',
`origin_name` string COMMENT '起始城市名称',
`dest_name` string COMMENT '目的城市名称',
`object_type_code` string COMMENT '对象类型代码,8非空网流向-城市，9非空网流向-省份',
`object_type` string COMMENT '对象类型，非空网流向-城市，非空网流向-省份',
`weight_level` string COMMENT '重量段',
`product_type` string COMMENT '产品类型',
`operation_type` string COMMENT '营运维度',
`predict_quantity` double COMMENT '预测件量',
`predict_waybill` double COMMENT '预测票量',
`predict_weight` double COMMENT '预测重量',
`predict_volume` double COMMENT '预测体积',
`record_time` timestamp COMMENT '预测生成时间',
`src_area_code` string COMMENT '始发业务区',
`src_fbq_code` string COMMENT '始发分拨区',
`src_hq_code` string COMMENT '始发大区',
`dest_area_code` string COMMENT '目的业务区',
`dest_fbq_code` string COMMENT '目的分拨区',
`dest_hq_code` string COMMENT '目的大区',
`src_province` string COMMENT '起始省份',
`dest_province` string COMMENT '目的省份')
PARTITIONED BY (
`partition_key` string COMMENT '分区日期yyyyMMdd')
ROW FORMAT SERDE
'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat';
*/