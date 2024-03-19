----------------------开发说明----------------------------
--* 名称:【预测归集-流向】非空网航空件归集
--* 任务ID:617182
--* 说明:非空网航空件归集
-- 3.城市流向特快（非空网） （1）归集省份对 （2）城市对增加省份字段
--* 作者:
--* 时间:

----------------------修改记录----------------------------
--* 修改人   修改时间      修改内容
-- 01431437   2023/05/15 09:20  -- 1、添加预测重量 2、优化省份对获取逻辑
----------------------------------------------------------
----------------------hive调优参数列表----------------------

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
and t1.inc_day='$[time(yyyyMMdd,-1d)]'  --20230417修改取数分区为昨日的分区
and t1.object_type = '航空城市流向'    --20230512 筛选 航空城市流向
and t1.object_code regexp '(\\\d{3})-(\\\d{3})' ; -- 20230519 过滤掉国际数据

-- 归集省份对
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
-- ,split(t1.object_name,'-')[0] as origin_Name
-- ,split(t1.object_name,'-')[1] as dest_Name
,case when t8.city_name is not null and split(t1.object_code,'-')[0] rlike '^\\d+$' then t8.city_name
            else '' end as origin_Name
,case when t9.city_name is not null and split(t1.object_code,'-')[1] rlike '^\\d+$' then t9.city_name
            else '' end as dest_Name
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
,case when t2.province is not null and split(t1.object_code,'-')[0] rlike '^\\d+$' then t2.province
			  when t2.province is null and t6.provinct_name is not null then replace(replace(t6.provinct_name,'省',''),'自治区','')
			  else '海外' end as src_province
,case when t3.province is not null and split(t1.object_code,'-')[1] rlike '^\\d+$' then t3.province
			  when t3.province is null and t7.provinct_name is not null then replace(replace(t7.provinct_name,'省',''),'自治区','')
			  else '海外' end as dest_province
from tmp_dm_predict.tmp_shenhy_noairflow_collect_tmp1 t1   
left join 
    (select city_code,province_name,province,distribution_name 
    from dm_ops.dim_city_area_distribution 
    group by city_code,province_name,province,distribution_name) t2
on nvl(split(t1.object_code,'-')[0],'a')=t2.city_code
left join 
    (select city_code,province_name,province,distribution_name 
    from dm_ops.dim_city_area_distribution 
    group by city_code,province_name,province,distribution_name) t3
on nvl(split(t1.object_code,'-')[1],'a')=t3.city_code
left join
 tmp_dm_predict.tmp_dm_fc_city_2_hq_df_1 t4
on nvl(split(t1.object_code,'-')[0],'a')=t4.city_code
left join 
 tmp_dm_predict.tmp_dm_fc_city_2_hq_df_1 t5
on nvl(split(t1.object_code,'-')[1],'a')=t5.city_code
left join 
(select dept_code,provinct_name from dim.dim_department where delete_flg='0'AND country_code ='CN') t6			-- 添加省份关联不到的取dim.dim_department
on nvl(split(t1.object_code,'-')[0],'a')=t6.dept_code
left join 				
 (select dept_code,provinct_name from dim.dim_department where delete_flg='0' AND country_code ='CN') t7			-- 添加省份关联不到的取dim.dim_department
on nvl(split(t1.object_code,'-')[1],'a')=t7.dept_code 
left join 
    (select dist_code,city_name from tmp_dm_predict.zhaojinyang_dim_department_city group by dist_code,city_name) t8  -- 20230519新增匹配城市名称
on nvl(split(t1.object_code,'-')[0],'a')=t8.dist_code
left join 
    (select dist_code,city_name from tmp_dm_predict.zhaojinyang_dim_department_city group by dist_code,city_name) t9  -- 20230519新增 匹配城市名称
on nvl(split(t1.object_code,'-')[1],'a')=t9.dist_code;



-- 空网件均重 -- 城市流向
drop table if exists tmp_dm_predict.tmp_dm_cityflow_month_wgt_1;
create table if not exists tmp_dm_predict.tmp_dm_cityflow_month_wgt_1 stored as parquet as
select
  send_province,
  arrive_province,
  dist_code,-- 起始城市
  avg(avg_weight) as avg_weight
from
  (
    select
      plan_send_dt,
      send_province,
      arrive_province,
      dist_code, -- 起始城市
      sum(real_weight) as sum_real_weight,
      sum(waybill_cnt) as sum_waybill_cnt,
      sum(real_weight) / sum(waybill_cnt) as avg_weight
    from
      (
        select
          plan_send_dt,
          t3.province as send_province,
          arrive_province,
          real_weight,
          waybill_cnt,
          avg_weight,
          zone_code,
          inc_day,
          t2.dist_code
        from
          dm_ops.dm_zz_air_waybill_avg_weight t1
          left join (
            select
              dept_code,
              dist_code
            from
              dim.dim_department
          ) t2 on t1.zone_code = t2.dept_code
          left join dm_ops.dim_city_area_distribution t3 on t2.dist_code = t3.city_code
        where
          inc_day between '$[time(yyyyMMdd,-45d)]' and '$[time(yyyyMMdd,-1d)]'
      ) t
    group by
      plan_send_dt,
      send_province,
      arrive_province,
      dist_code
  ) t1
group by
  send_province,
  arrive_province,
  dist_code;

-- 非空网- 预测重量计算  --城市流向 
drop table if exists tmp_dm_predict.tmp_shenhy_noairflow_collect_tmp2_1;
create table if not exists tmp_dm_predict.tmp_shenhy_noairflow_collect_tmp2_1 stored as parquet as
	select
	  data_version,
	  feature_version,
	  model_version,
	  predict_datetime,
	  origin_Code,
	  dest_Code,
	  origin_Name,
	  dest_Name,
	  object_Type_Code,
	  object_Type,
	  weight_Level,
	  product_Type,
	  operation_Type,
	  predict_Quantity,
	  predict_Waybill,
	  case
		when t2.avg_weight is not NULL then t1.predict_Quantity * t2.avg_weight		
		when t3.avg_weight is not NULL then t1.predict_Quantity * t3.avg_weight		
		when t4.avg_weight is not NULL then t1.predict_Quantity * t4.avg_weight
		else t1.predict_Quantity * t5.avg_weight
	  end as predict_Weight,
	  predict_Volume,
	  src_area_code,
	  src_fbq_code,
	  src_hq_code,
	  dest_area_code,
	  dest_fbq_code,
	  dest_hq_code,
	  src_province,
	  dest_province
    from
		tmp_dm_predict.tmp_shenhy_noairflow_collect_tmp2 t1
    left join tmp_dm_predict.tmp_dm_cityflow_month_wgt_1 t2 
        on nvl(t1.origin_Code,'null') = t2.dist_code
        and nvl(t1.dest_province,'null')= t2.arrive_province
    left join (
        select
			send_province,
			arrive_province,
			avg(avg_weight) as avg_weight
        from
			tmp_dm_predict.tmp_dm_cityflow_month_wgt_1
        group by
			send_province,
			arrive_province
    ) t3 
        on nvl(t1.src_province,'null') = t3.send_province
        and nvl(t1.dest_province,'null') = t3.arrive_province
	left join (
        select
			send_province,
			avg(avg_weight) as avg_weight
		from
			tmp_dm_predict.tmp_dm_cityflow_month_wgt_1
        group by send_province
    ) t4 
        on nvl(t1.src_province,'null') = t4.send_province
    left join (
        select
			avg(avg_weight) as avg_weight
        from
			tmp_dm_predict.tmp_dm_cityflow_month_wgt_1 
    ) t5 on 1=1 ;


 -- 非空网流向 - 省份 预测汇总
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
,sum(nvl(predict_Weight,0)) as predict_Weight
,sum(predict_Volume) as predict_Volume
,'' as src_Area_Code
,'' as src_Fbq_Code
,'' as src_Hq_Code
,'' as dest_Area_Code
,'' as dest_Fbq_Code
,'' as dest_Hq_Code
,src_province
,dest_province
from tmp_dm_predict.tmp_shenhy_noairflow_collect_tmp2_1 t1
group by 
data_version
,feature_version
,model_version
,predict_datetime
,src_province
,dest_province;

 -- 合并非空网 --省份 ，城市流向数据
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
from tmp_dm_predict.tmp_shenhy_noairflow_collect_tmp2_1 t1
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


-- 写入归集表
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