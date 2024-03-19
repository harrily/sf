
/*CASE WHEN type IN ('经济快件') THEN '慢产品'
WHEN type IN ('特快系列','陆升航','特色经济','鞋服大客户') THEN '快产品'
ELSE null END AS type
-- 经济快件-新空配（特快包裹+顺丰空配）
-- 特快系列-特快
*/

-- 航空件均重
drop table if exists tmp_dm_predict.tmp_dm_cityflow_tekuai_month_wgt_1;
create table if not exists tmp_dm_predict.tmp_dm_cityflow_tekuai_month_wgt_1 
stored as parquet as
select
  send_province,
  arrive_province,
  operation_type,
  dist_code, -- 起始城市
  avg(avg_weight) as avg_weight
from (
select
  plan_send_dt,
  send_province,
  arrive_province,
  operation_type,
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
      CASE WHEN operation_type IN ('经济快件') THEN '慢产品'
           WHEN operation_type IN ('特快系列','陆升航','特色经济','鞋服大客户') THEN '快产品'
           ELSE null 
       END AS operation_type,
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
    left join 
    dm_ops.dim_city_area_distribution t3
    on t2.dist_code=t3.city_code
    where
      inc_day between '$[time(yyyyMMdd,-45d)]' and '$[time(yyyyMMdd,-1d)]' 
  ) t
group by
  plan_send_dt,
  send_province,
  arrive_province,
  operation_type,
  dist_code) t1
  where operation_type is not null
  group by  send_province,
  arrive_province,
  operation_type,
  dist_code;



drop table if exists tmp_dm_predict.tmp_dm_cityflow_tekuai_month_wgt_2;
create table if not exists tmp_dm_predict.tmp_dm_cityflow_tekuai_month_wgt_2 
stored as parquet as
select
t1.*
,t2.province as src_province
,t3.province as dest_province
from 
(select * from dm_predict.hk_cityflow_predict_month_period where inc_day = '$[time(yyyyMMdd)]') t1
left join 
dm_ops.dim_city_area_distribution t2
on split(t1.object_code,'-')[0]=t2.city_code
left join 
dm_ops.dim_city_area_distribution t3
on split(t1.object_code,'-')[1]=t3.city_code;


drop table if exists tmp_dm_predict.tmp_dm_cityflow_tekuai_month_wgt_3;
create table if not exists tmp_dm_predict.tmp_dm_cityflow_tekuai_month_wgt_3 
stored as parquet as
select
  		data_version
		,feature_version
		,model_version
		,flag_predict
		,flag_outlier
		,record_time
		,object_code
		,object_name
		,object_type
		,weight_level
		,product_type
		,all_detail
		,t1.operation_type
		,is_air_flow
		,predict_quantity
		,case when t2.avg_weight is not NULL
           then t1.predict_quantity*t2.avg_weight
           when t3.avg_weight is not NULL
           then t1.predict_quantity*t3.avg_weight
          else t1.predict_quantity*t4.avg_weight
     end as predict_weight
		,predicted_datetime
		,task_type
		,object_type_code
		,predict_period
		,inc_day
		,src_province
		,dest_province
from tmp_dm_predict.tmp_dm_cityflow_tekuai_month_wgt_2 t1
left join 
tmp_dm_predict.tmp_dm_cityflow_tekuai_month_wgt_1  t2
on split(t1.object_code,'-')[0]=t2.dist_code
and t1.dest_province=t2.arrive_province
and t1.operation_type=t2.operation_type
left join 
(select 
  send_province,
  arrive_province,
  operation_type,
  avg(avg_weight) as avg_weight
from tmp_dm_predict.tmp_dm_cityflow_tekuai_month_wgt_1
group by send_province,
  arrive_province,
  operation_type) t3
on t1.src_province=t3.send_province
and t1.dest_province=t3.arrive_province
and t1.operation_type=t3.operation_type 
left join 
(select 
  operation_type,
  avg(avg_weight) as avg_weight
from tmp_dm_predict.tmp_dm_cityflow_tekuai_month_wgt_1
group by operation_type) t4
on t1.operation_type=t4.operation_type ;

set hive.exec.dynamic.partition= true;
set hive.exec.dynamic.partition.mode= nostrict;
set hive.exec.max.dynamic.partitions=2000;  -- 所有执行节点上最多可以多少个动态分区
set hive.exec.max.dynamic.partitions.pernode = 500;

insert overwrite table dm_predict.hk_cityflow_predict_month_collecting partition (inc_day,predict_period)
 select 
        data_version
		,feature_version
		,model_version
		,flag_predict
		,flag_outlier
		,from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') as record_time
		,object_code
		,object_name
		,object_type
		,weight_level
		,product_type
		,all_detail
		,operation_type
		,is_air_flow
		,predict_quantity
		,predict_weight
		,predicted_datetime
		,task_type
		,object_type_code
		,src_province
		,dest_province
        ,inc_day
        ,predict_period
from tmp_dm_predict.tmp_dm_cityflow_tekuai_month_wgt_3 ;


/*
CREATE TABLE `dm_predict.hk_cityflow_predict_month_collecting`(
	`data_version` string COMMENT '输入数据的版本，如实时预测场景，必须写明是哪个时间的版本，T+x Day的场景，必须写明基于哪天的特征',
	`feature_version` string COMMENT '引入建模的数据特征版本编号',
	`model_version` string COMMENT '模型编号实际工作的模型编号',
	`flag_predict` string COMMENT '是否有可预测性',
	`flag_outlier` string COMMENT '数据是否异常',
	`record_time` string COMMENT '实际预测服务写入记录的时间',
	`object_code` string COMMENT '被预测的对象编号，如网点编号、地区编号、AOI编号、流向编号等等，例如城市到城市：010-020，省到省：山东-广东，航空总量',
	`object_name` string COMMENT '被预测的对象中文名称，例如城市到城市：北京-广州，省到省：山东-广东，航空总量',
	`object_type` string COMMENT '航空总量、航空流向省到省、航空城市流向、航空流向营运维度',
	`weight_level` string COMMENT '重量段，不区分填all',
	`product_type` string COMMENT '产品板块，不区分填all',
	`all_detail` string COMMENT '是否区分产品重量段的标识符，0区分，1不区分',
	`operation_type` string COMMENT '鞋服大客户、新空配、特快、特色经济、陆升航，快产品、慢产品，operation_type除五维度其他预测维度不填',
	`is_air_flow` string COMMENT '是否空网，1：空网，0：非空网',
	`predict_quantity` bigint COMMENT '件量预测值',
	`predict_weight` double COMMENT '重量预测值',
	`predicted_datetime` string COMMENT '被预测的日期时间（如要预测2021-07-12）',
	`task_type` string COMMENT '区分收派类型',
	`object_type_code` string COMMENT '1航空总量、2航空流向省到省、3航空城市流向、4航空流向营运维度(日度)、5航空流向营运维度(月度)',
	src_province string comment '起始省份',
	dest_province string comment '目的省份')
PARTITIONED BY (
    `inc_day` string COMMENT '更新日期',
`predict_period` string COMMENT '提前预测的周期，1D日度、1M月度'
)
ROW FORMAT SERDE
'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
STORED AS INPUTFORMAT
'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
*/
