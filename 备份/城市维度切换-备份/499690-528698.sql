-- 摸底营运维度和短期模型配平
/*
drop table if exists tmp_dm_predict.tmp_shenhy_hk_dims_fixed_tmp3;
create table tmp_dm_predict.tmp_shenhy_hk_dims_fixed_tmp3 
stored as parquet as  
select 
    t1.data_version	    --		输入数据的版本，如实时预测场景，必须写明是哪个时间的版本，T+x Day的场景，必须写明基于哪天的特征
    ,t1.feature_version	    --		引入建模的数据特征版本编号
    ,t1.model_version	    --		模型编号实际工作的模型编号
    ,'0' as flag_predict	    --		是否有可预测性
    ,'0' as flag_outlier	    --		数据是否异常
    ,t1.record_time	    --		实际预测服务写入记录的时间
    ,concat(t1.Origin_code,'-',t1.dest_code) as object_code	     --		被预测的对象编号，如网点编号、地区编号、AOI编号、流向编号等等，例如城市到城市：010-020，省到省：山东-广东，航空总量
    ,concat(t1.Origin_name,'-',t1.dest_name) as object_name	     --		被预测的对象中文名称，例如城市到城市：北京-广州，省到省：山东-广东，航空总量
    ,'航空流向营运维度' as object_type	                          --		航空总量、航空流向省到省、航空城市流向、航空流向营运维度
    ,'all'  as weight_level	                                --		重量段，不区分填all
    ,'all'  as product_type	                                --		产品板块，不区分填all
    ,'1'    as all_detail	                                --		是否区分产品重量段的标识符，0区分，1不区分
    ,t1.operation_type	                                    --		鞋服大客户、新空配、特快、特色经济、陆升航，operation_type除五维度其他预测维度不填
    ,t1.type_qty_ratio*nvl(t2.predict_value,0) as  predict_value	    --		具体输出的预测值
    ,t1.predict_value as ori_predict_quantity                -- 原预测件量
    ,t2.predict_value  as flow_predict_quantity              -- 航空流向预测值
    ,case when t2.predict_period is not null 
          then  t2.predict_period
          else concat(datediff(t1.predicted_datetime ,to_date('$[time(yyyy-MM-dd)]'))+1,'D')
    end as predict_period   
    ,t1.predicted_datetime  	      
    ,'pickup' as task_type	        --	partition key	区分收派类型
    ,'4' as object_type_code	    --	partition key	1航空总量、2航空流向省到省、3航空城市流向、4航空流向营运维度
    ,'$[time(yyyyMMdd)]' as inc_day	        --	partition key	更新日期
    ,t3.province as src_province
    ,t4.province as dest_province  
from  tmp_dm_predict.tmp_shenhy_hk_dims_fixed_tmp2 t1
left join 
    (select 
        a.* 
    from dm_predict.hk_cityflow_qty_predict_day_short_period  a 
    where inc_day='$[time(yyyyMMdd)]'
    and object_type='航空城市流向' and all_detail='1' and task_type='pickup') t2
on t1.predicted_datetime=t2.predicted_datetime
and concat(t1.Origin_code,'-',t1.dest_code)=t2.object_code
left join 
    dm_ops.dim_city_area_distribution t3    -- 来自规调 获取省份
on t1.Origin_code=t3.city_code 
left join 
    dm_ops.dim_city_area_distribution t4
on t1.dest_code=t4.city_code;

--- 20221117添加 对航空城市流向有，但是航空营运维度无的情况，统一归为特快


drop table if exists tmp_dm_predict.tmp_shenhy_hk_dims_fixed_tmp3_0;
create table tmp_dm_predict.tmp_shenhy_hk_dims_fixed_tmp3_0 
stored as parquet as  
select 
    t1.data_version	    --		输入数据的版本，如实时预测场景，必须写明是哪个时间的版本，T+x Day的场景，必须写明基于哪天的特征
    ,t1.feature_version	    --		引入建模的数据特征版本编号
    ,t1.model_version	    --		模型编号实际工作的模型编号
    ,'0' as flag_predict	    --		是否有可预测性
    ,'0' as flag_outlier	    --		数据是否异常
    ,t1.record_time	    --		实际预测服务写入记录的时间
    ,t1.object_code 	     --		被预测的对象编号，如网点编号、地区编号、AOI编号、流向编号等等，例如城市到城市：010-020，省到省：山东-广东，航空总量
    ,t1.object_name	     --		被预测的对象中文名称，例如城市到城市：北京-广州，省到省：山东-广东，航空总量
    ,'航空流向营运维度' as object_type	                          --		航空总量、航空流向省到省、航空城市流向、航空流向营运维度
    ,'all'  as weight_level	                                --		重量段，不区分填all
    ,'all'  as product_type	                                --		产品板块，不区分填all
    ,'1'    as all_detail	                                --		是否区分产品重量段的标识符，0区分，1不区分
    ,'特快' as operation_type	                                    --		鞋服大客户、新空配、特快、特色经济、陆升航，operation_type除五维度其他预测维度不填
    ,nvl(t1.predict_value,0) as  predict_value	    --		具体输出的预测值
    ,t1.predict_value as ori_predict_quantity                -- 原预测件量
    ,t1.predict_value  as flow_predict_quantity              -- 航空流向预测值
    ,case when t1.predict_period is not null 
          then  t1.predict_period
          else concat(datediff(t1.predicted_datetime ,to_date('$[time(yyyy-MM-dd)]'))+1,'D')
    end as predict_period   
    ,t1.predicted_datetime  	      
    ,'pickup' as task_type	        --	partition key	区分收派类型
    ,'4' as object_type_code	    --	partition key	1航空总量、2航空流向省到省、3航空城市流向、4航空流向营运维度
    ,'$[time(yyyyMMdd)]' as inc_day	        --	partition key	更新日期
    ,t3.province as src_province
    ,t4.province as dest_province  
from   (select 
        a.* 
    from dm_predict.hk_cityflow_qty_predict_day_short_period  a -- 城市流向
    where inc_day='$[time(yyyyMMdd)]'
    and object_type='航空城市流向' and all_detail='1' and task_type='pickup')  t1
left join 
     tmp_dm_predict.tmp_shenhy_hk_dims_fixed_tmp2 t2
on t2.predicted_datetime=t1.predicted_datetime
and concat(t2.Origin_code,'-',t2.dest_code)=t1.object_code     
left join 
    dm_ops.dim_city_area_distribution t3    -- 来自规调 获取省份
on split(t1.object_code,'-')[0]=t3.city_code 
left join 
    dm_ops.dim_city_area_distribution t4
on split(t1.object_code,'-')[1]=t4.city_code
where t2.origin_code is null;*/


-- 摸底结果和短期结果配平
drop table if exists tmp_dm_predict.tmp_shenhy_hk_dims_fixed_tmp3;
create table tmp_dm_predict.tmp_shenhy_hk_dims_fixed_tmp3 
stored as parquet as  
select 
    t1.data_version	    --		输入数据的版本，如实时预测场景，必须写明是哪个时间的版本，T+x Day的场景，必须写明基于哪天的特征
    ,t1.feature_version	    --		引入建模的数据特征版本编号
    ,t1.model_version	    --		模型编号实际工作的模型编号
    ,'0' as flag_predict	    --		是否有可预测性
    ,'0' as flag_outlier	    --		数据是否异常
    ,t1.record_time	    --		实际预测服务写入记录的时间
    ,concat(t1.Origin_code,'-',t1.dest_code) as object_code	     --		被预测的对象编号，如网点编号、地区编号、AOI编号、流向编号等等，例如城市到城市：010-020，省到省：山东-广东，航空总量
    ,concat(t1.Origin_name,'-',t1.dest_name) as object_name	     --		被预测的对象中文名称，例如城市到城市：北京-广州，省到省：山东-广东，航空总量
    ,'航空流向营运维度' as object_type	                          --		航空总量、航空流向省到省、航空城市流向、航空流向营运维度
    ,'all'  as weight_level	                                --		重量段，不区分填all
    ,'all'  as product_type	                                --		产品板块，不区分填all
    ,'1'    as all_detail	                                --		是否区分产品重量段的标识符，0区分，1不区分
    ,t1.operation_type	                                    --		鞋服大客户、新空配、特快、特色经济、陆升航，operation_type除五维度其他预测维度不填
    ,t1.type_qty_ratio*nvl(t2.predict_value,0) as  predict_value	    --		具体输出的预测值
    ,t1.predict_value as ori_predict_quantity                -- 原预测件量
    ,t2.predict_value  as flow_predict_quantity              -- 航空流向预测值
    ,case when t2.predict_period is not null 
          then  t2.predict_period
          else concat(datediff(t1.predicted_datetime ,to_date('$[time(yyyy-MM-dd)]'))+1,'D')
    end as predict_period   
    ,t1.predicted_datetime  	      
    ,'pickup' as task_type	        --	partition key	区分收派类型
    ,'4' as object_type_code	    --	partition key	1航空总量、2航空流向省到省、3航空城市流向、4航空流向营运维度
    ,'$[time(yyyyMMdd)]' as inc_day	        --	partition key	更新日期
    ,t3.province as src_province
    ,t4.province as dest_province   
from  tmp_dm_predict.tmp_shenhy_hk_dims_fixed_tmp2 t1     --- 摸底结果
left join 
    tmp_dm_predict.tmp_shenhy_hk_dims_fixed_tmp_0_2 t2    --- 当天动态+短期模型结果
on t1.predicted_datetime=t2.predicted_datetime
and concat(t1.Origin_code,'-',t1.dest_code)=t2.object_code
left join 
    dm_ops.dim_city_area_distribution t3    -- 来自规调 获取省份
on t1.Origin_code=t3.city_code 
left join 
    dm_ops.dim_city_area_distribution t4
on t1.dest_code=t4.city_code;


-- 对于摸底无，（当天动态+短期）模型结果有的部分，填充到特快产品中
drop table if exists tmp_dm_predict.tmp_shenhy_hk_dims_fixed_tmp3_0;
create table tmp_dm_predict.tmp_shenhy_hk_dims_fixed_tmp3_0 
stored as parquet as  
select 
    t1.data_version	    --		输入数据的版本，如实时预测场景，必须写明是哪个时间的版本，T+x Day的场景，必须写明基于哪天的特征
    ,t1.feature_version	    --		引入建模的数据特征版本编号
    ,t1.model_version	    --		模型编号实际工作的模型编号
    ,'0' as flag_predict	    --		是否有可预测性
    ,'0' as flag_outlier	    --		数据是否异常
    ,t1.object_code 	     --		被预测的对象编号，如网点编号、地区编号、AOI编号、流向编号等等，例如城市到城市：010-020，省到省：山东-广东，航空总量
    ,t1.object_name	     --		被预测的对象中文名称，例如城市到城市：北京-广州，省到省：山东-广东，航空总量
    ,'航空流向营运维度' as object_type	                          --		航空总量、航空流向省到省、航空城市流向、航空流向营运维度
    ,'all'  as weight_level	                                --		重量段，不区分填all
    ,'all'  as product_type	                                --		产品板块，不区分填all
    ,'1'    as all_detail	                                --		是否区分产品重量段的标识符，0区分，1不区分
    ,'特快' as operation_type	                                    --		鞋服大客户、新空配、特快、特色经济、陆升航，operation_type除五维度其他预测维度不填
    ,nvl(t1.predict_value,0) as  predict_value	    --		具体输出的预测值
    ,t1.predict_value as ori_predict_quantity                -- 原预测件量
    ,t1.predict_value  as flow_predict_quantity              -- 航空流向预测值
    ,case when t1.predict_period is not null 
          then  t1.predict_period
          else concat(datediff(t1.predicted_datetime ,to_date('$[time(yyyy-MM-dd)]'))+1,'D')
    end as predict_period   
    ,t1.predicted_datetime  	      
    ,'pickup' as task_type	        --	partition key	区分收派类型
    ,'4' as object_type_code	    --	partition key	1航空总量、2航空流向省到省、3航空城市流向、4航空流向营运维度
    ,'$[time(yyyyMMdd)]' as inc_day	        --	partition key	更新日期
    ,t3.province as src_province
    ,t4.province as dest_province  
from   tmp_dm_predict.tmp_shenhy_hk_dims_fixed_tmp_0_2  t1
left join 
     tmp_dm_predict.tmp_shenhy_hk_dims_fixed_tmp2 t2
on t2.predicted_datetime=t1.predicted_datetime
and concat(t2.Origin_code,'-',t2.dest_code)=t1.object_code     
left join 
    dm_ops.dim_city_area_distribution t3    -- 来自规调 获取省份
on split(t1.object_code,'-')[0]=t3.city_code 
left join 
    dm_ops.dim_city_area_distribution t4
on split(t1.object_code,'-')[1]=t4.city_code
where t2.origin_code is null;



-- 合并两部分
drop table if exists tmp_dm_predict.tmp_shenhy_hk_dims_fixed_tmp3_1;
create table tmp_dm_predict.tmp_shenhy_hk_dims_fixed_tmp3_1 
stored as parquet as  
select 
data_version
	,feature_version
	,model_version
	,flag_predict
	,flag_outlier
	,object_code
	,object_name
	,object_type
	,weight_level
	,product_type
	,all_detail
	,operation_type
	,round(predict_value,0) as predict_value
	,predict_period
	,predicted_datetime
	,task_type
	,object_type_code
	,inc_day
from tmp_dm_predict.tmp_shenhy_hk_dims_fixed_tmp3
union all
select
	data_version
	,feature_version
	,model_version
	,flag_predict
	,flag_outlier
	,object_code
	,object_name
	,object_type
	,weight_level
	,product_type
	,all_detail
	,operation_type
	,round(predict_value,0) as predict_value
	,predict_period
	,predicted_datetime
	,task_type
	,object_type_code
	,inc_day
from tmp_dm_predict.tmp_shenhy_hk_dims_fixed_tmp3_0;



set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;  -- 非严格模式
set hive.exec.max.dynamic.partitions=2000;  -- 所有执行节点上最多可以多少个动态分区
set hive.exec.max.dynamic.partitions.pernode = 500;


insert overwrite table dm_predict.hk_cityflow_qty_predict_day_short_period partition(task_type,object_type_code,inc_day)
select
	data_version
	,feature_version
	,model_version
	,flag_predict
	,flag_outlier
	,from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss')  as record_time
	,object_code
	,object_name
	,object_type
	,weight_level
	,product_type
	,all_detail
	,operation_type
	,round(predict_value,0)
	,predict_period
	,predicted_datetime
	,task_type
	,object_type_code
	,inc_day
from tmp_dm_predict.tmp_shenhy_hk_dims_fixed_tmp3_1;