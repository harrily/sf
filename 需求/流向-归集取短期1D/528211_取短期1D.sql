
-- 航空短期模型写入

/***

-- 航空动态
-- 当天动态航空流向
drop table if exists tmp_dm_predict.tmp_dm_fc_city_flow_predict_collecting_di_1_1;
create table if not exists tmp_dm_predict.tmp_dm_fc_city_flow_predict_collecting_di_1_1 
stored as parquet as
select
     'T+1D' as data_version     
    ,'feature_v0.2' as feature_version   
    ,'lgbm_v0.2' as model_version     -- gaofeng  
    ,concat(substr(ds,1,4),'-',substr(ds,5,2),'-',substr(ds,7,2)) as predict_datetime
	,split(cityflow,'-')[0] as Origin_code        
	,split(cityflow,'-')[1] as Dest_code          
	,t2.city_name as Origin_name        
	,t3.city_name as Dest_name   
    ,'3' as object_type_code             -- 为了和原短期保持一致，便于后续统一调整
	,'航空城市流向' as object_type_name   -- 为了和原短期保持一致，便于后续统一调整 
    ,'all' as weight_level  
	,'' as product_type       
	,predict_quantity   -- 配平后件量
	,'' as predict_waybill 
	,'' as predict_volume   
    ,t4.province  as src_province 
    ,t5.province as dest_province
    ,inc_dayhour
	,from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') as record_time   
from (select * from dm_predict.dws_airflow_hour_pred_qty_hi t
where inc_day = '$[time(yyyyMMdd)]'   -- '$[time(yyyyMMdd,+0d)]' 20220527改为75D
		and  exists (select inc_dayhour 
                        from  (select max(inc_dayhour) as  inc_dayhour
                                    from dm_predict.dws_airflow_hour_pred_qty_hi
                                where inc_day = '$[time(yyyyMMdd)]' 
                                and inc_dayhour>=concat('$[time(yyyyMMdd)]','07')   -- 模型从7点开始才有数据
                             ) t1 where t.inc_dayhour=t1.inc_dayhour
                    ))t1   -- 取最大的小时结果
left join 
(select dist_code,city_name from dim.dim_department group by dist_code,city_name) t2
on split(cityflow,'-')[0]= t2.dist_code
left join 
(select dist_code,city_name from dim.dim_department group by dist_code,city_name) t3
on  split(cityflow,'-')[1]=t3.dist_code
left join 
dm_ops.dim_city_area_distribution t4   -- 来自规调 获取省份
on  split(cityflow,'-')[0]= t4.city_code
left join 
dm_ops.dim_city_area_distribution t5
on  split(cityflow,'-')[1]= t5.city_code;

-- 当天动态航空流向省到省
drop table if exists tmp_dm_predict.tmp_dm_fc_city_flow_predict_collecting_di_1_2;
create table if not exists tmp_dm_predict.tmp_dm_fc_city_flow_predict_collecting_di_1_2 
stored as parquet as
select
     data_version     
    ,feature_version   
    ,model_version      
    ,predict_datetime
	,src_province as Origin_code        
	,dest_province as Dest_code          
	,src_province as Origin_name        
	,dest_province as Dest_name   
    ,'2' as object_type_code               -- 为了和原短期保持一致，便于后续统一调整
	,'航空流向省到省' as object_type_name    -- 为了和原短期保持一致，便于后续统一调整
    ,'all' as weight_level  
	,'' as product_type       
	,sum(predict_quantity) as predict_quantity    
	,'' as predict_waybill 
    ,'' as predict_volume
from tmp_dm_predict.tmp_dm_fc_city_flow_predict_collecting_di_1_1 t1
group by 
    data_version     
    ,feature_version   
    ,model_version     
    ,predict_datetime
	,src_province         
	,dest_province ;

-- 当天动态航空总量结果
drop table if exists tmp_dm_predict.tmp_dm_fc_city_flow_predict_collecting_di_1_3;
create table if not exists tmp_dm_predict.tmp_dm_fc_city_flow_predict_collecting_di_1_3 
stored as parquet as
select
     data_version     
    ,feature_version   
    ,model_version      
    ,predict_datetime
	,'全部' as Origin_code        
	,'全部' as Dest_code          
	,'全部' as Origin_name        
	,'全部' as Dest_name   
    ,'1' as object_type_code             -- 为了和原短期保持一致，便于后续统一调整
	,'航空总量' as object_type_name      -- 为了和原短期保持一致，便于后续统一调整
    ,'all' as weight_level  
	,'' as product_type       
	,sum(predict_quantity) as predict_quantity    
	,'' as predict_waybill 
    ,'' as predict_volume
from tmp_dm_predict.tmp_dm_fc_city_flow_predict_collecting_di_1_2 t1
group by 
    data_version     
    ,feature_version   
    ,model_version     
    ,predict_datetime;

-- 整合当天航空流向预测（含航空流向 航空流向省到省 航空总量）
drop table if exists tmp_dm_predict.tmp_dm_fc_city_flow_predict_collecting_di_1_4;
create table if not exists tmp_dm_predict.tmp_dm_fc_city_flow_predict_collecting_di_1_4 
stored as parquet as
select
     data_version     
    ,feature_version   
    ,model_version  
    ,predict_datetime
	,Origin_code        
	,Dest_code          
	,Origin_name        
	,Dest_name   
    ,object_type_code   
	,object_type_name   
    ,weight_level  
	,product_type       
	,predict_quantity    
	,predict_waybill 
	,predict_volume   
from   tmp_dm_predict.tmp_dm_fc_city_flow_predict_collecting_di_1_1 t2   -- 航空流向
union all
select
     data_version     
    ,feature_version   
    ,model_version  
    ,predict_datetime
	,Origin_code        
	,Dest_code          
	,Origin_name        
	,Dest_name   
    ,object_type_code   
	,object_type_name   
    ,weight_level  
	,product_type       
	,predict_quantity   
	,predict_waybill 
	,predict_volume   
from   tmp_dm_predict.tmp_dm_fc_city_flow_predict_collecting_di_1_2 t3  -- 航空流向省到省
union all
select
     data_version     
    ,feature_version   
    ,model_version  
    ,predict_datetime
	,Origin_code        
	,Dest_code          
	,Origin_name        
	,Dest_name   
    ,object_type_code   
	,object_type_name   
    ,weight_level  
	,product_type       
	,predict_quantity    
	,predict_waybill 
	,predict_volume   
from   tmp_dm_predict.tmp_dm_fc_city_flow_predict_collecting_di_1_3 t4;   -- 航空总量
**/

-- 1D改为取短期模型1D [499690营运归集复用此临时表] -- 20230608修改
drop table if exists tmp_dm_predict.tmp_dm_fc_city_flow_predict_collecting_di_1_1;
create table if not exists tmp_dm_predict.tmp_dm_fc_city_flow_predict_collecting_di_1_1 
stored as parquet as
select 
    data_version
	,feature_version
	,model_version
	,predicted_datetime as predict_datetime
	,case when object_type='航空总量' 
          then '全部'
          else split(object_code,'-')[0]
    end as Origin_code
	,case when object_type='航空总量' 
          then '全部'
          else split(object_code,'-')[1]
    end as dest_code
	,case when object_type='航空总量' 
          then '全部'
          else split(object_name,'-')[0]
    end as  Origin_name
	,case when object_type='航空总量' 
          then '全部'
          else split(object_name,'-')[1]
    end as dest_name
	,object_type_code
	,object_type as object_type_name
	,'all' as weight_level
	,'' as product_type  
	,predict_value as  predict_quantity
	,'' as  predict_waybill
	,'' as  predict_weight
	,'' as  predict_volume
    from dm_predict.hk_cityflow_qty_predict_day_short_period t1
    where inc_day  =  '$[time(yyyyMMdd)]'
    and predicted_datetime ='$[time(yyyy-MM-dd,0d)]'    -- 取1D当天短期数据
    and object_type in ('航空总量','航空流向省到省','航空城市流向');

-- 取短期模型航空结果，从T+1到T+74
drop table if exists tmp_dm_predict.tmp_dm_fc_city_flow_predict_collecting_di_1_5;
create table if not exists tmp_dm_predict.tmp_dm_fc_city_flow_predict_collecting_di_1_5 
stored as parquet as
select 
    data_version
	,feature_version
	,model_version
	,predicted_datetime as predict_datetime
	,case when object_type='航空总量' 
          then '全部'
          else split(object_code,'-')[0]
    end as Origin_code
	,case when object_type='航空总量' 
          then '全部'
          else split(object_code,'-')[1]
    end as dest_code
	,case when object_type='航空总量' 
          then '全部'
          else split(object_name,'-')[0]
    end as  Origin_name
	,case when object_type='航空总量' 
          then '全部'
          else split(object_name,'-')[1]
    end as dest_name
	,object_type_code
	,object_type as object_type_name
	,'all' as weight_level
	,'' as product_type  
	,predict_value as  predict_quantity
	,'' as  predict_waybill
	,'' as  predict_weight
	,'' as  predict_volume
    from dm_predict.hk_cityflow_qty_predict_day_short_period t1
    where inc_day  =  '$[time(yyyyMMdd)]'
    and predicted_datetime>='$[time(yyyy-MM-dd,1d)]'    -- 取T+2D及以后的预测结果
    and object_type in ('航空总量','航空流向省到省','航空城市流向');

-- 整合当天及短期模型结果
drop table if exists tmp_dm_predict.tmp_dm_fc_hk_flow_predict_collecting_di_2;
create table if not exists tmp_dm_predict.tmp_dm_fc_hk_flow_predict_collecting_di_2 
stored as parquet as
select 
     data_version
	,feature_version
	,model_version
	,predict_datetime
	,Origin_code
	,dest_code
	,Origin_name
	,dest_name
	,object_type_code
	,object_type_name
	,weight_level
	,product_type  
	,predict_quantity
	,predict_waybill
	--,'' as predict_weight	   
	,predict_weight 	-- 使用短期0D -- 20230608修改
	,predict_volume
-- from tmp_dm_predict.tmp_dm_fc_city_flow_predict_collecting_di_1_4 t1  
from tmp_dm_predict.tmp_dm_fc_city_flow_predict_collecting_di_1_1 t1   -- 使用短期0D -- 20230608修改
union ALL
select 
     data_version
	,feature_version
	,model_version
	,predict_datetime
	,Origin_code
	,dest_code
	,Origin_name
	,dest_name
	,object_type_code
	,object_type_name
	,weight_level
	,product_type  
	,predict_quantity
	,predict_waybill
	,predict_weight
	,predict_volume
from tmp_dm_predict.tmp_dm_fc_city_flow_predict_collecting_di_1_5 ;




/*
drop table if exists tmp_dm_predict.tmp_dm_fc_hk_flow_predict_collecting_di_2;
create table if not exists tmp_dm_predict.tmp_dm_fc_hk_flow_predict_collecting_di_2 
stored as parquet as
select 
    data_version
	,feature_version
	,model_version
	,predicted_datetime as predict_datetime
	,case when object_type='航空总量' 
          then '全部'
          else split(object_code,'-')[0]
    end as Origin_code
	,case when object_type='航空总量' 
          then '全部'
          else split(object_code,'-')[1]
    end as dest_code
	,case when object_type='航空总量' 
          then '全部'
          else split(object_name,'-')[0]
    end as  Origin_name
	,case when object_type='航空总量' 
          then '全部'
          else split(object_name,'-')[1]
    end as dest_name
	,object_type_code
	,object_type as object_type_name
	,'all' as weight_level
	,'' as product_type  -- 待确认 不区分产品时，product_type为空
	,predict_value as  predict_quantity
	,'' as  predict_waybill
	,'' as  predict_weight
	,'' as  predict_volume
	-- ,from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') as record_time  -- 创建时间
    from dm_predict.hk_cityflow_qty_predict_day_short_period t1
    where inc_day  =  '$[time(yyyyMMdd)]'
    and object_type in ('航空总量','航空流向省到省','航空城市流向');*/