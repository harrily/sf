-- 获取省份字段
drop table if exists  tmp_dm_predict.tmp_shenhy_hk_dims_fixed_concat_tmp6;
create table if not exists tmp_dm_predict.tmp_shenhy_hk_dims_fixed_concat_tmp6 
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
	, weight_level  
    ,operation_type     
	,product_type       
	,predict_quantity   
	,predict_waybill    
	,predict_weight     
	,predict_volume 
    ,t2.province as src_province
    ,t3.province as dest_province 
from   tmp_dm_predict.tmp_shenhy_hk_dims_fixed_concat_tmp5 t1
left join 
dm_ops.dim_city_area_distribution t2   -- 来自规调 获取省份
on t1.Origin_code=t2.city_code 
left join 
dm_ops.dim_city_area_distribution t3
on t1.dest_code=t3.city_code;

--- 获取预测重量
drop table if exists  tmp_dm_predict.tmp_shenhy_hk_dims_fixed_concat_tmp7;
create table if not exists tmp_dm_predict.tmp_shenhy_hk_dims_fixed_concat_tmp7 
stored as parquet as
select
     t1.data_version     
    ,t1.feature_version   
    ,t1.model_version     
    ,t1.predict_datetime
	,t1.Origin_code        
	,t1.Dest_code          
	,t1.Origin_name        
	,t1.Dest_name          
	,t1.object_type_code   
	,t1.object_type_name        
	,t1.weight_level  
    ,t1.operation_type     
	,t1.product_type       
	,predict_quantity   -- 配平后件量
   -- ,ori_predict_quantity              -- 原预测件量
   --  ,predict_weight_Vhc              -- 件均重计算重量
		-- ,case  when t2.avg_qty_weight is not null and t2.avg_qty_weight>0
		--     then t2.avg_qty_weight*t1.predict_quantity
		--     when t2.avg_qty_weight is null or t2.avg_qty_weight<0
		--    then t3.avg_qty_weight*t1.predict_quantity
		--end as predict_weight
	,case  
		when t2.avg_qty_weight is not null and t2.avg_qty_weight>0 then t2.avg_qty_weight*t1.predict_quantity
		when t3.avg_qty_weight is not null and t3.avg_qty_weight>0 then t3.avg_qty_weight*t1.predict_quantity
		when t4.avg_qty_weight is not null and t4.avg_qty_weight>0 then t4.avg_qty_weight*t1.predict_quantity
		when t5.avg_qty_weight is not null and t5.avg_qty_weight>0 then t5.avg_qty_weight*t1.predict_quantity
	else t6.avg_qty_weight*t1.predict_quantity end as predict_Weight
    ,t2.avg_qty_weight
   --  ,t1.ori_predict_weight   -- 原预测重量
   --  ,t1.flow_predict_quantity  -- 航空流向预测值
    ,t1.predict_waybill 
	,t1.predict_volume
    ,src_province
    ,dest_province     -- 20221219增加省份字段
from tmp_dm_predict.tmp_shenhy_hk_dims_fixed_concat_tmp6  t1
left join 
tmp_ordi_predict.tmp_shenhy_air_five_weidu_actualW_1 t2    ---来自规调的件均重 近30天件均重
on  t1.Origin_code=t2.src_city_code
-- and t1.src_province=t2.send_province
and t1.dest_province=t2.arrive_province
and t1.operation_type=t2.operation_type
left join 
(select 
operation_type,avg(avg_qty_weight) as avg_qty_weight
from tmp_ordi_predict.tmp_shenhy_air_five_weidu_actualW_1
where avg_qty_weight>0
group by operation_type) t3
on t1.operation_type=t3.operation_type
-- 兜底匹配预测重量  20230614修改
left join
( select
    send_province,
    arrive_province,
    avg(avg_qty_weight) as avg_qty_weight
from
    tmp_ordi_predict.tmp_shenhy_air_five_weidu_actualW_1
group by
    send_province,
    arrive_province
) t4 
on nvl(t1.src_province,'null') = t4.send_province
and nvl(t1.dest_province,'null') = t4.arrive_province
left join (
select
    send_province,
    avg(avg_qty_weight) as avg_qty_weight
from
    tmp_ordi_predict.tmp_shenhy_air_five_weidu_actualW_1
group by send_province
) t5 
on nvl(t1.src_province,'null') = t5.send_province
left join (
select
    avg(avg_qty_weight) as avg_qty_weight
from
    tmp_ordi_predict.tmp_shenhy_air_five_weidu_actualW_1
) t6 on 1=1 ;

-- 获取分拨区等信息
drop table if exists  tmp_dm_predict.tmp_shenhy_hk_dims_fixed_concat_tmp8;
create table if not exists tmp_dm_predict.tmp_shenhy_hk_dims_fixed_concat_tmp8 
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
    ,operation_type     
	,product_type       
	,predict_quantity   
	,predict_waybill    
	,predict_weight     
	,predict_volume    
    ,from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') record_time   
    ,t2.area_code as src_area_code
    ,t2.fbq_code as src_fbq_code
    ,t2.hq_code as src_hq_code
    ,t3.area_code as dest_area_code 
    ,t3.fbq_code as dest_fbq_code
    ,t3.hq_code as dest_hq_code
    ,t1.src_province   -- 20221219增加省份字段
    ,t1.dest_province
 from tmp_dm_predict.tmp_shenhy_hk_dims_fixed_concat_tmp7 t1
 left join  
 tmp_dm_predict.tmp_dm_fc_city_2_hq_df_1 t2
on t1.Origin_code=t2.city_code
left join 
 tmp_dm_predict.tmp_dm_fc_city_2_hq_df_1 t3
on t1.dest_code=t3.city_code;



drop table if exists  tmp_dm_predict.tmp_shenhy_hk_dims_fixed_concat_tmp9;
create table if not exists tmp_dm_predict.tmp_shenhy_hk_dims_fixed_concat_tmp9 
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
    ,operation_type     
	,product_type       
	,round(predict_quantity,0) as predict_quantity   
	,predict_waybill    
	,round(predict_weight,0) as  predict_weight  
	,predict_volume     
	,from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss')  as record_time    
    ,src_area_code
    ,src_fbq_code
    ,src_hq_code
    ,dest_area_code 
    ,dest_fbq_code
    ,dest_hq_code
    ,src_province    -- 要在tmp7表中增加好  任务ID-528771
    ,dest_province   -- 要在tmp7表中增加好  任务ID-528771
	,replace(predict_datetime,'-','') as partition_key 
 from tmp_dm_predict.tmp_shenhy_hk_dims_fixed_concat_tmp8 t1
 union ALL
 select 
	 max(data_version) as  data_version    
    ,max(feature_version) as  feature_version  
    ,max(upper(model_version)) as  model_version    
    ,predict_datetime
	,src_province as Origin_code        
	,dest_province as Dest_code          
	,src_province as Origin_name        
	,dest_province as Dest_name          
	,'7' as object_type_code   
	,'航空流向营运维度省份汇总' as object_type_name        
	,weight_level  
    ,operation_type     
	,product_type       
	,sum(round(predict_quantity,0)) as predict_quantity   
	,sum(predict_waybill) as   predict_waybill  
	,sum(round(predict_weight,0)) as  predict_weight  
	,sum(predict_volume) as  predict_volume    
	,from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss')  as record_time    
    ,'' as src_area_code
    ,'' as src_fbq_code
    ,'' as src_hq_code
    ,'' as dest_area_code 
    ,'' as dest_fbq_code
    ,'' as dest_hq_code
    ,src_province    -- 20221219增加省份字段
    ,dest_province   -- 20221219增加省份字段
	,replace(predict_datetime,'-','') as partition_key 
 from tmp_dm_predict.tmp_shenhy_hk_dims_fixed_concat_tmp8 t1
 group by     
    predict_datetime            
	,weight_level  
    ,operation_type     
	,product_type       
    ,src_province    -- 20221219增加省份字段
    ,dest_province   -- 20221219增加省份字段
; 
