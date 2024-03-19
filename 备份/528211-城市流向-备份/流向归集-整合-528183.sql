
drop table if exists tmp_dm_predict.tmp_dm_fc_flow_predict_collecting_di_1;
create table if not exists tmp_dm_predict.tmp_dm_fc_flow_predict_collecting_di_1 
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
	,'' as operation_type
	,predict_quantity
	,predict_waybill
	,predict_weight
	,predict_volume 
	,from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss')  as record_time
from tmp_dm_predict.tmp_dm_fc_city_flow_predict_collecting_di_2  -- 城市流向 20230202修改
-- from  tmp_dm_predict.tmp_dm_fc_city_flow_predict_collecting_di_1   -- 城市流向
union ALL
select 
    data_version
	,feature_version
	,model_version
	,predict_datetime
	,Origin_code
	,Dest_code
	,Origin_name
	,Dest_name
	,case when object_type_name='航空城市流向'
          then '2'
          when object_type_name='航空流向省到省'
          then '6'
          when object_type_name='航空总量'
          then '5'
    end as object_type_code
	,case when object_type_name='航空城市流向'
         then '航空流向'
         else  object_type_name 
    end as object_type_name     -- ('航空总量','航空流向省到省','航空城市流向')
	,weight_level
	,product_type
	,'' as operation_type
	,predict_quantity
	,predict_waybill
	,predict_weight
	,predict_volume 
	,from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') as record_time
from tmp_dm_predict.tmp_dm_fc_hk_flow_predict_collecting_di_5  -- 短期模型+摸底
union ALL
select 
    data_version
	,feature_version
	,model_version
	,predict_datetime
	,Origin_code
	,Dest_code
	,Origin_name
	,Dest_name
	,case when object_type_name='航空城市流向'
          then '2'
          when object_type_name='航空流向省到省'
          then '6'
          when object_type_name='航空总量'
          then '5'
    end as object_type_code
	,case when object_type_name='航空城市流向'
         then '航空流向'
         else  object_type_name 
    end as object_type_name     -- ('航空总量','航空流向省到省','航空城市流向')
	,weight_level
	,product_type
	,'' as operation_type
	,predict_quantity
	,predict_waybill
    ,predict_weight
	,predict_volume 
	,from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') as record_time
from tmp_dm_predict.tmp_dm_fc_hk_flow_predict_collecting_di_7;  -- 长期模型+摸底


drop table if exists tmp_dm_predict.tmp_dm_fc_flow_predict_collecting_di_2;
create table if not exists tmp_dm_predict.tmp_dm_fc_flow_predict_collecting_di_2 
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
	,record_time   
    ,t2.area_code as src_area_code
    ,t2.fbq_code as src_fbq_code
    ,t2.hq_code as src_hq_code
    ,t3.area_code as dest_area_code 
    ,t3.fbq_code as dest_fbq_code
    ,t3.hq_code as dest_hq_code
 from tmp_dm_predict.tmp_dm_fc_flow_predict_collecting_di_1 t1
 left join
 tmp_dm_predict.tmp_dm_fc_city_2_hq_df_1 t2
on t1.Origin_code=t2.city_code
left join 
 tmp_dm_predict.tmp_dm_fc_city_2_hq_df_1 t3
on t1.dest_code=t3.city_code;