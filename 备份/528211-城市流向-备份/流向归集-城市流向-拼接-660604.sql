-- 城市流向摸底和模型结果拼接

drop table if exists tmp_dm_predict.tmp_dm_fc_flow_predict_citymodi_2;
create table if not exists tmp_dm_predict.tmp_dm_fc_flow_predict_citymodi_2
stored as parquet as
select 
    t1.predict_datetime
    ,case when t2.predict_datetime is not NULL
        then '1'
        else '0'
    end as if_modi
from tmp_dm_predict.tmp_dm_fc_city_flow_predict_collecting_di_1 t1  -- 长期模型
left join 
(select predict_datetime from tmp_dm_predict.tmp_dm_fc_flow_predict_citymodi_1   -- 摸底结果
group by predict_datetime) t2
on t1.predict_datetime=t2.predict_datetime
group by t1.predict_datetime
    ,case when t2.predict_datetime is not NULL
        then '1'
        else '0'
    end;

-- 合并摸底和模型结果
drop table if exists tmp_dm_predict.tmp_dm_fc_city_flow_predict_collecting_di_2;
create table if not exists tmp_dm_predict.tmp_dm_fc_city_flow_predict_collecting_di_2
stored as parquet as
select 
    data_version
	,feature_version
	,model_version
	,t1.predict_datetime
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
from(select * from  tmp_dm_predict.tmp_dm_fc_flow_predict_citymodi_2 where if_modi ='0' ) t1   -- 长期日期标记
left join 
tmp_dm_predict.tmp_dm_fc_city_flow_predict_collecting_di_1 t2  -- 长期结果表
on t1.predict_datetime=t2.predict_datetime
union all
select 
     data_version
	,feature_version
	,model_version
	,t1.predict_datetime
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
from(select * from  tmp_dm_predict.tmp_dm_fc_flow_predict_citymodi_2 where if_modi ='1' ) t1  -- 长期日期标记
left join 
tmp_dm_predict.tmp_dm_fc_flow_predict_citymodi_1 t2   -- 摸底
on t1.predict_datetime=t2.predict_datetime;