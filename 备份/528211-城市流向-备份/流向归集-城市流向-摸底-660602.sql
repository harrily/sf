
-- 城市流向归集调整--摸底来源调整
-- 20230202添加
drop table if exists tmp_dm_predict.tmp_dm_fc_flow_predict_citymodi_1;
create table if not exists tmp_dm_predict.tmp_dm_fc_flow_predict_citymodi_1 
stored as parquet as
select
	data_version
	,feature_version
	,model_version
	,predicted_datetime as predict_datetime
	,split(object_code,'-')[0] as Origin_code
	,split(object_code,'-')[1] as Dest_code
	,split(object_name,'-')[0] as Origin_name
	,split(object_name,'-')[1] as Dest_name
	,'1' as object_type_code
	,'城市流向' as object_type_name
	,case when weight_level='up_15kg' then '大于等于15kg'
		  when weight_level='below_15kg' or weight_level='bellow_15kg' then '小于15kg'  -- 20220823修改
    else weight_level
	end as weight_level
	,product_type
	,'' as operation_type
	,'' as predict_quantity
	,predict_value as predict_waybill
	,'' as predict_weight
	,'' as predict_volume 
	,from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss')  as record_time
-- from  tmp_dm_predict.tmp_dm_fc_city_flow_predict_collecting_di_1   -- 城市流向
from dm_predict.cityflow_qty_predict_gaofeng t1
    where ((inc_day between  '$[time(yyyyMMdd,3d)]' and  '$[time(yyyyMMdd,74d)]'
    ) or inc_day in ('20230429','20230430'))
    and object_type ='城市流向'
    and all_detail='0'; --雷超 20230427 20230429和20230430固定取摸底

