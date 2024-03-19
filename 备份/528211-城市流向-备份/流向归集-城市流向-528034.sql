
-------------------------------------归集-流向-----------------------------
----------------------开发说明----------------------------
--* 名称: 归集-流向
----------------------修改记录----------------------------
--* 修改人   修改时间      修改内容
--* 申海艳   20220718     添加航空6维度归集，结果表加字段operation_type         
----------------------------------------------------------
--* 修改人   修改时间      修改内容
--* 申海艳   20220721     由于航空6维度预测结果时效晚，避免互相影响，去掉该部分归集         
----------------------------------------------------------
--* 修改人   修改时间      修改内容
--* 申海艳   20220823     增加层级字段，修改重量段描述        
----------------------------------------------------------
--* 修改人   修改时间      修改内容
--* 申海艳   20221024     修改添加航空流向省到省  航空总量       
----------------------------------------------------------
--* 修改人   修改时间      修改内容
--* 申海艳   20230109     修改当天城市流向结果为动态输出的最新结果       
----------------------------------------------------------


---- 取城市流向归集
-- 城市流向动态结果
drop table if exists tmp_dm_predict.tmp_dm_fc_city_flow_predict_collecting_di_0_1;
create table if not exists tmp_dm_predict.tmp_dm_fc_city_flow_predict_collecting_di_0_1 
stored as parquet as
select 
	data_version
	,feature_version
	,model_version
	,predict_datetime
	,Origin_code
	,dest_code
	,t2.city_name as Origin_name
	,t3.city_name as dest_name
	,'1' as object_type_code
	,'城市流向' as object_type_name
	,case when weight_level='up_15kg' then '大于等于15kg'
		  when weight_level='below_15kg' or weight_level='bellow_15kg' then '小于15kg'  -- 20220823修改
    else weight_level
	end as weight_level
	,product_type
	,'' as  predict_quantity
	,predict_value as predict_waybill
	,'' as  predict_weight
	,'' as  predict_volume
    ,hour
    ,case when pmod(datediff(predict_datetime,'1920-01-01')-3,7)  in ('0','6','7') 
        then 'Y' else 'N' end as is_weekend -- 是否周末
from (
select 
    data_version
	,feature_version
	,model_version
            ,predicted_datetime  as predict_datetime           -- 预测日期
            ,split(object_code,'-')[0] as  Origin_code               -- 流向
			,split(object_code,'-')[1] as  dest_code               -- 流向
            ,'' as product_type   
            ,'all' as weight_level               -- 重量段
            ,predict_1 as predict_value
            ,hour
        from dm_ordi_predict.cityflow_dynamic_resultz_online t
        where inc_day = '$[time(yyyy-MM-dd)]'   -- '$[time(yyyyMMdd,+0d)]' 20220527改为75D
		and  exists (select max_hour 
                        from  (select max(cast(hour as int)) as  max_hour 
                                    from dm_ordi_predict.cityflow_dynamic_resultz_online
                                where inc_day = '$[time(yyyy-MM-dd)]' 
                                and  cast(hour as int)>=7   -- 模型从7点开始才有当天预测数据
                             ) t1 where t.hour=t1.max_hour
                    )
       ) t1
      left join 
       (select dist_code,city_name from dim.dim_department group by dist_code,city_name) t2
       on t1.Origin_code=t2.dist_code
      left join 
        (select dist_code,city_name from dim.dim_department group by dist_code,city_name)t3
       on t1.dest_code=t3.dist_code;

-- 生成城市流向含产品、重量段的调整结果表
drop table if exists tmp_dm_predict.tmp_dm_fc_city_flow_predict_collecting_di_0_2;
create table  tmp_dm_predict.tmp_dm_fc_city_flow_predict_collecting_di_0_2 
stored as parquet as
select 
    a.*,
    cn/sum(cn) over (partition by is_weekend,cityflow) as pct
FROM 
(
    select
        case when pmod(datediff(days,'1920-01-01')-3,7)  in ('0','6','7') 
        then 'Y' else 'N' end as is_weekend,
        cityflow,
        income_code,
        case 
            when weight_type_code in ('<3KG','[3KG,10KG)','[10KG,15KG)') then 'bellow_15kg'
            when weight_type_code in ('[15KG,20KG)','>=20KG') then 'up_15kg'
            else 'up_15kg'
        end as weight_type_code,
        sum(cast(all_waybill_num as double)) as cn
    from dm_ordi_predict.dws_static_cityflow_base t1
    WHERE inc_day between '$[time(yyyyMMdd,-29d)]' and '$[time(yyyyMMdd,-1d)]'
        and cityflow regexp '(\\d{3})-(\\d{3})' 
    GROUP BY 
        case when pmod(datediff(days,'1920-01-01')-3,7)  in ('0','6','7') 
        then 'Y' else 'N' end,
        cityflow,
        income_code,
        case 
            when weight_type_code in ('<3KG','[3KG,10KG)','[10KG,15KG)') then 'bellow_15kg'
            when weight_type_code in ('[15KG,20KG)','>=20KG') then 'up_15kg'
            else 'up_15kg'
        end
  ) a;

-- 拆分重量段和产品版块
drop table if exists tmp_dm_predict.tmp_dm_fc_city_flow_predict_collecting_di_0_3_1;
create table tmp_dm_predict.tmp_dm_fc_city_flow_predict_collecting_di_0_3_1
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
	,case when b.weight_type_code='up_15kg' then '大于等于15kg'
		  when b.weight_type_code='below_15kg' or b.weight_type_code='bellow_15kg' then '小于15kg'  -- 20220823修改
    else b.weight_type_code
	end as weight_level
	,b.income_code as product_type
	,'' as  predict_quantity
	,case when b.pct is not NULL
          then b.pct*a.predict_waybill
          else a.predict_waybill
    end as predict_waybill
	,'' as  predict_weight
	,'' as  predict_volume
    ,hour
    ,b.pct
    ,a.is_weekend
from tmp_dm_predict.tmp_dm_fc_city_flow_predict_collecting_di_0_1 a
left join tmp_dm_predict.tmp_dm_fc_city_flow_predict_collecting_di_0_2 b
on  concat(a.Origin_code,'-',a.dest_code)=b.cityflow
and a.is_weekend=b.is_weekend;

-- 历史未匹配到的流向进行兜底处理
drop table if exists tmp_dm_predict.tmp_dm_fc_city_flow_predict_collecting_di_0_3_2;
create table tmp_dm_predict.tmp_dm_fc_city_flow_predict_collecting_di_0_3_2
stored as parquet as
SELECT
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
	,case when t2.weight_type_code='up_15kg' then '大于等于15kg'
		  when t2.weight_type_code='below_15kg' or t2.weight_type_code='bellow_15kg' then '小于15kg'  -- 20220823修改
    else t2.weight_type_code
	end as weight_level
	,t2.income_code as product_type
	,'' as  predict_quantity
	,case when t1.pct is not NULL
          then t1.predict_waybill
          when t1.pct is null 
          then t1.predict_waybill*t2.pct_1
          else t1.predict_waybill
    end as predict_waybill
	,'' as  predict_weight
	,'' as  predict_volume
    ,hour
from (select * from tmp_dm_predict.tmp_dm_fc_city_flow_predict_collecting_di_0_3_1 where pct is null)t1
left join
    (select 
        is_weekend,
        income_code,
        weight_type_code,
        cn_week/sum(cn_week)over(partition by is_weekend) as pct_1
from  (
    select 
        is_weekend,
        income_code,
        weight_type_code,
        sum(cn) as cn_week
    from 
    tmp_dm_predict.tmp_dm_fc_city_flow_predict_collecting_di_0_2 
    group by is_weekend,
        income_code,
        weight_type_code) t)t2
on t1.is_weekend=t2.is_weekend;


-- 城市流向动态结果拆分重量段和产品版块
-- 城市流向预测结果
drop table if exists tmp_dm_predict.tmp_dm_fc_city_flow_predict_collecting_di_0_4;
create table if not exists tmp_dm_predict.tmp_dm_fc_city_flow_predict_collecting_di_0_4 
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
	,'1' as object_type_code
	,'城市流向' as object_type_name
	,case when weight_level='up_15kg' then '大于等于15kg'
		  when weight_level='below_15kg' or weight_level='bellow_15kg' then '小于15kg'  -- 20220823修改
    else weight_level
	end as weight_level
	,product_type
	,'' as  predict_quantity
	,predict_value as predict_waybill
	,'' as  predict_weight
	,'' as  predict_volume
	,from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') as record_time  -- 创建时间
from (
select data_version
	        ,feature_version
	        ,model_version
            ,predicted_datetime  as predict_datetime           -- 预测日期
            ,split(object_code,'-')[0] as  Origin_code               -- 流向
			,split(object_code,'-')[1] as  dest_code               -- 流向
			,split(object_name,'-')[0] as  Origin_name               -- 流向
			,split(object_name,'-')[1] as  dest_name               -- 流向
            ,product_type   -- 20220624修改
            ,weight_level               -- 重量段
            ,predict_value
        from dm_predict.city_cityflow_predict_result
        where predicted_datetime between '$[time(yyyy-MM-dd,1d)]' and  '$[time(yyyy-MM-dd,74d)]'  -- '$[time(yyyyMMdd,+0d)]' 20220527改为75D
        and all_detail = 0
		and weight_level is not null
       ) t1;

-- 整合当天和未来预测结果
drop table if exists tmp_dm_predict.tmp_dm_fc_city_flow_predict_collecting_di_1;
create table if not exists tmp_dm_predict.tmp_dm_fc_city_flow_predict_collecting_di_1 
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
	,predict_weight
	,predict_volume
from tmp_dm_predict.tmp_dm_fc_city_flow_predict_collecting_di_0_3_1 t1   -- 历史可以匹配到的
where pct is not null
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
from tmp_dm_predict.tmp_dm_fc_city_flow_predict_collecting_di_0_3_2 t1   -- 历史无法匹配的
union all
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
from tmp_dm_predict.tmp_dm_fc_city_flow_predict_collecting_di_0_4 t2;

/*
drop table if exists tmp_dm_predict.tmp_dm_fc_city_flow_predict_collecting_di_1;
create table if not exists tmp_dm_predict.tmp_dm_fc_city_flow_predict_collecting_di_1 
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
	,'1' as object_type_code
	,'城市流向' as object_type_name
	,case when weight_level='up_15kg' then '大于等于15kg'
		  when weight_level='below_15kg' or weight_level='bellow_15kg' then '小于15kg'  -- 20220823修改
    else weight_level
	end as weight_level
	,product_type
	,'' as  predict_quantity
	,predict_value as predict_waybill
	,'' as  predict_weight
	,'' as  predict_volume
	,from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') as record_time  -- 创建时间
from (
select      data_version
	,feature_version
	,model_version
            ,predicted_datetime  as predict_datetime           -- 预测日期
            ,split(object_code,'-')[0] as  Origin_code               -- 流向
			,split(object_code,'-')[1] as  dest_code               -- 流向
			,split(object_name,'-')[0] as  Origin_name               -- 流向
			,split(object_name,'-')[1] as  dest_name               -- 流向
            ,product_type   -- 20220624修改
            ,weight_level               -- 重量段
            ,predict_value
        from dm_predict.city_cityflow_predict_result
        where predicted_datetime between '$[time(yyyy-MM-dd)]' and  '$[time(yyyy-MM-dd,74d)]'  -- '$[time(yyyyMMdd,+0d)]' 20220527改为75D
        and all_detail = 0
		and weight_level is not null
       ) t1;
*/