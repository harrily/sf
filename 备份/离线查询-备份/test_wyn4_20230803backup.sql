-------------------------------------------------  oms  process 按天刷新，未限制小时，导致15-24 ，重复 -----------------------------

-- 验证备份表 -- 20221001数据
-- 1.1 2023-06-12 ~ 2023-07-12  oms 因process限制在每天，导致15:00~24:00数据重复
select inc_day,inc_dayhour,sum(all_order_num) as all_order_num, sum(air_order_num) as air_order_num 
from dm_ordi_predict.dws_cityflow_dynamic_order_hi_backup20230801
 where inc_day  between '20230610' and '20230610'
 group by inc_day,inc_dayhour

select inc_day,inc_dayhour,sum(all_order_num) as all_order_num, sum(air_order_num) as air_order_num 
from dm_ordi_predict.dws_cityflow_dynamic_order_hi
 where inc_day between '20230605' and '20230715'
 group by inc_day ,inc_dayhour

select inc_day,inc_dayhour,sum(all_order_num) as all_order_num, sum(air_order_num) as air_order_num 
from dm_ordi_predict.dws_cityflow_dynamic_order_hi_backup20230724
 where inc_day between '20230702' and '20230702'
 group by inc_day ,inc_dayhour
-- 20221011   359638357
-- 20221013  3.51313845E8
select inc_day,inc_dayhour,sum(all_order_num) as all_order_num, sum(air_order_num) as air_order_num 
-- from dm_ordi_predict.dws_cityflow_dynamic_order_hi_backup20230724
from dm_ordi_predict.dws_cityflow_dynamic_order_hi
 where inc_day  between '20230731' and '20230731'
 group by inc_day,inc_dayhour
--  1.1.2   oms 业务区表  15:~ 24没有异常，因为用的是历史数据。
    -- oms & 动态业务 ， 回刷 20230612 ~ 
select inc_day,inc_dayhour,sum(all_order_num) as all_order_num, sum(air_order_num) as air_order_num 
from dm_ordi_predict.dws_cityflow_dynamic_area_order_hi
 where inc_day between '20230605' and '20230715'
 group by inc_day ,inc_dayhour
--、oms  业务区 重刷切换航空件 
        --、刷新   '20230612' and '20230727' 
    -- 对比数据
    select t1.inc_day,t1.inc_dayhour,all_order_num,air_order_num,area_all_order_num,area_air_order_num,
    area_all_order_num-all_order_num as diff_all ,
    area_air_order_num-air_order_num as diff_air
    from 
        (select inc_day,inc_dayhour,sum(all_order_num) as all_order_num, sum(air_order_num) as air_order_num 
        from dm_ordi_predict.dws_cityflow_dynamic_order_hi
        where inc_day between '20230601' and '20230802'
        group by inc_day ,inc_dayhour) t1 
    inner join 
        (select inc_day,inc_dayhour,sum(all_order_num) as area_all_order_num, sum(air_order_num) as area_air_order_num 
        from dm_ordi_predict.dws_cityflow_dynamic_area_order_hi
        where inc_day between '20230601' and '20230802'
        group by inc_day ,inc_dayhour
    ) t2 on t1.inc_day = t2.inc_day and t1.inc_dayhour = t2.inc_dayhour

--、航空动态-业务区 航空件调整
    --income  对比数据  
        --、刷新   '20230612' and '20230727' 
      select t1.inc_day,t1.inc_dayhour,all_quantity_num,air_quantity_num,area_all_quantity_num,area_air_quantity_num,
    area_all_quantity_num-all_quantity_num as diff_all ,
    area_air_quantity_num-air_quantity_num as diff_air
    from 
        (select inc_day,inc_dayhour,sum(all_quantity_num) as all_quantity_num, sum(air_quantity_num) as air_quantity_num 
        from dm_ordi_predict.dws_air_flow_dynamic_income_qty_hi
        where inc_day between '20230601' and '20230802'
        group by inc_day ,inc_dayhour) t1 
    inner join 
        (select inc_day,inc_dayhour,sum(all_quantity_num) as area_all_quantity_num, sum(air_quantity_num) as area_air_quantity_num 
        from dm_ordi_predict.dws_air_flow_dynamic_area_income_qty_hi
        where inc_day between '20230601' and '20230802'
        group by inc_day ,inc_dayhour
    ) t2 on t1.inc_day = t2.inc_day and t1.inc_dayhour = t2.inc_dayhour
   -- pro 对比
    --、刷新   '20230612' and '20230727' 
     select t1.inc_day,t1.inc_dayhour,all_quantity_num,air_quantity_num,area_all_quantity_num,area_air_quantity_num,
    area_all_quantity_num-all_quantity_num as diff_all ,
    area_air_quantity_num-air_quantity_num as diff_air
    from 
        (select inc_day,inc_dayhour,sum(all_quantity_num) as all_quantity_num, sum(air_quantity_num) as air_quantity_num 
        from dm_ordi_predict.dws_air_flow_dynamic_pro_qty_hi
        where inc_day between '20230601' and '20230802'
        group by inc_day ,inc_dayhour) t1 
    inner join 
        (select inc_day,inc_dayhour,sum(all_quantity_num) as area_all_quantity_num, sum(air_quantity_num) as area_air_quantity_num 
        from dm_ordi_predict.dws_air_flow_dynamic_area_pro_qty_hi
        where inc_day between '20230601' and '20230802'
        group by inc_day ,inc_dayhour
    ) t2 on t1.inc_day = t2.inc_day and t1.inc_dayhour = t2.inc_dayhour

------------------------------------------------ 499690  营运 省份维度&城市预测重量重复 修复-------------------------------
 -- 1 省份维度重复，
    -- 、原因： 2022年某些时间段之前不同的版本，没有合并 
    -- 、异常数据区间： 20221220 ~ 20230105
    -- 解决： 1、底表关联省份字段  2、重新生成预测重量 3、生成城市&省份
select * from dm_predict.dws_fc_hk_six_dims_predict_collecting_di
where partition_key = '20221220'
and object_type_code = '7'
and src_province = '四川'
and dest_province = '黑龙江'

-- 验证 and src_province = '四川' and dest_province = '黑龙江'  ， 上游城市数据是否异常 
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
 from (
    select * from dm_predict.dws_fc_hk_six_dims_predict_collecting_di
    where partition_key = '20221220'
    and object_type_code = '4'
    and src_province = '四川'
    and dest_province = '黑龙江'
    and predict_datetime = '2022-12-20'
    -- and operation_type = '特快'
) group by  
    predict_datetime            
	,weight_level  
    ,operation_type     
	,product_type       
    ,src_province    -- 20221219增加省份字段
    ,dest_province   -- 20221219增加省份字段
; 
-- 2 城市维度，预测重量重复
    -- 原因： 件均重 重复 
    -- 异常数据 ： '20220808','20220809','20220823','20220824','20220825','20220826','20220827','20220905'
    -- 解决： 1、底表关联省份字段  2、重新生成预测重量 3、生成城市&省份
select * from dm_predict.dws_fc_hk_six_dims_predict_collecting_di
where partition_key = '20230423'
and origin_code  = '088'
and dest_code = '478'
-- 查看 type = 4 ,重复数据
select  Origin_code,dest_code,src_province,dest_province,weight_level,operation_type,product_type,predict_quantity,partition_key,count(1)
 from dm_predict.dws_fc_hk_six_dims_predict_collecting_di 
where partition_key >= '20220701' 
and object_type_code  = '4' 
group by  Origin_code,dest_code,src_province,dest_province,weight_level,operation_type,product_type,predict_quantity,partition_key
having count(1) > 1 
-- 查看 type = 7 重复数据
select predict_datetime,weight_level,operation_type,product_type,src_province,dest_province,partition_key,count(1)
 from dm_predict.dws_fc_hk_six_dims_predict_collecting_di 
where partition_key >= '20220701' 
and object_type_code  = '7' 
group by  
    predict_datetime,wei

-- 验证去重后的数据 
select * from 
(select 
    partition_key as partition_key,
    sum(predict_quantity) as predict_quantity_new,
    sum(predict_waybill) as  predict_waybill_new,
    sum(predict_weight) as  predict_weight_new,
    sum(predict_volume) as predict_volume_new
 from dm_predict.dws_fc_hk_six_dims_predict_collecting_di_backup20230803
group by partition_key 
) t1 
inner join 
(
    select 
      partition_key as partition_key,
    sum(predict_quantity) as predict_quantity,
    sum(predict_waybill) as  predict_waybill,
    sum(predict_weight) as  predict_weight,
    sum(predict_volume) as predict_volume
 from dm_predict.dws_fc_hk_six_dims_predict_collecting_di
 where ( partition_key in ('20220808','20220809','20220823','20220824','20220825','20220826','20220827','20220905')  
                or partition_key between '20221220' and '20230105' )
group by partition_key 
)t2 on t1.partition_key = t2.partition_key