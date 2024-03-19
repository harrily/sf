/**
	step1- his修复cityflow
**/
SET hive.exec.max.dynamic.partitions.pernode =1000;  
SET hive.exec.max.dynamic.partitions=1000;
SET hive.exec.dynamic.partition.mode=nonstrict;
	
insert overwrite table dm_ordi_predict.dws_static_his_cityflow_backup20230612 partition(inc_day)  	
SELECT
  days,
  concat(case 
        when split(cityflow,'-')[0]  in ('410','413') then '024'
        when split(cityflow,'-')[0]  = '910' then '029'
        when split(cityflow,'-')[0]  = '731' then '7311'
        when split(cityflow,'-')[0]  = '733' then '7313'
        when split(cityflow,'-')[0]  = '899' then '8981'
        when split(cityflow,'-')[0]  = '732' then '7312'
        when split(cityflow,'-')[0]  = '888' then '088'
    else split(cityflow,'-')[0] end ,'-',case 
        when split(cityflow,'-')[1]  in ('410','413') then '024'
        when split(cityflow,'-')[1]  = '910' then '029'
        when split(cityflow,'-')[1]  = '731' then '7311'
        when split(cityflow,'-')[1]  = '733' then '7313'
        when split(cityflow,'-')[1]  = '899' then '8981'
        when split(cityflow,'-')[1]  = '732' then '7312'
        when split(cityflow,'-')[1]  = '888' then '088'
    else split(cityflow,'-')[1] end) as cityflow ,
  type,
  income_code,
  product_code,
  weight_type_code,
  dist_code,
  area_code_jg,
  hq_code,
  limit_type_code,
  limit_tag,
  is_air_flow,
  is_air,
  is_air_waybill,
  sum(all_waybill_num) as all_waybill_num,
  sum(all_quantity) as all_quantity, 
  sum(weight) as weight, 
  sum(volume) as volume,
  sum(fact_air_waybill_num) as fact_air_waybill_num,
  sum(fact_air_quantity) as fact_air_quantity,
  sum(fact_air_weight) as fact_air_weight,
  sum(fact_air_volume) as fact_air_volume,
  inc_day
FROM
  dm_ordi_predict.dws_static_his_cityflow
 where inc_day between '20200101' and '20201231'	 -- 手动指定写入日期区间
 group by days,
	concat(case 
        when split(cityflow,'-')[0]  in ('410','413') then '024'
        when split(cityflow,'-')[0]  = '910' then '029'
        when split(cityflow,'-')[0]  = '731' then '7311'
        when split(cityflow,'-')[0]  = '733' then '7313'
        when split(cityflow,'-')[0]  = '899' then '8981'
        when split(cityflow,'-')[0]  = '732' then '7312'
        when split(cityflow,'-')[0]  = '888' then '088'
    else split(cityflow,'-')[0] end ,'-',case 
        when split(cityflow,'-')[1]  in ('410','413') then '024'
        when split(cityflow,'-')[1]  = '910' then '029'
        when split(cityflow,'-')[1]  = '731' then '7311'
        when split(cityflow,'-')[1]  = '733' then '7313'
        when split(cityflow,'-')[1]  = '899' then '8981'
        when split(cityflow,'-')[1]  = '732' then '7312'
        when split(cityflow,'-')[1]  = '888' then '088'
    else split(cityflow,'-')[1] end),
	type,
	income_code,
	product_code,
	weight_type_code,
	dist_code,
	area_code_jg,
	hq_code,
	limit_type_code,
	limit_tag,
	is_air_flow,
	is_air,
	is_air_waybill,
	inc_day 
;

/**
	step2 
**/
drop table tmp_ordi_predict.dws_static_cityflow_base_2022_tmp_v1_20230612;
create table tmp_ordi_predict.dws_static_cityflow_base_2022_tmp_v1_20230612 as 
select 
    days  --日期
    ,cityflow --网点代码
    ,is_air
	,income_code
	,weight_type_code
	,sum(all_waybill_num) as all_waybill_num
    ,sum(all_quantity) as all_quantity
    ,sum(if(is_air='1',all_waybill_num,0)) as all_waybill_num_air
    ,sum(if(is_air='1',all_quantity,0)) as all_quantity_air
	,sum(weight) as weight 
    ,sum(volume) as volume
    ,sum(fact_air_waybill_num) as fact_air_waybill_num
	,sum(fact_air_quantity) as fact_air_quantity
	,sum(fact_air_weight) as fact_air_weight
	,sum(fact_air_volume) as fact_air_volume
    ,inc_day
from 
    dm_ordi_predict.dws_static_his_cityflow_backup20230612
where  
    inc_day between '20200101' and '20201231'   -- 手动指定写入日期区间
group by 
    days
    ,type
    ,cityflow
    ,income_code
    ,weight_type_code
    ,is_air
    ,inc_day
;

/*
-- step3 -、归集省份对，分拨区（使用mapping表） ，写入base表
*/
SET hive.exec.max.dynamic.partitions.pernode =1000;  
SET hive.exec.max.dynamic.partitions=1000;
SET hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dm_ordi_predict.dws_static_cityflow_base_backup20230612 partition(inc_day) 
select
    days,
    --type,
    cityflow,
    is_air,
    income_code,
    weight_type_code,
    all_waybill_num,
    all_quantity,
    all_waybill_num_air,
    all_quantity_air,
    weight,
    volume
    ,fact_air_waybill_num
	,fact_air_quantity
	,fact_air_weight
	,fact_air_volume
	,if(t2.province is not null,t2.province,'海外') as src_province
    ,t2.distribution_name as src_distribution_name
	,if(t3.province is not null,t3.province,'海外') as dest_province
    ,t3.distribution_name as dest_distribution_name
    ,regexp_replace(days,'-','') as inc_day
from
    tmp_ordi_predict.dws_static_cityflow_base_2022_tmp_v1_20230612 t1
	left join
	(select
		city_code   --城市代码
		,city_name  --城市名称
		,province   --省份名称
		,distribution_name  -- 分拨区名称
	from dm_ordi_predict.dim_city_level_mapping_df 
	where inc_day='$[time(yyyyMMdd,-1d)]'
	and  if_foreign='0'  -- 筛选国内
	) t2
	on nvl(split(t1.cityflow,'-')[0],'a')=t2.city_code
	left join
	(select
		city_code   --城市代码
		,city_name  --城市名称
		,province   --省份名称
		,distribution_name  -- 分拨区名称
	from dm_ordi_predict.dim_city_level_mapping_df 
	where inc_day='$[time(yyyyMMdd,-1d)]'
	and  if_foreign='0'  -- 筛选国内
	) t3
	on nvl(split(t1.cityflow,'-')[1],'a')=t3.city_code;


