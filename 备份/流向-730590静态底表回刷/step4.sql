drop table tmp_ordi_predict.dws_static_cityflow_base_2022_tmp_v1_202206;
create table tmp_ordi_predict.dws_static_cityflow_base_2022_tmp_v1_202206 as 
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
    --,sum(special_econ_weight) as special_econ_weight
    --,sum(special_econ_quantity) as special_econ_quantity
    --,sum(special_econ_waybill_num) as special_econ_waybill_num
	--,sum(special_econ_volume) as special_econ_volume
    ,inc_day
from 
 -- !--  dm_ordi_predict.dws_static_his_cityflow 
  tmp_dm_predict.dws_static_his_cityflow_202206
 -- !-- where 
    -- !-- inc_day between '${yyyyMMdd7}' and '${yyyyMMdd1}'
group by 
    days
    ,type
    ,cityflow
    ,income_code
    ,weight_type_code
    ,is_air
    ,inc_day
;


-- 20220916增加字段 修改脚本  修改人：shenhy 01379968

/*
alter table dm_ordi_predict.dws_static_cityflow_base add columns (src_province string comment '始发省份') cascade;
alter table dm_ordi_predict.dws_static_cityflow_base add columns (src_distribution_name string comment '始发分拨区') cascade;
alter table dm_ordi_predict.dws_static_cityflow_base add columns (dest_province string comment '目的省份') cascade;
alter table dm_ordi_predict.dws_static_cityflow_base add columns (dest_distribution_name string comment '目的分拨区') cascade;
*/

SET hive.exec.max.dynamic.partitions.pernode =1000;  
SET hive.exec.max.dynamic.partitions=1000;
SET hive.exec.dynamic.partition.mode=nonstrict;
-- insert overwrite table dm_ordi_predict.dws_static_cityflow_base partition(inc_day) 

drop table if exists  tmp_ordi_predict.dws_static_cityflow_base_202206tmp;
create table tmp_ordi_predict.dws_static_cityflow_base_202206tmp stored as parquet as 
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
    ,case when t2.province is not null and split(t1.cityflow,'-')[0] rlike '^\\d+$' then t2.province
          when t2.province is null and b.provinct_name is not null then replace(replace(b.provinct_name,'省',''),'自治区','')
          else '海外' end as src_province
    ,t2.distribution_name as src_distribution_name
    ,case when t3.province is not null and split(t1.cityflow,'-')[1] rlike '^\\d+$' then t3.province
          when t3.province is null and c.provinct_name is not null then replace(replace(c.provinct_name,'省',''),'自治区','')
          else '海外' end as dest_province
    ,t3.distribution_name as dest_distribution_name
    ,regexp_replace(days,'-','') as inc_day
from
    tmp_ordi_predict.dws_static_cityflow_base_2022_tmp_v1_202206 t1
    left join 
    (select city_code,province_name,province,distribution_name 
    from dm_ops.dim_city_area_distribution a  
    group by city_code,province_name,province,distribution_name
    ) t2
    on nvl(split(t1.cityflow,'-')[0],'a')=t2.city_code
    left join 
      (select city_code,province_name,province,distribution_name 
      from dm_ops.dim_city_area_distribution a  
      group by city_code,province_name,province,distribution_name
    ) t3
    on nvl(split(t1.cityflow,'-')[1],'a')=t3.city_code
    left join
    (select dept_code,provinct_name from dim.dim_department where delete_flg='0'AND country_code ='CN') b
    on nvl(split(t1.cityflow,'-')[0],'a')=b.dept_code
    left join
    (select dept_code,provinct_name from dim.dim_department where delete_flg='0' AND country_code ='CN') c
    on nvl(split(t1.cityflow,'-')[1],'a')=c.dept_code;


/*
SET hive.exec.max.dynamic.partitions.pernode =1000;  
SET hive.exec.max.dynamic.partitions=1000;
SET hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dm_ordi_predict.dws_static_cityflow_base partition(inc_day) 
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
    --,special_econ_weight
    --,special_econ_quantity
    --,special_econ_waybill_num
	--,special_econ_volume
    ,regexp_replace(days,'-','') as inc_day
from
    tmp_ordi_predict.dws_static_cityflow_base_2022_tmp_v1;*/