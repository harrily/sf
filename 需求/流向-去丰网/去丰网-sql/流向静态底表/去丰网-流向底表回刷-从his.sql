----------------------开发说明----------------------------
--* 名称:流向静态预测底盘 - 去丰网-历史数据回刷
--* 任务ID:
--* 说明:流向静态预测底盘 - 去丰网-历史数据回刷
--* 作者: 01431437
--* 时间: 2023/05/23
-- 备注：输入别名表，rename替换原表
----------------------修改记录----------------------------
--* 修改人   修改时间      修改内容
--*
----------------------------------------------------------

SET hive.exec.max.dynamic.partitions.pernode =1000;  
SET hive.exec.max.dynamic.partitions=1000;
SET hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dm_ordi_predict.dws_static_his_cityflow_backup20230524 partition(inc_day)  
SELECT
  days,
  cityflow,
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
  all_waybill_num,
  all_quantity,
  weight,
  volume,
  fact_air_waybill_num,
  fact_air_quantity,
  fact_air_weight,
  fact_air_volume,
  inc_day
FROM
  dm_ordi_predict.dws_static_his_cityflow
  where inc_day between '20200101' and '20201231'   --  指定日期回写
  and !(nvl(income_code,'')='丰网' or nvl(hq_code,'')='CN39') ;



drop table tmp_ordi_predict.dws_static_cityflow_base_remove_fw_tmp_removefw;
create table tmp_ordi_predict.dws_static_cityflow_base_remove_fw_tmp_removefw as 
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
    dm_ordi_predict.dws_static_his_cityflow_backup20230524 
where 
    inc_day between '20200101' and '20201231'  -- 指定日期回写
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
insert overwrite table dm_ordi_predict.dws_static_cityflow_base_backup20230524 partition(inc_day) 
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
    tmp_ordi_predict.dws_static_cityflow_base_remove_fw_tmp_removefw t1
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


/**
-- dm_ordi_predict.dws_static_cityflow_base_backup2018_2019

-- dm_ordi_predict.dws_static_his_cityflow_backup2018_2019

  CREATE TABLE `dm_ordi_predict.dws_static_his_cityflow_backup20230524`(
  `days` string COMMENT '日期',
  `cityflow` string COMMENT '城市对',
  `type` string COMMENT '收派类型',
  `income_code` string COMMENT '产品板块',
  `product_code` string COMMENT '产品板块_重塑后',
  `weight_type_code` string COMMENT '重量段',
  `dist_code` string COMMENT '城市代码',
  `area_code_jg` string COMMENT '业务区代码_经管',
  `hq_code` string COMMENT '大区代码',
  `limit_type_code` string COMMENT '时效类型',
  `limit_tag` string COMMENT 'SOP标签',
  `is_air_flow` string COMMENT '满足航空流向1是0否',
  `is_air` string COMMENT '满足航空流向且满足航空产品1是0否',
  `is_air_waybill` string COMMENT '满足航空产品1是0否',
  `all_waybill_num` bigint COMMENT '票数',
  `all_quantity` bigint COMMENT '件数',
  `weight` double COMMENT '重量',
  `volume` double COMMENT '体积',
  `fact_air_waybill_num` bigint COMMENT '真实航空票数',
  `fact_air_quantity` bigint COMMENT '真实航空件数',
  `fact_air_weight` double COMMENT '真实航空重量',
  `fact_air_volume` double COMMENT '真实航空体积'
) PARTITIONED BY (`inc_day` string) ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat';


CREATE TABLE `dm_ordi_predict.dws_static_cityflow_base_backup20230524`(
  `days` string COMMENT '日期',
  `cityflow` string COMMENT '城市对',
  `is_air` string COMMENT '是否航空件1航空0非航空',
  `income_code` string COMMENT '产品板块',
  `weight_type_code` string COMMENT '重量段',
  `all_waybill_num` bigint COMMENT '票数',
  `all_quantity` bigint COMMENT '件数',
  `all_waybill_num_air` bigint COMMENT '航空票数',
  `all_quantity_air` bigint COMMENT '航空件数',
  `weight` double COMMENT '重量',
  `volume` double COMMENT '体积',
  `fact_air_waybill_num` bigint COMMENT '真实航空票数',
  `fact_air_quantity` bigint COMMENT '真实航空件数',
  `fact_air_weight` double COMMENT '真实航空重量',
  `fact_air_volume` double COMMENT '真实航空体积',
  `src_province` string COMMENT '始发省份',
  `src_distribution_name` string COMMENT '始发分拨区',
  `dest_province` string COMMENT '目的省份',
  `dest_distribution_name` string COMMENT '目的分拨区'
) PARTITIONED BY (`inc_day` string) ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat';

***/
