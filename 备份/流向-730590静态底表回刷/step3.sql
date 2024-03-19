----------------------开发说明----------------------------
--* 名称:流向新底盘-step2
--* 任务ID:
--* 说明:流向新底盘-step2
--* 作者: 张长发
--* 时间: 2022/9/02 15:45
----------------------修改记录----------------------------
--* 修改人   修改时间      修改内容
--* 雷超     20230424     新建任务
----------------------------------------------------------
-- 动态分区
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions=1000;
set hive.exec.max.dynamic.partitions.pernode=1000;


----------------------脚本逻辑----------------------------
--输入表： tmp_dm_predict.dws_static_city_flow_2022_tmp_v2
--输出表：  dm_ordi_predict.dws_static_his_cityflow_v2
--根据流向逻辑，汇总流向数据，同时关联产品板块，业务区等维表，添加维度
----------------------------------------------------------

drop table if exists  tmp_dm_predict.dws_static_his_cityflow_2022_tmp_v2_202206;
create table tmp_dm_predict.dws_static_his_cityflow_2022_tmp_v2_202206 stored as parquet as 
select 
    a.days
    ,a.cityflow
    ,a.type 
    ,nvl(a.income_code,'其他') as income_code 
    ,a.product_code 
    ,a.weight_type_code 
    ,a.dist_code 
    ,a.area_code_jg 
    ,a.hq_code
    ,a.limit_type_code
    ,a.limit_tag
    ,a.all_waybill_num 
    ,a.all_quantity 
    ,a.weight 
    ,nvl(a.volume,0) as volume
    ,a.fact_air_waybill_num
	,a.fact_air_quantity
	,a.fact_air_weight
	,a.fact_air_volume
    --,a.special_econ_weight
    --,a.special_econ_quantity
    --,a.special_econ_waybill_num
	--,a.special_econ_volume
    ,case   
        when a.product_code = 'SE000201' and b.cityflow is not null then '1'
		when a.product_code = 'SE0001' and b.cityflow is not null then '1'
		when a.product_code = 'SE0107' and b.cityflow is not null then '1'
        when a.product_code = 'SE0103' and b.cityflow is not null then '1'
        when a.product_code = 'SE0051' and b.cityflow is not null then '1'
		when a.product_code = 'SE0089' and b.cityflow is not null then '1'
		when a.product_code = 'SE0109' and b.cityflow is not null then '1'
		when a.product_code = 'SE0008' and b.cityflow is not null and (a.limit_tag ='T4' or a.limit_tag ='T801') then '1'
		when a.product_code = 'SE0137' and b.cityflow is not null then '1'
		when a.product_code = 'SE0121' and b.cityflow is not null then '1'
		when a.product_code = 'SE0004' and a.limit_tag = 'SP6' and b.cityflow is not null then '1'
        when a.product_code = 'SE0146' and b.cityflow is not null then '1'
        when a.product_code = 'SE0152' and b.cityflow is not null then '1'  --20230403 新增两个航空产品
        when a.product_code = 'SE000206' and b.cityflow is not null then '1'
		else '0' 
    end as is_air
    ,if(b.cityflow is not null,1,0) as is_air_flow
	,case   
        when a.product_code = 'SE000201' then '1'
		when a.product_code = 'SE0001' then '1'
		when a.product_code = 'SE0107' then '1'
        when a.product_code = 'SE0103' then '1'
        when a.product_code = 'SE0051' then '1'
		when a.product_code = 'SE0089' then '1'
		when a.product_code = 'SE0109' then '1'
		when a.product_code = 'SE0008' and (a.limit_tag ='T4' or a.limit_tag ='T801') then '1'
		when a.product_code = 'SE0137' then '1'
		when a.product_code = 'SE0121' then '1'
		when a.product_code = 'SE0004' and a.limit_tag = 'SP6' then '1'
        when a.product_code = 'SE0146' then '1'
        when a.product_code = 'SE0152' then '1' --20230403 新增两个航空产品
        when a.product_code = 'SE000206' then '1'
		else '0' 
     end as is_air_waybill
from 
(   
    select 
        cityflow --流向代码
        ,days  --日期
        ,type  --收派类型
        ,a.product_code --产品代码
        ,weight_type_code  --重量段
        ,dist_code --城市代码
        ,a.hq_code
        ,case 
            when a.hq_code = 'CN06' then 'CN06' 
            when a.hq_code = 'CN07' then 'CN07'
            when a.hq_code = 'CN39' then a.area_code_org 
            when a.hq_code = 'HQOP' then a.area_code_org
            when b.area_code is not null then b.area_code
            when b.area_code is null then a.area_code_org
            else 'other' end as area_code_jg --经管业务区
        ,z.income_code as income_code  --产品名称
        ,a.limit_type_code
        ,a.cargo_type_code	
        ,a.limit_tag
        ,all_waybill_num  --票量
        ,all_quantity  --件量
        ,weight
        ,volume
        ,fact_air_waybill_num
	    ,fact_air_quantity
	    ,fact_air_weight
	    ,fact_air_volume
        --,special_econ_weight
        --,special_econ_quantity
        --,special_econ_waybill_num
	    --,special_econ_volume
    from 
        tmp_dm_predict.dws_static_city_flow_2022_tmp_v202206 a  
    left join 
    (
          select
      prod_code as product_code,
      case
        when income_code = '1' then '时效'
        when income_code = '2' then '电商'
        when income_code = '3' then '国际'
        when income_code = '4' then '大件'
        when income_code = '5' then '医药'
        when income_code = '6' then '其他'
        when income_code = '8' then '冷运'
        when income_code = '9' then '丰网'
      end as income_code
    from dim.dim_prod_base_info_df
    where inc_day = '20230315'
       and (invalid_date >= '2023-03-15' or invalid_date is null)
       and income_code in ('1', '2', '3', '4', '5', '6', '8', '9')
      group by prod_code,
      case
        when income_code = '1' then '时效'
        when income_code = '2' then '电商'
        when income_code = '3' then '国际'
        when income_code = '4' then '大件'
        when income_code = '5' then '医药'
        when income_code = '6' then '其他'
        when income_code = '8' then '冷运'
        when income_code = '9' then '丰网'
      end
    )z
        on a.product_code= z.product_code 
    left join 
    (
        select 
            * 
        from 
            -- dm_fin_itp.itp_vs_rel_city_bg_area_mpp 
            tmp_ordi_predict.itp_vs_rel_city_bg_area_mpp
        where 
            BG_code='SF001' and to_tm='9999-12-31' and area_code not in ('852Y','886Y') and hq_version = 'YJ'
    ) b  
        on a.dist_code = b.city_code     
)a
left join 
(
    select 
        *
    from 
        dm_ordi_predict.cf_air_list
)b
    on a.cityflow = b.cityflow
;
        


SET hive.exec.max.dynamic.partitions.pernode =1000;  
SET hive.exec.max.dynamic.partitions=1000;
SET hive.exec.dynamic.partition.mode=nonstrict;
-- insert overwrite table dm_ordi_predict.dws_static_his_cityflow partition(inc_day)  

drop table if exists  tmp_dm_predict.dws_static_his_cityflow_202206;
create table tmp_dm_predict.dws_static_his_cityflow_202206 stored as parquet as 
select
    a.days
    ,a.cityflow 
    ,a.type 
    ,CASE WHEN product_code = 'SE0088' AND weight_type_code = '>=20KG' THEN '大件'
          WHEN product_code = 'SE0088' AND weight_type_code != '>=20KG' THEN '电商'  ELSE income_code END AS income_code
    ,a.product_code
    ,a.weight_type_code 
    ,a.dist_code 
    ,a.area_code_jg
	,a.hq_code
    ,a.limit_type_code
    ,a.limit_tag
	,a.is_air_flow
    ,a.is_air
    ,a.is_air_waybill
    ,sum(a.all_waybill_num) as  all_waybill_num
    ,sum(a.all_quantity) as all_quantity 
    ,sum(a.weight) as weight 
    ,sum(a.volume) as volume
    ,sum(a.fact_air_waybill_num) as fact_air_waybill_num
	,sum(a.fact_air_quantity) as fact_air_quantity
	,sum(a.fact_air_weight) as fact_air_weight
	,sum(a.fact_air_volume) as fact_air_volume
    --,special_econ_weight
    --,special_econ_quantity
    --,special_econ_waybill_num
    --,special_econ_volume
    ,regexp_replace(days,'-','') as inc_day
from
    tmp_dm_predict.dws_static_his_cityflow_2022_tmp_v2_202206 a
group by   a.days
    ,a.cityflow 
    ,a.type 
    ,CASE WHEN product_code = 'SE0088' AND weight_type_code = '>=20KG' THEN '大件'
          WHEN product_code = 'SE0088' AND weight_type_code != '>=20KG' THEN '电商'  ELSE income_code END
    ,a.product_code
    ,a.weight_type_code 
    ,a.dist_code 
    ,a.area_code_jg
	,a.hq_code
    ,a.limit_type_code
    ,a.limit_tag
	,a.is_air_flow
    ,a.is_air
    ,a.is_air_waybill;