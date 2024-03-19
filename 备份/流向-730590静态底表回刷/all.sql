----------------------开发说明----------------------------
-- 重刷流向底表 202206数据
----------------------修改记录----------------------------
--* 修改人   修改时间      修改内容
----------------------------------------------------------
----------------------hive调优参数列表----------------------
-- 动态分区
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions=1000;
set hive.exec.max.dynamic.partitions.pernode=1000;

----------------------脚本逻辑----------------------------
--输入表： dwd.dwd_pub_waybill_info_mid_di,tmp_dm_predict.dm_fact_air_waybill_no
--输出表： tmp_dm_predict.dws_static_city_flow_2022_tmp_v2 
--从运单表关联真实航空流向表数据，汇总流向数据
----------------------------------------------------------

drop table if exists tmp_dm_predict.dws_static_city_flow_2022_tmp_v202206;  -- 历史数据
create table tmp_dm_predict.dws_static_city_flow_2022_tmp_v202206 stored as parquet as 
--收件  
select 
    concat(nvl(a.src_dist_code,'#'),'-',nvl(a.dest_dist_code,'#')) as cityflow
	,substr(a.consigned_tm,1,10) as days  
	,'pickup' as type  
    ,product_code
	,case 
	       when meterage_weight_qty >= 20  then '>=20KG'
	       when meterage_weight_qty >= 15 and meterage_weight_qty <20 then '[15KG,20KG)'
	       when meterage_weight_qty >= 10 and meterage_weight_qty <15 then '[10KG,15KG)'
	       when meterage_weight_qty >= 3 and meterage_weight_qty < 10 then '[3KG,10KG)'
	    else '<3KG'
	end as weight_type_code 
	,src_dist_code as dist_code 
    ,src_area_code as area_code_org
    ,src_hq_code as hq_code

    ,limit_type_code
    ,cargo_type_code	
    ,limit_tag
    ,sum(volume) as volume

	,count(waybill_no) as all_waybill_num  
    ,sum(quantity) as all_quantity  --件量
	,sum(meterage_weight_qty) weight
	,sum(fact_air_waybill) as fact_air_waybill_num
	--,sum(special_econ_waybill) as special_econ_waybill_num
	,sum(fact_air_quantity) as fact_air_quantity
	--,sum(special_econ_quantity) as special_econ_quantity
	,sum(fact_air_weight) as fact_air_weight
	--,sum(special_econ_weight) as special_econ_weight
	,sum(fact_air_volume) as fact_air_volume
	--,sum(special_econ_volume) as special_econ_volume
from
(	select 
		a.src_dist_code,
		a.dest_dist_code,
		a.consigned_tm,
		a.product_code,
		round(case when upper(a.unit_weight)='LBS' then a.meterage_weight_qty*0.45359237
                   when upper(a.unit_weight)='G' then a.meterage_weight_qty/1000 
                   else a.meterage_weight_qty 
        end,3) as meterage_weight_qty,
		a.src_area_code,
		a.src_hq_code,
		a.limit_type_code,
		a.cargo_type_code,
        a.limit_tag,
		a.volume,
		a.waybill_no,
		a.quantity,
		if(b.waybill_no is not null ,1,0) as fact_air_waybill,
		--if(c.waybill_no is not null ,1,0) as special_econ_waybill,
		if(b.waybill_no is not null ,a.quantity,0) as fact_air_quantity,
		--if(c.waybill_no is not null ,a.quantity,0) as special_econ_quantity,
		if(b.waybill_no is not null ,a.meterage_weight_qty,0) as fact_air_weight,
		--if(c.waybill_no is not null ,a.meterage_weight_qty_kg,0) as special_econ_weight,
		if(b.waybill_no is not null ,a.volume,0) as fact_air_volume
		--if(c.waybill_no is not null ,a.volume,0) as special_econ_volume
	from
	dwd.dwd_waybill_info_dtl_di a
	left join (select waybill_no 
				-- from tmp_dm_predict.dm_fact_air_waybill_no
              from  dm_ordi_predict.dm_fact_air_waybill_no
              where inc_day  between '20230101' and '20230228' --  T-45D T-1D   -- 手动回刷历史数据 
              group by waybill_no
                ) b 
	on a.waybill_no = b.waybill_no
--      left join tmp_dm_predict.dws_fact_special_econ_dtl_tmp c on a.waybill_no = c.waybill_no
	where 
         a.inc_day>='20230101' and inc_day<='20230228' and   -- T-55D T-1D  -- 手动回刷历史数据 
	 substr(a.consigned_tm,0,10) between '2023-01-01' and '2023-02-28' ) a  -- 回刷历史数据 --  T-45D T-1D  -- 手动回刷历史数据 
group by  
	 concat(nvl(a.src_dist_code,'#'),'-',nvl(a.dest_dist_code,'#')) 
	,substr(a.consigned_tm,1,10)
	,product_code
	,case 
	       when meterage_weight_qty >= 20  then '>=20KG'
	       when meterage_weight_qty >= 15 and meterage_weight_qty <20 then '[15KG,20KG)'
	       when meterage_weight_qty >= 10 and meterage_weight_qty <15 then '[10KG,15KG)'
	       when meterage_weight_qty >= 3 and meterage_weight_qty < 10 then '[3KG,10KG)'
	    else '<3KG'
	end
	,src_dist_code
    ,src_area_code
    ,src_hq_code
    ,limit_type_code
    ,cargo_type_code
    ,limit_tag
; 

-----------------------------------------------  step3 ---------------------------------------------------------------





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
            tmp_ordi_predict.itp_vs_rel_city_bg_area_mpp  -- 当时没权限，使用临时表
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
	insert overwrite table dm_ordi_predict.dws_static_his_cityflow partition(inc_day)  
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
	
	
	
	
	-----------------------------------------------  step4444 ---------------------------------------------------------------
	
	
	
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
  dm_ordi_predict.dws_static_his_cityflow 
where 
    inc_day between '20230101' and '20230228'  -- 手动回刷历史数据 
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