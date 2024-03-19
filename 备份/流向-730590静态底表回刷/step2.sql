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
              where inc_day  between '20220601' and '20230714' --  T-45D T-1D
              group by waybill_no
                ) b 
	on a.waybill_no = b.waybill_no
--      left join tmp_dm_predict.dws_fact_special_econ_dtl_tmp c on a.waybill_no = c.waybill_no
	where 
         a.inc_day>='20220521' and inc_day<='20220714' and   -- T-55D T-1D
	 substr(a.consigned_tm,0,10) between '2022-06-01' and '2022-07-14' ) a  -- 回刷历史数据 --  T-45D T-1D
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