drop table if exists tmp_ordi_predict.tmp_allflow_shenhy_air_kuaiman_tmp1;
create table tmp_ordi_predict.tmp_allflow_shenhy_air_kuaiman_tmp1 
stored as parquet as 
select 
	days                    
	,t1.cityflow                
	,t1.is_air as is_air_flow			 
	,weight_type_code 
	,operation_type
	,sum(all_waybill_num)  as tekuai_waybill_num
	,sum(all_quantity) as tekuai_quantity         
	,sum(weight) as tekuai_weight           
	,sum(volume) as tekuai_volume      
	,if(t3.province is not null,t3.province,'海外') as src_province   -- 使用mapping表
	,t3.distribution_name as src_distribution_name                    -- 使用mapping表
	,if(t4.province is not null,t4.province,'海外') as dest_province   -- 使用mapping表
	,t4.distribution_name as dest_distribution_name                    -- 使用mapping表
from(
		select t.* 
			,case when product_code in ('SE0089','SE0146','SE0107') and limit_type_code ='T4' then '慢产品'
				  when product_code = 'SE0152' then '慢产品'
				  else '快产品'
			end as operation_type
		from dm_ordi_predict.dws_static_his_cityflow t  -- 后续改调度后用
    ) t1
-- 切换mapping表关联
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
on nvl(split(t1.cityflow,'-')[0],'a')=t3.city_code
left join
(select
	city_code   --城市代码
	,city_name  --城市名称
	,province   --省份名称
	,distribution_name  -- 分拨区名称
from dm_ordi_predict.dim_city_level_mapping_df 
where inc_day='$[time(yyyyMMdd,-1d)]'
and  if_foreign='0'  -- 筛选国内
) t4
on nvl(split(t1.cityflow,'-')[1],'a')=t4.city_code
where t1.inc_day between '$[time(yyyyMMdd,-45d)]' and '$[time(yyyyMMdd)]' 
-- and is_hangkong_waybill='1'
and t1.is_air_waybill = '1' 
group by days                    
	,t1.cityflow 
	,operation_type  
	,t1.is_air 		 
	,weight_type_code 
	,if(t3.province is not null,t3.province,'海外') 
	,t3.distribution_name                   
	,if(t4.province is not null,t4.province,'海外')
	,t4.distribution_name ;


set hive.exec.dynamic.partition.mode= nostrict;
set hive.exec.max.dynamic.partitions=2000;  -- 所有执行节点上最多可以多少个动态分区
set hive.exec.max.dynamic.partitions.pernode = 500;

insert overwrite table dm_ordi_predict.dws_static_cityflow_tekuai_base partition(inc_day)  
select
days                    
,cityflow                
,is_air_flow			 
,weight_type_code 
,operation_type
,tekuai_waybill_num
,tekuai_quantity         
,tekuai_weight           
,tekuai_volume      
,src_province
,src_distribution_name
,dest_province
,dest_distribution_name 
,regexp_replace(days,'-','') as inc_day
from  tmp_ordi_predict.tmp_allflow_shenhy_air_kuaiman_tmp1 ;
