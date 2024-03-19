
insert overwrite table dm_ordi_predict.dws_static_flow_master_data_di partition(inc_day) 
select 
  '$[time(yyyy-MM-dd,-1d)]' as days,
  origin_code,
  dest_code,
  origin_name,
  dest_name,
  object_type_code,
  object_type_name,
  sum(all_waybill_num) as all_waybill_num,
  sum(all_quantity) as all_quantity,
  nvl(sum(all_weight),0) as all_weight,
  round(sum(all_waybill_num) / 30, 4) as avg_waybill_num,
  round(sum(all_quantity) / 30, 4) as avg_quantity,
  nvl(round(sum(all_weight) / 30, 4),0) as avg_weight,
  percentile(cast(all_waybill_num as int), 0.5) as med_waybill_num,
  percentile(cast(all_quantity as int), 0.5) as med_quantity,
  nvl(percentile(cast(all_weight as int), 0.5),0) as med_weight,
  '${yyyyMMdd1}' as inc_day 
from 
(
	-- 城市流向
	select 
		days,
		split(city_flow,'-')[0] as origin_code,
		split(city_flow,'-')[1] as dest_code,
		origin_name,
		dest_name,
		'1' as object_type_code,
		'城市流向' as object_type_name,
		sum(cast(all_waybill_num as int)) as all_waybill_num,
		sum(cast(all_quantity as int)) as all_quantity,
		sum(cast(all_weight as int)) as  all_weight
	from(
		select 
			a.days,
			a.cityflow as city_flow,
			b.city_name as origin_name,
			c.city_name as dest_name,
			a.all_waybill_num,
			a.all_quantity,
			cast(null as double) as all_weight
		from 
		(select * from dm_ordi_predict.dws_static_cityflow_base
		 where inc_day >= '${yyyyMMdd30}' and inc_day <= '${yyyyMMdd1}' and cityflow regexp '(\\\d{3})-(\\\d{3})') a 
		left join(select city_code,city_name from dm_ordi_predict.dim_city_level_mapping_df where inc_day='${yyyyMMdd1}') b    
		on split(a.cityflow,'-')[0]=b.city_code
		left join(select city_code,city_name from dm_ordi_predict.dim_city_level_mapping_df where inc_day='${yyyyMMdd1}') c
		on split(a.cityflow,'-')[1]=c.city_code
	) t 
	group by
	  days,
	  split(city_flow,'-')[0],
	  split(city_flow,'-')[1],
	  origin_name,
	  dest_name
	union all 
	-- 航空流向
	select
		days,
		split(city_flow,'-')[0] as origin_code,
		split(city_flow,'-')[1] as dest_code,
		origin_name,
		dest_name,
		'2' as object_type_code,
		'航空流向' as object_type_name,
		sum(cast(all_waybill_num as int)) as all_waybill_num,
		sum(cast(all_quantity as int)) as all_quantity,
		sum(cast(all_weight as int)) as  all_weight
	from(
		select 
			a.days,
			a.cityflow as city_flow,
			b.city_name as origin_name,
			c.city_name as dest_name,
			a.all_waybill_num_air as all_waybill_num,
			a.all_quantity_air as all_quantity,
			cast(null as double) as all_weight
		from 
		(select * from dm_ordi_predict.dws_static_cityflow_base
			where inc_day >= '${yyyyMMdd30}'
			and inc_day <= '${yyyyMMdd1}' 
			and is_air = '1' 
			and cityflow regexp '(\\\d{3})-(\\\d{3})') a 
		left join(select city_code,city_name from dm_ordi_predict.dim_city_level_mapping_df where inc_day='${yyyyMMdd1}') b    
		on split(a.cityflow,'-')[0]=b.city_code
		left join(select city_code,city_name from dm_ordi_predict.dim_city_level_mapping_df where inc_day='${yyyyMMdd1}') c
		on split(a.cityflow,'-')[1]=c.city_code
	) t 
	group by
		days,
	  split(city_flow,'-')[0],
	  split(city_flow,'-')[1],
	  origin_name,
	  dest_name
)
group by
	origin_code,
	dest_code,
	origin_name,
	dest_name,
	object_type_code,
	object_type_name
; 
