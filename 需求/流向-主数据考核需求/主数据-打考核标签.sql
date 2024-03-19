-- 近30天真实主数据，写入T-1D分区
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
  '' as is_check,
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
			where inc_day >= '${yyyyMMdd30}'and inc_day <= '${yyyyMMdd1}' 
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
	-- 航空省份流向
	union all 
	select
		days,
		origin_name as origin_code,
		dest_name as dest_code,
		origin_name,
		dest_name,
		'3' as object_type_code,
		'航空省份流向' as object_type_name,
		sum(cast(all_waybill_num as int)) as all_waybill_num,
		sum(cast(all_quantity as int)) as all_quantity,
		sum(cast(all_weight as int)) as  all_weight
	from(
		select 
			a.days,
			b.province as origin_name,
			c.province as dest_name,
			a.all_waybill_num_air as all_waybill_num,
			a.all_quantity_air as all_quantity,
			cast(null as double) as all_weight
		from 
		(select * from dm_ordi_predict.dws_static_cityflow_base
			where inc_day >= '${yyyyMMdd30}'and inc_day <= '${yyyyMMdd1}' 
			and is_air = '1' 
			and cityflow regexp '(\\\d{3})-(\\\d{3})') a 
		left join(select city_code,province from dm_ordi_predict.dim_city_level_mapping_df where inc_day='${yyyyMMdd1}') b    
		on split(a.cityflow,'-')[0]=b.city_code
		left join(select city_code,province from dm_ordi_predict.dim_city_level_mapping_df where inc_day='${yyyyMMdd1}') c
		on split(a.cityflow,'-')[1]=c.city_code
	) t 
	group by
	  days,
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

-- 主数据打考核标签
insert overwrite table dm_ordi_predict.dws_static_flow_master_data_di partition(inc_day) 
-- 城市流向-打考核标签
select 
  days,
  origin_code,
  dest_code,
  origin_name,
  dest_name,
  object_type_code,
  object_type,
  a.all_waybill_num,
  all_quantity,
  all_weight,
  avg_waybill_num,
  avg_quantity,
  avg_weight,
  med_waybill_num,
  med_quantity,
  med_weight,
  if(b.cityflow is not null,b.is_check,'2') as is_check,	-- 0 不考核 1 考核 2 真实值为0
  a.inc_day 
from 
(
	SELECT * FROM dm_ordi_predict.dws_static_flow_master_data_di
		where inc_day = '${yyyyMMdd1}'
	and object_type_code = '1'
) a 
left join 
(
	-- 计算T-1D是否考核
	select 
		cityflow,
		if(sum(cast(all_waybill_num as int)) >= 1000,'1','0') as is_check
	from dm_ordi_predict.dws_static_cityflow_base
		where inc_day = '${yyyyMMdd1}'
	and cityflow regexp '(\\\d{3})-(\\\d{3})'
	group by cityflow
) b 
on concat(a.origin_code,'-',a.dest_code) = b.cityflow
-- 航空流向-打考核标签
union all 
select 
  days,
  origin_code,
  dest_code,
  origin_name,
  dest_name,
  object_type_code,
  object_type,
  all_waybill_num,
  all_quantity,
  all_weight,
  avg_waybill_num,
  avg_quantity,
  avg_weight,
  med_waybill_num,
  med_quantity,
  med_weight,
  if(b.cityflow is not null,b.is_check,'2') as is_check,	-- 0 不考核 1 考核 2 真实值为0
  a.inc_day 
from 
(
	SELECT * FROM dm_ordi_predict.dws_static_flow_master_data_di
		where inc_day = '${yyyyMMdd1}'
	and object_type_code = '2'
) a 
left join 
(	-- 计算T-1D是否考核
	select 
		a.cityflow, 
		if(b.cityflow is not null,'1','0') as is_check
	from 
		(
			select cityflow from dm_ordi_predict.dws_static_cityflow_base
			where inc_day = '${yyyyMMdd1}' 
			and is_air = '1' 
			and cityflow regexp '(\\\d{3})-(\\\d{3})'
			group by cityflow
		) a 
		left join 
		(
			select cityflow from dm_predict.airflow_keyflow_0327
			where keyflow_version='KeyFlow6831' group by cityflow
		) b on a.cityflow= b.cityflow 
) b 
on concat(a.origin_code,'-',a.dest_code) = b.cityflow
-- 航空省份流向-打考核标签
union all 
select 
  days,
  a.origin_code,
  a.dest_code,
  origin_name,
  dest_name,
  object_type_code,
  object_type,
  all_waybill_num,
  all_quantity,
  all_weight,
  avg_waybill_num,
  avg_quantity,
  avg_weight,
  med_waybill_num,
  med_quantity,
  med_weight,
  if(b.origin_code is not null and b.dest_code is not null,b.is_check,'2') as is_check,	-- 0 不考核 1 考核 2 真实值为0
  a.inc_day 
FROM 
(
	SELECT * FROM dm_ordi_predict.dws_static_flow_master_data_di
		where inc_day = '${yyyyMMdd1}'
	and object_type_code = '3'
) a 
left join 
(
	-- 计算T-1D是否考核
	select 
		b.province as origin_code,
		c.province as dest_code,
		'1' as is_check
	from 
	(
		select cityflow from dm_ordi_predict.dws_static_cityflow_base
		where inc_day = '${yyyyMMdd1}'
		and is_air = '1' 
		and cityflow regexp '(\\\d{3})-(\\\d{3})'
		group by cityflow
	) a 
	left join(select city_code,province from dm_ordi_predict.dim_city_level_mapping_df where inc_day='${yyyyMMdd1}') b    
	on split(a.cityflow,'-')[0]=b.city_code
	left join(select city_code,province from dm_ordi_predict.dim_city_level_mapping_df where inc_day='${yyyyMMdd1}') c
	on split(a.cityflow,'-')[1]=c.city_code
	group by
		b.province,
		c.province
) b 
on a.origin_code = b.origin_code 
and a.dest_code = b.dest_code 
;
