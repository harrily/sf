
set hive.auto.convert.join=true;  -- 预防倾斜


-- 判断是否特色经济
drop table if exists tmp_ordi_predict.tmp_shenhy_air_special_econ_tmp3;
create table tmp_ordi_predict.tmp_shenhy_air_special_econ_tmp3 
stored as parquet as  
select
	t1.*
	,t2.pro_name
	,t2.level_1_type
	,t2.level_2_type
from tmp_ordi_predict.tmp_shenhy_air_special_econ_tmp2 t1
left join 
(select 
	waybill_no,pro_name,level_1_type,level_2_type 
from 
	dm_bie.bie_fact_special_econ_dely_dtl
where inc_day between  '$[time(yyyyMMdd,-45d)]' and '$[time(yyyyMMdd,-11d)]'
union all
select 
 waybill_no,pro_name,level_1_type,level_2_type
from 
dm_ordi_predict.dws_spec_eco_pro_waybill_day
where inc_day between '$[time(yyyyMMdd,-10d)]' and  '$[time(yyyyMMdd,-1d)]')t2
on t1.waybill_no=t2.waybill_no;




drop table if exists tmp_ordi_predict.tmp_shenhy_air_special_econ_tmp4;
create table tmp_ordi_predict.tmp_shenhy_air_special_econ_tmp4 
stored as parquet as  
select
	t1.*
	,t2.dept_name as src_city_name
	,t3.dept_name as dest_city_name
from tmp_ordi_predict.tmp_shenhy_air_special_econ_tmp3 t1
left join 
(select 
	dept_code
	,dept_name
from dim.dim_department
where dept_code=dist_code) t2
on split(city_flow,'-')[0]=t2.dept_code
left join 
(select 
	dept_code
	,dept_name
from dim.dim_department
where dept_code=dist_code) t3
on split(city_flow,'-')[1]=t3.dept_code;
